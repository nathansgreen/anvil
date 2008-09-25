/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include <vector>

#include "rwfile.h"
#include "blob_buffer.h"
#include "simple_dtable.h"

/* simple dtable file format:
 * bytes 0-3: magic number
 * bytes 4-7: format version
 * bytes 8-11: key count
 * byte 12: key type (0 -> invalid, 1 -> uint32, 2 -> double, 3 -> string, 4 -> blob)
 * byte 13: key size (for uint32/string/blob; 1-4 bytes)
 * byte 14: data length size (1-4 bytes)
 * byte 15: offset size (1-4 bytes)
 * bytes 16-19: if key type is blob, blob comparator name length
 * bytes 20-n: if key type is blob and length > 0, blob comparator name
 * bytes 16-m, 20-m, or n+1-m: if key type is string/blob, a string table
 * byte 16 or m+1: main data tables
 * 
 * main data tables:
 * key array:
 * [] = byte 0-m: key
 * [] = byte m+1-n: data length
 *      byte n+1-o: data offset (relative to data start)
 * each data blob:
 * [] = byte 0-m: data bytes */

simple_dtable::iter::iter(const simple_dtable * source)
	: index(0), sdt_source(source)
{
}

bool simple_dtable::iter::valid() const
{
	return index < sdt_source->key_count;
}

bool simple_dtable::iter::next()
{
	if(index == sdt_source->key_count)
		return false;
	return ++index < sdt_source->key_count;
}

bool simple_dtable::iter::prev()
{
	if(!index)
		return false;
	index--;
	return true;
}

bool simple_dtable::iter::last()
{
	if(!sdt_source->key_count)
		return false;
	index = sdt_source->key_count - 1;
	return true;
}

dtype simple_dtable::iter::key() const
{
	return sdt_source->get_key(index);
}

bool simple_dtable::iter::seek(const dtype & key)
{
	return sdt_source->find_key(key, NULL, NULL, &index) >= 0;
}

metablob simple_dtable::iter::meta() const
{
	size_t data_length;
	sdt_source->get_key(index, &data_length);
	return data_length ? metablob(data_length) : metablob();
}

blob simple_dtable::iter::value() const
{
	return sdt_source->get_value(index);
}

const dtable * simple_dtable::iter::source() const
{
	return sdt_source;
}

dtable::iter * simple_dtable::iterator() const
{
	return new iter(this);
}

dtype simple_dtable::get_key(size_t index, size_t * data_length, off_t * data_offset) const
{
	assert(index < key_count);
	int r;
	uint8_t size = key_size + length_size + offset_size;
	uint8_t bytes[size];
	
	r = fp->read(key_start_off + size * index, bytes, size);
	assert(r == size);
	
	if(data_length)
		/* all data lengths are stored incremented by 1, to free up 0 for non-existent entries */
		*data_length = read_bytes(bytes, key_size, length_size) - 1;
	if(data_offset)
		*data_offset = read_bytes(bytes, key_size + length_size, offset_size);
	
	switch(ktype)
	{
		case dtype::UINT32:
			return dtype(read_bytes(bytes, 0, key_size));
		case dtype::DOUBLE:
		{
			double value;
			memcpy(&value, bytes, sizeof(double));
			return dtype(value);
		}
		case dtype::STRING:
			return dtype(st.get(read_bytes(bytes, 0, key_size)));
		case dtype::BLOB:
			return dtype(st.get_blob(read_bytes(bytes, 0, key_size)));
	}
	abort();
}

int simple_dtable::find_key(const dtype & key, size_t * data_length, off_t * data_offset, size_t * index) const
{
	/* binary search */
	ssize_t min = 0, max = key_count - 1;
	assert(ktype != dtype::BLOB || !cmp_name == !blob_cmp);
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		dtype value = get_key(mid, data_length, data_offset);
		int c = value.compare(key, blob_cmp);
		if(c < 0)
			min = mid + 1;
		else if(c > 0)
			max = mid - 1;
		else
		{
			if(index)
				*index = mid;
			return 0;
		}
	}
	if(index)
		*index = min;
	return -ENOENT;
}

blob simple_dtable::get_value(size_t index, size_t data_length, off_t data_offset) const
{
	blob_buffer value(data_length);
	value.set_size(data_length, false);
	assert(data_length == value.size());
	data_length = fp->read(data_start_off + data_offset, &value[0], data_length);
	assert(data_length == value.size());
	return value;
}

blob simple_dtable::get_value(size_t index) const
{
	assert(index < key_count);
	size_t data_length;
	off_t data_offset;
	dtype key = get_key(index, &data_length, &data_offset);
	return (data_length != (size_t) -1) ? get_value(index, data_length, data_offset) : blob();
}

blob simple_dtable::lookup(const dtype & key, bool * found) const
{
	size_t data_length, index;
	off_t data_offset;
	int r = find_key(key, &data_length, &data_offset, &index);
	if(r < 0)
	{
		*found = false;
		return blob();
	}
	*found = true;
	if(data_length == (size_t) -1)
		return blob();
	return get_value(index, data_length, data_offset);
}

int simple_dtable::init(int dfd, const char * file, const params & config)
{
	int r = -1;
	struct dtable_header header;
	if(fp)
		deinit();
	/* the larger the buffers, the more memory we use but the fewer read() system calls we'll make... */
	fp = rofile::open<16, 12>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read(0, &header) < 0)
		goto fail;
	if(header.magic != SDTABLE_MAGIC || header.version != SDTABLE_VERSION)
		goto fail;
	key_count = header.key_count;
	key_start_off = sizeof(header);
	key_size = header.key_size;
	length_size = header.length_size;
	offset_size = header.offset_size;
	switch(header.key_type)
	{
		case 1:
			ktype = dtype::UINT32;
			if(key_size > 4)
				goto fail;
			break;
		case 2:
			ktype = dtype::DOUBLE;
			if(key_size != sizeof(double))
				goto fail;
			break;
		case 4:
			uint32_t length;
			if(fp->read(key_start_off, &length) < 0)
				goto fail;
			key_start_off += sizeof(length);
			if(length)
			{
				char string[length];
				if(fp->read(key_start_off, string, length) != (ssize_t) length)
					goto fail;
				key_start_off += length;
				cmp_name = istr(string, length);
			}
			/* fall through */
		case 3:
			ktype = (header.key_type == 3) ? dtype::STRING : dtype::BLOB;
			if(key_size > 4)
				goto fail;
			r = st.init(fp, key_start_off);
			if(r < 0)
				goto fail;
			key_start_off += st.get_size();
			break;
		default:
			goto fail;
	}
	data_start_off = key_start_off + (key_size + length_size + offset_size) * key_count;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	return (r < 0) ? r : -1;
}

void simple_dtable::deinit()
{
	if(fp)
	{
		if(ktype == dtype::STRING)
			st.deinit();
		delete fp;
		fp = NULL;
		dtable::deinit();
	}
}

int simple_dtable::create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow)
{
	std::vector<istr> strings;
	std::vector<blob> blobs;
	dtable::iter * iter;
	dtype::ctype key_type = source->key_type();
	const blob_comparator * blob_cmp = source->get_blob_cmp();
	size_t key_count = 0, max_data_size = 0, total_data_size = 0;
	uint32_t max_key = 0;
	dtable_header header;
	int r, size;
	rwfile out;
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		metablob meta = iter->meta();
		iter->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		key_count++;
		assert(key.type == key_type);
		switch(key.type)
		{
			case dtype::UINT32:
				if(key.u32 > max_key)
					max_key = key.u32;
				break;
			case dtype::DOUBLE:
				/* nothing to do */
				break;
			case dtype::STRING:
				strings.push_back(key.str);
				break;
			case dtype::BLOB:
				blobs.push_back(key.blb);
				break;
		}
		if(meta.size() > max_data_size)
			max_data_size = meta.size();
		total_data_size += meta.size();
	}
	delete iter;
	
	/* now write the file */
	header.magic = SDTABLE_MAGIC;
	header.version = SDTABLE_VERSION;
	header.key_count = key_count;
	switch(key_type)
	{
		case dtype::UINT32:
			header.key_type = 1;
			header.key_size = byte_size(max_key);
			break;
		case dtype::DOUBLE:
			header.key_type = 2;
			header.key_size = sizeof(double);
			break;
		case dtype::STRING:
			header.key_type = 3;
			header.key_size = byte_size(strings.size() - 1);
			break;
		case dtype::BLOB:
			header.key_type = 4;
			header.key_size = byte_size(blobs.size() - 1);
			break;
	}
	/* we reserve size 0 for non-existent entries, so add 1 */
	header.length_size = byte_size(max_data_size + 1);
	header.offset_size = byte_size(total_data_size);
	size = header.key_size + header.length_size + header.offset_size;
	
	r = out.create(dfd, file);
	if(r < 0)
		return r;
	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;
	if(key_type == dtype::BLOB)
	{
		uint32_t length = blob_cmp ? strlen(blob_cmp->name) : 0;
		out.append(&length);
		if(length)
			out.append(blob_cmp->name);
	}
	if(key_type == dtype::STRING)
	{
		r = stringtbl::create(&out, strings);
		if(r < 0)
			goto fail_unlink;
	}
	else if(key_type == dtype::BLOB)
	{
		r = stringtbl::create(&out, blobs);
		if(r < 0)
			goto fail_unlink;
	}
	
	/* now the key array */
	total_data_size = 0;
	iter = source->iterator();
	while(iter->valid())
	{
		int i = 0;
		uint8_t bytes[size];
		dtype key = iter->key();
		metablob meta = iter->meta();
		iter->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		switch(key.type)
		{
			case dtype::UINT32:
				layout_bytes(bytes, &i, key.u32, header.key_size);
				break;
			case dtype::DOUBLE:
				memcpy(bytes, &key.dbl, sizeof(double));
				i += sizeof(double);
				break;
			case dtype::STRING:
				max_key = istr::locate(strings, key.str);
				layout_bytes(bytes, &i, max_key, header.key_size);
				break;
			case dtype::BLOB:
				max_key = blob::locate(blobs, key.blb, blob_cmp);
				layout_bytes(bytes, &i, max_key, header.key_size);
				break;
		}
		layout_bytes(bytes, &i, meta.exists() ? meta.size() + 1 : 0, header.length_size);
		layout_bytes(bytes, &i, total_data_size, header.offset_size);
		r = out.append(bytes, i);
		if(r != i)
		{
			delete iter;
			goto fail_unlink;
		}
		total_data_size += meta.size();
	}
	delete iter;
	
	/* and the data itself */
	iter = source->iterator();
	while(iter->valid())
	{
		blob value = iter->value();
		iter->next();
		if(!value.exists())
			continue;
		r = out.append(&value[0], value.size());
		if(r != (int) value.size())
		{
			delete iter;
			goto fail_unlink;
		}
	}
	delete iter;
	
	r = out.close();
	if(r < 0)
		goto fail_unlink;
	return 0;
	
fail_unlink:
	out.close();
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(simple_dtable);
