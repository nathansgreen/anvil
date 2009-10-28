/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include <vector>

#include "util.h"
#include "rwfile.h"
#include "blob_buffer.h"
#include "fixed_dtable.h"

/* TODO: the keys can be put in with the data since it's all fixed-size */

/* fixed dtable file format:
 * bytes 0-3: magic number
 * bytes 4-7: format version
 * bytes 8-11: key count
 * bytes 12-15: value size
 * byte 16: key type (0 -> invalid, 1 -> uint32, 2 -> double, 3 -> string, 4 -> blob)
 * byte 17: key size (for uint32/string/blob; 1-4 bytes)
 * bytes 18-21: if key type is blob, blob comparator name length
 * bytes 22-n: if key type is blob and length > 0, blob comparator name
 * bytes 18-m, 22-m, or n+1-m: if key type is string/blob, a string table
 * byte 18 or m+1: main data tables
 * 
 * main data tables:
 * key array:
 * [] = byte 0-m: key
 * [] = byte m+1: value exists (bool)
 * each data blob:
 * [] = byte 0-m: data bytes */

fixed_dtable::iter::iter(const fixed_dtable * source)
	: iter_source<fixed_dtable>(source), index(0)
{
}

bool fixed_dtable::iter::valid() const
{
	return index < dt_source->key_count;
}

bool fixed_dtable::iter::next()
{
	if(index == dt_source->key_count)
		return false;
	return ++index < dt_source->key_count;
}

bool fixed_dtable::iter::prev()
{
	if(!index)
		return false;
	index--;
	return true;
}

bool fixed_dtable::iter::first()
{
	if(!dt_source->key_count)
		return false;
	index = 0;
	return true;
}

bool fixed_dtable::iter::last()
{
	if(!dt_source->key_count)
		return false;
	index = dt_source->key_count - 1;
	return true;
}

dtype fixed_dtable::iter::key() const
{
	return dt_source->get_key(index);
}

bool fixed_dtable::iter::seek(const dtype & key)
{
	return dt_source->find_key(key, NULL, NULL, &index) >= 0;
}

bool fixed_dtable::iter::seek(const dtype_test & test)
{
	return dt_source->find_key(test, &index) >= 0;
}

bool fixed_dtable::iter::seek_index(size_t index)
{
	/* we allow seeking to one past the end, just
	 * as we allow getting there with next() */
	if(index < 0 || index > dt_source->key_count)
		return false;
	this->index = index;
	return index < dt_source->key_count;
}

size_t fixed_dtable::iter::get_index() const
{
	return index;
}

metablob fixed_dtable::iter::meta() const
{
	bool data_exists;
	dt_source->get_key(index, &data_exists);
	return data_exists ? metablob(dt_source->value_size) : metablob();
}

blob fixed_dtable::iter::value() const
{
	return dt_source->get_value(index);
}

const dtable * fixed_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * fixed_dtable::iterator() const
{
	return new iter(this);
}

bool fixed_dtable::present(const dtype & key, bool * found) const
{
	bool data_exists;
	if(find_key(key, &data_exists) < 0)
	{
		*found = false;
		return false;
	}
	*found = true;
	return data_exists;
}

dtype fixed_dtable::get_key(size_t index, bool * data_exists, off_t * data_offset) const
{
	assert(index < key_count);
	uint8_t size = key_size + 1;
	uint8_t bytes[size];
	int r;
	
	r = fp->read(key_start_off + size * index, bytes, size);
	assert(r == size);
	
	if(data_exists)
		*data_exists = bytes[key_size];
	if(data_offset)
		*data_offset = index * value_size;
	
	switch(ktype)
	{
		case dtype::UINT32:
			return dtype(util::read_bytes(bytes, 0, key_size));
		case dtype::DOUBLE:
		{
			double value;
			util::memcpy(&value, bytes, sizeof(double));
			return dtype(value);
		}
		case dtype::STRING:
			return dtype(st.get(util::read_bytes(bytes, 0, key_size)));
		case dtype::BLOB:
			return dtype(st.get_blob(util::read_bytes(bytes, 0, key_size)));
	}
	abort();
}

template<class T>
int fixed_dtable::find_key(const T & test, size_t * index, bool * data_exists, off_t * data_offset) const
{
	/* binary search */
	ssize_t min = 0, max = key_count - 1;
	assert(ktype != dtype::BLOB || !cmp_name == !blob_cmp);
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		dtype value = get_key(mid, data_exists, data_offset);
		int c = test(value);
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

blob fixed_dtable::get_value(size_t index, off_t data_offset) const
{
	size_t length;
	if(!value_size)
		return blob::empty;
	blob_buffer value(value_size);
	value.set_size(value_size, false);
	assert(value_size == value.size());
	length = fp->read(data_start_off + data_offset, &value[0], value_size);
	assert(length == value_size);
	return value;
}

blob fixed_dtable::get_value(size_t index) const
{
	assert(index < key_count);
	bool data_exists;
	off_t data_offset;
	dtype key = get_key(index, &data_exists, &data_offset);
	return data_exists ? get_value(index, data_offset) : blob();
}

blob fixed_dtable::lookup(const dtype & key, bool * found) const
{
	bool data_exists;
	size_t index;
	off_t data_offset;
	int r = find_key(key, &data_exists, &data_offset, &index);
	if(r < 0)
	{
		*found = false;
		return blob();
	}
	*found = true;
	if(!data_exists)
		return blob();
	return get_value(index, data_offset);
}

blob fixed_dtable::index(size_t index) const
{
	if(index < 0 || index >= key_count)
		return blob();
	return get_value(index);
}

bool fixed_dtable::contains_index(size_t index) const
{
	bool data_exists;
	if(index < 0 || index >= key_count)
		return false;
	get_key(index, &data_exists);
	return data_exists;
}

int fixed_dtable::init(int dfd, const char * file, const params & config)
{
	int r = -1;
	dtable_header header;
	if(fp)
		deinit();
	fp = rofile::open_mmap<64, 24>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read_type(0, &header) < 0)
		goto fail;
	if(header.magic != FDTABLE_MAGIC || header.version != FDTABLE_VERSION)
		goto fail;
	key_count = header.key_count;
	key_start_off = sizeof(header);
	value_size = header.value_size;
	key_size = header.key_size;
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
			if(fp->read_type(key_start_off, &length) < 0)
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
	data_start_off = key_start_off + (key_size + 1) * key_count;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	return (r < 0) ? r : -1;
}

void fixed_dtable::deinit()
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

int fixed_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	std::vector<istr> strings;
	std::vector<blob> blobs;
	dtype::ctype key_type = source->key_type();
	const blob_comparator * blob_cmp = source->get_blob_cmp();
	bool value_size_known = false;
	size_t key_count = 0;
	uint32_t max_key = 0;
	dtable_header header;
	rwfile out;
	int r;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	if(!config.get("value_size", &r, 0) || r < 0)
		return -EINVAL;
	if(r)
	{
		header.value_size = r;
		value_size_known = true;
	}
	else
		header.value_size = 0;
	
	/* just to be sure */
	source->first();
	while(source->valid())
	{
		dtype key = source->key();
		metablob meta = source->meta();
		source->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
				continue;
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
		key_count++;
		if(meta.exists() && !value_size_known)
		{
			header.value_size = meta.size();
			value_size_known = true;
		}
	}
	/* if we still don't know the value size, we'll just stick with 0 */
	
	/* now write the file */
	header.magic = FDTABLE_MAGIC;
	header.version = FDTABLE_VERSION;
	header.key_count = key_count;
	switch(key_type)
	{
		case dtype::UINT32:
			header.key_type = 1;
			header.key_size = util::byte_size(max_key);
			break;
		case dtype::DOUBLE:
			header.key_type = 2;
			header.key_size = sizeof(double);
			break;
		case dtype::STRING:
			header.key_type = 3;
			header.key_size = util::byte_size(strings.size() - 1);
			break;
		case dtype::BLOB:
			header.key_type = 4;
			header.key_size = util::byte_size(blobs.size() - 1);
			break;
	}
	
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
	max_key = 0;
	source->first();
	while(source->valid())
	{
		int i = 0;
		uint8_t bytes[header.key_size + 1];
		dtype key = source->key();
		metablob meta = source->meta();
		source->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
				continue;
		switch(key.type)
		{
			case dtype::UINT32:
				util::layout_bytes(bytes, &i, key.u32, header.key_size);
				break;
			case dtype::DOUBLE:
				util::memcpy(bytes, &key.dbl, sizeof(double));
				i += sizeof(double);
				break;
			case dtype::STRING:
				/* no need to locate the string; it's the next one */
				util::layout_bytes(bytes, &i, max_key, header.key_size);
				max_key++;
				break;
			case dtype::BLOB:
				/* no need to locate the blob; it's the next one */
				util::layout_bytes(bytes, &i, max_key, header.key_size);
				max_key++;
				break;
		}
		bytes[i++] = meta.exists();
		r = out.append(bytes, i);
		if(r != i)
			goto fail_unlink;
	}
	
	/* and the data itself */
	source->first();
	while(source->valid())
	{
		dtype key = source->key();
		blob value = source->value();
		if(!value.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
			{
				source->next();
				continue;
			}
		if(value.exists() && value.size() != header.value_size)
		{
			/* all the items in this dtable must be the same size */
			if(!source->reject(&value))
				goto fail_unlink;
			if(value.exists() && value.size() != header.value_size)
				goto fail_unlink;
		}
		if(value.exists())
			r = out.append(value);
		else
			r = out.pad(header.value_size);
		if(r < 0)
			goto fail_unlink;
		source->next();
	}
	
	r = out.close();
	if(r < 0)
		goto fail_unlink;
	return 0;
	
fail_unlink:
	out.close();
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(fixed_dtable);
