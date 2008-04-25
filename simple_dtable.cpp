/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>

#include "openat.h"
#include "transaction.h"

#include "stringset.h"
#include "simple_dtable.h"

/* simple dtable file format:
 * byte 0-3: magic number
 * byte 4-7: format version
 * bytes 8-11: key count
 * byte 12: key type (0 -> invalid, 1 -> uint32, 2 -> double, 3 -> string)
 * byte 13: key size (for uint32/string; 1-4 bytes)
 * byte 14: data length size (1-4 bytes)
 * byte 15: offset size (1-4 bytes)
 * byte 16-n: if key type is string, a string table
 * byte 16 or n+1: main data tables
 * 
 * main data tables:
 * key array:
 * [] = byte 0-m: key
 * [] = byte m+1-n: data length
 *      byte n+1-o: data offset (relative to data start)
 * each data blob:
 * [] = byte 0-m: data bytes */

/* TODO: use some sort of buffering here to avoid lots of small read()/write() calls */

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
	return ++index < sdt_source->key_count;
}

dtype simple_dtable::iter::key() const
{
	return sdt_source->get_key(index);
}

metablob simple_dtable::iter::meta() const
{
	size_t data_length;
	sdt_source->get_key(index, &data_length);
	return data_length ? metablob(data_length - 1) : metablob();
}

blob simple_dtable::iter::value() const
{
	return sdt_source->get_value(index);
}

const dtable * simple_dtable::iter::source() const
{
	return sdt_source;
}

dtable_iter * simple_dtable::iterator() const
{
	return new iter(this);
}

dtype simple_dtable::get_key(size_t index, size_t * data_length, off_t * data_offset) const
{
	assert(index < key_count);
	int r;
	uint8_t size = key_size + length_size + offset_size;
	uint8_t bytes[size];
	
	lseek(fd, key_start_off + size * index, SEEK_SET);
	r = read(fd, bytes, size);
	assert(r == size);
	
	if(data_length)
		/* all data lengths are stored incremented by 1, to free up 0 for negative entries */
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
			return dtype(st_get(&st, read_bytes(bytes, 0, key_size)));
	}
	abort();
}

int simple_dtable::find_key(dtype key, size_t * data_length, off_t * data_offset, size_t * index) const
{
	/* binary search */
	ssize_t min = 0, max = key_count - 1;
	if(!key_count)
		return -ENOENT;
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		dtype value = get_key(mid, data_length, data_offset);
		if(value < key)
			min = mid + 1;
		else if(value > key)
			max = mid - 1;
		else
		{
			if(index)
				*index = mid;
			return 0;
		}
	}
	return -ENOENT;
}

blob simple_dtable::get_value(size_t index, size_t data_length, off_t data_offset) const
{
	blob value(data_length);
	lseek(fd, data_start_off + data_offset, SEEK_SET);
	data_length = read(fd, value.memory(), value.size());
	assert(data_length == value.size());
	return value;
}

blob simple_dtable::get_value(size_t index) const
{
	assert(index < key_count);
	size_t data_length;
	off_t data_offset;
	dtype key = get_key(index, &data_length, &data_offset);
	return get_value(index, data_length, data_offset);
}

blob simple_dtable::lookup(dtype key, const dtable ** source) const
{
	size_t data_length, index;
	off_t data_offset;
	int r = find_key(key, &data_length, &data_offset, &index);
	if(r < 0)
	{
		*source = NULL;
		return blob();
	}
	*source = this;
	if(data_length == (size_t) -1)
		return blob();
	return get_value(index, data_length, data_offset);
}

int simple_dtable::init(int dfd, const char * file)
{
	int r = -1;
	struct dtable_header header;
	if(fd >= 0)
		deinit();
	fd = openat(dfd, file, O_RDONLY);
	if(fd < 0)
		return fd;
	if(read(fd, &header, sizeof(header)) != sizeof(header))
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
		case 3:
			ktype = dtype::STRING;
			if(key_size > 4)
				goto fail;
			r = st_init(&st, fd, key_start_off);
			if(r < 0)
				goto fail;
			key_start_off += st.size;
			r = lseek(fd, key_start_off, SEEK_SET);
			if(r < 0)
			{
				st_kill(&st);
				goto fail;
			}
			break;
		default:
			goto fail;
	}
	data_start_off = key_start_off + (key_size + length_size + offset_size) * key_count;
	
	return 0;
	
fail:
	close(fd);
	fd = -1;
	return (r < 0) ? r : -1;
}

void simple_dtable::deinit()
{
	if(fd >= 0)
	{
		if(ktype == dtype::STRING)
			st_kill(&st);
		close(fd);
		fd = -1;
	}
}

/* FIXME: by reserving space for the header, and storing the string table at the end of the file, we
 * can reduce the number of passes over the input keys to only 1 (but still that plus the data) */
int simple_dtable::create(int dfd, const char * file, const dtable * source, const dtable * shadow)
{
	dtable_iter * iter;
	dtype::ctype key_type = source->key_type();
	stringset strings;
	const char ** string_array = NULL;
	size_t string_count = 0, key_count = 0;
	size_t max_data_size = 0, total_data_size = 0;
	uint32_t max_key = 0;
	if(shadow && shadow->key_type() != key_type)
		return -EINVAL;
	if(key_type == dtype::STRING)
	{
		int r = strings.init();
		if(r < 0)
			return r;
	}
	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		metablob meta = iter->meta();
		iter->next();
		if(meta.negative())
			/* omit negative entries no longer needed */
			if(!shadow || shadow->find(key).negative())
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
				strings.add(key.str);
				break;
		}
		if(meta.size() > max_data_size)
			max_data_size = meta.size();
		total_data_size += meta.size();
	}
	delete iter;
	if(strings.ready())
	{
		string_count = strings.size();
		string_array = strings.array();
		if(!string_array)
			return -ENOMEM;
	}
	
	/* now write the file */
	int r, i, size;
	tx_fd fd;
	off_t out_off;
	
	dtable_header header;
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
			header.key_size = byte_size(string_count - 1);
			break;
		default:
			r = -EINVAL;
			goto out_strings;
	}
	/* we reserve size 0 for negative entries, so add 1 */
	header.length_size = byte_size(max_data_size + 1);
	header.offset_size = byte_size(total_data_size);
	size = header.key_size + header.length_size + header.offset_size;
	
	fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
	if(fd < 0)
	{
		r = fd;
		goto out_strings;
	}
	r = tx_write(fd, &header, 0, sizeof(header));
	if(r < 0)
	{
	fail_unlink:
		tx_close(fd);
		tx_unlink(dfd, file);
		goto out_strings;
	}
	out_off = sizeof(header);
	if(string_array)
	{
		r = st_create(fd, &out_off, string_array, string_count);
		if(r < 0)
			goto fail_unlink;
	}
	
	/* now the key array */
	total_data_size = 0;
	iter = source->iterator();
	while(iter->valid())
	{
		uint8_t bytes[size];
		dtype key = iter->key();
		metablob meta = iter->meta();
		iter->next();
		i = 0;
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
				break;
		}
		layout_bytes(bytes, &i, meta.negative() ? 0 : (meta.size() + 1), header.length_size);
		layout_bytes(bytes, &i, total_data_size, header.offset_size);
		r = tx_write(fd, bytes, out_off, i);
		if(r < 0)
		{
			delete iter;
			goto fail_unlink;
		}
		out_off += i;
		total_data_size += meta.size();
	}
	delete iter;
	
	/* and the data itself*/
	iter = source->iterator();
	while(iter->valid())
	{
		blob value = iter->value();
		iter->next();
		r = tx_write(fd, &value[0], out_off, value.size());
		if(r < 0)
		{
			delete iter;
			goto fail_unlink;
		}
		out_off += value.size();
	}
	delete iter;
	/* assume tx_close() works */
	tx_close(fd);
	r = 0;
	
out_strings:
	if(string_array)
		st_array_free(string_array, string_count);
	return r;
}
