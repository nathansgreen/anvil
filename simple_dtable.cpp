/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>

#include "openat.h"
#include "transaction.h"

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
	: index(0), source(source)
{
}

bool simple_dtable::iter::valid() const
{
	return index < source->key_count;
}

bool simple_dtable::iter::next()
{
	return ++index < source->key_count;
}

dtype simple_dtable::iter::key() const
{
	return source->get_key(index);
}

blob simple_dtable::iter::value() const
{
	return source->get_value(index);
}

const dtable * simple_dtable::iter::extra() const
{
	return source;
}

sane_iter3<dtype, blob, const dtable *> * simple_dtable::iterator() const
{
	return new iter(this);
}

dtype simple_dtable::get_key(size_t index, size_t * data_length, off_t * data_offset) const
{
	assert(index < key_count);
	int r;
	uint8_t size = key_size + length_size + offset_size;
	uint8_t i = 0, bytes[size];
	
	lseek(fd, key_start_off + size * index, SEEK_SET);
	r = read(fd, bytes, size);
	assert(r == size);
	
	if(data_length)
	{
		*data_length = 0;
		for(i = key_size; i < key_size + length_size; i++)
			*data_length = ((*data_length) << 8) | bytes[i];
		/* all data lengths are stored incremented by 1, to free up 0 for negative entries */
		--*data_length;
	}
	if(data_offset)
	{
		*data_offset = 0;
		for(i = key_size + length_size; i < size; i++)
			*data_offset = ((*data_offset) << 8) | bytes[i];
	}
	
	switch(key_type)
	{
		case dtype::UINT32:
		{
			uint32_t value = 0;
			for(i = 0; i < key_size; i++)
				value = (value << 8) | bytes[i];
			return dtype(value);
		}
		case dtype::DOUBLE:
		{
			double value;
			memcpy(&value, bytes, sizeof(double));
			return dtype(value);
		}
		case dtype::STRING:
		{
			uint32_t index = 0;
			for(i = 0; i < key_size; i++)
				index = (index << 8) | bytes[i];
			return dtype(st_get(&st, index));
		}
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
			key_type = dtype::UINT32;
			if(key_size > 4)
				goto fail;
			break;
		case 2:
			key_type = dtype::DOUBLE;
			if(key_size != sizeof(double))
				goto fail;
			break;
		case 3:
			key_type = dtype::STRING;
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
		if(key_type == dtype::STRING)
			st_kill(&st);
		close(fd);
		fd = -1;
	}
}

int simple_dtable::create(int dfd, const char * file, const dtable * source, const dtable * shadow)
{
	return -ENOSYS;
}
