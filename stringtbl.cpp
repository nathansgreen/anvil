/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <algorithm>

#include "blob_buffer.h"
#include "stringtbl.h"

#define STRINGTBL_VERSION 1

struct st_header {
	uint8_t version;
	uint8_t binary;
	uint8_t bytes[2];
	ssize_t count;
} __attribute__((packed));

int stringtbl::init(const rofile * fp, off_t start)
{
	int r;
	st_header header;
	off_t offset = start + sizeof(header);
	if(this->fp)
		deinit();
	r = fp->read(start, &header);
	if(r < 0)
		return (r < 0) ? r : -1;
	if(header.version != STRINGTBL_VERSION)
		return -EINVAL;
	if(header.bytes[0] > 4 || header.bytes[1] > 4)
		return -EINVAL;
	this->fp = fp;
	this->start = start;
	binary = header.binary;
	bytes[0] = header.bytes[0];
	bytes[1] = header.bytes[1];
	bytes[2] = bytes[0] + bytes[1];
	count = header.count;
	/* calculate size */
	size = sizeof(header) + bytes[2] * count;
	for(ssize_t i = 0; i < count; i++)
	{
		uint32_t value = 0;
		uint8_t buffer[8];
		r = fp->read(offset, buffer, bytes[2]);
		if(r != bytes[2])
		{
			this->fp = NULL;
			return (r < 0) ? r : -1;
		}
		offset += bytes[2];
		/* read big endian order */
		for(r = 0; r < bytes[0]; r++)
			value = (value << 8) | buffer[r];
		size += value;
	}
	for(ssize_t i = 0; i < ST_LRU; i++)
	{
		lru[i].index = -1;
		lru[i].string = NULL;
		assert(!lru[i].binary.exists());
	}
	lru_next = 0;
	return 0;
}

void stringtbl::deinit()
{
	if(fp)
	{
		for(ssize_t i = 0; i < ST_LRU; i++)
		{
			if(lru[i].string)
				free((void *) lru[i].string);
			if(lru[i].binary.exists())
				lru[i].binary = blob();
		}
		fp = NULL;
	}
}

const char * stringtbl::get(ssize_t index) const
{
	int i, bc = 0;
	off_t offset;
	ssize_t length = 0;
	uint8_t buffer[8];
	char * string;
	if(index < 0 || index >= count)
		return NULL;
	for(i = 0; i < ST_LRU; i++)
		if(lru[i].index == index)
			return lru[i].string;
	/* not in LRU */
	offset = start + sizeof(st_header) + index * bytes[2];
	i = fp->read(offset, buffer, bytes[2]);
	if(i != bytes[2])
		return NULL;
	/* read big endian order */
	for(i = 0; i < bytes[0]; i++)
		length = (length << 8) | buffer[bc++];
	offset = 0;
	for(i = 0; i < bytes[1]; i++)
		offset = (offset << 8) | buffer[bc++];
	offset += start;
	/* now we have the length and offset */
	string = (char *) malloc(length + 1);
	if(!string)
		return NULL;
	i = fp->read(offset, string, length);
	if(i != length)
	{
		free(string);
		return NULL;
	}
	string[length] = 0;
	i = lru_next;
	lru[i].index = index;
	if(lru[i].string)
		free((void *) lru[i].string);
	if(lru[i].binary.exists())
		lru[i].binary = blob();
	lru[i].string = string;
	return string;
}

const blob & stringtbl::get_blob(ssize_t index) const
{
	int i, bc = 0;
	off_t offset;
	ssize_t length = 0;
	uint8_t buffer[8];
	if(index < 0 || index >= count)
		return blob::dne;
	for(i = 0; i < ST_LRU; i++)
		if(lru[i].index == index)
			return lru[i].binary;
	/* not in LRU */
	offset = start + sizeof(st_header) + index * bytes[2];
	i = fp->read(offset, buffer, bytes[2]);
	if(i != bytes[2])
		return blob::dne;
	/* read big endian order */
	for(i = 0; i < bytes[0]; i++)
		length = (length << 8) | buffer[bc++];
	offset = 0;
	for(i = 0; i < bytes[1]; i++)
		offset = (offset << 8) | buffer[bc++];
	offset += start;
	/* now we have the length and offset */
	blob_buffer data(length);
	data.set_size(length, false);
	i = fp->read(offset, &data[0], length);
	if(i != length)
		return blob::dne;
	i = lru_next;
	lru[i].index = index;
	if(lru[i].string)
	{
		free((void *) lru[i].string);
		lru[i].string = NULL;
	}
	lru[i].binary = data;
	return lru[i].binary;
}

ssize_t stringtbl::locate(const char * string) const
{
	/* binary search */
	ssize_t min = 0, max = count - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		const char * value = get(index);
		if(!value)
			return -1;
		c = strcmp(value, string);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}

ssize_t stringtbl::locate(const blob & search, const blob_comparator * blob_cmp) const
{
	/* binary search */
	ssize_t min = 0, max = count - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		blob value = get_blob(index);
		if(!value.exists())
			return -1;
		c = blob_cmp ? blob_cmp->compare(value, search) : value.compare(search);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}

void stringtbl::array_sort(const char ** array, ssize_t count)
{
	std::sort(array, &array[count], strcmp_less());
}

void stringtbl::array_sort(blob * array, ssize_t count, const blob_comparator * blob_cmp)
{
	if(blob_cmp)
	{
		blob_comparator_object comparator(blob_cmp);
		std::sort(array, &array[count], comparator);
	}
	else
		std::sort(array, &array[count], blob_comparator_null());
}

int stringtbl::create(rwfile * fp, const char ** strings, ssize_t count, bool need_sort)
{
	st_header header = {STRINGTBL_VERSION, 0, {4, 1}, count};
	size_t size = 0, max = 0;
	ssize_t i;
	int r;
	/* the strings must be sorted */
	if(need_sort)
		array_sort(strings, count);
	for(i = 0; i < count; i++)
	{
		size_t length = strlen(strings[i]);
		size += length;
		if(length > max)
			max = length;
	}
	/* figure out the correct size of the length field */
	if(max < 0x100)
		header.bytes[0] = 1;
	else if(max < 0x10000)
		header.bytes[0] = 2;
	else if(max < 0x1000000)
		header.bytes[0] = 3;
	/* figure out the correct size of the offset field */
	for(i = 0; i < 3; i++)
	{
		max = sizeof(header) + (header.bytes[0] + header.bytes[1]) * count + size;
		if(max < 0x100)
			break;
		else if(max < 0x10000)
			header.bytes[1] = 2;
		else if(max < 0x1000000)
			header.bytes[1] = 3;
		else
			header.bytes[1] = 4;
	}
	/* write the header */
	r = fp->append(&header);
	if(r < 0)
		return r;
	/* start of strings */
	max = sizeof(header) + (header.bytes[0] + header.bytes[1]) * count;
	/* write the length/offset table */
	for(i = 0; i < count; i++)
	{
		int j, bc = 0;
		uint8_t buffer[8];
		uint32_t value;
		size = strlen(strings[i]);
		value = size;
		bc += header.bytes[0];
		/* write big endian order */
		for(j = 0; j < header.bytes[0]; j++)
		{
			buffer[bc - j - 1] = value & 0xFF;
			value >>= 8;
		}
		value = max;
		bc += header.bytes[1];
		for(j = 0; j < header.bytes[1]; j++)
		{
			buffer[bc - j - 1] = value & 0xFF;
			value >>= 8;
		}
		max += size;
		r = fp->append(buffer, bc);
		if(r != bc)
			return (r < 0) ? r : -1;
	}
	/* write the strings */
	for(i = 0; i < count; i++)
	{
		size = strlen(strings[i]);
		r = fp->append(strings[i], size);
		if(r != (int) size)
			return (r < 0) ? r : -1;
	}
	return 0;
}

int stringtbl::create(rwfile * fp, blob * blobs, ssize_t count, const blob_comparator * blob_cmp, bool need_sort)
{
	st_header header = {STRINGTBL_VERSION, 1, {4, 1}, count};
	size_t size = 0, max = 0;
	int r;
	for(ssize_t i = 0; i < count; i++)
	{
		size_t length = blobs[i].size();
		size += length;
		if(length > max)
			max = length;
	}
	/* the blobs must be sorted */
	if(need_sort)
		array_sort(blobs, count, blob_cmp);
	/* figure out the correct size of the length field */
	if(max < 0x100)
		header.bytes[0] = 1;
	else if(max < 0x10000)
		header.bytes[0] = 2;
	else if(max < 0x1000000)
		header.bytes[0] = 3;
	/* figure out the correct size of the offset field */
	for(ssize_t i = 0; i < 3; i++)
	{
		max = sizeof(header) + (header.bytes[0] + header.bytes[1]) * count + size;
		if(max < 0x100)
			break;
		else if(max < 0x10000)
			header.bytes[1] = 2;
		else if(max < 0x1000000)
			header.bytes[1] = 3;
		else
			header.bytes[1] = 4;
	}
	/* write the header */
	r = fp->append(&header);
	if(r < 0)
		return r;
	/* start of blobs */
	max = sizeof(header) + (header.bytes[0] + header.bytes[1]) * count;
	/* write the length/offset table */
	for(ssize_t i = 0; i < count; i++)
	{
		int j, bc = 0;
		uint8_t buffer[8];
		uint32_t value;
		size = blobs[i].size();
		value = size;
		bc += header.bytes[0];
		/* write big endian order */
		for(j = 0; j < header.bytes[0]; j++)
		{
			buffer[bc - j - 1] = value & 0xFF;
			value >>= 8;
		}
		value = max;
		bc += header.bytes[1];
		for(j = 0; j < header.bytes[1]; j++)
		{
			buffer[bc - j - 1] = value & 0xFF;
			value >>= 8;
		}
		max += size;
		r = fp->append(buffer, bc);
		if(r != bc)
			return (r < 0) ? r : -1;
	}
	/* write the blobs */
	for(ssize_t i = 0; i < count; i++)
	{
		size = blobs[i].size();
		r = fp->append(&blobs[i][0], size);
		if(r != (int) size)
			return (r < 0) ? r : -1;
	}
	return 0;
}
