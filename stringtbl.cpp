/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string.h>

#include "stringtbl.h"
#include "transaction.h"

/* TODO: use some sort of buffering here to avoid lots of small read()/write() calls */

struct st_header {
	ssize_t count;
	uint8_t bytes[2];
} __attribute__((packed));

int stringtbl::init(int fd, off_t start)
{
	int r;
	st_header header;
	if(this->fd >= 0)
		deinit();
	r = lseek(fd, start, SEEK_SET);
	if(r < 0)
		return r;
	r = ::read(fd, &header, sizeof(header));
	if(r != sizeof(header))
		return (r < 0) ? r : -1;
	this->fd = fd;
	this->start = start;
	count = header.count;
	bytes[0] = header.bytes[0];
	bytes[1] = header.bytes[1];
	bytes[2] = bytes[0] + bytes[1];
	/* calculate size */
	size = sizeof(header) + bytes[2] * count;
	for(ssize_t i = 0; i < count; i++)
	{
		uint32_t value = 0;
		uint8_t buffer[8];
		r = ::read(fd, buffer, bytes[2]);
		if(r != bytes[2])
			return (r < 0) ? r : -1;
		/* read big endian order */
		for(r = 0; r < bytes[0]; r++)
			value = (value << 8) | buffer[r];
		size += value;
	}
	for(ssize_t i = 0; i < ST_LRU; i++)
	{
		lru[i].index = -1;
		lru[i].string = NULL;
	}
	lru_next = 0;
	return 0;
}

void stringtbl::deinit()
{
	if(fd >= 0)
	{
		for(ssize_t i = 0; i < ST_LRU; i++)
			if(lru[i].string)
				free((void *) lru[i].string);
		fd = -1;
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
	i = lseek(fd, offset, SEEK_SET);
	if(i < 0)
		return NULL;
	i = ::read(fd, buffer, bytes[2]);
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
	i = lseek(fd, offset, SEEK_SET);
	if(i < 0)
		return NULL;
	string = (char *) malloc(length + 1);
	if(!string)
		return NULL;
	i = ::read(fd, string, length);
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
	lru[i].string = string;
	return string;
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

const char ** stringtbl::read() const
{
	ssize_t i;
	const char ** u = (const char **) malloc(sizeof(*u) * count);
	if(!u)
		return NULL;
	for(i = 0; i < count; i++)
	{
		const char * string = get(i);
		if(!string)
			break;
		/* TODO: suck out of lru array instead of strdup */
		u[i] = strdup(string);
		if(!u[i])
			break;
	}
	if(i < count)
	{
		while(i > 0)
			free((void *) u[--i]);
		free(u);
		return NULL;
	}
	return u;
}

static int st_strcmp(const void * a, const void * b)
{
	return strcmp(*(const char **) a, *(const char **) b);
}

void stringtbl::array_sort(const char ** array, ssize_t count)
{
	qsort(array, count, sizeof(*array), st_strcmp);
}

void stringtbl::array_free(const char ** array, ssize_t count)
{
	ssize_t i;
	for(i = 0; i < count; i++)
		free((void *) array[i]);
	free(array);
}

int stringtbl::create(int fd, off_t * start, const char ** strings, ssize_t count)
{
	struct st_header header = {count, {4, 1}};
	size_t size = 0, max = 0;
	ssize_t i;
	int r;
	/* the strings must be sorted */
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
	r = pwrite(fd, &header, sizeof(header), *start);
	if(r != sizeof(header))
		return (r < 0) ? r : -1;
	*start += sizeof(header);
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
		r = pwrite(fd, buffer, bc, *start);
		if(r != bc)
			return (r < 0) ? r : -1;
		*start += bc;
	}
	/* write the strings */
	for(i = 0; i < count; i++)
	{
		size = strlen(strings[i]);
		r = pwrite(fd, strings[i], size, *start);
		if(r < 0)
			return r;
		*start += size;
	}
	return 0;
}

int stringtbl::combine(int fd, off_t * start, const stringtbl * st1, const stringtbl * st2)
{
	ssize_t i1 = 0, i2 = 0;
	ssize_t total = 0;
	const char ** s1;
	const char ** s2;
	const char ** u;
	int r;
	/* read the source tables */
	s1 = st1->read();
	if(!s1)
		return -1;
	s2 = st2->read();
	if(!s2)
	{
		array_free(s1, st1->count);
		return -1;
	}
	/* count the total number of strings */
	while(i1 < st1->count && i2 < st2->count)
	{
		int c = strcmp(s1[i1], s2[i2]);
		if(c <= 0)
			i1++;
		if(c >= 0)
			i2++;
		total++;
	}
	total += (st1->count - i1) + (st2->count - i2);
	u = (const char **) malloc(sizeof(*u) * total);
	if(!u)
	{
		array_free(s2, st2->count);
		array_free(s1, st1->count);
	}
	/* merge the arrays */
	i1 = 0;
	i2 = 0;
	total = 0;
	while(i1 < st1->count && i2 < st2->count)
	{
		int c = strcmp(s1[i1], s2[i2]);
		if(c <= 0)
			u[total] = s1[i1++];
		else if(c > 0)
			u[total] = s2[i2++];
		if(!c)
			free((void *) s2[i2++]);
		total++;
	}
	while(i1 < st1->count)
		u[total++] = s1[i1++];
	while(i2 < st2->count)
		u[total++] = s2[i2++];
	free(s2);
	free(s1);
	/* create the combined string table */
	r = create(fd, start, u, total);
	array_free(u, total);
	return r;
}
