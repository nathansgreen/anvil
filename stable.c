/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <string.h>
#include <unistd.h>

#include "stable.h"
#include "transaction.h"

/* TODO: use some sort of buffering here to avoid lots of small read()/write() calls */

struct st_header {
	ssize_t count;
	uint8_t bytes[2];
};

int st_init(struct stable * st, int fd, off_t start)
{
	struct st_header header;
	ssize_t i;
	int r = lseek(fd, start, SEEK_SET);
	if(r < 0)
		return r;
	r = read(fd, &header, sizeof(header));
	if(r != sizeof(header))
		return (r < 0) ? r : -1;
	st->fd = fd;
	st->start = start;
	st->count = header.count;
	st->bytes[0] = header.bytes[0];
	st->bytes[1] = header.bytes[1];
	st->bytes[2] = st->bytes[0] + st->bytes[1];
	/* calculate st->size */
	st->size = sizeof(header) + st->bytes[2] * st->count;
	for(i = 0; i < st->count; i++)
	{
		uint32_t value = 0;
		uint8_t bytes[8];
		r = read(fd, bytes, st->bytes[2]);
		if(r != st->bytes[2])
			return (r < 0) ? r : -1;
		st->size += 0;
		for(r = 0; r < st->bytes[0]; r++)
			value = (value << 8) | bytes[r];
		st->size += value;
	}
	for(i = 0; i < ST_LRU; i++)
	{
		st->lru[i].index = -1;
		st->lru[i].string = NULL;
	}
	st->lru_next = 0;
	return 0;
}

int st_kill(struct stable * st)
{
	ssize_t i;
	for(i = 0; i < ST_LRU; i++)
		if(st->lru[i].string)
			free(st->lru[i].string);
	return 0;
}

const char * st_get(struct stable * st, ssize_t index)
{
	int i, bc = 0;
	off_t offset;
	size_t length = 0;
	uint8_t bytes[8];
	char * string;
	if(index < 0 || index >= st->count)
		return NULL;
	for(i = 0; i < ST_LRU; i++)
		if(st->lru[i].index == index)
			return st->lru[i].string;
	/* not in LRU */
	offset = st->start + sizeof(struct st_header) + index * st->bytes[2];
	i = lseek(st->fd, offset, SEEK_SET);
	if(i < 0)
		return NULL;
	i = read(st->fd, bytes, st->bytes[2]);
	if(i != st->bytes[2])
		return NULL;
	for(i = 0; i < st->bytes[0]; i++)
		length = (length << 8) | bytes[bc++];
	offset = 0;
	for(i = 0; i < st->bytes[1]; i++)
		offset = (offset << 8) | bytes[bc++];
	offset += st->start;
	/* now we have the length and offset */
	i = lseek(st->fd, offset, SEEK_SET);
	if(i < 0)
		return NULL;
	string = malloc(length + 1);
	if(!string)
		return NULL;
	i = read(st->fd, string, length);
	if(i != length)
	{
		free(string);
		return NULL;
	}
	string[length] = 0;
	i = st->lru_next;
	st->lru[i].index = index;
	if(st->lru[i].string)
		free(st->lru[i].string);
	st->lru[i].string = string;
	return string;
}

ssize_t st_locate(struct stable * st, const char * string)
{
	/* binary search */
	ssize_t min = 0, max = st->count - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		const char * value = st_get(st, index);
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

const char ** st_read(struct stable * st)
{
	ssize_t i;
	const char ** u = malloc(sizeof(*u) * st->count);
	if(!u)
		return NULL;
	for(i = 0; i < st->count; i++)
	{
		const char * string = st_get(st, i);
		if(!string)
			break;
		/* TODO: suck out of lru array instead of strdup */
		u[i] = strdup(string);
		if(!u[i])
			break;
	}
	if(i < st->count)
	{
		while(i > 0)
			free(u[--i]);
		free(u);
		return NULL;
	}
	return u;
}

void st_array_free(const char ** array, ssize_t count)
{
	ssize_t i;
	for(i = 0; i < count; i++)
		free(array[i]);
	free(array);
}

int st_create(tx_fd fd, off_t start, const char ** strings, ssize_t count)
{
	struct st_header header = {count, {4, 1}};
	size_t size = 0, max = 0;
	ssize_t i;
	int r;
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
	r = tx_write(fd, &header, start, sizeof(header));
	if(r < 0)
		return r;
	start += sizeof(header);
	/* start of strings */
	max = sizeof(header) + (header.bytes[0] + header.bytes[1]) * count;
	/* write the length/offset table */
	for(i = 0; i < count; i++)
	{
		int j, bc = 0;
		uint8_t bytes[8];
		uint32_t value;
		size = strlen(strings[i]);
		value = size;
		for(j = 0; j < header.bytes[0]; j++)
		{
			bytes[bc++] = value & 0xFF;
			value >>= 8;
		}
		value = max;
		for(j = 0; j < header.bytes[1]; j++)
		{
			bytes[bc++] = value & 0xFF;
			value >>= 8;
		}
		max += size;
		r = tx_write(fd, bytes, start, bc);
		if(r < 0)
			return r;
		start += bc;
	}
	/* write the strings */
	for(i = 0; i < count; i++)
	{
		size = strlen(strings[i]);
		r = tx_write(fd, strings[i], start, size);
		if(r < 0)
			return r;
		start += size;
	}
	return 0;
}

int st_combine(tx_fd fd, off_t start, struct stable * st1, struct stable * st2)
{
	ssize_t i1 = 0, i2 = 0;
	ssize_t total = 0;
	const char ** s1;
	const char ** s2;
	const char ** u;
	int r;
	/* read the source tables */
	s1 = st_read(st1);
	if(!s1)
		return -1;
	s2 = st_read(st2);
	if(!s2)
	{
		st_array_free(s1, st1->count);
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
	u = malloc(sizeof(*u) * total);
	if(!u)
	{
		st_array_free(s2, st2->count);
		st_array_free(s1, st1->count);
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
			free(s2[i2++]);
		total++;
	}
	while(i1 < st1->count)
		u[total++] = s1[i1++];
	while(i2 < st2->count)
		u[total++] = s2[i2++];
	free(s2);
	free(s1);
	/* create the combined string table */
	r = st_create(fd, start, u, total);
	st_array_free(u, total);
	return r;
}
