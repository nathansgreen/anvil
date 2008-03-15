/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>

#include "openat.h"
#include "stable.h"
#include "itable.h"

/* itable file format:
 * byte 0: k1 type (0 -> invalid, 1 -> int, 2 -> string)
 * byte 1: k2 type (same as k1 type)
 * byte 2-n: if k1 or k2 types are string, a string table
 * byte 2 or n+1: main data tables
 * 
 * main data tables:
 * bytes 0-3: k1 count
 * bytes 4-7: off_t base (can be extended to 8 bytes later)
 * byte 8: k1 size (1-4 bytes)
 * byte 9: k2 size (1-4 bytes)
 * byte 10: count size (1-4 bytes)
 * byte 11: offset (internal) size
 * byte 12: off_t (data) size
 * k1 array:
 * [] = byte 0-m: k2 count
 * [] = byte m+1-n: k1 value
 *      byte n+1-o: k2 array offset (relative to k1_offset)
 * each k2 array:
 * [] = byte 0-m: k2 value
 *      byte m+1-n: off_t */

/* TODO: use some sort of buffering here to avoid lots of small read()/write() calls */

struct itable_header {
	uint32_t k1_count;
	uint32_t off_base;
	uint8_t key_sizes[2];
	uint8_t count_size;
	uint8_t off_sizes[2];
} __attribute__((packed));

int itable_disk::init(int dfd, const char * file)
{
	int r = -1;
	uint8_t types[2];
	struct itable_header header;
	if(fd > -1)
		deinit();
	fd = openat(dfd, file, O_RDONLY);
	if(fd < 0)
		return fd;
	if(read(fd, types, 2) != 2)
		goto fail;
	if(!types[0] || !types[1])
		goto fail;
	k1t = (types[0] == 2) ? STRING : INT;
	k2t = (types[1] == 2) ? STRING : INT;
	k1_offset = 2;
	if(types[0] == 2 || types[1] == 2)
	{
		r = st_init(&st, fd, 2);
		if(r < 0)
			goto fail;
		k1_offset += st.size;
		r = lseek(fd, k1_offset, SEEK_SET);
		if(r < 0)
			goto fail_st;
	}
	r = read(fd, &header, sizeof(header));
	if(r != sizeof(header))
		goto fail_st;
	k1_offset += sizeof(header);
	k1_count = header.k1_count;
	off_base = header.off_base;
	key_sizes[0] = header.key_sizes[0];
	key_sizes[1] = header.key_sizes[1];
	count_size = header.count_size;
	off_sizes[0] = header.off_sizes[0];
	off_sizes[1] = header.off_sizes[1];
	entry_sizes[0] = count_size + key_sizes[0] + off_sizes[0];
	entry_sizes[1] = key_sizes[1] + off_sizes[1];
	
	/* sanity checks? */
	return 0;
	
fail_st:
	if(k1t == STRING || k2t == STRING)
		st_kill(&st);
fail:
	close(fd);
	fd = -1;
	return (r < 0) ? r : -1;
}

void itable_disk::deinit()
{
	if(fd < 0)
		return;
	if(k1t == STRING || k2t == STRING)
		st_kill(&st);
	close(fd);
	fd = -1;
}

int itable_disk::k1_get(size_t index, iv_int * value, size_t * k2_count, off_t * k2_offset)
{
	uint8_t bytes[12], bc = 0;
	int i, r;
	if(index >= k1_count)
		return -1;
	r = lseek(fd, k1_offset + entry_sizes[0] * index, SEEK_SET);
	if(r < 0)
		return r;
	r = read(fd, bytes, entry_sizes[0]);
	if(r != entry_sizes[0])
		return (r < 0) ? r : -1;
	*k2_count = 0;
	for(i = 0; i < count_size; i++)
		*k2_count = (*k2_count << 8) | bytes[bc++];
	*value = 0;
	for(i = 0; i < key_sizes[0]; i++)
		*value = (*value << 8) | bytes[bc++];
	*k2_offset = 0;
	for(i = 0; i < off_sizes[0]; i++)
		*k2_offset = (*k2_offset << 8) | bytes[bc++];
	return 0;
}

int itable_disk::k1_find(iv_int k1, size_t * k2_count, off_t * k2_offset)
{
	/* binary search */
	size_t min = 0, max = k1_count - 1;
	if(!k1_count)
		return -1;
	while(min <= max)
	{
		iv_int value;
		/* watch out for overflow! */
		size_t index = min + (max - min) / 2;
		if(k1_get(index, &value, k2_count, k2_offset) < 0)
			break;
		if(value < k1)
			min = index + 1;
		else if(value > k1)
			max = index - 1;
		else
			return 0;
	}
	return -1;
}

int itable_disk::k2_get(size_t k2_count, off_t k2_offset, size_t index, iv_int * value, off_t * offset)
{
	uint8_t bytes[8], bc = 0;
	int i, r;
	if(index >= k2_count)
		return -1;
	r = lseek(fd, k2_offset + entry_sizes[1] * index, SEEK_SET);
	if(r < 0)
		return r;
	r = read(fd, bytes, entry_sizes[1]);
	if(r != entry_sizes[1])
		return (r < 0) ? r : -1;
	*value = 0;
	for(i = 0; i < key_sizes[1]; i++)
		*value = (*value << 8) | bytes[bc++];
	*offset = 0;
	for(i = 0; i < off_sizes[1]; i++)
		*offset = (*offset << 8) | bytes[bc++];
	return 0;
}

int itable_disk::k2_find(size_t k2_count, off_t k2_offset, iv_int k2, off_t * offset)
{
	/* binary search */
	size_t min = 0, max = k2_count - 1;
	if(!k2_count)
		return -1;
	while(min <= max)
	{
		iv_int value;
		/* watch out for overflow! */
		size_t index = min + (max - min) / 2;
		if(k2_get(k2_count, k2_offset, index, &value, offset) < 0)
			break;
		if(value < k2)
			min = index + 1;
		else if(value > k2)
			max = index - 1;
		else
			return 0;
	}
	return -1;
}

bool itable_disk::has(iv_int k1)
{
	size_t k2_count;
	off_t k2_offset;
	return 0 <= k1_find(k1, &k2_count, &k2_offset);
}

bool itable_disk::has(const char * k1)
{
	ssize_t k1i;
	if(k1t != STRING)
		return false;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return false;
	return has((iv_int) k1i);
}

bool itable_disk::has(iv_int k1, iv_int k2)
{
	size_t k2_count;
	off_t k2_offset, offset;
	int r = k1_find(k1, &k2_count, &k2_offset);
	if(r < 0)
		return false;
	return 0 <= k2_find(k2_count, k1_offset + k2_offset, k2, &offset);
}

bool itable_disk::has(iv_int k1, const char * k2)
{
	ssize_t k2i;
	if(k2t != STRING)
		return false;
	k2i = st_locate(&st, k2);
	if(k2i < 0)
		return false;
	return has(k1, (iv_int) k2i);
}

bool itable_disk::has(const char * k1, iv_int k2)
{
	ssize_t k1i;
	if(k1t != STRING)
		return false;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return false;
	return has((iv_int) k1i, k2);
}

bool itable_disk::has(const char * k1, const char * k2)
{
	ssize_t k1i, k2i;
	if(k1t != STRING || k2t != STRING)
		return false;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return false;
	k2i = st_locate(&st, k2);
	if(k2i < 0)
		return false;
	return has((iv_int) k1i, (iv_int) k2i);
}

off_t itable_disk::get(iv_int k1, iv_int k2)
{
	size_t k2_count;
	off_t k2_offset, offset;
	int r = k1_find(k1, &k2_count, &k2_offset);
	if(r < 0)
		return INVAL_OFF_T;
	r = k2_find(k2_count, k1_offset + k2_offset, k2, &offset);
	if(r < 0)
		return INVAL_OFF_T;
	return offset;
}

off_t itable_disk::get(iv_int k1, const char * k2)
{
	ssize_t k2i;
	if(k2t != STRING)
		return INVAL_OFF_T;
	k2i = st_locate(&st, k2);
	if(k2i < 0)
		return INVAL_OFF_T;
	return get(k1, (iv_int) k2i);
}

off_t itable_disk::get(const char * k1, iv_int k2)
{
	ssize_t k1i;
	if(k1t != STRING)
		return INVAL_OFF_T;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return INVAL_OFF_T;
	return get((iv_int) k1i, k2);
}

off_t itable_disk::get(const char * k1, const char * k2)
{
	ssize_t k1i, k2i;
	if(k1t != STRING || k2t != STRING)
		return INVAL_OFF_T;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return INVAL_OFF_T;
	k2i = st_locate(&st, k2);
	if(k2i < 0)
		return INVAL_OFF_T;
	return get((iv_int) k1i, (iv_int) k2i);
}

int itable_disk::next(iv_int k1, iv_int * key)
{
}
int itable_disk::next(const char * k1, const char ** key)
{
}

int itable_disk::next(iv_int k1, iv_int k2, iv_int * key)
{
}
int itable_disk::next(iv_int k1, const char * k2, const char ** key)
{
}
int itable_disk::next(const char * k1, iv_int k2, iv_int * key)
{
}
int itable_disk::next(const char * k1, const char * k2, const char ** key)
{
}

int itable_disk::create(int dfd, const char * file, itable * source)
{
}
