/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include "openat.h"
#include "stable.h"
#include "hash_map.h"
#include "transaction.h"
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
	/* read big endian order */
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

int itable_disk::k1_find(iv_int k1, size_t * k2_count, off_t * k2_offset, size_t * index)
{
	/* binary search */
	size_t min = 0, max = k1_count - 1;
	if(!k1_count)
		return -1;
	while(min <= max)
	{
		iv_int value;
		/* watch out for overflow! */
		size_t mid = min + (max - min) / 2;
		if(k1_get(mid, &value, k2_count, k2_offset) < 0)
			break;
		if(value < k1)
			min = mid + 1;
		else if(value > k1)
			max = mid - 1;
		else
		{
			if(index)
				*index = mid;
			return 0;
		}
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
	/* read big endian order */
	*value = 0;
	for(i = 0; i < key_sizes[1]; i++)
		*value = (*value << 8) | bytes[bc++];
	*offset = 0;
	for(i = 0; i < off_sizes[1]; i++)
		*offset = (*offset << 8) | bytes[bc++];
	return 0;
}

int itable_disk::k2_find(size_t k2_count, off_t k2_offset, iv_int k2, off_t * offset, size_t * index)
{
	/* binary search */
	size_t min = 0, max = k2_count - 1;
	if(!k2_count)
		return -1;
	while(min <= max)
	{
		iv_int value;
		/* watch out for overflow! */
		size_t mid = min + (max - min) / 2;
		if(k2_get(k2_count, k2_offset, mid, &value, offset) < 0)
			break;
		if(value < k2)
			min = mid + 1;
		else if(value > k2)
			max = mid - 1;
		else
		{
			if(index)
				*index = mid;
			return 0;
		}
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
	return off_base + offset;
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

int itable_disk::iter(struct it * it)
{
	int r;
	it->clear();
	r = k1_get(0, &it->k1, &it->k2_count, &it->k2_offset);
	if(r < 0)
		return r;
	it->k1i = 0;
	it->k2i = 0;
	it->k2_offset += k1_offset;
	return 0;
}

int itable_disk::iter(struct it * it, iv_int k1)
{
	int r;
	it->clear();
	r = k1_find(k1, &it->k2_count, &it->k2_offset, &it->k1i);
	if(r < 0)
		return r;
	it->k2i = 0;
	it->k2_offset += k1_offset;
	return 0;
}

int itable_disk::iter(struct it * it, const char * k1)
{
	ssize_t k1i;
	if(k1t != STRING)
		return -1;
	k1i = st_locate(&st, k1);
	if(k1i < 0)
		return -1;
	return iter(it, (iv_int) k1i);
}

void itable::kill_iter(struct it * it)
{
	it->table = NULL;
}

int itable_disk::next(struct it * it, iv_int * k1, iv_int * k2, off_t * off)
{
	int r;
	if(it->k1i >= k1_count)
		return -ENOENT;
	for(;;)
	{
		/* same primary key */
		if(it->k2i < it->k2_count)
		{
			*k1 = it->k1;
			r = k2_get(it->k2_count, it->k2_offset, it->k2i++, k2, off);
			if(r < 0)
				return r;
			*off += off_base;
			return 0;
		}
		if(++it->k1i >= k1_count)
			return -ENOENT;
		it->k2i = 0;
		r = k1_get(it->k1i, &it->k1, &it->k2_count, &it->k2_offset);
		if(r < 0)
			return r;
		it->k2_offset += k1_offset;
	}
	/* never get here */
}

int itable_disk::next(struct it * it, iv_int * k1, const char ** k2, off_t * off)
{
	int r;
	iv_int k2i;
	if(k2t != STRING)
		return -1;
	r = next(it, k1, &k2i, off);
	if(r < 0)
		return r;
	*k2 = st_get(&st, k2i);
	if(!*k2)
		return -1;
	return 0;
}

int itable_disk::next(struct it * it, const char ** k1, iv_int * k2, off_t * off)
{
	int r;
	iv_int k1i;
	if(k1t != STRING)
		return -1;
	r = next(it, &k1i, k2, off);
	if(r < 0)
		return r;
	*k1 = st_get(&st, k1i);
	if(!*k1)
		return -1;
	return 0;
}

#if ST_LRU < 2
#error ST_LRU must be at least 2
#endif
int itable_disk::next(struct it * it, const char ** k1, const char ** k2, off_t * off)
{
	int r;
	iv_int k1i, k2i;
	if(k1t != STRING || k2t != STRING)
		return -1;
	r = next(it, &k1i, &k2i, off);
	if(r < 0)
		return r;
	*k1 = st_get(&st, k1i);
	if(!*k1)
		return -1;
	*k2 = st_get(&st, k2i);
	if(!*k2)
		return -1;
	return 0;
}

int itable_disk::next(struct it * it, iv_int * k1, size_t * k2_count)
{
	int r;
	if(it->k1i >= k1_count)
		return -ENOENT;
	if(it->k1i)
	{
		off_t k2_offset;
		r = k1_get(it->k1i, &it->k1, k2_count, &k2_offset);
		if(r < 0)
			return r;
	}
	else
		*k2_count = it->k2_count;
	*k1 = it->k1;
	it->k1i++;
	return 0;
}

int itable_disk::next(struct it * it, const char ** k1, size_t * k2_count)
{
	int r;
	iv_int k1i;
	if(k1t != STRING)
		return -1;
	r = next(it, &k1i, k2_count);
	if(r < 0)
		return r;
	*k1 = st_get(&st, k1i);
	if(!*k1)
		return -1;
	return 0;
}

/* get a unique pointer for the string by putting it in a string hash map */
int itable_disk::add_string(const char ** string, hash_map_t * string_map, size_t * max_strlen)
{
	char * copy;
	size_t length = strlen(*string);
	if(length > *max_strlen)
		*max_strlen = length;
	copy = (char *) hash_map_find_val(string_map, *string);
	if(!copy)
	{
		int r;
		copy = strdup(*string);
		if(!copy)
			return -ENOMEM;
		r = hash_map_insert(string_map, copy, copy);
		if(r < 0)
		{
			free(copy);
			return r;
		}
	}
	*string = copy;
	return 0;
}

ssize_t itable_disk::locate_string(const char ** array, ssize_t size, const char * string)
{
	/* binary search */
	ssize_t min = 0, max = size - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		c = strcmp(array[index], string);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}

int itable_disk::create(int dfd, const char * file, itable * source)
{
	struct it iter;
	hash_map_t * string_map = NULL;
	const char ** string_array = NULL;
	off_t min_off = 0, max_off = 0, off;
	size_t k1_count = 0, k2_count = 0, k2_count_max = 0;
	size_t k2_total = 0, max_strlen = 0, strings = 0;
	union { iv_int i; const char * s; } k1, old_k1, k2;
	iv_int k1_max = 0, k2_max = 0;
	int r = source->iter(&iter);
	if(r < 0)
		return r;
	if(source->k1_type() == STRING || source->k2_type() == STRING)
	{
		string_map = hash_map_create_str();
		if(!string_map)
			return -ENOMEM;
	}
	for(;;)
	{
		if(source->k1_type() == STRING)
		{
			if(source->k2_type() == STRING)
				r = source->next(&iter, &k1.s, &k2.s, &off);
			else
				r = source->next(&iter, &k1.s, &k2.i, &off);
		}
		else
		{
			if(source->k2_type() == STRING)
				r = source->next(&iter, &k1.i, &k2.s, &off);
			else
				r = source->next(&iter, &k1.i, &k2.i, &off);
		}
		if(r)
			break;
		if(source->k1_type() == STRING)
		{
			if((r = add_string(&k1.s, string_map, &max_strlen)) < 0)
				break;
		}
		else if(k1.i > k1_max)
			k1_max = k1.i;
		if(source->k2_type() == STRING)
		{
			if((r = add_string(&k2.s, string_map, &max_strlen)) < 0)
				break;
		}
		else if(k2.i > k2_max)
			k2_max = k2.i;
		if(!k1_count)
			min_off = off;
		/* we can compare strings by pointer because add_string() makes them unique */
		if(!k1_count || ((source->k1_type() == STRING) ? (k1.s != old_k1.s) : (k1.i != old_k1.i)))
		{
			if(k2_count > k2_count_max)
				k2_count_max = k2_count;
			k2_count = 0;
			if(source->k1_type() == STRING)
				old_k1.s = k1.s;
			else
				old_k1.i = k1.i;
			k1_count++;
		}
		if(off < min_off)
			min_off = off;
		if(off > max_off)
			max_off = off;
		k2_count++;
		k2_total++;
	}
	if(r != -ENOENT)
	{
		if(string_map)
		{
			hash_map_it2_t hm_it;
		fail_strings:
			hm_it = hash_map_it2_create(string_map);
			while(hash_map_it2_next(&hm_it))
				free((void *) hm_it.val);
			hash_map_destroy(string_map);
		}
		return (r < 0) ? r : -1;
	}
	/* now we have k1_count, k2_count_max, k2_total, min_off, and max_off,
	 * and, if appropriate, k1_max, k2_max, string_map, and max_strlen */
	if(string_map)
	{
		size_t i = 0;
		hash_map_it2_t hm_it;
		strings = hash_map_size(string_map);
		string_array = (const char **) malloc(sizeof(*string_array) * strings);
		if(!string_array)
		{
			r = -ENOMEM;
			goto fail_strings;
		}
		hm_it = hash_map_it2_create(string_map);
		while(hash_map_it2_next(&hm_it))
			string_array[i++] = (const char *) hm_it.val;
		assert(i == strings);
		hash_map_destroy(string_map);
		string_map = NULL;
	}
	
	/* now write the file */
	int bc;
	tx_fd fd;
	off_t out_off;
	uint8_t bytes[12];
	struct itable_header header;
	header.k1_count = k1_count;
	header.off_base = min_off;
	if(source->k1_type() == STRING)
		header.key_sizes[0] = byte_size(strings - 1);
	else
		header.key_sizes[0] = byte_size(k1_max);
	if(source->k2_type() == STRING)
		header.key_sizes[1] = byte_size(strings - 1);
	else
		header.key_sizes[1] = byte_size(k2_max);
	header.count_size = byte_size(k2_count_max);
	header.off_sizes[0] = 1; /* will be corrected later */
	header.off_sizes[1] = byte_size(max_off -= min_off);
	bytes[0] = (source->k1_type() == STRING) ? 2 : 1;
	bytes[1] = (source->k2_type() == STRING) ? 2 : 1;
	
	fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
	if(fd < 0)
	{
		r = fd;
		goto out_strings;
	}
	r = tx_write(fd, bytes, 0, 2);
	if(r < 0)
	{
	fail_unlink:
		tx_close(fd);
		tx_unlink(dfd, file);
		goto out_strings;
	}
	out_off = 2;
	if(string_array)
	{
		r = st_create(fd, &out_off, string_array, strings);
		if(r < 0)
			goto fail_unlink;
	}
	r = tx_write(fd, &header, out_off, sizeof(header));
	if(r < 0)
		goto fail_unlink;
	out_off += sizeof(header);
	off = (header.count_size + header.key_sizes[0] + header.off_sizes[0]) * k1_count;
	/* minimize header.off_sizes[0] */
	for(bc = 0; bc < 3; bc++)
	{
		header.off_sizes[0] = byte_size(off + (header.key_sizes[1] + header.off_sizes[1]) * k2_total);
		off = (header.count_size + header.key_sizes[0] + header.off_sizes[0]) * k1_count;
	}
	/* now the k1 array */
	source->iter(&iter);
	for(;;)
	{
		uint32_t value;
		if(source->k1_type() == STRING)
			r = source->next(&iter, &k1.s, &k2_count);
		else
			r = source->next(&iter, &k1.i, &k2_count);
		if(r)
			break;
		bc = 0;
		value = k2_count;
		layout_bytes(bytes, &bc, value, header.count_size);
		if(source->k1_type() == STRING)
			value = locate_string(string_array, strings, k1.s);
		else
			value = k1.i;
		layout_bytes(bytes, &bc, value, header.key_sizes[0]);
		value = off;
		off += (header.key_sizes[1] + header.off_sizes[1]) * k2_count;
		layout_bytes(bytes, &bc, value, header.off_sizes[0]);
		r = tx_write(fd, bytes, out_off, bc);
		if(r < 0)
			goto fail_unlink;
		out_off += bc;
	}
	if(r != -ENOENT)
		goto fail_unlink;
	/* and the k2 arrays */
	source->iter(&iter);
	for(;;)
	{
		uint32_t value;
		if(source->k1_type() == STRING)
		{
			if(source->k2_type() == STRING)
				r = source->next(&iter, &k1.s, &k2.s, &off);
			else
				r = source->next(&iter, &k1.s, &k2.i, &off);
		}
		else
		{
			if(source->k2_type() == STRING)
				r = source->next(&iter, &k1.i, &k2.s, &off);
			else
				r = source->next(&iter, &k1.i, &k2.i, &off);
		}
		if(r)
			break;
		bc = 0;
		if(source->k2_type() == STRING)
			value = locate_string(string_array, strings, k2.s);
		else
			value = k2.i;
		layout_bytes(bytes, &bc, value, header.key_sizes[1]);
		value = off - min_off;
		layout_bytes(bytes, &bc, value, header.off_sizes[1]);
		r = tx_write(fd, bytes, out_off, bc);
		if(r < 0)
			goto fail_unlink;
		out_off += bc;
	}
	if(r != -ENOENT)
		goto fail_unlink;
	/* assume tx_close() works */
	tx_close(fd);
	r = 0;
	
out_strings:
	if(string_array)
		st_array_free(string_array, strings);
	return r;
}
