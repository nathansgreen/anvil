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
#include "tempfile.h"
#include "itable.h"

/* These methods are part of the itable_disk class, but are here because they
 * are substantially different than the rest of the class (being for writing
 * itable_disk files instead of reading them). */

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
	tempfile k2_counts;
	stringset stringset;
	const char ** string_array = NULL;
	off_t min_off = 0, max_off = 0, off;
	size_t k1_count = 0, k2_count = 0, k2_count_max = 0;
	size_t k2_total = 0, max_strlen = 0, strings = 0;
	union { iv_int i; const char * s; } k1, old_k1, k2;
	iv_int k1_max = 0, k2_max = 0;
	int r = source->iter(&iter);
	if(r < 0)
		return r;
	r = k2_counts.create();
	if(r < 0)
		return r;
	if(source->k1_type() == STRING || source->k2_type() == STRING)
	{
		r = stringset.init();
		if(r < 0)
			return r;
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
			size_t length = strlen(k1.s);
			if(length > max_strlen)
				max_strlen = length;
			k1.s = stringset.add(k1.s);
			if(!k1.s)
				break;
		}
		else if(k1.i > k1_max)
			k1_max = k1.i;
		if(source->k2_type() == STRING)
		{
			size_t length = strlen(k2.s);
			if(length > max_strlen)
				max_strlen = length;
			k2.s = stringset.add(k2.s);
			if(!k2.s)
				break;
		}
		else if(k2.i > k2_max)
			k2_max = k2.i;
		if(!k1_count)
			min_off = off;
		/* we can compare strings by pointer because the stringset makes them unique */
		if(!k1_count || ((source->k1_type() == STRING) ? (k1.s != old_k1.s) : (k1.i != old_k1.i)))
		{
			if(k2_count > k2_count_max)
				k2_count_max = k2_count;
			if(k2_count)
			{
				r = k2_counts.append(k2_count);
				if(r < 0)
					return r;
			}
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
		return (r < 0) ? r : -1;
	if(k2_count > k2_count_max)
		k2_count_max = k2_count;
	if(k2_count)
	{
		r = k2_counts.append(k2_count);
		if(r < 0)
			return r;
	}
	assert(k1_count == k2_counts.count());
	/* now we have k1_count, k2_count_max, k2_total, min_off, and max_off,
	 * and, if appropriate, k1_max, k2_max, stringset, and max_strlen */
	if(stringset.ready())
	{
		strings = stringset.size();
		string_array = stringset.array();
		if(!string_array)
			return -ENOMEM;
	}
	
	/* now write the file */
	int bc;
	tx_fd fd;
	off_t out_off;
	uint8_t bytes[12];
	struct file_header file_hdr;
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
	file_hdr.magic = ITABLE_MAGIC;
	file_hdr.version = ITABLE_VERSION;
	file_hdr.types[0] = (source->k1_type() == STRING) ? 2 : 1;
	file_hdr.types[1] = (source->k2_type() == STRING) ? 2 : 1;
	
	fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
	if(fd < 0)
	{
		r = fd;
		goto out_strings;
	}
	r = tx_write(fd, &file_hdr, 0, sizeof(file_hdr));
	if(r < 0)
	{
	fail_unlink:
		tx_close(fd);
		tx_unlink(dfd, file);
		goto out_strings;
	}
	out_off = sizeof(file_hdr);
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
			r = source->next(&iter, &k1.s);
		else
			r = source->next(&iter, &k1.i);
		if(r)
			break;
		bc = 0;
		r = k2_counts.read(&value);
		if(r < 0)
		{
			if(r == -ENOENT)
				r = -1;
			break;
		}
		k2_count = value;
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

int itable_disk::create_with_datastore(int i_dfd, const char * i_file, itable * source, itable_datamap * map)
{
	return -ENOSYS;
}
