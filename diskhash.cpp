/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "openat.h"
#include "multimap.h"
#include "diskhash.h"

/* Disk hashes are basically "hash maps" implemented with the file system. No
 * serious attempt to cache is made here; a separate caching module should be
 * used on top of this interface. Currently we just construct a 32-bit hash
 * value from the key, and store all the values in a nested directory tree named
 * by the hexadecimal bytes of the hash value. */

/* iterators */

int diskhash_it::next()
{
}

size_t diskhash_it::size()
{
}

diskhash_it::~diskhash_it()
{
}

/* disk hashes */

diskhash::~diskhash()
{
	close(dh_fd);
	close(dir_fd);
}

size_t diskhash::keys()
{
	return key_count;
}

size_t diskhash::values()
{
	return value_count;
}

ssize_t diskhash::count_values(mm_val_t * key)
{
}

diskhash_it * diskhash::get_values(mm_val_t * key)
{
}

ssize_t diskhash::count_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

diskhash_it * diskhash::get_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

diskhash_it * diskhash::iterator()
{
}

int diskhash::remove_key(mm_val_t * key)
{
	int r, bucket = bucket_fd(key);
	char _key_name[22];
	const char * key_name = _key_name;
	if(bucket < 0)
		return (errno == ENOENT) ? 0 : bucket;
	switch(key_type)
	{
		case MM_U32:
			snprintf(_key_name, sizeof(_key_name), "%u", key->u32);
			break;
		case MM_U64:
			snprintf(_key_name, sizeof(_key_name), "%llu", key->u64);
			break;
		case MM_STR:
			key_name = (const char *) key;
			break;
		default:
			close(bucket);
			return -EINVAL;
	}
	r = drop(bucket, key_name);
	if(r < 0 && errno == ENOENT)
		r = 0;
	close(bucket);
	return r;
}

int diskhash::reset_key(mm_val_t * key, mm_val_t * value)
{
	remove_key(key);
	return append_value(key, value);
}

int diskhash::append_value(mm_val_t * key, mm_val_t * value)
{
	int key_dir = key_fd(key, true);
	/* XXX */
	close(key_dir);
	return 0;
}

int diskhash::remove_value(mm_val_t * key, mm_val_t * value)
{
	/* XXX */
}

int diskhash::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
	int r = remove_value(key, old_value);
	if(r < 0)
		return r;
	return append_value(key, new_value);
}

/* create a new diskhash (on disk) using the specified store path */
int diskhash::init(int dfd, const char * store, mm_type_t key_type, mm_type_t val_type)
{
	int dir_fd, fd, r;
	mm_type_t types[2];
	size_t zero[2] = {0, 0};
	if(key_type != MM_U32 && key_type != MM_U64 && key_type != MM_STR)
		return -EINVAL;
	r = mkdirat(dfd, store, 0775);
	if(r < 0)
		return r;
	dir_fd = openat(dfd, store, 0);
	if(dir_fd < 0)
	{
		r = dir_fd;
		goto fail_dir;
	}
	fd = openat(dir_fd, "dh", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
	{
		r = fd;
		goto fail_open;
	}
	types[DH_KT_IDX] = key_type;
	types[DH_VT_IDX] = val_type;
	r = write(fd, types, sizeof(types));
	if(r != sizeof(types))
		goto fail_write;
	r = write(fd, zero, sizeof(zero));
	if(r != sizeof(zero))
		goto fail_write;
	close(fd);
	
	close(dir_fd);
	return 0;
	
fail_write:
	close(fd);
	unlinkat(dir_fd, "dh", 0);
fail_open:
	close(dir_fd);
fail_dir:
	unlinkat(dfd, store, AT_REMOVEDIR);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

/* open a diskhash on disk, or return NULL on error */
diskhash * diskhash::open(uint8_t * id, int dfd, const char * store)
{
	int r, dh_fd, dir_fd = openat(dfd, store, 0);
	mm_type_t types[2];
	size_t counts[2];
	diskhash * dh = NULL;
	if(dir_fd < 0)
		return NULL;
	dh_fd = openat(dir_fd, "dh", O_RDWR);
	if(dh_fd < 0)
		goto close_dir;
	r = read(dh_fd, types, sizeof(types));
	if(r != sizeof(types))
		goto close_dh;
	r = read(dh_fd, counts, sizeof(counts));
	if(r != sizeof(counts))
		goto close_dh;
	dh = new diskhash(id, types[DH_KT_IDX], types[DH_VT_IDX], dir_fd, dh_fd, counts[DH_KC_IDX], counts[DH_VC_IDX]);
	if(!dh)
	{
	close_dh:
		close(dh_fd);
	close_dir:
		close(dir_fd);
	}
	return dh;
}

diskhash::diskhash(uint8_t * id, mm_type_t kt, mm_type_t vt, int dir, int dh, size_t keys, size_t values)
	: multimap(id), dir_fd(dir), dh_fd(dh), key_count(keys), value_count(values)
{
	key_type = kt;
	val_type = vt;
}

int diskhash::bucket_fd(mm_val_t * key, bool create)
{
	union {
		uint32_t u32;
		uint8_t bytes[4];
	} hash;
	char bucket[12];
	int fd;
	hash.u32 = hash_key(key);
	snprintf(bucket, sizeof(bucket), "%02x/%02x/%02x/%02x", hash.bytes[0], hash.bytes[1], hash.bytes[2], hash.bytes[3]);
	fd = openat(dir_fd, bucket, 0);
	if(fd < 0 && errno == ENOENT && create)
	{
		int i;
		fd = dir_fd;
		for(i = 0; i != 4; i++)
		{
			int sub;
			snprintf(bucket, sizeof(bucket), "%02x", hash.bytes[i]);
			sub = openat(fd, bucket, 0);
			if(sub < 0)
			{
				sub = mkdirat(fd, bucket, 0775);
				if(sub >= 0)
					sub = openat(fd, bucket, 0);
				if(sub < 0)
				{
					if(fd != dir_fd)
						close(fd);
					return sub;
				}
			}
			if(fd != dir_fd)
				close(fd);
			fd = sub;
		}
	}
	return fd;
}

int diskhash::key_fd(mm_val_t * key, bool create)
{
	int key_fd, bucket = bucket_fd(key, create);
	char _key_name[22];
	const char * key_name = _key_name;
	if(bucket < 0)
		return bucket;
	switch(key_type)
	{
		case MM_U32:
			snprintf(_key_name, sizeof(_key_name), "%u", key->u32);
			break;
		case MM_U64:
			snprintf(_key_name, sizeof(_key_name), "%llu", key->u64);
			break;
		case MM_STR:
			key_name = (const char *) key;
			break;
		default:
			close(bucket);
			return -EINVAL;
	}
	key_fd = openat(bucket, key_name, 0);
	if(key_fd < 0 && errno == ENOENT && create)
	{
		int r = mkdirat(bucket, key_name, 0775);
		if(r < 0)
			return r;
		key_fd = openat(bucket, key_name, 0);
	}
	close(bucket);
	return key_fd;
}
