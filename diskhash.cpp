/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
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
}

size_t diskhash::keys()
{
}

size_t diskhash::values()
{
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
}

int diskhash::reset_key(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::append_value(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::remove_value(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
}

/* create a new diskhash (on disk) using the specified store path */
int diskhash::init(int dfd, const char * store, mm_type_t key_type, mm_type_t val_type)
{
	int dir_fd, fd, r;
	r = mkdirat(dfd, store, 0755);
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
	r = write(fd, &key_type, sizeof(key_type));
	if(r != sizeof(key_type))
		goto fail_write;
	r = write(fd, &val_type, sizeof(val_type));
	if(r != sizeof(val_type))
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
diskhash * diskhash::open(int dfd, const char * store)
{
	return NULL;
}

diskhash::diskhash()
{
}
