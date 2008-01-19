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
#include "disktree.h"

/* Disk trees are basically "b+trees" implemented with the file system. No
 * serious attempt to cache is made here; a separate caching module should be
 * used on top of this interface. */

/* iterators */

int disktree_it::next()
{
}

size_t disktree_it::size()
{
}

disktree_it::~disktree_it()
{
}

/* disk trees */

disktree::~disktree()
{
}

size_t disktree::keys()
{
}

size_t disktree::values()
{
}

ssize_t disktree::count_values(mm_val_t * key)
{
}

disktree_it * disktree::get_values(mm_val_t * key)
{
}

ssize_t disktree::count_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

disktree_it * disktree::get_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

disktree_it * disktree::iterator()
{
}

int disktree::remove_key(mm_val_t * key)
{
}

int disktree::reset_key(mm_val_t * key, mm_val_t * value)
{
}

int disktree::append_value(mm_val_t * key, mm_val_t * value)
{
}

int disktree::remove_value(mm_val_t * key, mm_val_t * value)
{
}

int disktree::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
}

/* create a new disktree (on disk) using the specified store path */
int disktree::init(int dfd, const char * store, mm_type_t key_type, mm_type_t val_type)
{
	int dir_fd, fd, r;
	mm_type_t types[2];
	size_t zero[2] = {0, 0};
	if(key_type != MM_U32 && key_type != MM_U64 && key_type != MM_STR)
		return -EINVAL;
	r = mkdirat(dfd, store, 0755);
	if(r < 0)
		return r;
	dir_fd = openat(dfd, store, 0);
	if(dir_fd < 0)
	{
		r = dir_fd;
		goto fail_dir;
	}
	fd = openat(dir_fd, "dt", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
	{
		r = fd;
		goto fail_open;
	}
	types[DT_KT_IDX] = key_type;
	types[DT_VT_IDX] = val_type;
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
	unlinkat(dir_fd, "dt", 0);
fail_open:
	close(dir_fd);
fail_dir:
	unlinkat(dfd, store, AT_REMOVEDIR);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

/* open a disktree on disk, or return NULL on error */
disktree * disktree::open(uint8_t * id, int dfd, const char * store)
{
	return NULL;
}

disktree::disktree(uint8_t * id)
	: multimap(id)
{
}
