/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

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
int disktree::init(const char * store, mm_type_t key_type, mm_type_t val_type)
{
	int cwd_fd, fd, r;
	cwd_fd = ::open(".", 0);
	if(cwd_fd < 0)
		return cwd_fd;
	r = mkdir(store, 0755);
	if(r < 0)
		goto fail_mkdir;
	r = chdir(store);
	if(r < 0)
		goto fail_chdir;
	fd = ::open("dt", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_open;
	r = write(fd, &key_type, sizeof(key_type));
	if(r != sizeof(key_type))
		goto fail_write;
	r = write(fd, &val_type, sizeof(val_type));
	if(r != sizeof(val_type))
		goto fail_write;
	close(fd);
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return 0;
	
fail_write:
	close(fd);
	unlink("dt");
fail_open:
	fchdir(cwd_fd);
fail_chdir:
	rmdir(store);
fail_mkdir:
	close(cwd_fd);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

/* open a disktree on disk, or return NULL on error */
disktree * disktree::open(const char * store)
{
	return NULL;
}

disktree::disktree()
{
}
