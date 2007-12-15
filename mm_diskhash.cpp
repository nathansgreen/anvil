/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "multimap.h"
#include "mm_diskhash.h"

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

size_t diskhash::count_values(mm_val_t * key)
{
}

diskhash_it * diskhash::get_values(mm_val_t * key)
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
int diskhash::init(const char * store, mm_type_t key_type, mm_type_t val_type)
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
	fd = ::open("dh", O_WRONLY | O_CREAT, 0664);
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
	unlink("dh");
fail_open:
	fchdir(cwd_fd);
fail_chdir:
	rmdir(store);
fail_mkdir:
	close(cwd_fd);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

diskhash * open(const char * store)
{
}

diskhash::diskhash()
{
}
