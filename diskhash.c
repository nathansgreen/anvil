/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "diskhash.h"

/* Disk hashes are basically "hash maps" implemented with the file system. No
 * serious attempt to cache is made here; callers should do their own caching.
 * Currently we just construct a 32-bit hash value from the key, and store all
 * the values in a nested directory tree named by the hexadecimal bytes of the
 * hash value. */

/* create a new diskhash using the specified store path */
int diskhash_init(const char * store, dh_type_t key_type, dh_type_t val_type)
{
	int cwd_fd, fd, r;
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return cwd_fd;
	r = mkdir(store, 0755);
	if(r < 0)
		goto fail_mkdir;
	r = chdir(store);
	if(r < 0)
		goto fail_chdir;
	fd = open("dh", O_WRONLY | O_CREAT, 0664);
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

/* basically just rm -rf but the top-level thing must be a directory */
int diskhash_drop(const char * store)
{
	int r, cwd_fd;
	struct dirent * ent;
	DIR * dir = opendir(store);
	if(!dir)
		return -errno;
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
	{
		closedir(dir);
		return cwd_fd;
	}
	if((r = chdir(store)) < 0)
	{
	fail:
		fchdir(cwd_fd);
		close(cwd_fd);
		closedir(dir);
		return r;
	}
	while((ent = readdir(dir)))
	{
		struct stat st;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if((r = stat(ent->d_name, &st)) < 0)
			goto fail;
		if(S_ISDIR(st.st_mode))
			r = diskhash_drop(ent->d_name);
		else
			r = unlink(ent->d_name);
		if(r < 0)
			goto fail;
	}
	fchdir(cwd_fd);
	close(cwd_fd);
	closedir(dir);
	return rmdir(store);
}

/* open a diskhash */
diskhash_t * diskhash_open(const char * store)
{
	/* XXX */
	return (diskhash_t *) 1;
}

/* close a diskhash */
int diskhash_close(diskhash_t * dh)
{
	/* XXX */
	return 0;
}


/* get diskhash size */
size_t diskhash_size(diskhash_t * dh)
{
	/* XXX */
	return 0;
}


/* insert a new entry or replace an existing entry */
int diskhash_insert(diskhash_t * dh, const dh_val_t * key, const dh_val_t * val)
{
	/* XXX */
	return 0;
	return -ENOSYS;
}

/* remove an existing entry */
int diskhash_erase(diskhash_t * dh, const dh_val_t * key)
{
	/* XXX */
	return -ENOSYS;
}

/* look up an entry */
int diskhash_lookup(diskhash_t * dh, const dh_val_t * key, dh_val_t * val)
{
	/* XXX */
	return -ENOSYS;
}


/* initialize an iterator */
int diskhash_it_init(diskhash_t * dh, diskhash_it_t * it)
{
	/* XXX */
	return -ENOSYS;
}

/* get the next entry, or the first if the iterator is new */
int diskhash_it_next(diskhash_it_t * it)
{
	/* XXX */
	return -ENOSYS;
}

/* free any resources used by the iterator */
int diskhash_it_done(diskhash_it_t * it)
{
	/* XXX */
	return 0;
}
