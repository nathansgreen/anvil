/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "openat.h"
#include "multimap.h"

multimap_it::~multimap_it()
{
	free_key();
	free_value();
}

void multimap_it::free_key()
{
	if(!key)
		return;
	switch(it_map->get_key_type())
	{
		case MM_U32:
		case MM_U64:
			assert(key == &s_key);
			break;
		case MM_STR:
			free(key);
			break;
		default:
			/* TODO: use the toilet error stream */
			fprintf(stderr, "%s(): leaking key of unknown type at %p\n", __FUNCTION__, key);
	}
	key = NULL;
}

void multimap_it::free_value()
{
	if(!val)
		return;
	switch(it_map->get_val_type())
	{
		case MM_U32:
		case MM_U64:
			assert(val == &s_val);
			break;
		case MM_STR:
			free(val);
			break;
		case MM_BLOB:
			assert(val == &s_val);
			free(val->blob);
			break;
		default:
			/* TODO: use the toilet error stream */
			fprintf(stderr, "%s(): leaking value of unknown type at %p\n", __FUNCTION__, val);
	}
	val = NULL;
}

multimap::~multimap()
{
}

multimap::multimap(uint8_t * id)
	: toilet_id(id)
{
}

int multimap::copy(multimap * source, multimap * dest)
{
	multimap_it * it;
	size_t size;
	int r = 0;
	if(source->get_key_type() != dest->get_key_type())
		return -EINVAL;
	if(source->get_val_type() != dest->get_val_type())
		return -EINVAL;
	it = source->iterator();
	size = it->size();
	while(size-- > 0)
	{
		r = it->next();
		if(r < 0)
			break;
		r = dest->append_value(it->key, it->val);
		if(r < 0)
			break;
	}
	delete it;
	return r;
}

/* basically just rm -rf but the top-level thing must be a directory */
/* if count is non-null, increment *count by the number of names unlinked, except the top-level directory */
int multimap::drop(int dfd, const char * store, size_t * count)
{
	DIR * dir;
	struct dirent * ent;
	int r, dir_fd = openat(dfd, store, 0);
	if(dir_fd < 0)
		return dir_fd;
	dir = fdopendir(dir_fd);
	if(!dir)
	{
		r = -errno;
		close(dir_fd);
		errno = -r;
		return r;
	}
	dir_fd = dirfd(dir);
	while((ent = readdir(dir)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		r = unlinkat(dir_fd, ent->d_name, 0);
		if(r < 0 && errno == EISDIR)
			/* we could stat it to find out, but fstatat64() was doing
			 * something funky and this saves system calls anyway */
			r = drop(dir_fd, ent->d_name, count);
		if(r < 0)
		{
			int save = errno;
			closedir(dir);
			close(dir_fd);
			errno = save;
			return r;
		}
		if(count)
			(*count)++;
	}
	closedir(dir);
	return unlinkat(dfd, store, AT_REMOVEDIR);
}
