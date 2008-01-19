/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "openat.h"
#include "multimap.h"

multimap_it::~multimap_it()
{
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
int multimap::drop(int dfd, const char * store)
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
	while((ent = readdir(dir)))
	{
		struct stat64 st;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if((r = fstatat64(dir_fd, ent->d_name, &st, AT_SYMLINK_NOFOLLOW)) < 0)
		{
		fail:
			int save = errno;
			closedir(dir);
			close(dir_fd);
			errno = save;
			return r;
		}
		if(S_ISDIR(st.st_mode))
			r = drop(dir_fd, ent->d_name);
		else
			r = unlinkat(dir_fd, ent->d_name, 0);
		if(r < 0)
			goto fail;
	}
	closedir(dir);
	close(dir_fd);
	return unlinkat(dfd, store, AT_REMOVEDIR);
}
