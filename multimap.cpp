/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "multimap.h"

multimap_it::~multimap_it()
{
}

multimap::~multimap()
{
}

int multimap::copy(multimap * source, multimap * dest)
{
}

/* basically just rm -rf but the top-level thing must be a directory */
int multimap::drop(const char * store)
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
			r = drop(ent->d_name);
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
