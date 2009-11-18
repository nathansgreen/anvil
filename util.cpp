/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "openat.h"

#include "util.h"

int util::rm_r(int dfd, const char * path)
{
	DIR * dir;
	struct stat64 st;
	struct dirent * ent;
	int fd, copy, r = fstatat64(dfd, path, &st, AT_SYMLINK_NOFOLLOW);
	if(r < 0)
		return r;
	if(!S_ISDIR(st.st_mode))
		return unlinkat(dfd, path, 0);
	fd = openat(dfd, path, O_RDONLY);
	if(fd < 0)
		return fd;
	copy = dup(fd);
	if(copy < 0)
	{
		close(fd);
		return copy;
	}
	dir = fdopendir(copy);
	if(!dir)
	{
		close(copy);
		close(fd);
		return -1;
	}
	while((ent = readdir(dir)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		r = rm_r(fd, ent->d_name);
		if(r < 0)
			break;
	}
	closedir(dir);
	close(fd);
	if(r < 0)
		return r;
	return unlinkat(dfd, path, AT_REMOVEDIR);
}

istr util::tilde_home(const istr & path)
{
	size_t length;
	const char * home = getenv("HOME");
	if(!home)
		return path;
	length = strlen(home);
	if(!length)
		return path;
	if(home[length - 1] == '/')
		if(!--length)
			return path;
	if(strncmp(path, home, length))
		return path;
	if(path[length] != '/')
		return path[length] ? path : "~";
	char result[path.length() - length + 2];
	sprintf(result, "~%s", &path[length]);
	return result;
}
