/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "rofile.h"

int rofile::open(int dfd, const char * file)
{
	if(fd >= 0)
		close();
	fd = openat(dfd, file, O_RDONLY);
	if(fd < 0)
		return fd;
	f_size = lseek(fd, 0, SEEK_END);
	/* reset the buffers */
	reset();
	return 0;
}

void rofile::close()
{
	if(fd >= 0)
	{
		::close(fd);
		fd = -1;
	}
}
