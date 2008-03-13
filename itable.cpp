/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>

#include "openat.h"
#include "itable.h"

int itable_disk::init(int dfd, const char * file)
{
	if(fd > -1)
		deinit();
	fd = openat(dfd, file, O_RDONLY);
	if(fd < 0)
		return fd;
	/* ... */
}

void itable_disk::deinit()
{
	if(fd < 0)
		return;
	/* ... */
}

bool itable_disk::has(iv_int k1)
{
}
bool itable_disk::has(const char * k1)
{
}

bool itable_disk::has(iv_int k1, iv_int k2)
{
}
bool itable_disk::has(iv_int k1, const char * k2)
{
}
bool itable_disk::has(const char * k1, iv_int k2)
{
}
bool itable_disk::has(const char * k1, const char * k2)
{
}

off_t itable_disk::get(iv_int k1, iv_int k2)
{
}
off_t itable_disk::get(iv_int k1, const char * k2)
{
}
off_t itable_disk::get(const char * k1, iv_int k2)
{
}
off_t itable_disk::get(const char * k1, const char * k2)
{
}

int itable_disk::next(iv_int k1, iv_int * key)
{
}
int itable_disk::next(const char * k1, const char ** key)
{
}

int itable_disk::next(iv_int k1, iv_int k2, iv_int * key)
{
}
int itable_disk::next(iv_int k1, const char * k2, const char ** key)
{
}
int itable_disk::next(const char * k1, iv_int k2, iv_int * key)
{
}
int itable_disk::next(const char * k1, const char * k2, const char ** key)
{
}
