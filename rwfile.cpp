/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include "openat.h"

#include "rwfile.h"

int rwfile::create(int dfd, const char * file, patchgroup_id_t pid, mode_t mode)
{
	if(fd >= 0)
	{
		int r = close();
		if(r < 0)
			return r;
	}
	fd = openat(dfd, file, O_RDWR | O_CREAT | O_TRUNC, mode);
	if(fd < 0)
		return fd;
	write_mode = true;
	filled = 0;
	write_offset = 0;
	this->pid = pid;
	return 0;
}

int rwfile::open(int dfd, const char * file, off_t end_offset, patchgroup_id_t pid)
{
	if(fd >= 0)
	{
		int r = close();
		if(r < 0)
			return r;
	}
	fd = openat(dfd, file, O_RDWR);
	if(fd < 0)
		return fd;
	write_mode = true;
	filled = 0;
	write_offset = end_offset;
	this->pid = pid;
	return 0;
}

int rwfile::flush()
{
	ssize_t r = 0, written = 0;
	if(!filled)
		return 0;
	if(pid > 0)
		patchgroup_engage(pid);
	while(written < filled)
	{
		r = pwrite(fd, &buffer[written], filled - written, write_offset);
		if(r <= 0)
		{
			if(errno == EINTR)
				continue;
			if(written)
				/* part of the buffer was written, so move the rest */
				memmove(buffer, &buffer[written], filled - written);
			break;
		}
		written += r;
		write_offset += r;
	}
	if(pid > 0)
		patchgroup_disengage(pid);
	filled -= written;
	return (!filled) ? 0 : (r < 0) ? (int) r : -1;
}

int rwfile::close()
{
	if(write_mode && filled)
	{
		int r = flush();
		if(r < 0)
			return r;
	}
	::close(fd);
	return 0;
}

int rwfile::set_pid(patchgroup_id_t pid)
{
	if(write_mode && filled)
	{
		int r = flush();
		if(r < 0)
			return r;
	}
	this->pid = pid;
	return 0;
}

ssize_t rwfile::append(const void * data, ssize_t size)
{
	ssize_t r, orig = size;
	
	/* switch to write mode if necessary */
	if(!write_mode)
	{
		write_mode = true;
		filled = 0;
	}
	
	/* handle large writes without the buffer */
	if(size > buffer_size)
	{
		ssize_t written = 0;
		r = flush();
		if(r < 0)
			return r;
		if(pid > 0)
			patchgroup_engage(pid);
		while(written < size)
		{
			/* can't use void * in arithmetic... */
			r = pwrite(fd, &((uint8_t *) data)[written], size - written, write_offset);
			if(r <= 0)
			{
				if(errno == EINTR)
					continue;
				break;
			}
			written += r;
			write_offset += r;
		}
		if(pid > 0)
			patchgroup_disengage(pid);
		return written ? written : r;
	}
	
	if(filled + size >= buffer_size)
	{
		/* we'll [over]fill the buffer, so fill it and write it */
		memcpy(&buffer[filled], data, buffer_size - filled);
		size -= buffer_size - filled;
		/* can't use void * in arithmetic... */
		data = &((uint8_t *) data)[buffer_size - filled];
		filled = buffer_size;
		r = flush();
		if(r < 0)
		{
			/* could be zero, if we already had an error like this before */
			size = orig - size;
			return size ? size : r;
		}
	}
	
	/* just copy to the buffer */
	memcpy(&buffer[filled], data, size);
	filled += size;
	
	return orig;
}

ssize_t rwfile::read(off_t offset, void * data, ssize_t size)
{
	off_t buffer_offset;
	ssize_t orig = size;
	
	/* make sure any dirty data is written */
	if(write_mode && filled)
	{
		int r = flush();
		if(r < 0)
			return r;
	}
	
	/* handle large reads without the buffer */
	if(size > buffer_size)
		return pread(fd, data, size, offset);
	
	if(write_mode || offset < read_offset || read_offset + filled <= offset)
	{
		/* current buffer is useless, switch it out */
		write_mode = false;
		read_offset = offset;
		filled = pread(fd, buffer, buffer_size, offset);
		if(filled <= 0)
			return filled;
	}
	
	buffer_offset = offset - read_offset;
	if(buffer_offset + size <= filled)
	{
		/* current buffer handles the read completely */
		memcpy(data, &buffer[buffer_offset], size);
		return size;
	}
	
	/* get what we can from the current buffer */
	memcpy(data, &buffer[buffer_offset], filled - buffer_offset);
	buffer_offset = filled - buffer_offset;
	offset += buffer_offset;
	/* can't use void * in arithmetic... */
	data = &((uint8_t *) data)[buffer_offset];
	size -= buffer_offset;
	
	/* get the next buffer, which should be sufficient */
	read_offset = offset;
	filled = pread(fd, buffer, buffer_size, offset);
	if(filled > 0)
	{
		ssize_t left = (size < filled) ? size : filled;
		memcpy(data, buffer, left);
		size -= left;
	}
	
	return orig - size;
}
