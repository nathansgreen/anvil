/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __RWFILE_H
#define __RWFILE_H

#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>

#ifndef __cplusplus
#error rwfile.h is a C++ header file
#endif

extern "C" {
/* Featherstitch does not know about C++ so we include
 * its header file inside an extern "C" block. */
#include <patchgroup.h>
}

/* This class provides a stdio-like wrapper around a read/write file descriptor,
 * allowing data to be appended to the file (starting at a given position) and
 * optionally engaging a patchgroup before doing any writes. Both reads and
 * writes are buffered. */

class rwfile
{
public:
	/* buffer_size is in KiB */
	inline rwfile(ssize_t buffer_size = 8)
		: fd(-1), write_mode(true), filled(0), write_offset(0), pid(0), buffer(NULL)
	{
		this->buffer_size = buffer_size * 1024;
		buffer = new uint8_t[this->buffer_size];
	}
	
	inline ~rwfile()
	{
		if(fd >= 0)
		{
			int r = close();
			assert(r >= 0);
		}
		if(buffer)
			delete[] buffer;
	}
	
	int create(int dfd, const char * file, patchgroup_id_t pid = 0, mode_t mode = 0644);
	
	/* flush the buffer */
	int flush();
	
	/* flushes the buffer before closing, so might fail */
	int close();
	
	/* flushes the buffer before setting, so might fail */
	int set_pid(patchgroup_id_t pid);
	
	/* append some data to the file */
	ssize_t append(const void * data, ssize_t size);
	
	/* append the structure to the file */
	template<class T>
	inline ssize_t append(const T * data)
	{
		ssize_t r = append(data, sizeof(T));
		return (r == sizeof(T)) ? 0 : (r < 0) ? r : -1;
	}
	
protected:
	int fd;
	bool write_mode;
	ssize_t filled, buffer_size;
	off_t read_offset, write_offset;
	patchgroup_id_t pid;
	uint8_t * buffer;
};

#endif /* __RWFILE_H */
