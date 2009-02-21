/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
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

#include "blob.h"
#include "istr.h"

/* This class provides a stdio-like wrapper around a read/write file descriptor,
 * allowing data to be appended to the file (starting at a given position) and
 * optionally either calling a given handler or starting an external transaction
 * dependency before doing any writes. Both reads and writes are buffered. */

class rwfile
{
public:
	/* buffer_size is in KiB */
	inline rwfile(ssize_t buffer_size = 8)
		: fd(-1), write_mode(true), external(false), filled(0), write_offset(0), handler(NULL), buffer(NULL)
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
	
	int create(int dfd, const char * file, bool tx_external = false, mode_t mode = 0644);
	int open(int dfd, const char * file, off_t end_offset, bool tx_external = false);
	
	/* flush the buffer */
	int flush();
	
	/* flushes the buffer before closing, so might fail */
	int close();
	
	/* sets the logical end of the file */
	int truncate(off_t end_offset);
	
	class flush_handler
	{
	public:
		virtual int pre() = 0;
		virtual void post() = 0;
		inline virtual ~flush_handler() {}
	};
	/* these flush the buffer before setting, so might fail */
	int set_handler(flush_handler * handler);
	int set_external(bool tx_external);
	
	inline flush_handler * get_handler()
	{
		return handler;
	}
	
	inline bool get_external()
	{
		return external;
	}
	
	/* return the current idea of the end of the file */
	inline size_t end() const
	{
		return write_mode ? (write_offset + filled) : write_offset;
	}
	
	/* append some data to the file */
	ssize_t append(const void * data, ssize_t size);
	
	/* appends padding zeroes to the file */
	ssize_t pad(ssize_t size);
	
	/* read some data from the file */
	/* this should be const, but due to the shared cache, it's easier not to bother */
	ssize_t read(off_t offset, void * data, ssize_t size);
	
	/* read a string; copies twice, but hopefully it's not big */
	inline istr read_string(off_t offset, ssize_t length)
	{
		char string[length];
		ssize_t r = read(offset, string, length);
		if(r != length)
			return NULL;
		return istr(string, length);
	}
	
	/* append the structure to the file */
	template<class T>
	inline int append(const T * data)
	{
		ssize_t r = append(data, sizeof(T));
		return (r == sizeof(T)) ? 0 : (r < 0) ? (int) r : -1;
	}
	
	/* read the structure from the file */
	/* this should be const, but due to the shared cache, it's easier not to bother */
	template<class T>
	inline int read(off_t offset, T * data)
	{
		ssize_t r = read(offset, data, sizeof(T));
		return (r == sizeof(T)) ? 0 : (r < 0) ? (int) r : -1;
	}
	
	/* appends just the blob with no length field; you must have some way of determining the length later */
	inline int append(const blob & blob)
	{
		ssize_t size = blob.size();
		ssize_t r = size ? append(&blob[0], size) : 0;
		return (r == size) ? 0 : (r < 0) ? (int) r : -1;
	}
	
	/* appends just the string with no null terminator; you must have some way of determining the length later */
	inline int append(const istr & string)
	{
		ssize_t length = string.length();
		ssize_t r = length ? append((const char *) string, length) : 0;
		return (r == length) ? 0 : (r < 0) ? (int) r : -1;
	}
	
private:
	int fd;
	bool write_mode, external;
	ssize_t filled, buffer_size;
	off_t read_offset, write_offset;
	flush_handler * handler;
	uint8_t * buffer;
};

#endif /* __RWFILE_H */
