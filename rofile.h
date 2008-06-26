/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ROFILE_H
#define __ROFILE_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

#ifndef __cplusplus
#error rofile.h is a C++ header file
#endif

/* This class provides a stdio-like wrapper around a read-only file descriptor,
 * keeping track of several buffers for file data preread from different parts
 * of the file but not yet requested by the rest of the application. */

class rofile
{
public:
	inline rofile() : fd(-1) {}
	virtual inline ~rofile()
	{
		if(fd >= 0)
			close();
	}
	
	int open(int dfd, const char * file);
	void close();
	
	/* read some data from the file */
	virtual ssize_t read(off_t offset, void * data, ssize_t size) const = 0;
	
	/* read the structure from the file */
	template<class T>
	inline int read(off_t offset, T * data) const
	{
		ssize_t r = read(offset, data, sizeof(T));
		return (r == sizeof(T)) ? 0 : (r < 0) ? (int) r : -1;
	}
	
	/* buffer_size is in KiB */
	template<ssize_t buffer_size, int buffer_count>
	static rofile * open(int dfd, const char * file);
	
protected:
	/* reset all buffers */
	virtual void reset() = 0;
	
	int fd;
	off_t f_size;
	mutable size_t last_buffer;
};

/* buffer_size is in KiB */
template<ssize_t buffer_size, int buffer_count>
class rofile_size : public rofile
{
public:
	virtual ssize_t read(off_t offset, void * data, ssize_t size) const
	{
		ssize_t left = size;
		if(size > (ssize_t) sizeof(buffers[0].data))
			return pread(fd, data, size, offset);
		/* we will need at most two buffers now */
		if(!buffers[last_buffer].contains(offset))
		{
			if(!find_not_last(offset))
				/* cache it */
				if(!load_buffer(offset))
					return size - left; /* 0 */
		}
		if(buffers[last_buffer].use(&offset, &data, &left))
			return size - left;
		/* we still need a second buffer */
		if(!find_not_last(offset))
			/* cache it */
			if(!load_buffer(offset))
				return size - left;
		bool done = buffers[last_buffer].use(&offset, &data, &left);
		assert(done);
		return size - left;
	}
	
private:
	struct buffer
	{
		off_t offset;
		ssize_t size;
		/* this is the only place we use buffer_size directly */
		uint8_t data[buffer_size * 1024];
		
		inline bool contains(off_t byte)
		{
			/* note that if the buffer is empty, size will be 0 and this will fail */
			return offset <= byte && byte < offset + size;
		}
		
		/* returns true if no more can/should be read */
		bool use(off_t * byte, void ** target, ssize_t * left)
		{
			ssize_t start = *byte - offset;
			ssize_t total = size - start;
			assert(contains(*byte));
			if(*left < total)
				total = *left;
			memcpy(*target, &data[start], total);
			*byte += total;
			/* can't use void * in arithmetic... */
			*target = &((uint8_t *) *target)[total];
			*left -= total;
			return !*left || size < (ssize_t) sizeof(data);
		}
		
		/* reload this buffer from file to contain the given offset */
		inline bool load(int fd, off_t byte)
		{
			offset = byte - (byte % sizeof(data));
			size = pread(fd, data, sizeof(data), offset);
			if(size <= 0)
			{
				offset = -1;
				size = 0;
				return false;
			}
			return byte - offset < size;
		}
	};
	
	mutable buffer buffers[buffer_count];
	
	virtual void reset()
	{
		last_buffer = 0;
		for(size_t i = 0; i < buffer_count; i++)
		{
			buffers[i].offset = -1;
			buffers[i].size = 0;
		}
	}
	
	/* find the buffer containing the given offset and set last_buffer to its index */
	/* only use this if last_buffer is not already the index of such a buffer! */
	bool find_not_last(off_t offset) const
	{
		for(size_t i = 0; i < buffer_count; i++)
			if(i != last_buffer && buffers[i].contains(offset))
			{
				last_buffer = i;
				return true;
			}
		return false;
	}
	
	/* load a buffer from file containing the given offset, and set last_buffer to its index */
	bool load_buffer(off_t offset) const
	{
		if(offset >= f_size)
			return false;
		/* pick a buffer to reload */
		/* TODO we can do a better job than this! */
		size_t i = rand() % buffer_count;
		bool ok = buffers[i].load(fd, offset);
		if(ok)
			last_buffer = i;
		return ok;
	}
};

template<ssize_t buffer_size, int buffer_count>
rofile * rofile::open(int dfd, const char * file)
{
	rofile * size = new rofile_size<buffer_size, buffer_count>;
	if(size)
	{
		int r = size->open(dfd, file);
		if(r < 0)
		{
			delete size;
			size = NULL;
		}
	}
	return size;
}

#endif /* __ROFILE_H */
