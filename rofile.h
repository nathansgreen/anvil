/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ROFILE_H
#define __ROFILE_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>

#ifndef __cplusplus
#error rofile.h is a C++ header file
#endif

#include "util.h"

/* This class provides a stdio-like wrapper around a read-only file descriptor,
 * keeping track of several buffers for file data preread from different parts
 * of the file but not yet requested by the rest of the application. We expect
 * the file system buffer cache to do most of the real caching work; this class
 * just amortizes the cost of system calls over many small read requests. */

class rofile
{
public:
	inline rofile() : fd(-1) {}
	inline virtual ~rofile()
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
	
	/* buffer_size is in KiB */
	template<ssize_t buffer_size, int buffer_count>
	static rofile * open_mmap(int dfd, const char * file);
	
	/* size of file in bytes */
	inline off_t size() const { return f_size; }
	
protected:
	/* reset all buffers */
	virtual void reset() = 0;
	
	int fd;
	off_t f_size;
	mutable size_t last_buffer;

private:
	struct buffer_base
	{
		off_t offset;
		ssize_t size;
		int lru_index;
		
		inline bool contains(off_t byte)
		{
			/* note that if the buffer is empty, size will be 0 and this will fail */
			return offset <= byte && byte < offset + size;
		}
	};
	
	/* buffer_size is in bytes */
	template<ssize_t buffer_size>
	struct pread_buffer : public buffer_base
	{
		uint8_t data[buffer_size];
		
		/* reload this buffer from file to contain the given offset */
		inline bool load(int fd, off_t byte, off_t f_size, int lru_count)
		{
			offset = byte - (byte % buffer_size);
			size = pread(fd, data, buffer_size, offset);
			if(size <= 0)
			{
				offset = -1;
				size = 0;
				return false;
			}
			lru_index = lru_count;
			return byte - offset < size;
		}
		
		inline void reset()
		{
			offset = -1;
			size = 0;
			lru_index = 0;
		}
	};
	
	/* buffer_size is in bytes */
	template<ssize_t buffer_size>
	struct mmap_buffer : public buffer_base
	{
		uint8_t * data;
		
		/* reload this buffer from file to contain the given offset */
		inline bool load(int fd, off_t byte, off_t f_size, int lru_count)
		{
			unload();
			offset = byte - (byte % buffer_size);
			size = (f_size - offset);
			if(size > buffer_size)
				size = buffer_size;
			data = (uint8_t *) mmap(NULL, size, PROT_READ, MAP_SHARED, fd, offset);
			if(data == MAP_FAILED)
			{
				data = NULL;
				offset = -1;
				size = 0;
				return false;
			}
			madvise(data, size, MADV_WILLNEED);
			lru_index = lru_count;
			return true;
		}
		
		inline void unload()
		{
			if(data)
			{
				munmap(data, size);
				data = NULL;
			}
		}
		
		inline void reset()
		{
			unload();
			offset = -1;
			size = 0;
			lru_index = 0;
		}
		
		inline mmap_buffer() : data(NULL) {}
		inline ~mmap_buffer() { unload(); }
	};
	
	template<ssize_t buffer_size, class T>
	struct buffer : public T
	{
		/* returns true if no more can/should be read */
		bool use(off_t * byte, void ** target, ssize_t * left, int lru_count)
		{
			ssize_t start = *byte - T::offset;
			ssize_t total = T::size - start;
			assert(T::contains(*byte));
			if(*left < total)
				total = *left;
			util::memcpy(*target, &T::data[start], total);
			*byte += total;
			/* can't use void * in arithmetic... */
			*target = &((uint8_t *) *target)[total];
			*left -= total;
			T::lru_index = lru_count;
			return !*left || T::size < buffer_size;
		}
	};
};

/* buffer_size is in bytes */
template<ssize_t buffer_size, int buffer_count, class T>
class rofile_impl : public rofile
{
public:
	virtual ssize_t read(off_t offset, void * data, ssize_t size) const
	{
		ssize_t left = size;
		if(size > buffer_size)
			return pread(fd, data, size, offset);
		/* we will need at most two buffers now */
		if(!buffers[last_buffer].contains(offset))
		{
			if(!find_not_last(offset))
				/* cache it */
				if(!load_buffer(offset))
					return size - left; /* 0 */
		}
		if(buffers[last_buffer].use(&offset, &data, &left, ++lru_count))
			return size - left;
		/* we need a second buffer */
		if(!find_not_last(offset))
			/* cache it */
			if(!load_buffer(offset))
				return size - left;
		bool done = buffers[last_buffer].use(&offset, &data, &left, ++lru_count);
		assert(done);
		return size - left;
	}
	
private:
	
	mutable int lru_count;
	mutable T buffers[buffer_count];
	
	virtual void reset()
	{
		lru_count = 0;
		last_buffer = 0;
		for(size_t i = 0; i < buffer_count; i++)
			buffers[i].reset();
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
		size_t max_idx = 0;
		int max_age = lru_count - buffers[0].lru_index;
		if(offset >= f_size)
			return false;
		/* pick a buffer to reload */
		for(size_t i = 1; i < buffer_count; i++)
		{
			int age;
			if(!buffers[i].size)
			{
				max_idx = i;
				break;
			}
			age = lru_count - buffers[i].lru_index;
			if(age > max_age)
			{
				max_idx = i;
				max_age = age;
			}
		}
		bool ok = buffers[max_idx].load(fd, offset, f_size, lru_count);
		if(ok)
			last_buffer = max_idx;
		return ok;
	}
};

/* the buffer sizes must all match */
#define ROFILE_IMPL(buffer_size, buffer_count, method) \
	rofile_impl<(buffer_size) * 1024, buffer_count, buffer<(buffer_size) * 1024, method##_buffer<(buffer_size) * 1024> > >

template<ssize_t buffer_size, int buffer_count>
rofile * rofile::open(int dfd, const char * file)
{
	rofile * size = new ROFILE_IMPL(buffer_size, buffer_count, pread);
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

template<ssize_t buffer_size, int buffer_count>
rofile * rofile::open_mmap(int dfd, const char * file)
{
	rofile * size = new ROFILE_IMPL(buffer_size, buffer_count, mmap);
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
