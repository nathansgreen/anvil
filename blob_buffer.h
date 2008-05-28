/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BLOB_BUFFER_H
#define __BLOB_BUFFER_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error blob_buffer.h is a C++ header file
#endif

#include "blob.h"
#include "dtype.h"

class blob_buffer
{
public:
	inline blob_buffer() : buffer_capacity(0), internal(NULL) {}
	blob_buffer(size_t capacity);
	blob_buffer(size_t size, const void * data);
	blob_buffer(const blob & x);
	blob_buffer(const blob_buffer & x);
	blob_buffer & operator=(const blob & x);
	blob_buffer & operator=(const blob_buffer & x);
	
	inline ~blob_buffer()
	{
		if(internal && !--internal->shares)
			free(internal);
	}
	
	/* will *not* extend size or capacity */
	inline uint8_t & operator[](size_t i)
	{
		assert(internal);
		assert(i < internal->size);
		return internal->bytes[i];
	}
	
	/* will *not* extend size or capacity */
	template <class T>
	inline T & index(size_t i, size_t off = 0)
	{
		assert(internal);
		assert(off + (i + 1) * sizeof(T) <= internal->size);
		return *(T *) &internal->bytes[off + i * sizeof(T)];
	}
	
	/* will extend the size/capacity if necessary */
	inline int overwrite(size_t offset, const blob & x)
	{
		if(!x.exists())
			return 0;
		return overwrite(offset, &x[0], x.size());
	}
	
	inline int overwrite(size_t offset, const dtype & x)
	{
		/* hmm... the x.exists() test above will be uselessly inlined */
		return overwrite(offset, x.flatten());
	}
	
	inline int overwrite(size_t offset, const blob_buffer & x)
	{
		if(!x.exists())
			return 0;
		return overwrite(offset, x.internal->bytes, x.size());
	}
	
	int overwrite(size_t offset, const void * data, size_t length);
	
	inline int append(const blob & x)
	{
		return overwrite(size(), x);
	}
	
	inline int append(const dtype & x)
	{
		return overwrite(size(), x);
	}
	
	inline int append(const blob_buffer & x)
	{
		return overwrite(size(), x);
	}
	
	inline int append(const void * data, size_t length)
	{
		return overwrite(size(), data, length);
	}
	
	template<class T>
	inline blob_buffer & operator<<(const T & x)
	{
		int r = append(&x, sizeof(x));
		assert(r >= 0);
		return *this;
	}
	
	inline int layout_append(uint32_t value, uint8_t size)
	{
		int index = 0;
		uint8_t array[4];
		layout_bytes(array, &index, value, size);
		return append(array, size);
	}
	
	inline size_t size() const
	{
		return internal ? internal->size : 0;
	}
	
	inline size_t capacity() const
	{
		return buffer_capacity;
	}
	
	/* does this buffer exist? */
	inline bool exists() const
	{
		return internal != NULL;
	}
	
	int set_size(size_t size, bool clear = true);
	int set_capacity(size_t capacity);
	
	inline operator blob()
	{
		/* default constructor sets internal to NULL */
		blob value;
		value.internal = internal;
		if(internal)
			internal->shares++;
		return value;
	}
	
private:
	/* break sharing */
	int touch();
	
	size_t buffer_capacity;
	struct blob::blob_internal * internal;
};

#endif /* __BLOB_BUFFER_H */
