/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BLOB_H
#define __BLOB_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error blob.h is a C++ header file
#endif

/* blobs, the storage units of dtables */

class blob
{
public:
	/* non-existent blob constructor */
	inline blob() : internal(NULL) {}
	/* other constructors */
	blob(size_t size, const void * data);
	blob(const blob & x);
	blob & operator=(const blob & x);
	
	inline ~blob()
	{
		if(internal && !--internal->shares)
			free(internal);
	}
	
	inline const uint8_t & operator[](size_t i) const
	{
		assert(internal);
		assert(i < internal->size);
		return internal->bytes[i];
	}
	
	template <class T>
	inline const T & index(size_t i, size_t off = 0) const
	{
		assert(internal);
		assert(off + (i + 1) * sizeof(T) <= internal->size);
		return *(T *) &internal->bytes[off + i * sizeof(T)];
	}
	
	inline size_t size() const
	{
		return internal ? internal->size : 0;
	}
	
	inline size_t shares() const
	{
		return internal ? internal->shares : 0;
	}
	
	/* does this blob exist? */
	inline bool exists() const
	{
		return internal != NULL;
	}
	
	inline bool operator==(const blob & x) const
	{
		if(internal == x.internal)
			return true;
		if(!internal || !x.internal)
			return false;
		if(internal->size != x.internal->size)
			return false;
		return !memcmp(internal->bytes, x.internal->bytes, internal->size);
	}
	
	inline bool operator!=(const blob & x) const
	{
		return !(*this == x);
	}
	
	inline bool operator<(const blob & x) const
	{
		int r;
		size_t min;
		if(internal == x.internal)
			return false;
		if(!internal || !x.internal)
			return !internal;
		min = (internal->size < x.internal->size) ? internal->size : x.internal->size;
		r = memcmp(internal->bytes, x.internal->bytes, min);
		return r ? r < 0 : internal->size < x.internal->size;
	}
	
	inline bool operator<=(const blob & x) const
	{
		return !(x < *this);
	}
	
	inline bool operator>(const blob & x) const
	{
		return x < *this;
	}
	
	inline bool operator>=(const blob & x) const
	{
		return !(*this < x);
	}
	
private:
	struct blob_internal
	{
		size_t size;
		size_t shares;
		uint8_t bytes[0];
	} * internal;
	
	friend class blob_buffer;
};

/* a metablob does not have any actual data, but knows how long the data would
 * be and whether or not it's even present (i.e. a non-existent blob) */
class metablob
{
public:
	inline metablob() : data_size(0), data_exists(false) {}
	inline metablob(size_t size) : data_size(size), data_exists(true) {}
	inline metablob(const metablob & x) : data_size(x.data_size), data_exists(x.data_exists) {}
	inline metablob(const blob & x) : data_size(x.size()), data_exists(x.exists()) {}
	inline size_t size() const { return data_size; }
	inline bool exists() const { return data_exists; }
private:
	size_t data_size;
	bool data_exists;
};

/* some useful marshalling functions */

static inline uint8_t byte_size(uint32_t value)
{
	if(value < 0x100)
		return 1;
	if(value < 0x10000)
		return 2;
	if(value < 0x1000000)
		return 3;
	return 4;
}

template<class T>
static inline void layout_bytes(uint8_t * array, T * index, uint32_t value, uint8_t size)
{
	T i = *index;
	*index += size;
	/* write big endian order */
	while(size-- > 0)
	{
		array[i + size] = value & 0xFF;
		value >>= 8;
	}
}

template<class T>
static inline uint32_t read_bytes(const uint8_t * array, T * index, uint8_t size)
{
	uint32_t value = 0;
	T max = size + *index;
	/* read big endian order */
	for(; *index < max; ++*index)
		value = (value << 8) | array[*index];
	return value;
}

template<class T>
static inline uint32_t read_bytes(const uint8_t * array, T index, uint8_t size)
{
	uint32_t value = 0;
	T max = size + index;
	/* read big endian order */
	for(; index < max; ++index)
		value = (value << 8) | array[index];
	return value;
}

#endif /* __BLOB_H */
