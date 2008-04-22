/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BLOB_H
#define __BLOB_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error blob.h is a C++ header file
#endif

/* blobs, the storage units of dtables */

class blob
{
public:
	/* negative entry blob constructor */
	inline blob() : internal(NULL) {}
	/* other constructors */
	blob(size_t size);
	blob(size_t size, const uint8_t * data);
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
	
	inline size_t size() const
	{
		return internal ? internal->size : 0;
	}
	
	inline size_t shares() const
	{
		return internal ? internal->shares : 0;
	}
	
	/* is this blob a negative entry? */
	inline bool negative() const
	{
		return !internal;
	}
	
	inline uint8_t * memory()
	{
		if(!internal)
			return NULL;
		if(internal->shares > 1)
			touch();
		return &internal->bytes[0];
	}
	
	/* change size */
	int set_size(size_t size);
	
private:
	/* break sharing */
	int touch();
	
	struct blob_internal
	{
		size_t size;
		size_t shares;
		uint8_t bytes[0];
	} * internal;
};

/* a metablob does not have any actual data, but knows how long the data would
 * be and whether or not it's even present (i.e. a negative entry) */
class metablob
{
public:
	inline metablob() : data_size(0), is_negative(true) {}
	inline metablob(size_t size) : data_size(size), is_negative(false) {}
	inline metablob(const metablob & x) : data_size(x.data_size), is_negative(x.is_negative) {}
	inline metablob(const blob & x) : data_size(x.size()), is_negative(x.negative()) {}
	inline size_t size() const { return data_size; }
	inline bool negative() const { return is_negative; }
private:
	size_t data_size;
	bool is_negative;
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
