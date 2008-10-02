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

#include <vector>

/* blobs, the storage units of dtables */

class blob_comparator;

class blob
{
public:
	/* the nonexistent blob, for returning as an error from methods that return blob & */
	static const blob dne;
	/* the empty blob, which exists but has zero size */
	static const blob empty;
	
	/* non-existent blob constructor */
	inline blob() : internal(NULL) {}
	/* other constructors */
	blob(size_t size, const void * data);
	blob(const blob & x);
	blob & operator=(const blob & x);
	
	static ssize_t locate(const std::vector<blob> & array, const blob & blob, const blob_comparator * blob_cmp = NULL);
	
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
	
	inline int compare(const blob & x) const
	{
		int r;
		size_t min;
		if(internal == x.internal)
			return 0;
		if(!internal || !x.internal)
			return internal ? 1 : -1;
		min = (internal->size < x.internal->size) ? internal->size : x.internal->size;
		r = memcmp(internal->bytes, x.internal->bytes, min);
		return r ? r : (internal->size < x.internal->size) ? -1 : internal->size > x.internal->size;
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

#endif /* __BLOB_H */
