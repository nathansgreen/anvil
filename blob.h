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
/* need to allow finding sizes of blobs without reading the blob data:
 * make a sub-blob that doesn't have data? make data access virtual? */

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

#endif /* __BLOB_H */
