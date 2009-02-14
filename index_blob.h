/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_BLOB_H
#define __INDEX_BLOB_H

#include <assert.h>

#ifndef __cplusplus
#error index_blob.h is a C++ header file
#endif

#include "blob.h"

class index_blob
{
public:
	inline index_blob() : modified(false), resized(false), indices(NULL), count(0) {}
	index_blob(size_t count);
	index_blob(size_t count, const blob & x);
	index_blob(const index_blob & x);
	index_blob & operator=(const index_blob & x);
	
	inline blob get(size_t index) const
	{
		assert(index < count);
		return indices[index].value;
	}
	
	inline int set(size_t index, const blob & value)
	{
		assert(index < count);
		if(value.size() != indices[index].value.size() ||
		   value.exists() != indices[index].value.exists())
			resized = true;
		modified = true;
		indices[index].value = value;
		indices[index].modified = true;
		return 0;
	}
	
	blob flatten() const;
	
	inline ~index_blob()
	{
		if(indices)
			delete[] indices;
	}
	
private:
	struct sub
	{
		blob value;
		bool modified;
		inline sub() : modified(false) {}
	};
	
	mutable blob base;
	mutable bool modified, resized;
	sub * indices;
	size_t count;
};

#endif /* __INDEX_BLOB_H */
