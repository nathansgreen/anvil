/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BLOB_COMPARATOR_H
#define __BLOB_COMPARATOR_H

#include <assert.h>

#ifndef __cplusplus
#error blob_comparator.h is a C++ header file
#endif

#include "blob.h"
#include "istr.h"

/* A blob comparator compares blobs in a way other than just memcmp(), allowing
 * applications to sort dtables with blob keys in arbitrary ways. */
/* NOTE: blob comparators must be heap-allocated; they cannot be stored on the
 * stack. When a blob comparator is allocated it gets a use count of 1. Its
 * release() method should be called by the allocating code once it has passed
 * it to wherever it wants; that code in turn should retain() it and itself
 * call release() again later, when it is finished with the comparator. */
class blob_comparator
{
public:
	/* compare() need not compare nonexistent blobs; they cannot be keys. */
	virtual int compare(const blob & a, const blob & b) const = 0;
	
	/* hash() should be overridden if you compare non-identical blobs as
	 * equal; if not, you can just use this default implementation. */
	inline virtual size_t hash(const blob & blob) const
	{
		/* uses FNV hash taken from stl::tr1::hash */
		size_t r = 2166136261u;
		size_t length = blob.size();
		for(size_t i = 0; i < length; i++)
		{
			r ^= blob[i];
			r *= 16777619u;
		}
		return r;
	}
	
	/* A blob comparator has a name so that it can be stored into dtables
	 * which are created using this comparator, and later the name can be
	 * checked when opening those dtables to try to verify that the same
	 * sort order will be used (since otherwise the file will not work). */
	inline blob_comparator(const istr & name) : name(name), usage(1) {}
	inline virtual ~blob_comparator() { assert(!usage); }
	
	inline void retain() const { usage++; }
	inline void release() const { if(!--usage) delete this; }
	
	const istr name;
private:
	mutable int usage;
};

/* This class can be used to wrap a blob comparator pointer so that it can be
 * used for STL methods like std::sort(). */
class blob_comparator_object
{
public:
	inline bool operator()(const blob & a, const blob & b) const
	{
		return blob_cmp->compare(a, b) < 0;
	}
	
	inline blob_comparator_object(const blob_comparator * comparator = NULL) : blob_cmp(comparator) {}
	
private:
	const blob_comparator * blob_cmp;
};

/* This class can be used in place of a NULL blob comparator pointer. */
class blob_comparator_null
{
public:
	inline bool operator()(const blob & a, const blob & b) const
	{
		return a.compare(b) < 0;
	}
};

/* If it is necessary to change the comparator during the use of an STL
 * container like std::set, then blob_comparator_refobject will help you out. */
class blob_comparator_refobject
{
public:
	inline bool operator()(const blob & a, const blob & b) const
	{
		return (blob_cmp ? blob_cmp->compare(a, b) : a.compare(b)) < 0;
	}
	
	inline blob_comparator_refobject(const blob_comparator *& comparator) : blob_cmp(comparator) {}
	
private:
	const blob_comparator *& blob_cmp;
};

#endif /* __BLOB_COMPARATOR_H */
