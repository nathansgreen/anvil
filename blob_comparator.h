#ifndef __BLOB_COMPARATOR_H
#define __BLOB_COMPARATOR_H

#include <assert.h>

#ifndef __cplusplus
#error blob_comparator.h is a C++ header file
#endif

#include "blob.h"
#include "istr.h"

/* a blob comparator compares blobs in a way other than just memcmp(), allowing
 * applications to sort dtables with blob keys in arbitrary ways */
class blob_comparator
{
public:
	/* compare() need not compare nonexistent blobs; they cannot be keys */
	virtual int compare(const blob & a, const blob & b) const = 0;

	/* hash() should be overwritten if you compare non-identical blobs as
	 * equal if not you can just use this without needing to write your own */
	virtual size_t hash(const blob & a) const
	{
		/* uses FNV hash taken from stl::tr1::hash */
		size_t r = static_cast<size_t>(2166136261UL);
		size_t length = a.size();
		for (size_t i = 0; i < length; ++i)
		{
			r ^= static_cast<size_t>(a[i]);
			r *= static_cast<size_t>(16777619UL);
		}
		return r;
	}

	/* a blob comparator has a name so that it can be stored into dtables
	 * which are created using this comparator, and later the name can be
	 * checked when opening those dtables to try to verify that the same
	 * sort order will be used (since otherwise the file will not work) */
	inline blob_comparator(const istr & name) : name(name), stack(false), usage(1) {}
	inline virtual ~blob_comparator() { assert(!usage); }
	
	inline void retain() const { usage++; }
	inline void release() const { if(!--usage && !stack) delete this; }
	inline void on_stack() { assert(!stack && usage == 1); stack = true; usage = 0; }
	
	const istr name;
private:
	bool stack;
	mutable int usage;
};

/* this class can be used to wrap a blob comparator pointer so that it can be
 * used for STL methods like std::sort() */
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

/* this class can be used in place of a NULL blob comparator pointer */
class blob_comparator_null
{
public:
	inline bool operator()(const blob & a, const blob & b) const
	{
		return a.compare(b) < 0;
	}
};

/* if it is necessary to change the comparator during the use of an STL
 * container like std::set, then blob_comparator_refobject will help you out */
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
