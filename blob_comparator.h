#ifndef __BLOB_COMPARATOR_H
#define __BLOB_COMPARATOR_H

#include <assert.h>

#ifndef __cplusplus
#error blob_comparator.h is a C++ header file
#endif

#include "istr.h"
#include "blob.h"
#include "dtype.h"

/* a blob comparator compares blobs in a way other than just memcmp(), allowing
 * applications to sort dtables with blob keys in arbitrary ways */
class blob_comparator
{
public:
	/* compare() need not compare nonexistent blobs; they cannot be keys */
	virtual int compare(const blob & a, const blob & b) const = 0;
	inline int compare(const dtype & a, const dtype & b) const
	{
		assert(a.type == dtype::BLOB && b.type == dtype::BLOB);
		return compare(a.blb, b.blb);
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

#endif /* __BLOB_COMPARATOR_H */
