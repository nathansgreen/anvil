#ifndef __REVERSE_BLOB_COMPARATOR_H
#define __REVERSE_BLOB_COMPARATOR_H

#ifndef __cplusplus
#error reverse_blob_comparator.h is a C++ header file
#endif

#include "blob_comparator.h"

class reverse_blob_comparator : public blob_comparator
{
public:
	inline virtual int compare(const blob & a, const blob & b) const
	{
		return b.compare(a);
	}
	
	inline reverse_blob_comparator() : blob_comparator("reverse") {}
	inline reverse_blob_comparator(const istr & name) : blob_comparator(name) {}
	inline virtual ~reverse_blob_comparator() {}
};

#endif /* __REVERSE_BLOB_COMPARATOR_H */
