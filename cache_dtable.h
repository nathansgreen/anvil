/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CACHE_DTABLE_H
#define __CACHE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error cache_dtable.h is a C++ header file
#endif

#include <queue>
#include <ext/hash_map>

#include "blob.h"
#include "dtable.h"
#include "dtable_factory.h"

/* The cache dtable sits on top of another dtable, and merely adds caching. */

class cache_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	inline virtual bool writable() const { return base->writable(); }
	virtual int insert(const dtype & key, const blob & blob);
	virtual int remove(const dtype & key);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = base->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	inline cache_dtable() : base(NULL), cache(10, blob_cmp, blob_cmp) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline virtual ~cache_dtable()
	{
		if(base)
			deinit();
	}
	
	inline virtual int maintain() { return base->maintain(); }
	
	DECLARE_WRAP_FACTORY(cache_dtable);
	
private:
	struct entry
	{
		blob value;
		bool found;
	};
	
	void add_cache(const dtype & key, const blob & value, bool found) const;
	
	typedef __gnu_cxx::hash_map<const dtype, entry, dtype_hashing_comparator, dtype_hashing_comparator> blob_map;
	
	dtable * base;
	size_t cache_size;
	mutable blob_map cache;
	mutable std::queue<dtype> order;
};

#endif /* __CACHE_DTABLE_H */
