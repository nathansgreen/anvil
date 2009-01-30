/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MEMORY_DTABLE_H
#define __MEMORY_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error memory_dtable.h is a C++ header file
#endif

#include <ext/hash_map>
#include "avl/map.h"

#include "blob.h"
#include "istr.h"
#include "dtable.h"

/* The memory dtable stores data in memory, like the journal dtable, but does
 * not actually log anything to the journal or write a file itself. When it is
 * destroyed, the data goes with it. */
/* Depending on how it is used, it may be preferable either to completely remove
 * removed keys, or to store nonexistent blobs for those keys (as the journal
 * dtable does). This is controlled by the "full_remove" init() parameter. */

class memory_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	
	inline virtual bool writable() const { return true; }
	virtual int insert(const dtype & key, const blob & blob, bool append = false);
	virtual int remove(const dtype & key);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		/* we merely add this assertion, but it's important */
		assert(mdt_map.empty() || blob_cmp);
		return dtable::set_blob_cmp(cmp);
	}
	
	inline memory_dtable() : ready(false), full_rm(false), mdt_map(blob_cmp), mdt_hash(10, blob_cmp, blob_cmp) {}
	int init(dtype::ctype key_type, bool full_remove);
	void deinit();
	inline virtual ~memory_dtable()
	{
		if(ready)
			deinit();
	}
	
private:
	typedef avl::map<dtype, blob, dtype_comparator_refobject> memory_dtable_map;
	typedef __gnu_cxx::hash_map<const dtype, blob *, dtype_hashing_comparator, dtype_hashing_comparator> memory_dtable_hash;
	
	inline int add_node(const dtype & key, const blob & value);
	inline int add_node(const dtype & key, const blob & value, const memory_dtable_map::iterator & end);
	
	class iter : public iter_source<memory_dtable>
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool first();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const memory_dtable * source) : iter_source<memory_dtable>(source), mit(source->mdt_map.begin()) {}
		virtual ~iter() {}
	private:
		memory_dtable_map::const_iterator mit;
	};
	
	bool ready, full_rm;
	memory_dtable_map mdt_map;
	memory_dtable_hash mdt_hash;
};

#endif /* __MEMORY_DTABLE_H */
