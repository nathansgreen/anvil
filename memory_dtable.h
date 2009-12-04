/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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
#include <ext/pool_allocator.h>
#include "exception.h"
#include "avl/map.h"

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
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	
	inline virtual bool writable() const { return true; }
	virtual int insert(const dtype & key, const blob & blob, bool append = false, ATX_OPT);
	virtual int remove(const dtype & key, ATX_OPT);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		/* we merely add this assertion, but it's important */
		assert(mdt_map.empty() || blob_cmp);
		return dtable::set_blob_cmp(cmp);
	}
	
	inline memory_dtable() : ready(false), always_append(false), full_remove(false), mdt_map(blob_cmp), mdt_hash(10, blob_cmp, blob_cmp) {}
	int init(dtype::ctype key_type, bool always_append = false, bool full_remove = false);
	inline void reinit()
	{
		mdt_hash.clear();
		mdt_map.clear();
	}
	/* memory_dtable has a public destructor (and no factory) */
	inline virtual ~memory_dtable()
	{
		if(ready)
			deinit();
	}
	
protected:
	void deinit();
	
private:
	typedef __gnu_cxx::__pool_alloc<std::pair<const dtype, blob> > tree_pool_allocator;
	typedef __gnu_cxx::__pool_alloc<std::pair<const dtype, blob *> > hash_pool_allocator;
	typedef avl::map<dtype, blob, dtype_comparator_refobject, tree_pool_allocator> memory_dtable_map;
	typedef __gnu_cxx::hash_map<const dtype, blob *, dtype_hashing_comparator, dtype_hashing_comparator, hash_pool_allocator> memory_dtable_hash;
	
	inline int add_node(const dtype & key, const blob & value, bool append);
	/* tries to set an existing node, and calls add_node() otherwise */
	int set_node(const dtype & key, const blob & value, bool append);
	
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
	
	bool ready, always_append, full_remove;
	memory_dtable_map mdt_map;
	memory_dtable_hash mdt_hash;
};

#endif /* __MEMORY_DTABLE_H */
