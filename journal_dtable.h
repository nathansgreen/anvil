/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_DTABLE_H
#define __JOURNAL_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error journal_dtable.h is a C++ header file
#endif

#include <ext/hash_map>
#include <ext/pool_allocator.h>
#include "exception.h"
#include "avl/map.h"

#include "dtable.h"
#include "sys_journal.h"

/* The journal dtable doesn't have an associated file: all its data is stored in
 * a sys_journal. The only identifying part of journal dtables is their listener
 * ID, which must be chosen to be unique for each new journal dtable. */

class journal_dtable : public sys_journal::listening_dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	
	/* journal_dtable supports size() even though it is not otherwise indexable */
	inline virtual size_t size() const { return jdt_hash.size(); }
	inline virtual bool writable() const { return true; }
	virtual int insert(const dtype & key, const blob & blob, bool append = false, ATX_OPT);
	virtual int remove(const dtype & key, ATX_OPT);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		/* we merely add this assertion, but it's important */
		assert(jdt_hash.empty() || blob_cmp);
		return listening_dtable::set_blob_cmp(cmp);
	}
	
	/* for rollover */
	virtual int real_rollover(listening_dtable * target) const;
	inline virtual int accept(const dtype & key, const blob & value, bool append = false) { return set_node(key, value, append); }
	
	class journal_dtable_warehouse : public sys_journal::listening_dtable_warehouse_impl<journal_dtable>
	{
	protected:
		virtual journal_dtable * create(sys_journal::listener_id lid, const void * entry, size_t length, sys_journal * journal) const;
		virtual journal_dtable * create(sys_journal::listener_id lid, dtype::ctype key_type, sys_journal * journal) const;
	};
	
	static journal_dtable_warehouse warehouse;
	
	/* clear memory state, discard the current listener ID, set a new listener
	 * ID, and clear and release the blob comparator (if one has been set) */
	virtual int reinit(sys_journal::listener_id lid);
	
protected:
	/* journal_dtables should only be constructed by a journal_dtable_warehouse */
	inline journal_dtable() : initialized(false), jdt_map(blob_cmp), jdt_hash(10, blob_cmp, blob_cmp) {}
	int init(dtype::ctype key_type, sys_journal::listener_id lid, sys_journal * sysj);
	void deinit();
	inline virtual ~journal_dtable()
	{
		if(initialized)
			deinit();
	}
	
	static bool entry_key_type(const void * entry, size_t length, dtype::ctype * key_type);
	
	int log(const dtype & key, const blob & blob, bool append);
	
	typedef __gnu_cxx::__pool_alloc<std::pair<const dtype, blob *> > tree_pool_allocator;
	typedef __gnu_cxx::__pool_alloc<std::pair<const dtype, blob> > hash_pool_allocator;
	typedef avl::map<dtype, blob *, dtype_comparator_refobject, tree_pool_allocator> journal_dtable_map;
	typedef __gnu_cxx::hash_map<const dtype, blob, dtype_hashing_comparator, dtype_hashing_comparator, hash_pool_allocator> journal_dtable_hash;
	
	bool initialized;
	journal_dtable_map jdt_map;
	journal_dtable_hash jdt_hash;
	
private:
	class iter : public iter_source<journal_dtable>
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
		inline iter(const journal_dtable * source) : iter_source<journal_dtable>(source), jit(source->jdt_map.begin()) {}
		virtual ~iter() {}
	private:
		journal_dtable_map::const_iterator jit;
	};
	
	int log_blob_cmp();
	template<class T> inline int log(T * entry, const blob & blob, size_t offset = 0);
	int set_node(const dtype & key, const blob & value, bool append);
	
	virtual int journal_replay(void *& entry, size_t length);
};

#endif /* __JOURNAL_DTABLE_H */
