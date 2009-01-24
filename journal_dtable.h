/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
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
#include <map>

#include "blob.h"
#include "istr.h"
#include "dtable.h"
#include "stringset.h"
#include "sys_journal.h"

/* The journal dtable doesn't have an associated file: all its data is stored in
 * a sys_journal. The only identifying part of journal dtables is their listener
 * ID, which must be chosen to be unique for each new journal dtable. */

class journal_dtable : public dtable, public sys_journal::journal_listener
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	
	/* journal_dtable supports size() even though it is not otherwise indexable */
	inline virtual size_t size() const { return jdt_hash.size(); }
	inline virtual bool writable() const { return true; }
	virtual int insert(const dtype & key, const blob & blob, bool append = false);
	virtual int remove(const dtype & key);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		/* we merely add this assertion, but it's important */
		assert(jdt_map.empty() || blob_cmp);
		return dtable::set_blob_cmp(cmp);
	}
	
	inline journal_dtable() : string_index(0), jdt_map(blob_cmp), jdt_hash(10, blob_cmp, blob_cmp) {}
	int init(dtype::ctype key_type, sys_journal::listener_id lid, bool always_append = false, sys_journal * journal = NULL);
	/* reinitialize, optionally discarding the old entries from the journal */
	/* NOTE: also clears and releases the blob comparator, if one has been set */
	int reinit(sys_journal::listener_id lid, bool discard = true);
	void deinit();
	inline virtual ~journal_dtable()
	{
		if(id() != sys_journal::NO_ID)
			deinit();
	}
	
	virtual int journal_replay(void *& entry, size_t length);
	
private:
	typedef std::map<dtype, blob, dtype_comparator_refobject> journal_dtable_map;
	typedef __gnu_cxx::hash_map<const dtype, blob *, dtype_hashing_comparator, dtype_hashing_comparator> journal_dtable_hash;
	
	inline int add_node(const dtype & key, const blob & value, bool append);
	
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
	
	int add_string(const istr & string, uint32_t * index);
	int log_blob_cmp();
	template<class T> inline int log(T * entry, const blob & blob, size_t offset = 0);
	int log(const dtype & key, const blob & blob, bool append);
	
	stringset strings;
	bool always_append;
	uint32_t string_index;
	journal_dtable_map jdt_map;
	journal_dtable_hash jdt_hash;
};

#endif /* __JOURNAL_DTABLE_H */
