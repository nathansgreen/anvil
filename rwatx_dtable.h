/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __RWATX_DTABLE_H
#define __RWATX_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error rwatx_dtable.h is a C++ header file
#endif

#include <ext/hash_map>
#include <ext/hash_set>

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* This dtable implements read-write aborting transactions. It builds on the
 * abortable transaction mechanism, tracking (via implicit locking) which
 * abortable transactions have read and written what values, and forces
 * transactions to abort when they conflict. Using this module, Anvil provides
 * full ACID transactions. Note, however, that since transactions are aborted at
 * the first sign of a conflict (rather than blocking and aborting only on
 * circular wait), the famous Ethernet 1/e utilization effect applies. (Well,
 * except it is probably much worse due to the way read-write locks work.) */

class rwatx_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	inline virtual bool writable() const { return base->writable(); }
	virtual int insert(const dtype & key, const blob & blob, bool append = false, ATX_OPT);
	virtual int remove(const dtype & key, ATX_OPT);
	
	inline virtual abortable_tx create_tx();
	inline virtual int check_tx(ATX_REQ) const;
	inline virtual int commit_tx(ATX_REQ);
	inline virtual void abort_tx(ATX_REQ);
	
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
	
	inline virtual int maintain(bool force = false) { return base->maintain(force); }
	
	DECLARE_WRAP_FACTORY(rwatx_dtable);
	
	inline rwatx_dtable() : base(NULL), keys(10, blob_cmp, blob_cmp), chain(this) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~rwatx_dtable()
	{
		if(base)
			deinit();
	}
	
private:
	class iter : public iter_source<rwatx_dtable, dtable_wrap_iter>
	{
	public:
		/* It's not totally clear what calls count as "reading" the keys pointed
		 * to by the iterator. For instance, if we seek to a key, the return value
		 * indicates whether that key was found or not, but is that really the
		 * same as calling both key() and value() when pointing at that key? To
		 * be safe, however, we err on the side of caution: pretty much anything
		 * that mentions the key or value counts as a read to us. Maybe we could
		 * do better in the future with more careful analysis of the semantics. */
		virtual dtype key() const { dtype k = base->key(); dt_source->note_read(k, atx); return k; }
		virtual bool seek(const dtype & key) { dt_source->note_read(key, atx); return base->seek(key); }
		virtual bool seek(const dtype_test & test)
		{
			/* this could actually be even more paranoid by interposing on the test */
			bool found = base->seek(test);
			if(found)
				dt_source->note_read(base->key(), atx);
			return found;
		}
		virtual metablob meta() const { dt_source->note_read(base->key(), atx); return base->meta(); }
		virtual blob value() const { dt_source->note_read(base->key(), atx); return base->value(); }
		inline iter(dtable::iter * base, const rwatx_dtable * source, ATX_DEF)
			: iter_source<rwatx_dtable, dtable_wrap_iter>(base, source), atx(atx)
		{
			claim_base = true;
		}
		virtual ~iter() {}
	private:
		abortable_tx atx;
	};
	
	struct key_status
	{
		__gnu_cxx::hash_set<abortable_tx> readers;
		abortable_tx writer;
		bool write_lock;
		inline key_status() : writer(NO_ABORTABLE_TX), write_lock(false) {}
		inline key_status(abortable_tx atx) : writer(atx), write_lock(true) {}
	};
	typedef __gnu_cxx::hash_map<dtype, key_status, dtype_hashing_comparator, dtype_hashing_comparator> key_status_map;
	
	typedef __gnu_cxx::hash_set<dtype, dtype_hashing_comparator, dtype_hashing_comparator> key_set;
	struct atx_status
	{
		mutable key_set reads;
		key_set writes;
		mutable bool aborted;
		inline atx_status(const blob_comparator * const & blob_cmp)
			: reads(10, blob_cmp, blob_cmp), writes(10, blob_cmp, blob_cmp), aborted(false) {}
	};
	typedef __gnu_cxx::hash_map<abortable_tx, atx_status> atx_status_map;
	
	/* these return false on failure, e.g. if a conflict is detected */
	bool note_read(const dtype & key, ATX_REQ) const;
	bool note_write(const dtype & key, ATX_REQ);
	
	/* helper for commit_tx() and abort_tx() */
	void remove_tx(const atx_status_map::iterator & it);
	
	dtable * base;
	mutable key_status_map keys;
	atx_status_map rwatx;
	/* used for iterator requests that aren't part of an abortable transaction */
	mutable chain_callback chain;
};

#endif /* __RWATX_DTABLE_H */
