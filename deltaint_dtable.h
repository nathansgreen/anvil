/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DELTAINT_DTABLE_H
#define __DELTAINT_DTABLE_H

#ifndef __cplusplus
#error deltaint_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* The deltaint dtable stores 32-bit integers as deltas from the previous value,
 * and keeps a secondary dtable storing periodic reference values to aid in
 * random lookup and seeking. */

class deltaint_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	
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
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(deltaint_dtable);
	
	inline deltaint_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~deltaint_dtable()
	{
		if(base)
			deinit();
	}
	
private:
	class iter : public iter_source<deltaint_dtable, dtable_wrap_iter_noindex>
	{
	public:
		virtual bool next();
		virtual bool prev();
		virtual bool first();
		virtual bool last();
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual metablob meta() const;
		virtual blob value() const;
		inline iter(dtable::iter * base, const deltaint_dtable * source);
		virtual ~iter() {}
	private:
		uint32_t current;
		bool exists;
	};
	
	/* used in create() to wrap source iterators on the way down */
	class rev_iter_delta : public dtable_wrap_iter_noindex
	{
	public:
		virtual bool next();
		virtual bool prev() { abort(); }
		virtual bool first();
		virtual bool last() { abort(); }
		virtual bool seek(const dtype & key) { abort(); }
		virtual bool seek(const dtype_test & test) { abort(); }
		virtual metablob meta() const;
		virtual blob value() const;
		/* we can't tolerate any rejection here */
		virtual bool reject(blob * replacement) { return false; }
		inline rev_iter_delta(dtable::iter * base);
		virtual ~rev_iter_delta() {}
		mutable bool failed;
	private:
		bool reject_check(blob * value);
		uint32_t delta, previous;
		bool exists;
	};
	class rev_iter_ref : public dtable_wrap_iter_noindex
	{
	public:
		virtual bool next();
		virtual bool prev() { abort(); }
		virtual bool first();
		virtual bool last() { abort(); }
		virtual bool seek(const dtype & key) { abort(); }
		virtual bool seek(const dtype_test & test) { abort(); }
		/* we can actually tolerate any rejection at all here */
		virtual bool reject(blob * replacement);
		inline rev_iter_ref(dtable::iter * base, size_t skip);
		virtual ~rev_iter_ref() {}
	private:
		size_t skip;
	};
	
	dtable * base;
	dtable * reference;
	/* we use this iterator in lookup() to scan the base to compute
	 * the values for keys between reference keys */
	dtable::iter * scan_iter;
	/* we only need this iterator because dtable does not provide an
	 * iter::seek()-like way to find the next key that does exist */
	dtable::iter * ref_iter;
};

#endif /* __DELTAINT_DTABLE_H */
