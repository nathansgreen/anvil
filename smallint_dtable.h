/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SMALLINT_DTABLE_H
#define __SMALLINT_DTABLE_H

#ifndef __cplusplus
#error smallint_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* The smallint dtable stores integers as 8, 16, or 24 bits (depending on
 * configuration). 32-bit integers which do not fit are rejected. */

class smallint_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	virtual size_t size() const;
	/* writable, insert, remove? */
	
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
	
	/* smallint_dtable supports indexed access if its base does */
	static bool static_indexed_access(const params & config);
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(smallint_dtable);
	
	inline smallint_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~smallint_dtable()
	{
		if(base)
			deinit();
	}
	
private:
	class iter : public iter_source<smallint_dtable, dtable_wrap_iter>
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		inline iter(dtable::iter * base, const smallint_dtable * source);
		virtual ~iter() {}
	};
	
	/* used in create() to wrap source iterators on the way down */
	class rev_iter : public dtable_wrap_iter
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		virtual bool reject(blob * replacement);
		inline rev_iter(dtable::iter * base, size_t byte_count);
		virtual ~rev_iter() {}
		mutable bool failed;
	private:
		size_t byte_count;
	};
	
	static blob unpack(blob packed, size_t byte_count);
	static bool pack(blob * unpacked, size_t byte_count);
	
	dtable * base;
	size_t byte_count;
};

#endif /* __SMALLINT_DTABLE_H */
