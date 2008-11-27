/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BTREE_DTABLE_H
#define __BTREE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error btree_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "dtable_factory.h"

/* The btree dtable must be created with another read-only dtable, and builds a
 * btree key index for it. The base dtable must support indexed access. */

class btree_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	virtual blob index(size_t index) const;
	virtual size_t size() const;
	
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
	
	inline btree_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline virtual ~btree_dtable()
	{
		if(base)
			deinit();
	}
	
	static inline bool static_indexed_access() { return true; }
	
	static int create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow = NULL);
	DECLARE_RO_FACTORY(btree_dtable);
	
private:
	class iter : public dtable::iter
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
		virtual bool seek(size_t index);
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(dtable::iter * base, const btree_dtable * source);
		virtual ~iter() { }
		
	private:
		dtable::iter * base_iter;
		const btree_dtable * bdt_source;
	};
	
	dtable * base;
};

#endif /* __BTREE_DTABLE_H */
