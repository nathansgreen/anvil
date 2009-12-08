/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __UNIQ_DTABLE_H
#define __UNIQ_DTABLE_H

#ifndef __cplusplus
#error uniq_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* TODO: if the value base supports indexed access, use that instead of the
 * integer keys we store in it. For some underlying dtable types that may be
 * much faster, although really the user should just use one where it's the same
 * anyway (e.g. linear dtable or array dtable). */

class uniq_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	virtual size_t size() const;
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = keybase->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	/* uniq_dtable supports indexed access if its keybase does */
	static bool static_indexed_access(const params & config);
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(uniq_dtable);
	
	inline uniq_dtable() : keybase(NULL), valuebase(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~uniq_dtable()
	{
		if(keybase)
			deinit();
	}
	
private:
	class iter : public iter_source<uniq_dtable, dtable_wrap_iter>
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		inline iter(dtable::iter * base, const uniq_dtable * source);
		virtual ~iter() {}
	};
	
	dtable * keybase;
	dtable * valuebase;
};

#endif /* __UNIQ_DTABLE_H */
