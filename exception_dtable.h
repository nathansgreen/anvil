/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __EXCEPTION_DTABLE_H
#define __EXCEPTION_DTABLE_H

#ifndef __cplusplus
#error exception_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"

/* exception tables */

class exception_dtable : public dtable
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
			value = alt->set_blob_cmp(cmp);
			assert(value >= 0);
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(exception_dtable);
	
	inline exception_dtable() : base(NULL), alt(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~exception_dtable()
	{
		if(base || alt)
			deinit();
	}
	
private:
	class iter : public iter_source<exception_dtable>
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
		inline iter(const exception_dtable * source);
		virtual ~iter();
		
	private:
		struct sub
		{
			dtable::iter * iter;
			bool valid;
		};
		
		sub * base_sub;
		sub * alt_sub;
		sub * current_sub;
		enum direction {FORWARD, BACKWARD} lastdir;
	};
	
	/* we'll define it in the source file */
	class reject_iter;
	
	dtable * base;
	dtable * alt;
	blob reject_value;
};

#endif /* __EXCEPTION_DTABLE_H */
