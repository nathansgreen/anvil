/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_CTABLE_H
#define __SIMPLE_CTABLE_H

#ifndef __cplusplus
#error simple_ctable.h is a C++ header file
#endif

#include "ctable.h"
#include "sub_blob.h"
#include "ctable_factory.h"

class simple_ctable : public ctable
{
public:
	virtual dtable::key_iter * keys() const;
	virtual iter * iterator() const;
	virtual iter * iterator(const dtype & key) const;
	virtual blob find(const dtype & key, const istr & column) const;
	virtual bool contains(const dtype & key) const;
	
	inline virtual bool writable() const
	{
		return wdt_source ? wdt_source->writable() : false;
	}
	
	virtual int insert(const dtype & key, const istr & column, const blob & value, bool append = false);
	virtual int remove(const dtype & key, const istr & column);
	inline virtual int remove(const dtype & key)
	{
		return wdt_source->remove(key);
	}
	
	inline int init(const dtable * source, const params & config = params())
	{
		dt_source = source;
		wdt_source = NULL;
		return 0;
	}
	inline int init(dtable * source, const params & config = params())
	{
		dt_source = source;
		wdt_source = source;
		return 0;
	}
	
	DECLARE_CT_FACTORY(simple_ctable);
	
	inline simple_ctable() : wdt_source(NULL) {}
	inline virtual ~simple_ctable() {}
	
	virtual int maintain()
	{
		return wdt_source ? wdt_source->maintain() : 0;
	}
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		return wdt_source ? wdt_source->set_blob_cmp(cmp) : -EINVAL;
	}
	
private:
	class iter : public ctable::iter
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
		virtual const istr & column() const;
		virtual blob value() const;
		inline iter(dtable::iter * src);
		inline iter(const blob & value);
		virtual ~iter()
		{
			if(columns)
				delete columns;
			if(source)
				delete source;
		}
		
	private:
		inline void advance();
		
		dtable::iter * source;
		/* need to keep the row around so its iterator will work */
		sub_blob row;
		sub_blob::iter * columns;
	};
	
	dtable * wdt_source;
};

#endif /* __SIMPLE_CTABLE_H */
