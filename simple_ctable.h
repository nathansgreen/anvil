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

class simple_ctable : public ctable
{
public:
	virtual ctable_iter * iterator() const;
	virtual ctable_iter * iterator(dtype key) const;
	virtual blob find(dtype key, const char * column) const;
	
	inline virtual bool writable() const
	{
		return wdt_source ? wdt_source->writable() : false;
	}
	
	virtual int append(dtype key, const char * column, const blob & value);
	virtual int remove(dtype key, const char * column);
	inline virtual int remove(dtype key)
	{
		return wdt_source->remove(key);
	}
	
	inline int init(const dtable * source)
	{
		dt_source = source;
		wdt_source = NULL;
		return 0;
	}
	inline int init(dtable * source)
	{
		dt_source = source;
		wdt_source = source;
		return 0;
	}
	
	static ctable_static_factory<simple_ctable> factory;
	
	inline simple_ctable() : wdt_source(NULL) {}
	inline virtual ~simple_ctable() {}
	
private:
	class iter : public ctable_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual const char * column() const;
		virtual blob value() const;
		inline iter(dtable_iter * src);
		inline iter(const blob & value);
		virtual ~iter()
		{
			if(columns)
				delete columns;
			if(source)
				delete source;
		}
		
	private:
		dtable_iter * source;
		/* need to keep the row around so its iterator will work */
		sub_blob row;
		sub_blob_iter * columns;
	};
	
	dtable * wdt_source;
};

#endif /* __SIMPLE_CTABLE_H */
