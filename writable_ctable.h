/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __WRITABLE_CTABLE_H
#define __WRITABLE_CTABLE_H

#ifndef __cplusplus
#error writable_ctable.h is a C++ header file
#endif

#include "ctable.h"
#include "sub_blob.h"
#include "simple_ctable.h"
#include "writable_dtable.h"
#include "managed_dtable.h"

template<class T>
class writable_ctable : public T
{
public:
	/* notice that this is not virtual; care should be taken
	 * not to call the base ctable's version of init() */
	inline int init(writable_dtable * source)
	{
		this->dt_source = source;
		wdt_source = source;
		return 0;
	}
	
	/* if we implemented our own find(), this could avoid flattening every time */
	int append(dtype key, const char * column, const blob & value)
	{
		int r = 0;
		blob row = wdt_source->find(key);
		if(!row.negative() || !value.negative())
		{
			sub_blob columns(row);
			columns.set(column, value);
			r = wdt_source->append(key, columns.flatten());
		}
		return r;
	}
	
	/* remove just a column */
	/* if gc_row is set, then remove the row if this was the only column left */
	int remove(dtype key, const char * column, bool gc_row = false)
	{
		int r = append(key, column, blob());
		if(r >= 0 && gc_row)
		{
			/* TODO: improve this... */
			blob row = wdt_source->find(key);
			sub_blob columns(row);
			sub_blob_iter * iter = columns.iterator();
			bool last = !iter->valid();
			delete iter;
			if(last)
				remove(key);
		}
		return r;
	}
	
	/* remove the whole row */
	inline int remove(dtype key)
	{
		return wdt_source->remove(key);
	}
	
private:
	/* we could also just always cast dt_source... */
	writable_dtable * wdt_source;
};

typedef writable_ctable<simple_ctable> writable_simple_ctable;

#endif /* __WRITABLE_CTABLE_H */
