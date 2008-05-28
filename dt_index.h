/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DT_INDEX_H
#define __DT_INDEX_H

#ifndef __cplusplus
#error dt_index.h is a C++ header file
#endif

#include "dtable.h"

/* secondary indices */

class dt_index
{
public:
	class iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		virtual dtype key() const = 0;
		virtual dtype pri() const = 0;
		virtual ~iter() {}
	};
	
	virtual bool unique() const = 0;
	virtual bool writable() const = 0;
	
	/* only usable if unique() returns true */
	virtual dtype map(dtype key) const = 0;
	
	virtual iter * iterator() const = 0;
	virtual iter * iterator(dtype key) const = 0;
	
	/* only usable if writable() returns true */
	virtual int set(const dtype & key, const dtype & pri) = 0;
	virtual int remove(dtype key) = 0;
	
	/* only usable if unique() returns false and writable() returns true */
	virtual int add(dtype key, dtype pri) = 0;
	virtual int update(dtype key, dtype old_pri, dtype new_pri) = 0;
	virtual int remove(dtype key, dtype pri) = 0;
	
	virtual ~dt_index() {};
};

#endif /* __DT_INDEX_H */
