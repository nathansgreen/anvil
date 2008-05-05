/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_H
#define __CTABLE_H

#ifndef __cplusplus
#error ctable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"

class ctable_iter
{
public:
	virtual bool valid() const = 0;
	/* see the note about dtable_iter in dtable.h */
	virtual bool next() = 0;
	virtual dtype key() const = 0;
	virtual const char * column() const = 0;
	virtual blob value() const = 0;
	virtual ~ctable_iter() {}
};

/* column tables */

class ctable
{
public:
	virtual ctable_iter * iterator() const = 0;
	virtual blob find(dtype key, const char * column) const = 0;
	inline dtype::ctype key_type() const { return dt_source->key_type(); }
	inline ctable() : dt_source(NULL) {}
	inline virtual ~ctable() {}
	
protected:
	const dtable * dt_source;
};

class writable_ctable : virtual public ctable
{
public:
	virtual int append(dtype key, const char * column, const blob & value) = 0;
	/* remove just a column */
	/* if gc_row is set, then remove the row if this was the only column left */
	virtual int remove(dtype key, const char * column, bool gc_row = false) = 0;
	/* remove the whole row */
	virtual int remove(dtype key) = 0;
	inline writable_ctable() : wdt_source(NULL) {}
	inline virtual ~writable_ctable() {}

protected:
	writable_dtable * wdt_source;
};

#endif /* __CTABLE_H */
