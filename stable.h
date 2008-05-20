/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STABLE_H
#define __STABLE_H

#ifndef __cplusplus
#error stable.h is a C++ header file
#endif

#include "dtype.h"
#include "dtable.h"

/* schema tables, like typed ctables (similar to old gtable) */

class stable
{
public:
	/* iterate through the columns of an stable */
	class column_iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		virtual const char * name() const = 0;
		virtual size_t row_count() const = 0;
		virtual dtype::ctype type() const = 0;
		virtual ~column_iter() {}
	};
	
	/* iterate through the actual data */
	class iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		/* can't call key() if you got this iterator via iterator(key) */
		virtual dtype key() const = 0;
		virtual const char * column() const = 0;
		virtual dtype value() const = 0;
		virtual ~iter() {}
	};
	
	virtual column_iter * columns() const = 0;
	virtual size_t column_count() const = 0;
	virtual size_t row_count(const char * column) const = 0;
	virtual dtype::ctype column_type(const char * column) const = 0;
	
	virtual dtable::key_iter * keys() const = 0;
	virtual iter * iterator() const = 0;
	virtual iter * iterator(dtype key) const = 0;
	
	/* returns true if found, otherwise does not change *value */
	virtual bool find(dtype key, const char * column, dtype * value) const = 0;
	
	virtual bool writable() const = 0;
	
	virtual int append(dtype key, const char * column, const dtype & value) = 0;
	/* remove just a column */
	virtual int remove(dtype key, const char * column) = 0;
	/* remove the whole row */
	virtual int remove(dtype key) = 0;
	
	/* maintenance callback; does nothing by default */
	inline virtual int maintain() { return 0; }
	
	virtual dtype::ctype key_type() const = 0;
	inline virtual ~stable() {}
};

#endif /* __STABLE_H */
