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

/* column tables */

class ctable
{
public:
	class iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		/* can't call key() or seek() if you got this iterator via iterator(key) */
		virtual dtype key() const = 0;
		virtual bool seek(const dtype & key) = 0;
		virtual const istr & column() const = 0;
		virtual blob value() const = 0;
		virtual ~iter() {}
	};
	
	virtual dtable::key_iter * keys() const = 0;
	virtual iter * iterator() const = 0;
	virtual iter * iterator(const dtype & key) const = 0;
	virtual blob find(const dtype & key, const istr & column) const = 0;
	virtual bool contains(const dtype & key) const = 0;
	virtual bool writable() const = 0;
	virtual int append(const dtype & key, const istr & column, const blob & value) = 0;
	/* remove just a column */
	virtual int remove(const dtype & key, const istr & column) = 0;
	/* remove the whole row */
	virtual int remove(const dtype & key) = 0;
	inline dtype::ctype key_type() const { return dt_source->key_type(); }
	inline ctable() : dt_source(NULL) {}
	inline virtual ~ctable() {}
	
	/* maintenance callback; does nothing by default */
	inline virtual int maintain() { return 0; }
	
protected:
	const dtable * dt_source;
};

#endif /* __CTABLE_H */
