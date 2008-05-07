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
	/* can't call key() if you got this iterator via iterator(key) */
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
	virtual ctable_iter * iterator(dtype key) const = 0;
	virtual blob find(dtype key, const char * column) const = 0;
	virtual bool writable() const = 0;
	virtual int append(dtype key, const char * column, const blob & value) = 0;
	/* remove just a column */
	virtual int remove(dtype key, const char * column) = 0;
	/* remove the whole row */
	virtual int remove(dtype key) = 0;
	inline dtype::ctype key_type() const { return dt_source->key_type(); }
	inline ctable() : dt_source(NULL) {}
	inline virtual ~ctable() {}
	
protected:
	const dtable * dt_source;
};

/* although ctable itself does not suggest that it be implemented on top of dtables,
 * ctable_factory basically does require that for any ctables built via factories */
class ctable_factory
{
public:
	//virtual ctable * open(int dfd, const char * name) const = 0;
	virtual ctable * open(const dtable * dt_source) const = 0;
	virtual ctable * open(dtable * dt_source) const = 0;
	
	inline virtual void retain()
	{
		ref_count++;
	}
	
	inline virtual void release()
	{
		if(--ref_count <= 0)
			delete this;
	}
	
	ctable_factory() : ref_count(1) {}
	virtual ~ctable_factory() {}
	
private:
	int ref_count;
};

template<class T>
class ctable_static_factory : public ctable_factory
{
public:
	virtual ctable * open(const dtable * dt_source) const
	{
		T * table = new T;
		int r = table->init(dt_source);
		if(r < 0)
		{
			delete table;
			table = NULL;
		}
		return table;
	}
	
	virtual ctable * open(dtable * dt_source) const
	{
		T * table = new T;
		int r = table->init(dt_source);
		if(r < 0)
		{
			delete table;
			table = NULL;
		}
		return table;
	}
	
	/* these do not get freed; they are supposed to be statically allocated */
	virtual void retain()
	{
	}
	virtual void release()
	{
	}
};

#endif /* __CTABLE_H */
