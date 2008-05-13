/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_H
#define __DTABLE_H

#include <errno.h>

#ifndef __cplusplus
#error dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtype.h"

/* data tables */

class dtable
{
public:
	class key_iter
	{
	public:
		virtual bool valid() const = 0;
		/* Since these iterators are virtual, we will have a pointer to them
		 * rather than an actual instance when we're using them. As a result, it
		 * is not as useful to override operators, because we'd have to
		 * dereference the local variable in order to use the overloaded
		 * operators. In particular we'd need ++*it instead of just ++it, yet
		 * both would compile without error. So, we use next() here. */
		virtual bool next() = 0;
		virtual dtype key() const = 0;
		virtual ~key_iter() {}
	};
	class iter : public key_iter
	{
	public:
		virtual metablob meta() const = 0;
		virtual blob value() const = 0;
		virtual const dtable * source() const = 0;
		virtual ~iter() {}
	};
	
	virtual iter * iterator() const = 0;
	virtual blob lookup(dtype key, const dtable ** source) const = 0;
	inline blob find(dtype key) const { const dtable * source; return lookup(key, &source); }
	inline virtual bool writable() const { return false; }
	inline virtual int append(dtype key, const blob & blob) { return -ENOSYS; }
	inline virtual int remove(dtype key) { return -ENOSYS; }
	inline dtype::ctype key_type() const { return ktype; }
	inline virtual ~dtable() {}
	
protected:
	dtype::ctype ktype;
};

/* the empty dtable is very simple, and is used for dtable_factory's default create() method */
class empty_dtable : public dtable
{
public:
	virtual iter * iterator() const { return new iter(); }
	inline virtual blob lookup(dtype key, const dtable ** source) const { return blob(); }
	inline empty_dtable(dtype::ctype key_type) { ktype = key_type; }
	inline virtual ~empty_dtable() {}
	
private:
	class iter : public dtable::iter
	{
	public:
		virtual bool valid() const { return false; }
		virtual bool next() { return false; }
		/* well, really we have nothing to return */
		virtual dtype key() const { return dtype(0u); }
		virtual metablob meta() const { return metablob(); }
		virtual blob value() const { return blob(); }
		virtual const dtable * source() const { return NULL; }
		virtual ~iter() {}
	};
};

/* override one or both create() methods in subclasses */
class dtable_factory
{
public:
	virtual dtable * open(int dfd, const char * name) const = 0;
	
	inline virtual int create(int dfd, const char * name, dtype::ctype key_type)
	{
		empty_dtable empty(key_type);
		return create(dfd, name, &empty, NULL);
	}
	
	/* non-existent entries in the source which are present in the shadow
	 * (as existent entries) will be kept as non-existent entries in the
	 * result, otherwise they will be omitted since they are not needed */
	/* shadow may also be NULL in which case it is treated as empty */
	inline virtual int create(int dfd, const char * name, const dtable * source, const dtable * shadow) const
	{
		return -ENOSYS;
	}
	
	inline virtual void retain(int count = 1)
	{
		assert(count > 0);
		ref_count += count;
	}
	
	inline virtual void release()
	{
		if(--ref_count <= 0)
			delete this;
	}
	
	dtable_factory() : ref_count(1) {}
	virtual ~dtable_factory() {}
	
private:
	int ref_count;
};

template<class T>
class dtable_static_factory : public dtable_factory
{
public:
	virtual dtable * open(int dfd, const char * name) const
	{
		T * disk = new T;
		int r = disk->init(dfd, name);
		if(r < 0)
		{
			delete disk;
			disk = NULL;
		}
		return disk;
	}
	
	virtual int create(int dfd, const char * name, const dtable * source, const dtable * shadow) const
	{
		return T::create(dfd, name, source, shadow);
	}
	
	/* these do not get freed; they are supposed to be statically allocated */
	virtual void retain(int count = 1)
	{
	}
	virtual void release()
	{
	}
};

#endif /* __DTABLE_H */
