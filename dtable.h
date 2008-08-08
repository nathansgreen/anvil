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
		virtual bool prev() = 0;
		virtual bool last() = 0;
		virtual dtype key() const = 0;
		/* Seeks this iterator to the requested key, or the next key if the
		 * requested key is not present. Returns true if the requested key was
		 * found, and false otherwise. */
		virtual bool seek(const dtype & key) = 0;
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
	virtual blob lookup(const dtype & key, const dtable ** source) const = 0;
	inline blob find(const dtype & key) const { const dtable * source; return lookup(key, &source); }
	inline virtual bool writable() const { return false; }
	inline virtual int append(const dtype & key, const blob & blob) { return -ENOSYS; }
	inline virtual int remove(const dtype & key) { return -ENOSYS; }
	inline dtype::ctype key_type() const { return ktype; }
	inline virtual ~dtable() {}
	
	/* maintenance callback; does nothing by default */
	inline virtual int maintain() { return 0; }
	
protected:
	dtype::ctype ktype;
};

#endif /* __DTABLE_H */
