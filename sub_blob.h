/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SUB_BLOB_H
#define __SUB_BLOB_H

#ifndef __cplusplus
#error sub_blob.h is a C++ header file
#endif

#include "blob.h"
#include "istr.h"

class sub_blob
{
public:
	/* sub_blob isn't really meant to be an abstract base class (and it isn't), so
	 * its iterator doesn't have to be all virtual like this... but the other
	 * iterators are all done this way, so we'll be consistent here */
	class iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		virtual const istr & column() const = 0;
		virtual blob value() const = 0;
		virtual ~iter() {}
	};
	
	inline sub_blob() : modified(false), overrides(NULL) {}
	inline sub_blob(const blob & x) : base(x), modified(false), overrides(NULL) {}
	inline sub_blob(const sub_blob & x) : base(x.flatten()), modified(false), overrides(NULL) {}
	inline sub_blob & operator=(const sub_blob & x)
	{
		if(this == &x)
			return *this;
		while(overrides)
			delete overrides;
		base = x.flatten();
		modified = false;
		return *this;
	}
	
	blob get(const istr & column) const;
	int set(const istr & column, const blob & value);
	int remove(const istr & column);
	/* defaults to internalizing the flattened
	 * blob, which invalidates any iterators */
	blob flatten(bool internalize = true);
	/* the const version never internalizes the flattened blob */
	blob flatten() const;
	/* see above about calling flatten() while iterating,
	 * but get(), set(), and remove() should all work */
	iter * iterator() const;
	
	inline ~sub_blob()
	{
		while(overrides)
			delete overrides;
	}
	
private:
	blob base;
	bool modified;
	
	struct override
	{
		istr name;
		blob value;
		override ** prev;
		override * next;
		
		inline override(const istr & column, const blob & x, override ** first)
			: name(column), value(x)
		{
			prev = first;
			next = *first;
			*first = this;
			if(next)
				next->prev = &next;
		}
		
		inline ~override()
		{
			*prev = next;
			if(next)
				next->prev = prev;
		}
	};
	mutable override * overrides;
	
	class named_iter : public iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual const istr & column() const;
		virtual blob value() const;
		inline named_iter(override * first) : current(first) {}
		virtual ~named_iter() {}
		
	private:
		const override * current;
	};
	
	override * find(const istr & column) const;
	blob extract(const istr & column) const;
	/* populate the override list with the current values */
	void populate() const;
};

#endif /* __SUB_BLOB_H */
