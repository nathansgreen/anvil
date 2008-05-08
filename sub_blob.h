/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SUB_BLOB_H
#define __SUB_BLOB_H

#include <string.h>

#ifndef __cplusplus
#error sub_blob.h is a C++ header file
#endif

#include "blob.h"


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
		/* returned string is good at least until the next call to next() */
		virtual const char * column() const = 0;
		virtual blob value() const = 0;
		virtual ~iter() {}
	};
	
	inline sub_blob() : modified(false), overrides(NULL) {}
	inline sub_blob(const blob & x) : base(x), modified(false), overrides(NULL) {}
	
	blob get(const char * column) const;
	int set(const char * column, const blob & value);
	int remove(const char * column);
	blob flatten(bool internalize = true);
	/* it is undefined what happens if you call flatten() while
	 * iterating, but get(), set(), and remove() should all work */
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
		const char * name;
		blob value;
		override ** prev;
		override * next;
		
		inline override(const char * column, const blob & x, override ** first = NULL)
			: name(strdup(column)), value(x)
		{
			if(first)
			{
				prev = first;
				next = *first;
				*first = this;
			}
			else
			{
				prev = NULL;
				next = NULL;
			}
		}
		
		inline ~override()
		{
			if(prev)
				*prev = next;
			free((void *) name);
		}
	};
	mutable override * overrides;
	
	class named_iter : public iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual const char * column() const;
		virtual blob value() const;
		inline named_iter(override * first) : current(first) {}
		virtual ~named_iter() {}
		
	private:
		const override * current;
	};
	
	override * find(const char * column) const;
	blob extract(const char * column) const;
	/* populate the override list with the current values */
	void populate() const;
};

#endif /* __SUB_BLOB_H */
