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
	inline sub_blob() : modified(false), overrides(NULL) {}
	inline sub_blob(const blob & x) : base(x), modified(false), overrides(NULL) {}
	
	blob get(const char * column);
	int set(const char * column, const blob & value);
	int remove(const char * column);
	blob flatten(bool internalize = true);
	
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
	} * overrides;
	
	override * find(const char * column);
	blob extract(const char * column);
	/* populate the override list with the current values */
	void populate();
};

#endif /* __SUB_BLOB_H */
