/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ISTR_H
#define __ISTR_H

#include <string.h>
#include <stdlib.h>

#ifndef __cplusplus
#error istr.h is a C++ header file
#endif

#include "blob.h"

/* This class is meant to replace uses of "const char *", but nothing more. In
 * particular, it is not a generic string class supporting a full library of
 * string manipulations, or even comparisons. The only thing it does is
 * reference counting and automatic deallocation of immutable strings. It can be
 * used almost anywhere a const char * can, with the notable exception of
 * unions. (Also, it must be explicitly cast to const char *, or have str()
 * called, to pass through ... as in printf().) */

class istr
{
public:
	inline istr(const char * x = NULL)
		: shared(NULL)
	{
		*this = x;
	}
	
	inline istr(const istr & x)
	{
		shared = x.shared;
		if(shared)
			shared->count++;
	}
	
	inline istr(const blob & x)
	{
		/* doing a little memory trick so can't just use new */
		shared = (share *) malloc(sizeof(*shared) + x.size() + 1);
		shared->count = 1;
		if(x.size())
			strncpy(shared->string, &x.index<char>(0), x.size());
		shared->string[x.size()] = 0;
	}
	
	inline istr & operator=(const istr & x)
	{
		if(this == &x)
			return *this;
		if(shared && --shared->count <= 0)
			free(shared);
		shared = x.shared;
		if(shared)
			shared->count++;
		return *this;
	}
	
	inline istr & operator=(const char * x)
	{
		if(shared && --shared->count <= 0)
			free(shared);
		if(x)
		{
			/* doing a little memory trick so can't just use new */
			shared = (share *) malloc(sizeof(*shared) + strlen(x) + 1);
			shared->count = 1;
			strcpy(shared->string, x);
		}
		else
			shared = NULL;
		return *this;
	}
	
	inline const char * str() const
	{
		return shared ? shared->string : NULL;
	}
	
	/* this precludes the easy addition of things like comparison operators, but that's OK */
	inline operator const char * () const
	{
		return str();
	}
	
	inline ~istr()
	{
		if(shared && --shared->count <= 0)
			free(shared);
	}
	
private:
	struct share
	{
		size_t count;
		char string[0];
	};
	
	share * shared;
};

#endif /* __ISTR_H */
