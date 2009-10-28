/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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

#include <vector>

/* This class is meant to replace uses of "const char *", but nothing more. In
 * particular, it is not a generic string class supporting a full library of
 * string manipulations, or even comparisons. The only thing it does is
 * reference counting and automatic deallocation of immutable strings. It can be
 * used almost anywhere a const char * can, with the notable exception of
 * unions. (Also, it must be explicitly cast to const char *, or have str()
 * called, to pass through ... as in printf().) */

/* Well, OK, we'll do one other thing here. We'll support concatenation with the
 * + operator, since you'd never use that on const char * anyway. But that's it. */

class istr
{
public:
	/* the null istr, for returning as an error from methods that return istr & */
	static const istr null;
	
	inline istr(const char * x = NULL)
		: shared(NULL)
	{
		*this = x;
	}
	
	inline istr(const char * x, size_t length)
	{
		shared = share::alloc(length);
		shared->count = 1;
		if(length)
			strncpy(shared->string, x, length);
		shared->string[length] = 0;
	}
	
	/* concatenate the strings together */
	inline istr(const char * x, const char * y, const char * z = NULL)
	{
		size_t length = strlen(x) + strlen(y) + (z ? strlen(z) : 0);
		shared = share::alloc(length);
		shared->count = 1;
		strcpy(shared->string, x);
		strcat(shared->string, y);
		if(z)
			strcat(shared->string, z);
		assert(strlen(shared->string) == length);
	}
	
	inline istr(const istr & x)
	{
		shared = x.shared;
		if(shared)
			shared->count++;
	}
	
	inline istr(const blob & x)
	{
		shared = share::alloc(x.size());
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
			shared = share::alloc(strlen(x));
			shared->count = 1;
			strcpy(shared->string, x);
		}
		else
			shared = NULL;
		return *this;
	}
	
	inline istr operator+(const char * x) const
	{
		if(!shared || !x)
			/* this return here is why we don't just call this below */
			return shared ? *this : istr(x);
		size_t length = strlen(shared->string) + strlen(x);
		istr result(shared->string, length);
		strcat(result.shared->string, x);
		return result;
	}
	
	inline istr operator+(const istr & x) const
	{
		if(!shared || !x.shared)
			return shared ? *this : x;
		size_t length = strlen(shared->string) + strlen(x.shared->string);
		istr result(shared->string, length);
		strcat(result.shared->string, x.shared->string);
		return result;
	}
	
	inline istr & operator+=(const istr & x)
	{
		istr result = *this + x;
		*this = result;
		return *this;
	}
	
	inline size_t length() const
	{
		return shared ? strlen(shared->string) : 0;
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
	
	static inline ssize_t locate(const istr * array, size_t size, const istr & key)
	{
		return locate_generic(array, size, key);
	}
	static inline ssize_t locate(const std::vector<istr> & array, const istr & key)
	{
		/* must explicitly parameterize locate_generic;
		 * otherwise we try to pass the vector by value */
		return locate_generic<const std::vector<istr> &>(array, array.size(), key);
	}
	
private:
	struct share
	{
		size_t count;
		char string[0];
		static share * alloc(size_t length)
		{
			/* doing a little memory trick so can't just use new */
			length += sizeof(share) + 4;
			return (share *) malloc(length & ~3);
		}
	};
	
	template<class T>
	static ssize_t locate_generic(T array, size_t size, const istr & key);
	
	/* as this is the only state, istr instances will be equal if their strings are pointer
	 * equivalent - which is what we want anyway, so no need to define operator== */
	share * shared;
};

/* useful for std::map, etc. */
struct strcmp_less
{
	/* stick with const char * here to make sure we never try to make istr
	 * instances out of const char * (which copies) just to compare them */
	inline bool operator()(const char * a, const char * b) const
	{
		return strcmp(a, b) < 0;
	}
};

#endif /* __ISTR_H */
