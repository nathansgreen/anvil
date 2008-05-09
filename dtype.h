/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTYPE_H
#define __DTYPE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#ifndef __cplusplus
#error dtype.h is a C++ header file
#endif

/* all data stored in toilet is wrapped by this type */

class dtype
{
public:
	enum ctype {UINT32, DOUBLE, STRING};
	ctype type;
	
	struct dt_string
	{
		size_t shares;
		char string[0];
	};
	
	union
	{
		uint32_t u32;
		double dbl;
		struct {
			const char * str;
			dt_string * _str;
		};
	};
	
	inline dtype(uint32_t x) : type(UINT32), u32(x) {}
	inline dtype(double x) : type(DOUBLE), dbl(x) {}
	inline dtype(const char * x)
		: type(STRING)
	{
		_str = (dt_string *) malloc(sizeof(dt_string) + strlen(x) + 1);
		_str->shares = 1;
		strcpy(_str->string, x);
		str = _str->string;
	}
	/* simple copy constructor that just uses operator= */
	inline dtype(const dtype & x) : type(UINT32) { *this = x; }
	inline dtype(const blob & b, ctype t)
		: type(t)
	{
		assert(b.exists());
		switch(t)
		{
			case UINT32:
				assert(b.size() == sizeof(uint32_t));
				u32 = b.index<uint32_t>(0);
				return;
			case DOUBLE:
				assert(b.size() == sizeof(double));
				dbl = b.index<double>(0);
				return;
			case STRING:
				_str = (dt_string *) malloc(sizeof(dt_string) + b.size() + 1);
				_str->shares = 1;
				if(b.size())
					strncpy(_str->string, &b.index<char>(0), b.size());
				_str->string[b.size()] = 0;
				str = _str->string;
				return;
		}
		abort();
	}

	inline ~dtype() { if(type == STRING && --_str->shares <= 0) free(_str); }
	
	inline blob flatten() const
	{
		switch(type)
		{
			case UINT32:
				return blob(sizeof(uint32_t), &u32);
			case DOUBLE:
				return blob(sizeof(double), &dbl);
			case STRING:
				return blob(strlen(str), str);
		}
		abort();
	}
	
	static inline const char * name(ctype type)
	{
		switch(type)
		{
			case UINT32:
				return "uint32";
			case DOUBLE:
				return "double";
			case STRING:
				return "string";
		}
		return "unknown";
	}
	static inline const char * name(const dtype & value)
	{
		return name(value.type);
	}
	
	inline dtype & operator=(const dtype & x)
	{
		if(this == &x)
			return *this;
		if(type == STRING && --_str->shares <= 0)
			free(_str);
		switch(type = x.type)
		{
			case UINT32:
				u32 = x.u32;
				return *this;
			case DOUBLE:
				dbl = x.dbl;
				return *this;
			case STRING:
				_str = x._str;
				_str->shares++;
				str = _str->string;
				return *this;
		}
		abort();
	}
	
	inline bool operator==(const dtype & x) const
	{
		assert(type == x.type);
		switch(type)
		{
			case UINT32:
				return u32 == x.u32;
			case DOUBLE:
				return dbl == x.dbl;
			case STRING:
				return !strcmp(str, x.str);
		}
		abort();
	}
	
	inline bool operator!=(const dtype & x) const
	{
		return !(*this == x);
	}
	
	inline bool operator<(const dtype & x) const
	{
		assert(type == x.type);
		switch(type)
		{
			case UINT32:
				return u32 < x.u32;
			case DOUBLE:
				return dbl < x.dbl;
			case STRING:
				return strcmp(str, x.str) < 0;
		}
		abort();
	}
	
	inline bool operator<=(const dtype & x) const
	{
		return !(x < *this);
	}
	
	inline bool operator>(const dtype & x) const
	{
		return x < *this;
	}
	
	inline bool operator>=(const dtype & x) const
	{
		return !(*this < x);
	}
};

#endif /* __DTYPE_H */
