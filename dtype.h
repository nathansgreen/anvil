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
	
	union
	{
		uint32_t u32;
		double dbl;
		const char * str;
	};
	
	inline dtype(uint32_t x) : type(UINT32), u32(x) {}
	inline dtype(double x) : type(DOUBLE), dbl(x) {}
	inline dtype(const char * x) : type(STRING), str(strdup(x)) {}
	inline ~dtype() { if(type == STRING && str) free((void *) str); }
	
	inline dtype & operator=(const dtype & x)
	{
		if(this == &x)
			return *this;
		if(type == STRING)
			free((void *) str);
		switch(type = x.type)
		{
			case UINT32:
				u32 = x.u32;
				return *this;
			case DOUBLE:
				dbl = x.dbl;
				return *this;
			case STRING:
				str = strdup(x.str);
				assert(str);
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
