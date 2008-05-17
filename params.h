/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __PARAMS_H
#define __PARAMS_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef __cplusplus
#error params.h is a C++ header file
#endif

#include <map>
#include "istr.h"

class params
{
public:
	inline void remove(const istr & name)
	{
		values.erase(name);
	}
	
	template<class T>
	inline void set(const istr & name, T value)
	{
		values[name] = value;
	}
	/* cause a compilation error if "class_name" is not the name of a type */
#define set_class(name, class_name) set(name, (const char *) (const class_name **) #class_name)
	
	/* these return false on a type mismatch */
	bool get(const istr & name, bool * value, bool dfl = false) const;
	bool get(const istr & name, int * value, int dfl = 0) const;
	bool get(const istr & name, float * value, float dfl = 0) const;
	bool get(const istr & name, istr * value, const istr & dfl = NULL) const;
	bool get(const istr & name, params * value, const params & dfl = params()) const;
	
	inline void reset()
	{
		values.clear();
	}
	
private:
	struct strcmp_less
	{
		inline bool operator()(const istr & a, const istr & b) const
		{
			return strcmp(a, b) < 0;
		}
	};
	
	/* can't be defined until later */
	struct param;
	
	/* /me dislikes std::map immensely */
	bool hate_std_map_get(const istr & name, const param ** p) const;
	
	typedef std::map<istr, param, strcmp_less> value_map;
	value_map values;
};

struct params::param
{
	enum { BOOL = 2, INT, FLT, STR, PRM } type;
	union
	{
		bool b;
		int i;
		float f;
	};
	/* alas, we can't put these in the union */
	istr s;
	params p;
	inline param() : type(INT), i(0) {}
	inline param(bool x) : type(BOOL), b(x) {}
	inline param(int x) : type(INT), i(x) {}
	inline param(float x) : type(FLT), f(x) {}
	inline param(const istr & x) : type(STR), i(0), s(x) {}
	/* this is necessary so const char * doesn't end up being used as bool */
	inline param(const char * x) : type(STR), i(0), s(x) {}
	inline param(const params & x) : type(PRM), i(0), p(x) {}
};

#endif /* __PARAMS_H */
