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

/* this enum is available to C code */
enum dtype_ctype
{
	/* we want this 0 so that a zeroed out dtype will be
	 * correctly initialized, and the integer zero even */
	DT_UINT32 = 0,
	DT_DOUBLE,
	DT_STRING,
	DT_BLOB
};

#ifdef __cplusplus

#include <ext/hash_map>

#include "blob.h"
#include "istr.h"
#include "magic_test.h"
#include "blob_comparator.h"

/* all data stored in toilet is wrapped by this type */

class dtype
{
public:
	enum ctype
	{
		UINT32 = DT_UINT32,
		DOUBLE = DT_DOUBLE,
		STRING = DT_STRING,
		BLOB = DT_BLOB
	};
	ctype type;
	
	union
	{
		uint32_t u32;
		double dbl;
	};
	/* alas, we can't put these in the union */
	istr str;
	blob blb;
	
	inline dtype(uint32_t x) : type(UINT32), u32(x) {}
	inline dtype(double x) : type(DOUBLE), dbl(x) {}
	inline dtype(const istr & x) : type(STRING), u32(0), str(x) {}
	/* have to provide this even though usually istr is transparent */
	inline dtype(const char * x) : type(STRING), u32(0), str(x) {}
	inline dtype(const char * x, size_t length) : type(STRING), u32(0), str(x, length) {}
	inline dtype(const blob & x) : type(BLOB), u32(0), blb(x) {}
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
				str = b;
				return;
			case BLOB:
				blb = b;
				return;
		}
		abort();
	}

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
			case BLOB:
				return blb;
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
			case BLOB:
				return "blob";
		}
		return "unknown";
	}
	
	inline int compare(const dtype & x, const blob_comparator * blob_cmp = NULL) const
	{
		assert(type == x.type);
		switch(type)
		{
			case UINT32:
				return (u32 < x.u32) ? -1 : u32 != x.u32;
			case DOUBLE:
				return (dbl < x.dbl) ? -1 : dbl != x.dbl;
			case STRING:
				return strcmp(str, x.str);
			case BLOB:
				return blob_cmp ? blob_cmp->compare(blb, x.blb) : blb.compare(x.blb);
		}
		abort();
	}
	
	/* avoid constructing a second dtype if it is not necessary */
	inline int compare(uint32_t x) const
	{
		assert(type == UINT32);
		return (u32 < x) ? -1 : u32 != x;
	}
	
	inline int compare(double x) const
	{
		assert(type == DOUBLE);
		return (dbl < x) ? -1 : dbl != x;
	}
	
	inline int compare(const char * x) const
	{
		assert(type == STRING);
		return strcmp(str, x);
	}
	
	inline int compare(const istr & x) const
	{
		assert(type == STRING);
		return strcmp(str, x);
	}
	
	inline int compare(const blob & x, const blob_comparator * blob_cmp = NULL) const
	{
		assert(type == BLOB);
		return blob_cmp ? blob_cmp->compare(blb, x) : blb.compare(x);
	}
};

/* this class can be used to wrap a blob comparator pointer so that it can be
 * used for STL methods like std::sort() when you have dtypes and not blobs */
class dtype_comparator_object
{
public:
	inline bool operator()(const dtype & a, const dtype & b) const
	{
		return a.compare(b, blob_cmp) < 0;
	}
	
	inline dtype_comparator_object(const blob_comparator * comparator = NULL) : blob_cmp(comparator) {}
	
private:
	const blob_comparator * blob_cmp;
};

/* if it is necessary to change the comparator during the use of an STL
 * container like std::set, then dtype_comparator_refobject will help you out */
class dtype_comparator_refobject
{
public:
	inline bool operator()(const dtype & a, const dtype & b) const
	{
		return a.compare(b, blob_cmp) < 0;
	}
	
	inline dtype_comparator_refobject(const blob_comparator *& comparator) : blob_cmp(comparator) {}
	
private:
	const blob_comparator *& blob_cmp;
};

template<class T>
struct dtype_hash_helper
{
};

template<>
struct dtype_hash_helper<unsigned int>
{
	inline size_t operator()(unsigned int x) const
	{
		return x;
	}
};

template<>
struct dtype_hash_helper<unsigned long>
{
	inline size_t operator()(unsigned long x) const
	{
		return x;
	}
};

template<>
struct dtype_hash_helper<void *>
{
	size_t operator()(const void * x) const
	{
		/* this will be one of the above two cases */
		return dtype_hash_helper<uintptr_t>()((uintptr_t) x);
	}
};

template<>
struct dtype_hash_helper<double>
{
	inline size_t operator()(double x) const
	{
		union {
			uint64_t i;
			double d;
		} u = {i: 0};
		u.d = x;
		/* we count on the compiler to optimize this */
		if(sizeof(size_t) == sizeof(uint64_t))
			return u.i;
		return (size_t) ((u.i >> 32) ^ u.i);
	}
};

/* good for hash_map; has an equality operator and a hash operator */
class dtype_hashing_comparator
{
public:
	inline bool operator()(const dtype & a, const dtype & b) const
	{
		return !a.compare(b, blob_cmp);
	}
	
	inline size_t operator()(const dtype & dt) const
	{
		switch(dt.type)
		{
			case dtype::UINT32:
				return dt.u32;
			case dtype::DOUBLE:
				/* 0 and -0 both hash to zero */
				if(dt.dbl == 0.0)
					return 0;
				return dtype_hash_helper<double>()(dt.dbl);
			case dtype::STRING:
				return __gnu_cxx::hash<const char *>()((const char *) dt.str);
			case dtype::BLOB:
			{
				if(blob_cmp)
					return blob_cmp->hash(dt.blb);
				
				/* uses FNV hash taken from stl::tr1::hash */
				size_t r = 2166136261u;
				size_t length = dt.blb.size();
				for(size_t i = 0; i < length; i++)
				{
					r ^= dt.blb[i];
					r *= 16777619;
				}
				return r;
			}
		}
		abort();
	}
	
	inline dtype_hashing_comparator(const blob_comparator *& comparator) : blob_cmp(comparator) {}
	
private:
	const blob_comparator *& blob_cmp;
};

typedef magic_test<dtype> dtype_test;

class dtype_fixed_test : public dtype_test
{
public:
	inline dtype_fixed_test(const dtype & key) : secret(key) { }
	
	inline virtual int operator()(const dtype & key) const
	{
		return key.compare(secret);
	}
	
private:
	const dtype secret;
};

#endif /* __cplusplus */

#endif /* __DTYPE_H */
