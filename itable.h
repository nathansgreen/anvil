/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ITABLE_H
#define __ITABLE_H

#include <limits.h>
#include <sys/types.h>

#ifndef __cplusplus
#error itable.h is a C++ header file
#endif

typedef int iv_int;
#define IV_INT_MIN INT_MIN

class ivalue
{
public:
	enum vtype {
		NONE,
		INT,
		STRING,
		BLOB
	};
	
	vtype type();
	
	iv_int v_int();
	const char * v_string();
	void * v_blob(size_t * length);
};

class itable
{
public:
	/* get the types of the keys */
	ivalue::vtype k1_type();
	ivalue::vtype k2_type();
	
	/* test whether there is a value for the given key */
	bool has(iv_int k1);
	bool has(const char * k1);
	
	bool has(iv_int k1, iv_int k2);
	bool has(iv_int k1, const char * k2);
	bool has(const char * k1, iv_int k2);
	bool has(const char * k1, const char * k2);
	
	/* get the value for the given key */
	int get(iv_int k1, iv_int k2, ivalue * value);
	int get(iv_int k1, const char * k2, ivalue * value);
	int get(const char * k1, iv_int k2, ivalue * value);
	int get(const char * k1, const char * k2, ivalue * value);
	
	/* get the next key >= the given key */
	int next(iv_int k1, iv_int * key);
	int next(const char * k1, const char ** key);
	
	int next(iv_int k1, iv_int k2, iv_int * key);
	int next(iv_int k1, const char * k2, const char ** key);
	int next(const char * k1, iv_int k2, iv_int * key);
	int next(const char * k1, const char * k2, const char ** key);
};

#endif /* __ITABLE_H */
