/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DISKHASH_H
#define __DISKHASH_H

#include <stdint.h>
#include <sys/types.h>

#include "multimap.h"

#ifndef __cplusplus
#error diskhash.h is a C++ header file
#endif

class diskhash_it : public multimap_it
{
public:
	virtual int next();
	virtual size_t size();
	virtual ~diskhash_it();
private:
};

class diskhash : public multimap
{
public:
	virtual ~diskhash();
	
	virtual size_t keys();
	virtual size_t values();
	
	virtual ssize_t count_values(mm_val_t * key);
	virtual diskhash_it * get_values(mm_val_t * key);
	virtual ssize_t count_range(mm_val_t * low_key, mm_val_t * high_key);
	virtual diskhash_it * get_range(mm_val_t * low_key, mm_val_t * high_key);
	
	virtual diskhash_it * iterator();
	
	virtual int remove_key(mm_val_t * key);
	virtual int reset_key(mm_val_t * key, mm_val_t * value);
	virtual int append_value(mm_val_t * key, mm_val_t * value);
	virtual int remove_value(mm_val_t * key, mm_val_t * value);
	virtual int update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value);
	
	/* create a new diskhash (on disk) using the specified store path */
	static int init(int dfd, const char * store, mm_type_t key_type, mm_type_t val_type);
	/* open a diskhash on disk, or return NULL on error */
	static diskhash * open(int dfd, const char * store);
private:
	diskhash();
};

#endif /* __DISKHASH_H */
