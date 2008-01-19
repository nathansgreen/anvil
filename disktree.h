/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DISKTREE_H
#define __DISKTREE_H

#include <stdint.h>
#include <sys/types.h>

#include "multimap.h"

#ifndef __cplusplus
#error disktree.h is a C++ header file
#endif

#define DT_KT_IDX 0
#define DT_VT_IDX 1
#define DT_KC_IDX 0
#define DT_VC_IDX 1

class disktree_it : public multimap_it
{
public:
	virtual int next();
	virtual size_t size();
	virtual ~disktree_it();
private:
};

class disktree : public multimap
{
public:
	virtual ~disktree();
	
	virtual size_t keys();
	virtual size_t values();
	
	virtual ssize_t count_values(mm_val_t * key);
	virtual disktree_it * get_values(mm_val_t * key);
	virtual ssize_t count_range(mm_val_t * low_key, mm_val_t * high_key);
	virtual disktree_it * get_range(mm_val_t * low_key, mm_val_t * high_key);
	
	virtual disktree_it * iterator();
	
	virtual int remove_key(mm_val_t * key);
	virtual int reset_key(mm_val_t * key, mm_val_t * value);
	virtual int append_value(mm_val_t * key, mm_val_t * value);
	virtual int remove_value(mm_val_t * key, mm_val_t * value);
	virtual int update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value);
	
	/* create a new disktree (on disk) using the specified store path */
	static int init(int dfd, const char * store, mm_type_t key_type, mm_type_t val_type);
	/* open a disktree on disk, or return NULL on error */
	static disktree * open(uint8_t * id, int dfd, const char * store);
private:
	disktree(uint8_t * id);
};

#endif /* __DISKTREE_H */
