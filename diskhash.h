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

#define DH_KT_IDX 0
#define DH_VT_IDX 1
#define DH_KC_IDX 0
#define DH_VC_IDX 1

class diskhash;

class diskhash_it : public multimap_it
{
public:
	virtual int next();
	virtual size_t size();
	virtual ~diskhash_it();
private:
	DIR * dir;
	int key_fd;
	size_t values;
	diskhash_it(diskhash * dh, mm_val_t * it_key, DIR * key_dir, int key_fd, size_t count);
	friend class diskhash;
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
	static diskhash * open(uint8_t * id, int dfd, const char * store);
private:
	int dir_fd, dh_fd;
	size_t key_count, value_count;
	int bucket_fd(mm_val_t * key, bool create = false);
	int key_fd(mm_val_t * key, bool create = false);
	diskhash(uint8_t * id, mm_type_t kt, mm_type_t vt, int dir, int dh, size_t keys, size_t values);
};

#endif /* __DISKHASH_H */
