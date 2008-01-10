/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MEMCACHE_H
#define __MEMCACHE_H

#include <stdint.h>
#include <sys/types.h>

#include "multimap.h"

#ifndef __cplusplus
#error memcache.h is a C++ header file
#endif

class memcache_it : public multimap_it
{
public:
	virtual int next();
	virtual size_t size();
	virtual ~memcache_it();
private:
};

class memcache : public multimap
{
public:
	memcache(multimap * map);
	virtual ~memcache();
	
	virtual size_t keys();
	virtual size_t values();
	
	virtual ssize_t count_values(mm_val_t * key);
	virtual memcache_it * get_values(mm_val_t * key);
	virtual ssize_t count_range(mm_val_t * low_key, mm_val_t * high_key);
	virtual memcache_it * get_range(mm_val_t * low_key, mm_val_t * high_key);
	
	virtual memcache_it * iterator();
	
	virtual int remove_key(mm_val_t * key);
	virtual int reset_key(mm_val_t * key, mm_val_t * value);
	virtual int append_value(mm_val_t * key, mm_val_t * value);
	virtual int remove_value(mm_val_t * key, mm_val_t * value);
	virtual int update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value);
};

#endif /* __MEMCACHE_H */
