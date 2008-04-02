/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ITABLE_DATAMAP_H
#define __ITABLE_DATAMAP_H

#include <stdint.h>

#include "hash_map.h"
#include "itable.h"

#ifndef __cplusplus
#error itable_datamap.h is a C++ header file
#endif

class itable_datamap : public itable
{
public:
	/* test whether there is an entry for the given key */
	virtual bool has(iv_int k1);
	virtual bool has(const char * k1);
	
	virtual bool has(iv_int k1, iv_int k2);
	virtual bool has(iv_int k1, const char * k2);
	virtual bool has(const char * k1, iv_int k2);
	virtual bool has(const char * k1, const char * k2);
	
	/* get the offset for the given key */
	virtual off_t _get(iv_int k1, iv_int k2, itable ** source);
	virtual off_t _get(iv_int k1, const char * k2, itable ** source);
	virtual off_t _get(const char * k1, iv_int k2, itable ** source);
	virtual off_t _get(const char * k1, const char * k2, itable ** source);
	
	/* iterate through the offsets: set up iterators */
	virtual int iter(struct it * it);
	virtual int iter(struct it * it, iv_int k1);
	virtual int iter(struct it * it, const char * k1);
	
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int _next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int _next(struct it * it, iv_int * k1);
	virtual int _next(struct it * it, const char ** k1);
	
	virtual datastore * get_datastore(iv_int k1, iv_int k2);
	virtual datastore * get_datastore(iv_int k1, const char * k2);
	virtual datastore * get_datastore(const char * k1, iv_int k2);
	virtual datastore * get_datastore(const char * k1, const char * k2);
	
	inline itable_datamap();
	int init(itable * itable, datastore * dfl_store = NULL);
	void deinit();
	inline virtual ~itable_datamap();
	
	/* returns the old itable */
	itable * set_itable(itable * itable);
	/* returns the old default datastore */
	datastore * set_default_store(datastore * store);
	/* pass NULL for datastore to clear the datastore */
	datastore * set_k1_store(iv_int k1, datastore * store);
	datastore * set_k1_store(const char * k1, datastore * store);
	datastore * set_k2_store(iv_int k2, datastore * store);
	datastore * set_k2_store(const char * k2, datastore * store);
	
private:
	itable * base;
	datastore * default_store;
	hash_map_t * k1_stores;
	hash_map_t * k2_stores;
};

inline itable_datamap::itable_datamap()
	: base(NULL), default_store(NULL), k1_stores(NULL), k2_stores(NULL)
{
}

inline itable_datamap::~itable_datamap()
{
	if(k1_stores)
		deinit();
}

#endif /* __ITABLE_DATAMAP_H */
