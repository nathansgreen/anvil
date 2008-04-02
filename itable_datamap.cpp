/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "itable_datamap.h"

bool itable_datamap::has(iv_int k1)
{
	return base->has(k1);
}

bool itable_datamap::has(const char * k1)
{
	return base->has(k1);
}

bool itable_datamap::has(iv_int k1, iv_int k2)
{
	return base->has(k1, k2);
}

bool itable_datamap::has(iv_int k1, const char * k2)
{
	return base->has(k1, k2);
}

bool itable_datamap::has(const char * k1, iv_int k2)
{
	return base->has(k1, k2);
}

bool itable_datamap::has(const char * k1, const char * k2)
{
	return base->has(k1, k2);
}

off_t itable_datamap::_get(iv_int k1, iv_int k2, itable ** source)
{
	off_t off = base->get(k1, k2);
	if(source)
		*source = this;
	return off;
}

off_t itable_datamap::_get(iv_int k1, const char * k2, itable ** source)
{
	off_t off = base->get(k1, k2);
	if(source)
		*source = this;
	return off;
}

off_t itable_datamap::_get(const char * k1, iv_int k2, itable ** source)
{
	off_t off = base->get(k1, k2);
	if(source)
		*source = this;
	return off;
}

off_t itable_datamap::_get(const char * k1, const char * k2, itable ** source)
{
	off_t off = base->get(k1, k2);
	if(source)
		*source = this;
	return off;
}

int itable_datamap::iter(struct it * it)
{
	return base->iter(it);
}

int itable_datamap::iter(struct it * it, iv_int k1)
{
	return base->iter(it, k1);
}

int itable_datamap::iter(struct it * it, const char * k1)
{
	return base->iter(it, k1);
}

int itable_datamap::_next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source)
{
	int r = base->next(it, k1, k2, off);
	if(source)
		*source = this;
	return r;
}

int itable_datamap::_next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source)
{
	int r = base->next(it, k1, k2, off);
	if(source)
		*source = this;
	return r;
}

int itable_datamap::_next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source)
{
	int r = base->next(it, k1, k2, off);
	if(source)
		*source = this;
	return r;
}

int itable_datamap::_next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source)
{
	int r = base->next(it, k1, k2, off);
	if(source)
		*source = this;
	return r;
}

int itable_datamap::_next(struct it * it, iv_int * k1)
{
	return base->next(it, k1);
}

int itable_datamap::_next(struct it * it, const char ** k1)
{
	return base->next(it, k1);
}

datastore * itable_datamap::get_datastore(iv_int k1, iv_int k2)
{
	datastore * store = (datastore *) hash_map_find_val(k2_stores, (void *) k2);
	if(store)
		return store;
	store = (datastore *) hash_map_find_val(k1_stores, (void *) k1);
	return store ? store : default_store;
}

datastore * itable_datamap::get_datastore(iv_int k1, const char * k2)
{
	datastore * store = (datastore *) hash_map_find_val(k2_stores, k2);
	if(store)
		return store;
	store = (datastore *) hash_map_find_val(k1_stores, (void *) k1);
	return store ? store : default_store;
}

datastore * itable_datamap::get_datastore(const char * k1, iv_int k2)
{
	datastore * store = (datastore *) hash_map_find_val(k2_stores, (void *) k2);
	if(store)
		return store;
	store = (datastore *) hash_map_find_val(k1_stores, k1);
	return store ? store : default_store;
}

datastore * itable_datamap::get_datastore(const char * k1, const char * k2)
{
	datastore * store = (datastore *) hash_map_find_val(k2_stores, k2);
	if(store)
		return store;
	store = (datastore *) hash_map_find_val(k1_stores, k1);
	return store ? store : default_store;
}

int itable_datamap::init(itable * itable, datastore * dfl_store)
{
	if(base)
		deinit();
	if(itable->k1_type() == STRING)
		k1_stores = hash_map_create_str();
	else
		k1_stores = hash_map_create();
	if(!k1_stores)
		return -ENOMEM;
	if(itable->k2_type() == STRING)
		k2_stores = hash_map_create_str();
	else
		k2_stores = hash_map_create();
	if(!k2_stores)
	{
		hash_map_destroy(k1_stores);
		k1_stores = NULL;
		return -ENOMEM;
	}
	base = itable;
	default_store = dfl_store;
	return 0;
}

void itable_datamap::deinit()
{
	default_store = NULL;
	if(k1_stores)
	{
		hash_map_it2_t hm_it;
		if(base->k2_type() == STRING)
		{
			hm_it = hash_map_it2_create(k2_stores);
			while(hash_map_it2_next(&hm_it))
				free((void *) hm_it.key);
		}
		hash_map_destroy(k2_stores);
		k2_stores = NULL;
		if(base->k1_type() == STRING)
		{
			hm_it = hash_map_it2_create(k1_stores);
			while(hash_map_it2_next(&hm_it))
				free((void *) hm_it.key);
		}
		hash_map_destroy(k1_stores);
		k1_stores = NULL;
	}
	base = NULL;
}

itable * itable_datamap::set_itable(itable * itable)
{
	class itable * old = base;
	if(base->k1_type() != itable->k1_type())
		return NULL;
	if(base->k2_type() != itable->k2_type())
		return NULL;
	base = itable;
	return old;
}

datastore * itable_datamap::set_default_store(datastore * store)
{
	datastore * old = default_store;
	default_store = store;
	return old;
}

datastore * itable_datamap::set_k1_store(iv_int k1, datastore * store)
{
	datastore * old;
	if(base->k1_type() == STRING)
		return NULL;
	if(store)
	{
		old = (datastore *) hash_map_find_val(k1_stores, (void *) k1);
		if(hash_map_insert(k1_stores, (void *) k1, store) < 0)
			return NULL;
	}
	else
		old = (datastore *) hash_map_erase(k1_stores, (void *) k1);
	return old;
}

datastore * itable_datamap::set_k1_store(const char * k1, datastore * store)
{
	datastore * old;
	hash_map_elt_t elt;
	if(base->k1_type() != STRING)
		return NULL;
	elt = hash_map_find_elt(k1_stores, k1);
	old = (datastore *) elt.val;
	if(store)
	{
		if(!elt.key)
		{
			k1 = strdup(k1);
			if(!k1)
				return NULL;
		}
		else
			k1 = (const char *) elt.key;
		if(hash_map_insert(k1_stores, k1, store) < 0)
		{
			if(!elt.key)
				free((void *) k1);
			return NULL;
		}
	}
	else
	{
		hash_map_erase(k1_stores, k1);
		if(elt.key)
			free((void *) elt.key);
	}
	return old;
}

datastore * itable_datamap::set_k2_store(iv_int k2, datastore * store)
{
	datastore * old;
	if(base->k2_type() == STRING)
		return NULL;
	if(store)
	{
		old = (datastore *) hash_map_find_val(k2_stores, (void *) k2);
		if(hash_map_insert(k2_stores, (void *) k2, store) < 0)
			return NULL;
	}
	else
		old = (datastore *) hash_map_erase(k2_stores, (void *) k2);
	return old;
}

datastore * itable_datamap::set_k2_store(const char * k2, datastore * store)
{
	datastore * old;
	hash_map_elt_t elt;
	if(base->k2_type() != STRING)
		return NULL;
	elt = hash_map_find_elt(k2_stores, k2);
	old = (datastore *) elt.val;
	if(store)
	{
		if(!elt.key)
		{
			k2 = strdup(k2);
			if(!k2)
				return NULL;
		}
		else
			k2 = (const char *) elt.key;
		if(hash_map_insert(k2_stores, k2, store) < 0)
		{
			if(!elt.key)
				free((void *) k2);
			return NULL;
		}
	}
	else
	{
		hash_map_erase(k2_stores, k2);
		if(elt.key)
			free((void *) elt.key);
	}
	return old;
}
