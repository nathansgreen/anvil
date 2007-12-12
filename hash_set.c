/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "platform.h"

#include "hash_set.h"

hash_set_t * hash_set_create(void)
{
	return hash_map_create();
}

hash_set_t * hash_set_create_ptr(void)
{
	return hash_map_create_ptr();
}

hash_set_t * hash_set_create_str(void)
{
	return hash_map_create_str();
}

hash_set_t * hash_set_create_size(size_t n, bool auto_resize)
{
	return hash_map_create_size(n, auto_resize);
}

hash_set_t * hash_set_create_size_ptr(size_t n, bool auto_resize)
{
	return hash_map_create_size_ptr(n, auto_resize);
}

hash_set_t * hash_set_create_size_str(size_t n, bool auto_resize)
{
	return hash_map_create_size_str(n, auto_resize);
}

hash_set_t * hash_set_copy(const hash_set_t * hs)
{
	return hash_map_copy(hs);
}

void hash_set_destroy(hash_set_t * hs)
{
	hash_map_destroy(hs);
}

size_t hash_set_size(const hash_set_t * hs)
{
	return hash_map_size(hs);
}

bool hash_set_empty(const hash_set_t * hs)
{
	return hash_map_empty(hs);
}

int hash_set_insert(hash_set_t * hs, const void * v)
{
	/* 1 is just some nonzero value */
	return hash_map_insert(hs, v, (void *) 1);
}

int hash_set_erase(hash_set_t * hs, const void * v)
{
	return hash_map_erase(hs, v) ? 0 : -ENOENT;
}

void hash_set_clear(hash_set_t * hs)
{
	hash_map_clear(hs);
}

int hash_set_lookup(const hash_set_t * hs, const void * v)
{
	return hash_map_find_val(hs, v) != NULL;
}

size_t hash_set_bucket_count(const hash_set_t * hs)
{
	return hash_map_bucket_count(hs);
}

int hash_set_resize(hash_set_t * hs, size_t n)
{
	return hash_map_resize(hs, n);
}

hash_set_it2_t hash_set_it2_create(hash_set_t * hs)
{
	return hash_map_it2_create(hs);
}

bool hash_set_it2_next(hash_set_it2_t * it)
{
	return hash_map_it2_next(it);
}

void hash_set_it_init(hash_set_it_t * it, hash_set_t * hs)
{
	hash_map_it_init(it, hs);
}

const void * hash_set_val_next(hash_set_it_t * it)
{
	return hash_map_val_next(it);
}
