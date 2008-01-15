/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "multimap.h"
#include "memcache.h"

/* this is a dummy memcache; it doesn't cache at all */

/* iterators */

int memcache_it::next()
{
	return base->next();
}

size_t memcache_it::size()
{
	return base->size();
}

memcache_it::~memcache_it()
{
	delete base;
}

memcache_it::memcache_it(multimap_it * it)
	: base(it)
{
}

/* mem caches */

memcache::memcache(multimap * map)
	: base(map)
{
	key_type = map->get_key_type();
	val_type = map->get_val_type();
}

memcache::~memcache()
{
}

size_t memcache::keys()
{
	return base->keys();
}

size_t memcache::values()
{
	return base->values();
}

ssize_t memcache::count_values(mm_val_t * key)
{
	return base->count_values(key);
}

memcache_it * memcache::wrap_it(multimap_it * base_it)
{
	memcache_it * it;
	if(!base_it)
		return NULL;
	it = new memcache_it(base_it);
	if(!it)
	{
		delete base_it;
		return NULL;
	}
	return it;
}

memcache_it * memcache::get_values(mm_val_t * key)
{
	return wrap_it(base->get_values(key));
}

ssize_t memcache::count_range(mm_val_t * low_key, mm_val_t * high_key)
{
	return base->count_range(low_key, high_key);
}

memcache_it * memcache::get_range(mm_val_t * low_key, mm_val_t * high_key)
{
	return wrap_it(base->get_range(low_key, high_key));
}

memcache_it * memcache::iterator()
{
	return wrap_it(base->iterator());
}

int memcache::remove_key(mm_val_t * key)
{
	return base->remove_key(key);
}

int memcache::reset_key(mm_val_t * key, mm_val_t * value)
{
	return base->reset_key(key, value);
}

int memcache::append_value(mm_val_t * key, mm_val_t * value)
{
	return base->append_value(key, value);
}

int memcache::remove_value(mm_val_t * key, mm_val_t * value)
{
	return base->remove_value(key, value);
}

int memcache::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
	return base->update_value(key, old_value, new_value);
}
