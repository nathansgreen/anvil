/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "multimap.h"
#include "memcache.h"

/* iterators */

int memcache_it::next()
{
}

size_t memcache_it::size()
{
}

memcache_it::~memcache_it()
{
}

/* mem caches */

memcache::memcache(multimap * map)
{
}

memcache::~memcache()
{
}

size_t memcache::keys()
{
}

size_t memcache::values()
{
}

size_t memcache::count_values(mm_val_t * key)
{
}

memcache_it * memcache::get_values(mm_val_t * key)
{
}

memcache_it * memcache::get_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

memcache_it * memcache::iterator()
{
}

int memcache::remove_key(mm_val_t * key)
{
}

int memcache::reset_key(mm_val_t * key, mm_val_t * value)
{
}

int memcache::append_value(mm_val_t * key, mm_val_t * value)
{
}

int memcache::remove_value(mm_val_t * key, mm_val_t * value)
{
}

int memcache::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
}
