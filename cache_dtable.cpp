/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "cache_dtable.h"

dtable::iter * cache_dtable::iterator() const
{
	/* use the underlying iterator directly; we don't want to kill our cache iterating
	 * though everything nor would it be easy to take advantage of our cache anyway */
	return base->iterator();
}

void cache_dtable::add_cache(const dtype & key, const blob & value, bool found) const
{
	/* FIXME: this is a very simple eviction algorithm: evict
	 * the oldest item, regardless of when it has been used */
	assert(!cache.count(key));
	if(cache_size)
	{
		if(cache.size() == cache_size)
		{
			cache.erase(order.front());
			order.pop();
		}
		assert(cache.size() < cache_size);
	}
	cache[key] = (entry) {value, found};
	order.push(key);
	assert(cache.size() == order.size());
}

blob cache_dtable::lookup(const dtype & key, bool * found) const
{
	cache_map::const_iterator iter = cache.find(key);
	if(iter != cache.end())
	{
		*found = (*iter).second.found;
		return (*iter).second.value;
	}
	blob value = base->lookup(key, found);
	add_cache(key, value, *found);
	return value;
}

int cache_dtable::insert(const dtype & key, const blob & blob, bool append)
{
	cache_map::iterator iter;
	int value = base->insert(key, blob, append);
	if(value < 0)
		return value;
	iter = cache.find(key);
	if(iter != cache.end())
	{
		(*iter).second.found = true;
		(*iter).second.value = blob;
	}
	else
		add_cache(key, blob, true);
	return value;
}

int cache_dtable::remove(const dtype & key)
{
	cache_map::iterator iter;
	int value = base->remove(key);
	if(value < 0)
		return value;
	iter = cache.find(key);
	if(iter != cache.end())
	{
		(*iter).second.found = false;
		(*iter).second.value = blob();
	}
	else
		add_cache(key, blob(), false);
	return value;
}

int cache_dtable::init(int dfd, const char * file, const params & config)
{
	int r;
	const dtable_factory * factory;
	params base_config;
	if(base)
		deinit();
	if(!config.get("cache_size", &r, 0) || r < 0)
		return -EINVAL;
	cache_size = r;
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	base = factory->open(dfd, file, base_config);
	if(!base)
		return -1;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	return 0;
}

void cache_dtable::deinit()
{
	if(base)
	{
		cache.clear();
		while(!order.empty())
			order.pop();
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

DEFINE_WRAP_FACTORY(cache_dtable);
