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
	assert(!cache.count(key));
	/* XXX FIXME this has no limit to how much it will cache... */
	cache[key] = (entry) {value, found};
}

blob cache_dtable::lookup(const dtype & key, bool * found) const
{
	blob_map::const_iterator iter = cache.find(key);
	if(iter != cache.end())
	{
		*found = (*iter).second.found;
		return (*iter).second.value;
	}
	blob value = base->lookup(key, found);
	add_cache(key, value, *found);
	return value;
}

int cache_dtable::append(const dtype & key, const blob & blob)
{
	blob_map::iterator iter;
	int value = base->append(key, blob);
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
	blob_map::iterator iter;
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
	const dtable_factory * factory;
	params base_config;
	if(base)
		deinit();
	factory = dt_factory_registry::lookup(config, "base");
	if(!factory)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	base = factory->open(dfd, file, base_config);
	if(!base)
		return -1;
	cmp_name = base->get_cmp_name();
	return 0;
}

void cache_dtable::deinit()
{
	if(base)
	{
		cache.clear();
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

DEFINE_WRAP_FACTORY(cache_dtable);
