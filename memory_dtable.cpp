/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "hack_avl_map.h"
#include "memory_dtable.h"

bool memory_dtable::iter::valid() const
{
	return mit != dt_source->mdt_map.end();
}

bool memory_dtable::iter::next()
{
	if(mit != dt_source->mdt_map.end())
		++mit;
	return mit != dt_source->mdt_map.end();
}

bool memory_dtable::iter::prev()
{
	if(mit == dt_source->mdt_map.begin())
		return false;
	--mit;
	return true;
}

bool memory_dtable::iter::first()
{
	mit = dt_source->mdt_map.begin();
	return mit != dt_source->mdt_map.end();
}

bool memory_dtable::iter::last()
{
	mit = dt_source->mdt_map.end();
	if(mit == dt_source->mdt_map.begin())
		return false;
	--mit;
	return true;
}

dtype memory_dtable::iter::key() const
{
	return mit->first;
}

bool memory_dtable::iter::seek(const dtype & key)
{
	mit = dt_source->mdt_map.lower_bound(key);
	if(mit == dt_source->mdt_map.end())
		return false;
	return !dt_source->mdt_map.key_comp()(key, mit->first);
}

bool memory_dtable::iter::seek(const dtype_test & test)
{
	mit = lower_bound(dt_source->mdt_map, test);
	if(mit == dt_source->mdt_map.end())
		return false;
	return !test(mit->first);
}

metablob memory_dtable::iter::meta() const
{
	return mit->second;
}

blob memory_dtable::iter::value() const
{
	return mit->second;
}

const dtable * memory_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * memory_dtable::iterator() const
{
	return new iter(this);
}

bool memory_dtable::present(const dtype & key, bool * found) const
{
	memory_dtable_hash::const_iterator it = mdt_hash.find(key);
	if(it != mdt_hash.end())
	{
		*found = true;
		return it->second->exists();
	}
	*found = false;
	return false;
}

blob memory_dtable::lookup(const dtype & key, bool * found) const
{
	memory_dtable_hash::const_iterator it = mdt_hash.find(key);
	if(it != mdt_hash.end())
	{
		*found = true;
		return *(it->second);
	}
	*found = false;
	return blob();
}

int memory_dtable::insert(const dtype & key, const blob & blob, bool append)
{
	if(key.type != ktype || (ktype == dtype::BLOB && !key.blb.exists()))
		return -EINVAL;
	/* empty hash table? just add it */
	if(!mdt_hash.size())
		return add_node(key, blob);
	if(append)
	{
		memory_dtable_map::iterator end = mdt_map.end();
		memory_dtable_map::iterator last = end;
		int c = (--last)->first.compare(key, blob_cmp);
		/* this is the expected case, if the hint was correct */
		if(c < 0)
			return add_node(key, blob, end);
		if(!c)
		{
			last->second = blob;
			return 0;
		}
	}
	memory_dtable_hash::iterator it = mdt_hash.find(key);
	if(it != mdt_hash.end())
	{
		*(it->second) = blob;
		return 0;
	}
	return add_node(key, blob);
}

int memory_dtable::remove(const dtype & key)
{
	if(full_rm)
	{
		if(mdt_hash.erase(key))
			mdt_map.erase(key);
		return 0;
	}
	return insert(key, blob());
}

int memory_dtable::init(dtype::ctype key_type, bool full_remove)
{
	if(ready)
		deinit();
	assert(mdt_map.empty());
	assert(mdt_hash.empty());
	assert(!cmp_name);
	ktype = key_type;
	ready = true;
	full_rm = full_remove;
	return 0;
}

void memory_dtable::deinit()
{
	mdt_hash.clear();
	mdt_map.clear();
	dtable::deinit();
	ready = false;
}

int memory_dtable::add_node(const dtype & key, const blob & value)
{
	blob & map_value = mdt_map[key];
	map_value = value;
	mdt_hash[key] = &map_value;
	return 0;
}

int memory_dtable::add_node(const dtype & key, const blob & value, const memory_dtable_map::iterator & end)
{
	memory_dtable_map::value_type pair(key, value);
	memory_dtable_map::iterator it = mdt_map.insert(end, pair);
	mdt_hash[key] = &(it->second);
	return 0;
}
