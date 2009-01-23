/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_CACHE_ITER_H
#define __DTABLE_CACHE_ITER_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error dtable_cache_iter.h is a C++ header file
#endif

#include "dtable.h"

/* The dtable caching iterator caches the keys and values it returns, so that
 * subsequent calls return the same value without asking the underlying iterator
 * again. This is particularly useful if the values are to be passed up to C,
 * since they can then be used without additional references as long as the
 * caching iterator is not repositioned. */

class dtable_cache_key_iter : public dtable::key_iter
{
public:
	virtual bool valid() const
	{
		return iter->valid();
	}
	virtual bool next()
	{
		kill_cache();
		return iter->next();
	}
	virtual bool prev()
	{
		kill_cache();
		return iter->prev();
	}
	virtual bool first()
	{
		kill_cache();
		return iter->first();
	}
	virtual bool last()
	{
		kill_cache();
		return iter->last();
	}
	virtual dtype key() const
	{
		if(!key_cached)
		{
			cached_key = iter->key();
			key_cached = true;
		}
		return cached_key;
	}
	virtual dtype::ctype key_type() const
	{
		return iter->key_type();
	}
	virtual bool seek(const dtype & key)
	{
		kill_cache();
		return iter->seek(key);
	}
	virtual bool seek(const dtype_test & test)
	{
		kill_cache();
		return iter->seek(test);
	}
	virtual bool seek_index(size_t index)
	{
		kill_cache();
		return iter->seek_index(index);
	}
	
	inline dtable_cache_key_iter(dtable::key_iter * iter)
		: iter(iter), key_cached(false), cached_key(0u)
	{
	}
	virtual ~dtable_cache_key_iter()
	{
		delete iter;
	}
	
private:
	inline void kill_cache()
	{
		key_cached = false;
		/* this might hold memory, so reset it */
		cached_key = dtype(0u);
	}
	
	dtable::key_iter * iter;
	mutable bool key_cached;
	mutable dtype cached_key;
	
	void operator=(const dtable_cache_key_iter &);
	dtable_cache_key_iter(const dtable_cache_key_iter &);
};

/* it would be nice if this could inherit from dtable_cache_key_iter
 * above, but then we'd have to deal with multiple inheritance */
class dtable_cache_iter : public dtable::iter
{
public:
	virtual bool valid() const
	{
		return iter->valid();
	}
	virtual bool next()
	{
		kill_cache();
		return iter->next();
	}
	virtual bool prev()
	{
		kill_cache();
		return iter->prev();
	}
	virtual bool first()
	{
		kill_cache();
		return iter->first();
	}
	virtual bool last()
	{
		kill_cache();
		return iter->last();
	}
	virtual dtype key() const
	{
		if(!key_cached)
		{
			cached_key = iter->key();
			key_cached = true;
		}
		return cached_key;
	}
	virtual dtype::ctype key_type() const
	{
		return iter->key_type();
	}
	virtual bool seek(const dtype & key)
	{
		kill_cache();
		return iter->seek(key);
	}
	virtual bool seek(const dtype_test & test)
	{
		kill_cache();
		return iter->seek(test);
	}
	virtual bool seek_index(size_t index)
	{
		kill_cache();
		return iter->seek_index(index);
	}
	virtual metablob meta() const
	{
		return iter->meta();
	}
	virtual blob value() const
	{
		if(!value_cached)
		{
			cached_value = iter->value();
			value_cached = true;
		}
		return cached_value;
	}
	virtual const dtable * source() const
	{
		return iter->source();
	}
	
	inline dtable_cache_iter(dtable::iter * iter)
		: iter(iter), key_cached(false), value_cached(false), cached_key(0u)
	{
	}
	virtual ~dtable_cache_iter()
	{
		delete iter;
	}
	
private:
	inline void kill_cache()
	{
		key_cached = false;
		value_cached = false;
		/* these might hold memory, so reset them */
		cached_key = dtype(0u);
		cached_value = blob();
	}
	
	dtable::iter * iter;
	mutable bool key_cached, value_cached;
	mutable dtype cached_key;
	mutable blob cached_value;
	
	void operator=(const dtable_cache_iter &);
	dtable_cache_iter(const dtable_cache_iter &);
};

#endif /* __DTABLE_CACHE_ITER_H */
