/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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

#include "dtable_wrap_iter.h"

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
		return base->valid();
	}
	virtual bool next()
	{
		kill_cache();
		return base->next();
	}
	virtual bool prev()
	{
		kill_cache();
		return base->prev();
	}
	virtual bool first()
	{
		kill_cache();
		return base->first();
	}
	virtual bool last()
	{
		kill_cache();
		return base->last();
	}
	virtual dtype key() const
	{
		if(!key_cached)
		{
			cached_key = base->key();
			key_cached = true;
		}
		return cached_key;
	}
	virtual bool seek(const dtype & key)
	{
		kill_cache();
		return base->seek(key);
	}
	virtual bool seek(const dtype_test & test)
	{
		kill_cache();
		return base->seek(test);
	}
	virtual bool seek_index(size_t index)
	{
		kill_cache();
		return base->seek_index(index);
	}
	virtual dtype::ctype key_type() const
	{
		return base->key_type();
	}
	virtual const blob_comparator * get_blob_cmp() const
	{
		return base->get_blob_cmp();
	}
	virtual const istr & get_cmp_name() const
	{
		return base->get_cmp_name();
	}
	
	inline dtable_cache_key_iter(dtable::key_iter * base)
		: base(base), key_cached(false), cached_key(0u)
	{
	}
	virtual ~dtable_cache_key_iter()
	{
		delete base;
	}
	
private:
	inline void kill_cache()
	{
		key_cached = false;
		/* this might hold memory, so reset it */
		cached_key = dtype(0u);
	}
	
	dtable::key_iter * base;
	mutable bool key_cached;
	mutable dtype cached_key;
	
	void operator=(const dtable_cache_key_iter &);
	dtable_cache_key_iter(const dtable_cache_key_iter &);
};

/* it would be nice if this could inherit from dtable_cache_key_iter
 * above, but then we'd have to deal with multiple inheritance */
class dtable_cache_iter : public dtable_wrap_iter
{
public:
	virtual bool next()
	{
		kill_cache();
		return base->next();
	}
	virtual bool prev()
	{
		kill_cache();
		return base->prev();
	}
	virtual bool first()
	{
		kill_cache();
		return base->first();
	}
	virtual bool last()
	{
		kill_cache();
		return base->last();
	}
	virtual dtype key() const
	{
		if(!key_cached)
		{
			cached_key = base->key();
			key_cached = true;
		}
		return cached_key;
	}
	virtual bool seek(const dtype & key)
	{
		kill_cache();
		return base->seek(key);
	}
	virtual bool seek(const dtype_test & test)
	{
		kill_cache();
		return base->seek(test);
	}
	virtual bool seek_index(size_t index)
	{
		kill_cache();
		return base->seek_index(index);
	}
	virtual blob value() const
	{
		if(!value_cached)
		{
			cached_value = base->value();
			value_cached = true;
		}
		return cached_value;
	}
	
	inline dtable_cache_iter(dtable::iter * base, bool claim_base = false)
		: dtable_wrap_iter(base, claim_base), key_cached(false), value_cached(false), cached_key(0u)
	{
	}
	virtual ~dtable_cache_iter() {}
	
private:
	inline void kill_cache()
	{
		key_cached = false;
		value_cached = false;
		/* these might hold memory, so reset them */
		cached_key = dtype(0u);
		cached_value = blob();
	}
	
	mutable bool key_cached, value_cached;
	mutable dtype cached_key;
	mutable blob cached_value;
	
	void operator=(const dtable_cache_iter &);
	dtable_cache_iter(const dtable_cache_iter &);
};

#endif /* __DTABLE_CACHE_ITER_H */
