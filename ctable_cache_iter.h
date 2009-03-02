/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_CACHE_ITER_H
#define __CTABLE_CACHE_ITER_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error ctable_cache_iter.h is a C++ header file
#endif

#include "ctable.h"

/* The ctable caching iterator caches the keys, values, and column names it
 * returns, so that subsequent calls return the same value without asking the
 * underlying iterator again. This is particularly useful if the values are to
 * be passed up to C, since they can then be used without additional references
 * as long as the caching iterator is not repositioned. */

class ctable_cache_iter : public ctable::iter
{
public:
	virtual bool valid() const
	{
		return iter->valid();
	}
	virtual bool next(bool row = false)
	{
		kill_cache();
		return iter->next(row);
	}
	virtual bool prev(bool row = false)
	{
		kill_cache();
		return iter->prev(row);
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
	virtual size_t column() const
	{
		return iter->column();
	}
	virtual const istr & name() const
	{
		if(!name_cached)
		{
			cached_name = iter->name();
			name_cached = true;
		}
		return cached_name;
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
	/* TODO: cache this? */
	virtual blob index(size_t column) const
	{
		return iter->index(column);
	}
	
	inline ctable_cache_iter(ctable::iter * iter)
		: iter(iter), key_cached(false), value_cached(false), cached_key(0u)
	{
	}
	virtual ~ctable_cache_iter()
	{
		delete iter;
	}
	
private:
	inline void kill_cache()
	{
		key_cached = false;
		value_cached = false;
		name_cached = false;
		/* these might hold memory, so reset them */
		cached_key = dtype(0u);
		cached_value = blob();
		cached_name = NULL;
	}
	
	ctable::iter * iter;
	mutable bool key_cached, value_cached, name_cached;
	mutable dtype cached_key;
	mutable blob cached_value;
	mutable istr cached_name;
	
	void operator=(const ctable_cache_iter &);
	ctable_cache_iter(const ctable_cache_iter &);
};

#endif /* __CTABLE_CACHE_ITER_H */
