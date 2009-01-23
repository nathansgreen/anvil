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
	virtual const istr & column() const
	{
		if(!column_cached)
		{
			cached_column = iter->column();
			column_cached = true;
		}
		return cached_column;
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
		column_cached = false;
		/* these might hold memory, so reset them */
		cached_key = dtype(0u);
		cached_value = blob();
		cached_column = NULL;
	}
	
	ctable::iter * iter;
	mutable bool key_cached, value_cached, column_cached;
	mutable dtype cached_key;
	mutable blob cached_value;
	mutable istr cached_column;
	
	void operator=(const ctable_cache_iter &);
	ctable_cache_iter(const ctable_cache_iter &);
};

#endif /* __CTABLE_CACHE_ITER_H */
