/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_SKIP_ITER_H
#define __DTABLE_SKIP_ITER_H

#ifndef __cplusplus
#error dtable_skip_iter.h is a C++ header file
#endif

#include "dtable_wrap_iter.h"

/* These dtable iterator wrappers automate the task of skipping certain keys or
 * values, at the cost of an additional virtual function call compared to
 * integrating the logic into wherever this functionality is needed. Most
 * commonly this would be used to skip nonexistent values with dne_skip_test. */

template<class T>
class dtable_skip_iter_noindex : public dtable_wrap_iter_noindex
{
public:
	inline virtual bool next()
	{
		return advance(true);
	}
	
	inline virtual bool prev()
	{
		return retreat(true);
	}
	
	inline virtual bool first()
	{
		if(!base->first())
			return false;
		return advance();
	}
	
	inline virtual bool last()
	{
		if(!base->last())
			return false;
		return retreat();
	}
	
	inline virtual bool seek(const dtype & key)
	{
		bool found = base->seek(key);
		if(found && !skip(base))
			return true;
		advance();
		return false;
	}
	
	inline virtual bool seek(const dtype_test & test)
	{
		bool found = base->seek(test);
		if(found && !skip(base))
			return true;
		advance();
		return false;
	}
	
	inline dtable_skip_iter_noindex(dtable::iter * base, bool claim_base = false)
		: dtable_wrap_iter_noindex(base, claim_base)
	{
		advance();
	}
	inline dtable_skip_iter_noindex(dtable::iter * base, const T & skip, bool claim_base = false)
		: dtable_wrap_iter_noindex(base, claim_base), skip(skip)
	{
		advance();
	}
	inline virtual ~dtable_skip_iter_noindex() {}
	
protected:
	T skip;
	
	inline bool advance(bool initial = false)
	{
		bool valid;
		if(initial)
			while((valid = base->next()) && skip(base));
		else
		{
			valid = base->valid();
			while(valid && skip(base))
				valid = base->next();
		}
		return valid;
	}
	
	inline bool retreat(bool initial = false)
	{
		bool valid;
		if(initial)
			while((valid = base->prev()) && skip(base));
		else
		{
			valid = base->valid();
			while(valid && skip(base))
				valid = base->prev();
		}
		if(!valid)
			advance();
		return valid;
	}
};

template<class T>
class dtable_skip_iter : public dtable_skip_iter_noindex<T>
{
public:
	inline virtual bool seek_index(size_t index)
	{
		bool found = this->base->seek_index(index);
		if(found && !skip(this->base))
			return true;
		this->advance();
		return false;
	}
	
	/* we have to reimplement this since we're not inheriting it through dtable_wrap_iter */
	inline virtual size_t get_index() const { return this->base->get_index(); }
	
	inline dtable_skip_iter(dtable::iter * base, bool claim_base = false)
		: dtable_skip_iter_noindex<T>(base, claim_base) {}
	inline dtable_skip_iter(dtable::iter * base, const T & skip, bool claim_base = false)
		: dtable_skip_iter_noindex<T>(base, skip, claim_base) {}
	inline virtual ~dtable_skip_iter() {}
};

struct dne_skip_test
{
	inline bool operator()(const dtable::iter * iter)
	{
		return !iter->meta().exists();
	}
};

typedef dtable_skip_iter_noindex<dne_skip_test> dtable_skip_dne_iter_noindex;
typedef dtable_skip_iter<dne_skip_test> dtable_skip_dne_iter;

#endif /* __DTABLE_SKIP_ITER_H */
