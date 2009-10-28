/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_SKIP_ITER_H
#define __DTABLE_SKIP_ITER_H

#ifndef __cplusplus
#error dtable_skip_iter.h is a C++ header file
#endif

#include "dtable_wrap_iter.h"

/* these dtable iterator wrappers automate the task of skipping nonexistent
 * values, at the cost of an additional virtual function call compared to
 * integrating the logic into wherever this functionality is needed */

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
		if(found && base->meta().exists())
			return true;
		advance();
		return false;
	}
	
	inline virtual bool seek(const dtype_test & test)
	{
		bool found = base->seek(test);
		if(found && base->meta().exists())
			return true;
		advance();
		return false;
	}
	
	inline dtable_skip_iter_noindex(dtable::iter * base, bool claim_base = false) : dtable_wrap_iter_noindex(base, claim_base)
	{
		advance();
	}
	inline virtual ~dtable_skip_iter_noindex() {}
	
protected:
	inline bool advance(bool initial = false)
	{
		bool valid;
		if(initial)
			while((valid = base->next()) && !base->meta().exists());
		else
		{
			valid = base->valid();
			while(valid && !base->meta().exists())
				valid = base->next();
		}
		return valid;
	}
	
	inline bool retreat(bool initial = false)
	{
		bool valid;
		if(initial)
			while((valid = base->prev()) && !base->meta().exists());
		else
		{
			valid = base->valid();
			while(valid && !base->meta().exists())
				valid = base->prev();
		}
		if(!valid)
			advance();
		return valid;
	}
};

class dtable_skip_iter : public dtable_skip_iter_noindex
{
public:
	inline virtual bool seek_index(size_t index)
	{
		bool found = base->seek_index(index);
		if(found && base->meta().exists())
			return true;
		advance();
		return false;
	}
	
	/* we have to reimplement this since we're not inheriting it through dtable_wrap_iter */
	inline virtual size_t get_index() const { return base->get_index(); }
	
	inline dtable_skip_iter(dtable::iter * base, bool claim_base = false) : dtable_skip_iter_noindex(base, claim_base) {}
	inline virtual ~dtable_skip_iter() {}
};

#endif /* __DTABLE_SKIP_ITER_H */
