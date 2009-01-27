/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_ITER_FILTER_H
#define __DTABLE_ITER_FILTER_H

#ifndef __cplusplus
#error dtable_iter_filter.h is a C++ header file
#endif

#include "dtable.h"

/* The dtable_iter_filter template provides a convenient way for specialized
 * dtables to implement the filter_iterator() method, so that they can be used
 * from exception_dtable via factories. They need only define a class that keeps
 * any necessary state and provides an accept() method, which will be used as a
 * parent class of the template. See array_dtable for an example. */

/* dtable iterator filters created using this template automatically support the
 * reject_value config parameter (a blob), to allow setting what rejected values
 * will be replaced with in the filtered output */

template<class T>
class dtable_iter_filter : public dtable::iter, public T
{
public:
	virtual bool valid() const
	{
		return base->valid();
	}
	virtual bool next()
	{
		if(base->next())
		{
			check_reject();
			return true;
		}
		rejected = false;
		hit_end = true;
		return false;
	}
	virtual bool prev()
	{
		/* no need to worry about hit_end or even adding to rejects
		 * here - we've already seen all of this, and we certainly
		 * won't get to the end by going backwards */
		bool valid = base->prev();
		if(valid)
			check_reject(false);
		else
			rejected = false;
		return valid;
	}
	/* don't support last() for now; it's probably not needed */
	virtual bool last() { abort(); }
	virtual bool first()
	{
		bool valid = base->first();
		if(valid)
			check_reject(false);
		else
			rejected = false;
		return valid;
	}
	virtual dtype key() const
	{
		return base->key();
	}
	/* don't support seeking for now; it's probably not needed */
	virtual bool seek(const dtype & key) { abort(); }
	virtual bool seek(const dtype_test & test) { abort(); }
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
	virtual metablob meta() const
	{
		return rejected ? metablob(reject_value) : base->meta();
	}
	virtual blob value() const
	{
		return rejected ? reject_value : base->value();
	}
	virtual const dtable * source() const
	{
		return base->source();
	}
	
	inline dtable_iter_filter() : base(NULL), rejects(NULL) {}
	inline int init(dtable::iter * source, const params & config, dtable * rejects, bool claim_base = false)
	{
		int r = T::init(config);
		if(r < 0)
			return r;
		if(!rejects->writable())
			return -EINVAL;
		if(!config.get("reject_value", &reject_value, blob()))
			return -EINVAL;
		/* just to be sure */
		source->first();
		base = source;
		this->rejects = rejects;
		this->claim_base = claim_base;
		hit_end = false;
		if(base->valid())
			check_reject();
		else
		{
			rejected = false;
			hit_end = true;
		}
		return 0;
	}
	virtual ~dtable_iter_filter()
	{
		if(base && claim_base)
			delete base;
	}
	
private:
	void check_reject(bool add_rejects = true)
	{
		assert(base->valid());
		rejected = !T::accept(const_cast<const dtable::iter *>(base));
		if(rejected && !hit_end && add_rejects && rejects->insert(base->key(), base->value()) < 0)
			/* by not rejecting something that should be rejected, we'll
			 * trigger an error elsewhere, which is what we want */
			rejected = false;
	}
	
	dtable::iter * base;
	dtable * rejects;
	blob reject_value;
	bool claim_base, rejected, hit_end;
};

#endif /* __DTABLE_ITER_FILTER_H */
