/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

/* Currently the exception dtable will check if a key exists in a base
 * table and if not it will check an alternative table. Eddie proposed
 * a version where the exception dtable would be parameterized with a function
 * that would be used to determine which table to check first. We currently
 * don't support this now, but it should be simple to add. */

#include "exception_dtable.h"

exception_dtable::iter::iter(const exception_dtable * source)
	: iter_source<exception_dtable>(source), lastdir(FORWARD)
{
	base_iter = new sub;
	alternatives_iter = new sub;
	base_iter->iter = dt_source->base->iterator();
	alternatives_iter->iter = dt_source->alternatives->iterator();
	first();
}

bool exception_dtable::iter::valid() const
{
	return current_iter->iter->valid();
}

bool exception_dtable::iter::next()
{
	const blob_comparator * blob_cmp = dt_source->blob_cmp;
	sub * other_iter = (current_iter == base_iter) ? alternatives_iter : base_iter;
	if(lastdir != FORWARD)
	{
		while(other_iter->iter->valid() &&
				 (other_iter->iter->key().compare(current_iter->iter->key(), blob_cmp) <= 0))
		{
			other_iter->valid = other_iter->iter->next();
		}
		lastdir = FORWARD;
	}
	current_iter->valid = current_iter->iter->next();
	if(current_iter->valid)
	{
		if(other_iter->valid)
		{
			int comp = current_iter->iter->key().compare(other_iter->iter->key(), blob_cmp);
			if(comp == 0)
			{
				other_iter->valid = other_iter->iter->next();
			}
			if(comp <= 0)
				return true;
		}
		else
			return true;
	}

	if(other_iter->valid)
	{
		current_iter = other_iter;
		return true;
	}

	return false;
}

bool exception_dtable::iter::prev()
{
	const blob_comparator * blob_cmp = dt_source->blob_cmp;
	sub * other_iter = (current_iter == base_iter) ? alternatives_iter : base_iter;
	if(lastdir != BACKWARD)
	{
		/* Are we at the last element ? */
		if(!other_iter->valid)
			other_iter->valid = other_iter->iter->prev();
		while(other_iter->valid &&
				 (other_iter->iter->key().compare(current_iter->iter->key(), blob_cmp) >= 0))
		{
			other_iter->valid = other_iter->iter->prev();
			if(!other_iter->valid)
				break;
		}
		lastdir = BACKWARD;
	}
	current_iter->valid = current_iter->iter->prev();
	if(current_iter->valid)
	{
		if(other_iter->valid)
		{
			int comp = current_iter->iter->key().compare(other_iter->iter->key(), blob_cmp);
			if(comp == 0)
				other_iter->valid = other_iter->iter->prev();
			if(comp >= 0)
				return true;
		}
		else
			return true;
	}

	if(other_iter->valid)
	{
		current_iter = other_iter;
		return true;
	}

	return false;
}

bool exception_dtable::iter::first()
{
	bool base_first = base_iter->iter->first();
	bool alternatives_first = alternatives_iter->iter->first();
	if(!(base_first || alternatives_first))
		return false;
	if(base_first && alternatives_first)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		if(base_iter->iter->key().compare(alternatives_iter->iter->key(), blob_cmp) <= 0)
			current_iter = base_iter;
		else
			current_iter = alternatives_iter;
	}
	else
		current_iter = base_first ? base_iter : alternatives_iter;

	base_iter->valid = base_first;
	alternatives_iter->valid = alternatives_first;

	lastdir = FORWARD;

	return true;
}

bool exception_dtable::iter::last()
{
	bool base_last = base_iter->iter->last();
	bool exception_last = alternatives_iter->iter->last();
	if(!(base_last || exception_last))
		return false;
	if(base_last && exception_last)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		if(base_iter->iter->key().compare(alternatives_iter->iter->key(), blob_cmp) >= 0)
			current_iter = base_iter;
		else
			current_iter = alternatives_iter;
	}
	else
		current_iter = base_last ? base_iter : alternatives_iter;

	lastdir = FORWARD;
	return true;
}

dtype exception_dtable::iter::key() const
{
	return current_iter->iter->key();
}

bool exception_dtable::iter::seek(const dtype & key)
{
	bool base_seek = base_iter->iter->seek(key);
	bool exception_seek = alternatives_iter->iter->seek(key);
	
	lastdir = FORWARD;

	if(base_seek)
		current_iter = base_iter;
	else if(exception_seek)
		current_iter = alternatives_iter;
	else
		return false;
	return true;
}

bool exception_dtable::iter::seek(const dtype_test & test)
{
	bool base_seek = base_iter->iter->seek(test);
	bool exception_seek = alternatives_iter->iter->seek(test);
	
	lastdir = FORWARD;
	
	if(base_seek)
		current_iter = base_iter;
	else if(exception_seek)
		current_iter = alternatives_iter;
	else
		return false;
	return true;
}

metablob exception_dtable::iter::meta() const
{
	return current_iter->iter->meta();
}

blob exception_dtable::iter::value() const
{
	return current_iter->iter->value();
}

const dtable * exception_dtable::iter::source() const
{
	return current_iter->iter->source();
}

exception_dtable::iter::~iter()
{
	if(base_iter)
	{
		delete base_iter->iter;
		delete base_iter;
	}
	if(alternatives_iter)
	{
		delete alternatives_iter->iter;
		delete alternatives_iter;
	}
	current_iter = NULL;
}

dtable::iter * exception_dtable::iterator() const
{
	return new iter(this);
}

blob exception_dtable::lookup(const dtype & key, bool * found) const
{
	blob value = base->lookup(key, found);
	if(*found)
		return value;
	value = alternatives->lookup(key, found);
	return value;
}

dtype exception_dtable::get_key(size_t index) const
{
	return dtype(0.0);
}

blob exception_dtable::get_value(size_t index) const
{
	dtype key = get_key(index);
	bool f;
	return lookup(key, &f);
}

int exception_dtable::init(const dtable * dt, const dtable * edt)
{
	if(base || alternatives)
		deinit();
	assert(dt && edt);
	base = dt;
	alternatives = edt;
	ktype = dt->key_type();
	if(edt->key_type() != ktype)
		return -EINVAL;
	return 0;
}

void exception_dtable::deinit()
{
	if(!base && !alternatives)
		return;
	base = NULL;
	alternatives = NULL;
	dtable::deinit();
}
