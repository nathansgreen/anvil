/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "dt_simple_index.h"

dtype dt_simple_index::map(dtype key) const
{
	/* give the unique pri for this key; only makes sense if unique is true */
}

dt_index_iter * dt_simple_index::iterator(dtype key) const
{
	/* iterate over only this one key */
}

dt_index_iter * dt_simple_index::iterator() const
{
	/* iterate over all keys */
}

int dt_simple_index::set(dtype key, dtype pri)
{
	/* for unique: set the pri for this key, even if it does not yet exist */
}

int dt_simple_index::remove(dtype key)
{
	/* for unique: remove the pri for this key */
}

int dt_simple_index::add(dtype key, dtype pri)
{
	/* for !unique: add this pri to this key if it is not already there */
}

int dt_simple_index::update(dtype key, dtype old_pri, dtype new_pri)
{
	/* for !unique: change this key's mapping to old_pri to new_pri */
}

int dt_simple_index::remove(dtype key, dtype pri)
{
	/* for !unique: remove this pri from this key */
}

int dt_simple_index::init(const dtable * store, journal_dtable * append, bool unique)
{
	/* any further checking here? */
	this->unique = unique;
	this->store = store;
	this->append = append;
	return 0;
}
