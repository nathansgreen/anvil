/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "sub_blob.h"
#include "simple_ctable.h"

bool simple_ctable::iter::valid() const
{
	return columns ? columns->valid() : false;
}

bool simple_ctable::iter::next()
{
	if(columns)
	{
		delete columns;
		columns = NULL;
	}
	do {
		if(!source->next())
			return false;
	} while(source->value().negative());
	row = sub_blob(source->value());
	columns = row.iterator();
	return columns->valid();
}

dtype simple_ctable::iter::key() const
{
	return source->key();
}

const char * simple_ctable::iter::column() const
{
	return columns->column();
}

blob simple_ctable::iter::value() const
{
	return columns->value();
}

ctable_iter * simple_ctable::iterator() const
{
	return new iter(dt_source->iterator());
}

blob simple_ctable::find(dtype key, const char * column) const
{
	blob row = dt_source->find(key);
	if(row.negative())
		return row;
	/* not super efficient, but we can fix it later */
	sub_blob columns(row);
	return columns.get(column);
}

int simple_ctable::init(dtable * source)
{
	dt_source = source;
	return 0;
}
