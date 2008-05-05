/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "sub_blob.h"
#include "simple_ctable.h"

simple_ctable::iter::iter(dtable_iter * src)
	: source(src), columns(NULL)
{
	while(source->valid())
	{
		blob value = source->value();
		if(value.exists())
		{
			row = sub_blob(value);
			columns = row.iterator();
			if(columns->valid())
				break;
			delete columns;
			columns = NULL;
		}
		source->next();
	}
}

bool simple_ctable::iter::valid() const
{
	return columns ? columns->valid() : false;
}

bool simple_ctable::iter::next()
{
	if(columns)
	{
		if(columns->next())
			return true;
		delete columns;
		columns = NULL;
	}
	for(;;)
	{
		if(!source->next())
			return false;
		blob value = source->value();
		if(!value.exists())
			continue;
		row = sub_blob(value);
		columns = row.iterator();
		if(columns->valid())
			return true;
		delete columns;
		columns = NULL;
	}
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
	if(!row.exists())
		return row;
	/* not super efficient, but we can fix it later */
	sub_blob columns(row);
	return columns.get(column);
}

/* if we implemented our own find(), this could avoid flattening every time */
int writable_simple_ctable::append(dtype key, const char * column, const blob & value)
{
	int r = 0;
	blob row = wdt_source->find(key);
	if(row.exists() || value.exists())
	{
		sub_blob columns(row);
		columns.set(column, value);
		r = wdt_source->append(key, columns.flatten());
	}
	return r;
}

int writable_simple_ctable::remove(dtype key, const char * column, bool gc_row)
{
	int r = append(key, column, blob());
	if(r >= 0 && gc_row)
	{
		/* TODO: improve this... */
		blob row = wdt_source->find(key);
		sub_blob columns(row);
		sub_blob_iter * iter = columns.iterator();
		bool last = !iter->valid();
		delete iter;
		if(last)
			remove(key);
	}
	return r;
}
