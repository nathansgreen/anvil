/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <stdarg.h>

#include "overlay_dtable.h"

overlay_dtable::iter::iter(const overlay_dtable * source)
	: source(source)
{
	size_t i;
	subs = new sub[source->table_count];
	for(i = 0; i < source->table_count; i++)
	{
		subs[i].iter = source->tables[i]->iterator();
		subs[i].empty = true;
		subs[i].valid = true;
	}
	next();
}

bool overlay_dtable::iter::valid() const
{
	return next_index < source->table_count;
}

/* this will let negative entry blobs shadow positive ones just like we want
 * without any special handling, since next() and valid() still return true */
bool overlay_dtable::iter::next()
{
	size_t i;
	bool first = true;
	dtype min_key((uint32_t) 0);
	next_index = source->table_count;
	for(i = 0; i < source->table_count; i++)
	{
		if(subs[i].empty)
		{
			/* fill in empty slots */
			subs[i].empty = false;
			subs[i].valid = subs[i].iter->next();
		}
		if(!subs[i].valid)
			/* skip exhausted tables */
			continue;
		if(first || subs[i].iter->key() < min_key)
		{
			first = false;
			next_index = i;
			min_key = subs[i].iter->key();
		}
		else if(subs[i].iter->key() == min_key)
			/* skip shadowed entry */
			subs[i].empty = true;
	}
	if(next_index == source->table_count)
		return false;
	subs[i].empty = true;
	return true;
}

dtype overlay_dtable::iter::key() const
{
	return subs[next_index].iter->key();
}

blob overlay_dtable::iter::value() const
{
	return subs[next_index].iter->value();
}

const dtable * overlay_dtable::iter::extra() const
{
	return subs[next_index].iter->extra();
}

sane_iter3<dtype, blob, const dtable *> * overlay_dtable::iterator() const
{
	return new iter(this);
}

blob overlay_dtable::lookup(dtype key, const dtable ** source) const
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		blob value = tables[i]->lookup(key, source);
		if(*source)
			return value;
	}
	*source = NULL;
	return blob();
}

int overlay_dtable::init(const dtable * dt1, ...)
{
	va_list ap;
	size_t count = 1;
	dtable * table;
	
	va_start(ap, dt1);
	while((table = va_arg(ap, dtable *)))
	{
		/* check key type? */
		count++;
	}
	va_end(ap);
	
	tables = new const dtable *[count];
	if(!tables)
		return -ENOMEM;
	table_count = count;
	tables[0] = dt1;
	
	va_start(ap, dt1);
	for(count = 1; count < table_count; count++)
		tables[count] = va_arg(ap, dtable *);
	va_end(ap);
	return 0;
}

int overlay_dtable::init(const dtable ** dts, size_t count)
{
	if(count < 1)
		return -EINVAL;
	
	/* check key types? */
	
	tables = new const dtable *[count];
	if(!tables)
		return -ENOMEM;
	table_count = count;
	memcpy(tables, dts, sizeof(*dts) * count);
	return 0;
}

void overlay_dtable::deinit()
{
	if(!tables)
		return;
	delete[] tables;
	tables = NULL;
	table_count = 0;
}
