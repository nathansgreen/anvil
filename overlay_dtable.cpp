/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <stdarg.h>

#include "overlay_dtable.h"

overlay_dtable::iter::iter(const overlay_dtable * source)
	: ovr_source(source)
{
	subs = new sub[source->table_count];
	for(size_t i = 0; i < source->table_count; i++)
	{
		subs[i].iter = source->tables[i]->iterator();
		subs[i].empty = !subs[i].iter->valid();
		subs[i].valid = subs[i].iter->valid();
	}
	next();
}

overlay_dtable::iter::~iter()
{
	for(size_t i = 0; i < ovr_source->table_count; i++)
		delete subs[i].iter;
	delete[] subs;
}

bool overlay_dtable::iter::valid() const
{
	return next_index < ovr_source->table_count;
}

/* this will let non-existent blobs shadow positive ones just like we want
 * without any special handling, since next() and valid() still return true */
bool overlay_dtable::iter::next()
{
	bool first = true;
	dtype min_key((uint32_t) 0);
	next_index = ovr_source->table_count;
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		if(subs[i].empty && subs[i].valid)
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
	if(next_index == ovr_source->table_count)
		return false;
	subs[next_index].empty = true;
	return true;
}

dtype overlay_dtable::iter::key() const
{
	return subs[next_index].iter->key();
}

bool overlay_dtable::iter::seek(const dtype & key)
{
	bool found = false;
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		if(subs[i].iter->seek(key))
			found = true;
		subs[i].empty = !subs[i].iter->valid();
		subs[i].valid = subs[i].iter->valid();
	}
	next();
	return found;
}

metablob overlay_dtable::iter::meta() const
{
	return subs[next_index].iter->meta();
}

blob overlay_dtable::iter::value() const
{
	return subs[next_index].iter->value();
}

const dtable * overlay_dtable::iter::source() const
{
	return subs[next_index].iter->source();
}

dtable::iter * overlay_dtable::iterator() const
{
	return new iter(this);
}

blob overlay_dtable::lookup(const dtype & key, const dtable ** source) const
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
	
	if(tables)
		deinit();
	ktype = dt1->key_type();
	va_start(ap, dt1);
	while((table = va_arg(ap, dtable *)))
	{
		if(table->key_type() != ktype)
		{
			va_end(ap);
			return -EINVAL;
		}
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
	if(tables)
		deinit();
	ktype = dts[0]->key_type();
	for(size_t i = 1; i < count; i++)
		if(dts[i]->key_type() != ktype)
			return -EINVAL;
	
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
