/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <stdarg.h>

#include "overlay_dtable.h"

overlay_dtable::iter::iter(const overlay_dtable * source)
	: ovr_source(source), lastdir(FORWARD), past_beginning(false)
{
	subs = new sub[source->table_count];
	for(size_t i = 0; i < source->table_count; i++)
	{
		subs[i].iter = source->tables[i]->iterator();
		subs[i].empty = !subs[i].iter->valid();
		subs[i].valid = subs[i].iter->valid();
		subs[i].shadow = false;
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
	return current_index < ovr_source->table_count;
}

/* this will let non-existent blobs shadow extant ones just like we want
 * without any special handling, since next() and valid() still return true */
bool overlay_dtable::iter::next()
{
	bool first = true;
	dtype min_key(0u);
	const blob_comparator * blob_cmp = ovr_source->blob_cmp;
	current_index = ovr_source->table_count;
	
	if(lastdir == BACKWARD)
	{
		for(size_t i = 0; i < ovr_source->table_count; i++)
		{
			assert(subs[i].empty || subs[i].valid);
			if(subs[i].empty && !subs[i].valid)
			{
				subs[i].empty = !subs[i].iter->valid();
				subs[i].valid = subs[i].iter->valid();
			}
			else
			{
				subs[i].empty = true;
				subs[i].valid = true;
			}
			subs[i].shadow = false;
		}
		lastdir = FORWARD;
		if(past_beginning)
		{
			past_beginning = false;
			next();
		}
	}
	
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		int c;
		if(subs[i].empty && subs[i].valid)
		{
			/* fill in empty slots */
			subs[i].valid = subs[i].iter->next();
			subs[i].empty = !subs[i].valid;
		}
		if(!subs[i].valid || subs[i].shadow)
			/* skip exhausted and shadowed tables */
			continue;
		if(first || (c = subs[i].iter->key().compare(min_key, blob_cmp)) < 0)
		{
			first = false;
			current_index = i;
			min_key = subs[i].iter->key();
		}
		else if(!c)
			/* shadow this entry */
			subs[i].shadow = true;
	}
	if(current_index == ovr_source->table_count)
		return false;
	subs[current_index].empty = true;
	for(size_t i = current_index + 1; i < ovr_source->table_count; i++)
		if(subs[i].shadow)
		{
			/* skip shadowed entry */
			subs[i].empty = true;
			subs[i].shadow = false;
		}
	return true;
}

bool overlay_dtable::iter::prev()
{
	bool first = true;
	dtype max_key(0u);
	const blob_comparator * blob_cmp = ovr_source->blob_cmp;
	size_t next_index = ovr_source->table_count;
	
	if(lastdir == FORWARD)
	{
		for(size_t i = 0; i < ovr_source->table_count; i++)
		{
			assert(subs[i].empty || subs[i].valid);
			subs[i].empty = true;
			subs[i].valid = true;
			subs[i].shadow = false;
		}
		lastdir = BACKWARD;
	}
	
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		int c;
		if(subs[i].empty && subs[i].valid)
		{
			/* fill in empty slots */
			subs[i].valid = subs[i].iter->prev();
			subs[i].empty = !subs[i].valid;
		}
		if(!subs[i].valid || subs[i].shadow)
			/* skip exhausted and shadowed tables */
			continue;
		if(first || (c = subs[i].iter->key().compare(max_key, blob_cmp)) > 0)
		{
			first = false;
			next_index = i;
			max_key = subs[i].iter->key();
		}
		else if(!c)
			/* shadow this entry */
			subs[i].shadow = true;
	}
	if(next_index == ovr_source->table_count)
	{
		/* we have gone "past the beginning" and when we reverse direction
		 * again, we'll find the first element rather than the second as we
		 * should (since we're supposed to stay at the first element)... so,
		 * set the past_beginning flag so we'll know to fix it up later */
		past_beginning = true;
		return false;
	}
	current_index = next_index;
	subs[current_index].empty = true;
	for(size_t i = current_index + 1; i < ovr_source->table_count; i++)
		if(subs[i].shadow)
		{
			/* skip shadowed entry */
			subs[i].empty = true;
			subs[i].shadow = false;
		}
	return true;
}

bool overlay_dtable::iter::first()
{
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		subs[i].iter->first();
		subs[i].empty = !subs[i].iter->valid();
		subs[i].valid = subs[i].iter->valid();
		subs[i].shadow = false;
	}
	lastdir = FORWARD;
	past_beginning = false;
	return next();
}

bool overlay_dtable::iter::last()
{
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		subs[i].iter->last();
		if(subs[i].iter->valid())
			subs[i].iter->next();
		subs[i].valid = false;
		subs[i].empty = true;
		subs[i].shadow = false;
	}
	lastdir = FORWARD;
	past_beginning = false;
	return prev();
}

dtype overlay_dtable::iter::key() const
{
	return subs[current_index].iter->key();
}

dtype::ctype overlay_dtable::iter::key_type() const
{
	return ovr_source->key_type();
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
		subs[i].shadow = false;
	}
	lastdir = FORWARD;
	past_beginning = false;
	next();
	return found;
}

bool overlay_dtable::iter::seek(const dtype_test & test)
{
	bool found = false;
	for(size_t i = 0; i < ovr_source->table_count; i++)
	{
		if(subs[i].iter->seek(test))
			found = true;
		subs[i].empty = !subs[i].iter->valid();
		subs[i].valid = subs[i].iter->valid();
		subs[i].shadow = false;
	}
	lastdir = FORWARD;
	past_beginning = false;
	next();
	return found;
}

metablob overlay_dtable::iter::meta() const
{
	return subs[current_index].iter->meta();
}

blob overlay_dtable::iter::value() const
{
	return subs[current_index].iter->value();
}

const dtable * overlay_dtable::iter::source() const
{
	return subs[current_index].iter->source();
}

dtable::iter * overlay_dtable::iterator() const
{
	return new iter(this);
}

blob overlay_dtable::lookup(const dtype & key, bool * found) const
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		blob value = tables[i]->lookup(key, found);
		if(*found)
			return value;
	}
	*found = false;
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
	dtable::deinit();
}
