/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <assert.h>

#include "util.h"
#include "blob_buffer.h"
#include "sub_blob.h"

/* sub_blob is one way you can nest blobs inside other blobs (e.g. to implement
 * table columns, or even higher-dimensional data structures on top of dtables) */

/* sub_blob is not currently optimized for dealing with large numbers of columns
 * in a single blob; it could be adapted to do this without too much trouble,
 * but a more important optimization is to make a version of simple_dtable that
 * knows about the embedded column names and pulls them out into a string table
 * so they are not stored once per row */

/* flat blob format:
 * byte 0: length size (1-4)
 * bytes 1-n: sub-blobs
 * 
 * sub-blob:
 * byte 0-n: length
 * byte n+1: column name length
 * bytes n+2-m: column name
 * bytes m+1-o: blob data */

bool sub_blob::named_iter::valid() const
{
	return current != NULL;
}

bool sub_blob::named_iter::prev()
{
	if(current)
	{
		if(current->pprev != &source->overrides)
			current = (override *) current->pprev;
	}
	else
		current = previous;
	return current != NULL;
}

bool sub_blob::named_iter::next()
{
	if(current)
	{
		previous = current;
		current = current->next;
	}
	return current != NULL;
}

bool sub_blob::named_iter::last()
{
	while(current && current->next)
	{
		previous = current;
		current = current->next;
	}
	return current != NULL;
}

const istr & sub_blob::named_iter::column() const
{
	return current->name;
}

blob sub_blob::named_iter::value() const
{
	return current->value;
}

blob sub_blob::extract(const istr & column) const
{
	if(base.size() < 1)
		/* no columns */
		return blob();
	size_t offset = 1, size = strlen(column);
	uint8_t length_size = base[0];
	while(offset + length_size + 1 < base.size())
	{
		size_t length = util::read_bytes(&base[0], &offset, length_size);
		size_t label = base[offset++];
		if(label != size || memcmp(&base[offset], column, label))
		{
			offset += label + length;
			continue;
		}
		offset += label;
		return blob(length, &base[offset]);
	}
	return blob();
}

blob sub_blob::get(const istr & column) const
{
	override * ovr = find(column);
	if(ovr)
		return ovr->value;
	blob value = extract(column);
	/* don't add non-existent blobs to the override list */
	if(value.exists())
		new override(column, value, &overrides);
	return value;
}

int sub_blob::set(const istr & column, const blob & value)
{
	override * ovr = find(column);
	if(ovr)
	{
		ovr->value = value;
		return 0;
	}
	/* we only store the column name length as one byte */
	if(strlen(column) > 0xFF)
		return -EINVAL;
	if(!new override(column, value, &overrides))
		return -ENOMEM;
	modified = true;
	return 0;
}

int sub_blob::remove(const istr & column)
{
	return set(column, blob());
}

blob sub_blob::flatten(bool internalize)
{
	blob flat = const_cast<const sub_blob *>(this)->flatten();
	if(internalize)
	{
		base = flat;
		while(overrides)
			delete overrides;
	}
	return flat;
}

blob sub_blob::flatten() const
{
	size_t count = 0, total_size = 1;
	uint32_t max_length = 0;
	uint8_t length_size;
	if(!modified)
		return base;
	populate();
	for(override * scan = overrides; scan; scan = scan->next)
	{
		if(!scan->value.exists())
			continue;
		count++;
		total_size += strlen(scan->name);
		total_size += scan->value.size();
		if(scan->value.size() > max_length)
			max_length = scan->value.size();
	}
	length_size = util::byte_size(max_length);
	total_size += count * (length_size + 1);
	
	blob_buffer flat(total_size);
	flat << length_size;
	for(override * scan = overrides; scan; scan = scan->next)
	{
		uint8_t name = strlen(scan->name);
		if(!scan->value.exists())
			continue;
		flat.layout_append(scan->value.size(), length_size);
		flat << name;
		flat.append(scan->name, name);
		flat.append(scan->value);
	}
	assert(total_size == flat.size());
	return flat;
}

sub_blob::iter * sub_blob::iterator() const
{
	populate();
	return new named_iter(this);
}

void sub_blob::populate() const
{
	if(base.size() < 1)
		/* no columns */
		return;
	size_t offset = 1;
	uint8_t length_size = base[0];
	while(offset + length_size + 1 < base.size())
	{
		size_t length = util::read_bytes(&base[0], &offset, length_size);
		size_t label = base[offset++];
		istr name(&base.index<char>(offset), label);
		
		offset += label;
		assert(offset + length <= base.size());
		
		if(!find(name))
			/* it hasn't been populated yet, so do it now */
			new override(name, blob(length, &base[offset]), &overrides);
		
		offset += length;
	}
}

sub_blob::override * sub_blob::find(const istr & column) const
{
	for(override * scan = overrides; scan; scan = scan->next)
		if(!strcmp(scan->name, column))
			return scan;
	return NULL;
}
