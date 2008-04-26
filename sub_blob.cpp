/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

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

bool sub_blob::iter::valid() const
{
	return current != NULL;
}

bool sub_blob::iter::next()
{
	if(current)
		current = current->next;
	return current != NULL;
}

const char * sub_blob::iter::column() const
{
	return current->name;
}

blob sub_blob::iter::value() const
{
	return current->value;
}

blob sub_blob::extract(const char * column) const
{
	if(base.size() < 1)
		/* no columns */
		return blob();
	size_t offset = 1, size = strlen(column);
	uint8_t length_size = base[0];
	while(offset + length_size + 1 < base.size())
	{
		size_t length = read_bytes(&base[0], &offset, length_size);
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

blob sub_blob::get(const char * column) const
{
	override * ovr = find(column);
	if(ovr)
		return ovr->value;
	blob value = extract(column);
	/* don't add negative entries to the override list */
	if(!value.negative())
		/* the override list is just a cache for the purposes of get(), so we can cast it */
		new override(column, extract(column), const_cast<override **>(&overrides));
	return value;
}

int sub_blob::set(const char * column, const blob & value)
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

int sub_blob::remove(const char * column)
{
	return set(column, blob());
}

blob sub_blob::flatten(bool internalize)
{
	size_t count = 0, total_size = 1;
	uint32_t max_length = 0;
	uint8_t length_size;
	if(!modified)
		return base;
	populate();
	for(override * scan = overrides; scan; scan = scan->next)
	{
		if(scan->value.negative())
			continue;
		count++;
		total_size += strlen(scan->name);
		total_size += scan->value.size();
		if(scan->value.size() > max_length)
			max_length = scan->value.size();
	}
	length_size = byte_size(max_length);
	total_size += count * (length_size + 1);
	
	blob flat(total_size);
	uint8_t * memory = flat.memory();
	total_size = 1;
	memory[0] = length_size;
	for(override * scan = overrides; scan; scan = scan->next)
	{
		uint8_t name = strlen(scan->name);
		if(scan->value.negative())
			continue;
		layout_bytes(memory, &total_size, scan->value.size(), length_size);
		memory[total_size++] = name;
		memcpy(&memory[total_size], scan->name, name);
		memcpy(&memory[total_size += name], &scan->value[0], scan->value.size());
		total_size += scan->value.size();
	}
	assert(total_size == flat.size());
	if(internalize)
	{
		base = flat;
		while(overrides)
			delete overrides;
	}
	return flat;
}

sub_blob_iter * sub_blob::iterator() const
{
	populate();
	return new iter(overrides);
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
		override * ovr;
		size_t length = read_bytes(&base[0], &offset, length_size);
		size_t label = base[offset++];
		char * name = (char *) malloc(label + 1);
		assert(name);
		
		memcpy(name, &base[offset], label);
		name[label] = 0;
		offset += label;
		
		ovr = find(name);
		if(!ovr)
			/* it hasn't been populated yet, so do it now */
			/* see earlier note about the override list and constness */
			new override(name, blob(length, &base[offset]), const_cast<override **>(&overrides));
		
		offset += length;
		free(name);
	}
}

sub_blob::override * sub_blob::find(const char * column) const
{
	for(override * scan = overrides; scan; scan = scan->next)
		if(!strcmp(scan->name, column))
			return scan;
	return NULL;
}
