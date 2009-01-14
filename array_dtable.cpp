/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include "rwfile.h"
#include "blob_buffer.h"
#include "array_dtable.h"

array_dtable::iter::iter(const array_dtable * source)
	: index(0), adt_source(source)
{
}

bool array_dtable::iter::valid() const
{
	return index < adt_source->array_size;
}

bool array_dtable::iter::next()
{
	if(index == adt_source->array_size)
		return false;
	while(++index < adt_source->array_size && adt_source->is_hole(index));
	return index < adt_source->array_size;
}

bool array_dtable::iter::prev()
{
	if(index == adt_source->min_key)
		return false;
	while(--index > adt_source->min_key && adt_source->is_hole(index));
	/* the first index can't be a hole, or it wouldn't be the first index */
	return true;
}

bool array_dtable::iter::first()
{
	if(!adt_source->array_size)
		return false;
	index = adt_source->min_key;
	return true;
}

bool array_dtable::iter::last()
{
	if(!adt_source->array_size)
		return false;
	index = adt_source->array_size - 1;
	return true;
}

dtype array_dtable::iter::key() const
{
	return dtype(index + adt_source->min_key);
}

bool array_dtable::iter::seek(const dtype & key)
{
	assert(key.type == dtype::UINT32);
	if(adt_source->is_hole(key.u32 - adt_source->min_key))
		return false;
	index = key.u32 - adt_source->min_key;
	return true;
}

bool array_dtable::iter::seek(const dtype_test & test)
{
	return adt_source->find_key(test, &index) >= 0;
}

bool array_dtable::iter::seek_index(size_t index)
{
	if(adt_source->is_hole(index))
		return false;
	this->index = index;
	return index < adt_source->array_size;
}

size_t array_dtable::iter::get_index() const
{
	return index;
}

metablob array_dtable::iter::meta() const
{
	return metablob(adt_source->value_size);
}

blob array_dtable::iter::value() const
{
	bool found;
	return adt_source->get_value(index, &found);
}

const dtable * array_dtable::iter::source() const
{
	return adt_source;
}

dtable::iter * array_dtable::iterator() const
{
	return new iter(this);
}

blob array_dtable::get_value(size_t index, bool * found) const
{
	off_t offset;
	size_t data_length;
	uint8_t type = index_type(index, &offset);
	if(type != ARRAY_INDEX_VALID)
	{
		*found = (type == ARRAY_INDEX_DNE);
		return blob();
	}
	blob_buffer value(value_size);
	value.set_size(value_size, false);
	data_length = fp->read(offset + sizeof(uint8_t), &value[0], value_size);
	assert(data_length == value_size);
	*found = true;
	return value;
}

uint8_t array_dtable::index_type(size_t index, off_t * offset) const
{
	/* we use an extra byte to see if the value exists hence the value_size + 1 */
	uint8_t type;
	off_t file_off = sizeof(dtable_header) + (index * (value_size + 1));
	size_t data_length = fp->read(file_off, &type, sizeof(type));
	assert(index < array_size);
	assert(data_length == sizeof(type));
	if(offset)
		*offset = file_off;
	return type;
}

int array_dtable::find_key(const dtype_test & test, size_t * index) const
{
	/* binary search */
	size_t min = min_key, max = min_key + array_size - 1;
	assert(ktype != dtype::BLOB || !cmp_name == !blob_cmp);
	while(min <= max)
	{
		/* watch out for overflow! */
		size_t mid = min + (max - min) / 2;
		int c = test(dtype(mid));
		if(c < 0)
			min = mid + 1;
		else if(c > 0)
			max = mid - 1;
		else
		{
			if(is_hole(mid - min_key))
			{
				/* convert to index */
				mid -= min_key;
				/* find next valid index */
				while(++mid < array_size && is_hole(mid));
				if(index)
					*index = mid + min_key;
				return -ENOENT;
			}
			if(index)
				*index = mid;
			return 0;
		}
	}
	if(index)
		*index = min;
	return -ENOENT;
}

blob array_dtable::lookup(const dtype & key, bool * found) const
{
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return blob();
	}
	return get_value(key.u32 - min_key, found);
}

blob array_dtable::index(size_t index) const
{
	bool found;
	if(index >= array_size)
		return blob();
	return get_value(index, &found);
}

int array_dtable::init(int dfd, const char * file, const params & config)
{
	dtable_header header;
	if(fp)
		deinit();
	fp = rofile::open_mmap<64, 24>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read(0, &header) < 0)
		goto fail;
	if(header.magic != ADTABLE_MAGIC || header.version != ADTABLE_VERSION)
		goto fail;
	ktype = dtype::UINT32;
	min_key = header.min_key;
	key_count = header.key_count;
	array_size = header.array_size;
	value_size = header.value_size;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	return -1;
}

void array_dtable::deinit()
{
	if(fp)
	{
		delete fp;
		fp = NULL;
		dtable::deinit();
	}
}

int array_dtable::create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow)
{
	int r;
	rwfile out;
	dtable::iter * iter;
	uint8_t * zero_data;
	dtable_header header;
	dtype::ctype key_type;
	uint32_t index = 0, max_key = 0;
	bool min_key_known = false;
	bool value_size_known = false;
	
	if(!source)
		return -EINVAL;
	key_type = source->key_type();
	if(key_type != dtype::UINT32)
			return -EINVAL;
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	header.min_key = 0;
	header.key_count = 0;
	header.value_size = 0;
	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		metablob meta = iter->meta();
		iter->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		assert(key.type == key_type);
		if(!min_key_known)
		{
			header.min_key = key.u32;
			min_key_known = true;
		}
		max_key = key.u32;
		header.key_count++;
		if(meta.exists())
		{
			if(!value_size_known)
			{
				header.value_size = meta.size();
				value_size_known = true;
			}
			/* all the items in this dtable must be the same size */
			else if(meta.size() != header.value_size)
			{
				delete iter;
				return -EINVAL;
			}
		}
	}
	delete iter;
	
	header.magic = ADTABLE_MAGIC;
	header.version = ADTABLE_VERSION;
	header.array_size = min_key_known ? max_key - header.min_key + 1 : 0;
	
	r = out.create(dfd, file);
	if(r < 0)
		return r;
	
	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;
	
	zero_data = new uint8_t[header.value_size];
	if(!zero_data)
		return -1;
	memset(zero_data, 0, header.value_size);
	
	iter = source->iterator();
	while(iter->valid())
	{
		uint8_t type;
		dtype key = iter->key();
		blob value = iter->value();
		metablob meta = iter->meta();
		iter->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		while(index < key.u32 - header.min_key)
		{
			type = ARRAY_INDEX_HOLE;
			out.append<uint8_t>(&type);
			out.append(zero_data, header.value_size);
			index++;
		}
		type = meta.exists() ? ARRAY_INDEX_VALID : ARRAY_INDEX_DNE;
		r = out.append<uint8_t>(&type);
		if(r < 0)
			goto fail_unlink;
		r = out.append(value);
		if(r < 0)
			goto fail_unlink;
		index++;
	}
	
	delete iter;
	delete[] zero_data;
	
	r = out.close();
	if(r < 0)
		goto fail_unlink;
	return 0;
	
fail_unlink:
	out.close();
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(array_dtable);
