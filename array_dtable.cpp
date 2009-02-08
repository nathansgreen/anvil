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
	: iter_source<array_dtable>(source), index(0)
{
}

bool array_dtable::iter::valid() const
{
	return index < dt_source->array_size;
}

bool array_dtable::iter::next()
{
	if(index == dt_source->array_size)
		return false;
	while(++index < dt_source->array_size && dt_source->is_hole(index));
	return index < dt_source->array_size;
}

bool array_dtable::iter::prev()
{
	if(index == dt_source->min_key)
		return false;
	while(--index > dt_source->min_key && dt_source->is_hole(index));
	/* the first index can't be a hole, or it wouldn't be the first index */
	return true;
}

bool array_dtable::iter::first()
{
	if(!dt_source->array_size)
		return false;
	index = 0;
	return true;
}

bool array_dtable::iter::last()
{
	if(!dt_source->array_size)
		return false;
	index = dt_source->array_size - 1;
	return true;
}

dtype array_dtable::iter::key() const
{
	assert(index < dt_source->array_size);
	uint32_t key = index + dt_source->min_key;
	return dtype(key);
}

bool array_dtable::iter::seek(const dtype & key)
{
	assert(key.type == dtype::UINT32);
	if(dt_source->is_hole(key.u32 - dt_source->min_key))
		return false;
	index = key.u32 - dt_source->min_key;
	return true;
}

bool array_dtable::iter::seek(const dtype_test & test)
{
	return dt_source->find_key(test, &index) >= 0;
}

bool array_dtable::iter::seek_index(size_t index)
{
	if(dt_source->is_hole(index))
		return false;
	this->index = index;
	return index < dt_source->array_size;
}

size_t array_dtable::iter::get_index() const
{
	return index;
}

metablob array_dtable::iter::meta() const
{
	return metablob(dt_source->value_size);
}

blob array_dtable::iter::value() const
{
	bool found;
	return dt_source->get_value(index, &found);
}

const dtable * array_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * array_dtable::iterator() const
{
	return new iter(this);
}

bool array_dtable::present(const dtype & key, bool * found) const
{
	uint8_t type;
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return false;
	}
	if(!tag_byte)
		return get_value(key.u32 - min_key, found).exists();
	type = index_type(key.u32 - min_key);
	*found = type != ARRAY_INDEX_HOLE;
	return type == ARRAY_INDEX_VALID;
}

blob array_dtable::get_value(size_t index, bool * found) const
{
	off_t offset;
	size_t data_length;
	if(tag_byte)
	{
		uint8_t type = index_type(index, &offset);
		if(type != ARRAY_INDEX_VALID)
		{
			*found = (type == ARRAY_INDEX_DNE);
			return blob();
		}
	}
	else
		offset = data_start + index * value_size;
	if(!value_size)
	{
		/* we can do nothing but assume it was found */
		*found = true;
		return blob::empty;
	}
	blob_buffer value(value_size);
	value.set_size(value_size, false);
	data_length = fp->read(offset, &value[0], value_size);
	assert(data_length == value_size);
	if(!tag_byte)
	{
		*found = hole_value.compare(value) != 0;
		if(!*found || !dne_value.compare(value))
			value = blob();
	}
	else
		*found = true;
	return value;
}

uint8_t array_dtable::index_type(size_t index, off_t * offset) const
{
	assert(tag_byte);
	uint8_t type;
	/* value_size + 1 due to the tag byte */
	off_t file_off = data_start + index * (value_size + 1);
	size_t data_length = fp->read(file_off, &type, sizeof(type));
	assert(index < array_size);
	assert(data_length == sizeof(type));
	if(offset)
		*offset = file_off + sizeof(type);
	return type;
}

bool array_dtable::is_hole(size_t index) const
{
	if(!tag_byte)
	{
		bool found;
		get_value(index, &found);
		return !found;
	}
	return index_type(index) == ARRAY_INDEX_HOLE;
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
		int c = test(dtype((uint32_t) mid));
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

bool array_dtable::contains_index(size_t index) const
{
	if(index >= array_size)
		return false;
	if(!tag_byte)
	{
		bool found;
		return get_value(index, &found).exists();
	}
	return index_type(index) == ARRAY_INDEX_VALID;
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
	tag_byte = header.tag_byte;
	
	data_start = sizeof(header);
	if(header.hole)
	{
		size_t data_length;
		blob_buffer buffer(value_size);
		buffer.set_size(value_size, false);
		data_length = fp->read(data_start, &buffer[0], value_size);
		assert(data_length == value_size);
		data_start += value_size;
		hole_value = buffer;
	}
	if(header.dne)
	{
		size_t data_length;
		blob_buffer buffer(value_size);
		buffer.set_size(value_size, false);
		data_length = fp->read(data_start, &buffer[0], value_size);
		assert(data_length == value_size);
		data_start += value_size;
		dne_value = buffer;
	}
	if(header.hole && header.dne)
		/* cannot have the same value for these */
		if(!hole_value.compare(dne_value))
			goto fail;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	hole_value = blob();
	dne_value = blob();
	return -1;
}

void array_dtable::deinit()
{
	if(fp)
	{
		delete fp;
		fp = NULL;
		hole_value = blob();
		dne_value = blob();
		dtable::deinit();
	}
}

int array_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int r;
	rwfile out;
	uint8_t * zero_data;
	dtable_header header;
	dtype::ctype key_type;
	uint32_t index = 0, max_key = 0;
	bool min_key_known = false;
	bool value_size_known = false;
	bool hole_ok = true, dne_ok = true;
	blob hole_value, dne_value;
	bool tag_byte;
	
	if(!source)
		return -EINVAL;
	key_type = source->key_type();
	if(key_type != dtype::UINT32)
		return -EINVAL;
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	if(!config.get("value_size", &r, 0) || r < 0)
		return -EINVAL;
	if(r)
	{
		header.value_size = r;
		value_size_known = true;
	}
	else
		header.value_size = 0;
	
	/* deal with hole and nonexistent value configuration */
	if(!config.get_blob_or_string("hole_value", &hole_value))
		return -EINVAL;
	if(hole_value.exists())
	{
		if(!value_size_known)
		{
			header.value_size = hole_value.size();
			value_size_known = true;
		}
		else if(hole_value.size() != header.value_size)
			return -EINVAL;
	}
	if(!config.get_blob_or_string("dne_value", &dne_value))
		return -EINVAL;
	if(dne_value.exists())
	{
		if(!value_size_known)
		{
			header.value_size = dne_value.size();
			value_size_known = true;
		}
		else if(dne_value.size() != header.value_size)
			return -EINVAL;
	}
	if(!config.get("tag_byte", &tag_byte, true))
		return -EINVAL;
	if(hole_value.exists() && dne_value.exists())
	{
		tag_byte = false;
		/* cannot have the same value for these */
		if(!hole_value.compare(dne_value))
			return -EINVAL;
	}
	if(!tag_byte)
	{
		hole_ok = hole_value.exists();
		dne_ok = dne_value.exists();
	}
	
	header.min_key = 0;
	header.key_count = 0;
	header.tag_byte = tag_byte;
	header.hole = hole_value.exists();
	header.dne = dne_value.exists();
	/* just to be sure */
	source->first();
	while(source->valid())
	{
		dtype key = source->key();
		metablob meta = source->meta();
		if(!meta.exists())
		{
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
			{
				source->next();
				continue;
			}
			if(!dne_ok)
				return -EINVAL;
		}
		assert(key.type == key_type);
		if(!min_key_known)
		{
			header.min_key = key.u32;
			min_key_known = true;
		}
		else if(!hole_ok && key.u32 != max_key + 1)
			return -EINVAL;
		max_key = key.u32;
		header.key_count++;
		if(meta.exists() && !value_size_known)
		{
			header.value_size = meta.size();
			value_size_known = true;
		}
		source->next();
	}
	
	header.magic = ADTABLE_MAGIC;
	header.version = ADTABLE_VERSION;
	header.array_size = min_key_known ? max_key - header.min_key + 1 : 0;
	
	r = out.create(dfd, file);
	if(r < 0)
		return r;
	
	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;
	if(header.hole)
	{
		r = out.append(hole_value);
		if(r < 0)
			goto fail_unlink;
	}
	if(header.dne)
	{
		r = out.append(dne_value);
		if(r < 0)
			goto fail_unlink;
	}
	
	zero_data = new uint8_t[header.value_size];
	if(!zero_data)
		return -1;
	memset(zero_data, 0, header.value_size);
	
	source->first();
	while(source->valid())
	{
		dtype key = source->key();
		blob value = source->value();
		if(!value.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
			{
				source->next();
				continue;
			}
		while(index < key.u32 - header.min_key)
		{
			assert(hole_ok);
			if(tag_byte)
			{
				uint8_t type = ARRAY_INDEX_HOLE;
				r = out.append<uint8_t>(&type);
				if(r < 0)
					goto fail_unlink;
				r = out.append(zero_data, header.value_size);
			}
			else
				r = out.append(hole_value);
			if(r < 0)
				goto fail_unlink;
			index++;
		}
		if(value.exists() && value.size() != header.value_size)
		{
			/* all the items in this dtable must be the same size */
			if(!source->reject(&value))
				goto fail_unlink;
			if(value.exists() && value.size() != header.value_size)
				goto fail_unlink;
		}
		if(tag_byte)
		{
			uint8_t type = value.exists() ? ARRAY_INDEX_VALID : ARRAY_INDEX_DNE;
			r = out.append<uint8_t>(&type);
			if(r < 0)
				goto fail_unlink;
		}
		else if(!value.exists())
		{
			assert(dne_ok);
			value = dne_value;
		}
		if(value.exists())
			r = out.append(value);
		else
			r = out.append(zero_data, header.value_size);
		if(r < 0)
			goto fail_unlink;
		source->next();
		index++;
	}
	
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
