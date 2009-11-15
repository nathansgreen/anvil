/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include <vector>

#include "util.h"
#include "rwfile.h"
#include "blob_buffer.h"
#include "linear_dtable.h"

/* linear dtable file format:
 * bytes 0-3: magic number
 * bytes 4-7: format version
 * bytes 8-11: minimum key
 * bytes 12-15: key count
 * bytes 16-19: array size
 * byte 20: data length size (1-4 bytes)
 * byte 21: offset size (1-4 bytes)
 * byte 22: main data tables
 * 
 * main data tables:
 * key array:
 * [] = byte 0-m: data length
 *      byte m+1-n: data offset (relative to data start)
 * each data blob:
 * [] = byte 0-m: data bytes */

linear_dtable::iter::iter(const linear_dtable * source)
	: iter_source<linear_dtable>(source), index(0)
{
	/* no need to skip past holes: the first element must not be a hole */
}

bool linear_dtable::iter::valid() const
{
	/* index will never point at a hole so this is a sufficient test */
	return index < dt_source->array_size;
}

bool linear_dtable::iter::next()
{
	while(++index < dt_source->array_size)
		if(!dt_source->is_hole(index))
			return true;
	return false;
}

bool linear_dtable::iter::prev()
{
	if(!index)
		return false;
	while(--index)
		if(!dt_source->is_hole(index))
			return true;
	/* we should never get here: the first element must not be a hole */
	assert(0);
}

bool linear_dtable::iter::first()
{
	if(!dt_source->key_count)
		return false;
	index = 0;
	/* no need to skip past holes: the first element must not be a hole */
	return true;
}

bool linear_dtable::iter::last()
{
	if(!dt_source->key_count)
		return false;
	index = dt_source->array_size - 1;
	/* no need to skip past holes: the last element must not be a hole */
	return true;
}

dtype linear_dtable::iter::key() const
{
	uint32_t key = index + dt_source->min_key;
	return dtype(key);
}

bool linear_dtable::iter::seek(const dtype & key)
{
	index = key.u32 - dt_source->min_key;
	if(index > dt_source->array_size)
	{
		index = dt_source->array_size;
		return false;
	}
	if(!dt_source->is_hole(index))
		return true;
	/* the last element must not be a hole so no need to check going past it */
	while(dt_source->is_hole(++index));
	return true;
}

bool linear_dtable::iter::seek(const dtype_test & test)
{
	return dt_source->find_key(test, &index) >= 0;
}

bool linear_dtable::iter::seek_index(size_t index)
{
	/* we allow seeking to one past the end, just
	 * as we allow getting there with next() */
	if(index < 0)
		return false;
	if(index > dt_source->array_size)
	{
		this->index = dt_source->array_size;
		return false;
	}
	if(dt_source->is_hole(index))
		return false;
	this->index = index;
	return true;
}

size_t linear_dtable::iter::get_index() const
{
	return index;
}

metablob linear_dtable::iter::meta() const
{
	size_t data_length;
	dt_source->get_index(index, &data_length);
	return (data_length != (size_t) -1) ? metablob(data_length) : metablob();
}

blob linear_dtable::iter::value() const
{
	bool found;
	return dt_source->get_value(index, &found);
}

const dtable * linear_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * linear_dtable::iterator(ATX_DEF) const
{
	return new iter(this);
}

bool linear_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	size_t data_length, index;
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return false;
	}
	index = key.u32 - min_key;
	if(!get_index(index, &data_length))
	{
		*found = false;
		return false;
	}
	*found = true;
	return data_length != (size_t) -1;
}

/* returns false for holes, true otherwise; stores interpreted data length */
bool linear_dtable::get_index(size_t index, size_t * data_length, off_t * data_offset) const
{
	int r;
	uint8_t size = length_size + offset_size;
	uint8_t bytes[size];
	size_t length;
	assert(index < key_count);
	
	r = fp->read(sizeof(dtable_header) + size * index, bytes, size);
	assert(r == size);
	
	length = util::read_bytes(bytes, 0, length_size);
	if(!length)
		return false;
	
	if(data_length)
		/* all data lengths are stored incremented by 2, to
		 * free up 0 for holes and 1 for non-existent values */
		*data_length = length - 2;
	if(data_offset)
		*data_offset = util::read_bytes(bytes, length_size, offset_size);
	
	return true;
}

int linear_dtable::find_key(const dtype_test & test, size_t * index) const
{
	/* binary search */
	ssize_t min = min_key, max = min_key + array_size - 1;
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
				min = mid;
				break;
			}
			if(index)
				*index = mid - min_key;
			return 0;
		}
	}
	/* convert to index */
	min -= min_key;
	/* find next valid index */
	while(min < (ssize_t) array_size && is_hole(min))
		min++;
	if(index)
		*index = min;
	return -ENOENT;
}

bool linear_dtable::is_hole(size_t index) const
{
	assert(index < key_count);
	return !get_index(index);
}

blob linear_dtable::get_value(size_t index, bool * found) const
{
	assert(index < key_count);
	size_t data_length;
	off_t data_offset;
	if(!get_index(index, &data_length, &data_offset))
	{
		*found = false;
		return blob();
	}
	*found = true;
	if(data_length == (size_t) -1)
		return blob();
	if(!data_length)
		return blob::empty;
	blob_buffer value(data_length);
	value.set_size(data_length, false);
	assert(data_length == value.size());
	data_length = fp->read(data_start_off + data_offset, &value[0], data_length);
	assert(data_length == value.size());
	return value;
}

blob linear_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	size_t index;
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return false;
	}
	index = key.u32 - min_key;
	return get_value(index, found);
}

blob linear_dtable::index(size_t index) const
{
	bool found;
	if(index < 0 || index >= key_count)
		return blob();
	return get_value(index, &found);
}

bool linear_dtable::contains_index(size_t index) const
{
	if(index < 0 || index >= key_count)
		return false;
	return get_index(index);
}

int linear_dtable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	int r = -1;
	dtable_header header;
	if(fp)
		deinit();
	fp = rofile::open_mmap<64, 24>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read_type(0, &header) < 0)
		goto fail;
	if(header.magic != LDTABLE_MAGIC || header.version != LDTABLE_VERSION)
		goto fail;
	ktype = dtype::UINT32;
	min_key = header.min_key;
	key_count = header.key_count;
	array_size = header.array_size;
	length_size = header.length_size;
	offset_size = header.offset_size;
	data_start_off = sizeof(header) + (length_size + offset_size) * key_count;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	return (r < 0) ? r : -1;
}

void linear_dtable::deinit()
{
	if(fp)
	{
		delete fp;
		fp = NULL;
		dtable::deinit();
	}
}

int linear_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	size_t max_data_size = 0, total_data_size = 0;
	uint32_t max_key = 0, index = 0;
	bool min_key_known = false;
	dtable_header header;
	int r, size;
	rwfile out;
	
	if(!source)
		return -EINVAL;
	if(source->key_type() != dtype::UINT32)
		return -EINVAL;
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	header.min_key = 0;
	header.key_count = 0;
	/* just to be sure */
	source->first();
	while(source->valid())
	{
		dtype key = source->key();
		metablob meta = source->meta();
		source->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
				continue;
		header.key_count++;
		assert(key.type == dtype::UINT32);
		if(!min_key_known)
		{
			header.min_key = key.u32;
			min_key_known = true;
		}
		if(key.u32 > max_key)
			max_key = key.u32;
		if(meta.size() > max_data_size)
			max_data_size = meta.size();
		total_data_size += meta.size();
	}
	
	/* now write the file */
	header.magic = LDTABLE_MAGIC;
	header.version = LDTABLE_VERSION;
	header.array_size = max_key - header.min_key + 1;
	/* we reserve size 0 for holes and 1 for non-existent entries, so add 2 */
	header.length_size = util::byte_size(max_data_size + 2);
	header.offset_size = util::byte_size(total_data_size);
	size = header.length_size + header.offset_size;
	
	r = out.create(dfd, file);
	if(r < 0)
		return r;
	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;
	
	/* now the key array */
	max_key = 0;
	total_data_size = 0;
	source->first();
	while(source->valid())
	{
		int i = 0;
		uint8_t bytes[size];
		dtype key = source->key();
		metablob meta = source->meta();
		source->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
				continue;
		while(index < key.u32 - header.min_key)
		{
			/* fill in the hole */
			util::layout_bytes(bytes, 0, 0, header.length_size);
			r = out.append(bytes, header.length_size);
			if(r < 0)
				goto fail_unlink;
			r = out.pad(size - header.length_size);
			if(r < 0)
				goto fail_unlink;
			index++;
		}
		util::layout_bytes(bytes, &i, meta.exists() ? meta.size() + 2 : 1, header.length_size);
		util::layout_bytes(bytes, &i, total_data_size, header.offset_size);
		r = out.append(bytes, i);
		if(r != i)
			goto fail_unlink;
		total_data_size += meta.size();
		index++;
	}
	
	/* and the data itself */
	source->first();
	while(source->valid())
	{
		blob value = source->value();
		source->next();
		/* nonexistent blobs have size 0 */
		if(!value.size())
			continue;
		r = out.append(value);
		if(r < 0)
			goto fail_unlink;
	}
	
	r = out.close();
	if(r < 0)
		goto fail_unlink;
	return 0;
	
fail_unlink:
	out.close();
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(linear_dtable);
