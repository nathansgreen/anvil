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
	: index(0), bdt_source(source)
{
}

bool array_dtable::iter::valid() const
{
	return index <= bdt_source->max_key;
}

bool array_dtable::iter::next()
{
	if(index == bdt_source->max_key+1)
		return false;

	while(++index <= bdt_source->max_key && !bdt_source->exists(index))
		continue;

	return index <= bdt_source->max_key;
}

bool array_dtable::iter::prev()
{
	if(index == bdt_source->min_key)
		return false;
	index--;
	return true;
}

bool array_dtable::iter::last()
{
	if(!bdt_source->max_key)
		return false;
	index = bdt_source->max_key;
	return true;
}

bool array_dtable::iter::first()
{
	if(!bdt_source->max_key)
		return false;
	index = bdt_source->min_key;
	return true;
}

dtype array_dtable::iter::key() const
{
	return dtype(index);
}

bool array_dtable::iter::seek(const dtype & key)
{
	assert(key.type == dtype::UINT32);
	if(key.u32 > bdt_source->max_key || key.u32 < bdt_source->min_key)
		return false;
	index = key.u32;
	return true;
}

bool array_dtable::iter::seek(const dtype_test & test)
{
	return bdt_source->find_key(test, &index) >= 0;
}

bool array_dtable::iter::seek_index(size_t idx)
{
	if(idx < 0 || idx > bdt_source->max_key)
		return false;
	index = idx;
	return index <= bdt_source->max_key;
}

metablob array_dtable::iter::meta() const
{
	/* we should do something better than just hardcoding uint32_t
	 * maybe use bdt_source->ktype somehow? */
	return metablob(sizeof(uint32_t));
}

blob array_dtable::iter::value() const
{
	bool test;
	return bdt_source->get_value(index, &test);
}

const dtable * array_dtable::iter::source() const
{
	return bdt_source;
}

dtable::iter * array_dtable::iterator() const
{
	return new iter(this);
}

blob array_dtable::get_value(uint32_t key, bool * found) const
{
	assert(key <= max_key && key >= min_key);
	/* we use an extra byte to see if the value exists hence the value_size + 1 */
	size_t offset = sizeof(dtable_header) + ((key - min_key) * (value_size+1));
	uint8_t exists;
	size_t data_length = fp->read(offset, &exists, sizeof(exists));
	assert(data_length == sizeof(exists));
	if(!exists)
	{
		*found = false;
		return blob();
	}
	blob_buffer value(value_size);
	value.set_size(value_size, false);
	data_length = fp->read(offset+sizeof(exists), &value[0], value_size);
	assert(data_length == value_size);
	*found = true;
	return value;
}

bool array_dtable::exists(uint32_t key) const
{
	assert(key <= max_key && key >= min_key);
	/* we use an extra byte to see if the value exists hence the value_size + 1 */
	size_t offset = sizeof(dtable_header) + ((key - min_key) * (value_size+1));
	uint8_t exists;
	size_t data_length = fp->read(offset, &exists, sizeof(exists));
	assert(data_length == sizeof(exists));
	return exists;
}

template<class T>
int array_dtable::find_key(const T & test, size_t * index) const
{
	/* binary search */
	size_t min = min_key, max = max_key;
	assert(ktype != dtype::BLOB || !cmp_name == !blob_cmp);
	while(min <= max)
	{
		/* watch out for overflow! */
		size_t mid = min + (max - min) / 2;
		dtype value = mid;
		int c = test(value);
		if(c < 0)
			min = mid + 1;
		else if(c > 0)
			max = mid - 1;
		else
		{
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
	if(key.u32 > max_key || key.u32 < min_key)
	{
		*found = false;
		return blob();
	}
	return get_value(key.u32, found);
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
	max_key = header.max_key;
	value_size = header.value_size;
	min_key = header.min_key;
	ktype = dtype::UINT32;

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
	dtable::iter * iter;
	if(!source)
		return -EINVAL;
	dtype::ctype key_type = source->key_type();
	dtable_header header;
	uint32_t max_data_size = 0, i = 0;
	uint8_t * zero_data;
	int r;
	rwfile out;
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	iter = source->iterator();
	uint32_t min_key = 0,	max_key = 0;
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
		if(key.u32 < min_key)
			min_key = key.u32;
		if(key.u32 > max_key)
			max_key = key.u32;
		if(max_data_size == 0 && meta.size() > max_data_size)
			max_data_size = meta.size();
		/* all the items in this dtable must be the same size */
		if(meta.size() != max_data_size)
			return -EINVAL;
	}
	delete iter;

	header.magic = ADTABLE_MAGIC;
	header.version = ADTABLE_VERSION;
	header.max_key = max_key;
	header.min_key = min_key;
	header.value_size = max_data_size;

	if(key_type != dtype::UINT32)
			return -EINVAL;

	r = out.create(dfd, file);
	if(r < 0)
		return r;

	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;

	zero_data = new uint8_t[max_data_size+1];
	if(!zero_data)
		return -1;
	memset(zero_data, 0, max_data_size+1);

	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		blob value = iter->value();
		metablob meta = iter->meta();
		iter->next();
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		while(i < key.u32)
		{
			out.append(zero_data,max_data_size+1);
			++i;
		}
		uint8_t exists = 1;
		r = out.append<uint8_t>(&exists);
		if(r < 0)
			goto fail_unlink;
		r = out.append(value);
		if(r < 0)
			goto fail_unlink;
		++i;

	}

	delete iter;
	delete [] zero_data;

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
