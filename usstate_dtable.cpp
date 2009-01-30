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
#include "usstate_dtable.h"

/* NOTE: this must be sorted, and we expect all codes to be length 2 */
const blob usstate_dtable::state_codes[USSTATE_COUNT + 1] = {
	"AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
	"HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME",
	"MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM",
	"NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
	"TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY", blob()};

usstate_dtable::iter::iter(const usstate_dtable * source)
	: iter_source<usstate_dtable>(source), index(0)
{
}

bool usstate_dtable::iter::valid() const
{
	return index < dt_source->array_size;
}

bool usstate_dtable::iter::next()
{
	if(index == dt_source->array_size)
		return false;
	while(++index < dt_source->array_size && dt_source->is_hole(index));
	return index < dt_source->array_size;
}

bool usstate_dtable::iter::prev()
{
	if(index == dt_source->min_key)
		return false;
	while(--index > dt_source->min_key && dt_source->is_hole(index));
	/* the first index can't be a hole, or it wouldn't be the first index */
	return true;
}

bool usstate_dtable::iter::first()
{
	if(!dt_source->array_size)
		return false;
	index = dt_source->min_key;
	return true;
}

bool usstate_dtable::iter::last()
{
	if(!dt_source->array_size)
		return false;
	index = dt_source->array_size - 1;
	return true;
}

dtype usstate_dtable::iter::key() const
{
	assert(index < dt_source->array_size);
	uint32_t key = index + dt_source->min_key;
	return dtype(key);
}

bool usstate_dtable::iter::seek(const dtype & key)
{
	assert(key.type == dtype::UINT32);
	if(dt_source->is_hole(key.u32 - dt_source->min_key))
		return false;
	index = key.u32 - dt_source->min_key;
	return true;
}

bool usstate_dtable::iter::seek(const dtype_test & test)
{
	return dt_source->find_key(test, &index) >= 0;
}

bool usstate_dtable::iter::seek_index(size_t index)
{
	if(dt_source->is_hole(index))
		return false;
	this->index = index;
	return index < dt_source->array_size;
}

size_t usstate_dtable::iter::get_index() const
{
	return index;
}

metablob usstate_dtable::iter::meta() const
{
	return metablob(dt_source->value_size);
}

blob usstate_dtable::iter::value() const
{
	bool found;
	return dt_source->get_value(index, &found);
}

const dtable * usstate_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * usstate_dtable::iterator() const
{
	return new iter(this);
}

bool usstate_dtable::present(const dtype & key, bool * found) const
{
	uint8_t value;
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return false;
	}
	value = index_value(key.u32 - min_key);
	*found = value < USSTATE_INDEX_HOLE;
	return value < USSTATE_COUNT;
}

blob usstate_dtable::get_value(size_t index, bool * found) const
{
	uint8_t value = index_value(index);
	if(value < USSTATE_INDEX_HOLE)
	{
		*found = true;
		return state_codes[value];
	}
	*found = false;
	return blob();
}

uint8_t usstate_dtable::index_value(size_t index) const
{
	uint8_t value;
	off_t file_off = sizeof(dtable_header) + index;
	size_t data_length = fp->read(file_off, &value, sizeof(value));
	assert(index < array_size);
	assert(data_length == sizeof(value));
	return value;
}

int usstate_dtable::find_key(const dtype_test & test, size_t * index) const
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

blob usstate_dtable::lookup(const dtype & key, bool * found) const
{
	assert(key.type == dtype::UINT32);
	if(key.u32 < min_key || min_key + array_size <= key.u32)
	{
		*found = false;
		return blob();
	}
	return get_value(key.u32 - min_key, found);
}

blob usstate_dtable::index(size_t index) const
{
	bool found;
	if(index >= array_size)
		return blob();
	return get_value(index, &found);
}

bool usstate_dtable::contains_index(size_t index) const
{
	if(index >= array_size)
		return false;
	return index_value(index) < USSTATE_COUNT;
}

int usstate_dtable::init(int dfd, const char * file, const params & config)
{
	dtable_header header;
	if(fp)
		deinit();
	fp = rofile::open_mmap<64, 24>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read(0, &header) < 0)
		goto fail;
	if(header.magic != USSDTABLE_MAGIC || header.version != USSDTABLE_VERSION)
		goto fail;
	ktype = dtype::UINT32;
	min_key = header.min_key;
	key_count = header.key_count;
	array_size = header.array_size;
	
	return 0;
	
fail:
	delete fp;
	fp = NULL;
	return -1;
}

void usstate_dtable::deinit()
{
	if(fp)
	{
		delete fp;
		fp = NULL;
		dtable::deinit();
	}
}

int usstate_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int r;
	rwfile out;
	dtable_header header;
	dtype::ctype key_type;
	uint32_t index = 0, max_key = 0;
	bool min_key_known = false;
	
	if(!source)
		return -EINVAL;
	key_type = source->key_type();
	if(key_type != dtype::UINT32)
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
		if(!meta.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->contains(key))
			{
				source->next();
				continue;
			}
		assert(key.type == key_type);
		if(!min_key_known)
		{
			header.min_key = key.u32;
			min_key_known = true;
		}
		max_key = key.u32;
		header.key_count++;
		/* we'll do a full check later; for now just make sure the values are size 2 */
		if(meta.exists() && meta.size() != 2)
			if(!source->reject())
				return -EINVAL;
		source->next();
	}
	
	header.magic = USSDTABLE_MAGIC;
	header.version = USSDTABLE_VERSION;
	header.array_size = min_key_known ? max_key - header.min_key + 1 : 0;
	
	r = out.create(dfd, file);
	if(r < 0)
		return r;
	
	r = out.append(&header);
	if(r < 0)
		goto fail_unlink;
	
	source->first();
	while(source->valid())
	{
		uint8_t code;
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
			code = USSTATE_INDEX_HOLE;
			r = out.append<uint8_t>(&code);
			if(r < 0)
				goto fail_unlink;
			index++;
		}
		if(value.exists() && value.size() != 2)
			/* it was already successfully reject()ed above */
			value = blob();
		if(value.exists())
		{
			ssize_t index = blob::locate(state_codes, USSTATE_COUNT, value);
			/* this is the full check we didn't do earlier */
			if(index < 0 || index >= USSTATE_COUNT)
			{
				if(!source->reject())
				{
					r = -EINVAL;
					goto fail_unlink;
				}
				index = USSTATE_INDEX_DNE;
			}
			code = index;
		}
		else
			code = USSTATE_INDEX_DNE;
		r = out.append<uint8_t>(&code);
		if(r < 0)
			goto fail_unlink;
		source->next();
		index++;
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

DEFINE_RO_FACTORY(usstate_dtable);
