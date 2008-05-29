/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "dt_simple_index.h"
#include "blob_buffer.h"

/* multi value blob format:
 * 4 bytes: value length m
 * m bytes: value
 */

dt_simple_index::iter::iter(const dt_simple_index * src, dtable::iter * iter, dtype::ctype type)
	: seckey(0U), source(src), store(iter), pritype(type), offset(0), is_valid(true){}

dt_simple_index::iter::iter(const dt_simple_index * src, const dtype & key, dtype::ctype type)
	: seckey(key), source(src), store(NULL), pritype(type), offset(0)
{
	multi_value = src->ro_store->find(key);
	if (!multi_value.exists())
		is_valid = false;
	is_valid = true;
}

bool dt_simple_index::iter::valid() const
{
	return store ? store->valid() : is_valid;
}

bool dt_simple_index::iter::next()
{
	if(store)
		return store->next();
	else
	{
		uint32_t next;
		source->find(multi_value, seckey, offset, next);
		if(next < multi_value.size())
		{
			offset = next;
			return true;
		}
		is_valid = false;
		return false;
	}
}

dtype dt_simple_index::iter::key() const
{
	return store ? store->key() : seckey;
}

dtype dt_simple_index::iter::pri() const
{
	if(store)
		return dtype(store->value(), pritype);

	switch(pritype)
	{
		case dtype::STRING:
		{
			uint32_t len = multi_value.index<uint32_t>(0, offset);
			if(offset + sizeof(uint32_t) + len > multi_value.size())
				break;
			return dtype(multi_value.index<const char *>(0, offset + sizeof(uint32_t)), len);
		}
		case dtype::UINT32:
			return dtype(multi_value.index<uint32_t>(0, offset));
		case dtype::DOUBLE:
			return dtype(multi_value.index<double>(0, offset));
	}
	assert(false); //TODO: better way of handling errors
}

int dt_simple_index::map(const dtype & key, dtype & value) const
{
	/* give the unique pri for this key; only makes sense if unique is true */
	assert(is_unique);
	blob pri = ro_store->find(key);
	if (!pri.exists())
		return -1;
	value = dtype(pri, ref_table->key_type());
	return 0;
}

dt_index::iter * dt_simple_index::iterator() const
{
	/* iterate over all keys */
	return new iter(this, ro_store->iterator(), ref_table->key_type());
}

dt_index::iter * dt_simple_index::iterator(dtype key) const
{
	/* iterate over only this one key */
	return new iter(this, key, ref_table->key_type());
}

int dt_simple_index::set(const dtype & key, const dtype & pri)
{
	/* for unique: set the pri for this key, even if it does not yet exist */
	assert(is_unique);
	if(rw_store)
		return rw_store->append(key, pri.flatten());
	return -1;
}

int dt_simple_index::remove(const dtype & key)
{
	/* for unique: remove the pri for this key */
	assert(is_unique);
	if(rw_store)
		return rw_store->remove(key);
	return -1;
}

int dt_simple_index::add(const dtype & key, const dtype & pri)
{
	/* for !unique: add this pri to this key if it is not already there */
	assert(!is_unique);
	if(!rw_store || ro_store->key_type() != key.type || ref_table->key_type() != pri.type)
		return -1;
	blob_buffer old = ro_store->find(key);
	if (!old.exists())
		return -1;
	if(ref_table->key_type() == dtype::STRING)
		old << strlen(pri.str);
	return rw_store->append(key, old << pri);
}

int dt_simple_index::update(const dtype & key, const dtype & old_pri, const dtype & new_pri)
{
	/* for !unique: change this key's mapping to old_pri to new_pri */
	assert(!is_unique && rw_store);
	int r;
	if(!rw_store || ro_store->key_type() != key.type || ref_table->key_type() != new_pri.type ||
			ref_table->key_type() != old_pri.type)
		return -1;
	blob_buffer data = ro_store->find(key);
	if (!data.exists())
		return -1;
	uint32_t start = 0, end = 0;
	r = find(data, old_pri, start, end);
	if(r < 0)
		return r;
	r = data.overwrite(start, new_pri);
	if(r < 0)
		return r;
	return rw_store->append(key, data);
}

int dt_simple_index::remove(const dtype & key, const dtype & pri)
{
	/* for !unique: remove this pri from this key */
	assert(!is_unique);
	if(!rw_store || ro_store->key_type() != key.type || ref_table->key_type() != pri.type)
		return -1;
	int r;
	blob_buffer data = ro_store->find(key);
	if (!data.exists())
		return -1;
	uint32_t start = 0, end = 0;
	r = find(data, pri, start, end);
	if(r < 0)
		return r;
	if(end < data.size())
	{
		r = data.overwrite(start, &data[end], data.size() - end);
		if(r < 0)
			return r;
	}

	if(end >= data.size())
		r = data.set_size(start);
	else
		r = data.set_size(start + (data.size() - end));

	if(r < 0)
		return r;
	return rw_store->append(key, data);
}

int dt_simple_index::init(const dtable * store, const dtable * table, bool unique)
{
	/* any further checking here? */
	is_unique = unique;
	this->ref_table = table;
	this->ro_store = store;
	this->rw_store = NULL;
	return 0;
}

int dt_simple_index::init(dtable * store, const dtable * table, bool unique)
{
	/* any further checking here? */
	is_unique = unique;
	this->ref_table = table;
	this->ro_store = store;
	this->rw_store = store->writable() ? store : NULL;
	return 0;
}

/* finds the region of blob that is equal to pri and sets idx to this byte offset, sets next to the
 * byte offset of where the next item in the blob. If set is non null then instead of finding something
 * equal to pri it make set equal to the dtype at the idx byte offset
 */
int dt_simple_index::find(const blob & b, const dtype & pri, uint32_t & idx, uint32_t & next, dtype * set) const
{
	assert(pri.type == ref_table->key_type());
	switch(pri.type)
	{
		case dtype::STRING:
		{
			uint32_t len, i = idx;
			while(i + sizeof(uint32_t) < b.size())
			{
				len = b.index<uint32_t>(0, i);
				if(i + sizeof(uint32_t) + len > b.size())
					break;
				dtype temp(b.index<const char *>(0, i + sizeof(uint32_t)), len);
				if(set)
					*set = temp;
				if(set || pri == temp)
				{
					idx = i;
					next = i + sizeof(uint32_t) + len;
					return 0;
				}
				i += sizeof(uint32_t) + len;
			}
			break;
		}
		case dtype::UINT32:
		{
			for(uint32_t i = idx; i < b.size(); i += sizeof(uint32_t))
			{
				if(set)
					*set = b.index<uint32_t>(0, i);
				if(set || b.index<uint32_t>(0, i) == pri.u32)
				{
					idx = i;
					next = idx + sizeof(uint32_t);
					return 0;
				}
			}
			break;
		}
		case dtype::DOUBLE:
		{
			for(uint32_t i = idx; i < b.size(); i += sizeof(double))
			{
				if(set)
					*set = b.index<uint32_t>(0, i);
				if(set || b.index<double>(0, i) == pri.dbl)
				{
					idx = i;
					next = idx + sizeof(double);
					return 0;
				}
			}
			break;
		}
		default:
		{
			return -1;

		}
	}
	return -1;
}
