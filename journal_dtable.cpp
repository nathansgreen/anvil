/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "util.h"
#include "hack_avl_map.h"
#include "journal_dtable.h"

bool journal_dtable::iter::valid() const
{
	return jit != dt_source->jdt_map.end();
}

bool journal_dtable::iter::next()
{
	if(jit != dt_source->jdt_map.end())
		++jit;
	return jit != dt_source->jdt_map.end();
}

bool journal_dtable::iter::prev()
{
	if(jit == dt_source->jdt_map.begin())
		return false;
	--jit;
	return true;
}

bool journal_dtable::iter::first()
{
	jit = dt_source->jdt_map.begin();
	return jit != dt_source->jdt_map.end();
}

bool journal_dtable::iter::last()
{
	jit = dt_source->jdt_map.end();
	if(jit == dt_source->jdt_map.begin())
		return false;
	--jit;
	return true;
}

dtype journal_dtable::iter::key() const
{
	return jit->first;
}

bool journal_dtable::iter::seek(const dtype & key)
{
	jit = dt_source->jdt_map.lower_bound(key);
	if(jit == dt_source->jdt_map.end())
		return false;
	return !dt_source->jdt_map.key_comp()(key, jit->first);
}

bool journal_dtable::iter::seek(const dtype_test & test)
{
	jit = lower_bound(dt_source->jdt_map, test);
	if(jit == dt_source->jdt_map.end())
		return false;
	return !test(jit->first);
}

metablob journal_dtable::iter::meta() const
{
	return jit->second;
}

blob journal_dtable::iter::value() const
{
	return jit->second;
}

const dtable * journal_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * journal_dtable::iterator() const
{
	return new iter(this);
}

bool journal_dtable::present(const dtype & key, bool * found) const
{
	journal_dtable_hash::const_iterator it = jdt_hash.find(key);
	if(it != jdt_hash.end())
	{
		*found = true;
		return it->second->exists();
	}
	*found = false;
	return false;
}

blob journal_dtable::lookup(const dtype & key, bool * found) const
{
	journal_dtable_hash::const_iterator it = jdt_hash.find(key);
	if(it != jdt_hash.end())
	{
		*found = true;
		return *(it->second);
	}
	*found = false;
	return blob();
}

#define JDT_KEY_U32 1
struct jdt_key_u32
{
	uint8_t type;
	uint8_t append;
	uint32_t key;
	size_t size;
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_DBL 2
struct jdt_key_dbl
{
	uint8_t type;
	uint8_t append;
	double key;
	size_t size;
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_STR 3
struct jdt_key_str
{
	uint8_t type;
	uint8_t append;
	size_t key_size;
	size_t size;
	/* key stored first, then data */
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_BLOB 4
struct jdt_key_blob
{
	uint8_t type;
	uint8_t append;
	size_t key_size;
	size_t size;
	/* key stored first, then data */
	uint8_t data[0];
} __attribute__((packed));

#define JDT_BLOB_CMP 5
struct jdt_blob_cmp
{
	uint8_t type;
	size_t length;
	char name[0];
} __attribute__((packed));

int journal_dtable::log_blob_cmp()
{
	int r;
	size_t length = strlen(blob_cmp->name);
	jdt_blob_cmp * entry = (jdt_blob_cmp *) malloc(sizeof(*entry) + length);
	if(!entry)
		return -ENOMEM;
	entry->type = JDT_BLOB_CMP;
	entry->length = length;
	util::memcpy(entry->name, blob_cmp->name, length);
	r = journal_append(entry, sizeof(*entry) + length);
	free(entry);
	return 0;
}

template<class T> inline int journal_dtable::log(T * entry, const blob & blob, size_t offset)
{
	int r;
	size_t size = sizeof(*entry) + offset;
	if(blob.exists())
	{
		entry->size = blob.size();
		if(entry->size)
			util::memcpy(&entry->data[offset], &blob[0], entry->size);
		size += entry->size;
	}
	else
		entry->size = -1;
	r = journal_append(entry, size);
	free(entry);
	return r;
}

int journal_dtable::log(const dtype & key, const blob & blob, bool append)
{
	if(ktype == dtype::BLOB && blob_cmp && !cmp_name)
	{
		/* not logged yet, so log it now */
		int value = log_blob_cmp();
		if(value < 0)
			return value;
		cmp_name = blob_cmp->name;
	}
	switch(key.type)
	{
		case dtype::UINT32:
		{
			jdt_key_u32 * entry = (jdt_key_u32 *) malloc(sizeof(*entry) + blob.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_U32;
			entry->append = append;
			entry->key = key.u32;
			return log(entry, blob);
		}
		case dtype::DOUBLE:
		{
			jdt_key_dbl * entry = (jdt_key_dbl *) malloc(sizeof(*entry) + blob.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_DBL;
			entry->append = append;
			entry->key = key.dbl;
			return log(entry, blob);
		}
		case dtype::STRING:
		{
			jdt_key_str * entry = (jdt_key_str *) malloc(sizeof(*entry) + blob.size() + key.str.length());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_STR;
			entry->append = append;
			entry->key_size = key.str.length();
			if(entry->key_size)
				util::memcpy(entry->data, key.str, entry->key_size);
			return log(entry, blob, entry->key_size);
		}
		case dtype::BLOB:
		{
			jdt_key_blob * entry = (jdt_key_blob *) malloc(sizeof(*entry) + blob.size() + key.blb.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_BLOB;
			entry->append = append;
			entry->key_size = key.blb.size();
			if(entry->key_size)
				util::memcpy(entry->data, &key.blb[0], entry->key_size);
			return log(entry, blob, entry->key_size);
		}
	}
	abort();
}

int journal_dtable::insert(const dtype & key, const blob & blob, bool append)
{
	int r;
	if(key.type != ktype || (ktype == dtype::BLOB && !key.blb.exists()))
		return -EINVAL;
	r = log(key, blob, append);
	if(r < 0)
		return r;
	return set_node(key, blob, append);
}

int journal_dtable::remove(const dtype & key)
{
	return insert(key, blob());
}

int journal_dtable::init(dtype::ctype key_type, sys_journal::listener_id lid, bool always_append, sys_journal * journal)
{
	if(lid == sys_journal::NO_ID)
		return -EINVAL;
	if(initialized)
		deinit();
	assert(jdt_map.empty());
	assert(jdt_hash.empty());
	assert(!cmp_name);
	ktype = key_type;
	this->always_append = always_append;
	set_id(lid);
	set_journal(journal);
	initialized = true;
	return 0;
}

int journal_dtable::reinit(sys_journal::listener_id lid, bool discard)
{
	if(!initialized)
		return -EBUSY;
	if(lid == sys_journal::NO_ID)
		return -EINVAL;
	if(discard)
		journal_discard();
	if(blob_cmp)
	{
		blob_cmp->release();
		blob_cmp = NULL;
	}
	cmp_name = NULL;
	jdt_hash.clear();
	jdt_map.clear();
	set_id(lid);
	return 0;
}

void journal_dtable::deinit(bool discard)
{
	if(discard)
		journal_discard();
	jdt_hash.clear();
	jdt_map.clear();
	initialized = false;
	dtable::deinit();
}

int journal_dtable::add_node(const dtype & key, const blob & value, bool append)
{
	journal_dtable_map::value_type pair(key, value);
	journal_dtable_map::iterator it;
	if(append || always_append)
		it = jdt_map.insert(jdt_map.end(), pair);
	else
		it = jdt_map.insert(pair).first;
	jdt_hash[key] = &(it->second);
	return 0;
}

int journal_dtable::set_node(const dtype & key, const blob & value, bool append)
{
	journal_dtable_hash::iterator it = jdt_hash.find(key);
	if(it != jdt_hash.end())
	{
		*(it->second) = value;
		return 0;
	}
	return add_node(key, value, append);
}

int journal_dtable::real_rollover(listening_dtable * target) const
{
	journal_dtable_map::const_iterator it;
	for(it = jdt_map.begin(); it != jdt_map.end(); ++it)
	{
		int r = target->accept(it->first, it->second);
		if(r < 0)
			/* FIXME: we're pretty screwed if this occurs... might be best to abort */
			return r;
	}
	return 0;
}

int journal_dtable::journal_replay(void *& entry, size_t length)
{
	blob value;
	switch(*(uint8_t *) entry)
	{
		case JDT_KEY_U32:
		{
			jdt_key_u32 * u32 = (jdt_key_u32 *) entry;
			if(ktype != dtype::UINT32)
				return -EINVAL;
			if(u32->size != (size_t) -1)
				value = blob(u32->size, u32->data);
			return set_node(u32->key, value, u32->append);
		}
		case JDT_KEY_DBL:
		{
			jdt_key_dbl * dbl = (jdt_key_dbl *) entry;
			if(ktype != dtype::DOUBLE)
				return -EINVAL;
			if(dbl->size != (size_t) -1)
				value = blob(dbl->size, dbl->data);
			return set_node(dbl->key, value, dbl->append);
		}
		case JDT_KEY_STR:
		{
			jdt_key_str * str = (jdt_key_str *) entry;
			istr key((const char *) str->data, str->key_size);
			if(str->size != (size_t) -1)
				value = blob(str->size, &str->data[str->key_size]);
			return set_node(key, value, str->append);
		}
		case JDT_KEY_BLOB:
		{
			jdt_key_blob * blb = (jdt_key_blob *) entry;
			blob key(blb->key_size, blb->data);
			if(blb->size != (size_t) -1)
				value = blob(blb->size, &blb->data[blb->key_size]);
			if(cmp_name && !blob_cmp)
			{
				/* if we need a blob comparator and don't have
				 * one yet, then queue this journal entry */
				add_pending(key, value, blb->append);
				return 0;
			}
			return set_node(key, value, blb->append);
		}
		case JDT_BLOB_CMP:
		{
			jdt_blob_cmp * name = (jdt_blob_cmp *) entry;
			istr copy(name->name, name->length);
			assert(cmp_name || jdt_hash.empty());
			if(cmp_name && strcmp(cmp_name, copy))
				return -EINVAL;
			cmp_name = copy;
			return 0;
		}
		default:
			return -EINVAL;
	}
	return 0;
}

bool journal_dtable::entry_key_type(const void * entry, size_t length, dtype::ctype * key_type)
{
	switch(*(uint8_t *) entry)
	{
		case JDT_KEY_U32:
			*key_type = dtype::UINT32;
			break;
		case JDT_KEY_DBL:
			*key_type = dtype::DOUBLE;
			break;
		case JDT_KEY_STR:
			*key_type = dtype::STRING;
			break;
		case JDT_KEY_BLOB:
		case JDT_BLOB_CMP:
			*key_type = dtype::BLOB;
			break;
		default:
			return false;
	}
	return true;
}

journal_dtable * journal_dtable::journal_dtable_warehouse::create(sys_journal::listener_id lid, const void * entry, size_t length, sys_journal * journal) const
{
	dtype::ctype key_type;
	if(!entry_key_type(entry, length, &key_type))
		return NULL;
	journal_dtable * jdt = new journal_dtable;
	if(jdt->init(key_type, lid, false, journal) < 0)
	{
		delete jdt;
		jdt = NULL;
	}
	return jdt;
}

journal_dtable * journal_dtable::journal_dtable_warehouse::create(sys_journal::listener_id lid, dtype::ctype key_type, sys_journal * journal) const
{
	journal_dtable * jdt = new journal_dtable;
	if(jdt->init(key_type, lid, false, journal) < 0)
	{
		delete jdt;
		jdt = NULL;
	}
	return jdt;
}

journal_dtable::journal_dtable_warehouse journal_dtable::warehouse;
