/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "journal_dtable.h"

bool journal_dtable::iter::valid() const
{
	return jit != jdt_source->jdt_map.end();
}

bool journal_dtable::iter::next()
{
	if(jit != jdt_source->jdt_map.end())
		++jit;
	return jit != jdt_source->jdt_map.end();
}

bool journal_dtable::iter::prev()
{
	if(jit != jdt_source->jdt_map.begin())
		--jit;
	return jit != jdt_source->jdt_map.end();
}

bool journal_dtable::iter::last()
{
	jit = jdt_source->jdt_map.end();
	if(jit != jdt_source->jdt_map.begin())
		--jit;
	return jit != jdt_source->jdt_map.end();
}

dtype journal_dtable::iter::key() const
{
	return jit->first;
}

bool journal_dtable::iter::seek(const dtype & key)
{
	jit = jdt_source->jdt_map.lower_bound(key);
	if(jit == jdt_source->jdt_map.end())
		return false;
	return !jdt_source->jdt_map.key_comp()(key, jit->first);
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
	return jdt_source;
}

dtable::iter * journal_dtable::iterator() const
{
	return new iter(this);
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

#define JDT_STRING 1
struct jdt_string
{
	uint8_t type;
	size_t length;
	char string[0];
} __attribute__((packed));

#define JDT_KEY_U32 2
struct jdt_key_u32
{
	uint8_t type;
	uint32_t key;
	size_t size;
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_DBL 3
struct jdt_key_dbl
{
	uint8_t type;
	double key;
	size_t size;
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_STR 4
struct jdt_key_str
{
	uint8_t type;
	uint32_t index;
	size_t size;
	uint8_t data[0];
} __attribute__((packed));

#define JDT_KEY_BLOB 5
struct jdt_key_blob
{
	uint8_t type;
	size_t key_size;
	size_t size;
	/* key stored first, then data */
	uint8_t data[0];
} __attribute__((packed));

#define JDT_BLOB_CMP 6
struct jdt_blob_cmp
{
	uint8_t type;
	size_t length;
	char name[0];
} __attribute__((packed));

int journal_dtable::add_string(const istr & string, uint32_t * index)
{
	int r;
	size_t length;
	jdt_string * entry;
	if(strings.lookup(string, index))
		return 0;
	if(!strings.add(string, index))
		return -ENOMEM;
	length = strlen(string);
	entry = (jdt_string *) malloc(sizeof(*entry) + length);
	if(!entry)
		return -ENOMEM;
	entry->type = JDT_STRING;
	entry->length = length;
	memcpy(entry->string, string, length);
	r = journal_append(entry, sizeof(*entry) + length);
	free(entry);
	return r;
}

int journal_dtable::log_blob_cmp()
{
	int r;
	size_t length = strlen(blob_cmp->name);
	jdt_blob_cmp * entry = (jdt_blob_cmp *) malloc(sizeof(*entry) + length);
	if(!entry)
		return -ENOMEM;
	entry->type = JDT_BLOB_CMP;
	entry->length = length;
	memcpy(entry->name, blob_cmp->name, length);
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
		memcpy(&entry->data[offset], &blob[0], entry->size);
		size += entry->size;
	}
	else
		entry->size = -1;
	r = journal_append(entry, size);
	free(entry);
	return r;
}

int journal_dtable::log(const dtype & key, const blob & blob)
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
			entry->key = key.u32;
			return log(entry, blob);
		}
		case dtype::DOUBLE:
		{
			jdt_key_dbl * entry = (jdt_key_dbl *) malloc(sizeof(*entry) + blob.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_DBL;
			entry->key = key.dbl;
			return log(entry, blob);
		}
		case dtype::STRING:
		{
			int r;
			jdt_key_str * entry = (jdt_key_str *) malloc(sizeof(*entry) + blob.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_STR;
			r = add_string(key.str, &entry->index);
			if(r < 0)
			{
				free(entry);
				return r;
			}
			return log(entry, blob);
		}
		case dtype::BLOB:
		{
			jdt_key_blob * entry = (jdt_key_blob *) malloc(sizeof(*entry) + blob.size() + key.blb.size());
			if(!entry)
				return -ENOMEM;
			entry->type = JDT_KEY_BLOB;
			entry->key_size = key.blb.size();
			memcpy(entry->data, &key.blb[0], entry->key_size);
			return log(entry, blob, entry->key_size);
		}
	}
	abort();
}

int journal_dtable::append(const dtype & key, const blob & blob)
{
	int r;
	if(key.type != ktype || (ktype == dtype::BLOB && !key.blb.exists()))
		return -EINVAL;
	r = log(key, blob);
	if(r < 0)
		return r;
	journal_dtable_hash::iterator it = jdt_hash.find(key);
	if(it != jdt_hash.end())
	{
		*(it->second) = blob;
		return 0;
	}
	return add_node(key, blob);
}

int journal_dtable::remove(const dtype & key)
{
	return append(key, blob());
}

int journal_dtable::init(dtype::ctype key_type, sys_journal::listener_id lid, sys_journal * journal)
{
	int r;
	if(id() != sys_journal::NO_ID)
		deinit();
	assert(jdt_map.empty());
	assert(jdt_hash.empty());
	assert(!cmp_name);
	ktype = key_type;
	r = strings.init(true);
	if(r < 0)
		return r;
	string_index = 0;
	set_id(lid);
	set_journal(journal);
	return 0;
}

int journal_dtable::reinit(sys_journal::listener_id lid, bool discard)
{
	int r;
	if(id() == sys_journal::NO_ID)
		return -EBUSY;
	if(lid == sys_journal::NO_ID)
		return -EINVAL;
	if(discard)
		journal_discard();
	deinit();
	r = strings.init(true);
	if(r < 0)
		return r;
	string_index = 0;
	set_id(lid);
	return 0;
}

void journal_dtable::deinit()
{
	set_id(sys_journal::NO_ID);
	/* no explicit deinitialization, so reinitialize empty */
	strings.init();
	jdt_hash.clear();
	jdt_map.clear();
	dtable::deinit();
}

int journal_dtable::add_node(const dtype & key, const blob & value)
{
	blob & map_value = jdt_map[key];
	map_value = value;
	jdt_hash[key] = &map_value;
	return 0;
}

int journal_dtable::journal_replay(void *& entry, size_t length)
{
	if(cmp_name && !blob_cmp)
		/* if we need a blob comparator and don't have
		 * one yet, then don't accept journal entries */
		return -EBUSY;
	switch(*(uint8_t *) entry)
	{
		case JDT_STRING:
		{
			uint32_t index;
			jdt_string * string = (jdt_string *) entry;
			char * copy = (char *) malloc(string->length + 1);
			if(!copy)
				return -ENOMEM;
			memcpy(copy, string->string, string->length);
			copy[string->length] = 0;
			if(!strings.add(copy, &index))
			{
				free(copy);
				return -ENOMEM;
			}
			assert(index == string_index);
			string_index++;
			free(copy);
			return 0;
		}
		case JDT_KEY_U32:
		{
			jdt_key_u32 * u32 = (jdt_key_u32 *) entry;
			if(ktype != dtype::UINT32)
				return -EINVAL;
			if(u32->size == (size_t) -1)
				return add_node(u32->key, blob());
			return add_node(u32->key, blob(u32->size, u32->data));
		}
		case JDT_KEY_DBL:
		{
			jdt_key_dbl * dbl = (jdt_key_dbl *) entry;
			if(ktype != dtype::DOUBLE)
				return -EINVAL;
			if(dbl->size == (size_t) -1)
				return add_node(dbl->key, blob());
			return add_node(dbl->key, blob(dbl->size, dbl->data));
		}
		case JDT_KEY_STR:
		{
			jdt_key_str * str = (jdt_key_str *) entry;
			istr string = strings.lookup(str->index);
			if(!string || ktype != dtype::STRING)
				return -EINVAL;
			if(str->size == (size_t) -1)
				return add_node(string, blob());
			return add_node(string, blob(str->size, str->data));
		}
		case JDT_KEY_BLOB:
		{
			jdt_key_blob * blb = (jdt_key_blob *) entry;
			blob key(blb->key_size, blb->data);
			if(blb->size == (size_t) -1)
				return add_node(key, blob());
			return add_node(key, blob(blb->size, &blb->data[blb->key_size]));
		}
		case JDT_BLOB_CMP:
		{
			jdt_blob_cmp * name = (jdt_blob_cmp *) entry;
			istr copy(name->name, name->length);
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
