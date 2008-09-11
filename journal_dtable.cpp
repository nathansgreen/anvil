/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "journal_dtable.h"

bool journal_dtable::iter::valid() const
{
	return jdt_node != NULL;
}

bool journal_dtable::iter::next()
{
	next_node(&jdt_node);
	return jdt_node != NULL;
}

bool journal_dtable::iter::prev()
{
	node * node;
	if(jdt_node)
	{
		node = jdt_node;
		prev_node(&node);
	}
	else
		for(node = jdt_source->root; node && node->right; node = node->right);
	if(node)
		jdt_node = node;
	return node != NULL;
}

bool journal_dtable::iter::last()
{
	for(jdt_node = jdt_source->root; jdt_node && jdt_node->right; jdt_node = jdt_node->right);
	return jdt_node != NULL;
}

dtype journal_dtable::iter::key() const
{
	return jdt_node->key;
}

bool journal_dtable::iter::seek(const dtype & key)
{
	bool found;
	jdt_node = jdt_source->find_node_next(key, &found);
	return found;
}

metablob journal_dtable::iter::meta() const
{
	return jdt_node->value;
}

blob journal_dtable::iter::value() const
{
	return jdt_node->value;
}

const dtable * journal_dtable::iter::source() const
{
	return jdt_source;
}

dtable::iter * journal_dtable::iterator() const
{
	node * node;
	/* find first node */
	for(node = root; node && node->left; node = node->left);
	return new iter(node, this);
}

blob journal_dtable::lookup(const dtype & key, const dtable ** source) const
{
	node * node = find_node(key);
	if(node)
	{
		*source = this;
		return node->value;
	}
	*source = NULL;
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
	if(blob_cmp && !cmp_name)
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
	node * node;
	if(key.type != ktype)
		return -EINVAL;
	r = log(key, blob);
	if(r < 0)
		return r;
	node = find_node(key);
	if(node)
	{
		node->value = blob;
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
	assert(!root);
	assert(!cmp_name);
	ktype = key_type;
	/* the stringset is not used for blob keys in journal_dtable,
	 * so no blob comparator is needed (that's the NULL) */
	r = strings.init(NULL, true);
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
	/* the stringset is not used for blob keys in journal_dtable,
	 * so no blob comparator is needed (that's the NULL) */
	r = strings.init(NULL, true);
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
	strings.init(NULL);
	if(root)
	{
		kill_nodes(root);
		root = NULL;
	}
	dtable::deinit();
}

journal_dtable::node * journal_dtable::find_node(const dtype & key) const
{
	int c;
	node * node = root;
	while(node && (c = node->key.compare(key, blob_cmp)))
		node = (c < 0) ? node->right : node->left;
	return node;
}

journal_dtable::node * journal_dtable::find_node_next(const dtype & key, bool * found) const
{
	int c;
	node * node = root;
	if(!node)
	{
		*found = false;
		return NULL;
	}
	while((c = node->key.compare(key, blob_cmp)))
		if(c < 0)
		{
			if(!node->right)
			{
				next_node(&node);
				*found = false;
				return node;
			}
			node = node->right;
		}
		else
		{
			if(!node->left)
			{
				*found = false;
				return node;
			}
			node = node->left;
		}
	*found = true;
	return node;
}

/* a simple (unbalanced) binary tree for now */
int journal_dtable::add_node(const dtype & key, const blob & value)
{
	int c;
	node ** ptr = &root;
	node * old = NULL;
	node * node = *ptr;
	while(node && (c = node->key.compare(key, blob_cmp)))
	{
		ptr = (c < 0) ? &node->right : &node->left;
		old = node;
		node = *ptr;
	}
	if(node)
	{
		node->value = value;
		return 0;
	}
	node = new struct node(key);
	if(!node)
		return -ENOMEM;
	node->value = value;
	node->up = old;
	node->left = NULL;
	node->right = NULL;
	*ptr = node;
	return 0;
}

void journal_dtable::next_node(node ** n)
{
	node * node = *n;
	if(node->right)
	{
		node = node->right;
		while(node->left)
			node = node->left;
	}
	else
	{
		while(node->up && node->up->right == node)
			node = node->up;
		node = node->up;
	}
	*n = node;
}

void journal_dtable::prev_node(node ** n)
{
	node * node = *n;
	if(node->left)
	{
		node = node->left;
		while(node->right)
			node = node->right;
	}
	else
	{
		while(node->up && node->up->left == node)
			node = node->up;
		node = node->up;
	}
	*n = node;
}

void journal_dtable::kill_nodes(node * n)
{
	if(n->left)
		kill_nodes(n->left);
	if(n->right)
		kill_nodes(n->right);
	delete n;
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
