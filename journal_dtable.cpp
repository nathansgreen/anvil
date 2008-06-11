/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "journal_dtable.h"

bool journal_dtable::iter::valid() const
{
	return jdt_next != NULL;
}

bool journal_dtable::iter::next()
{
	next_node(&jdt_next);
	return jdt_next != NULL;
}

dtype journal_dtable::iter::key() const
{
	return jdt_next->key;
}

metablob journal_dtable::iter::meta() const
{
	return jdt_next->value;
}

blob journal_dtable::iter::value() const
{
	return jdt_next->value;
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

template<class T> inline int journal_dtable::log(T * entry, const blob & blob)
{
	int r;
	size_t size = sizeof(*entry);
	if(blob.exists())
	{
		entry->size = blob.size();
		memcpy(entry->data, &blob[0], entry->size);
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
	if(root)
	{
		kill_nodes(root);
		root = NULL;
	}
}

journal_dtable::node * journal_dtable::find_node(const dtype & key) const
{
	node * node = root;
	while(node && node->key != key)
		node = (node->key < key) ? node->right : node->left;
	return node;
}

/* a simple binary tree for now */
int journal_dtable::add_node(const dtype & key, const blob & value)
{
	node ** ptr = &root;
	node * old = NULL;
	node * node = *ptr;
	while(node && node->key != key)
	{
		ptr = (node->key < key) ? &node->right : &node->left;
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
		default:
			return -EINVAL;
	}
	return 0;
}
