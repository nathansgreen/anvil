/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "openat.h"
#include "toilet.h"
#include "index.h"

/* static_assert(x) will generate a compile-time error if 'x' is false. */
#define static_assert(x) switch (x) case 0: case (x):

int toilet_index_init(int dfd, const char * path, t_type type)
{
	mm_type_t mm_type = MM_NONE;
	int r, fd, dir_fd;
	r = mkdirat(dfd, path, 0775);
	if(r < 0 && errno != EEXIST)
		goto fail_simple;
	dir_fd = openat(dfd, path, 0);
	if(dir_fd < 0)
	{
		r = dir_fd;
		goto fail_simple;
	}
	
	switch(type)
	{
		case T_ID:
			static_assert(sizeof(((t_value *) NULL)->v_id) == sizeof(((mm_val_t *) NULL)->u32));
			mm_type = MM_U32;
			break;
		case T_INT:
			static_assert(sizeof(((t_value *) NULL)->v_int) == sizeof(((mm_val_t *) NULL)->u64));
			mm_type = MM_U64;
			break;
		case T_STRING:
			mm_type = MM_STR;
			break;
		case T_BLOB:
			r = -EINVAL;
			goto fail_simple;
	}
	fd = openat(dir_fd, "key-type", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
	{
		r = -errno;
		goto fail_simple;
	}
	r = write(fd, &type, sizeof(type));
	close(fd);
	if(r != sizeof(type))
		goto fail_unlink;
	
	/* start with just a disk hash; we'll add a tree later if necessary */
	r = diskhash::init(dir_fd, "dh", mm_type, MM_U32);
	if(r < 0)
		goto fail_unlink;
	
	close(dir_fd);
	return 0;
	
fail_unlink:
	unlinkat(dir_fd, "key-type", 0);
	close(dir_fd);
fail_simple:
	return r;
}

int toilet_index_drop(int dfd, const char * store)
{
	return multimap::drop(dfd, store);
}

t_index * toilet_open_index(uint8_t * id, int dfd, const char * path, const char * name)
{
	t_index * index;
	int dir_fd, fd = openat(dfd, path, 0);
	if(fd < 0)
		return NULL;
	dir_fd = openat(fd, name, 0);
	close(fd);
	if(dir_fd < 0)
		return NULL;
	
	index = (t_index *) malloc(sizeof(*index));
	if(!index)
		goto fail_malloc;
	index->type = t_index::I_NONE;
	
	fd = openat(dir_fd, "key-type", O_RDONLY);
	if(fd < 0)
		goto fail_key;
	if(read(fd, &index->data_type, sizeof(index->data_type)) != sizeof(index->data_type))
	{
		close(fd);
		goto fail_key;
	}
	close(fd);
	if(index->data_type != T_ID && index->data_type != T_INT && index->data_type != T_STRING)
		goto fail_key;
	
	index->hash.disk = diskhash::open(id, dir_fd, "dh");
	if(index->hash.disk)
	{
		index->type |= t_index::I_HASH;
		if(index->hash.disk->get_val_type() != MM_U32)
			goto fail_hash;
		index->hash.cache = new memcache(id, index->hash.disk);
		if(!index->hash.cache)
			goto fail_hash;
	}
	index->tree.disk = disktree::open(id, dir_fd, "dt");
	if(index->tree.disk)
	{
		index->type |= t_index::I_TREE;
		if(index->tree.disk->get_val_type() != MM_U32)
			goto fail_hash;
		index->tree.cache = new memcache(id, index->tree.disk);
		if(!index->tree.cache)
			goto fail_tree;
	}
	
	if(index->type == t_index::I_NONE)
		goto fail_both;
	
	close(dir_fd);
	return index;
	
fail_both:
	if(index->tree.disk)
	{
		delete index->tree.cache;
fail_tree:
		delete index->tree.disk;
	}
	if(index->hash.disk)
	{
		delete index->hash.cache;
fail_hash:
		delete index->hash.disk;
	}
fail_key:
	free(index);
fail_malloc:
	close(dir_fd);
	return NULL;
}

t_type toilet_index_type(t_index * index)
{
	return index->data_type;
}

void toilet_close_index(t_index * index)
{
	if(index->type & t_index::I_HASH)
	{
		delete index->hash.cache;
		delete index->hash.disk;
	}
	if(index->type & t_index::I_TREE)
	{
		delete index->tree.cache;
		delete index->tree.disk;
	}
	free(index);
}

/* store a toilet value into a multimap value, or convert the pointers */
static inline mm_val_t * toilet_to_multimap_value(t_type type, t_value * value, mm_val_t * mm)
{
	switch(type)
	{
		case T_ID:
			mm->u32 = value->v_id;
			return mm;
		case T_INT:
			mm->u64 = value->v_int;
			return mm;
		case T_STRING:
			return (mm_val_t *) &value->v_string;
		default:
			return NULL;
	}
}

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value * value)
{
	mm_val_t mm_id = {u32: id}, mm_value;
	mm_val_t * mm_pvalue = toilet_to_multimap_value(type, value, &mm_value);
	if(type != index->data_type)
		return -EINVAL;
	if(index->type & t_index::I_HASH)
	{
		int r = index->hash.cache->append_value(mm_pvalue, &mm_id);
		if(r < 0)
			return r;
	}
	if(index->type & t_index::I_TREE)
	{
		int r = index->tree.cache->append_value(mm_pvalue, &mm_id);
		if(r < 0)
		{
			/* XXX: reset the hash somehow? */
			return r;
		}
	}
	return 0;
}

int toilet_index_change(t_index * index, t_row_id id, t_type type, t_value * old_value, t_value * new_value)
{
	toilet_index_remove(index, id, type, old_value);
	toilet_index_add(index, id, type, new_value);
	return 0;
}

int toilet_index_remove(t_index * index, t_row_id id, t_type type, t_value * value)
{
	mm_val_t mm_id = {u32: id}, mm_value;
	mm_val_t * mm_pvalue = toilet_to_multimap_value(type, value, &mm_value);
	if(type != index->data_type)
		return -EINVAL;
	if(index->type & t_index::I_HASH)
	{
		int r = index->hash.cache->remove_value(mm_pvalue, &mm_id);
		if(r < 0)
			return r;
	}
	if(index->type & t_index::I_TREE)
	{
		int r = index->tree.cache->remove_value(mm_pvalue, &mm_id);
		if(r < 0)
		{
			/* XXX: restore the hash somehow? */
			return r;
		}
	}
	return 0;
}

static t_rowset * multimap_it_to_rowset(multimap_it * it)
{
	size_t size = it->size();
	t_rowset * rowset = (t_rowset *) malloc(sizeof(*rowset));
	if(!rowset)
		goto fail_rowset;
	rowset->rows = vector_create_capacity(size);
	if(!rowset->rows)
		goto fail_rows;
	rowset->ids = hash_set_create_size(size, true);
	if(!rowset->ids)
		goto fail_ids;
	rowset->out_count = 1;
	
	/* should we call it->size() instead? */
	while(size--)
	{
		t_row_id id;
		if(it->next() < 0)
			goto fail_add;
		id = it->val->u32;
		if(vector_push_back(rowset->rows, (void *) id) < 0)
			goto fail_add;
		if(hash_set_insert(rowset->ids, (void *) id) < 0)
			goto fail_add;
	}
	
	return rowset;
fail_add:
	hash_set_destroy(rowset->ids);
fail_ids:
	vector_destroy(rowset->rows);
fail_rows:
	free(rowset);
fail_rowset:
	delete it;
	return NULL;
}

ssize_t toilet_index_size(t_index * index)
{
	if(index->type & t_index::I_HASH)
		return index->hash.cache->values();
	if(index->type & t_index::I_TREE)
		return index->tree.cache->values();
	return -1;
}

t_rowset * toilet_index_list(t_index * index, t_type type)
{
	multimap_it * it = NULL;
	if(type != index->data_type)
		return NULL;
	if(index->type & t_index::I_HASH)
		it = index->hash.cache->iterator();
	else if(index->type & t_index::I_TREE)
		it = index->tree.cache->iterator();
	if(!it)
		return NULL;
	return multimap_it_to_rowset(it);
}

ssize_t toilet_index_count(t_index * index, t_type type, t_value * value)
{
	mm_val_t mm_value;
	mm_val_t * mm_pvalue = toilet_to_multimap_value(type, value, &mm_value);
	if(type != index->data_type)
		return -EINVAL;
	if(index->type & t_index::I_HASH)
		return index->hash.cache->count_values(mm_pvalue);
	if(index->type & t_index::I_TREE)
		return index->tree.cache->count_values(mm_pvalue);
	return -1;
}

t_rowset * toilet_index_find(t_index * index, t_type type, t_value * value)
{
	mm_val_t mm_value;
	mm_val_t * mm_pvalue = toilet_to_multimap_value(type, value, &mm_value);
	multimap_it * it = NULL;
	if(type != index->data_type)
		return NULL;
	if(index->type & t_index::I_HASH)
		it = index->hash.cache->get_values(mm_pvalue);
	else if(index->type & t_index::I_TREE)
		it = index->tree.cache->get_values(mm_pvalue);
	if(!it)
		return NULL;
	return multimap_it_to_rowset(it);
}

ssize_t toilet_index_count_range(t_index * index, t_type type, t_value * low_value, t_value * high_value)
{
	mm_val_t mm_value[2];
	mm_val_t * low_pvalue = toilet_to_multimap_value(type, low_value, &mm_value[0]);
	mm_val_t * high_pvalue = toilet_to_multimap_value(type, high_value, &mm_value[1]);
	if(type != index->data_type)
		return -EINVAL;
	if(index->type & t_index::I_HASH)
		return index->hash.cache->count_range(low_pvalue, high_pvalue);
	if(index->type & t_index::I_TREE)
		return index->tree.cache->count_range(low_pvalue, high_pvalue);
	return -1;
}

t_rowset * toilet_index_find_range(t_index * index, t_type type, t_value * low_value, t_value * high_value)
{
	mm_val_t mm_value[2];
	mm_val_t * low_pvalue = toilet_to_multimap_value(type, low_value, &mm_value[0]);
	mm_val_t * high_pvalue = toilet_to_multimap_value(type, high_value, &mm_value[1]);
	multimap_it * it = NULL;
	if(type != index->data_type)
		return NULL;
	/* prefer the tree for this type of query */
	if(index->type & t_index::I_TREE)
		it = index->tree.cache->get_range(low_pvalue, high_pvalue);
	else if(index->type & t_index::I_HASH)
		it = index->hash.cache->get_range(low_pvalue, high_pvalue);
	if(!it)
		return NULL;
	return multimap_it_to_rowset(it);
}
