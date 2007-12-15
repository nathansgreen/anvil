/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "toilet.h"
#include "index.h"

int toilet_index_init(const char * path)
{
	return diskhash::init("indices/id/dh", MM_U32, MM_NONE);
}

t_index * toilet_open_index(const char * path, const char * name)
{
	t_index * index;
	int cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return NULL;
	index = (t_index *) malloc(sizeof(*index));
	if(!index)
		goto fail_malloc;
	index->type = t_index::I_NONE;
	if(chdir(path) < 0)
		goto fail_chdir;
	if(chdir(name) < 0)
		goto fail_chdir;
	index->hash.disk = diskhash::open("dh");
	if(index->hash.disk)
	{
		index->type |= t_index::I_HASH;
		index->hash.cache = new memcache(index->hash.disk);
		if(!index->hash.cache)
			goto fail_hash;
	}
	index->tree.disk = disktree::open("dt");
	if(index->tree.disk)
	{
		index->type |= t_index::I_TREE;
		index->tree.cache = new memcache(index->tree.disk);
		if(!index->tree.cache)
			goto fail_tree;
	}
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return index;
	
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
fail_chdir:
	free(index);
fail_malloc:
	fchdir(cwd_fd);
	close(cwd_fd);
	return NULL;
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

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value value)
{
	if(type == T_ID && (index->type & t_index::I_HASH))
	{
		mm_val_t mm_value;
		static_assert(sizeof(value.v_id) == sizeof(mm_value.u32));
		mm_value.u32 = value.v_id;
		return index->hash.cache->reset_key(&mm_value, &mm_value);
	}
	/* XXX */
	return -ENOSYS;
}
