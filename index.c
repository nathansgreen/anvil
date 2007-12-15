/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "toilet.h"
#include "diskhash.h"
#include "index.h"

t_index * toilet_open_index(const char * path, const char * name)
{
	t_index * index;
	int cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return NULL;
	index = malloc(sizeof(*index));
	if(!index)
		goto fail_malloc;
	index->type = I_NONE;
	if(chdir(path) < 0)
		goto fail_chdir;
	if(chdir(name) < 0)
		goto fail_chdir;
	index->hash.disk = diskhash_open("dh");
	if(index->hash.disk)
	{
		index->type |= I_HASH;
		index->hash.cache = hash_map_create();
		if(!index->hash.cache)
			goto fail_hash;
	}
	/* XXX: tree */
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return index;
	
	if(index->hash.disk)
fail_hash:
		diskhash_close(index->hash.disk);
fail_chdir:
	free(index);
fail_malloc:
	fchdir(cwd_fd);
	close(cwd_fd);
	return NULL;
}

void toilet_close_index(t_index * index)
{
	if(index->type & I_HASH)
	{
		diskhash_close(index->hash.disk);
		/* XXX: free the contents of cache */
		hash_map_destroy(index->hash.cache);
	}
	free(index);
}

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value value)
{
	if(type == T_ID && (index->type & I_HASH))
	{
		dh_val_t dh_value;
		static_assert(sizeof(value.v_id) == sizeof(dh_value.u32));
		dh_value.u32 = value.v_id;
		return diskhash_insert(index->hash.disk, &dh_value, &dh_value);
	}
	/* XXX */
	return -ENOSYS;
}
