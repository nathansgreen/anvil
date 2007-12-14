/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_H
#define __INDEX_H

#include "hash_map.h"
#include "diskhash.h"
#include "toilet.h"

#ifdef __cplusplus
extern "C" {
#endif

struct t_index {
	/* note that I_BOTH == I_HASH | I_TREE */
	enum { I_NONE = 0, I_HASH = 1, I_TREE = 2, I_BOTH = 3 } type;
	struct {
		/* value -> rowset */
		hash_map_t * cache;
		/* value -> blob of row IDs */
		diskhash_t * disk;
	} hash;
	void * tree;
};

t_index * toilet_open_index(const char * path, const char * name);
void toilet_close_index(t_index * index);

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value value);

#ifdef __cplusplus
}
#endif

#endif /* __INDEX_H */
