/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_H
#define __INDEX_H

#include "toilet.h"

#ifdef __cplusplus

#include "multimap.h"
#include "diskhash.h"
#include "disktree.h"
#include "memcache.h"

struct t_index {
	/* note that I_BOTH == I_HASH | I_TREE */
	enum i_type { I_NONE = 0, I_HASH = 1, I_TREE = 2, I_BOTH = 3 } type;
	t_type data_type;
	/* value -> blob of row IDs */
	struct {
		memcache * cache;
		diskhash * disk;
	} hash;
	struct {
		memcache * cache;
		disktree * disk;
	} tree;
};

/* Annoyingly, C++ enums do not by themselves support these bitwise operations,
 * due to type errors. Rather than force the expansion of |= and &= everywhere
 * to add casts, we do it here as inline operators. */

inline t_index::i_type & operator|=(t_index::i_type &x, const t_index::i_type &y)
{
	x = (t_index::i_type) (x | y);
	return x;
}

inline t_index::i_type & operator&=(t_index::i_type &x, const t_index::i_type &y)
{
	x = (t_index::i_type) (x & y);
	return x;
}

extern "C" {
#endif

int toilet_index_init(int dfd, const char * path, t_type type);

t_index * toilet_open_index(uint8_t * id, int dfd, const char * path, const char * name);
void toilet_close_index(t_index * index);

t_type toilet_index_type(t_index * index);

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value value);
int toilet_index_change(t_index * index, t_row_id id, t_type type, t_value old_value, t_value new_value);
int toilet_index_remove(t_index * index, t_row_id id, t_type type, t_value value);

ssize_t toilet_index_count(t_index * index, t_type type, t_value value);
t_rowset * toilet_index_find(t_index * index, t_type type, t_value value);
ssize_t toilet_index_count_range(t_index * index, t_type type, t_value low_value, t_value high_value);
t_rowset * toilet_index_find_range(t_index * index, t_type type, t_value low_value, t_value high_value);

#ifdef __cplusplus
}
#endif

#endif /* __INDEX_H */
