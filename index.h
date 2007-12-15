/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_H
#define __INDEX_H

#include "toilet.h"

#ifdef __cplusplus

#include "multimap.h"
#include "mm_diskhash.h"
#include "mm_disktree.h"
#include "mm_memcache.h"

struct t_index {
	/* note that I_BOTH == I_HASH | I_TREE */
	enum i_type { I_NONE = 0, I_HASH = 1, I_TREE = 2, I_BOTH = 3 } type;
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

/* Annoyingly, enums do not by themselves support these bitwise operations, due
 * to type errors. Rather than force the expansion of |= and &= everywhere to
 * add casts, we do it here as inline operators. */

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

int toilet_index_init(const char * path);

t_index * toilet_open_index(const char * path, const char * name);
void toilet_close_index(t_index * index);

int toilet_index_add(t_index * index, t_row_id id, t_type type, t_value value);

#ifdef __cplusplus
}
#endif

#endif /* __INDEX_H */
