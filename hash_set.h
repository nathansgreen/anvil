/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef HASH_SET_H
#define HASH_SET_H

#include "hash_map.h"

typedef hash_map_t hash_set_t;

// Create a hash_set.
hash_set_t * hash_set_create(void);
hash_set_t * hash_set_create_ptr(void);
hash_set_t * hash_set_create_str(void);
// Create a hash_set, reserve space for n entries, allow/don't auto resizing.
hash_set_t * hash_set_create_size(size_t n, bool auto_resize);
hash_set_t * hash_set_create_size_ptr(size_t n, bool auto_resize);
hash_set_t * hash_set_create_size_str(size_t n, bool auto_resize);
// Create a hash set that contains the same elements as hs
hash_set_t * hash_set_copy(const hash_set_t * hs);
// Destroy a hash_set, does not destroy keys or vals.
void         hash_set_destroy(hash_set_t * hs);

// Return number of items in the hash_set.
size_t hash_set_size(const hash_set_t * hs);
// Return whether hash_set is empty.
bool   hash_set_empty(const hash_set_t * hs);
// Insert the given value.
// Returns 0 or 1 on success, or -ENOMEM.
int    hash_set_insert(hash_set_t * hs, const void * v);
// Remove the given value. Does not destroy the value.
// Returns 0 on success, or -ENOENT if v is not in the hash_set.
int    hash_set_erase(hash_set_t * hs, const void * v);
// Remove all values. Does not destroy the values.
void   hash_set_clear(hash_set_t * hs);
// Return 0 if v is not in the hash_set, and nonzero otherwise.
int    hash_set_lookup(const hash_set_t * hs, const void * v);

// Return the number of buckets currently allocated.
size_t hash_set_bucket_count(const hash_set_t * hs);
// Resize the number of buckets to n.
// Returns 0 on success, 1 on no resize needed, or -ENOMEM.
int    hash_set_resize(hash_set_t * hs, size_t n);


typedef hash_map_it2_t hash_set_it2_t;

hash_set_it2_t hash_set_it2_create(hash_set_t * hs);
// Iterate through the hash set values using it.
// - Returns false once the end of the hash set is reached.
// - Behavior is undefined if you begin iterating, then insert an element,
//   resize the set, or delete the next element, and then continue iterating
//   using the old iterator. (Define HASH_MAP_IT_MOD_DEBUG to detect some
//   cases.)
bool hash_set_it2_next(hash_set_it2_t * it);


typedef hash_map_it_t hash_set_it_t;

void hash_set_it_init(hash_set_it_t * it, hash_set_t * hs);
// Iterate through the hash set values using hs_it.
// - Returns NULL when the end of the hash set is reached.
// - Behavior is undefined if you begin iterating, modify hs, and then continue
//   iterating using the old hs_it. (Define HASH_MAP_IT_MOD_DEBUG to detect.)
const void * hash_set_val_next(hash_set_it_t * it);

#endif /* !HASH_SET_H */
