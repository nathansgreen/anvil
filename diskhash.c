/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "diskhash.h"

/* create a new diskhash using the specified store path */
int diskhash_init(const char * store, dh_type_t key_type, dh_type_t val_type)
{
	/* XXX */
	return mkdir(store, 0775);
}

/* basically just rm -rf */
int diskhash_drop(const char * store)
{
}

/* open a diskhash */
diskhash_t * diskhash_open(const char * store)
{
}

/* close a diskhash */
int diskhash_close(diskhash_t * dh)
{
}


/* get diskhash size */
size_t diskhash_size(diskhash_t * dh)
{
}


/* insert a new entry or replace an existing entry */
int diskhash_insert(diskhash_t * dh, const dh_val_t * key, const dh_val_t * val)
{
}

/* remove an existing entry */
int diskhash_erase(diskhash_t * dh, const dh_val_t * key)
{
}

/* look up an entry */
int diskhash_lookup(diskhash_t * dh, const dh_val_t * key, dh_val_t * val)
{
}


/* initialize an iterator */
int diskhash_it_init(diskhash_t * dh, diskhash_it_t * it)
{
}

/* get the next entry, or the first if the iterator is new */
int diskhash_it_next(diskhash_it_t * it)
{
}

/* free any resources used by the iterator */
int diskhash_it_done(diskhash_it_t * it)
{
}
