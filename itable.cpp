/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "itable.h"

/* itables are immutable on-disk maps from two keys (the primary key and the
 * secondary key) to a value. The keys can be either integers or strings; the
 * values can be integers, strings, or blobs. The itable's data is sorted on
 * disk first by the primary key and then by the secondary key. */

/* This is the abstract base class of several different kinds of itables. Aside
 * from the obvious implementation that reads a single file storing all the
 * data, there is also a layering itable that allows one itable to overlay
 * another, or for atables (append-only tables) to overlay itables. */
