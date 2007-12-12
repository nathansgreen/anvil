/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_H
#define __INDEX_H

#include "toilet.h"

t_index * toilet_get_index(const char * path, const char * name);
void toilet_free_index(t_index * index);

#endif /* __INDEX_H */
