/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DT_INDEX_H
#define __DT_INDEX_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error dt_index.h is a C++ header file
#endif

#include "dtable.h"

class dt_index;

class dt_index_iter
{
public:
	virtual bool valid() const = 0;
	/* operator++ can suck it; we're using next() */
	virtual bool next() = 0;
	virtual dtype key() const = 0;
	virtual dtype pri() const = 0;
	virtual const dt_index * source() const = 0;
	virtual ~dt_index_iter() {}
};

/* secondary indices */

class dt_index
{
public:
	virtual bool is_unique() const = 0;
	/* only usable if is_unique() returns true */
	virtual dtype map(dtype key) const = 0;
	virtual dt_index_iter * iterator(dtype key) const = 0;
	virtual dt_index_iter * iterator() const = 0;
	virtual ~dt_index() {};
};

#endif /* __DT_INDEX_H */
