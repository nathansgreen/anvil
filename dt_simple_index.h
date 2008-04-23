/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DT_SIMPLE_INDEX_H
#define __DT_SIMPLE_INDEX_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error dt_simple_index.h is a C++ header file
#endif

#include "dt_index.h"
#include "dtable.h"
#include "journal_dtable.h"

class dt_simple_index : public dt_index
{
public:
	inline virtual bool is_unique() const
	{
		return unique;
	}
	/* only usable if is_unique() returns true */
	virtual dtype map(dtype key) const;
	virtual dt_index_iter * iterator(dtype key) const;
	virtual dt_index_iter * iterator() const;
	
	int set(dtype key, dtype pri);
	int remove(dtype key);
	
	int add(dtype key, dtype pri);
	int update(dtype key, dtype old_pri, dtype new_pri);
	int remove(dtype key, dtype pri);
	
	inline dt_simple_index() {}
	int init(const dtable * store, journal_dtable * append, bool unique);
	inline virtual ~dt_simple_index() {}
	
private:
	bool unique;
	const dtable * store;
	const journal_dtable * append;
};

#endif /* __DT_SIMPLE_INDEX_H */