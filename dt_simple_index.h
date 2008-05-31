/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DT_SIMPLE_INDEX_H
#define __DT_SIMPLE_INDEX_H

#ifndef __cplusplus
#error dt_simple_index.h is a C++ header file
#endif

#include "dtable.h"
#include "dt_index.h"
#include "dtype.h"
#include "journal_dtable.h"
#include "blob_buffer.h"

class dt_simple_index : public dt_index
{
public:
	inline virtual bool unique() const
	{
		return is_unique;
	}
	
	inline virtual bool writable() const
	{
		return rw_store ? rw_store->writable() : false;
	}
	
	virtual int map(const dtype & key, dtype & value) const;
	
	virtual iter * iterator() const;
	virtual iter * iterator(dtype key) const;
	
	virtual int set(const dtype & key, const dtype & pri);
	virtual int remove(const dtype & key);
	
	virtual int add(const dtype & key, const dtype & pri);
	virtual int update(const dtype & key, const dtype & old_pri, const dtype & new_pri);
	virtual int remove(const dtype & key, const dtype & pri);
	
	inline dt_simple_index() : ro_store(NULL), rw_store(NULL) {}
	/* read only version */
	int init(const dtable * store, const dtable * table, bool unique);
	/* if you want this index to be writable */
	int init(dtable * store, const dtable * table, bool unique);
	inline virtual ~dt_simple_index() {}
	
private:
	bool is_unique;
	const dtable * ref_table;
	const dtable * ro_store;
	dtable * rw_store;
	int find(const blob & b, const dtype & pri, uint32_t & idx, uint32_t & next, dtype * set = NULL) const;

	class iter : public dt_index::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual dtype pri() const;
		inline iter(const dt_simple_index * src, dtable::iter * iter, dtype::ctype type);
		inline iter(const dt_simple_index * src, const dtype & key, dtype::ctype type);
		virtual ~iter() {}

	private:
		dtype seckey;
		blob multi_value;
		const dt_simple_index * source;
		dtable::iter * store;
		dtype::ctype pritype;
		uint32_t offset;
		bool is_valid;
	};
};

#endif /* __DT_SIMPLE_INDEX_H */
