/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SCHEMA_TABLE_H
#define __SCHEMA_TABLE_H

#ifndef __cplusplus
#error schema_table.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "ctable.h"

/* schema_table isn't an abstract base class, so its iterator doesn't have to be
 * all virtual like this... but the other iterators are all done this way, so
 * we'll be consistent here (besides, later there may be other column_iters) */
class column_iter
{
public:
	virtual bool valid() const = 0;
	/* see the note about dtable_iter in dtable.h */
	virtual bool next() = 0;
	virtual const char * name() const = 0;
	virtual size_t row_count() const = 0;
	virtual dtype::ctype type() const = 0;
	virtual ~column_iter() {}
};

class schema_table : public ctable
{
public:
	virtual ctable_iter * iterator() const;
	virtual ctable_iter * iterator(dtype key) const;
	virtual blob find(dtype key, const char * column) const;
	virtual bool writable() const;
	virtual int append(dtype key, const char * column, const blob & value);
	virtual int remove(dtype key, const char * column);
	virtual int remove(dtype key);
	column_iter * columns() const;
	int init(int dfd, const char * name, dtable_factory * meta, dtable_factory * data, ctable_factory * columns);
	void deinit();
	inline schema_table() : md_dfd(-1), dt_meta(NULL), ct_data(NULL) {}
	inline virtual ~schema_table()
	{
		if(md_dfd >= 0)
			deinit();
	}
	
	static int create(int dfd, const char * name, dtable_factory * meta_factory, dtable_factory * data_factory, dtype::ctype key_type);
	
private:
	int adjust_column(const char * column, ssize_t delta);
	
	struct column
	{
		const char * name;
		size_t row_count;
		dtype::ctype type;
	};
	
	class iter : public column_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual const char * name() const;
		virtual size_t row_count() const;
		virtual dtype::ctype type() const;
		inline iter(dtable_iter * source);
		virtual ~iter() {}
	private:
		dtype key;
		blob value;
		dtable_iter * meta;
	};
	
	int md_dfd;
	dtable * dt_meta;
	ctable * ct_data;
};

#endif /* __SCHEMA_TABLE_H */
