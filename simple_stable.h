/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_STABLE_H
#define __SIMPLE_STABLE_H

#ifndef __cplusplus
#error simple_stable.h is a C++ header file
#endif

#include "dtable.h"
#include "ctable.h"
#include "stable.h"

class simple_stable : public stable
{
public:
	virtual column_iter * columns() const;
	virtual iter * iterator() const;
	virtual iter * iterator(dtype key) const;
	virtual bool find(dtype key, const char * column, dtype * value) const;
	virtual bool writable() const;
	virtual int append(dtype key, const char * column, const dtype & value);
	virtual int remove(dtype key, const char * column);
	virtual int remove(dtype key);
	int init(int dfd, const char * name, dtable_factory * meta, dtable_factory * data, ctable_factory * columns);
	void deinit();
	inline simple_stable() : md_dfd(-1), dt_meta(NULL), _dt_data(NULL), ct_data(NULL) {}
	inline virtual ~simple_stable()
	{
		if(md_dfd >= 0)
			deinit();
	}
	
	static int create(int dfd, const char * name, dtable_factory * meta_factory, dtable_factory * data_factory, dtype::ctype key_type);
	
private:
	struct column
	{
		const char * name;
		size_t row_count;
		dtype::ctype type;
	};
	
	const column * get_column(const char * column) const;
	int adjust_column(const char * column, ssize_t delta, dtype::ctype type);
	
	class citer : public column_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual const char * name() const;
		virtual size_t row_count() const;
		virtual dtype::ctype type() const;
		inline citer(dtable::iter * source);
		virtual ~citer() {}
	private:
		dtype key;
		blob value;
		dtable::iter * meta;
	};
	
	class siter : public stable::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual const char * column() const;
		virtual dtype value() const;
		inline siter(ctable::iter * source);
		virtual ~siter() {}
	private:
		ctable::iter * data;
	};
	
	int md_dfd;
	dtable * dt_meta;
	dtable * _dt_data;
	ctable * ct_data;
};

#endif /* __SIMPLE_STABLE_H */
