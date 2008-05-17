/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_STABLE_H
#define __SIMPLE_STABLE_H

#ifndef __cplusplus
#error simple_stable.h is a C++ header file
#endif

#include <map>

#include "dtable.h"
#include "ctable.h"
#include "stable.h"
#include "dtable_factory.h"

class simple_stable : public stable
{
public:
	virtual column_iter * columns() const;
	virtual size_t column_count() const;
	virtual size_t row_count(const char * column) const;
	virtual dtype::ctype column_type(const char * column) const;
	
	virtual dtable::key_iter * keys() const;
	virtual iter * iterator() const;
	virtual iter * iterator(dtype key) const;
	
	virtual bool find(dtype key, const char * column, dtype * value) const;
	
	virtual bool writable() const;
	
	virtual int append(dtype key, const char * column, const dtype & value);
	virtual int remove(dtype key, const char * column);
	virtual int remove(dtype key);
	
	virtual dtype::ctype key_type() const;
	
	int init(int dfd, const char * name, const params & config, ctable_factory * columns);
	void deinit();
	inline simple_stable() : md_dfd(-1), dt_meta(NULL), _dt_data(NULL), ct_data(NULL) {}
	inline virtual ~simple_stable()
	{
		if(md_dfd >= 0)
			deinit();
	}
	
	static int create(int dfd, const char * name, const params & config, dtype::ctype key_type);
	
private:
	struct strcmp_less
	{
		inline bool operator()(const char * a, const char * b) const
		{
			return strcmp(a, b) < 0;
		}
	};
	
	struct column_info
	{
		size_t row_count;
		dtype::ctype type;
	};
	
	/* /me dislikes std::map immensely */
	typedef std::map<const char *, column_info, strcmp_less> std_column_map;
	typedef std_column_map::const_iterator column_map_iter;
	typedef std_column_map::iterator column_map_full_iter;
	std_column_map column_map;
	
	int load_columns();
	const column_info * get_column(const char * column) const;
	int adjust_column(const char * column, ssize_t delta, dtype::ctype type);
	
	class citer : public column_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual const char * name() const;
		virtual size_t row_count() const;
		virtual dtype::ctype type() const;
		inline citer(column_map_iter source, column_map_iter last) : meta(source), end(last) {}
		virtual ~citer() {}
	private:
		column_map_iter meta;
		column_map_iter end;
	};
	
	class siter : public stable::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual const char * column() const;
		virtual dtype value() const;
		inline siter(ctable::iter * source, const stable * types) : data(source), meta(types) {}
		virtual ~siter() {}
	private:
		ctable::iter * data;
		const stable * meta;
	};
	
	int md_dfd;
	dtable * dt_meta;
	dtable * _dt_data;
	ctable * ct_data;
};

#endif /* __SIMPLE_STABLE_H */
