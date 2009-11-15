/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __COLUMN_CTABLE_H
#define __COLUMN_CTABLE_H

#ifndef __cplusplus
#error column_ctable.h is a C++ header file
#endif

#include <map>

#include "ctable.h"
#include "ctable_factory.h"

#define COLUMN_CTABLE_MAGIC 0x36BC4B9D
#define COLUMN_CTABLE_VERSION 0

class column_ctable : public ctable
{
public:
	virtual dtable::key_iter * keys() const;
	virtual iter * iterator() const;
	virtual p_iter * iterator(const size_t * columns, size_t count) const;
	virtual blob find(const dtype & key, size_t column) const;
	virtual bool contains(const dtype & key) const;
	
	inline virtual bool writable() const
	{
		return column_table[0]->writable();
	}
	
	virtual int insert(const dtype & key, size_t column, const blob & value, bool append = false);
	virtual int remove(const dtype & key, size_t column);
	virtual int remove(const dtype & key);
	
	virtual int set_blob_cmp(const blob_comparator * cmp);
	
	virtual int maintain(bool force = false);
	
	inline column_ctable() : column_table(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	void deinit();
	inline virtual ~column_ctable()
	{
		if(column_count)
			deinit();
	}
	
	static int create(int dfd, const char * file, const params & config, dtype::ctype key_type);
	DECLARE_CT_FACTORY(column_ctable);
	
private:
	struct ctable_header
	{
		uint32_t magic;
		uint32_t version;
		uint32_t columns;
	} __attribute__((packed));
	
	class iter : public ctable::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next(bool row = false);
		virtual bool prev(bool row = false);
		virtual bool first();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual dtype::ctype key_type() const;
		virtual size_t column() const;
		virtual const istr & name() const;
		virtual blob value() const;
		virtual blob index(size_t column) const;
		inline iter(const column_ctable * base);
		virtual ~iter()
		{
			for(size_t i = 0; i < base->column_count; i++)
				delete source[i];
			delete[] source;
		}
		
	private:
		bool all_next_skip();
		bool all_prev_skip();
		
		size_t number;
		dtable::iter ** source;
		const column_ctable * base;
	};
	
	class p_iter : public ctable::p_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool first();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual dtype::ctype key_type() const;
		virtual blob value(size_t column) const;
		inline p_iter(const column_ctable * base, const size_t * columns, size_t count);
		virtual ~p_iter()
		{
			for(size_t i = 0; i < base->column_count; i++)
				if(source[i])
					delete source[i];
			delete[] source;
		}
		
	private:
		size_t start;
		dtable::iter ** source;
		const column_ctable * base;
	};
	
	dtable ** column_table;
};

#endif /* __COLUMN_CTABLE_H */
