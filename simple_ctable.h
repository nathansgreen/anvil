/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_CTABLE_H
#define __SIMPLE_CTABLE_H

#ifndef __cplusplus
#error simple_ctable.h is a C++ header file
#endif

#include <map>

#include "ctable.h"
#include "index_blob.h"
#include "ctable_factory.h"

#define SIMPLE_CTABLE_MAGIC 0x83E157C8
#define SIMPLE_CTABLE_VERSION 0

class simple_ctable : public ctable
{
public:
	virtual dtable::key_iter * keys() const;
	virtual iter * iterator() const;
	virtual p_iter * iterator(const size_t * columns, size_t count) const;
	virtual blob find(const dtype & key, size_t column) const;
	virtual int find(const dtype & key, colval * values, size_t count) const;
	virtual bool contains(const dtype & key) const;
	
	inline virtual bool writable() const
	{
		return base->writable();
	}
	
	virtual int insert(const dtype & key, size_t column, const blob & value, bool append = false);
	virtual int insert(const dtype & key, const colval * values, size_t count, bool append = false);
	virtual int remove(const dtype & key, size_t column);
	virtual int remove(const dtype & key, size_t * columns, size_t count);
	inline virtual int remove(const dtype & key)
	{
		return base->remove(key);
	}
	
	virtual int maintain(bool force = false)
	{
		return base->maintain(force);
	}
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = base->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = ctable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	inline simple_ctable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline virtual ~simple_ctable()
	{
		if(base)
			deinit();
	}
	
	static int create(int dfd, const char * file, const params & config, dtype::ctype key_type);
	DECLARE_CT_FACTORY(simple_ctable);
	
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
		inline iter(const simple_ctable * base, dtable::iter * source);
		virtual ~iter()
		{
			delete source;
		}
		
	private:
		/* skip forward/backward past any nonexistent stuff */
		inline bool next_column(bool reset = false);
		inline bool prev_column(bool reset = false);
		inline bool advance(bool initial = false);
		inline bool retreat(bool initial = false);
		inline bool next_row(bool initial);
		inline bool prev_row(bool initial);
		
		const simple_ctable * base;
		dtable::iter * source;
		index_blob row;
		size_t number;
	};
	
	/* really just a simplified version of iter above */
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
		inline p_iter(const simple_ctable * base, dtable::iter * source);
		virtual ~p_iter()
		{
			delete source;
		}
		
	private:
		const simple_ctable * base;
		dtable::iter * source;
		index_blob row;
	};
	
	dtable * base;
};

#endif /* __SIMPLE_CTABLE_H */
