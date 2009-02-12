/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_CTABLE_H
#define __SIMPLE_CTABLE_H

#ifndef __cplusplus
#error simple_ctable.h is a C++ header file
#endif

#include "ctable.h"
#include "sub_blob.h"
#include "ctable_factory.h"

class simple_ctable : public ctable
{
public:
	virtual dtable::key_iter * keys() const;
	virtual iter * iterator() const;
	virtual iter * iterator(const dtype & key) const;
	virtual blob find(const dtype & key, const istr & column) const;
	virtual blob find(const dtype & key, size_t column) const;
	virtual bool contains(const dtype & key) const;
	
	inline virtual bool writable() const
	{
		return base->writable();
	}
	
	virtual int insert(const dtype & key, const istr & column, const blob & value, bool append = false);
	virtual int insert(const dtype & key, size_t column, const blob & value, bool append = false);
	virtual int remove(const dtype & key, const istr & column);
	virtual int remove(const dtype & key, size_t column);
	inline virtual int remove(const dtype & key)
	{
		return base->remove(key);
	}
	
	virtual int maintain()
	{
		return base->maintain();
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
	class iter : public ctable::iter
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
		virtual const istr & column() const;
		virtual blob value() const;
		inline iter(dtable::iter * src);
		inline iter(const blob & value);
		virtual ~iter()
		{
			if(columns)
				delete columns;
			if(source)
				delete source;
		}
		
	private:
		inline void advance();
		
		dtable::iter * source;
		/* need to keep the row around so its iterator will work */
		sub_blob row;
		sub_blob::iter * columns;
	};
	
	dtable * base;
};

#endif /* __SIMPLE_CTABLE_H */
