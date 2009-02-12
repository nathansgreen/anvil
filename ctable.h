/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_H
#define __CTABLE_H

#ifndef __cplusplus
#error ctable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "transaction.h"
#include "blob_comparator.h"

/* column tables */

class ctable
{
public:
	class iter
	{
	public:
		virtual bool valid() const = 0;
		/* see the note about dtable::iter in dtable.h */
		virtual bool next() = 0;
		virtual bool prev() = 0;
		virtual bool first() = 0;
		virtual bool last() = 0;
		/* can't call key() or seek() if you got this iterator via iterator(key) */
		virtual dtype key() const = 0;
		virtual bool seek(const dtype & key) = 0;
		virtual bool seek(const dtype_test & test) = 0;
		virtual dtype::ctype key_type() const = 0;
		virtual const istr & column() const = 0;
		virtual blob value() const = 0;
		inline iter() {}
		virtual ~iter() {}
	private:
		void operator=(const iter &);
		iter(const iter &);
	};
	
	virtual dtable::key_iter * keys() const = 0;
	virtual iter * iterator() const = 0;
	virtual iter * iterator(const dtype & key) const = 0;
	virtual blob find(const dtype & key, const istr & column) const = 0;
	virtual bool contains(const dtype & key) const = 0;
	virtual bool writable() const = 0;
	virtual int insert(const dtype & key, const istr & column, const blob & value, bool append = false) = 0;
	/* remove just a column */
	virtual int remove(const dtype & key, const istr & column) = 0;
	/* remove the whole row */
	virtual int remove(const dtype & key) = 0;
	inline dtype::ctype key_type() const { return ktype; }
	inline const blob_comparator * get_blob_cmp() const { return blob_cmp; }
	inline const istr & get_cmp_name() const { return cmp_name; }
	inline ctable() : blob_cmp(NULL) {}
	/* subclass destructors should [indirectly] call ctable::deinit() to avoid this assert */
	inline virtual ~ctable() { assert(!blob_cmp); }
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		const char * match = blob_cmp ? blob_cmp->name : cmp_name;
		if(match && strcmp(match, cmp->name))
			return -EINVAL;
		cmp->retain();
		if(blob_cmp)
			blob_cmp->release();
		blob_cmp = cmp;
		return 0;
	}
	
	/* maintenance callback; does nothing by default */
	inline virtual int maintain() { return 0; }
	
	struct colval
	{
		istr name;
		blob value;
	};
	/* default implementations of multi-column methods */
	virtual int insert(const dtype & key, const colval * values, size_t count, bool append = false)
	{
		int r = tx_start_r();
		if(r < 0)
			return r;
		for(size_t i = 0; i < count; i++)
			if((r = insert(key, values[i].name, values[i].value, append)) < 0)
				break;
		tx_end_r();
		return r;
	}
	virtual int remove(const dtype & key, const istr * columns, size_t count)
	{
		int r = tx_start_r();
		if(r < 0)
			return r;
		for(size_t i = 0; i < count; i++)
			if((r = remove(key, columns[i])) < 0)
				break;
		tx_end_r();
		return r;
	}
	
protected:
	dtype::ctype ktype;
	const blob_comparator * blob_cmp;
	/* the required blob_comparator name, if any */
	istr cmp_name;
	
	inline void deinit()
	{
		if(blob_cmp)
		{
			blob_cmp->release();
			blob_cmp = NULL;
		}
		cmp_name = NULL;
	}
	
private:
	void operator=(const ctable &);
	ctable(const ctable &);
};

#endif /* __CTABLE_H */
