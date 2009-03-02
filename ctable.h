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
	
	/* column indices */
	inline size_t index(const istr & column) const
	{
		name_map::const_iterator number = column_map.find(column);
		if(number == column_map.end())
			return (size_t) -1;
		assert(number->second < column_count);
		return number->second;
	}
	inline const istr & name(size_t index) const
	{
		assert(index < column_count);
		return column_name[index];
	}
	
	virtual dtable::key_iter * keys() const = 0;
	virtual dtable::iter * values(size_t column) const = 0;
	inline dtable::iter * values(const istr & column) const
	{
		size_t i = index(column);
		return (i != (size_t) -1) ? values(i) : NULL;
	}
	virtual iter * iterator() const = 0;
	inline virtual iter * iterator(const dtype & key) const
	{
		if(!contains(key))
			return NULL;
		iter * i = iterator();
		bool found = i->seek(key);
		assert(found);
		return i;
	}
	virtual blob find(const dtype & key, size_t column) const = 0;
	inline blob find(const dtype & key, const istr & column) const
	{
		size_t i = index(column);
		return (i != (size_t) -1) ? find(key, i) : blob();
	}
	virtual bool contains(const dtype & key) const = 0;
	virtual bool writable() const = 0;
	virtual int insert(const dtype & key, size_t column, const blob & value, bool append = false) = 0;
	inline int insert(const dtype & key, const istr & column, const blob & value, bool append = false)
	{
		size_t i = index(column);
		return (i != (size_t) -1) ? insert(key, i, value, append) : -1;
	}
	/* remove just a column */
	virtual int remove(const dtype & key, size_t column) = 0;
	inline int remove(const dtype & key, const istr & column)
	{
		size_t i = index(column);
		return (i != (size_t) -1) ? remove(key, i) : -1;
	}
	/* remove the whole row */
	virtual int remove(const dtype & key) = 0;
	inline dtype::ctype key_type() const { return ktype; }
	inline const blob_comparator * get_blob_cmp() const { return blob_cmp; }
	inline const istr & get_cmp_name() const { return cmp_name; }
	inline ctable() : blob_cmp(NULL), column_count(0), column_name(0) {}
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
	inline virtual int maintain(bool force = false) { return 0; }
	
	struct colval
	{
		size_t index;
		blob value;
	};
	/* default implementations of multi-column methods */
	virtual int find(const dtype & key, colval * values, size_t count) const
	{
		for(size_t i = 0; i < count; i++)
			values[i].value = find(key, values[i].index);
		return 0;
	}
	virtual int insert(const dtype & key, const colval * values, size_t count, bool append = false)
	{
		int r = tx_start_r();
		if(r < 0)
			return r;
		for(size_t i = 0; i < count; i++)
			if((r = insert(key, values[i].index, values[i].value, append)) < 0)
				break;
		tx_end_r();
		return r;
	}
	virtual int remove(const dtype & key, size_t * columns, size_t count)
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
	
	typedef std::map<istr, size_t, strcmp_less> name_map;
	
	size_t column_count;
	istr * column_name;
	name_map column_map;
	
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
