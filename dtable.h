/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_H
#define __DTABLE_H

#include <errno.h>

#ifndef __cplusplus
#error dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtype.h"
#include "params.h"
#include "blob_comparator.h"

/* data tables */

class ktable
{
public:
	virtual bool present(const dtype & key, bool * found) const = 0;
	/* contains(key) == dtable::find(key).exists(), but maybe more efficient */
	inline bool contains(const dtype & key) const { bool found; return present(key, &found); }
	inline dtype::ctype key_type() const { return ktype; }
	inline const blob_comparator * get_blob_cmp() const { return blob_cmp; }
	inline const istr & get_cmp_name() const { return cmp_name; }
	inline ktable() : blob_cmp(NULL) {}
	inline virtual ~ktable() {}
	
protected:
	dtype::ctype ktype;
	const blob_comparator * blob_cmp;
	/* the required blob_comparator name, if any */
	istr cmp_name;
	
private:
	void operator=(const ktable &);
	ktable(const ktable &);
};

class dtable : public ktable
{
public:
	class key_iter
	{
		/* Since these iterators are virtual, we will have a pointer to them
		 * rather than an actual instance when we're using them. As a result,
		 * it is not as useful to override operators, because we'd have to
		 * dereference the local variable in order to use the overloaded
		 * operators. In particular we'd need ++*it instead of just ++it, yet
		 * both would compile without error. So, we use next() etc. here. */
	public:
		/* Iterators may point at any valid entry, or a single "invalid" entry
		 * which is immediately after the last valid entry. In the case of an
		 * empty dtable, the invalid entry is the only entry. */
		
		/* Returns true if the iterator currently points at a valid entry. */
		virtual bool valid() const = 0;
		
		/* next() and prev() return true if the iterator is moved forward or
		 * backward, respectively, to a valid entry - note that prev() will
		 * not move past the first entry, but will return false when called
		 * when the cursor already points at the first entry. */
		virtual bool next() = 0;
		virtual bool prev() = 0;
		
		/* first() and last() return true if the iterator is left pointing at
		 * the first or last entry, which (excepting errors) can only fail if
		 * the dtable itself is empty and thus has no valid entries. */
		virtual bool first() = 0;
		virtual bool last() = 0;
		
		/* Returns the key the iterator currently points at, or calls abort()
		 * if the iterator does not point at a valid entry. */
		virtual dtype key() const = 0;
		
		/* Seeks this iterator to the requested key, or the next key if the
		 * requested key is not present. Returns true if the requested key was
		 * found, and false otherwise. In the latter case valid() should be
		 * called to check whether the iterator points at a valid entry. */
		virtual bool seek(const dtype & key) = 0;
		virtual bool seek(const dtype_test & test) = 0;
		/* Seeks this iterator to the requested index. May not be supported by
		 * all dtables. See dtable_factory::indexed_access() for details. */
		virtual bool seek_index(size_t index) { return false; }
		/* Gets an index for later use with seek_index(). Same restrictions. */
		virtual size_t get_index() const { return (size_t) -1; }
		
		/* Wrappers for the underlying dtable methods of the same names. */
		virtual dtype::ctype key_type() const = 0;
		virtual const blob_comparator * get_blob_cmp() const = 0;
		virtual const istr & get_cmp_name() const = 0;
		
		inline key_iter() {}
		virtual ~key_iter() {}
	private:
		void operator=(const key_iter &);
		key_iter(const key_iter &);
	};
	class iter : public key_iter
	{
	public:
		virtual metablob meta() const = 0;
		virtual blob value() const = 0;
		virtual const dtable * source() const = 0;
		
		/* When a disk-based dtable's create() method is reading data from an
		 * input iterator, it may find that it cannot store some particular
		 * value. In that case, it should call reject() on the iterator. If
		 * reject() returns true, the rejection has been handled in some way,
		 * and the create() method can continue. Otherwise it should return
		 * an error, as the value cannot be stored as requested. */
		virtual bool reject() { return false; }
		
		inline iter() {}
		virtual ~iter() {}
	private:
		void operator=(const iter &);
		iter(const iter &);
	};
	/* for iterators that want to have dtable wrappers implemented for them */
	template<class T, class P = iter> class iter_source : public P
	{
	public:
		virtual dtype::ctype key_type() const { return dt_source->key_type(); }
		virtual const blob_comparator * get_blob_cmp() const { return dt_source->get_blob_cmp(); }
		virtual const istr & get_cmp_name() const { return dt_source->get_cmp_name(); }
		inline iter_source(const T * dt_source) : dt_source(dt_source) {}
		/* for wrapper iterators, e.g. dtable_wrap_iter */
		inline iter_source(iter * base, const T * dt_source) : P(base), dt_source(dt_source) {}
	protected:
		const T * dt_source;
	};
	
	virtual iter * iterator() const = 0;
	virtual blob lookup(const dtype & key, bool * found) const = 0;
	inline blob find(const dtype & key) const { bool found; return lookup(key, &found); }
	/* index(), contains_index(), and size() only work when iter::seek_index() works, see above */
	inline virtual blob index(size_t index) const { return blob(); }
	inline virtual bool contains_index(size_t index) const { return false; }
	inline virtual size_t size() const { return (size_t) -1; }
	inline virtual bool writable() const { return false; }
	inline virtual int insert(const dtype & key, const blob & blob, bool append = false) { return -ENOSYS; }
	inline virtual int remove(const dtype & key) { return -ENOSYS; }
	inline dtable() {}
	/* subclass destructors should [indirectly] call dtable::deinit(), but do it here too */
	inline virtual ~dtable() { if(blob_cmp) deinit(); }
	
	/* when using blob keys and a custom blob comparator, this will be necessary */
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
	
	/* subclasses can specify that they support indexed access */
	static inline bool static_indexed_access(const params & config) { return false; }
	
protected:
	inline void deinit()
	{
		if(blob_cmp)
		{
			blob_cmp->release();
			blob_cmp = NULL;
		}
		cmp_name = NULL;
	}
	
	/* helper for create() methods: checks source and shadow to make sure they agree */
	static inline bool source_shadow_ok(dtable::iter * source, const ktable * shadow)
	{
		if(!shadow)
			return true;
		if(source->key_type() != shadow->key_type())
			return false;
		if(shadow->key_type() == dtype::BLOB)
		{
			const blob_comparator * source_cmp = source->get_blob_cmp();
			/* TODO: check get_cmp_name() first? */
			/* we don't require blob comparators to be the same
			 * object, but both must either exist or not exist */
			if(!source_cmp != !shadow->get_blob_cmp())
				return false;
			/* and if they exist, they must have the same name */
			if(source_cmp && strcmp(source_cmp->name, shadow->get_blob_cmp()->name))
				return false;
		}
		return true;
	}
	
private:
	void operator=(const dtable &);
	dtable(const dtable &);
};

#endif /* __DTABLE_H */
