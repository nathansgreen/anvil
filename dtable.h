/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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
#include "atomic.h"
#include "params.h"
#include "callback.h"
#include "blob_comparator.h"

/* abortable transaction handle */
typedef uint32_t abortable_tx;
#define NO_ABORTABLE_TX ((abortable_tx) 0)
/* make it easier to add this parameter to many functions */
#define ATX_DEF abortable_tx atx
/* for prototypes, required or optional parameter */
#define ATX_REQ ATX_DEF
#define ATX_OPT ATX_REQ = NO_ABORTABLE_TX

/* key tables (used for shadow checks) */
class ktable
{
public:
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const = 0;
	/* contains(key) == dtable::find(key).exists(), but maybe more efficient */
	inline bool contains(const dtype & key, ATX_OPT) const { bool found; return present(key, &found, atx); }
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

/* data tables */
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
		 * and the create() method can continue by storing the replacement
		 * value. Otherwise, or if the replacement value cannot be stored, it
		 * should return an error, as it cannot store the requested value. */
		virtual bool reject(blob * replacement) { return false; }
		
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
		inline iter_source(const T * dt_source) : dt_source(dt_source) { dt_source->retain(); }
		/* for wrapper iterators, e.g. dtable_wrap_iter */
		inline iter_source(iter * base, const T * dt_source) : P(base), dt_source(dt_source) { dt_source->retain(); }
		inline ~iter_source() { dt_source->release(); }
	protected:
		const T * dt_source;
	};
	
	/* read-only stuff */
	virtual iter * iterator(ATX_OPT) const = 0;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const = 0;
	inline blob find(const dtype & key, ATX_OPT) const { bool found; return lookup(key, &found, atx); }
	/* index(), contains_index(), and size() only work when iter::seek_index() works, see above */
	inline virtual blob index(size_t index) const { return blob(); }
	inline virtual bool contains_index(size_t index) const { return false; }
	inline virtual size_t size() const { return (size_t) -1; }
	
	inline virtual bool writable() const { return false; }
	/* writable dtables support these */
	inline virtual int insert(const dtype & key, const blob & blob, bool append = false, ATX_OPT) { return -ENOSYS; }
	inline virtual int remove(const dtype & key, ATX_OPT) { return -ENOSYS; }
	
	/* abortable transactions; not supported by default */
	inline virtual abortable_tx create_tx() { return NO_ABORTABLE_TX; }
	inline virtual int commit_tx(ATX_REQ) { return -ENOSYS; }
	inline virtual void abort_tx(ATX_REQ) {}
	
	inline dtable() : usage(0) {}
	/* calls the destructor by default, but can be overridden */
	inline virtual void destroy() const { delete this; }
	
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
	inline virtual int maintain(bool force = false) { return 0; }
	
	/* subclasses can specify that they support indexed access */
	static inline bool static_indexed_access(const params & config) { return false; }
	
	/* Iterators can refer to the dtables from which they came, so dtables
	 * must not be destroyed until all their iterators have been destroyed.
	 * We keep a reference count and support a callback mechanism to allow
	 * notification when the last iterator is destroyed. */
	/* NOTE: It is not guaranteed that a new iterator has not been created
	 * between the usage count reaching zero and the callback being called;
	 * therefore, callbacks should usually only be registered once no new
	 * iterators will be created for the dtable in question. */
	inline bool in_use() const { return usage.get() > 0; }
	inline void add_unused_callback(callback * cb) { unused_callbacks.add(cb); }
	inline void remove_unused_callback(callback * cb) { unused_callbacks.remove(cb); }
	
	/* dtables which return the iterators of another dtable, instead of
	 * their own, need to chain the usage callbacks so that they will
	 * correctly appear "in use" and call their own callbacks when the
	 * underlying dtable is no longer in use. See bloom_dtable for an
	 * example of how to use this functionality. */
	class chain_callback : public callback
	{
	public:
		inline chain_callback(const dtable * target) : target(target) {}
		virtual void invoke() { target->release(); }
		/* This probably should never happen: the underlying dtable has an
		 * outstanding iterator yet is being destroyed. Nevertheless, our
		 * usage count is +1 because of it, so we treat it like invoke(). */
		virtual void release() { target->release(); }
	private:
		const dtable * target;
	};
	
protected:
	/* iterator usage counting */
	inline void retain() const { usage.inc(); }
	inline void release() const { if(!usage.dec()) unused_callbacks.invoke(); }
	inline iter * iterator_chain_usage(chain_callback * chain, dtable * source, ATX_OPT) const
	{
		iter * it = source->iterator(atx);
		if(it && !usage.get())
		{
			retain();
			source->add_unused_callback(chain);
		}
		return it;
	}
	
	inline void deinit()
	{
		/* should we warn if it is in use? */
		unused_callbacks.release();
		usage.zero();
		if(blob_cmp)
		{
			blob_cmp->release();
			blob_cmp = NULL;
		}
		cmp_name = NULL;
	}
	
	/* subclass destructors should [indirectly] call dtable::deinit() to avoid these asserts */
	inline virtual ~dtable() { assert(!blob_cmp); assert(!usage.get()); }
	
	/* helper for create() methods: checks source and shadow to make sure they agree */
	template<class T>
	static inline bool source_shadow_ok(T * source, const ktable * shadow)
	{
		if(!shadow)
			return true;
		if(source->key_type() != shadow->key_type())
			return false;
		if(shadow->key_type() == dtype::BLOB)
		{
			const blob_comparator * source_cmp = source->get_blob_cmp();
			const blob_comparator * shadow_cmp = shadow->get_blob_cmp();
			/* TODO: check get_cmp_name() first? */
			/* we don't require blob comparators to be the same
			 * object, but both must either exist or not exist */
			if(!source_cmp != !shadow_cmp)
				return false;
			/* and if they exist, they must have the same name */
			if(source_cmp && strcmp(source_cmp->name, shadow_cmp->name))
				return false;
		}
		return true;
	}
	
	/* these IDs, unlike sys_journal IDs, are ephemeral and
	 * restart from zero every time the system starts up */
	static inline abortable_tx create_tx_id()
	{
		abortable_tx atx;
		do { atx = atx_handle.inc(); } while(atx == NO_ABORTABLE_TX);
		return atx;
	}
	
private:
	mutable atomic<int> usage;
	mutable callbacks unused_callbacks;
	
	static atomic<abortable_tx> atx_handle;
	
	void operator=(const dtable &);
	dtable(const dtable &);
};

#endif /* __DTABLE_H */
