/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __UNIQ_DTABLE_H
#define __UNIQ_DTABLE_H

#ifndef __cplusplus
#error uniq_dtable.h is a C++ header file
#endif

#include <ext/hash_map>

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"
#include "concat_queue.h"

/* TODO: if the value base supports indexed access, use that instead of the
 * integer keys we store in it. For some underlying dtable types that may be
 * much faster, although really the user should just use one where it's the same
 * anyway (e.g. simple dtable or linear dtable). */

class uniq_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	virtual size_t size() const;
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = keybase->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	/* uniq_dtable supports indexed access if its keybase does */
	static bool static_indexed_access(const params & config);
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(uniq_dtable);
	
	inline uniq_dtable() : keybase(NULL), valuebase(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~uniq_dtable()
	{
		if(keybase)
			deinit();
	}
	
private:
	class iter : public iter_source<uniq_dtable, dtable_wrap_iter>
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		inline iter(dtable::iter * base, const uniq_dtable * source);
		virtual ~iter() {}
	};
	
	class sliding_window
	{
	public:
		struct idx_ref
		{
			uint32_t index;
			/* rejected if replacement exists */
			blob replacement;
		};
		
		inline sliding_window(size_t window_size) : window_size(window_size), next_index(0) {}
		idx_ref * append(const blob & value, bool * store = NULL);
		inline void reset()
		{
			values.clear();
			queue.clear();
			next_index = 0;
		}
	private:
		typedef __gnu_cxx::hash_map<blob, idx_ref, blob_hashing_comparator, blob_hashing_comparator> value_map;
		typedef concat_queue<blob> value_queue;
		
		value_map values;
		value_queue queue;
		size_t window_size;
		uint32_t next_index;
	};
	
	/* used in create() to wrap source iterators on the way down */
	class rev_iter_key : public dtable_wrap_iter
	{
	public:
		inline virtual bool next() { return advance(false); }
		virtual bool first();
		virtual metablob meta() const;
		inline virtual blob value() const { return current_value; }
		
		/* the keybase is not allowed to reject: it will get nice
		 * predictable values and should be able to store them all */
		inline virtual bool reject(blob * replacement) { return false; }
		
		/* we *could* implement these, but it would be really inefficient and it's
		 * not really necessary as no create() methods actually use these methods
		 * (and this iterator is only ever passed to create() methods) */
		inline virtual bool prev() { abort(); }
		inline virtual bool last() { abort(); }
		inline virtual bool seek(const dtype & key) { abort(); }
		inline virtual bool seek(const dtype_test & test) { abort(); }
		
		/* these don't make sense for downward-passed iterators */
		inline virtual bool seek_index(size_t index) { abort(); }
		inline virtual size_t get_index() const { abort(); }
		
		rev_iter_key(dtable::iter * base, size_t window_size);
		virtual ~rev_iter_key() {}
	private:
		bool advance(bool do_first);
		
		sliding_window window;
		blob current_value;
	};
	
	class rev_iter_value : public dtable::iter
	{
	public:
		inline virtual bool valid() const { return current_valid; }
		inline virtual bool next() { return advance(false); }
		virtual bool first();
		inline virtual dtype key() const { return current_key; }
		inline virtual metablob meta() const { return metablob(current_value); }
		inline virtual blob value() const { return current_value; }
		
		/* we need to be in the loop for rejection */
		virtual bool reject(blob * replacement);
		
		/* we *could* implement these, but it would be really inefficient and it's
		 * not really necessary as no create() methods actually use these methods
		 * (and this iterator is only ever passed to create() methods) */
		inline virtual bool prev() { abort(); }
		inline virtual bool last() { abort(); }
		inline virtual bool seek(const dtype & key) { abort(); }
		inline virtual bool seek(const dtype_test & test) { abort(); }
		
		/* these don't make sense for downward-passed iterators */
		inline virtual bool seek_index(size_t index) { abort(); }
		inline virtual size_t get_index() const { abort(); }
		
		/* we always have the integer key type for the valuebase */
		inline virtual dtype::ctype key_type() const { return dtype::UINT32; }
		inline virtual const blob_comparator * get_blob_cmp() const { return NULL; }
		inline virtual const istr & get_cmp_name() const { return istr::null; }
		inline virtual const dtable * source() const { return base->source(); }
		
		rev_iter_value(dtable::iter * base, size_t window_size);
		virtual ~rev_iter_value() {}
		
		bool failed;
		
	private:
		bool advance(bool do_first);
		
		dtable::iter * base;
		sliding_window window;
		dtype current_key;
		blob current_value;
		bool current_valid;
	};
	
	dtable * keybase;
	dtable * valuebase;
};

#endif /* __UNIQ_DTABLE_H */
