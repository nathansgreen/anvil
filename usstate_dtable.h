/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __USSTATE_DTABLE_H
#define __USSTATE_DTABLE_H

#ifndef __cplusplus
#error usstate_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* 50 states plus DC and Puerto Rico */
#define USSTATE_COUNT 52

class usstate_dtable : public dtable
{
public:
	static const blob state_codes[USSTATE_COUNT];
	
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	virtual size_t size() const;
	/* writable, insert, remove? */
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = base->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	/* usstate_dtable supports indexed access if its base does */
	static bool static_indexed_access(const params & config);
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(usstate_dtable);
	
	inline usstate_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~usstate_dtable()
	{
		if(base)
			deinit();
	}
	
private:
	inline void static_asserts()
	{
		/* we're using one byte to store these things */
		static_assert(USSTATE_COUNT <= 255);
	}
	
	class iter : public iter_source<usstate_dtable, dtable_wrap_iter>
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		inline iter(dtable::iter * base, const usstate_dtable * source);
		virtual ~iter() {}
	};
	
	/* used in create() to wrap source iterators on the way down */
	class rev_iter : public dtable_wrap_iter
	{
	public:
		virtual metablob meta() const;
		virtual blob value() const;
		virtual bool reject(blob * replacement);
		inline rev_iter(dtable::iter * base, blob passthrough_value);
		virtual ~rev_iter() {}
		mutable bool failed;
	private:
		blob passthrough_value;
	};
	
	static blob unpack(blob packed, const blob & passthrough_value);
	static bool pack(blob * unpacked, const blob & passthrough_value);
	
	dtable * base;
	blob passthrough_value;
};

#endif /* __USSTATE_DTABLE_H */
