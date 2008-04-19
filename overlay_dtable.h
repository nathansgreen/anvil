/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __OVERLAY_DTABLE_H
#define __OVERLAY_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "blob.h"
#include "dtable.h"

#ifndef __cplusplus
#error overlay_dtable.h is a C++ header file
#endif

/* The overlay dtable just combines underlying dtables in the order specified. */

class overlay_dtable : public dtable
{
	virtual dtable_iter * iterator() const;
	virtual blob lookup(dtype key, const dtable ** source) const;
	
	inline overlay_dtable() : tables(NULL), table_count(0) {}
	int init(const dtable * dt1, ...);
	int init(const dtable ** dts, size_t count);
	void deinit();
	inline virtual ~overlay_dtable()
	{
		if(tables)
			deinit();
	}
	
private:
	class iter : public dtable_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const overlay_dtable * source);
		virtual ~iter() { delete[] subs; }
		
	private:
		struct sub
		{
			dtable_iter * iter;
			bool empty, valid;
		};
		
		sub * subs;
		size_t next_index;
		const overlay_dtable * ovr_source;
	};
	
	const dtable ** tables;
	size_t table_count;
};

#endif /* __OVERLAY_DTABLE_H */
