/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __EMPTY_DTABLE_H
#define __EMPTY_DTABLE_H

#ifndef __cplusplus
#error empty_dtable.h is a C++ header file
#endif

#include "dtable.h"

/* the empty dtable always contains nothing */

class empty_dtable : public dtable
{
public:
	virtual iter * iterator() const { return new iter(); }
	inline virtual blob lookup(const dtype & key, const dtable ** source) const { return blob(); }
	inline empty_dtable(dtype::ctype key_type) { ktype = key_type; }
	inline virtual ~empty_dtable() {}
	
private:
	class iter : public dtable::iter
	{
	public:
		virtual bool valid() const { return false; }
		virtual bool next() { return false; }
		virtual bool prev() { return false; }
		virtual bool last() { return false; }
		/* well, really we have nothing to return */
		virtual dtype key() const { return dtype(0u); }
		virtual bool seek(const dtype & key) { return false; }
		virtual metablob meta() const { return metablob(); }
		virtual blob value() const { return blob(); }
		virtual const dtable * source() const { return NULL; }
		virtual ~iter() {}
	};
};

#endif /* __EMPTY_DTABLE_H */
