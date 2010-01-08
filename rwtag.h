/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __RWTAG_H
#define __RWTAG_H

#include <assert.h>

#ifndef __cplusplus
#error rwtag.h is a C++ header file
#endif

/* This template is called rwtag, not rwlock, because it does not use atomic
 * operations or otherwise make itself thread-safe. It is non-blocking and
 * should only be used in contexts where these properties are appropriate. */

/* NOTE: T must be a signed integer type; unsigned types will not work! */
template<class T>
class rwtag
{
public:
	inline rwtag() : tag(0) {}
	inline ~rwtag() { assert(!tag); }
	
	/* these force you to not copy or overwrite tags that are held */
	inline rwtag(const rwtag & x) : tag(0) { assert(!x.tag); }
	inline rwtag & operator=(const rwtag & x) { assert(!tag); assert(!x.tag); return *this; }
	
	/* returns true on success */
	inline bool read_tag() { if(tag < 0) return false; tag++; return true; }
	
	/* returns true if other readers remain */
	inline bool read_untag() { assert(tag > 0); return --tag > 0; }
	
	/* returns true if there are readers */
	inline bool read_tagged() { return tag > 0; }
	
	/* return true on success */
	inline bool write_tag() { if(tag) return false; tag = -1; return true; }
	inline bool write_upgrade() { if(tag != 1) return false; tag = -1; return true; }
	
	inline void write_untag() { assert(tag < 0); tag = 0; }
	
	/* returns true if there are writers */
	inline bool write_tagged() { return tag < 0; }
	
	/* returns true if there are readers or writers */
	inline bool is_tagged() { return tag != 0; }
	
	inline void reset() { tag = 0; }
	
private:
	T tag;
};

#endif /* __RWTAG_H */
