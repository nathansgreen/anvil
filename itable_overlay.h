/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ITABLE_OVERLAY_H
#define __ITABLE_OVERLAY_H

#include <stdint.h>

#include "itable.h"

#ifndef __cplusplus
#error itable_overlay.h is a C++ header file
#endif

class itable_overlay : public itable
{
public:
	/* test whether there is an entry for the given key */
	virtual bool has(iv_int k1);
	virtual bool has(const char * k1);
	
	virtual bool has(iv_int k1, iv_int k2);
	virtual bool has(iv_int k1, const char * k2);
	virtual bool has(const char * k1, iv_int k2);
	virtual bool has(const char * k1, const char * k2);
	
	/* get the offset for the given key */
	virtual off_t get(iv_int k1, iv_int k2);
	virtual off_t get(iv_int k1, const char * k2);
	virtual off_t get(const char * k1, iv_int k2);
	virtual off_t get(const char * k1, const char * k2);
	
	/* iterate through the offsets: set up iterators */
	virtual int iter(struct it * it);
	virtual int iter(struct it * it, iv_int k1);
	virtual int iter(struct it * it, const char * k1);
	virtual void kill_iter(struct it * it);
	
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source = NULL);
	virtual int next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source = NULL);
	virtual int next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source = NULL);
	virtual int next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source = NULL);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int next(struct it * it, iv_int * k1);
	virtual int next(struct it * it, const char ** k1);
	
	inline itable_overlay();
	/* later parameters / higher indices are searched later, so put more recent data first */
	int init(itable * it1, ...);
	int init(itable ** array, size_t length);
	void deinit();
	inline virtual ~itable_overlay();
	
private:
	itable ** tables;
	size_t table_count;
};

struct itable::it::overlay {
	struct it iter;
	int r, empty;
	union {
		iv_int i;
		const char * s;
	} last_k1, last_k2;
	off_t last_off;
};

inline itable_overlay::itable_overlay()
	: tables(NULL), table_count(0)
{
}

inline itable_overlay::~itable_overlay()
{
	deinit();
}

#endif /* __ITABLE_OVERLAY_H */
