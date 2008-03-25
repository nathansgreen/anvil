/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ATABLE_H
#define __ATABLE_H

#include <sys/types.h>

#include "transaction.h"
#include "stringset.h"
#include "itable.h"

#ifndef __cplusplus
#error atable.h is a C++ header file
#endif

#define ATABLE_MAGIC 0x0A7AB1E0
#define ATABLE_VERSION 0x0000

class atable : public itable
{
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
	
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int next(struct it * it, iv_int * k1, iv_int * k2, off_t * off);
	virtual int next(struct it * it, iv_int * k1, const char ** k2, off_t * off);
	virtual int next(struct it * it, const char ** k1, iv_int * k2, off_t * off);
	virtual int next(struct it * it, const char ** k1, const char ** k2, off_t * off);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int next(struct it * it, iv_int * k1);
	virtual int next(struct it * it, const char ** k1);
	
	/* append records */
	int append(iv_int k1, iv_int k2, off_t off);
	int append(iv_int k1, const char * k2, off_t off);
	int append(const char * k1, iv_int k2, off_t off);
	int append(const char * k1, const char * k2, off_t off);
	
	inline atable();
	int init(int dfd, const char * file, ktype k1, ktype k2);
	void deinit();
	inline virtual ~atable();
	
private:
	int add_string(const char * string, uint32_t * index);
	int playback();
	
	tx_fd fd;
	off_t offset;
	stringset strings;
};

inline atable::atable()
	: fd(-1), offset(-1)
{
}

inline atable::~atable()
{
	if(fd >= 0)
		deinit();
}

#endif /* __ATABLE_H */
