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
	/* this maybe should be part of itable, and used in other itable subclasses? */
	union key {
		iv_int i;
		const char * s;
		/* convert iv_int or const char * to key automatically */
		inline key() { }
		inline key(iv_int x) : i(x) { }
		inline key(const char * x) : s(x) { }
	};
	
	struct node {
		key k1, k2;
		off_t value;
		node * up;
		node * left;
		node * right;
	};
	
	bool has_node(key k1);
	node * find_node(key k1, key k2);
	/* will reuse an existing node if possible */
	int add_node(key k1, key k2, off_t off);
	static void next_node(node ** n);
	static inline int cmp_keys(ktype type, key a, key b);
	inline int cmp_node(node * n, key k1, key k2);
	static void kill_nodes(node * n);
	
	int add_string(const char * string, uint32_t * index);
	int log(iv_int k1, iv_int k2, off_t off);
	int playback();
	
	tx_fd fd;
	off_t offset;
	stringset strings;
	node * root;
};

inline atable::atable()
	: fd(-1), offset(-1), root(NULL)
{
}

inline atable::~atable()
{
	if(fd >= 0)
		deinit();
}

#endif /* __ATABLE_H */
