/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ITABLE_H
#define __ITABLE_H

#include <limits.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <sys/types.h>

#include "stable.h"
#include "hash_map.h"

#ifndef __cplusplus
#error itable.h is a C++ header file
#endif

/* itables are immutable on-disk maps from two keys (the primary key and the
 * secondary key) to an offset. The keys can be either integers or strings; the
 * offsets are of type off_t but may be stored more compactly. The itable's data
 * is sorted on disk first by the primary key and then by the secondary key. */

/* for itable_disk::create() iv_int should be unsigned */
typedef unsigned int iv_int;
#define IV_INT_MIN 0
#define IV_INT_MAX UINT_MAX

#define ITABLE_MAGIC 0x017AB1E0
#define ITABLE_VERSION 0x0000

#define INVAL_OFF_T ((off_t) -1)

/* This is the abstract base class of several different kinds of itables. Aside
 * from the obvious implementation that reads a single file storing all the
 * data, there is also a layering itable that allows one itable to overlay
 * another, or for atables (append-only tables) to overlay itables. */

class itable
{
public:
	enum ktype { NONE, INT, STRING };
	struct it {
		/* these fields are sufficient for itable_disk */
		size_t k1i, k2i;
		size_t k2_count;
		off_t k2_offset;
		bool single_k1, only_k1;
		iv_int k1;
		
		/* additional fields for other itable types */
		struct overlay; /* only defined in itable_overlay.h */
		struct overlay * ovr;
		
		/* automatic destructor to call itable::kill_iter(): set "table" to use */
		inline it();
		inline ~it();
		itable * table;
		/* always call clear() before using a struct it in itable::iter() */
		inline void clear();
	};
	
	/* get the types of the keys */
	virtual inline ktype k1_type();
	virtual inline ktype k2_type();
	
	/* test whether there is an entry for the given key */
	virtual bool has(iv_int k1) = 0;
	virtual bool has(const char * k1) = 0;
	
	virtual bool has(iv_int k1, iv_int k2) = 0;
	virtual bool has(iv_int k1, const char * k2) = 0;
	virtual bool has(const char * k1, iv_int k2) = 0;
	virtual bool has(const char * k1, const char * k2) = 0;
	
	/* get the offset for the given key */
	virtual off_t get(iv_int k1, iv_int k2) = 0;
	virtual off_t get(iv_int k1, const char * k2) = 0;
	virtual off_t get(const char * k1, iv_int k2) = 0;
	virtual off_t get(const char * k1, const char * k2) = 0;
	
	/* iterate through the offsets: set up iterators */
	virtual int iter(struct it * it) = 0;
	virtual int iter(struct it * it, iv_int k1) = 0;
	virtual int iter(struct it * it, const char * k1) = 0;
	virtual void kill_iter(struct it * it);
	
	/* NOTE: These iterators are required to return the offsets in sorted order,
	 * first by primary key and then by secondary key. */
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int next(struct it * it, iv_int * k1, iv_int * k2, off_t * off) = 0;
	virtual int next(struct it * it, iv_int * k1, const char ** k2, off_t * off) = 0;
	virtual int next(struct it * it, const char ** k1, iv_int * k2, off_t * off) = 0;
	virtual int next(struct it * it, const char ** k1, const char ** k2, off_t * off) = 0;
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int next(struct it * it, iv_int * k1) = 0;
	virtual int next(struct it * it, const char ** k1) = 0;
	
	inline itable();
	inline virtual ~itable();
	
	/* number of bytes needed to store a value (1-4) */
	static inline uint8_t byte_size(uint32_t value);
	static inline void layout_bytes(uint8_t * array, int * index, uint32_t value, int size);
	
protected:
	ktype k1t, k2t;
};

class itable_disk : public itable
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
	
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int next(struct it * it, iv_int * k1, iv_int * k2, off_t * off);
	virtual int next(struct it * it, iv_int * k1, const char ** k2, off_t * off);
	virtual int next(struct it * it, const char ** k1, iv_int * k2, off_t * off);
	virtual int next(struct it * it, const char ** k1, const char ** k2, off_t * off);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int next(struct it * it, iv_int * k1);
	virtual int next(struct it * it, const char ** k1);
	
	inline itable_disk();
	int init(int dfd, const char * file);
	void deinit();
	inline virtual ~itable_disk();
	
	/* not just copy, since the source can be composite */
	static int create(int dfd, const char * file, itable * source);
	
private:
	int fd;
	off_t k1_offset;
	uint8_t key_sizes[2];
	struct stable st;
	size_t k1_count;
	off_t off_base;
	uint8_t count_size, off_sizes[2];
	uint8_t entry_sizes[2];
	
	int k1_get(size_t index, iv_int * value, size_t * k2_count, off_t * k2_offset);
	int k1_find(iv_int k1, size_t * k2_count, off_t * k2_offset, size_t * index = NULL);
	
	int k2_get(size_t k2_count, off_t k2_offset, size_t index, iv_int * value, off_t * offset);
	int k2_find(size_t k2_count, off_t k2_offset, iv_int k2, off_t * offset, size_t * index = NULL);
	
	/* helpers for create() above */
	static int add_string(const char ** string, hash_map_t * string_map, size_t * max_strlen);
	static ssize_t locate_string(const char ** array, ssize_t size, const char * string);
};

/* itable inlines */

inline itable::ktype itable::k1_type()
{
	return k1t;
}

inline itable::ktype itable::k2_type()
{
	return k2t;
}

inline itable::itable()
	: k1t(NONE), k2t(NONE)
{
}

inline itable::~itable()
{
}

inline itable::it::it()
	: table(NULL)
{
}

inline itable::it::~it()
{
	clear();
}

inline void itable::it::clear()
{
	if(table)
	{
		table->kill_iter(this);
		assert(!table);
	}
}

inline uint8_t itable::byte_size(uint32_t value)
{
	if(value < 0x100)
		return 1;
	if(value < 0x10000)
		return 2;
	if(value < 0x1000000)
		return 3;
	return 4;
}

inline void itable::layout_bytes(uint8_t * array, int * index, uint32_t value, int size)
{
	uint8_t i = *index;
	*index += size;
	/* write big endian order */
	while(size-- > 0)
	{
		array[i + size] = value & 0xFF;
		value >>= 8;
	}
}

/* itable_disk inlines */

inline itable_disk::itable_disk()
	: fd(-1)
{
}

inline itable_disk::~itable_disk()
{
	deinit();
}

#endif /* __ITABLE_H */
