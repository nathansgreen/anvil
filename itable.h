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
#include "datastore.h"
#include "stringset.h"

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
		union {
			struct overlay * ovr;
			void * atb_next; /* really a struct atable::node * */
		};
		
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
	virtual off_t _get(iv_int k1, iv_int k2, itable ** source) = 0;
	virtual off_t _get(iv_int k1, const char * k2, itable ** source) = 0;
	virtual off_t _get(const char * k1, iv_int k2, itable ** source) = 0;
	virtual off_t _get(const char * k1, const char * k2, itable ** source) = 0;
	
	inline off_t get(iv_int k1, iv_int k2, itable ** source = NULL);
	inline off_t get(iv_int k1, const char * k2, itable ** source = NULL);
	inline off_t get(const char * k1, iv_int k2, itable ** source = NULL);
	inline off_t get(const char * k1, const char * k2, itable ** source = NULL);
	
	/* iterate through the offsets: set up iterators */
	virtual int iter(struct it * it) = 0;
	virtual int iter(struct it * it, iv_int k1) = 0;
	virtual int iter(struct it * it, const char * k1) = 0;
	virtual void kill_iter(struct it * it);
	
	/* NOTE: These iterators are required to return the offsets in sorted order,
	 * first by primary key and then by secondary key. */
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int _next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source) = 0;
	virtual int _next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source) = 0;
	virtual int _next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source) = 0;
	virtual int _next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source) = 0;
	
	inline int next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source = NULL);
	inline int next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source = NULL);
	inline int next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source = NULL);
	inline int next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source = NULL);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int _next(struct it * it, iv_int * k1) = 0;
	virtual int _next(struct it * it, const char ** k1) = 0;
	
	inline int next(struct it * it, iv_int * k1);
	inline int next(struct it * it, const char ** k1);
	
	virtual datastore * get_datastore(iv_int k1, iv_int k2);
	virtual datastore * get_datastore(iv_int k1, const char * k2);
	virtual datastore * get_datastore(const char * k1, iv_int k2);
	virtual datastore * get_datastore(const char * k1, const char * k2);
	
	inline itable();
	inline virtual ~itable();
	
	/* number of bytes needed to store a value (1-4) */
	static inline uint8_t byte_size(uint32_t value);
	static inline void layout_bytes(uint8_t * array, int * index, uint32_t value, int size);
	
protected:
	ktype k1t, k2t;
};

#include "itable_datamap.h"

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
	virtual off_t _get(iv_int k1, iv_int k2, itable ** source);
	virtual off_t _get(iv_int k1, const char * k2, itable ** source);
	virtual off_t _get(const char * k1, iv_int k2, itable ** source);
	virtual off_t _get(const char * k1, const char * k2, itable ** source);
	
	/* iterate through the offsets: set up iterators */
	virtual int iter(struct it * it);
	virtual int iter(struct it * it, iv_int k1);
	virtual int iter(struct it * it, const char * k1);
	
	/* return 0 for success and < 0 for failure (-ENOENT when done) */
	virtual int _next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source);
	virtual int _next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source);
	
	/* iterate only through the primary keys (not mixable with above calls!) */
	virtual int _next(struct it * it, iv_int * k1);
	virtual int _next(struct it * it, const char ** k1);
	
	inline itable_disk();
	int init(int dfd, const char * file);
	void deinit();
	inline virtual ~itable_disk();
	
	/* not just copy, since the source can be composite */
	static int create(int dfd, const char * file, itable * source);
	static int create_with_datastore(int i_dfd, const char * i_file, itable * source, itable_datamap * map);
	
private:
	struct file_header {
		uint32_t magic;
		uint16_t version;
		uint8_t types[2];
	} __attribute__((packed));
	
	struct itable_header {
		uint32_t k1_count;
		uint32_t off_base;
		uint8_t key_sizes[2];
		uint8_t count_size;
		uint8_t off_sizes[2];
	} __attribute__((packed));
	
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
	
	/* helper for create() above */
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

inline off_t itable::get(iv_int k1, iv_int k2, itable ** source)
{
	return _get(k1, k2, source);
}

inline off_t itable::get(iv_int k1, const char * k2, itable ** source)
{
	return _get(k1, k2, source);
}

inline off_t itable::get(const char * k1, iv_int k2, itable ** source)
{
	return _get(k1, k2, source);
}

inline off_t itable::get(const char * k1, const char * k2, itable ** source)
{
	return _get(k1, k2, source);
}

inline int itable::next(struct it * it, iv_int * k1, iv_int * k2, off_t * off, itable ** source)
{
	return _next(it, k1, k2, off, source);
}

inline int itable::next(struct it * it, iv_int * k1, const char ** k2, off_t * off, itable ** source)
{
	return _next(it, k1, k2, off, source);
}

inline int itable::next(struct it * it, const char ** k1, iv_int * k2, off_t * off, itable ** source)
{
	return _next(it, k1, k2, off, source);
}

inline int itable::next(struct it * it, const char ** k1, const char ** k2, off_t * off, itable ** source)
{
	return _next(it, k1, k2, off, source);
}

inline int itable::next(struct it * it, iv_int * k1)
{
	return _next(it, k1);
}

inline int itable::next(struct it * it, const char ** k1)
{
	return _next(it, k1);
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
	if(fd >= 0)
		deinit();
}

#endif /* __ITABLE_H */
