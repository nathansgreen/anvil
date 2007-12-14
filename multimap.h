/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MULTIMAP_H
#define __MULTIMAP_H

#include <stdint.h>
#include <sys/types.h>

#ifndef __cplusplus
#error multimap.h is a C++ header file
#endif

typedef enum {
	MM_NONE = 0,
	MM_U32 = 1,
	MM_U64 = 2,
	MM_STR = 3,
	/* blobs cannot be keys */
	MM_BLOB = 4
} mm_type_t;

/* NOTE: To use this union for strings, just cast the char * to a mm_val_t *. */
typedef union {
	uint32_t u32;
	uint64_t u64;
	const char str[0];
	struct {
		int blob_len;
		void * blob;
	};
} mm_val_t;

class multimap;

class multimap_it
{
public:
	/* fields where each entry is stored */
	mm_val_t * key;
	mm_val_t * val;
	
	/* get the next entry, or the first if the iterator is new */
	virtual int next() = 0;
	/* return the number of remaining entries */
	virtual size_t size() = 0;
	
	virtual ~multimap_it() = 0;
	
protected:
	multimap * map;
};

class multimap
{
public:
	/* query size */
	virtual size_t keys() = 0;
	virtual size_t values() = 0;
	
	virtual int remove_key(mm_val_t * key) = 0;
	virtual int reset_key(mm_val_t * key, mm_val_t * value) = 0;
	virtual int append_value(mm_val_t * key, mm_val_t * value) = 0;
	virtual int remove_value(mm_val_t * key, mm_val_t * value) = 0;
	virtual int update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value) = 0;
	
	virtual size_t count_values(mm_val_t * key) = 0;
	virtual multimap_it * get_range(mm_val_t * low_key, mm_val_t * high_key);
	
	virtual multimap_it * iterator() = 0;
	
	virtual ~multimap() = 0;
	
protected:
	inline uint32_t hash_str(const char * string);
};

inline uint32_t multimap::hash_str(const char * string)
{
	uint32_t hash = 0x5AFEDA7A;
	if(!string)
		return 0;
	while(*string)
	{
		/* ROL 3 */
		hash = (hash << 3) | (hash >> 29);
		hash ^= *(string++);
	}
	return hash;
}

#endif /* __MULTIMAP_H */
