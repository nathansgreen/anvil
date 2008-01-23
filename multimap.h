/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MULTIMAP_H
#define __MULTIMAP_H

#include <stdint.h>
#include <sys/types.h>

#include "blowfish.h"
#include "toilet.h"

#ifndef __cplusplus
#error multimap.h is a C++ header file
#endif

typedef enum {
	/* MM_NONE means "invalid" */
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
	char str[0];
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
	
	virtual ~multimap_it();
	
	multimap * it_map;
protected:
	mm_val_t s_key, s_val;
	void free_key();
	void free_value();
	inline multimap_it(multimap * map);
};

class multimap
{
public:
	/* query size */
	virtual size_t keys() = 0;
	virtual size_t values() = 0;
	
	/* iterate */
	virtual multimap_it * iterator() = 0;
	
	/* lookup values */
	virtual ssize_t count_values(mm_val_t * key) = 0;
	virtual multimap_it * get_values(mm_val_t * key) = 0;
	virtual ssize_t count_range(mm_val_t * low_key, mm_val_t * high_key) = 0;
	virtual multimap_it * get_range(mm_val_t * low_key, mm_val_t * high_key) = 0;
	
	/* modify */
	virtual int remove_key(mm_val_t * key) = 0;
	virtual int reset_key(mm_val_t * key, mm_val_t * value) = 0;
	virtual int append_value(mm_val_t * key, mm_val_t * value) = 0;
	/* TODO: it strikes me that for blobs, it may be more efficient to support removal by index,
	 * or to require that when val_type is MM_BLOB that there is only one value, or something */
	virtual int remove_value(mm_val_t * key, mm_val_t * value) = 0;
	virtual int update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value) = 0;
	
	virtual ~multimap();
	
	inline mm_type_t get_key_type();
	inline mm_type_t get_val_type();
	
	/* copy one multimap into another (possibly of a different type) */
	static int copy(multimap * source, multimap * dest);
	
	/* delete the multimap on disk (rm -rf) */
	static int drop(int dfd, const char * store, size_t * count = NULL);
	
protected:
	multimap(uint8_t * id);
	uint8_t * toilet_id;
	mm_type_t key_type;
	mm_type_t val_type;
	
	inline uint32_t hash_key(const mm_val_t * key);
	
private:
	static inline uint32_t hash_u32(uint32_t u32);
	static inline uint32_t hash_u64(uint64_t u64);
	static inline uint32_t hash_str(const char * string);
};

inline multimap_it::multimap_it(multimap * map)
	: key(NULL), val(NULL), it_map(map)
{
}

inline mm_type_t multimap::get_key_type()
{
	return key_type;
}

inline mm_type_t multimap::get_val_type()
{
	return val_type;
}

inline uint32_t multimap::hash_u32(uint32_t u32)
{
	return u32;
}

inline uint32_t multimap::hash_u64(uint64_t u64)
{
	return ((uint32_t) (u64 >> 32)) ^ (uint32_t) u64;
}

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

inline uint32_t multimap::hash_key(const mm_val_t * key)
{
	bf_ctx bfc;
	uint32_t hash;
	switch(key_type)
	{
		case MM_U32:
			hash = hash_u32(key->u32);
			break;
		case MM_U64:
			hash = hash_u64(key->u64);
			break;
		case MM_STR:
			hash = hash_str(key->str);
			break;
		default:
			/* invalid */
			return 0;
	}
	bf_setkey(&bfc, toilet_id, T_ID_SIZE);
	return bf32_encipher(&bfc, hash);
}

#endif /* __MULTIMAP_H */
