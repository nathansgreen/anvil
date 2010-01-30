/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ANVIL_H
#define __ANVIL_H

/* This is the main C header file for Anvil. */
/* It provides relatively direct access to most C++ Anvil features through a C interface. */

#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

#include "dtype.h"

#ifdef __cplusplus
extern "C" {
#else
#include <stdbool.h>
#endif

struct anvil_istr
{
	/* sizeof(istr) */
	uint8_t _space[sizeof(void *)];
};
typedef struct anvil_istr anvil_istr;
struct anvil_blob
{
	/* sizeof(blob) */
	uint8_t _space[sizeof(void *)];
};
typedef struct anvil_blob anvil_blob;

struct anvil_metablob
{
	/* same as metablob */
	size_t _size;
	bool _exists;
};
typedef struct anvil_metablob anvil_metablob;

struct anvil_blob_buffer
{
	/* sizeof(blob_buffer) */
	uint8_t _space[sizeof(size_t) + sizeof(void *)];
};
typedef struct anvil_blob_buffer anvil_blob_buffer;

/* while we have stack instances of params in C++, we'll do heap
 * allocation here since params are not in the critical path */
struct anvil_params;
typedef struct anvil_params anvil_params;

typedef enum dtype_ctype anvil_dtype_type;
struct anvil_dtype
{
	/* sizeof(dtype) */
	/* use long instead of anvil_dtype_type for correct 64-bit alignment */
	uint8_t _space[sizeof(long) + sizeof(double) + 2 * sizeof(void *)];
};
typedef struct anvil_dtype anvil_dtype;

/* these are all pointer types in C++ already */
struct anvil_dtable;
typedef struct anvil_dtable anvil_dtable;
struct anvil_dtable_key_iter;
typedef struct anvil_dtable_key_iter anvil_dtable_key_iter;
struct anvil_dtable_iter;
typedef struct anvil_dtable_iter anvil_dtable_iter;
struct anvil_ctable;
typedef struct anvil_ctable anvil_ctable;
struct anvil_ctable_iter;
typedef struct anvil_ctable_iter anvil_ctable_iter;
struct anvil_stable;
typedef struct anvil_stable anvil_stable;
struct anvil_stable_col_iter;
typedef struct anvil_stable_col_iter anvil_stable_col_iter;
struct anvil_stable_iter;
typedef struct anvil_stable_iter anvil_stable_iter;

struct anvil_blobcmp;
typedef struct anvil_blobcmp anvil_blobcmp;

/* C blob comparator function */
typedef int (*blobcmp_func)(const void * b1, size_t s1, const void * b2, size_t s2, void * user);
typedef void (*blobcmp_free)(void * user);

/* for the *_seek_test() functions, the magic blob key test */
typedef int (*blob_test)(const void *, size_t, void *);

/* use Anvil runtime environment (journals, etc.) at this path */
int anvil_init(const char * path);

/* istr */
int anvil_istr_new(anvil_istr * c, const char * str);
int anvil_istr_copy(anvil_istr * c, const anvil_istr * src);
void anvil_istr_kill(anvil_istr * c);

const char * anvil_istr_str(const anvil_istr * c);
size_t anvil_istr_length(const anvil_istr * c);

/* blob */
int anvil_blob_new(anvil_blob * c, size_t size, const void * data);
int anvil_blob_str(anvil_blob * c, const char * str);
int anvil_blob_dne(anvil_blob * c);
int anvil_blob_empty(anvil_blob * c);
int anvil_blob_copy(anvil_blob * c, const anvil_blob * src);
int anvil_blob_copy_buffer(anvil_blob * c, const anvil_blob_buffer * src);
void anvil_blob_kill(anvil_blob * c);

bool anvil_blob_exists(const anvil_blob * c);
size_t anvil_blob_size(const anvil_blob * c);
const void * anvil_blob_data(const anvil_blob * c);

/* these work using blob buffers; for more than one overwrite/append, you should do that manually */
int anvil_blob_overwrite(anvil_blob * c, size_t offset, const void * data, size_t length);
int anvil_blob_append(anvil_blob * c, const void * data, size_t length);

/* metablob */
int anvil_metablob_copy(anvil_metablob * c, const anvil_metablob * src);
void anvil_metablob_kill(anvil_metablob * c);

bool anvil_metablob_exists(const anvil_metablob * c);
size_t anvil_metablob_size(const anvil_metablob * c);

/* blob_buffer */
int anvil_blob_buffer_new(anvil_blob_buffer * c);
int anvil_blob_buffer_new_capacity(anvil_blob_buffer * c, size_t capacity);
int anvil_blob_buffer_new_data(anvil_blob_buffer * c, size_t size, const void * data);
int anvil_blob_buffer_copy(anvil_blob_buffer * c, const anvil_blob_buffer * src);
int anvil_blob_buffer_copy_blob(anvil_blob_buffer * c, const anvil_blob * src);
void anvil_blob_buffer_kill(anvil_blob_buffer * c);

bool anvil_blob_buffer_exists(const anvil_blob_buffer * c);
size_t anvil_blob_buffer_size(const anvil_blob_buffer * c);
size_t anvil_blob_buffer_capacity(const anvil_blob_buffer * c);
const void * anvil_blob_buffer_data(const anvil_blob_buffer * c);

int anvil_blob_buffer_set_size(anvil_blob_buffer * c, size_t size);
int anvil_blob_buffer_set_capacity(anvil_blob_buffer * c, size_t capacity);
int anvil_blob_buffer_overwrite(anvil_blob_buffer * c, size_t offset, const void * data, size_t length);
int anvil_blob_buffer_append(anvil_blob_buffer * c, const void * data, size_t length);

/* params */
anvil_params * anvil_params_new(void);
int anvil_params_parse(anvil_params * c, const char * string);
void anvil_params_kill(anvil_params * c);

/* dtype */
int anvil_dtype_int(anvil_dtype * c, uint32_t value);
int anvil_dtype_dbl(anvil_dtype * c, double value);
int anvil_dtype_str(anvil_dtype * c, const char * value);
int anvil_dtype_istr(anvil_dtype * c, const anvil_istr * value);
int anvil_dtype_blb(anvil_dtype * c, const anvil_blob * value);
int anvil_dtype_copy(anvil_dtype * c, const anvil_dtype * src);
void anvil_dtype_kill(anvil_dtype * c);

anvil_dtype_type anvil_dtype_get_type(const anvil_dtype * c);
int anvil_dtype_get_int(const anvil_dtype * c, uint32_t * value);
int anvil_dtype_get_dbl(const anvil_dtype * c, double * value);
int anvil_dtype_get_str(const anvil_dtype * c, anvil_istr * value);
int anvil_dtype_get_blb(const anvil_dtype * c, anvil_blob * value);

int anvil_dtype_compare(const anvil_dtype * a, const anvil_dtype * b);
int anvil_dtype_compare_blobcmp(const anvil_dtype * a, const anvil_dtype * b, const anvil_blobcmp * cmp);

const char * anvil_dtype_type_name(anvil_dtype_type type);

/* dtable */
int anvil_dtable_create(const char * type, int dfd, const char * name, const anvil_params * config, const anvil_dtable * source, const anvil_dtable * shadow);
int anvil_dtable_create_empty(const char * type, int dfd, const char * name, const anvil_params * config, anvil_dtype_type key_type);
anvil_dtable * anvil_dtable_open(const char * type, int dfd, const char * name, const anvil_params * config);
void anvil_dtable_kill(anvil_dtable * c);

bool anvil_dtable_contains(const anvil_dtable * c, const anvil_dtype * key);
bool anvil_dtable_contains_tx(const anvil_dtable * c, const anvil_dtype * key, abortable_tx atx);
int anvil_dtable_find(const anvil_dtable * c, const anvil_dtype * key, anvil_blob * value);
int anvil_dtable_find_tx(const anvil_dtable * c, const anvil_dtype * key, anvil_blob * value, abortable_tx atx);
bool anvil_dtable_writable(const anvil_dtable * c);
int anvil_dtable_insert(anvil_dtable * c, const anvil_dtype * key, const anvil_blob * value, bool append);
int anvil_dtable_insert_tx(anvil_dtable * c, const anvil_dtype * key, const anvil_blob * value, bool append, abortable_tx atx);
int anvil_dtable_remove(anvil_dtable * c, const anvil_dtype * key);
int anvil_dtable_remove_tx(anvil_dtable * c, const anvil_dtype * key, abortable_tx atx);
anvil_dtype_type anvil_dtable_key_type(const anvil_dtable * c);
int anvil_dtable_set_blob_cmp(anvil_dtable * c, const anvil_blobcmp * cmp);
const char * anvil_dtable_get_cmp_name(const anvil_dtable * c);
int anvil_dtable_maintain(anvil_dtable * c);

abortable_tx anvil_dtable_create_atx(anvil_dtable * c);
int anvil_dtable_check_atx(const anvil_dtable * c, abortable_tx atx);
int anvil_dtable_commit_atx(anvil_dtable * c, abortable_tx atx);
void anvil_dtable_abort_atx(anvil_dtable * c, abortable_tx atx);

anvil_dtable_key_iter * anvil_dtable_keys(const anvil_dtable * c);
anvil_dtable_key_iter * anvil_dtable_keys_tx(const anvil_dtable * c, abortable_tx atx);
bool anvil_dtable_key_iter_valid(const anvil_dtable_key_iter * c);
bool anvil_dtable_key_iter_next(anvil_dtable_key_iter * c);
bool anvil_dtable_key_iter_prev(anvil_dtable_key_iter * c);
bool anvil_dtable_key_iter_first(anvil_dtable_key_iter * c);
bool anvil_dtable_key_iter_last(anvil_dtable_key_iter * c);
int anvil_dtable_key_iter_key(const anvil_dtable_key_iter * c, anvil_dtype * key);
bool anvil_dtable_key_iter_seek(anvil_dtable_key_iter * c, const anvil_dtype * key);
bool anvil_dtable_key_iter_seek_test(anvil_dtable_key_iter * c, blob_test test, void * user);
void anvil_dtable_key_iter_kill(anvil_dtable_key_iter * c);

anvil_dtable_iter * anvil_dtable_iterator(const anvil_dtable * c);
anvil_dtable_iter * anvil_dtable_iterator_tx(const anvil_dtable * c, abortable_tx atx);
bool anvil_dtable_iter_valid(const anvil_dtable_iter * c);
bool anvil_dtable_iter_next(anvil_dtable_iter * c);
bool anvil_dtable_iter_prev(anvil_dtable_iter * c);
bool anvil_dtable_iter_first(anvil_dtable_iter * c);
bool anvil_dtable_iter_last(anvil_dtable_iter * c);
int anvil_dtable_iter_key(const anvil_dtable_iter * c, anvil_dtype * key);
bool anvil_dtable_iter_seek(anvil_dtable_iter * c, const anvil_dtype * key);
bool anvil_dtable_iter_seek_test(anvil_dtable_iter * c, blob_test test, void * user);
void anvil_dtable_iter_meta(const anvil_dtable_iter * c, anvil_metablob * meta);
int anvil_dtable_iter_value(const anvil_dtable_iter * c, anvil_blob * value);
void anvil_dtable_iter_kill(anvil_dtable_iter * c);

/* ctable */
int anvil_ctable_create(const char * type, int dfd, const char * name, const anvil_params * config, anvil_dtype_type key_type);
anvil_ctable * anvil_ctable_open(const char * type, int dfd, const char * name, const anvil_params * config);
void anvil_ctable_kill(anvil_ctable * c);

int anvil_ctable_find(const anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column, anvil_blob * value);
bool anvil_ctable_writable(const anvil_ctable * c);
int anvil_ctable_insert(anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column, const anvil_blob * value, bool append);
int anvil_ctable_remove(anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column);
int anvil_ctable_remove_row(anvil_ctable * c, const anvil_dtype * key);
anvil_dtype_type anvil_ctable_key_type(const anvil_ctable * c);
int anvil_ctable_set_blob_cmp(anvil_ctable * c, const anvil_blobcmp * cmp);
const char * anvil_ctable_get_cmp_name(const anvil_ctable * c);
int anvil_ctable_maintain(anvil_ctable * c);

anvil_dtable_key_iter * anvil_ctable_keys(const anvil_ctable * c);
/* all other functions same as above for anvil_dtable_key_iter */

anvil_ctable_iter * anvil_ctable_iterator(const anvil_ctable * c);
bool anvil_ctable_iter_valid(const anvil_ctable_iter * c);
bool anvil_ctable_iter_next(anvil_ctable_iter * c);
bool anvil_ctable_iter_prev(anvil_ctable_iter * c);
bool anvil_ctable_iter_first(anvil_ctable_iter * c);
bool anvil_ctable_iter_last(anvil_ctable_iter * c);
int anvil_ctable_iter_key(const anvil_ctable_iter * c, anvil_dtype * key);
bool anvil_ctable_iter_seek(anvil_ctable_iter * c, const anvil_dtype * key);
bool anvil_ctable_iter_seek_test(anvil_ctable_iter * c, blob_test test, void * user);
size_t anvil_ctable_iter_column(const anvil_ctable_iter * c);
const char * anvil_ctable_iter_name(const anvil_ctable_iter * c);
int anvil_ctable_iter_value(const anvil_ctable_iter * c, anvil_blob * value);
void anvil_ctable_iter_kill(anvil_ctable_iter * c);

/* blobcmp */
anvil_blobcmp * anvil_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill, bool free_user);
anvil_blobcmp * anvil_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill);
const char * anvil_blobcmp_name(const anvil_blobcmp * blobcmp);
void anvil_blobcmp_retain(anvil_blobcmp * blobcmp);
void anvil_blobcmp_release(anvil_blobcmp ** blobcmp);

/* dtables should only be opened once, but it is often convenient to be able to
 * act as though they can be opened many times - so we provide a layer of
 * indirection on opening and closing them, to simulate multi-opened dtables */
struct anvil_dtable_cache;
typedef struct anvil_dtable_cache anvil_dtable_cache;

anvil_dtable_cache * anvil_dtable_cache_new(int dir_fd, const char * type);
void anvil_dtable_cache_kill(anvil_dtable_cache * c);

int anvil_dtable_cache_create(anvil_dtable_cache * c, int index, const anvil_params * config, const anvil_dtable * source, const anvil_dtable * shadow);
int anvil_dtable_cache_create_empty(anvil_dtable_cache * c, int index, const anvil_params * config, anvil_dtype_type key_type);

anvil_dtable * anvil_dtable_cache_open(anvil_dtable_cache * c, int index, const anvil_params * config);
void anvil_dtable_cache_close(anvil_dtable_cache * c, anvil_dtable * dtable);
int anvil_dtable_cache_maintain(anvil_dtable_cache * c, int index);
bool anvil_dtable_cache_can_maintain(anvil_dtable_cache * c, int index);

/* the index must itself be open already */
anvil_dtable_iter * anvil_dtable_cache_iter(anvil_dtable_cache * c, int index);
void anvil_dtable_cache_close_iter(anvil_dtable_cache * c, anvil_dtable_iter * iter);

#ifdef __cplusplus
}

#include <ext/hash_map>

#include "params.h"
#include "dtable.h"
#include "ctable.h"
#include "stable.h"
#include "blob_buffer.h"

/* static_assert() must be used in a function, so declare one that is never
 * called but returns its own address to avoid unused function warnings */
static void * __anvil_static_asserts(void)
{
	static_assert(sizeof(anvil_istr) == sizeof(istr));
	static_assert(sizeof(anvil_blob) == sizeof(blob));
	static_assert(sizeof(anvil_dtype) == sizeof(dtype));
	static_assert(sizeof(anvil_blob_buffer) == sizeof(blob_buffer));
	return (void *) __anvil_static_asserts;
}

template<class C, class CPP>
union anvil_union_cast
{
	C * c;
	CPP * cpp;
	inline anvil_union_cast(C * c) : c(c) {}
	inline anvil_union_cast(CPP * cpp) : cpp(cpp) {}
	inline operator C *() const { return c; }
	inline operator CPP *() const { return cpp; }
	/* treat it more like the C++ variety pointer */
	inline CPP * operator->() const { return cpp; }
	inline CPP & operator*() { return *cpp; }
};

#define ANVIL_UNION_CAST(C, CPP) typedef anvil_union_cast<C, CPP> C##_union; typedef anvil_union_cast<const C, const CPP> C##_union_const

ANVIL_UNION_CAST(anvil_istr, istr);
ANVIL_UNION_CAST(anvil_blob, blob);
ANVIL_UNION_CAST(anvil_blob_buffer, blob_buffer);
ANVIL_UNION_CAST(anvil_params, params);
ANVIL_UNION_CAST(anvil_dtype, dtype);
ANVIL_UNION_CAST(anvil_dtable, dtable);
ANVIL_UNION_CAST(anvil_dtable_key_iter, dtable::key_iter);
ANVIL_UNION_CAST(anvil_dtable_iter, dtable::iter);
ANVIL_UNION_CAST(anvil_ctable, ctable);
ANVIL_UNION_CAST(anvil_ctable_iter, ctable::iter);
ANVIL_UNION_CAST(anvil_stable, stable);
ANVIL_UNION_CAST(anvil_stable_col_iter, stable::column_iter);
ANVIL_UNION_CAST(anvil_stable_iter, stable::iter);

struct anvil_blobcmp : public blob_comparator
{
	blobcmp_func cmp;
	blobcmp_free kill;
	bool copied;
	void * user;
	
	virtual int compare(const blob & a, const blob & b) const
	{
		return cmp(&a[0], a.size(), &b[0], b.size(), user);
	}
	
	inline virtual size_t hash(const blob & blob) const
	{
		/* do we need something better here? */
		return blob_comparator::hash(blob);
	}
	
	inline anvil_blobcmp(const istr & name) : blob_comparator(name) {}
	inline virtual ~anvil_blobcmp()
	{
		if(kill)
			kill(user);
		if(copied)
			free(user);
	}
};

class anvil_dtype_test : public dtype_test
{
public:
	virtual int operator()(const dtype & key) const
	{
		assert(key.type == dtype::BLOB);
		return test(&key.blb[0], key.blb.size(), user);
	}
	
	anvil_dtype_test(blob_test test, void * user) : test(test), user(user) {}
private:
	blob_test test;
	void * user;
};

struct anvil_dtable_cache
{
	int dir_fd;
	const char * type;
	uint32_t ticks;
	
#define OPEN_DTABLE_ITERS 8
#define RECENT_OPEN_DTABLES 32
#define DELTA_TICKS 256
	struct open_dtable
	{
		anvil_dtable * dtable;
		anvil_dtable_iter * iters[OPEN_DTABLE_ITERS];
		int count;
		uint32_t last_tick;
		
		inline open_dtable()
			: dtable(NULL), count(1), last_tick(0)
		{
			for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
				iters[i] = NULL;
		}
		inline void init(anvil_dtable * dtable)
		{
			assert(!this->dtable);
			this->dtable = dtable;
		}
		inline ~open_dtable()
		{
			for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
				assert(!iters[i]);
		}
	};
	struct pointer_hash
	{
		template<class T>
		inline size_t operator()(T * pointer) const
		{
			return dtype_hash_helper<void *>()(pointer);
		}
	};
	typedef __gnu_cxx::hash_map<int, open_dtable> idx_map;
	typedef __gnu_cxx::hash_map<anvil_dtable *, int, pointer_hash> dt_map;
	typedef __gnu_cxx::hash_map<anvil_dtable_iter *, int, pointer_hash> it_map;
	
	idx_map index_map;
	dt_map dtable_map;
	it_map iter_map;
	anvil_dtable * recent[RECENT_OPEN_DTABLES];
	
	inline anvil_dtable_cache(int dir_fd, const char * type)
		: dir_fd(dir_fd), type(type), ticks(0)
	{
		for(int i = 0; i < RECENT_OPEN_DTABLES; i++)
			recent[i] = NULL;
	}
	~anvil_dtable_cache();
	
	anvil_dtable * open(int index, const anvil_params * config);
	void close(anvil_dtable * dtable);
	bool can_maintain(int index);
	int maintain(int index);
	
	anvil_dtable_iter * iterator(int index);
	void close(anvil_dtable_iter * dt_iter);
};

#endif

#endif /* __ANVIL_H */
