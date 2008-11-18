/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOILETPP_H
#define __TOILETPP_H

/* This is the main C header file for toilet. */
/* It provides relatively direct access to most C++ toilet features through a C interface. */

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

struct tpp_istr
{
	/* sizeof(istr) */
	uint8_t _space[sizeof(void *)];
};
typedef struct tpp_istr tpp_istr;
struct tpp_blob
{
	/* sizeof(blob) */
	uint8_t _space[sizeof(void *)];
};
typedef struct tpp_blob tpp_blob;

struct tpp_mblob
{
	/* same as metablob */
	size_t _size;
	bool _exists;
};
typedef struct tpp_mblob tpp_mblob;

/* while we have stack instances of params in C++, we'll do heap
 * allocation here since params are not in the critical path */
struct tpp_params;
typedef struct tpp_params tpp_params;

typedef enum dtype_ctype tpp_dtype_type;
struct tpp_dtype
{
	/* sizeof(dtype) */
	/* use long instead of tpp_dtype_type for correct 64-bit alignment */
	uint8_t _space[sizeof(long) + sizeof(double) + 2 * sizeof(void *)];
};
typedef struct tpp_dtype tpp_dtype;

/* these are all pointer types in C++ already */
struct tpp_dtable;
typedef struct tpp_dtable tpp_dtable;
struct tpp_dtable_key_iter;
typedef struct tpp_dtable_key_iter tpp_dtable_key_iter;
struct tpp_dtable_iter;
typedef struct tpp_dtable_iter tpp_dtable_iter;
struct tpp_ctable;
typedef struct tpp_ctable tpp_ctable;
struct tpp_ctable_iter;
typedef struct tpp_ctable_iter tpp_ctable_iter;
struct tpp_stable;
typedef struct tpp_stable tpp_stable;
struct tpp_stable_col_iter;
typedef struct tpp_stable_col_iter tpp_stable_col_iter;
struct tpp_stable_iter;
typedef struct tpp_stable_iter tpp_stable_iter;

struct tpp_blobcmp;
typedef struct tpp_blobcmp tpp_blobcmp;

/* C blob comparator function */
typedef int (*blobcmp_func)(const void * b1, size_t s1, const void * b2, size_t s2, void * user);
typedef void (*blobcmp_free)(void * user);

/* use toilet runtime environment (journals, etc.) at this path */
int tpp_init(const char * path);

/* istr */
int tpp_istr_new(tpp_istr * c, const char * str);
int tpp_istr_copy(tpp_istr * c, const tpp_istr * src);
void tpp_istr_kill(tpp_istr * c);

const char * tpp_istr_str(const tpp_istr * c);
size_t tpp_istr_length(const tpp_istr * c);

/* blob */
int tpp_blob_new(tpp_blob * c, size_t size, const void * data);
int tpp_blob_str(tpp_blob * c, const char * str);
int tpp_blob_dne(tpp_blob * c);
int tpp_blob_empty(tpp_blob * c);
int tpp_blob_copy(tpp_blob * c, const tpp_blob * src);
void tpp_blob_kill(tpp_blob * c);

bool tpp_blob_exists(const tpp_blob * c);
size_t tpp_blob_size(const tpp_blob * c);
const void * tpp_blob_data(const tpp_blob * c);

int tpp_mblob_copy(tpp_mblob * c, const tpp_mblob * src);
void tpp_mblob_kill(tpp_mblob * c);

bool tpp_mblob_exists(const tpp_mblob * c);
size_t tpp_mblob_size(const tpp_mblob * c);

/* params */
tpp_params * tpp_params_new(void);
int tpp_params_parse(tpp_params * c, const char * string);
void tpp_params_kill(tpp_params * c);

/* dtype */
int tpp_dtype_int(tpp_dtype * c, uint32_t value);
int tpp_dtype_dbl(tpp_dtype * c, double value);
int tpp_dtype_str(tpp_dtype * c, const char * value);
int tpp_dtype_istr(tpp_dtype * c, const tpp_istr * value);
int tpp_dtype_blb(tpp_dtype * c, const tpp_blob * value);
int tpp_dtype_copy(tpp_dtype * c, const tpp_dtype * src);
void tpp_dtype_kill(tpp_dtype * c);

tpp_dtype_type tpp_dtype_get_type(const tpp_dtype * c);
int tpp_dtype_get_int(const tpp_dtype * c, uint32_t * value);
int tpp_dtype_get_dbl(const tpp_dtype * c, double * value);
int tpp_dtype_get_str(const tpp_dtype * c, tpp_istr * value);
int tpp_dtype_get_blb(const tpp_dtype * c, tpp_blob * value);

int tpp_dtype_compare(const tpp_dtype * a, const tpp_dtype * b);
int tpp_dtype_compare_blobcmp(const tpp_dtype * a, const tpp_dtype * b, const tpp_blobcmp * cmp);

const char * tpp_dtype_type_name(tpp_dtype_type type);

/* dtable */
int tpp_dtable_create(const char * type, int dfd, const char * name, const tpp_params * config, const tpp_dtable * source, const tpp_dtable * shadow);
int tpp_dtable_create_empty(const char * type, int dfd, const char * name, const tpp_params * config, tpp_dtype_type key_type);
tpp_dtable * tpp_dtable_open(const char * type, int dfd, const char * name, const tpp_params * config);
void tpp_dtable_kill(tpp_dtable * c);

int tpp_dtable_find(const tpp_dtable * c, const tpp_dtype * key, tpp_blob * value);
bool tpp_dtable_writable(const tpp_dtable * c);
int tpp_dtable_insert(tpp_dtable * c, const tpp_dtype * key, const tpp_blob * value, bool append);
int tpp_dtable_remove(tpp_dtable * c, const tpp_dtype * key);
tpp_dtype_type tpp_dtable_key_type(const tpp_dtable * c);
int tpp_dtable_set_blob_cmp(tpp_dtable * c, const tpp_blobcmp * cmp);
const char * tpp_dtable_get_cmp_name(const tpp_dtable * c);
int tpp_dtable_maintain(tpp_dtable * c);

tpp_dtable_key_iter * tpp_dtable_keys(const tpp_dtable * c);
bool tpp_dtable_key_iter_valid(const tpp_dtable_key_iter * c);
bool tpp_dtable_key_iter_next(tpp_dtable_key_iter * c);
bool tpp_dtable_key_iter_prev(tpp_dtable_key_iter * c);
bool tpp_dtable_key_iter_first(tpp_dtable_key_iter * c);
bool tpp_dtable_key_iter_last(tpp_dtable_key_iter * c);
int tpp_dtable_key_iter_key(const tpp_dtable_key_iter * c, tpp_dtype * key);
bool tpp_dtable_key_iter_seek(tpp_dtable_key_iter * c, const tpp_dtype * key);
/* bool tpp_dtable_key_iter_seek_test(tpp_dtable_key_iter * c, const dtype_test * test); */
void tpp_dtable_key_iter_kill(tpp_dtable_key_iter * c);

tpp_dtable_iter * tpp_dtable_iterator(const tpp_dtable * c);
bool tpp_dtable_iter_valid(const tpp_dtable_iter * c);
bool tpp_dtable_iter_next(tpp_dtable_iter * c);
bool tpp_dtable_iter_prev(tpp_dtable_iter * c);
bool tpp_dtable_iter_first(tpp_dtable_iter * c);
bool tpp_dtable_iter_last(tpp_dtable_iter * c);
int tpp_dtable_iter_key(const tpp_dtable_iter * c, tpp_dtype * key);
bool tpp_dtable_iter_seek(tpp_dtable_iter * c, const tpp_dtype * key);
/* bool tpp_dtable_iter_seek_test(tpp_dtable_iter * c, const dtype_test * test); */
void tpp_dtable_iter_meta(const tpp_dtable_iter * c, tpp_mblob * meta);
int tpp_dtable_iter_value(const tpp_dtable_iter * c, tpp_blob * value);
void tpp_dtable_iter_kill(tpp_dtable_iter * c);

/* ctable */
tpp_ctable * tpp_ctable_open(const char * type, tpp_dtable * dt_source, const tpp_params * config);
tpp_ctable * tpp_ctable_open_const(const char * type, const tpp_dtable * dt_source, const tpp_params * config);
void tpp_ctable_kill(tpp_ctable * c);

int tpp_ctable_find(const tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column, tpp_blob * value);
bool tpp_ctable_writable(const tpp_ctable * c);
int tpp_ctable_insert(tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column, const tpp_blob * value, bool append);
int tpp_ctable_remove(tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column);
int tpp_ctable_remove_row(tpp_ctable * c, const tpp_dtype * key);
tpp_dtype_type tpp_ctable_key_type(const tpp_ctable * c);
int tpp_ctable_set_blob_cmp(tpp_ctable * c, const tpp_blobcmp * cmp);
const char * tpp_ctable_get_cmp_name(const tpp_ctable * c);
int tpp_ctable_maintain(tpp_ctable * c);

tpp_dtable_key_iter * tpp_ctable_keys(const tpp_ctable * c);
/* all other functions same as above for tpp_dtable_key_iter */

tpp_ctable_iter * tpp_ctable_iterator(const tpp_ctable * c);
bool tpp_ctable_iter_valid(const tpp_ctable_iter * c);
bool tpp_ctable_iter_next(tpp_ctable_iter * c);
bool tpp_ctable_iter_prev(tpp_ctable_iter * c);
bool tpp_ctable_iter_first(tpp_ctable_iter * c);
bool tpp_ctable_iter_last(tpp_ctable_iter * c);
int tpp_ctable_iter_key(const tpp_ctable_iter * c, tpp_dtype * key);
bool tpp_ctable_iter_seek(tpp_ctable_iter * c, const tpp_dtype * key);
/* bool tpp_ctable_iter_seek_test(tpp_ctable_iter * c, const dtype_test * test); */
const char * tpp_ctable_iter_column(const tpp_ctable_iter * c);
int tpp_ctable_iter_value(const tpp_ctable_iter * c, tpp_blob * value);
void tpp_ctable_iter_kill(tpp_ctable_iter * c);

/* blobcmp */
tpp_blobcmp * tpp_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill, bool free_user);
tpp_blobcmp * tpp_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill);
const char * tpp_blobcmp_name(const tpp_blobcmp * blobcmp);
void tpp_blobcmp_retain(tpp_blobcmp * blobcmp);
void tpp_blobcmp_release(tpp_blobcmp ** blobcmp);

#ifdef __cplusplus
}

#include "blob.h"
#include "istr.h"
#include "params.h"
#include "dtable.h"
#include "ctable.h"
#include "stable.h"

#define static_assert(x) do { switch(0) { case 0: case (x): ; } } while(0)

/* static_assert() must be used in a function, so declare one that is never
 * called but returns its own address to avoid unused function warnings */
static void * __tpp_static_asserts(void)
{
	static_assert(sizeof(tpp_istr) == sizeof(istr));
	static_assert(sizeof(tpp_blob) == sizeof(blob));
	static_assert(sizeof(tpp_dtype) == sizeof(dtype));
	return (void *) __tpp_static_asserts;
}

template<class C, class CPP>
union tpp_union_cast
{
	C * c;
	CPP * cpp;
	inline tpp_union_cast(C * c) : c(c) {}
	inline tpp_union_cast(CPP * cpp) : cpp(cpp) {}
	inline operator C *() const { return c; }
	inline operator CPP *() const { return cpp; }
	/* treat it more like the C++ variety pointer */
	inline CPP * operator->() const { return cpp; }
	inline CPP & operator*() { return *cpp; }
};

#define TPP_UNION_CAST(C, CPP) typedef tpp_union_cast<C, CPP> C##_union; typedef tpp_union_cast<const C, const CPP> C##_union_const

TPP_UNION_CAST(tpp_istr, istr);
TPP_UNION_CAST(tpp_blob, blob);
TPP_UNION_CAST(tpp_params, params);
TPP_UNION_CAST(tpp_dtype, dtype);
TPP_UNION_CAST(tpp_dtable, dtable);
TPP_UNION_CAST(tpp_dtable_key_iter, dtable::key_iter);
TPP_UNION_CAST(tpp_dtable_iter, dtable::iter);
TPP_UNION_CAST(tpp_ctable, ctable);
TPP_UNION_CAST(tpp_ctable_iter, ctable::iter);
TPP_UNION_CAST(tpp_stable, stable);
TPP_UNION_CAST(tpp_stable_col_iter, stable::column_iter);
TPP_UNION_CAST(tpp_stable_iter, stable::iter);

struct tpp_blobcmp : public blob_comparator
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
	
	inline tpp_blobcmp(const istr & name) : blob_comparator(name) {}
	inline virtual ~tpp_blobcmp()
	{
		if(kill)
			kill(user);
		if(copied)
			free(user);
	}
};

#endif

#endif /* __TOILETPP_H */
