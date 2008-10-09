/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOILETPP_H
#define __TOILETPP_H

/* This is the main C header file for toilet. */

#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#else
#include <stdbool.h>
#endif

enum t_type
{
	T_INT = 1,
	T_FLOAT = 2,
	T_STRING = 3,
	T_BLOB = 4
};
typedef enum t_type t_type;

static inline const char * toilet_name_type(t_type type) __attribute__((always_inline));
static inline const char * toilet_name_type(t_type type)
{
	switch(type)
	{
		case T_INT:
			return "int";
		case T_FLOAT:
			return "float";
		case T_STRING:
			return "string";
		case T_BLOB:
			return "blob";
	}
	return "(unknown)";
}

/* this should be uint64_t later */
typedef uint32_t t_row_id;
#define ROW_FORMAT "%u"

struct t_gtable;
typedef struct t_gtable t_gtable;

struct t_columns;
typedef struct t_columns t_columns;

struct t_row;
typedef struct t_row t_row;

struct t_toilet;
typedef struct t_toilet t_toilet;

struct t_cursor;
typedef struct t_cursor t_cursor;

struct t_rowset;
typedef struct t_rowset t_rowset;

struct t_blobcmp;
typedef struct t_blobcmp t_blobcmp;

/* C blob comparator function */
typedef int (*blobcmp_func)(const void * b1, size_t s1, const void * b2, size_t s2, void * user);
typedef void (*blobcmp_free)(void * user);

/* NOTE: To use this union for strings, just cast the char * to a t_value *. */
union t_value
{
	uint32_t v_int;
	double v_float;
	const char v_string[0];
	struct {
		size_t length;
		void * data;
	} v_blob;
};
typedef union t_value t_value;

struct t_simple_query
{
	/* the content in here should allow some sort
	 * of convenient querying on a single gtable */
	/* XXX: for now, we just query for a single field having a specific value */
	const char * name;
	enum t_type type;
	const t_value * values[2];
};
typedef struct t_simple_query t_simple_query;

/* use toilet runtime environment (journals, etc.) at this path */
int toilet_init(const char * path);

/* databases */

int toilet_new(const char * path);
/* there is no "toilet_drop()" because you can do that with rm -rf */

t_toilet * toilet_open(const char * path, FILE * errors);
int toilet_close(t_toilet * toilet);

size_t toilet_gtables_count(t_toilet * toilet);
const char * toilet_gtables_name(t_toilet * toilet, size_t index);

/* gtables */

int toilet_new_gtable(t_toilet * toilet, const char * name);
int toilet_new_gtable_blobkey(t_toilet * toilet, const char * name);
int toilet_drop_gtable(t_gtable * gtable);

t_gtable * toilet_get_gtable(t_toilet * toilet, const char * name);
const char * toilet_gtable_name(t_gtable * gtable);
bool toilet_gtable_blobkey(t_gtable * gtable);
const char * toilet_gtable_blobcmp_name(t_gtable * gtable);
/* on success, will release the blobcmp when it is done with it */
int toilet_gtable_set_blobcmp(t_gtable * gtable, t_blobcmp * blobcmp);
int toilet_gtable_maintain(t_gtable * gtable);
void toilet_put_gtable(t_gtable * gtable);

/* columns */

t_columns * toilet_gtable_columns(t_gtable * gtable);
bool toilet_columns_valid(t_columns * columns);
const char * toilet_columns_name(t_columns * columns);
t_type toilet_columns_type(t_columns * columns);
size_t toilet_columns_row_count(t_columns * columns);
void toilet_columns_next(t_columns * columns);
void toilet_put_columns(t_columns * columns);

t_type toilet_gtable_column_type(t_gtable * gtable, const char * name);
size_t toilet_gtable_column_row_count(t_gtable * gtable, const char * name);

/* rows */

int toilet_new_row(t_gtable * gtable, t_row_id * new_id);
int toilet_drop_row(t_row * row);

t_row * toilet_get_row(t_gtable * gtable, t_row_id row_id);
t_row * toilet_get_row_blobkey(t_gtable * gtable, const void * key, size_t key_size);
void toilet_put_row(t_row * row);

t_row_id toilet_row_id(t_row * row);
const void * toilet_row_blobkey(t_row * row, size_t * key_size);
t_gtable * toilet_row_gtable(t_row * row);

/* values */

/* the values returned are valid until the row is modified or closed */
const t_value * toilet_row_value(t_row * row, const char * key, t_type type);
const t_value * toilet_row_value_type(t_row * row, const char * key, t_type * type);

/* set the value (it will be copied) */
int toilet_row_set_value(t_row * row, const char * key, t_type type, const t_value * value);
/* remove the key and its value */
int toilet_row_remove_key(t_row * row, const char * key);

/* cursors */

t_cursor * toilet_gtable_cursor(t_gtable * gtable);
int toilet_cursor_valid(t_cursor * cursor);
/* these return true if they found an exact match, false otherwise */
bool toilet_cursor_seek(t_cursor * cursor, t_row_id id);
bool toilet_cursor_seek_blobkey(t_cursor * cursor, const void * key, size_t key_size);
bool toilet_cursor_seek_magic(t_cursor * cursor, int (*magic)(const void *, size_t, void *), void * user);
int toilet_cursor_next(t_cursor * cursor);
int toilet_cursor_prev(t_cursor * cursor);
int toilet_cursor_last(t_cursor * cursor);
t_row_id toilet_cursor_row_id(t_cursor * cursor);
const void * toilet_cursor_row_blobkey(t_cursor * cursor, size_t * key_size);
void toilet_close_cursor(t_cursor * cursor);

/* queries and rowsets */
/* these calls don't work with blobkey gtables yet */

t_rowset * toilet_simple_query(t_gtable * gtable, t_simple_query * query);
ssize_t toilet_count_simple_query(t_gtable * gtable, t_simple_query * query);

size_t toilet_rowset_size(t_rowset * rowset);
t_row_id toilet_rowset_row(t_rowset * rowset, size_t index);
bool toilet_rowset_contains(t_rowset * rowset, t_row_id id);
void toilet_put_rowset(t_rowset * rowset);

/* blob comparators */

t_blobcmp * toilet_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill);
t_blobcmp * toilet_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill);
const char * toilet_blobcmp_name(const t_blobcmp * blobcmp);
void toilet_blobcmp_retain(t_blobcmp * blobcmp);
void toilet_blobcmp_release(t_blobcmp ** blobcmp);

#ifdef __cplusplus
}

#include <vector>
#include <map>
#include <set>

#include "transaction.h"

#include "istr.h"
#include "stable.h"

#define GTABLE_NAME_LENGTH 63

struct t_gtable
{
	istr name;
	stable * table;
	t_toilet * toilet;
	t_blobcmp * blobcmp;
	int out_count;
	inline t_gtable() : blobcmp(NULL), out_count(1) {}
};

/* t_columns is really just stable::column_iter */
union t_columns_union
{
	t_columns * columns;
	stable::column_iter * iter;
};

/* /me dislikes std::map immensely */
typedef std::map<istr, t_value *, strcmp_less> value_map;

struct t_row
{
	t_row_id id;
	blob blobkey;
	t_gtable * gtable;
	value_map values;
	int out_count;
	
	inline t_row(t_row_id row_id) : id(row_id) {}
	inline t_row(const void * key, size_t key_size) : id(0), blobkey(key_size, key) {}
	
	inline ~t_row()
	{
		value_map::iterator it = values.begin();
		for(; it != values.end(); ++it)
			free(it->second);
	}
};

#define T_ID_SIZE 16

struct t_toilet
{
	uint8_t id[T_ID_SIZE];
	t_row_id next_row;
	istr path;
	tx_fd row_fd;
	int path_fd;
	/* error stream */
	FILE * errors;
	/* all gtable names */
	std::vector<istr> gtable_names;
	/* cache of gtables currently out */
	std::map<istr, t_gtable *, strcmp_less> gtables;
	/* cache of rows currently out */
	std::map<t_row_id, t_row *> rows;
	int out_count;
	inline t_toilet() : out_count(1) {}
};

/* t_cursor is really just dtable::key_iter */
union t_cursor_union
{
	t_cursor * cursor;
	dtable::key_iter * iter;
};

struct t_rowset
{
	std::vector<t_row_id> rows;
	std::set<t_row_id> ids;
	int out_count;
	inline t_rowset() : out_count(1) {}
};

struct t_blobcmp : public blob_comparator
{
	blobcmp_func cmp;
	blobcmp_free kill;
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
	
	inline t_blobcmp(const istr & name) : blob_comparator(name) {}
	inline virtual ~t_blobcmp()
	{
		if(kill)
			kill(user);
	}
};

#endif

#endif /* __TOILETPP_H */
