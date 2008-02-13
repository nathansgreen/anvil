/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOILET_H
#define __TOILET_H

/* This is the main toilet header file. */

#include <stdio.h>
#include <stdint.h>

#include "vector.h"
#include "hash_map.h"
#include "hash_set.h"

#ifdef __cplusplus
extern "C" {
#endif

/* struct t_index is declared in index.h */
struct t_index;
typedef struct t_index t_index;

enum t_type {
	T_ID = 0,
	T_INT = 1,
	T_STRING = 2,
	T_BLOB = 3
};
typedef enum t_type t_type;

struct t_column {
	const char * name;
	enum t_type type;
	unsigned int count;
	unsigned int flags;
	t_index * index;
};
typedef struct t_column t_column;

/* column flags */
#define T_COLUMN_MULTI 0x1

#define GTABLE_NAME_LENGTH 63

struct t_toilet;

struct t_gtable {
	const char * name;
	vector_t * columns;
	hash_map_t * column_map;
	/* internal stuff now */
	struct t_toilet * toilet;
	int out_count;
};
typedef struct t_gtable t_gtable;

/* this should be uint64_t later, but we need better hash maps */
typedef uint32_t t_row_id;
#define ROW_FORMAT "%08x"

/* NOTE: To use this union for strings, just cast the char * to a t_value *. */
union t_value {
	t_row_id v_id;
	long long v_int;
	const char v_string[0];
	struct {
		int length;
		void * data;
	} v_blob;
};
typedef union t_value t_value;

struct t_values {
	enum t_type type;
	vector_t * values;
};
typedef struct t_values t_values;

struct t_row {
	t_row_id id;
	t_gtable * gtable;
	/* key -> values */
	hash_map_t * columns;
	/* internal stuff now */
	int row_path_fd;
	int out_count;
};
typedef struct t_row t_row;

#define T_ID_SIZE 16

struct t_toilet {
	uint8_t id[T_ID_SIZE];
	t_row_id next_row;
	/* internal stuff now */
	const char * path;
	int path_fd;
	int row_fd;
	/* error stream */
	FILE * errors;
	/* all gtable names */
	vector_t * gtable_names;
	/* cache of gtables currently out */
	hash_map_t * gtables;
	/* cache of rows currently out */
	hash_map_t * rows;
};
typedef struct t_toilet t_toilet;

struct t_rowset {
	vector_t * rows;
	hash_set_t * ids;
	/* internal stuff now */
	int out_count;
};
typedef struct t_rowset t_rowset;

struct t_query {
	/* the content in here should allow some sort
	 * of convenient querying on a single gtable */
	/* XXX: for now, we just query for a single field having a specific value */
	const char * name;
	enum t_type type;
	t_value * values[2];
};
typedef struct t_query t_query;

static inline const char * toilet_name_type(t_type type) __attribute__((always_inline));
static inline const char * toilet_name_type(t_type type)
{
	switch(type)
	{
		case T_ID:
			return "id";
		case T_INT:
			return "int";
		case T_STRING:
			return "string";
		case T_BLOB:
			return "blob";
	}
	return "(unknown)";
}

/* databases */

int toilet_new(const char * path);
/* there is no "toilet_drop()" because you can do that with rm -rf */

t_toilet * toilet_open(const char * path, FILE * errors);
int toilet_close(t_toilet * toilet);

/* gtables */

int toilet_new_gtable(t_toilet * toilet, const char * name);
int toilet_drop_gtable(t_gtable * gtable);

t_gtable * toilet_get_gtable(t_toilet * toilet, const char * name);
void toilet_put_gtable(t_gtable * gtable);

#define GTABLES(t) vector_size((t)->gtable_names)
#define GTABLE_NAME(t, i) ((char *) vector_elt((t)->gtable_names, i))
/* this also works for columns */
#define NAME(gc) ((gc)->name)

/* columns */

#define COLUMNS(g) vector_size((g)->columns)
#define COLUMN(g, i) ((t_column *) vector_elt((g)->columns, (i)))
#define COUNT(c) ((c)->count)
/* this also works for values */
#define TYPE(cv) ((cv)->type)

t_column * toilet_gtable_get_column(t_gtable * gtable, const char * name);
int toilet_column_is_multi(t_column * column);

int toilet_column_set_multi(t_column * column, int multi);

/* rows */

int toilet_new_row(t_gtable * gtable, t_row_id * new_id);
int toilet_drop_row(t_row * row);

t_row * toilet_get_row(t_toilet * toilet, t_row_id row_id);
void toilet_put_row(t_row * row);

#define ID(r) ((r)->id)

/* values */

/* the values returned are valid so long as the row is not closed */
t_value * toilet_row_value(t_row * row, const char * key, t_type type); /* non-multi columns */
t_values * toilet_row_values(t_row * row, const char * key); /* multi columns */

/* NOTE: none of the following functions may be applied to the automatic "id" column */

/* set the value (it will be copied) */
int toilet_row_set_value(t_row * row, const char * key, t_type type, t_value * value); /* non-multi columns */
/* remove the key and its value */
int toilet_row_remove_key(t_row * row, const char * key); /* non-multi columns */

/* append a new value (it will be copied) */
int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value); /* multi columns */
/* replace all values with a new one (it will be copied) [same as set_value, but for multi columns] */
int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value); /* multi columns */
/* remove all values [same as remove_key, but for multi columns] */
int toilet_row_remove_values(t_row * row, const char * key); /* multi columns */

#define VALUES(vs) vector_size((vs)->values)
#define VALUE(vs, i) ((t_value *) vector_elt((vs)->values, (i)))

/* remove a single value */
int toilet_row_remove_value(t_row * row, t_values * values, int index); /* multi columns */
/* update a single value (it will be copied) */
int toilet_row_update_value(t_row * row, t_values * values, int index, t_value * value); /* multi columns */

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query);
ssize_t toilet_count_query(t_gtable * gtable, t_query * query);
void toilet_put_rowset(t_rowset * rowset);

#define ROWS(r) (vector_size((r)->rows))
#define ROW(r, i) ((t_row_id) vector_elt((r)->rows, i))

#ifdef __cplusplus
}
#endif

#endif /* __TOILET_H */
