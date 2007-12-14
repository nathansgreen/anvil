/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOILET_H
#define __TOILET_H

/* This is the main toilet header file. */

#include "vector.h"
#include "hash_map.h"
#include "hash_set.h"
#include "diskhash.h"

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
	t_index * index;
};
typedef struct t_column t_column;

#define GTABLE_NAME_LENGTH 63

struct t_gtable {
	const char * name;
	vector_t * columns;
	hash_map_t * column_map;
	/* internal stuff now */
	int out_count;
};
typedef struct t_gtable t_gtable;

/* this should be uint64_t later, but we need better hash maps */
typedef uint32_t t_row_id;

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

#define ID_SIZE 16

struct toilet {
	uint8_t id[ID_SIZE];
	t_row_id next_row;
	/* internal stuff now */
	const char * path;
	int path_fd;
	int row_fd;
	/* cache of gtables currently out */
	hash_map_t * gtables;
	/* cache of rows currently out */
	hash_map_t * rows;
};
typedef struct toilet toilet;

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
	t_value value;
};
typedef struct t_query t_query;

/* databases */

int toilet_new(const char * path);
/* there is no "toilet_drop()" because you can do that with rm -rf */

toilet * toilet_open(const char * path);
int toilet_close(toilet * toilet);

/* gtables */

int toilet_new_gtable(toilet * toilet, const char * name);
int toilet_drop_gtable(t_gtable * gtable);

t_gtable * toilet_get_gtable(toilet * toilet, const char * name);
void toilet_put_gtable(toilet * toilet, t_gtable * gtable);

#define COLUMNS(g) vector_size((g)->columns)
#define COLUMN(g, i) ((t_column *) vector_elt((g)->columns, (i)))

/* rows */

int toilet_new_row(toilet * toilet, t_gtable * gtable, t_row_id * new_id);
int toilet_drop_row(t_row * row);

t_row * toilet_get_row(toilet * toilet, t_row_id row_id);
void toilet_put_row(toilet * toilet, t_row * row);

#define ID(r) ((r)->id)

/* values */

/* the values returned are valid so long as the row is not closed */
t_values * toilet_row_value(t_row * row, const char * key);

#define TYPE(v) ((v)->type)
#define VALUES(v) vector_size((v)->values)
#define VALUE(v, i) ((t_value *) vector_elt((v)->values, (i)))

/* NOTE: none of the following functions may be applied to the automatic "id" column */

/* remove all values */
int toilet_row_remove_values(t_row * row, const char * key);
/* append a new value (it will be copied) */
int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value);
/* replace all values with a new one (it will be copied) */
int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value);

/* remove a single value */
int toilet_value_remove(t_values * values, int index);
/* append a new value (it will be copied) */
int toilet_value_append(t_values * values, t_type type, t_value * value);
/* update a single value (it will be copied) */
int toilet_value_update(t_values * values, int index, t_value * value);

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query);
int toilet_put_rowset(t_rowset * rowset);

#define ROWS(r) ((r)->count)
#define ROW(r, i) ((t_row_id) vector_elt((r)->rows, i))

#ifdef __cplusplus
}
#endif

#endif /* __TOILET_H */
