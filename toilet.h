#ifndef __TOILET_H
#define __TOILET_H

/* This is the main toilet header file. */

#include "vector.h"
#include "hash_map.h"

struct t_index {
	enum { NONE, HASH, TREE, BOTH } type;
	/* value -> set of rows */
	hash_map_t * hash;
	void * tree;
};
typedef struct t_index t_index;

enum t_type {
	T_ID,
	T_INT,
	T_STRING,
	T_BLOB
};
typedef enum t_type t_type;

struct t_column {
	const char * name;
	enum t_type type;
	int count;
	t_index * index;
};
typedef struct t_column t_column;

struct t_gtable {
	vector_t * columns;
	/* ...? */
	/* internal stuff now */
	int out_count;
};
typedef struct t_gtable t_gtable;

/* this should be uint64_t later, but we need better hash maps */
typedef uint32_t t_row_id;

union t_value {
	t_row_id v_id;
	long long v_int;
	const char * v_string;
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

struct toilet {
	/* all internal stuff */
	const char * path;
	int path_fd;
	/* cache of gtables currently out */
	hash_map_t * gtables;
	/* cache of rows currently out */
	hash_map_t * rows;
};
typedef struct toilet toilet;

struct t_query {
	/* XXX ... */
};
typedef struct t_query t_query;

struct t_rowset {
	/* XXX ... */
};
typedef struct t_rowset t_rowset;

/* databases */

toilet * toilet_open(const char * path);
int toilet_close(toilet * toilet);

/* gtables */

t_gtable * toilet_new_gtable(toilet * toilet, const char * name);
int toilet_drop_gtable(t_gtable * gtable);

t_gtable * toilet_get_gtable(toilet * toilet, const char * name);
int toilet_put_gtable(t_gtable * gtable);

#define COLUMNS(g) vector_size((g)->columns)
#define COLUMN(g, i) ((t_column *) vector_elt((g)->columns, (i)))

/* rows */

t_row * toilet_new_row(t_gtable * gtable);
int toilet_drop_row(t_row * row);

t_row * toilet_get_row(toilet * toilet, t_row_id id);
int toilet_put_row(t_row * row);

#define ID(r) ((r)->id)

/* values */

/* the values returned are valid so long as the row is not closed */
t_values * toilet_row_value(t_row * row, const char * key);

#define TYPE(v) ((v)->type)
#define VALUES(v) vector_size((v)->values)
#define VALUE(v, i) ((t_value *) vector_elt((v)->values, (i)))

/* remove all values */
int toilet_row_remove_values(t_row * row, const char * key);
/* replace all values with a new one (it will be copied) */
int toilet_row_replace_values(t_row * row, const char * key, t_value * value);

/* remove a single value */
int toilet_value_remove(t_values * values, int index);
/* append a new value (it will be copied) */
int toilet_value_append(t_values * values, t_value * value);
/* update a single value (it will be copied) */
int toilet_value_update(t_values * values, int index, t_value * value);

/* queries */

t_rowset * toilet_query(toilet * toilet, t_query * query);
t_rowset * toilet_query_gtable(t_gtable * gtable, t_query * query);
int toilet_put_rowset(t_rowset * rowset);

#define ROWS(r) ((r)->count)
#define ROW(r, i) ((t_row_id) vector_elt((r)->rows, i))

#endif /* __TOILET_H */
