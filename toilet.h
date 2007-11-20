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
	t_gtable * gtable;
	/* key -> values */
	hash_map_t * columns;
};
typedef struct t_row t_row;

struct toilet {
	/* ... */
};
typedef struct toilet toilet;

toilet * toilet_open(const char * path);
int toilet_close(toilet * toilet);

/* TODO: figure out how to handle memory management for t_* structure pointers */

t_row * toilet_row(toilet * toilet, t_row_id id);

t_values * row_value(t_row * row, const char * name);

#endif /* __TOILET_H */
