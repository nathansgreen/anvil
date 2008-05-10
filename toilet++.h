/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOILETPP_H
#define __TOILETPP_H

/* This is the main C header file for toilet. */

#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

enum t_type
{
	T_ID = 0,
	T_INT = 1,
	T_FLOAT = 2,
	T_STRING = 3
};
typedef enum t_type t_type;

static inline const char * toiletpp_name_type(t_type type) __attribute__((always_inline));
static inline const char * toiletpp_name_type(t_type type)
{
	switch(type)
	{
		case T_ID:
			return "id";
		case T_INT:
			return "int";
		case T_FLOAT:
			return "float";
		case T_STRING:
			return "string";
	}
	return "(unknown)";
}

/* this should be uint64_t later */
typedef uint32_t t_row_id;
#define ROW_FORMAT "%08x"

struct t_gtable;
typedef struct t_gtable t_gtable;

struct t_row;
typedef struct t_row t_row;

struct t_toilet;
typedef struct t_toilet t_toilet;

struct t_rowset;
typedef struct t_rowset t_rowset;

/* NOTE: To use this union for strings, just cast the char * to a t_value *. */
union t_value
{
	t_row_id v_id;
	uint32_t v_int;
	double v_float;
	const char v_string[0];
	struct {
		int length;
		void * data;
	} v_blob;
};
typedef union t_value t_value;

struct t_query
{
	/* the content in here should allow some sort
	 * of convenient querying on a single gtable */
	/* XXX: for now, we just query for a single field having a specific value */
	const char * name;
	enum t_type type;
	const t_value * values[2];
};
typedef struct t_query t_query;

/* databases */

int toiletpp_new(const char * path);
/* there is no "toiletpp_drop()" because you can do that with rm -rf */

t_toilet * toiletpp_open(const char * path, FILE * errors);
int toiletpp_close(t_toilet * toilet);

size_t toiletpp_gtable_count(t_toilet * toilet);
const char * toiletpp_gtable_name(t_toilet * toilet, size_t index);

/* gtables */

int toiletpp_new_gtable(t_toilet * toilet, const char * name);
int toiletpp_drop_gtable(t_gtable * gtable);

t_gtable * toiletpp_get_gtable(t_toilet * toilet, const char * name);
void toiletpp_put_gtable(t_gtable * gtable);

/* columns */

size_t toiletpp_gtable_column_count(t_gtable * gtable);
t_type toiletpp_gtable_column_type(t_gtable * gtable, const char * name);
size_t toiletpp_gtable_column_rows(t_gtable * gtable, const char * name);

/* rows */

int toiletpp_new_row(t_gtable * gtable, t_row_id * new_id);
int toiletpp_drop_row(t_row * row);

t_row * toiletpp_get_row(t_gtable * gtable, t_row_id row_id);
void toiletpp_put_row(t_row * row);

t_row_id toiletpp_row_id(t_row * row);

/* values */

/* the values returned are valid until toiletpp_put_row() */
const t_value * toiletpp_row_value(t_row * row, const char * key, t_type type);
const t_value * toiletpp_row_value_type(t_row * row, const char * key, t_type * type);

/* set the value (it will be copied) */
int toiletpp_row_set_value(t_row * row, const char * key, t_type type, t_value * value);
/* remove the key and its value */
int toiletpp_row_remove_key(t_row * row, const char * key);

/* queries and rowsets */

t_rowset * toiletpp_query(t_gtable * gtable, t_query * query);
ssize_t toiletpp_count_query(t_gtable * gtable, t_query * query);

size_t toiletpp_rowset_size(t_rowset * rowset);
t_row_id toiletpp_rowset_row(t_rowset * rowset, size_t index);
void toiletpp_put_rowset(t_rowset * rowset);

#ifdef __cplusplus
}

#include <vector>
#include <map>
#include <set>

#include "stable.h"

#define GTABLE_NAME_LENGTH 63

struct t_gtable
{
	const char * name;
	stable * table;
	t_toilet * toilet;
	int out_count;
};

struct strcmp_less
{
	inline bool operator()(const char * a, const char * b) const
	{
		return strcmp(a, b) < 0;
	}
};

/* /me dislikes std::map immensely */
typedef std::map<const char *, t_value *, strcmp_less> value_map;

struct t_row
{
	t_row_id id;
	t_gtable * gtable;
	value_map values;
	int out_count;
};

#define T_ID_SIZE 16

struct t_toilet
{
	uint8_t id[T_ID_SIZE];
	t_row_id next_row;
	const char * path;
	int path_fd, row_fd;
	/* error stream */
	FILE * errors;
	/* all gtable names */
	std::vector<const char *> gtable_names;
	/* cache of gtables currently out */
	std::map<const char *, t_gtable *, strcmp_less> gtables;
	/* cache of rows currently out */
	std::map<t_row_id, t_row *> rows;
};

struct t_rowset
{
	std::vector<t_row_id> rows;
	std::set<t_row_id> ids;
	int out_count;
};

#endif

#endif /* __TOILETPP_H */
