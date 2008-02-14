/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef PHP_TOILET_H
#define PHP_TOILET_H 1

#include <toilet.h>

#define PHP_TOILET_VERSION "0.1"
#define PHP_TOILET_EXTNAME "toilet"

#define PHP_TOILET_RES_NAME "toilet database"

#define PHP_GTABLE_RES_NAME "toilet gtable"

#define PHP_COLUMN_RES_NAME "toilet column header"

typedef struct php_rowid {
	t_row_id rowid;
	t_toilet * toilet;
} php_rowid;
#define PHP_ROWID_RES_NAME "toilet row ID"

PHP_MINIT_FUNCTION(toilet);
PHP_MSHUTDOWN_FUNCTION(toilet);

/* takes a string, returns a toilet */
PHP_FUNCTION(toilet_open);
/* takes a toilet, returns a boolean */
PHP_FUNCTION(toilet_close);
/* takes a toilet, returns an array of strings */
PHP_FUNCTION(toilet_gtables);
/* takes a toilet and a string, returns a gtable */
PHP_FUNCTION(toilet_gtable);
/* takes a toilet and a string, returns a boolean */
PHP_FUNCTION(toilet_new_gtable);

/* takes a gtable, returns a string */
PHP_FUNCTION(gtable_name);
/* takes a gtable, returns a boolean */
PHP_FUNCTION(gtable_close);
/* takes a gtable, returns an associative array of string => type (as string) */
PHP_FUNCTION(gtable_columns);
/* takes a gtable and a string, returns a column */
PHP_FUNCTION(gtable_column);
/* takes a gtable, a string, and up to two values, returns an array of ids */
PHP_FUNCTION(gtable_query);
/* takes a gtable, a string, and up to two values, returns an int */
PHP_FUNCTION(gtable_count_query);
/* takes a gtable, returns an array of ids */
PHP_FUNCTION(gtable_rows);
/* takes a gtable, returns a rowid */
PHP_FUNCTION(gtable_new_row);

/* takes a column, returns a string */
PHP_FUNCTION(column_name);
/* takes a column, returns a type (as string) */
PHP_FUNCTION(column_type);
/* takes a column, returns a long */
PHP_FUNCTION(column_count);
/* takes a column, returns a boolean */
PHP_FUNCTION(column_is_multi);

/* takes two rowids, returns a boolean */
PHP_FUNCTION(rowid_equal);
/* takes a rowid and an optional array of strings, returns an associative array of values */
PHP_FUNCTION(rowid_get_row);
/* takes a rowid, returns a string */
PHP_FUNCTION(rowid_format);
/* takes a toilet and a string, returns a rowid */
PHP_FUNCTION(rowid_parse);
/* takes a rowid, an associative array of values, and optionally an associative array of type names, returns a boolean */
PHP_FUNCTION(rowid_set_values);
/* takes a rowid, returns a boolean */
PHP_FUNCTION(rowid_drop);

extern zend_module_entry toilet_module_entry;
#define phpext_toilet_ptr &toilet_module_entry

#endif
