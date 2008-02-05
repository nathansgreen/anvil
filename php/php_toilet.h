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

PHP_FUNCTION(toilet_open);
PHP_FUNCTION(toilet_close);
PHP_FUNCTION(toilet_gtables);
PHP_FUNCTION(toilet_gtable);

PHP_FUNCTION(gtable_name);
PHP_FUNCTION(gtable_close);
PHP_FUNCTION(gtable_columns);
PHP_FUNCTION(gtable_column);
PHP_FUNCTION(gtable_rows);

PHP_FUNCTION(column_name);
PHP_FUNCTION(column_type);
PHP_FUNCTION(column_count);
PHP_FUNCTION(column_is_multi);

PHP_FUNCTION(rowid_get_row);
PHP_FUNCTION(rowid_value);

extern zend_module_entry toilet_module_entry;
#define phpext_toilet_ptr &toilet_module_entry

#endif
