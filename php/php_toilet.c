/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_toilet.h"

static int le_toilet;
static int le_gtable;
static int le_rowid;

static function_entry toilet_functions[] = {
	PHP_FE(toilet_open, NULL)
	PHP_FE(toilet_close, NULL)
	PHP_FE(toilet_gtables, NULL)
	PHP_FE(toilet_gtable, NULL)
	PHP_FE(gtable_name, NULL)
	PHP_FE(gtable_close, NULL)
	PHP_FE(gtable_rows, NULL)
	PHP_FE(rowid_value, NULL)
	{ NULL, NULL, NULL}
};

zend_module_entry toilet_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	PHP_TOILET_EXTNAME,
	toilet_functions,
	PHP_MINIT(toilet),
	PHP_MSHUTDOWN(toilet),
	NULL,
	NULL,
	NULL,
#if ZEND_MODULE_API_NO >= 20010901
	PHP_TOILET_VERSION,
#endif
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_TOILET
ZEND_GET_MODULE(toilet)
#endif

static void php_toilet_dtor(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	t_toilet * toilet = (t_toilet *) rsrc->ptr;
	toilet_close(toilet);
}

static void php_gtable_dtor(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	t_gtable * gtable = (t_gtable *) rsrc->ptr;
	toilet_put_gtable(gtable);
}

static void php_rowid_dtor(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	php_rowid * rowid = (php_rowid *) rsrc->ptr;
	efree(rowid);
}

PHP_MINIT_FUNCTION(toilet)
{
	le_toilet = zend_register_list_destructors_ex(php_toilet_dtor, NULL, PHP_TOILET_RES_NAME, module_number);
	le_gtable = zend_register_list_destructors_ex(php_gtable_dtor, NULL, PHP_GTABLE_RES_NAME, module_number);
	le_rowid = zend_register_list_destructors_ex(php_rowid_dtor, NULL, PHP_ROWID_RES_NAME, module_number);
	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(toilet)
{
	return SUCCESS;
}

/* takes a string, returns a toilet */
PHP_FUNCTION(toilet_open)
{
	t_toilet * toilet;
	char * path = NULL;
	int path_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &path, &path_len) == FAILURE)
		RETURN_NULL();
	toilet = toilet_open(path, NULL);
	if(!toilet)
		RETURN_NULL();
	ZEND_REGISTER_RESOURCE(return_value, toilet, le_toilet);
}

/* takes a toilet, returns a boolean */
PHP_FUNCTION(toilet_close)
{
	t_toilet * toilet;
	zval * ztoilet;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &ztoilet) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	zend_list_delete(Z_LVAL_P(ztoilet));
	RETURN_TRUE;
}

/* takes a toilet, returns an array of strings */
PHP_FUNCTION(toilet_gtables)
{
	int i;
	t_toilet * toilet;
	zval * ztoilet;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &ztoilet) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	array_init(return_value);
	for(i = 0; i < GTABLES(toilet); i++)
		add_next_index_string(return_value, GTABLE_NAME(toilet, i), 1);
}

/* takes a toilet and a string, returns a gtable */
PHP_FUNCTION(toilet_gtable)
{
	t_gtable * gtable;
	t_toilet * toilet;
	zval * ztoilet;
	char * name = NULL;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &ztoilet, &name, &name_len) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	gtable = toilet_get_gtable(toilet, name);
	if(!gtable)
		RETURN_NULL();
	ZEND_REGISTER_RESOURCE(return_value, gtable, le_gtable);
}

/* takes a toilet, returns a string */
PHP_FUNCTION(gtable_name)
{
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	RETURN_STRING(NAME(gtable), 1);
}

/* takes a gtable, returns a boolean */
PHP_FUNCTION(gtable_close)
{
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	zend_list_delete(Z_LVAL_P(zgtable));
	RETURN_TRUE;
}

/* takes a gtable, returns an array of ids */
PHP_FUNCTION(gtable_rows)
{
	t_query query = {.name = NULL};
	t_rowset * rows;
	t_gtable * gtable;
	zval * zgtable;
	int i;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	rows = toilet_query(gtable, &query);
	if(!rows)
		RETURN_NULL();
	array_init(return_value);
	for(i = 0; i < ROWS(rows); i++)
	{
		zval * zrowid;
		php_rowid * rowid = emalloc(sizeof(*rowid));
		rowid->rowid = ROW(rows, i);
		ALLOC_INIT_ZVAL(zrowid);
		ZEND_REGISTER_RESOURCE(zrowid, rowid, le_rowid);
		add_next_index_zval(return_value, zrowid);
	}
	toilet_put_rowset(rows);
}

/* takes a rowid, returns a long */
PHP_FUNCTION(rowid_value)
{
	php_rowid * rowid;
	zval * zrowid;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zrowid) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(rowid, php_rowid *, &zrowid, -1, PHP_ROWID_RES_NAME, le_rowid);
	RETURN_LONG(rowid->rowid);
}
