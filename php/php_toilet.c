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
	PHP_FE(toilet_init, NULL)
	PHP_FE(toilet_open, NULL)
	PHP_FE(toilet_close, NULL)
	PHP_FE(toilet_gtables, NULL)
	PHP_FE(toilet_gtable, NULL)
	PHP_FE(toilet_new_gtable, NULL)
	PHP_FE(gtable_name, NULL)
	PHP_FE(gtable_close, NULL)
	PHP_FE(gtable_columns, NULL)
	PHP_FE(gtable_column_type, NULL)
	PHP_FE(gtable_column_row_count, NULL)
	PHP_FE(gtable_query, NULL)
	PHP_FE(gtable_count_query, NULL)
	PHP_FE(gtable_rows, NULL)
	PHP_FE(gtable_new_row, NULL)
	PHP_FE(gtable_maintain, NULL)
	PHP_FE(rowid_equal, NULL)
	PHP_FE(rowid_get_row, NULL)
	PHP_FE(rowid_format, NULL)
	PHP_FE(rowid_parse, NULL)
	PHP_FE(rowid_set_values, NULL)
	PHP_FE(rowid_drop, NULL)
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

/* takes a string, returns a long */
PHP_FUNCTION(toilet_init)
{
	char * path = NULL;
	int path_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &path, &path_len) == FAILURE)
		RETURN_NULL();
	RETURN_LONG(toilet_init(path));
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
	int i, max;
	t_toilet * toilet;
	zval * ztoilet;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &ztoilet) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	array_init(return_value);
	max = toilet_gtables_count(toilet);
	for(i = 0; i < max; i++)
		add_next_index_string(return_value, (char *) toilet_gtables_name(toilet, i), 1);
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

/* takes a toilet and a string, returns a boolean */
PHP_FUNCTION(toilet_new_gtable)
{
	t_toilet * toilet;
	zval * ztoilet;
	char * name = NULL;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &ztoilet, &name, &name_len) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	if(toilet_new_gtable(toilet, name) < 0)
		RETURN_FALSE;
	RETURN_TRUE;
}

/* takes a gtable, returns a string */
PHP_FUNCTION(gtable_name)
{
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	RETURN_STRING((char *) toilet_gtable_name(gtable), 1);
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

/* takes a gtable, returns an associative array of string => type (as string) */
PHP_FUNCTION(gtable_columns)
{
	t_columns * columns;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	array_init(return_value);
	columns = toilet_gtable_columns(gtable);
	while(toilet_columns_valid(columns))
	{
		const char * name = toilet_columns_name(columns);
		const char * type = toilet_name_type(toilet_columns_type(columns));
		add_assoc_string(return_value, (char *) name, (char *) type, 1);
	}
}

/* takes a gtable and a string, returns a type (as string) */
PHP_FUNCTION(gtable_column_type)
{
	t_gtable * gtable;
	zval * zgtable;
	char * name = NULL;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &zgtable, &name, &name_len) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	if(!toilet_gtable_column_row_count(gtable, name))
		RETURN_NULL();
	RETURN_STRING((char *) toilet_name_type(toilet_gtable_column_type(gtable, name)), 1);
}

/* takes a gtable and a string, returns a long */
PHP_FUNCTION(gtable_column_row_count)
{
	t_gtable * gtable;
	zval * zgtable;
	char * name = NULL;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &zgtable, &name, &name_len) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	RETURN_LONG(toilet_gtable_column_row_count(gtable, name));
}

static void rowset_to_array(t_gtable * gtable, t_rowset * rowset, zval * array)
{
	size_t i, max = toilet_rowset_size(rowset);
	for(i = 0; i < max; i++)
	{
		zval * zrowid;
		php_rowid * rowid = emalloc(sizeof(*rowid));
		rowid->rowid = toilet_rowset_row(rowset, i);
		rowid->gtable = gtable;
		ALLOC_INIT_ZVAL(zrowid);
		ZEND_REGISTER_RESOURCE(zrowid, rowid, le_rowid);
		add_next_index_zval(array, zrowid);
	}
	toilet_put_rowset(rowset);
}

static int verify_zval_convert(zval * zvalue, t_type type, const t_value ** value, t_value * space)
{
	int r = -EINVAL;
	switch(type)
	{
		case T_ID:
			if(Z_TYPE_P(zvalue) == IS_RESOURCE)
			{
				php_rowid * rowid = (php_rowid *) zend_fetch_resource(&zvalue TSRMLS_CC, -1, PHP_ROWID_RES_NAME, NULL, 1, le_rowid);
				if(rowid)
				{
					space->v_id = rowid->rowid;
					*value = space;
					r = 0;
				}
			}
			else if(Z_TYPE_P(zvalue) == IS_LONG)
			{
				space->v_id = Z_LVAL_P(zvalue);
				*value = space;
				r = 0;
			}
			break;
		case T_INT:
			if(Z_TYPE_P(zvalue) == IS_LONG)
			{
				space->v_int = Z_LVAL_P(zvalue);
				*value = space;
				r = 0;
			}
			else if(Z_TYPE_P(zvalue) == IS_RESOURCE)
			{
				php_rowid * rowid = (php_rowid *) zend_fetch_resource(&zvalue TSRMLS_CC, -1, PHP_ROWID_RES_NAME, NULL, 1, le_rowid);
				if(rowid)
				{
					space->v_int = rowid->rowid;
					*value = space;
					r = 0;
				}
			}
			break;
		case T_STRING:
			if(Z_TYPE_P(zvalue) == IS_STRING)
			{
				*value = (t_value *) Z_STRVAL_P(zvalue);
				r = 0;
			}
			break;
		/*case T_BLOB:
			if(Z_TYPE_P(zvalue) == IS_STRING)
			{
				space->v_blob.data = Z_STRVAL_P(zvalue);
				space->v_blob.length = Z_STRLEN_P(zvalue);
				*value = space;
				r = 0;
			}
			break;*/
	}
	return r;
}

/* takes a gtable, a string, and up to two values, returns an array of ids */
PHP_FUNCTION(gtable_query)
{
	t_simple_query query;
	t_value values[2];
	t_rowset * rows;
	t_gtable * gtable;
	zval * zgtable;
	zval * zlow = NULL;
	zval * zhigh = NULL;
	char * name;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs|zz", &zgtable, &name, &name_len, &zlow, &zhigh) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	query.name = name;
	if(!toilet_gtable_column_row_count(gtable, name))
	{
		if(strcmp(name, "id"))
		{
			array_init(return_value);
			return;
		}
		query.type = T_ID;
	}
	else
		query.type = toilet_gtable_column_type(gtable, name);
	if(zlow)
	{
		if(verify_zval_convert(zlow, query.type, &query.values[0], &values[0]) < 0)
			RETURN_NULL();
	}
	else
		query.values[0] = NULL;
	if(zhigh)
	{
		if(verify_zval_convert(zhigh, query.type, &query.values[1], &values[1]) < 0)
			RETURN_NULL();
	}
	else
		query.values[1] = NULL;
	rows = toilet_simple_query(gtable, &query);
	if(!rows)
		RETURN_NULL();
	array_init(return_value);
	rowset_to_array(gtable, rows, return_value);
}

/* takes a gtable, a string, and up to two values, returns a long */
PHP_FUNCTION(gtable_count_query)
{
	t_simple_query query;
	t_value values[2];
	t_rowset * rows;
	t_gtable * gtable;
	zval * zgtable;
	zval * zlow = NULL;
	zval * zhigh = NULL;
	char * name;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs|zz", &zgtable, &name, &name_len, &zlow, &zhigh) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	query.name = name;
	if(!toilet_gtable_column_row_count(gtable, name))
	{
		array_init(return_value);
		return;
	}
	query.type = toilet_gtable_column_type(gtable, name);
	if(zlow)
	{
		if(verify_zval_convert(zlow, query.type, &query.values[0], &values[0]) < 0)
			RETURN_NULL();
	}
	else
		query.values[0] = NULL;
	if(zhigh)
	{
		if(verify_zval_convert(zhigh, query.type, &query.values[1], &values[1]) < 0)
			RETURN_NULL();
	}
	else
		query.values[1] = NULL;
	RETURN_LONG(toilet_count_simple_query(gtable, &query));
}

/* takes a gtable, returns an array of ids */
PHP_FUNCTION(gtable_rows)
{
	t_simple_query query = {.name = NULL};
	t_rowset * rows;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	rows = toilet_simple_query(gtable, &query);
	if(!rows)
		RETURN_NULL();
	array_init(return_value);
	rowset_to_array(gtable, rows, return_value);
}

/* takes a gtable, returns a rowid */
PHP_FUNCTION(gtable_new_row)
{
	php_rowid * prowid;
	t_row_id rowid;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	if(toilet_new_row(gtable, &rowid) < 0)
		RETURN_NULL();
	prowid = emalloc(sizeof(*prowid));
	prowid->rowid = rowid;
	prowid->gtable = gtable;
	ZEND_REGISTER_RESOURCE(return_value, prowid, le_rowid);
}

/* takes a gtable, returns a long */
PHP_FUNCTION(gtable_maintain)
{
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	RETURN_LONG(toilet_gtable_maintain(gtable));
}

static void row_hash_populate_column(zval * hash, t_row * row, const char * name, t_type type)
{
	t_value id_value;
	const t_value * value;
	if(!strcmp(name, "id"))
	{
		id_value.v_id = toilet_row_id(row);
		value = &id_value;
	}
	else
	{
		value = toilet_row_value(row, name, type);
		if(!value)
			return;
	}
	switch(type)
	{
		case T_ID:
		{
			zval * zrowid;
			php_rowid * rowid = emalloc(sizeof(*rowid));
			rowid->rowid = value->v_id;
			rowid->gtable = toilet_row_gtable(row);
			ALLOC_INIT_ZVAL(zrowid);
			ZEND_REGISTER_RESOURCE(zrowid, rowid, le_rowid);
			add_assoc_zval(hash, (char *) name, zrowid);
			break;
		}
		case T_INT:
			add_assoc_long(hash, (char *) name, value->v_int);
			break;
		case T_STRING:
			add_assoc_string(hash, (char *) name, (char *) value->v_string, 1);
			break;
		/*case T_BLOB:
			add_assoc_stringl(hash, (char *) name, value->v_blob.data, value->v_blob.length, 1);
			break;*/
	}
}

/* takes two rowids, returns a boolean */
PHP_FUNCTION(rowid_equal)
{
	php_rowid * rowids[2];
	zval * zrowids[2];
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rr", &zrowids[0], &zrowids[1]) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(rowids[0], php_rowid *, &zrowids[0], -1, PHP_ROWID_RES_NAME, le_rowid);
	ZEND_FETCH_RESOURCE(rowids[1], php_rowid *, &zrowids[1], -1, PHP_ROWID_RES_NAME, le_rowid);
	if(rowids[0]->rowid == rowids[1]->rowid && rowids[0]->gtable == rowids[1]->gtable)
		RETURN_TRUE;
	RETURN_FALSE;
}

/* takes a rowid and an optional array of strings, returns an associative array of values */
PHP_FUNCTION(rowid_get_row)
{
	t_row * row;
	php_rowid * rowid;
	zval * zrowid;
	zval * zcolnames = NULL;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r|a", &zrowid, &zcolnames) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(rowid, php_rowid *, &zrowid, -1, PHP_ROWID_RES_NAME, le_rowid);
	row = toilet_get_row(rowid->gtable, rowid->rowid);
	if(!row)
		RETURN_NULL();
	array_init(return_value);
	if(zcolnames)
	{
		/* only specific columns */
		zval ** zname;
		HashPosition pointer;
		HashTable * colnames = Z_ARRVAL_P(zcolnames);
		/* "foreach" */
		for(zend_hash_internal_pointer_reset_ex(colnames, &pointer);
		    zend_hash_get_current_data_ex(colnames, (void **) &zname, &pointer) == SUCCESS;
		    zend_hash_move_forward_ex(colnames, &pointer))
		{
			char * name;
			t_type type;
			if(Z_TYPE_PP(zname) != IS_STRING)
				continue;
			name = Z_STRVAL_PP(zname);
			if(!toilet_gtable_column_row_count(toilet_row_gtable(row), name))
			{
				if(strcmp(name, "id"))
					continue;
				type = T_ID;
			}
			else
				type = toilet_gtable_column_type(toilet_row_gtable(row), name);
			row_hash_populate_column(return_value, row, name, type);
		}
	}
	else
	{
		/* all columns */
		t_columns * columns = toilet_gtable_columns(toilet_row_gtable(row));
		/* TODO: optimize for only those columns in this row */
		while(toilet_columns_valid(columns))
		{
			const char * name = toilet_columns_name(columns);
			t_type type = toilet_columns_type(columns);
			row_hash_populate_column(return_value, row, name, type);
		}
		row_hash_populate_column(return_value, row, "id", T_ID);
	}
	toilet_put_row(row);
}

/* takes a rowid, returns a string */
PHP_FUNCTION(rowid_format)
{
	char name[9];
	php_rowid * rowid;
	zval * zrowid;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zrowid) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(rowid, php_rowid *, &zrowid, -1, PHP_ROWID_RES_NAME, le_rowid);
	snprintf(name, sizeof(name), ROW_FORMAT, rowid->rowid);
	RETURN_STRING(name, 1);
}

/* takes a gtable and a string, returns a rowid */
PHP_FUNCTION(rowid_parse)
{
	t_row_id rowid;
	php_rowid * prowid;
	t_gtable * gtable;
	zval * zgtable;
	char * name = NULL;
	int name_len, chars;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &zgtable, &name, &name_len) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	if(sscanf(name, ROW_FORMAT "%n", &rowid, &chars) != 1)
		RETURN_NULL();
	if(chars != name_len)
		RETURN_NULL();
	prowid = emalloc(sizeof(*prowid));
	prowid->rowid = rowid;
	prowid->gtable = gtable;
	ZEND_REGISTER_RESOURCE(return_value, prowid, le_rowid);
}

static int parse_type(const char * string, t_type * type)
{
	if(!strcmp(string, "id"))
		*type = T_ID;
	else if(!strcmp(string, "int"))
		*type = T_INT;
	else if(!strcmp(string, "string"))
		*type = T_STRING;
	/*else if(!strcmp(string, "blob"))
		*type = T_BLOB;*/
	else
		return -EINVAL;
	return 0;
}

static int guess_zval_convert(zval * zvalue, t_type * type, const t_value ** value, t_value * space)
{
	int r = -EINVAL;
	php_rowid * rowid;
	switch(Z_TYPE_P(zvalue))
	{
		case IS_RESOURCE:
			rowid = (php_rowid *) zend_fetch_resource(&zvalue TSRMLS_CC, -1, PHP_ROWID_RES_NAME, NULL, 1, le_rowid);
			if(rowid)
			{
				*type = T_ID;
				space->v_id = rowid->rowid;
				*value = space;
				r = 0;
			}
			break;
		case IS_LONG:
			*type = T_INT;
			space->v_int = Z_LVAL_P(zvalue);
			*value = space;
			r = 0;
			break;
		case IS_STRING:
			/* warn? this could also be a blob... */
			*type = T_STRING;
			*value = (t_value *) Z_STRVAL_P(zvalue);
			r = 0;
			break;
		default:
			break;
	}
	return r;
}

/* takes a rowid, an associative array of values, and optionally an associative array of type names, returns a boolean */
PHP_FUNCTION(rowid_set_values)
{
	t_row * row;
	php_rowid * rowid;
	zval * zrowid;
	zval * zvalues;
	zval * ztypes = NULL;
	char * name;
	unsigned int name_len;
	unsigned long index;
	zval ** zvalue;
	HashPosition pointer;
	HashTable * values;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ra|a", &zrowid, &zvalues, &ztypes) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(rowid, php_rowid *, &zrowid, -1, PHP_ROWID_RES_NAME, le_rowid);
	row = toilet_get_row(rowid->gtable, rowid->rowid);
	if(!row)
		RETURN_FALSE;
	values = Z_ARRVAL_P(zvalues);
	/* "foreach" */
	for(zend_hash_internal_pointer_reset_ex(values, &pointer);
	    zend_hash_get_current_data_ex(values, (void **) &zvalue, &pointer) == SUCCESS &&
	    zend_hash_get_current_key_ex(values, &name, &name_len, &index, 0, &pointer) != HASH_KEY_NON_EXISTANT;
	    zend_hash_move_forward_ex(values, &pointer))
	{
		if(Z_TYPE_PP(zvalue) == IS_NULL)
		{
			if(toilet_row_remove_key(row, name) < 0)
				/* warning? fail? */
				continue;
		}
		else
		{
			t_gtable * gtable = toilet_row_gtable(row);
			size_t count = toilet_gtable_column_row_count(gtable, name);
			t_value space;
			const t_value * value;
			zval ** ztype;
			char * type_name = NULL;
			t_type type = T_ID; /* to avoid warnings */
			if(ztypes && zend_hash_find(Z_ARRVAL_P(ztypes), name, name_len, (void **) &ztype) == SUCCESS)
				if(Z_TYPE_PP(ztype) == IS_STRING)
				{
					type_name = Z_STRVAL_PP(ztype);
					if(parse_type(type_name, &type) < 0)
						/* complain? */
						type_name = NULL;
				}
			if(count && !type_name)
				type = toilet_gtable_column_type(gtable, name);
			if(!count && !type_name)
			{
				/* warning? (only for string/blob?) */
				if(guess_zval_convert(*zvalue, &type, &value, &space) < 0)
					/* warning? fail? */
					continue;
			}
			else
			{
				if(verify_zval_convert(*zvalue, type, &value, &space) < 0)
					/* warning? fail? */
					continue;
			}
			if(count && type_name)
			{
				if(type != toilet_gtable_column_type(gtable, name))
					/* warning? fail? */
					continue;
			}
			if(toilet_row_set_value(row, name, type, value) < 0)
				/* warning? fail? */
				continue;
		}
	}
	toilet_put_row(row);
	RETURN_TRUE;
}

/* takes a rowid, returns a boolean */
PHP_FUNCTION(rowid_drop)
{
	t_row * row;
	php_rowid * rowid;
	zval * zrowid;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zrowid) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(rowid, php_rowid *, &zrowid, -1, PHP_ROWID_RES_NAME, le_rowid);
	row = toilet_get_row(rowid->gtable, rowid->rowid);
	if(!row)
		RETURN_FALSE;
	if(toilet_drop_row(row) < 0)
	{
		toilet_put_row(row);
		RETURN_FALSE;
	}
	zend_list_delete(Z_LVAL_P(zrowid));
	RETURN_TRUE;
}
