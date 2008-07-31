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
	PHP_FE(rowid_get_row, NULL)
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

PHP_MINIT_FUNCTION(toilet)
{
	le_toilet = zend_register_list_destructors_ex(php_toilet_dtor, NULL, PHP_TOILET_RES_NAME, module_number);
	le_gtable = zend_register_list_destructors_ex(php_gtable_dtor, NULL, PHP_GTABLE_RES_NAME, module_number);
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
		toilet_columns_next(columns);
	}
	toilet_put_columns(columns);
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

static void rowset_to_array(t_rowset * rowset, zval * array)
{
	size_t i, max = toilet_rowset_size(rowset);
	for(i = 0; i < max; i++)
		add_next_index_long(array, toilet_rowset_row(rowset, i));
	toilet_put_rowset(rowset);
}

static int verify_zval_convert(zval * zvalue, t_type type, const t_value ** value, t_value * space)
{
	int r = -EINVAL;
	switch(type)
	{
		case T_INT:
			if(Z_TYPE_P(zvalue) == IS_LONG)
			{
				space->v_int = Z_LVAL_P(zvalue);
				*value = space;
				r = 0;
			}
			break;
		case T_FLOAT:
			if(Z_TYPE_P(zvalue) == IS_DOUBLE)
			{
				space->v_float = Z_DVAL_P(zvalue);
				*value = space;
				r = 0;
			}
			break;
		case T_STRING:
			if(Z_TYPE_P(zvalue) == IS_STRING)
			{
				*value = (t_value *) Z_STRVAL_P(zvalue);
				r = 0;
			}
			break;
		case T_BLOB:
			if(Z_TYPE_P(zvalue) == IS_STRING)
			{
				space->v_blob.data = Z_STRVAL_P(zvalue);
				space->v_blob.length = Z_STRLEN_P(zvalue);
				*value = space;
				r = 0;
			}
			break;
	}
	return r;
}

/* takes a gtable, a string, and up to two values, returns an array of rowids */
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
		query.type = T_INT;
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
	rowset_to_array(rows, return_value);
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
		if(strcmp(name, "id"))
		{
			array_init(return_value);
			return;
		}
		query.type = T_INT;
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
	RETURN_LONG(toilet_count_simple_query(gtable, &query));
}

/* takes a gtable, returns an array of rowids */
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
	rowset_to_array(rows, return_value);
}

/* takes a gtable, returns a rowid */
PHP_FUNCTION(gtable_new_row)
{
	t_row_id rowid;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	if(toilet_new_row(gtable, &rowid) < 0)
		RETURN_NULL();
	RETURN_LONG(rowid);
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
		id_value.v_int = toilet_row_id(row);
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
		case T_INT:
			add_assoc_long(hash, (char *) name, value->v_int);
			break;
		case T_FLOAT:
			add_assoc_float(hash, (char *) name, value->v_float);
			break;
		case T_STRING:
			add_assoc_string(hash, (char *) name, (char *) value->v_string, 1);
			break;
		case T_BLOB:
			add_assoc_stringl(hash, (char *) name, value->v_blob.data, value->v_blob.length, 1);
			break;
	}
}

/* takes a gtable, a rowid, and an optional array of strings, returns an associative array of values */
PHP_FUNCTION(rowid_get_row)
{
	t_row * row;
	t_gtable * gtable;
	zval * zgtable;
	long rowid;
	zval * zcolnames = NULL;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rl|a", &zgtable, &rowid, &zcolnames) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	row = toilet_get_row(gtable, rowid);
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
				type = T_INT;
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
			toilet_columns_next(columns);
		}
		row_hash_populate_column(return_value, row, "id", T_INT);
		toilet_put_columns(columns);
	}
	toilet_put_row(row);
}

static int parse_type(const char * string, t_type * type)
{
	if(!strcmp(string, "int"))
		*type = T_INT;
	else if(!strcmp(string, "float"))
		*type = T_FLOAT;
	else if(!strcmp(string, "string"))
		*type = T_STRING;
	else if(!strcmp(string, "blob"))
		*type = T_BLOB;
	else
		return -EINVAL;
	return 0;
}

static int guess_zval_convert(zval * zvalue, t_type * type, const t_value ** value, t_value * space)
{
	int r = -EINVAL;
	switch(Z_TYPE_P(zvalue))
	{
		case IS_LONG:
			*type = T_INT;
			space->v_int = Z_LVAL_P(zvalue);
			*value = space;
			r = 0;
			break;
		case IS_DOUBLE:
			*type = T_FLOAT;
			space->v_float = Z_DVAL_P(zvalue);
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

/* takes a gtable, a rowid, an associative array of values, and optionally an associative array of type names, returns a boolean */
PHP_FUNCTION(rowid_set_values)
{
	t_row * row;
	t_gtable * gtable;
	zval * zgtable;
	long rowid;
	zval * zvalues;
	zval * ztypes = NULL;
	char * name;
	unsigned int name_len;
	unsigned long index;
	zval ** zvalue;
	HashPosition pointer;
	HashTable * values;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rla|a", &zgtable, &rowid, &zvalues, &ztypes) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	row = toilet_get_row(gtable, rowid);
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
			t_type type = T_INT; /* to avoid warnings */
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

/* takes a gtable and a rowid, returns a boolean */
PHP_FUNCTION(rowid_drop)
{
	t_row * row;
	t_gtable * gtable;
	zval * zgtable;
	long rowid;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rl", &zgtable, &rowid) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	row = toilet_get_row(gtable, rowid);
	if(!row)
		RETURN_FALSE;
	if(toilet_drop_row(row) < 0)
	{
		toilet_put_row(row);
		RETURN_FALSE;
	}
	RETURN_TRUE;
}
