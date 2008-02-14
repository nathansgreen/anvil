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
static int le_column;
static int le_rowid;

static function_entry toilet_functions[] = {
	PHP_FE(toilet_open, NULL)
	PHP_FE(toilet_close, NULL)
	PHP_FE(toilet_gtables, NULL)
	PHP_FE(toilet_gtable, NULL)
	PHP_FE(toilet_new_gtable, NULL)
	PHP_FE(gtable_name, NULL)
	PHP_FE(gtable_close, NULL)
	PHP_FE(gtable_columns, NULL)
	PHP_FE(gtable_query, NULL)
	PHP_FE(gtable_count_query, NULL)
	PHP_FE(gtable_rows, NULL)
	PHP_FE(gtable_new_row, NULL)
	PHP_FE(column_name, NULL)
	PHP_FE(column_type, NULL)
	PHP_FE(column_count, NULL)
	PHP_FE(column_is_multi, NULL)
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
	le_column = zend_register_list_destructors_ex(NULL, NULL, PHP_COLUMN_RES_NAME, module_number);
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
	RETURN_STRING((char *) NAME(gtable), 1);
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
	int i;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	array_init(return_value);
	for(i = 0; i < COLUMNS(gtable); i++)
	{
		t_column * column = COLUMN(gtable, i);
		const char * type = toilet_name_type(TYPE(column));
		add_assoc_string(return_value, (char *) NAME(column), (char *) type, 1);
	}
}

/* takes a gtable and a string, returns a column */
PHP_FUNCTION(gtable_column)
{
	t_column * column;
	t_gtable * gtable;
	zval * zgtable;
	char * name = NULL;
	int name_len;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &zgtable, &name, &name_len) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	column = toilet_gtable_get_column(gtable, name);
	if(!column)
		RETURN_NULL();
	ZEND_REGISTER_RESOURCE(return_value, column, le_column);
}

static void rowset_to_array(t_toilet * toilet, t_rowset * rowset, zval * array)
{
	int i;
	for(i = 0; i < ROWS(rowset); i++)
	{
		zval * zrowid;
		php_rowid * rowid = emalloc(sizeof(*rowid));
		rowid->rowid = ROW(rowset, i);
		rowid->toilet = toilet;
		ALLOC_INIT_ZVAL(zrowid);
		ZEND_REGISTER_RESOURCE(zrowid, rowid, le_rowid);
		add_next_index_zval(array, zrowid);
	}
	toilet_put_rowset(rowset);
}

static int verify_zval_convert(zval * zvalue, t_type type, t_value ** value)
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
					(*value)->v_id = rowid->rowid;
					r = 0;
				}
			}
			break;
		case T_INT:
			if(Z_TYPE_P(zvalue) == IS_LONG)
			{
				(*value)->v_int = Z_LVAL_P(zvalue);
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
				(*value)->v_blob.data = Z_STRVAL_P(zvalue);
				(*value)->v_blob.length = Z_STRLEN_P(zvalue);
			}
			break;
	}
	return r;
}

/* takes a gtable, a string, and up to two values, returns an array of ids */
PHP_FUNCTION(gtable_query)
{
	t_query query;
	t_value values[2];
	t_rowset * rows;
	t_column * column;
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
	column = toilet_gtable_get_column(gtable, name);
	if(!column)
		RETURN_NULL();
	query.type = column->type;
	if(zlow)
	{
		query.values[0] = &values[0];
		if(verify_zval_convert(zlow, column->type, &query.values[0]) < 0)
			RETURN_NULL();
	}
	else
		query.values[0] = NULL;
	if(zhigh)
	{
		query.values[1] = &values[1];
		if(verify_zval_convert(zhigh, column->type, &query.values[1]) < 0)
			RETURN_NULL();
	}
	else
		query.values[1] = NULL;
	rows = toilet_query(gtable, &query);
	if(!rows)
		RETURN_NULL();
	array_init(return_value);
	rowset_to_array(gtable->toilet, rows, return_value);
}

/* takes a gtable, a string, and up to two values, returns a long */
PHP_FUNCTION(gtable_count_query)
{
	t_query query;
	t_value values[2];
	t_rowset * rows;
	t_column * column;
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
	column = toilet_gtable_get_column(gtable, name);
	if(!column)
		RETURN_NULL();
	query.type = column->type;
	if(zlow)
	{
		query.values[0] = &values[0];
		if(verify_zval_convert(zlow, column->type, &query.values[0]) < 0)
			RETURN_NULL();
	}
	else
		query.values[0] = NULL;
	if(zhigh)
	{
		query.values[1] = &values[1];
		if(verify_zval_convert(zhigh, column->type, &query.values[1]) < 0)
			RETURN_NULL();
	}
	else
		query.values[1] = NULL;
	RETURN_LONG(toilet_count_query(gtable, &query));
}

/* takes a gtable, returns an array of ids */
PHP_FUNCTION(gtable_rows)
{
	t_query query = {.name = NULL};
	t_rowset * rows;
	t_gtable * gtable;
	zval * zgtable;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zgtable) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(gtable, t_gtable *, &zgtable, -1, PHP_GTABLE_RES_NAME, le_gtable);
	rows = toilet_query(gtable, &query);
	if(!rows)
		RETURN_NULL();
	array_init(return_value);
	rowset_to_array(gtable->toilet, rows, return_value);
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
	prowid->toilet = gtable->toilet;
	ZEND_REGISTER_RESOURCE(return_value, prowid, le_rowid);
}

/* takes a column, returns a string */
PHP_FUNCTION(column_name)
{
	t_column * column;
	zval * zcolumn;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zcolumn) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(column, t_column *, &zcolumn, -1, PHP_COLUMN_RES_NAME, le_column);
	RETURN_STRING((char *) NAME(column), 1);
}

/* takes a column, returns a type (as string) */
PHP_FUNCTION(column_type)
{
	t_column * column;
	zval * zcolumn;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zcolumn) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(column, t_column *, &zcolumn, -1, PHP_COLUMN_RES_NAME, le_column);
	RETURN_STRING((char *) toilet_name_type(TYPE(column)), 1);
}

/* takes a column, returns a long */
PHP_FUNCTION(column_count)
{
	t_column * column;
	zval * zcolumn;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zcolumn) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(column, t_column *, &zcolumn, -1, PHP_COLUMN_RES_NAME, le_column);
	RETURN_LONG(COUNT(column));
}

/* takes a column, returns a boolean */
PHP_FUNCTION(column_is_multi)
{
	t_column * column;
	zval * zcolumn;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &zcolumn) == FAILURE)
		RETURN_FALSE;
	ZEND_FETCH_RESOURCE(column, t_column *, &zcolumn, -1, PHP_COLUMN_RES_NAME, le_column);
	RETURN_BOOL(toilet_column_is_multi(column));
}

static void row_hash_populate_column(zval * hash, t_row * row, t_column * column)
{
	if(toilet_column_is_multi(column))
	{
		/* TODO */
	}
	else
	{
		t_value id_value;
		t_value * value;
		if(!strcmp(NAME(column), "id"))
		{
			id_value.v_id = row->id;
			value = &id_value;
		}
		else
		{
			value = toilet_row_value(row, NAME(column), TYPE(column));
			if(!value)
				return;
		}
		switch(TYPE(column))
		{
			case T_ID:
			{
				zval * zrowid;
				php_rowid * rowid = emalloc(sizeof(*rowid));
				rowid->rowid = value->v_id;
				rowid->toilet = row->gtable->toilet;
				ALLOC_INIT_ZVAL(zrowid);
				ZEND_REGISTER_RESOURCE(zrowid, rowid, le_rowid);
				add_assoc_zval(hash, (char *) NAME(column), zrowid);
				break;
			}
			case T_INT:
				add_assoc_long(hash, (char *) NAME(column), value->v_int);
				break;
			case T_STRING:
				add_assoc_string(hash, (char *) NAME(column), (char *) value->v_string, 1);
				break;
			case T_BLOB:
				add_assoc_stringl(hash, (char *) NAME(column), value->v_blob.data, value->v_blob.length, 1);
				break;
		}
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
	if(rowids[0]->rowid == rowids[1]->rowid && rowids[0]->toilet == rowids[1]->toilet)
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
	row = toilet_get_row(rowid->toilet, rowid->rowid);
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
			t_column * column;
			if(Z_TYPE_PP(zname) != IS_STRING)
				continue;
			name = Z_STRVAL_PP(zname);
			column = toilet_gtable_get_column(row->gtable, name);
			if(!column)
				/* TODO: warn "no such column in gtable" ? */
				continue;
			row_hash_populate_column(return_value, row, column);
		}
	}
	else
	{
		/* all columns */
		int i;
		/* TODO: optimize for only those columns in this row */
		for(i = 0; i < COLUMNS(row->gtable); i++)
			row_hash_populate_column(return_value, row, COLUMN(row->gtable, i));
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

/* takes a toilet and a string, returns a rowid */
PHP_FUNCTION(rowid_parse)
{
	t_row_id rowid;
	php_rowid * prowid;
	t_toilet * toilet;
	zval * ztoilet;
	char * name = NULL;
	int name_len, chars;
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs", &ztoilet, &name, &name_len) == FAILURE)
		RETURN_NULL();
	ZEND_FETCH_RESOURCE(toilet, t_toilet *, &ztoilet, -1, PHP_TOILET_RES_NAME, le_toilet);
	if(sscanf(name, ROW_FORMAT "%n", &rowid, &chars) != 1)
		RETURN_NULL();
	if(chars != name_len)
		RETURN_NULL();
	prowid = emalloc(sizeof(*prowid));
	prowid->rowid = rowid;
	prowid->toilet = toilet;
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
	else if(!strcmp(string, "blob"))
		*type = T_BLOB;
	else
		return -EINVAL;
	return 0;
}

static int guess_zval_convert(zval * zvalue, t_type * type, t_value ** value)
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
				(*value)->v_id = rowid->rowid;
				r = 0;
			}
			break;
		case IS_LONG:
			*type = T_INT;
			(*value)->v_int = Z_LVAL_P(zvalue);
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
	row = toilet_get_row(rowid->toilet, rowid->rowid);
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
			t_column * column = toilet_gtable_get_column(row->gtable, name);
			t_value _value;
			t_value * value = &_value;
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
			if(column && !type_name)
				type = TYPE(column);
			if(!column && !type_name)
			{
				/* warning? (only for string/blob?) */
				if(guess_zval_convert(*zvalue, &type, &value) < 0)
					/* warning? fail? */
					continue;
			}
			else
			{
				if(verify_zval_convert(*zvalue, type, &value) < 0)
					/* warning? fail? */
					continue;
			}
			if(column && type_name)
			{
				if(type != TYPE(column))
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
	row = toilet_get_row(rowid->toilet, rowid->rowid);
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
