/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "openat.h"
#include "blowfish.h"
#include "toilet++.h"
#include "transaction.h"

#include "simple_dtable.h"
#include "managed_dtable.h"
#include "simple_ctable.h"
#include "simple_stable.h"

int toilet_new(const char * path)
{
	int r, dir_fd, fd;
	FILE * version;
	uint8_t id[T_ID_SIZE];
	t_row_id next = 0;
	
	r = mkdir(path, 0775);
	if(r < 0)
		return r;
	dir_fd = open(path, 0);
	if(dir_fd < 0)
		goto fail_open;
	
	version = fopenat(dir_fd, "=toilet-version", "w");
	if(!version)
		goto fail_version;
	fprintf(version, "1\n");
	fclose(version);
	
	fd = open("/dev/urandom", O_RDONLY);
	if(fd < 0)
		goto fail_id_1;
	r = read(fd, &id, sizeof(id));
	close(fd);
	if(r != sizeof(id))
		goto fail_id_1;
	fd = openat(dir_fd, "=toilet-id", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_1;
	r = write(fd, &id, sizeof(id));
	close(fd);
	if(r != sizeof(id))
		goto fail_id_2;
	
	fd = openat(dir_fd, "=next-row", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_2;
	r = write(fd, &next, sizeof(next));
	close(fd);
	if(r != sizeof(next))
		goto fail_next;
	
	close(dir_fd);
	return 0;
	
fail_next:
	unlinkat(dir_fd, "=next-row", 0);
fail_id_2:
	unlinkat(dir_fd, "=toilet-id", 0);
fail_id_1:
	unlinkat(dir_fd, "=toilet-version", 0);
fail_version:
	close(dir_fd);
fail_open:
	rmdir(path);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

t_toilet * toilet_open(const char * path, FILE * errors)
{
	DIR * gtable_list;
	struct dirent * ent;
	FILE * version_file;
	char version_str[16];
	int dir_fd, id_fd, copy;
	t_toilet * toilet;
	
	dir_fd = open(path, 0);
	if(dir_fd < 0)
		return NULL;
	
	/* allocate and initialize the structure */
	toilet = new t_toilet;
	if(!toilet)
		goto fail_new;
	memset(&toilet->id, 0, sizeof(toilet->id));
	toilet->next_row = 0;
	toilet->path = path;
	toilet->path_fd = dir_fd;
	toilet->errors = errors ? errors : stderr;
	
	/* check the version */
	version_file = fopenat(dir_fd, "=toilet-version", "r");
	if(!version_file)
		goto fail_fopen;
	fgets(version_str, sizeof(version_str), version_file);
	if(feof(version_file) || ferror(version_file))
		goto fail_fgets;
	if(strcmp(version_str, "1\n"))
		goto fail_fgets;
	
	/* get the database ID */
	id_fd = openat(dir_fd, "=toilet-id", O_RDONLY);
	if(id_fd < 0)
		goto fail_fgets;
	if(read(id_fd, &toilet->id, sizeof(toilet->id)) != sizeof(toilet->id))
		goto fail_read_1;
	
	/* get the next row ID source value */
	toilet->row_fd = tx_open(dir_fd, "=next-row", O_RDWR);
	if(toilet->row_fd < 0)
		goto fail_read_1;
	if(read(tx_read_fd(toilet->row_fd), &toilet->next_row, sizeof(toilet->next_row)) != sizeof(toilet->next_row))
		goto fail_read_2;
	
	/* get the list of gtable names */
	copy = dup(dir_fd);
	if(copy < 0)
		goto fail_read_2;
	gtable_list = fdopendir(copy);
	if(!gtable_list)
	{
		close(copy);
		goto fail_read_2;
	}
	while((ent = readdir(gtable_list)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(ent->d_name[0] == '=')
			continue;
		toilet->gtable_names.push_back(ent->d_name);
	}
	closedir(gtable_list);
	
	close(id_fd);
	fclose(version_file);
	
	return toilet;
	
	/* error handling */
fail_read_2:
	tx_close(toilet->row_fd);
fail_read_1:
	close(id_fd);
fail_fgets:
	fclose(version_file);
fail_fopen:
	delete toilet;
fail_new:
	close(dir_fd);
	return NULL;
}

int toilet_close(t_toilet * toilet)
{
	/* should be more stuff here, e.g. check not in use */
	close(toilet->path_fd);
	tx_close(toilet->row_fd);
	delete toilet;
	return 0;
}

size_t toilet_gtables_count(t_toilet * toilet)
{
	return toilet->gtable_names.size();
}

const char * toilet_gtables_name(t_toilet * toilet, size_t index)
{
	return toilet->gtable_names[index];
}

int toilet_new_gtable(t_toilet * toilet, const char * name)
{
	int r;
	params config, base_config;
	
	/* already exists and in hash? */
	if(toilet->gtables.count(name))
		return -EEXIST;
	if(strlen(name) > GTABLE_NAME_LENGTH)
		return -ENAMETOOLONG;
	if(!*name || *name == '=')
		return -EINVAL;
	
	base_config.set_class("base", simple_dtable);
	config.set("meta_config", base_config);
	config.set("data_config", base_config);
	config.set_class("meta", managed_dtable);
	config.set_class("data", managed_dtable);
	r = tx_start();
	if(r < 0)
		return r;
	/* will fail if the gtable already exists */
	r = simple_stable::create(toilet->path_fd, name, config, dtype::UINT32);
	tx_end(0);
	if(r < 0)
		return r;
	toilet->gtable_names.push_back(name);
	
	return 0;
}

int toilet_drop_gtable(t_gtable * gtable)
{
	return -ENOSYS;
}

t_gtable * toilet_get_gtable(t_toilet * toilet, const char * name)
{
	int r;
	t_gtable * gtable;
	simple_stable * sst;
	params config, base_config;
	
	if(toilet->gtables.count(name))
	{
		gtable = toilet->gtables[name];
		gtable->out_count++;
		return gtable;
	}
	
	gtable = new t_gtable;
	if(!gtable)
		return NULL;
	gtable->name = name;
	gtable->table = sst = new simple_stable;
	if(!gtable->table)
		goto fail_table;
	gtable->toilet = toilet;
	gtable->out_count = 1;
	
	base_config.set_class("base", simple_dtable);
	config.set("meta_config", base_config);
	config.set("data_config", base_config);
	config.set_class("meta", managed_dtable);
	config.set_class("data", managed_dtable);
	config.set_class("columns", simple_ctable);
	r = tx_start();
	if(r < 0)
		goto fail_open;
	r = sst->init(toilet->path_fd, name, config);
	tx_end(0);
	if(r < 0)
		goto fail_open;
	
	toilet->gtables[gtable->name] = gtable;
	
	return gtable;
	
fail_open:
	delete gtable->table;
fail_table:
	delete gtable;
	return NULL;
}

const char * toilet_gtable_name(t_gtable * gtable)
{
	return gtable->name;
}

int toilet_gtable_maintain(t_gtable * gtable)
{
	int r = tx_start();
	if(r < 0)
		return r;
	r = gtable->table->maintain();
	tx_end(0);
	return r;
}

void toilet_put_gtable(t_gtable * gtable)
{
	if(--gtable->out_count <= 0)
	{
		gtable->toilet->gtables.erase(gtable->name);
		delete gtable->table;
		delete gtable;
	}
}

t_columns * toilet_gtable_columns(t_gtable * gtable)
{
	t_columns_union safer;
	safer.iter = gtable->table->columns();
	return safer.columns;
}

bool toilet_columns_valid(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	return safer.iter->valid();
}

const char * toilet_columns_name(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	return safer.iter->name();
}

t_type toilet_columns_type(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	switch(safer.iter->type())
	{
		case dtype::UINT32:
			return T_INT;
		case dtype::DOUBLE:
			return T_FLOAT;
		case dtype::STRING:
			return T_STRING;
	}
	abort();
}

size_t toilet_columns_row_count(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	return safer.iter->row_count();
}

void toilet_columns_next(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	safer.iter->next();
}

void toilet_put_columns(t_columns * columns)
{
	t_columns_union safer;
	safer.columns = columns;
	delete safer.iter;
}

t_type toilet_gtable_column_type(t_gtable * gtable, const char * name)
{
	dtype::ctype type = gtable->table->column_type(name);
	switch(type)
	{
		case dtype::UINT32:
			return T_INT;
		case dtype::DOUBLE:
			return T_FLOAT;
		case dtype::STRING:
			return T_STRING;
	}
	abort();
}

size_t toilet_gtable_column_row_count(t_gtable * gtable, const char * name)
{
	return gtable->table->row_count(name);
}

static int toilet_new_row_id(t_toilet * toilet, t_row_id * row)
{
	int r;
	bf_ctx bfc;
	t_row_id next = toilet->next_row + 1;
	r = tx_write(toilet->row_fd, &next, sizeof(next), 0);
	if(r < 0)
		return r;
	bf_setkey(&bfc, toilet->id, sizeof(toilet->id));
	*row = bf32_encipher(&bfc, toilet->next_row);
	toilet->next_row = next;
	return 0;
}

int toilet_new_row(t_gtable * gtable, t_row_id * new_id)
{
	int r = tx_start();
	if(r < 0)
		return r;
	/* doesn't actually do anything other than reserve a row ID! */
	/* technically this row ID can therefore be used in several gtables... */
	r = toilet_new_row_id(gtable->toilet, new_id);
	tx_end(0);
	return r;
}

int toilet_drop_row(t_row * row)
{
	int r = tx_start();
	if(r < 0)
		return r;
	r = row->gtable->table->remove(row->id);
	tx_end(0);
	if(r >= 0)
		toilet_put_row(row);
	return r;
}

t_row * toilet_get_row(t_gtable * gtable, t_row_id row_id)
{
	t_row * row = new t_row;
	row->id = row_id;
	row->gtable = gtable;
	row->out_count = 1;
	gtable->out_count++;
	return row;
}

void toilet_put_row(t_row * row)
{
	if(--row->out_count <= 0)
	{
		toilet_put_gtable(row->gtable);
		delete row;
	}
}

t_row_id toilet_row_id(t_row * row)
{
	return row->id;
}

t_gtable * toilet_row_gtable(t_row * row)
{
	return row->gtable;
}

const t_value * toilet_row_value(t_row * row, const char * key, t_type type)
{
	/* check the cache first */
	if(row->values.count(key))
		return row->values[key];
	dtype value(0u);
	t_value * converted;
	bool found = row->gtable->table->find(row->id, key, &value);
	if(!found)
		return NULL;
	switch(value.type)
	{
		case dtype::UINT32:
			if(type != T_INT)
				return NULL;
			converted = (t_value *) malloc(sizeof(*converted));
			converted->v_int = value.u32;
			row->values[key] = converted;
			return converted;
		case dtype::DOUBLE:
			if(type != T_FLOAT)
				return NULL;
			converted = (t_value *) malloc(sizeof(*converted));
			converted->v_float = value.dbl;
			row->values[key] = converted;
			return converted;
		case dtype::STRING:
			if(type != T_STRING)
				return NULL;
			converted = (t_value *) strdup(value.str);
			row->values[key] = converted;
			return converted;
	}
	abort();
}

const t_value * toilet_row_value_type(t_row * row, const char * key, t_type * type)
{
	*type = toilet_gtable_column_type(row->gtable, key);
	return toilet_row_value(row, key, *type);
}

int toilet_row_set_value(t_row * row, const char * key, t_type type, const t_value * value)
{
	int r = tx_start();
	if(r < 0)
		return r;
	switch(type)
	{
		case T_ID:
			/* ... hmm, maybe we want to do something special here? */
		case T_INT:
			r = row->gtable->table->append(row->id, key, value->v_int);
			tx_end(0);
			return r;
		case T_FLOAT:
			r = row->gtable->table->append(row->id, key, value->v_float);
			tx_end(0);
			return r;
		case T_STRING:
			r = row->gtable->table->append(row->id, key, value->v_string);
			tx_end(0);
			return r;
	}
	abort();
}

int toilet_row_remove_key(t_row * row, const char * key)
{
	int r = tx_start();
	if(r < 0)
		return r;
	r = row->gtable->table->remove(row->id, key);
	tx_end(0);
	return r;
}

static bool toilet_row_matches(t_gtable * gtable, t_row_id id, t_simple_query * query)
{
	/* match all rows in the gtable */
	if(!query->name)
		return true;
	dtype value(0u);
	if(!gtable->table->find(dtype(id), query->name, &value))
		return false;
	/* match all rows with this column */
	if(!query->values[0])
		return true;
	switch(query->type)
	{
		case T_ID:
		case T_INT:
			assert(value.type == dtype::UINT32);
			if(!query->values[1])
				return value == dtype(query->values[0]->v_int);
			return dtype(query->values[0]->v_int) <= value && value <= dtype(query->values[1]->v_int);
		case T_FLOAT:
			assert(value.type == dtype::DOUBLE);
			if(!query->values[1])
				return value == dtype(query->values[0]->v_float);
			return dtype(query->values[0]->v_float) <= value && value <= dtype(query->values[1]->v_float);
		case T_STRING:
			assert(value.type == dtype::STRING);
			if(!query->values[1])
				return value == dtype(query->values[0]->v_string);
			return dtype(query->values[0]->v_string) <= value && value <= dtype(query->values[1]->v_string);
	}
	abort();
}

t_rowset * toilet_simple_query(t_gtable * gtable, t_simple_query * query)
{
	if(query->name)
	{
		/* no such column */
		if(!gtable->table->row_count(query->name))
			return new t_rowset;
		switch(gtable->table->column_type(query->name))
		{
			case dtype::UINT32:
				if(query->type != T_ID && query->type != T_INT)
					return NULL;
				break;
			case dtype::DOUBLE:
				if(query->type != T_FLOAT)
					return NULL;
				break;
			case dtype::STRING:
				if(query->type != T_STRING)
					return NULL;
				break;
			/* no default; want the compiler to warn of new cases */
		}
	}
	t_rowset * result = new t_rowset;
	/* we don't have indices yet, so just iterate and find the matches */
	dtable::key_iter * iter = gtable->table->keys();
	while(iter->valid())
	{
		dtype key = iter->key();
		t_row_id id = key.u32;
		assert(key.type == dtype::UINT32);
		if(toilet_row_matches(gtable, id, query))
		{
			result->rows.push_back(id);
			result->ids.insert(id);
		}
		iter->next();
	}
	delete iter;
	return result;
}

ssize_t toilet_count_simple_query(t_gtable * gtable, t_simple_query * query)
{
	/* no such column */
	if(!gtable->table->row_count(query->name))
		return 0;
	switch(gtable->table->column_type(query->name))
	{
		case dtype::UINT32:
			if(query->type != T_ID && query->type != T_INT)
				return -EINVAL;
			break;
		case dtype::DOUBLE:
			if(query->type != T_FLOAT)
				return -EINVAL;
			break;
		case dtype::STRING:
			if(query->type != T_STRING)
				return -EINVAL;
			break;
		/* no default; want the compiler to warn of new cases */
	}
	ssize_t result = 0;
	/* we don't have indices yet, so just iterate and find the matches */
	dtable::key_iter * iter = gtable->table->keys();
	while(iter->valid())
	{
		dtype key = iter->key();
		t_row_id id = key.u32;
		assert(key.type == dtype::UINT32);
		if(toilet_row_matches(gtable, id, query))
			result++;
		iter->next();
	}
	delete iter;
	return result;
}

size_t toilet_rowset_size(t_rowset * rowset)
{
	return rowset->rows.size();
}

t_row_id toilet_rowset_row(t_rowset * rowset, size_t index)
{
	return rowset->rows[index];
}

bool toilet_rowset_contains(t_rowset * rowset, t_row_id id)
{
	return rowset->ids.count(id) > 0;
}

void toilet_put_rowset(t_rowset * rowset)
{
	if(!--rowset->out_count)
		/* just IDs in rowsets, nothing to put() */
		delete rowset;
}
