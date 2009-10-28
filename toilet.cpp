/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "openat.h"
#include "blowfish.h"

#include "util.h"
#include "toilet.h"
#include "params.h"
#include "transaction.h"
#include "sys_journal.h"
#include "simple_stable.h"

static void rename_gmon_out(void)
{
	struct stat gmon;
	int r = stat("gmon.out", &gmon);
	if(r >= 0)
	{
		char name[sizeof("gmon.out.") + 12];
		for(uint32_t seq = 0; seq != (uint32_t) -1; seq++)
		{
			snprintf(name, sizeof(name), "gmon.out.%u", seq);
			r = stat(name, &gmon);
			if(r < 0)
			{
				fprintf(stderr, "%s() renaming gmon.out to %s\n", __FUNCTION__, name);
				rename("gmon.out", name);
				break;
			}
		}
	}
}

int toilet_init(const char * path)
{
	int r, fd = open(path, 0);
	if(fd < 0)
		return fd;
	rename_gmon_out();
	/* make maximum log size 4MB */
	r = tx_init(fd, 4194304);
	if(r >= 0)
	{
		r = tx_start_r();
		if(r >= 0)
		{
			sys_journal * global = sys_journal::get_global_journal();
			r = sys_journal::set_unique_id_file(fd, "sys_journal_id", true);
			if(r >= 0)
				r = global->init(fd, "sys_journal", true);
			if(r >= 0)
			{
				/* maybe we should not always do this here? */
				r = global->filter();
				r = tx_end_r();
			}
			else
				tx_end_r();
		}
	}
	close(fd);
	return r;
}

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
	util::memset(&toilet->id, 0, sizeof(toilet->id));
	toilet->path = path;
	toilet->path_fd = dir_fd;
	toilet->errors = errors ? errors : stderr;
	for(int i = 0; i < RECENT_GTABLES; i++)
		toilet->recent_gtable[i] = NULL;
	
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
	if(tx_read(toilet->row_fd, &toilet->next_row, sizeof(toilet->next_row), 0) != sizeof(toilet->next_row))
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
	if(--toilet->out_count <= 0)
	{
		close(toilet->path_fd);
		tx_close(toilet->row_fd);
		delete toilet;
	}
	else if(toilet->out_count <= toilet->recent_gtables)
		for(int i = 0; i < RECENT_GTABLES; i++)
			if(toilet->recent_gtable[i])
				/* this will end up calling toilet_close() recursively */
				toilet_put_gtable(toilet->recent_gtable[i]);
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

static int toilet_new_gtable_type(t_toilet * toilet, const char * name, dtype::ctype type)
{
	int r;
	params config;
	
	/* already exists and in hash? */
	if(toilet->gtables.count(name))
		return -EEXIST;
	if(strlen(name) > GTABLE_NAME_LENGTH)
		return -ENAMETOOLONG;
	if(!*name || *name == '=')
		return -EINVAL;
	
	r = params::parse(LITERAL(
	config [
		"meta" class(dt) cache_dtable
		"meta_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
			]
		]
		"data" class(ct) simple_ctable
		"data_config" config [
			"base" class(dt) cache_dtable
			"base_config" config [
				"cache_size" int 40000
				"base" class(dt) managed_dtable
				"base_config" config [
					"base" class(dt) simple_dtable
				]
			]
		]
	]), &config);
	if(r < 0)
		return -EILSEQ;
	r = tx_start_r();
	if(r < 0)
		return r;
	/* will fail if the gtable already exists */
	r = simple_stable::create(toilet->path_fd, name, config, type);
	tx_end_r();
	if(r < 0)
		return r;
	toilet->gtable_names.push_back(name);
	
	return 0;
}

int toilet_new_gtable(t_toilet * toilet, const char * name)
{
	return toilet_new_gtable_type(toilet, name, dtype::UINT32);
}

int toilet_new_gtable_blobkey(t_toilet * toilet, const char * name)
{
	return toilet_new_gtable_type(toilet, name, dtype::BLOB);
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
	params config;
	
	if(toilet->gtables.count(name))
	{
		gtable = toilet->gtables[name];
		gtable->out_count++;
		if(gtable != toilet->recent_gtable[gtable->recent_gtable_index])
		{
			if(toilet->recent_gtable[toilet->recent_gtable_next])
			{
				assert(toilet->recent_gtable[toilet->recent_gtable_next]->recent_gtable_index == toilet->recent_gtable_next);
				toilet_put_gtable(toilet->recent_gtable[toilet->recent_gtable_next]);
			}
			else
				toilet->recent_gtables++;
			gtable->out_count++;
			gtable->recent_gtable_index = toilet->recent_gtable_next;
			toilet->recent_gtable[toilet->recent_gtable_next++] = gtable;
			if(toilet->recent_gtable_next == RECENT_GTABLES)
				toilet->recent_gtable_next = 0;
		}
		return gtable;
	}
	
	r = params::parse(LITERAL(
	config [
		"meta" class(dt) cache_dtable
		"meta_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
			]
		]
		"data" class(ct) simple_ctable
		"data_config" config [
			"base" class(dt) cache_dtable
			"base_config" config [
				"cache_size" int 40000
				"base" class(dt) managed_dtable
				"base_config" config [
					"base" class(dt) simple_dtable
				]
			]
		]
	]), &config);
	if(r < 0)
		return NULL;
	
	gtable = new t_gtable;
	if(!gtable)
		return NULL;
	gtable->name = name;
	gtable->table = sst = new simple_stable;
	if(!gtable->table)
		goto fail_table;
	gtable->toilet = toilet;
	
	r = tx_start_r();
	if(r < 0)
		goto fail_open;
	r = sst->init(toilet->path_fd, name, config);
	tx_end_r();
	if(r < 0)
		goto fail_open;
	
	toilet->gtables[gtable->name] = gtable;
	toilet->out_count++;
	
	if(toilet->recent_gtable[toilet->recent_gtable_next])
		toilet_put_gtable(toilet->recent_gtable[toilet->recent_gtable_next]);
	else
		toilet->recent_gtables++;
	gtable->out_count++;
	gtable->recent_gtable_index = toilet->recent_gtable_next;
	toilet->recent_gtable[toilet->recent_gtable_next++] = gtable;
	if(toilet->recent_gtable_next == RECENT_GTABLES)
		toilet->recent_gtable_next = 0;
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

bool toilet_gtable_blobkey(t_gtable * gtable)
{
	return gtable->table->key_type() == dtype::BLOB;
}

const char * toilet_gtable_blobcmp_name(t_gtable * gtable)
{
	return gtable->table->get_cmp_name();
}

int toilet_gtable_set_blobcmp(t_gtable * gtable, t_blobcmp * blobcmp)
{
	int value = gtable->table->set_blob_cmp(blobcmp);
	if(value >= 0)
	{
		if(gtable->blobcmp)
			gtable->blobcmp->release();
		gtable->blobcmp = blobcmp;
		/* don't need to retain it; it was passed retained already */
	}
	return value;
}

int toilet_gtable_maintain(t_gtable * gtable)
{
	int r = tx_start_r();
	if(r < 0)
		return r;
	if(gtable->cursor)
	{
		delete gtable->cursor;
		gtable->cursor = NULL;
	}
	r = gtable->table->maintain();
	tx_end_r();
	return r;
}

void toilet_put_gtable(t_gtable * gtable)
{
	if(--gtable->out_count <= 0)
	{
		if(gtable->toilet->recent_gtable[gtable->recent_gtable_index] == gtable)
		{
			gtable->toilet->recent_gtable[gtable->recent_gtable_index] = NULL;
			gtable->toilet->recent_gtables--;
		}
		gtable->toilet->gtables.erase(gtable->name);
		if(gtable->cursor)
			delete gtable->cursor;
		delete gtable->table;
		if(gtable->blobcmp)
			gtable->blobcmp->release();
		toilet_close(gtable->toilet);
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
		case dtype::BLOB:
			return T_BLOB;
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
		case dtype::BLOB:
			return T_BLOB;
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
	int r = tx_start_r();
	if(r < 0)
		return r;
	/* doesn't actually do anything other than reserve a row ID! */
	/* technically this row ID can therefore be used in several gtables... */
	r = toilet_new_row_id(gtable->toilet, new_id);
	tx_end_r();
	return r;
}

int toilet_drop_row(t_row * row)
{
	int r = tx_start_r();
	if(r < 0)
		return r;
	r = row->gtable->table->remove(row->id);
	tx_end_r();
	if(r >= 0)
		toilet_put_row(row);
	return r;
}

t_row * toilet_get_row(t_gtable * gtable, t_row_id row_id)
{
	t_row * row = new t_row(row_id);
	row->gtable = gtable;
	row->out_count = 1;
	gtable->out_count++;
	return row;
}

t_row * toilet_get_row_blobkey(t_gtable * gtable, const void * key, size_t key_size)
{
	t_row * row = new t_row(key, key_size);
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

const void * toilet_row_blobkey(t_row * row, size_t * key_size)
{
	*key_size = row->blobkey.size();
	/* will fail if this is not really a blobkey row */
	return &row->blobkey[0];
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
		case dtype::BLOB:
			if(type != T_BLOB)
				return NULL;
			converted = (t_value *) malloc(sizeof(*converted));
			converted->v_blob.length = value.blb.size();
			converted->v_blob.data = malloc(converted->v_blob.length);
			util::memcpy(converted->v_blob.data, &value.blb[0], converted->v_blob.length);
			return converted;
	}
	abort();
}

const t_value * toilet_row_value_type(t_row * row, const char * key, t_type * type)
{
	*type = toilet_gtable_column_type(row->gtable, key);
	return toilet_row_value(row, key, *type);
}

int toilet_row_set_value_hint(t_row * row, const char * key, t_type type, const t_value * value, bool append)
{
	int r = tx_start_r();
	if(r < 0)
		return r;
	switch(type)
	{
		case T_INT:
			if(row->gtable->table->key_type() == dtype::BLOB)
				r = row->gtable->table->insert(row->blobkey, key, value->v_int, append);
			else
				r = row->gtable->table->insert(row->id, key, value->v_int, append);
			if(r >= 0 && row->values.count(key))
				row->values[key]->v_int = value->v_int;
			tx_end_r();
			return r;
		case T_FLOAT:
			if(row->gtable->table->key_type() == dtype::BLOB)
				r = row->gtable->table->insert(row->blobkey, key, value->v_float, append);
			else
				r = row->gtable->table->insert(row->id, key, value->v_float, append);
			if(r >= 0 && row->values.count(key))
				row->values[key]->v_float = value->v_float;
			tx_end_r();
			return r;
		case T_STRING:
			if(row->gtable->table->key_type() == dtype::BLOB)
				r = row->gtable->table->insert(row->blobkey, key, value->v_string, append);
			else
				r = row->gtable->table->insert(row->id, key, value->v_string, append);
			if(r >= 0 && row->values.count(key))
			{
				free(row->values[key]);
				row->values[key] = (t_value *) strdup(value->v_string);
			}
			tx_end_r();
			return r;
		case T_BLOB:
		{
			blob b(value->v_blob.length, value->v_blob.data);
			if(row->gtable->table->key_type() == dtype::BLOB)
				r = row->gtable->table->insert(row->blobkey, key, b, append);
			else
				r = row->gtable->table->insert(row->id, key, b, append);
			if(r >= 0 && row->values.count(key))
			{
				t_value * cache = row->values[key];
				free(cache->v_blob.data);
				cache->v_blob.length = value->v_blob.length;
				cache->v_blob.data = malloc(value->v_blob.length);
				util::memcpy(cache->v_blob.data, value->v_blob.data, value->v_blob.length);
			}
			tx_end_r();
			return r;
		}
	}
	abort();
}

int toilet_row_remove_key(t_row * row, const char * key)
{
	int r = tx_start_r();
	if(r < 0)
		return r;
	r = row->gtable->table->remove(row->id, key);
	if(r >= 0 && row->values.count(key))
	{
		free(row->values[key]);
		row->values.erase(key);
	}
	tx_end_r();
	return r;
}

t_cursor * toilet_gtable_cursor(t_gtable * gtable)
{
	t_cursor * cursor;
	if(gtable->cursor)
	{
		cursor = gtable->cursor;
		assert(cursor->gtable == gtable);
		gtable->cursor = NULL;
		cursor->iter->first();
		return cursor;
	}
	cursor = new t_cursor;
	cursor->iter = gtable->table->keys();
	cursor->gtable = gtable;
	return cursor;
}

int toilet_cursor_valid(t_cursor * cursor)
{
	return cursor->iter->valid();
}

bool toilet_cursor_seek(t_cursor * cursor, t_row_id id)
{
	return cursor->iter->seek(id);
}

bool toilet_cursor_seek_blobkey(t_cursor * cursor, const void * key, size_t key_size)
{
	return cursor->iter->seek(dtype(blob(key_size, key)));
}

bool toilet_cursor_seek_magic(t_cursor * cursor, int (*magic)(const void *, size_t, void *), void * user)
{
	class local : public dtype_test
	{
	public:
		typedef int (*test_fnp)(const void * key, size_t key_size, void * user);
		
		virtual int operator()(const dtype & key) const
		{
			assert(key.type == dtype::BLOB);
			return test(&key.blb[0], key.blb.size(), user);
		}
		
		local(test_fnp test, void * user) : test(test), user(user) {}
	private:
		test_fnp test;
		void * user;
	} test(magic, user);
	return cursor->iter->seek(test);
}

int toilet_cursor_next(t_cursor * cursor)
{
	return cursor->iter->next();
}

int toilet_cursor_prev(t_cursor * cursor)
{
	return cursor->iter->prev();
}

int toilet_cursor_first(t_cursor * cursor)
{
	return cursor->iter->first();
}

int toilet_cursor_last(t_cursor * cursor)
{
	return cursor->iter->last();
}

t_row_id toilet_cursor_row_id(t_cursor * cursor)
{
	dtype key = cursor->iter->key();
	assert(key.type == dtype::UINT32);
	return key.u32;
}

const void * toilet_cursor_row_blobkey(t_cursor * cursor, size_t * key_size)
{
	dtype key = cursor->iter->key();
	assert(key.type == dtype::BLOB);
	*key_size = key.blb.size();
	return &key.blb[0];
}

void toilet_close_cursor(t_cursor * cursor)
{
	if(cursor->gtable->cursor)
		delete cursor;
	else
		cursor->gtable->cursor = cursor;
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
		case T_INT:
			assert(value.type == dtype::UINT32);
			if(!query->values[1])
				return !value.compare(query->values[0]->v_int);
			return 0 <= value.compare(query->values[0]->v_int) && value.compare(query->values[1]->v_int) <= 0;
		case T_FLOAT:
			assert(value.type == dtype::DOUBLE);
			if(!query->values[1])
				return !value.compare(query->values[0]->v_float);
			return 0 <= value.compare(query->values[0]->v_float) && value.compare(query->values[1]->v_float) <= 0;
		case T_STRING:
			assert(value.type == dtype::STRING);
			if(!query->values[1])
				return !value.compare(query->values[0]->v_string);
			return 0 <= value.compare(query->values[0]->v_string) && value.compare(query->values[1]->v_string) <= 0;
		case T_BLOB:
		{
			assert(value.type == dtype::BLOB);
			/* FIXME: copies the blobs just to compare them */
			blob b0(query->values[0]->v_blob.length, query->values[0]->v_blob.data), b1;
			if(!query->values[1])
				return !value.compare(b0);
			b1 = blob(query->values[1]->v_blob.length, query->values[1]->v_blob.data);
			return 0 <= value.compare(b0) && value.compare(b1) <= 0;
		}
	}
	abort();
}

t_rowset * toilet_simple_query(t_gtable * gtable, t_simple_query * query)
{
	if(query->name)
	{
		/* no such column */
		if(!gtable->table->row_count(query->name))
		{
			t_rowset * result = new t_rowset;
			if(!strcmp(query->name, "id"))
			{
				if(!query->values[0])
				{
					delete result;
					query->name = NULL;
					goto no_name;
				}
				if(query->values[1])
				{
					delete result;
					return NULL;
				}
				if(gtable->table->contains(query->values[0]->v_int))
				{
					t_row_id id = query->values[0]->v_int;
					result->rows.push_back(id);
					result->ids.insert(id);
				}
			}
			return result;
		}
		switch(gtable->table->column_type(query->name))
		{
			case dtype::UINT32:
				if(query->type != T_INT)
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
			case dtype::BLOB:
				if(query->type != T_BLOB)
					return NULL;
				break;
			/* no default; want the compiler to warn of new cases */
		}
	}
no_name:
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
			if(query->type != T_INT)
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
		case dtype::BLOB:
			if(query->type != T_BLOB)
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

t_blobcmp * toilet_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill, bool free_user)
{
	t_blobcmp * blobcmp = new t_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = free_user;
	blobcmp->user = user;
	return blobcmp;
}

t_blobcmp * toilet_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill)
{
	t_blobcmp * blobcmp = new t_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = true;
	blobcmp->user = malloc(size);
	util::memcpy(blobcmp->user, user, size);
	return blobcmp;
}

const char * toilet_blobcmp_name(const t_blobcmp * blobcmp)
{
	return blobcmp->name;
}

void toilet_blobcmp_retain(t_blobcmp * blobcmp)
{
	blobcmp->retain();
}

void toilet_blobcmp_release(t_blobcmp ** blobcmp)
{
	(*blobcmp)->release();
	*blobcmp = NULL;
}
