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

#include "simple_dtable.h"
#include "managed_dtable.h"
#include "simple_ctable.h"
#include "simple_stable.h"

int toiletpp_new(const char * path)
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
	
	r = mkdirat(dir_fd, "=rows", 0775);
	if(r < 0)
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

t_toilet * toiletpp_open(const char * path, FILE * errors)
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
	toilet->path = strdup(path);
	if(!toilet->path)
		goto fail_strdup;
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
	toilet->row_fd = openat(dir_fd, "=next-row", O_RDWR);
	if(toilet->row_fd < 0)
		goto fail_read_1;
	if(read(toilet->row_fd, &toilet->next_row, sizeof(toilet->next_row)) != sizeof(toilet->next_row))
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
		char * name;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(ent->d_name[0] == '=')
			continue;
		name = strdup(ent->d_name);
		if(!name)
			goto fail_loop;
		toilet->gtable_names.push_back(name);
	}
	closedir(gtable_list);
	
	close(id_fd);
	fclose(version_file);
	
	return toilet;
	
	/* error handling */
fail_loop:
	while(!toilet->gtable_names.empty())
	{
		size_t last = toilet->gtable_names.size() - 1;
		free((void *) toilet->gtable_names[last]);
		toilet->gtable_names.pop_back();
	}
	closedir(gtable_list);
fail_read_2:
	close(toilet->row_fd);
fail_read_1:
	close(id_fd);
fail_fgets:
	fclose(version_file);
fail_fopen:
	free((void *) toilet->path);
fail_strdup:
	delete toilet;
fail_new:
	close(dir_fd);
	return NULL;
}

int toiletpp_close(t_toilet * toilet)
{
	/* should be more stuff here, e.g. check not in use */
	while(!toilet->gtable_names.empty())
	{
		size_t last = toilet->gtable_names.size() - 1;
		free((void *) toilet->gtable_names[last]);
		toilet->gtable_names.pop_back();
	}
	free((void *) toilet->path);
	close(toilet->path_fd);
	close(toilet->row_fd);
	delete toilet;
	return 0;
}

size_t toiletpp_gtable_count(t_toilet * toilet)
{
	return toilet->gtable_names.size();
}

const char * toiletpp_gtable_name(t_toilet * toilet, size_t index)
{
	return toilet->gtable_names[index];
}

int toiletpp_new_gtable(t_toilet * toilet, const char * name)
{
	int r;
	char * name_copy;
	dtable_factory * mdtf;
	
	/* already exists and in hash? */
	if(toilet->gtables.count(name))
		return -EEXIST;
	if(strlen(name) > GTABLE_NAME_LENGTH)
		return -ENAMETOOLONG;
	if(!*name || *name == '=')
		return -EINVAL;
	
	mdtf = new managed_dtable_factory(&simple_dtable::factory);
	mdtf->retain();
	/* will fail if the gtable already exists, but will still release mdtf */
	r = simple_stable::create(toilet->path_fd, name, mdtf, mdtf, dtype::UINT32);
	if(r < 0)
		return r;
	name_copy = strdup(name);
	if(!name_copy)
	{
		/* XXX delete the stable on disk */
		return -ENOMEM;
	}
	toilet->gtable_names.push_back(name_copy);
	
	return 0;
}

int toiletpp_drop_gtable(t_gtable * gtable)
{
	return -ENOSYS;
}

t_gtable * toiletpp_get_gtable(t_toilet * toilet, const char * name)
{
	t_gtable * gtable;
	simple_stable * sst;
	dtable_factory * mdtf;
	
	if(toilet->gtables.count(name))
	{
		gtable = toilet->gtables[name];
		gtable->out_count++;
		return gtable;
	}
	
	gtable = new t_gtable;
	if(!gtable)
		return NULL;
	gtable->name = strdup(name);
	if(!gtable->name)
		goto fail_name;
	gtable->table = sst = new simple_stable;
	if(!gtable->table)
		goto fail_table;
	gtable->toilet = toilet;
	gtable->out_count = 1;
	
	mdtf = new managed_dtable_factory(&simple_dtable::factory);
	mdtf->retain();
	if(sst->init(toilet->path_fd, name, mdtf, mdtf, &simple_ctable::factory) < 0)
		goto fail_open;
	
	toilet->gtables[gtable->name] = gtable;
	
	return gtable;
	
fail_open:
	delete gtable->table;
fail_table:
	free((void *) gtable->name);
fail_name:
	delete gtable;
	return NULL;
}

void toiletpp_put_gtable(t_gtable * gtable)
{
	if(--gtable->out_count <= 0)
	{
		gtable->toilet->gtables.erase(gtable->name);
		delete gtable->table;
		free((void *) gtable->name);
		delete gtable;
	}
}

size_t toiletpp_gtable_column_count(t_gtable * gtable)
{
	return gtable->table->column_count();
}

t_type toiletpp_gtable_column_type(t_gtable * gtable, const char * name)
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

size_t toiletpp_gtable_column_rows(t_gtable * gtable, const char * name)
{
	return gtable->table->row_count(name);
}

static int toiletpp_new_row_id(t_toilet * toilet, t_row_id * row)
{
	bf_ctx bfc;
	t_row_id next = toilet->next_row + 1;
	lseek(toilet->row_fd, SEEK_SET, 0);
	if(write(toilet->row_fd, &next, sizeof(next)) != sizeof(next))
		return -1;
	bf_setkey(&bfc, toilet->id, sizeof(toilet->id));
	*row = bf32_encipher(&bfc, toilet->next_row);
	toilet->next_row = next;
	return 0;
}

int toiletpp_new_row(t_gtable * gtable, t_row_id * new_id)
{
	/* doesn't actually do anything other than reserve a row ID! */
	/* technically this row ID can therefore be used in several gtables... */
	return toiletpp_new_row_id(gtable->toilet, new_id);
}

int toiletpp_drop_row(t_row * row)
{
	int r = row->gtable->table->remove(row->id);
	if(r >= 0)
		toiletpp_put_row(row);
	return r;
}

t_row * toiletpp_get_row(t_gtable * gtable, t_row_id row_id)
{
	t_row * row = new t_row;
	row->id = row_id;
	row->gtable = gtable;
	row->out_count = 1;
	gtable->out_count++;
	return row;
}

void toiletpp_put_row(t_row * row)
{
	if(--row->out_count <= 0)
	{
		while(!row->values.empty())
		{
			value_map::iterator it = row->values.begin();
			const char * column = it->first;
			t_value * value = it->second;
			row->values.erase(column);
			delete column;
			delete value;
		}
		toiletpp_put_gtable(row->gtable);
		delete row;
	}
}

t_row_id toiletpp_row_id(t_row * row)
{
	return row->id;
}

const t_value * toiletpp_row_value(t_row * row, const char * key, t_type type)
{
	/* check the cache first */
	if(row->values.count(key))
		return row->values[key];
	dtype value(0u);
	t_value * converted;
	bool found = row->gtable->table->find(row->id, key, &value);
	if(!found)
		return NULL;
	key = strdup(key);
	if(!key)
		return NULL;
	switch(value.type)
	{
		case dtype::UINT32:
			if(type != T_INT)
				return NULL;
			converted = new t_value;
			converted->v_int = value.u32;
			row->values[key] = converted;
			return converted;
		case dtype::DOUBLE:
			if(type != T_FLOAT)
				return NULL;
			converted = new t_value;
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

const t_value * toiletpp_row_value_type(t_row * row, const char * key, t_type * type)
{
	*type = toiletpp_gtable_column_type(row->gtable, key);
	return toiletpp_row_value(row, key, *type);
}

int toiletpp_row_set_value(t_row * row, const char * key, t_type type, t_value * value)
{
	switch(type)
	{
		case T_ID:
			/* ... hmm, maybe we want to do something special here? */
		case T_INT:
			return row->gtable->table->append(row->id, key, value->v_int);
		case T_FLOAT:
			return row->gtable->table->append(row->id, key, value->v_float);
		case T_STRING:
			return row->gtable->table->append(row->id, key, value->v_string);
	}
	abort();
}

int toiletpp_row_remove_key(t_row * row, const char * key)
{
	return row->gtable->table->remove(row->id, key);
}

t_rowset * toiletpp_query(t_gtable * gtable, t_query * query)
{
	/* XXX */
	return NULL;
}

ssize_t toiletpp_count_query(t_gtable * gtable, t_query * query)
{
	/* XXX */
	return -1;
}

size_t toiletpp_rowset_size(t_rowset * rowset)
{
	return rowset->rows.size();
}

t_row_id toiletpp_rowset_row(t_rowset * rowset, size_t index)
{
	return rowset->rows[index];
}

void toiletpp_put_rowset(t_rowset * rowset)
{
	if(!--rowset->out_count)
		/* just IDs in rowsets, nothing to put() */
		delete rowset;
}
