/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "openat.h"
#include "toilet.h"
#include "hash_map.h"
#include "blowfish.h"
#include "index.h"

/* This initial implementation of Toilet stores databases using the file system.
 * Each gtable gets a subdirectory in the database directory, and has
 * subdirectories "columns" and "indices" that store the gtable metadata and
 * indices, respectively. The database-wide "=rows" directory stores a tree of
 * subdirectories: each row ID (32-bit numbers for now) is written in
 * hexadecimal and each two digits form a subdirectory name. The last such
 * subdirectory contains a directory for each key in the row, and in these
 * directories are files containing the values for that key, one per file. */

/* Here is a map of the database directory structure:
 *
 * [db]/                                      The top level database
 * [db]/=toilet-version                       Version of this database
 * [db]/=toilet-id                            This database's ID
 * [db]/=next-row                             Next row ID source value
 * [db]/[gt1]/                                A gtable directory
 * [db]/[gt1]/columns/                        The column specifiers
 * [db]/[gt1]/columns/[col1]                  A particular column
 * [db]/[gt1]/columns/[col2]...               More columns...
 * [db]/[gt1]/indices/[col1]/...              Index data for a column
 * [db]/[gt1]/indices/[col2]/...              More index data...
 * [db]/[gt2]/...                             More gtables...
 * [db]/=rows/                                The rows in all gtables
 * [db]/=rows/XX/XX/XX/XX/                    A row directory
 * [db]/=rows/XX/XX/XX/XX/=gtable             File storing gtable name
 * [db]/=rows/XX/XX/XX/XX/[key1]/             A key in that row
 * [db]/=rows/XX/XX/XX/XX/[key1]/0            A value of that key
 * [db]/=rows/XX/XX/XX/XX/[key1]/1...         More values...
 * [db]/=rows/XX/XX/XX/XX/[key2]/...          More keys...
 *
 * "=toilet-version" is currently a single line file: "0"
 * "=toilet-id" is a random 128-bit database identifier stored literally
 * "=next-row" starts at 0 and increments for each row (more on this below)
 * The column files [col1], [col2], etc. store 8 bytes: first, the number of
 *   rows that have that column key in this gtable (32 bits), and second, the
 *   column type (another 32 bits)
 * The key files [key1], [key2], etc. store values in a way dependent on the
 *   type: row IDs and integers as host endian data, and strings and blobs
 *   literally with no line termination or other changes
 *
 * We want row IDs to be unpredictable both to help the directories that store
 * them work better and to avoid programmers trying to interpret them or count
 * on their behavior. To do this, we put an increasing counter through a 32-bit
 * blowfish encryption and use the result as the row ID. The database ID is used
 * as the key. This gives us a random automorphism on the 32-bit integers. */

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
	fprintf(version, "0\n");
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

/* This function returns a toilet pointer. That is, a sign like this:
 * 
 * +------------------+
 * |                  |
 * |  Restrooms  -->  |
 * |                  |
 * +------------------+
 * 
 * The pointer part is the arrow, obviously. The rest of the sign constitutes
 * the type, and may actually indicate a subclass such as "men's room." */

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
	toilet = malloc(sizeof(*toilet));
	if(!toilet)
		goto fail_malloc;
	memset(&toilet->id, 0, sizeof(toilet->id));
	toilet->next_row = 0;
	toilet->path = strdup(path);
	if(!toilet->path)
		goto fail_strdup;
	toilet->path_fd = dir_fd;
	toilet->errors = errors ? errors : stderr;
	toilet->gtable_names = vector_create();
	if(!toilet->gtable_names)
		goto fail_vector;
	toilet->gtables = hash_map_create_str();
	if(!toilet->gtables)
		goto fail_hash_1;
	toilet->rows = hash_map_create();
	if(!toilet->rows)
		goto fail_hash_2;
	
	/* check the version */
	version_file = fopenat(dir_fd, "=toilet-version", "r");
	if(!version_file)
		goto fail_fopen;
	fgets(version_str, sizeof(version_str), version_file);
	if(feof(version_file) || ferror(version_file))
		goto fail_fgets;
	if(strcmp(version_str, "0\n"))
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
		if(vector_push_back(toilet->gtable_names, name) < 0)
		{
			free(name);
			goto fail_loop;
		}
	}
	closedir(gtable_list);
	
	close(id_fd);
	fclose(version_file);
	
	return toilet;
	
	/* error handling */
fail_loop:
	closedir(gtable_list);
fail_read_2:
	close(toilet->row_fd);
fail_read_1:
	close(id_fd);
fail_fgets:
	fclose(version_file);
fail_fopen:
	hash_map_destroy(toilet->rows);
fail_hash_2:
	hash_map_destroy(toilet->gtables);
fail_hash_1:
	vector_destroy(toilet->gtable_names);
fail_vector:
	free((void *) toilet->path);
fail_strdup:
	/* i.e., not a pay toilet */
	free(toilet);
fail_malloc:
	close(dir_fd);
	return NULL;
}

int toilet_close(t_toilet * toilet)
{
	/* XXX should be more stuff here */
	hash_map_destroy(toilet->rows);
	hash_map_destroy(toilet->gtables);
	while(!vector_empty(toilet->gtable_names))
	{
		char * name = vector_pop_back(toilet->gtable_names);
		free(name);
	}
	vector_destroy(toilet->gtable_names);
	free((void *) toilet->path);
	close(toilet->path_fd);
	close(toilet->row_fd);
	free(toilet);
	return 0;
}

/* gtables */

int toilet_new_gtable(t_toilet * toilet, const char * name)
{
	int r, dir_fd, id_fd;
	uint32_t data[2];
	char * name_copy;
	
	/* already exists and in hash? */
	if(hash_map_find_val(toilet->gtables, name))
		return -EEXIST;
	if(strlen(name) > GTABLE_NAME_LENGTH)
		return -ENAMETOOLONG;
	if(!*name || *name == '=')
		return -EINVAL;
	
	/* will fail if the gtable already exists */
	if((r = mkdirat(toilet->path_fd, name, 0775)) < 0)
		return r;
	name_copy = strdup(name);
	if(!name_copy)
	{
		r = -ENOMEM;
		goto fail_copy;
	}
	if((r = vector_push_back(toilet->gtable_names, name_copy)) < 0)
		goto fail_push;
	dir_fd = openat(toilet->path_fd, name, 0);
	if(dir_fd < 0)
		goto fail_open;
	if((r = mkdirat(dir_fd, "columns", 0775)) < 0)
		goto fail_inside_1;
	if((r = mkdirat(dir_fd, "indices", 0775)) < 0)
		goto fail_inside_2;
	if((r = mkdirat(dir_fd, "indices/id", 0775)) < 0)
		goto fail_inside_3;
	id_fd = openat(dir_fd, "columns/id", O_WRONLY | O_CREAT, 0664);
	if(id_fd < 0)
	{
		r = id_fd;
		goto fail_id_1;
	}
	data[0] = 0;
	data[1] = T_ID;
	r = write(id_fd, data, sizeof(data));
	close(id_fd);
	if(r != sizeof(data))
		goto fail_id_2;
	
	r = toilet_index_init(dir_fd, "indices/id", T_ID);
	if(r < 0)
		goto fail_id_2;
	
	close(dir_fd);
	return 0;
	
fail_id_2:
	unlinkat(dir_fd, "columns/id", 0);
fail_id_1:
	unlinkat(dir_fd, "indices/id", AT_REMOVEDIR);
fail_inside_3:
	unlinkat(dir_fd, "indices", AT_REMOVEDIR);
fail_inside_2:
	unlinkat(dir_fd, "columns", AT_REMOVEDIR);
fail_inside_1:
	close(dir_fd);
fail_open:
	vector_pop_back(toilet->gtable_names);
fail_push:
	free(name_copy);
fail_copy:
	unlinkat(toilet->path_fd, name, AT_REMOVEDIR);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_gtable(t_gtable * gtable)
{
	return -ENOSYS;
}

static t_column * toilet_open_column(uint8_t * id, int dfd, const char * name)
{
	int fd;
	uint32_t data[2];
	t_column * column = malloc(sizeof(*column));
	if(!column)
		return NULL;
	column->name = strdup(name);
	if(!column->name)
		goto fail_name;
	
	fd = openat(dfd, name, O_RDONLY);
	if(fd < 0)
		goto fail_open;
	if(read(fd, data, sizeof(data)) != sizeof(data))
		goto fail_read;
	column->type = data[1];
	column->count = data[0];
	column->flags = 0;
	switch(column->type)
	{
		/* force a compiler warning if we add new types */
		case T_ID:
		case T_INT:
		case T_STRING:
		case T_BLOB:
			column->index = toilet_open_index(id, dfd, "../indices", name);
			if(!column->index)
				goto fail_read;
			
			close(fd);
			return column;
	}
	
fail_read:
	close(fd);
fail_open:
	free((char *) column->name);
fail_name:
	free(column);
	return NULL;
}

static void toilet_close_column(t_column * column)
{
	toilet_close_index(column->index);
	free((char *) column->name);
	free(column);
}

static int toilet_column_update_count(t_gtable * gtable, t_column * column, int delta)
{
	uint32_t data;
	int r, fd, column_fd, gtable_fd;
	r = gtable_fd = openat(gtable->toilet->path_fd, gtable->name, 0);
	if(r < 0)
		goto fail;
	r = column_fd = openat(gtable_fd, "columns", 0);
	if(r < 0)
		goto fail_gtable;
	r = fd = openat(column_fd, column->name, O_WRONLY);
	if(r < 0)
		goto fail_column;
	data = column->count + delta;
	r = write(fd, &data, sizeof(data));
	if(r != sizeof(data))
		goto fail_fd;
	close(fd);
	close(column_fd);
	close(gtable_fd);
	column->count += delta;
	return 0;
	
fail_fd:
	close(fd);
fail_column:
	close(column_fd);
fail_gtable:
	close(gtable_fd);
fail:
	fprintf(gtable->toilet->errors, "%s(): failed to update column count for '%s'\n", __FUNCTION__, column->name);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

static int toilet_column_new(t_gtable * gtable, const char * name, t_type type)
{
	uint32_t data[2];
	t_column * column;
	int r, column_fd, index_fd = -1, fd, gtable_fd;
	if(!*name || *name == '=')
		return -EINVAL;
	gtable_fd = openat(gtable->toilet->path_fd, gtable->name, 0);
	if(gtable_fd < 0)
		return gtable_fd;
	column_fd = openat(gtable_fd, "columns", 0);
	if(column_fd < 0)
	{
		close(gtable_fd);
		return column_fd;
	}
	if(type != T_BLOB)
	{
		index_fd = openat(gtable_fd, "indices", 0);
		if(index_fd < 0)
		{
			close(gtable_fd);
			r = index_fd;
			goto fail;
		}
	}
	close(gtable_fd);
	
	fd = openat(column_fd, name, O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
	{
		r = fd;
		goto fail_index;
	}
	data[0] = 0;
	data[1] = type;
	if(write(fd, data, sizeof(data)) != sizeof(data))
	{
		close(fd);
		r = -1;
		goto fail_unlink;
	}
	close(fd);
	
	if(type != T_BLOB)
	{
		r = mkdirat(index_fd, name, 0775);
		if(r < 0)
			goto fail_unlink;
		r = toilet_index_init(index_fd, name, type);
		if(r < 0)
		{
			unlinkat(index_fd, name, AT_REMOVEDIR);
			goto fail_unlink;
		}
		close(index_fd);
	}
	/* now it's constructed, but we haven't loaded it into memory */
	
	column = toilet_open_column(gtable->toilet->id, column_fd, name);
	if(!column)
	{
		fprintf(gtable->toilet->errors, "%s(): can't load new column '%s'\n", __FUNCTION__, name);
		r = -1;
		goto fail;
	}
	r = vector_push_back(gtable->columns, column);
	if(r < 0)
	{
		fprintf(gtable->toilet->errors, "%s(): can't push new column '%s'\n", __FUNCTION__, name);
		toilet_close_column(column);
		goto fail;
	}
	r = hash_map_insert(gtable->column_map, column->name, column);
	if(r < 0)
	{
		fprintf(gtable->toilet->errors, "%s(): can't insert new column '%s'\n", __FUNCTION__, name);
		vector_pop_back(gtable->columns);
		toilet_close_column(column);
		goto fail;
	}
	
	close(column_fd);
	
	return 0;
	
fail_unlink:
	unlinkat(column_fd, name, 0);
fail_index:
	if(type != T_BLOB)
		close(index_fd);
fail:
	close(column_fd);
	return r;
}

static int toilet_column_drop(t_gtable * gtable, t_column * column)
{
	int column_fd, index_fd = -1, i;
	int gtable_fd = openat(gtable->toilet->path_fd, gtable->name, 0);
	if(gtable_fd < 0)
		return gtable_fd;
	column_fd = openat(gtable_fd, "columns", 0);
	if(column_fd < 0)
	{
		close(gtable_fd);
		return column_fd;
	}
	if(column->type != T_BLOB)
	{
		index_fd = openat(gtable_fd, "indices", 0);
		if(index_fd < 0)
		{
			close(gtable_fd);
			close(column_fd);
			return index_fd;
		}
	}
	close(gtable_fd);
	
	/* XXX add error handling to this function (starting here-ish) */
	unlinkat(column_fd, column->name, 0);
	close(column_fd);
	
	if(column->type != T_BLOB)
	{
		toilet_index_drop(index_fd, column->name);
		unlinkat(index_fd, column->name, AT_REMOVEDIR);
		close(index_fd);
	}
	
	hash_map_erase(gtable->column_map, column->name);
	for(i = 0; i < vector_size(gtable->columns); i++)
		if(vector_elt(gtable->columns, i) == column)
			break;
	assert(i < vector_size(gtable->columns));
	vector_erase(gtable->columns, i);
	toilet_close_column(column);
	
	return 0;
}

t_gtable * toilet_get_gtable(t_toilet * toilet, const char * name)
{
	int table_fd, column_fd, copy;
	t_gtable * gtable;
	struct dirent * ent;
	DIR * dir;
	int id = 0;
	
	gtable = (t_gtable *) hash_map_find_val(toilet->gtables, name);
	if(gtable)
	{
		gtable->out_count++;
		return gtable;
	}
	
	gtable = malloc(sizeof(*gtable));
	if(!gtable)
		return NULL;
	gtable->name = strdup(name);
	if(!gtable->name)
		goto fail_name;
	gtable->columns = vector_create();
	if(!gtable->columns)
		goto fail_vector;
	gtable->column_map = hash_map_create_str();
	if(!gtable->column_map)
		goto fail_map;
	gtable->toilet = toilet;
	gtable->out_count = 1;
	
	table_fd = openat(toilet->path_fd, name, 0);
	if(table_fd < 0)
		goto fail_gtable;
	column_fd = openat(table_fd, "columns", 0);
	if(column_fd < 0)
		goto fail_column;
	
	/* don't count on the fd passed to fdopendir() sticking around */
	copy = dup(column_fd);
	if(copy < 0)
		goto fail_dup;
	dir = fdopendir(copy);
	if(!dir)
	{
		close(copy);
		goto fail_column;
	}
	while((ent = readdir(dir)))
	{
		t_column * column;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(!strcmp(ent->d_name, "id"))
			id++;
		column = toilet_open_column(toilet->id, column_fd, ent->d_name);
		if(!column)
			goto fail_columns;
		if(vector_push_back(gtable->columns, column) < 0)
		{
			toilet_close_column(column);
			goto fail_columns;
		}
		if(hash_map_insert(gtable->column_map, column->name, column) < 0)
			goto fail_columns;
	}
	/* there must be an ID column */
	if(id != 1)
		goto fail_columns;
	if(hash_map_insert(toilet->gtables, gtable->name, gtable) < 0)
		goto fail_columns;
	
	closedir(dir);
	close(column_fd);
	close(table_fd);
	return gtable;
	
fail_columns:
	for(id = vector_size(gtable->columns) - 1; id >= 0; id--)
	{
		t_column * column = (t_column *) vector_elt(gtable->columns, id);
		hash_map_erase(gtable->column_map, column->name);
		toilet_close_column(column);
	}
	closedir(dir);
fail_dup:
	close(column_fd);
fail_column:
	close(table_fd);
fail_gtable:
	hash_map_destroy(gtable->column_map);
fail_map:
	vector_destroy(gtable->columns);
fail_vector:
	free((char *) gtable->name);
fail_name:
	free(gtable);
	return NULL;
}

void toilet_put_gtable(t_gtable * gtable)
{
	int i;
	if(--gtable->out_count)
		return;
	hash_map_erase(gtable->toilet->gtables, gtable->name);
	hash_map_destroy(gtable->column_map);
	for(i = vector_size(gtable->columns) - 1; i >= 0; i--)
		toilet_close_column((t_column *) vector_elt(gtable->columns, i));
	vector_destroy(gtable->columns);
	free((char *) gtable->name);
	free(gtable);
}

/* columns */

t_column * toilet_gtable_get_column(t_gtable * gtable, const char * name)
{
	return (t_column *) hash_map_find_val(gtable->column_map, name);
}

int toilet_column_is_multi(t_column * column)
{
	return column->flags & T_COLUMN_MULTI;
}

int toilet_column_set_multi(t_column * column, int multi)
{
	return -ENOSYS;
}

/* rows */

static int toilet_new_row_id(t_toilet * toilet, t_row_id * row)
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

static const char * row_formats[] = {
	"=rows",
	"=rows/%02x",
	"=rows/%02x/%02x",
	"=rows/%02x/%02x/%02x",
	"=rows/%02x/%02x/%02x/%02x"
};
#define ROW_FORMATS (sizeof(row_formats) / sizeof(row_formats[0]))

int toilet_new_row(t_gtable * gtable, t_row_id * new_id)
{
	int i, r, row_fd;
	char row[] = "=rows/xx/xx/xx/xx";
	union {
		t_row_id id;
		uint8_t bytes[sizeof(t_row_id)];
	} id;
	t_value id_value;
	t_column * id_col;
	if((r = toilet_new_row_id(gtable->toilet, &id.id)) < 0)
		goto fail;
	for(i = 0; i < ROW_FORMATS; i++)
	{
		sprintf(row, row_formats[i], id.bytes[0], id.bytes[1], id.bytes[2], id.bytes[3]);
		row_fd = openat(gtable->toilet->path_fd, row, 0);
		if(row_fd < 0)
		{
			if((r = mkdirat(gtable->toilet->path_fd, row, 0775)) < 0)
				goto fail;
			row_fd = openat(gtable->toilet->path_fd, row, 0);
			if(row_fd < 0)
			{
				r = row_fd;
				goto fail;
			}
		}
		else
			/* make sure the row doesn't already exist */
			assert(i != ROW_FORMATS - 1);
		if(i < ROW_FORMATS - 1)
			close(row_fd);
	}
	
	i = openat(row_fd, "=gtable", O_WRONLY | O_CREAT, 0664);
	if(i < 0)
	{
		r = i;
		goto fail_close;
	}
	r = write(i, gtable->name, strlen(gtable->name));
	close(i);
	if(r != strlen(gtable->name))
		goto fail_unlink;
	
	id_col = hash_map_find_val(gtable->column_map, "id");
	if(!id_col)
		goto fail_unlink;
	id_value.v_id = id.id;
	r = toilet_index_add(id_col->index, id.id, T_ID, &id_value);
	if(r < 0)
		goto fail_unlink;
	r = toilet_column_update_count(gtable, id_col, 1);
	if(r < 0)
		goto fail_column;
	*new_id = id.id;
	
	close(row_fd);
	return 0;
	
fail_column:
	toilet_index_remove(id_col->index, id.id, T_ID, &id_value);
fail_unlink:
	unlinkat(row_fd, "=gtable", 0);
fail_close:
	close(row_fd);
fail:
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_row(t_row * row)
{
	/* TODO: more efficient all-at-once implementation? */
	union {
		t_row_id id;
		uint8_t bytes[sizeof(t_row_id)];
	} id;
	DIR * dir;
	t_value id_value;
	t_column * column;
	struct dirent * ent;
	t_gtable * gtable = row->gtable;
	char row_dir[] = "=rows/xx/xx/xx/xx";
	int i, r, copy = dup(row->row_path_fd);
	if(copy < 0)
		return copy;
	dir = fdopendir(copy);
	if(!dir)
	{
		close(copy);
		return -1;
	}
	while((ent = readdir(dir)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(ent->d_name[0] == '=')
			continue;
		column = hash_map_find_val(gtable->column_map, ent->d_name);
		if(!column)
		{
			/* TODO: this will cause us problems; we need to just delete it in this case */
			fprintf(gtable->toilet->errors, "%s(): ignoring unknown column key '%s' in row 0x" ROW_FORMAT "\n", __FUNCTION__, ent->d_name, row->id);
			continue;
		}
		if(column->flags & T_COLUMN_MULTI)
			r = toilet_row_remove_values(row, ent->d_name);
		else
			r = toilet_row_remove_key(row, ent->d_name);
		if(r < 0)
		{
			fprintf(gtable->toilet->errors, "%s(): failed to remove column '%s' in row 0x" ROW_FORMAT "\n", __FUNCTION__, ent->d_name, row->id);
			break;
		}
	}
	if(ent)
		return -1;
	closedir(dir);
	
	id.id = row->id;
	id_value.v_id = row->id;
	column = hash_map_find_val(gtable->column_map, "id");
	r = toilet_column_update_count(gtable, column, -1);
	if(r < 0)
		return r;
	r = toilet_index_remove(column->index, row->id, T_ID, &id_value);
	if(r < 0)
	{
		fprintf(gtable->toilet->errors, "%s(): failed to remove row 0x" ROW_FORMAT " from ID index\n", __FUNCTION__, row->id);
		return r;
	}
	r = unlinkat(row->row_path_fd, "=gtable", 0);
	if(r < 0)
	{
		fprintf(gtable->toilet->errors, "%s(): failed to remove gtable link in row 0x" ROW_FORMAT "\n", __FUNCTION__, row->id);
		return r;
	}
	/* TODO: use counts are kind of hacky right now */
	assert(row->out_count == 1);
	toilet_put_row(row);
	
	/* remove row dir */
	for(i = ROW_FORMATS - 1; i >= 0; i--)
	{
		sprintf(row_dir, row_formats[i], id.bytes[0], id.bytes[1], id.bytes[2], id.bytes[3]);
		r = unlinkat(gtable->toilet->path_fd, row_dir, AT_REMOVEDIR);
		if(r < 0)
			break;
	}
	if(i == ROW_FORMATS - 1)
		fprintf(gtable->toilet->errors, "%s(): could not remove row 0x" ROW_FORMAT "\n", __FUNCTION__, id.id);
	
	return 0;
}

static void toilet_free_values(t_values * values)
{
	size_t j, count = vector_size(values->values);
	for(j = 0; j < count; j++)
	{
		t_value * value = (t_value *) vector_elt(values->values, j);
		if(values->type == T_BLOB)
			free(value->v_blob.data);
		free(value);
	}
	vector_destroy(values->values);
	free(values);
}

static void toilet_depopulate_row(t_row * row)
{
	size_t i, size = vector_size(row->gtable->columns);
	for(i = 0; i < size; i++)
	{
		t_column * column = (t_column *) vector_elt(row->gtable->columns, i);
		t_values * values = (t_values *) hash_map_find_val(row->columns, column->name);
		if(values)
			toilet_free_values(values);
	}
}

static int toilet_populate_row(t_row * row)
{
	DIR * dir;
	DIR * sub;
	t_column * column;
	t_values * values;
	struct dirent * ent;
	struct dirent * sub_ent;
	t_value * value = NULL;
	int r, fd, sub_fd, copy = dup(row->row_path_fd);
	if(copy < 0)
		return copy;
	dir = fdopendir(copy);
	if(!dir)
	{
		r = -errno;
		close(copy);
		goto fail;
	}
	while((ent = readdir(dir)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(ent->d_name[0] == '=')
			continue;
		column = hash_map_find_val(row->gtable->column_map, ent->d_name);
		if(!column)
		{
			fprintf(row->gtable->toilet->errors, "%s(): ignoring unknown column key '%s' in row 0x" ROW_FORMAT "\n", __FUNCTION__, ent->d_name, row->id);
			continue;
		}
		r = -ENOMEM;
		values = malloc(sizeof(*values));
		if(!values)
			goto fail_values;
		values->type = column->type;
		values->values = vector_create();
		if(!values->values)
			goto fail_vector;
		sub_fd = openat(row->row_path_fd, ent->d_name, 0);
		if(sub_fd < 0)
		{
			r = sub_fd;
			goto fail_openat;
		}
		copy = dup(sub_fd);
		if(copy < 0)
		{
			r = copy;
			goto fail_opendir;
		}
		sub = fdopendir(copy);
		if(!sub)
		{
			r = -errno;
			close(copy);
			goto fail_opendir;
		}
		while((sub_ent = readdir(sub)))
		{
			struct stat stat;
			if(!strcmp(sub_ent->d_name, ".") || !strcmp(sub_ent->d_name, ".."))
				continue;
			fd = openat(sub_fd, sub_ent->d_name, O_RDONLY);
			if(fd < 0)
			{
				r = fd;
				goto fail_insert;
			}
			if(values->type != T_STRING)
			{
				value = malloc(sizeof(*value));
				if(!value)
					goto fail_malloc;
			}
			switch(values->type)
			{
				case T_ID:
					r = read(fd, &value->v_id, sizeof(value->v_id));
					if(r != sizeof(value->v_id))
						goto fail_read;
					break;
				case T_INT:
					r = read(fd, &value->v_int, sizeof(value->v_int));
					if(r != sizeof(value->v_int))
						goto fail_read;
					break;
				case T_STRING:
					r = fstat(fd, &stat);
					if(r < 0)
						goto fail_malloc;
					value = malloc(stat.st_size + 1);
					if(!value)
						goto fail_malloc;
					((char *) value)[stat.st_size] = 0;
					r = read(fd, value, stat.st_size);
					if(r != stat.st_size)
						goto fail_read;
					break;
				case T_BLOB:
					r = fstat(fd, &stat);
					if(r < 0)
						goto fail_read;
					value->v_blob.length = stat.st_size;
					value->v_blob.data = malloc(stat.st_size);
					if(!value->v_blob.data)
						goto fail_read;
					r = read(fd, value->v_blob.data, stat.st_size);
					if(r != stat.st_size)
						goto fail_push;
					break;
			}
			r = vector_push_back(values->values, value);
			if(r < 0)
				goto fail_push;
			close(fd);
		}
		r = hash_map_insert(row->columns, column->name, values);
		if(r < 0)
			goto fail_insert;
		closedir(sub);
		close(sub_fd);
	}
	closedir(dir);
	return 0;
	
fail_push:
	if(values->type == T_BLOB)
		free(value->v_blob.data);
fail_read:
	free(value);
fail_malloc:
	close(fd);
fail_insert:
	toilet_free_values(values);
	closedir(sub);
fail_opendir:
	close(sub_fd);
fail_openat:
	vector_destroy(values->values);
fail_vector:
	free(values);
fail_values:
	closedir(dir);
	toilet_depopulate_row(row);
fail:
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

t_row * toilet_get_row(t_toilet * toilet, t_row_id row_id)
{
	ssize_t length;
	int dir_fd, fd;
	char row_name[] = "=rows/xx/xx/xx/xx";
	char gtable_name[GTABLE_NAME_LENGTH + 1];
	union {
		t_row_id id;
		uint8_t bytes[sizeof(t_row_id)];
	} id;
	t_row * row;
	id.id = row_id;
	sprintf(row_name, row_formats[ROW_FORMATS - 1], id.bytes[0], id.bytes[1], id.bytes[2], id.bytes[3]);
	
	dir_fd = openat(toilet->path_fd, row_name, 0);
	if(dir_fd < 0)
		return NULL;
	
	row = malloc(sizeof(*row));
	if(!row)
		goto fail;
	row->id = row_id;
	
	fd = openat(dir_fd, "=gtable", O_RDONLY);
	if(fd < 0)
		goto fail_open;
	length = read(fd, gtable_name, GTABLE_NAME_LENGTH);
	close(fd);
	if(length <= 0)
		goto fail_open;
	gtable_name[length] = 0;
	row->gtable = toilet_get_gtable(toilet, gtable_name);
	if(!row->gtable)
		goto fail_open;
	row->row_path_fd = dir_fd;
	row->columns = hash_map_create_str();
	if(!row->columns)
		goto fail_columns;
	if(toilet_populate_row(row) < 0)
		goto fail_populate;
	row->out_count = 1;
	if(hash_map_insert(toilet->rows, (void *) row_id, row) < 0)
		goto fail_hash;
	
	return row;
	
fail_hash:
	toilet_depopulate_row(row);
fail_populate:
	hash_map_destroy(row->columns);
fail_columns:
	toilet_put_gtable(row->gtable);
fail_open:
	free(row);
fail:
	close(dir_fd);
	return NULL;
}

void toilet_put_row(t_row * row)
{
	if(!--row->out_count)
	{
		hash_map_erase(row->gtable->toilet->rows, (void *) row->id);
		toilet_depopulate_row(row);
		hash_map_destroy(row->columns);
		toilet_put_gtable(row->gtable);
		close(row->row_path_fd);
		free(row);
	}
}

/* values */

t_value * toilet_row_value(t_row * row, const char * key, t_type type)
{
	t_values * values;
	t_column * column = hash_map_find_val(row->gtable->column_map, key);
	if(column && (column->flags & T_COLUMN_MULTI))
	{
		fprintf(row->gtable->toilet->errors, "%s(): request for single value from multi-column '%s'\n", __FUNCTION__, key);
		return NULL;
	}
	values = hash_map_find_val(row->columns, key);
	if(!values)
		return NULL;
	assert(vector_size(values->values) == 1);
	if(values->type != type)
	{
		fprintf(row->gtable->toilet->errors, "%s(): request for type %d value from column '%s' of type %d\n", __FUNCTION__, type, key, values->type);
		return NULL;
	}
	return (t_value *) vector_elt(values->values, 0);
}

t_values * toilet_row_values(t_row * row, const char * key)
{
	return hash_map_find_val(row->columns, key);
}

static t_value * toilet_copy_value(t_type type, t_value * value)
{
	t_value * copy = NULL;
	if(type != T_STRING)
	{
		copy = malloc(sizeof(*copy));
		if(!copy)
			return NULL;
	}
	switch(type)
	{
		case T_ID:
			copy->v_id = value->v_id;
			return copy;
		case T_INT:
			copy->v_int = value->v_int;
			return copy;
		case T_STRING:
			return (t_value *) strdup(value->v_string);
		case T_BLOB:
			copy->v_blob.data = malloc(value->v_blob.length);
			if(copy->v_blob.data)
			{
				copy->v_blob.length = value->v_blob.length;
				memcpy(copy->v_blob.data, value->v_blob.data, value->v_blob.length);
				return copy;
			}
	}
	free(copy);
	return NULL;
}

static int toilet_write_value(int fd, t_type type, t_value * value)
{
	size_t size = 0;
	ssize_t result = -1;
	switch(type)
	{
		case T_ID:
			size = sizeof(value->v_id);
			result = write(fd, &value->v_id, size);
			break;
		case T_INT:
			size = sizeof(value->v_int);
			result = write(fd, &value->v_int, size);
			break;
		case T_STRING:
			size = strlen(value->v_string);
			result = write(fd, value, size);
			break;
		case T_BLOB:
			size = value->v_blob.length;
			result = write(fd, value->v_blob.data, size);
			break;
	}
	return (size == result) ? 0 : -1;
}

int toilet_row_set_value(t_row * row, const char * key, t_type type, t_value * value)
{
	int r, dir_fd, value_fd;
	t_column * column = hash_map_find_val(row->gtable->column_map, key);
	t_values * values = hash_map_find_val(row->columns, key);
	if(!strcmp(key, "id"))
	{
		fprintf(row->gtable->toilet->errors, "%s(): attempt to set type %d value in ID column\n", __FUNCTION__, type);
		return -EINVAL;
	}
	if(column)
	{
		if(column->flags & T_COLUMN_MULTI)
		{
			fprintf(row->gtable->toilet->errors, "%s(): attempt to set single value in multi-column '%s'\n", __FUNCTION__, key);
			return -EINVAL;
		}
		if(type != column->type)
		{
			fprintf(row->gtable->toilet->errors, "%s(): attempt to set type %d value in column '%s' of type %d\n", __FUNCTION__, type, key, column->type);
			return -EINVAL;
		}
	}
	else
	{
		r = toilet_column_new(row->gtable, key, type);
		if(r < 0)
			return r;
		column = hash_map_find_val(row->gtable->column_map, key);
		assert(column);
	}
	/* XXX add error handling to this function (starting here-ish) */
	value = toilet_copy_value(type, value);
	if(values)
	{
		if(type != T_BLOB)
			toilet_index_change(column->index, row->id, type, (t_value *) vector_elt(values->values, 0), value);
		toilet_free_values(values);
		hash_map_erase(row->columns, key);
		dir_fd = openat(row->row_path_fd, key, 0);
		value_fd = openat(dir_fd, "0", O_WRONLY | O_TRUNC);
	}
	else
	{
		toilet_column_update_count(row->gtable, column, 1);
		if(type != T_BLOB)
			toilet_index_add(column->index, row->id, type, value);
		mkdirat(row->row_path_fd, key, 0775);
		dir_fd = openat(row->row_path_fd, key, 0);
		value_fd = openat(dir_fd, "0", O_WRONLY | O_CREAT, 0664);
	}
	close(dir_fd);
	toilet_write_value(value_fd, type, value);
	close(value_fd);
	values = malloc(sizeof(*values));
	values->type = type;
	values->values = vector_create();
	vector_push_back(values->values, value);
	hash_map_insert(row->columns, column->name, values);
	return 0;
}

int toilet_row_remove_key(t_row * row, const char * key)
{
	int dir_fd;
	t_column * column = hash_map_find_val(row->gtable->column_map, key);
	t_values * values = hash_map_find_val(row->columns, key);
	if(!strcmp(key, "id"))
	{
		fprintf(row->gtable->toilet->errors, "%s(): attempt to remove ID column\n", __FUNCTION__);
		return -EINVAL;
	}
	if(!column)
		/* -ENOENT? */
		return 0;
	if(column->flags & T_COLUMN_MULTI)
	{
		fprintf(row->gtable->toilet->errors, "%s(): attempt to remove single value in multi-column '%s'\n", __FUNCTION__, key);
		return -EINVAL;
	}
	if(!values)
		/* -ENOENT? */
		return 0;
	/* XXX add error handling to this function (starting here-ish) */
	toilet_index_remove(column->index, row->id, column->type, (t_value *) vector_elt(values->values, 0));
	toilet_free_values(values);
	hash_map_erase(row->columns, key);
	if(column->count == 1)
		/* column is gone now */
		toilet_column_drop(row->gtable, column);
	else
		toilet_column_update_count(row->gtable, column, -1);
	dir_fd = openat(row->row_path_fd, key, 0);
	unlinkat(dir_fd, "0", 0);
	close(dir_fd);
	unlinkat(row->row_path_fd, key, AT_REMOVEDIR);
	return 0;
}

int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value)
{
	/* multicolumns */
	return -ENOSYS;
}

int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value)
{
	/* TODO: more efficient, atomic */
	int r = toilet_row_remove_values(row, key);
	if(r < 0)
		return r;
	return toilet_row_append_value(row, key, type, value);
}

int toilet_row_remove_values(t_row * row, const char * key)
{
	/* multicolumns */
	return -ENOSYS;
}

int toilet_row_remove_value(t_row * row, t_values * values, int index)
{
	/* multicolumns */
	return -ENOSYS;
}

int toilet_row_update_value(t_row * row, t_values * values, int index, t_value * value)
{
	/* multicolumns */
	return -ENOSYS;
}

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query)
{
	t_column * column = hash_map_find_val(gtable->column_map, query->name);
	if(!column)
		return NULL;
	if(!column->index)
	{
		fprintf(gtable->toilet->errors, "%s(): query over non-indexed column '%s' of type %d\n", __FUNCTION__, column->name, column->type);
		return NULL;
	}
	return toilet_index_find(column->index, query->type, query->value);
}

void toilet_put_rowset(t_rowset * rowset)
{
	if(!--rowset->out_count)
	{
		/* just IDs in these structures */
		vector_destroy(rowset->rows);
		hash_set_destroy(rowset->ids);
		free(rowset);
	}
}
