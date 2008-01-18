/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

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
 * indices, respectively. The database-wide "rows" directory stores a tree of
 * subdirectories: each row ID (32-bit numbers for now) is written in
 * hexadecimal and each two digits form a subdirectory name. The last such
 * subdirectory contains a directory for each key in the row, and in these
 * directories are files containing the values for that key, one per file. */

/* Here is a map of the database directory structure:
 *
 * [db]/                                      The top level database
 * [db]/toilet-version                        Version of this database
 * [db]/toilet-id                             This database's ID
 * [db]/next-row                              Next row ID source value
 * [db]/[gt1]/                                A gtable directory
 * [db]/[gt1]/columns/                        The column specifiers
 * [db]/[gt1]/columns/[col1]                  A particular column
 * [db]/[gt1]/columns/[col2]...               More columns...
 * [db]/[gt1]/indices/[col1]/...              Index data for a column
 * [db]/[gt1]/indices/[col2]/...              More index data...
 * [db]/[gt2]/...                             More gtables...
 * [db]/rows/                                 The rows in all gtables
 * [db]/rows/XX/XX/XX/XX/                     A row directory
 * [db]/rows/XX/XX/XX/XX/gtable->             Symlink to the gtable
 * [db]/rows/XX/XX/XX/XX/[key1]/              A key in that row
 * [db]/rows/XX/XX/XX/XX/[key1]/0             A value of that key
 * [db]/rows/XX/XX/XX/XX/[key1]/1...          More values...
 * [db]/rows/XX/XX/XX/XX/[key2]/...           More keys...
 *
 * "toilet-version" is currently a single line file: "0"
 * "toilet-id" is a random 128-bit database identifier stored literally
 * "next-row" starts at 0 and increments for each row (more on this below)
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
	uint8_t id[ID_SIZE];
	t_row_id next = 0;
	
	r = mkdir(path, 0775);
	if(r < 0)
		return r;
	dir_fd = open(path, 0);
	if(dir_fd < 0)
		goto fail_open;
	
	version = fopenat(dir_fd, "toilet-version", "w");
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
	fd = openat(dir_fd, "toilet-id", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_1;
	r = write(fd, &id, sizeof(id));
	close(fd);
	if(r != sizeof(id))
		goto fail_id_2;
	
	fd = openat(dir_fd, "next-row", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_2;
	r = write(fd, &next, sizeof(next));
	close(fd);
	if(r != sizeof(next))
		goto fail_next;
	
	r = mkdirat(dir_fd, "rows", 0775);
	if(r < 0)
		goto fail_next;
	
	close(dir_fd);
	return 0;
	
fail_next:
	unlinkat(dir_fd, "next-row", 0);
fail_id_2:
	unlinkat(dir_fd, "toilet-id", 0);
fail_id_1:
	unlinkat(dir_fd, "toilet-version", 0);
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

toilet * toilet_open(const char * path, FILE * errors)
{
	FILE * version_file;
	char version_str[16];
	int dir_fd, id_fd;
	toilet * toilet;
	
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
	toilet->gtables = hash_map_create_str();
	if(!toilet->gtables)
		goto fail_hash_1;
	toilet->rows = hash_map_create();
	if(!toilet->rows)
		goto fail_hash_2;
	
	/* check the version */
	version_file = fopenat(dir_fd, "toilet-version", "r");
	if(!version_file)
		goto fail_fopen;
	fgets(version_str, sizeof(version_str), version_file);
	if(feof(version_file) || ferror(version_file))
		goto fail_fgets;
	if(strcmp(version_str, "0\n"))
		goto fail_fgets;
	
	/* get the database ID */
	id_fd = openat(dir_fd, "toilet-id", O_RDONLY);
	if(id_fd < 0)
		goto fail_fgets;
	if(read(id_fd, &toilet->id, sizeof(toilet->id)) != sizeof(toilet->id))
		goto fail_read_1;
	
	/* get the next row ID source value */
	toilet->row_fd = openat(dir_fd, "next-row", O_RDWR);
	if(toilet->row_fd < 0)
		goto fail_read_1;
	if(read(toilet->row_fd, &toilet->next_row, sizeof(toilet->next_row)) != sizeof(toilet->next_row))
		goto fail_read_2;
	
	close(id_fd);
	fclose(version_file);
	
	return toilet;
	
	/* error handling */
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
	free((void *) toilet->path);
fail_strdup:
	/* i.e., not a pay toilet */
	free(toilet);
fail_malloc:
	close(dir_fd);
	return NULL;
}

int toilet_close(toilet * toilet)
{
	/* XXX should be more stuff here */
	hash_map_destroy(toilet->rows);
	hash_map_destroy(toilet->gtables);
	free((void *) toilet->path);
	close(toilet->path_fd);
	close(toilet->row_fd);
	free(toilet);
	return 0;
}

/* gtables */

int toilet_new_gtable(toilet * toilet, const char * name)
{
	int r, dir_fd, id_fd;
	uint32_t data[2];
	
	/* already exists and in hash? */
	if(hash_map_find_val(toilet->gtables, name))
		return -EEXIST;
	if(strlen(name) > GTABLE_NAME_LENGTH)
		return -ENAMETOOLONG;
	if(!*name)
		return -EINVAL;
	
	/* will fail if the gtable already exists */
	if((r = mkdirat(toilet->path_fd, name, 0775)) < 0)
		return r;
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
	unlinkat(toilet->path_fd, name, AT_REMOVEDIR);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_gtable(t_gtable * gtable)
{
	/* XXX */
	return -ENOSYS;
}

static t_column * toilet_open_column(int dfd, const char * name)
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
	switch(column->type)
	{
		default:
			goto fail_read;
		case T_ID:
		case T_INT:
		case T_STRING:
		case T_BLOB:
			/* placate compiler */ ;
	}
	column->index = toilet_open_index(dfd, "../indices", name);
	if(!column->index)
		goto fail_read;
	
	close(fd);
	return column;
	
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

t_gtable * toilet_get_gtable(toilet * toilet, const char * name)
{
	int table_fd, column_fd;
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
	gtable->out_count = 1;
	
	table_fd = openat(toilet->path_fd, name, 0);
	if(table_fd < 0)
		goto fail_gtable;
	column_fd = openat(table_fd, "columns", 0);
	if(column_fd < 0)
		goto fail_column;
	
	dir = fdopendir(column_fd);
	if(!dir)
	{
		close(column_fd);
		goto fail_column;
	}
	while((ent = readdir(dir)))
	{
		t_column * column;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		if(!strcmp(ent->d_name, "id"))
			id++;
		column = toilet_open_column(column_fd, ent->d_name);
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

void toilet_put_gtable(toilet * toilet, t_gtable * gtable)
{
	int i;
	if(--gtable->out_count)
		return;
	hash_map_erase(toilet->gtables, gtable->name);
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

static int toilet_new_row_id(toilet * toilet, t_row_id * row)
{
	bf_ctx bfc;
	*row = toilet->next_row;
	lseek(toilet->row_fd, SEEK_SET, 0);
	if(write(toilet->row_fd, &toilet->next_row, sizeof(toilet->next_row)) != sizeof(toilet->next_row))
		return -1;
	toilet->next_row++;
	bf_setkey(&bfc, toilet->id, sizeof(toilet->id));
	*row = bf32_encipher(&bfc, *row);
	return 0;
}

static const char * row_formats[] = {
	"rows",
	"rows/%02x",
	"rows/%02x/%02x",
	"rows/%02x/%02x/%02x",
	"rows/%02x/%02x/%02x/%02x"
};
#define ROW_FORMATS (sizeof(row_formats) / sizeof(row_formats[0]))

int toilet_new_row(toilet * toilet, t_gtable * gtable, t_row_id * new_id)
{
	int i, r, row_fd;
	char row[] = "rows/xx/xx/xx/xx";
	union {
		t_row_id id;
		uint8_t bytes[sizeof(t_row_id)];
	} id;
	t_column * id_col;
	if((r = toilet_new_row_id(toilet, &id.id)) < 0)
		goto fail;
	for(i = 0; i < ROW_FORMATS; i++)
	{
		sprintf(row, row_formats[i], id.bytes[0], id.bytes[1], id.bytes[2], id.bytes[3]);
		row_fd = openat(toilet->path_fd, row, 0);
		if(row_fd < 0)
		{
			if((r = mkdirat(toilet->path_fd, row, 0775)) < 0)
				goto fail;
			row_fd = openat(toilet->path_fd, row, 0);
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
	
	i = openat(row_fd, "gtable", O_WRONLY | O_CREAT, 0664);
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
	r = toilet_index_add(id_col->index, id.id, T_ID, (t_value) id.id);
	if(r < 0)
		goto fail_unlink;
	*new_id = id.id;
	
	close(row_fd);
	return 0;
	
fail_unlink:
	unlinkat(row_fd, "gtable", 0);
fail_close:
	close(row_fd);
fail:
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_row(t_row * row)
{
	/* XXX */
	return -ENOSYS;
}

t_row * toilet_get_row(toilet * toilet, t_row_id row_id)
{
	int dir_fd, fd;
	ssize_t length;
	char row_name[] = "rows/xx/xx/xx/xx";
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
	
	fd = openat(dir_fd, "gtable", O_RDONLY);
	if(fd < 0)
		goto fail_open;
	length = read(fd, gtable_name, GTABLE_NAME_LENGTH);
	close(fd);
	if(length <= 0)
		goto fail_open;
	row_name[length] = 0;
	row->gtable = toilet_get_gtable(toilet, row_name);
	if(!row->gtable)
		goto fail_open;
	/* XXX: get column values */
	row->columns = NULL;
	row->row_path_fd = dir_fd;
	row->out_count = 1;
	if(hash_map_insert(toilet->rows, (void *) row_id, row) < 0)
		goto fail_hash;
	
	return row;
	
fail_hash:
	toilet_put_gtable(toilet, row->gtable);
fail_open:
	free(row);
fail:
	close(dir_fd);
	return NULL;
}

void toilet_put_row(toilet * toilet, t_row * row)
{
}

/* values */

t_value * toilet_row_value(t_row * row, const char * key, t_type type)
{
}

t_values * toilet_row_values(t_row * row, const char * key)
{
}

int toilet_row_set_value(t_row * row, const char * key, t_type type, t_value * value)
{
}

int toilet_row_remove_key(t_row * row, const char * key)
{
}

int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value)
{
}

int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value)
{
}

int toilet_row_remove_values(t_row * row, const char * key)
{
}

int toilet_row_remove_value(t_row * row, t_values * values, int index)
{
}

int toilet_row_update_value(t_row * row, t_values * values, int index, t_value * value)
{
}

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query)
{
}

int toilet_put_rowset(t_rowset * rowset)
{
	if(!--rowset->out_count)
	{
		/* just IDs in these structures */
		vector_destroy(rowset->rows);
		hash_set_destroy(rowset->ids);
		free(rowset);
	}
}
