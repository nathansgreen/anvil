#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>

#include "toilet.h"
#include "hash_map.h"
#include "blowfish.h"
#include "diskhash.h"

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
	int r, cwd_fd, fd;
	FILE * version;
	uint8_t id[ID_SIZE];
	t_row_id next = 0;
	
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return cwd_fd;
	r = mkdir(path, 0775);
	if(r < 0)
		goto fail_mkdir;
	r = chdir(path);
	if(r < 0)
		goto fail_chdir;
	
	version = fopen("toilet-version", "w");
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
	fd = open("toilet-id", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_1;
	r = write(fd, &id, sizeof(id));
	close(fd);
	if(r != sizeof(id))
		goto fail_id_2;
	
	fd = open("next-row", O_WRONLY | O_CREAT, 0664);
	if(fd < 0)
		goto fail_id_2;
	r = write(fd, &next, sizeof(next));
	close(fd);
	if(r != sizeof(next))
		goto fail_next;
	
	r = mkdir("rows", 0775);
	if(r < 0)
		goto fail_next;
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return 0;
	
fail_next:
	unlink("next-row");
fail_id_2:
	unlink("toilet-id");
fail_id_1:
	unlink("toilet-version");
fail_version:
	fchdir(cwd_fd);
fail_chdir:
	rmdir(path);
fail_mkdir:
	close(cwd_fd);
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

toilet * toilet_open(const char * path)
{
	FILE * version_file;
	char version_str[16];
	int path_fd, cwd_fd, id_fd;
	toilet * toilet;
	
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return NULL;
	if(chdir(path) < 0)
		goto close_1;
	path_fd = open(".", 0);
	if(path_fd < 0)
		goto cwd;
	
	/* allocate and initialize the structure */
	toilet = malloc(sizeof(*toilet));
	if(!toilet)
		goto close_2;
	memset(&toilet->id, 0, sizeof(toilet->id));
	toilet->next_row = 0;
	toilet->path = strdup(path);
	if(!toilet->path)
		goto free_1;
	toilet->path_fd = path_fd;
	toilet->gtables = hash_map_create_str();
	if(!toilet->gtables)
		goto free_2;
	toilet->rows = hash_map_create();
	if(!toilet->rows)
		goto destroy_1;
	
	/* check the version */
	version_file = fopen("toilet-version", "r");
	if(!version_file)
		goto destroy_2;
	fgets(version_str, sizeof(version_str), version_file);
	if(feof(version_file) || ferror(version_file))
		goto fclose;
	if(strcmp(version_str, "0\n"))
		goto fclose;
	
	/* get the database ID */
	id_fd = open("toilet-id", O_RDONLY);
	if(id_fd < 0)
		goto fclose;
	if(read(id_fd, &toilet->id, sizeof(toilet->id)) != sizeof(toilet->id))
		goto close_3;
	
	/* get the next row ID source value */
	toilet->row_fd = open("next-row", O_RDWR);
	if(toilet->row_fd < 0)
		goto close_3;
	if(read(toilet->row_fd, &toilet->next_row, sizeof(toilet->next_row)) != sizeof(toilet->next_row))
		goto close_4;
	
	close(id_fd);
	fclose(version_file);
	fchdir(cwd_fd);
	
	return toilet;
	
	/* error handling */
close_4:
	close(toilet->row_fd);
close_3:
	close(id_fd);
fclose:
	fclose(version_file);
destroy_2:
	hash_map_destroy(toilet->rows);
destroy_1:
	hash_map_destroy(toilet->gtables);
free_2:
	free((void *) toilet->path);
free_1:
	/* i.e., not a pay toilet */
	free(toilet);
close_2:
	close(path_fd);
cwd:
	fchdir(cwd_fd);
close_1:
	close(cwd_fd);
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
	int r, cwd_fd, id_fd;
	uint32_t data[2];
	
	/* already exists and in hash? */
	if(hash_map_find_val(toilet->gtables, name))
		return -EEXIST;
	
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return cwd_fd;
	fchdir(toilet->path_fd);
	
	/* will fail if the gtable already exists */
	if((r = mkdir(name, 0775)) < 0)
		goto fail_create;
	if((r = chdir(name)) < 0)
		goto fail_chdir;
	if((r = mkdir("columns", 0775)) < 0)
		goto fail_inside_1;
	if((r = mkdir("indices", 0775)) < 0)
		goto fail_inside_2;
	if((r = mkdir("indices/id", 0775)) < 0)
		goto fail_inside_3;
	id_fd = open("columns/id", O_WRONLY | O_CREAT, 0664);
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
	
	r = diskhash_init("indices/id/dh", DH_U32, DH_NONE);
	if(r < 0)
		goto fail_id_2;
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return 0;
	
fail_id_2:
	unlink("columns/id");
fail_id_1:
	rmdir("indices/id");
fail_inside_3:
	rmdir("indices");
fail_inside_2:
	rmdir("columns");
fail_inside_1:
	chdir("..");
fail_chdir:
	rmdir(name);
fail_create:
	fchdir(cwd_fd);
	close(cwd_fd);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_gtable(t_gtable * gtable)
{
	/* XXX */
	return -ENOSYS;
}

/* assumes we're already in the gtable/columns directory */
static t_index * toilet_get_index(const char * name)
{
	t_index * index;
	int cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return NULL;
	index = malloc(sizeof(*index));
	if(!index)
		goto fail_malloc;
	index->type = I_NONE;
	if(chdir("../indices") < 0)
		goto fail_chdir;
	if(chdir(name) < 0)
		goto fail_chdir;
	index->hash.disk = diskhash_open("dh");
	if(index->hash.disk)
	{
		index->type |= I_HASH;
		index->hash.cache = hash_map_create();
		if(!index->hash.cache)
			goto fail_hash;
	}
	/* XXX: tree */
	
	fchdir(cwd_fd);
	close(cwd_fd);
	return index;
	
	if(index->hash.disk)
fail_hash:
		diskhash_close(index->hash.disk);
fail_chdir:
	free(index);
fail_malloc:
	fchdir(cwd_fd);
	close(cwd_fd);
	return NULL;
}

static void toilet_free_index(t_index * index)
{
}

/* assumes we're already in the gtable/columns directory */
static t_column * toilet_get_column(const char * name)
{
	int fd;
	uint32_t data[2];
	t_column * column = malloc(sizeof(*column));
	if(!column)
		return NULL;
	column->name = strdup(name);
	if(!column->name)
		goto fail_name;
	
	fd = open(name, O_RDONLY);
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
	column->index = toilet_get_index(name);
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

static void toilet_free_column(t_column * column)
{
	/* XXX column->index */
	free((char *) column->name);
	free(column);
}

t_gtable * toilet_get_gtable(toilet * toilet, const char * name)
{
	int cwd_fd;
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
	gtable->out_count = 1;
	
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		goto fail_cwd;
	if(fchdir(toilet->path_fd) < 0)
		goto fail_db;
	if(chdir(name) < 0)
		goto fail_gtable;
	if(chdir("columns") < 0)
		goto fail_gtable;
	
	dir = opendir(".");
	if(!dir)
		goto fail_gtable;
	while((ent = readdir(dir)))
	{
		t_column * column;
		if(!strcmp(ent->d_name, "id"))
			id++;
		column = toilet_get_column(ent->d_name);
		if(!column)
			goto fail_columns;
		if(vector_push_back(gtable->columns, column) < 0)
		{
			toilet_free_column(column);
			goto fail_columns;
		}
	}
	/* there must be an ID column */
	if(id != 1)
		goto fail_columns;
	if(hash_map_insert(toilet->gtables, gtable->name, gtable) < 0)
		goto fail_columns;
	
	closedir(dir);
	fchdir(cwd_fd);
	close(cwd_fd);
	return gtable;
	
fail_columns:
	for(id = vector_size(gtable->columns) - 1; id >= 0; id--)
		toilet_free_column((t_column *) vector_elt(gtable->columns, id));
	closedir(dir);
fail_gtable:
	fchdir(cwd_fd);
fail_db:
	close(cwd_fd);
fail_cwd:
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
	for(i = vector_size(gtable->columns) - 1; i >= 0; i--)
		toilet_free_column((t_column *) vector_elt(gtable->columns, i));
	free((char *) gtable->name);
	free(gtable);
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

int toilet_new_row(toilet * toilet, t_gtable * gtable)
{
	int i, r, cwd_fd, row_fd;
	char row[] = "rows/xx/xx/xx/xx";
	union {
		t_row_id id;
		uint8_t bytes[sizeof(t_row_id)];
	} id;
	cwd_fd = open(".", 0);
	if(cwd_fd < 0)
		return cwd_fd;
	fchdir(toilet->path_fd);
	if((r = toilet_new_row_id(toilet, &id.id)) < 0)
		goto fail;
	for(i = 0; i < ROW_FORMATS; i++)
	{
		sprintf(row, row_formats[i], id.bytes[0], id.bytes[1], id.bytes[2], id.bytes[3]);
		row_fd = open(row, 0);
		if(row_fd < 0)
		{
			if((r = mkdir(row, 0775)) < 0)
				goto fail;
			row_fd = open(row, 0);
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
	fchdir(row_fd);
	/* XXX ... */
	
fail:
	fchdir(cwd_fd);
	close(cwd_fd);
	/* make sure it's an error value */
	return (r < 0) ? r : -1;
}

int toilet_drop_row(t_row * row)
{
	/* XXX */
	return -ENOSYS;
}

t_row * toilet_get_row(toilet * toilet, t_row_id id)
{
}

void toilet_put_row(t_row * row)
{
}

/* values */

t_values * toilet_row_value(t_row * row, const char * key)
{
}

int toilet_row_remove_values(t_row * row, const char * key)
{
}

int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value)
{
}

int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value)
{
}


int toilet_value_remove(t_values * values, int index)
{
}

int toilet_value_append(t_values * values, t_type type, t_value * value)
{
}

int toilet_value_update(t_values * values, int index, t_value * value)
{
}

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query)
{
}

int toilet_put_rowset(t_rowset * rowset)
{
}
