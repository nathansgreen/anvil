#include <stdlib.h>
#include <string.h>

#include "toilet.h"
#include "hash_map.h"
#include "blowfish.h"

/* This initial implementation of Toilet stores databases using the file system.
 * Each gtable gets a subdirectory in the database directory, and has
 * subdirectories "columns" and "rows" that store the gtable metadata and data,
 * respectively. The "rows" directory stores a tree of subdirectories: each row
 * ID (32-bit numbers for now) is written in hexadecimal and each two digits
 * form a subdirectory name. The last such subdirectory contains a directory for
 * each key in the row, and in these directories are files containing the values
 * for that key, one per file. */

/* [db]/                                      The top level database
 * [db]/toilet-version                        Version of this database
 * [db]/toilet-id                             This database's ID
 * [db]/next-row                              Next row ID source value
 * [db]/[gt1]/                                A gtable directory
 * [db]/[gt1]/columns/                        The column specifiers
 * [db]/[gt1]/columns/[col1]                  A particular column
 * [db]/[gt1]/columns/[col2]...               More columns...
 * [db]/[gt2]/...                             More gtables...
 * [db]/rows/                                 The rows in all gtables
 * [db]/rows/XX/XX/XX/XX/                     A row directory
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
	/* should be more stuff here */
	hash_map_destroy(toilet->rows);
	hash_map_destroy(toilet->gtables);
	free((void *) toilet->path);
	close(toilet->path_fd);
	close(toilet->row_fd);
	free(toilet);
	return 0;
}

/* gtables */

t_gtable * toilet_new_gtable(toilet * toilet, const char * name)
{
}

int toilet_drop_gtable(t_gtable * gtable)
{
}

t_gtable * toilet_get_gtable(toilet * toilet, const char * name)
{
}

int toilet_put_gtable(t_gtable * gtable)
{
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

t_row * toilet_new_row(toilet * toilet, t_gtable * gtable)
{
	t_row_id id;
	if(toilet_new_row_id(toilet, &id) < 0)
		return NULL;
	/* XXX ... */
}

int toilet_drop_row(t_row * row)
{
}

t_row * toilet_get_row(toilet * toilet, t_row_id id)
{
}

int toilet_put_row(t_row * row)
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
