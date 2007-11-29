#include <stdlib.h>
#include <string.h>

#include "toilet.h"
#include "hash_map.h"

/* This initial implementation of Toilet stores databases using the file system.
 * Each gtable gets a subdirectory in the database directory, which itself must
 * have a file called "toilet-version" containing the single line "0". Each
 * gtable directory has a directory called "columns" containing files named
 * after each possible column in the gtable, and these files store two pieces of
 * information: the type of the column, and the number of rows which have values
 * for that column. Each gtable directory also has a set of subdirectory trees
 * containing rows; the row ID (a 32-bit number for now) is written in
 * hexadecimal and each two digits form a directory name. The last such
 * subdirectory contains a directory for each key in the row, and in these
 * directories are files containing the values for that key, one per file,
 * named starting at "0" and continuing for as many values as there are. */

/* This function returns a toilet pointer. That is, a sign like this:
 * 
 * +------------------+
 * |                  |
 * |  Restrooms  -->  |
 * |                  |
 * +------------------+
 * 
 * The pointer part is the arrow, obviously. The rest of the sign constitutes
 * the type, and may actually indicate a subclass such as "men's room."
 */

toilet * toilet_open(const char * path)
{
	int path_fd, cwd;
	toilet * toilet;
	
	cwd = open(".", 0);
	if(cwd < 0)
		return NULL;
	if(chdir(path) < 0)
		goto close_1;
	path_fd = open(".", 0);
	if(path_fd < 0)
		goto cwd;
	
	toilet = malloc(sizeof(*toilet));
	if(!toilet)
		goto close_2;
	toilet->path = strdup(path);
	if(!toilet->path)
		goto free_1;
	toilet->path_fd = path_fd;
	toilet->gtables = hash_map_create_str();
	if(!toilet->gtables)
		goto free_2;
	toilet->rows = hash_map_create();
	if(!toilet->rows)
		goto destroy;
	
	/* ... */
	fchdir(cwd);
	
	return toilet;
	
	hash_map_destroy(toilet->rows);
destroy:
	hash_map_destroy(toilet->gtables);
free_2:
	free((void *) toilet->path);
free_1:
	/* i.e., not a pay toilet */
	free(toilet);
close_2:
	close(path_fd);
cwd:
	fchdir(cwd);
close_1:
	close(cwd);
	return NULL;
}

int toilet_close(toilet * toilet)
{
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

t_row * toilet_new_row(t_gtable * gtable)
{
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

