/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "openat.h"
#include "transaction.h"
#include "toilet++.h"

static t_toilet * open_toilet = NULL;
static t_gtable * open_gtable = NULL;
static t_row * open_row = NULL;

static int command_create(int argc, const char * argv[])
{
	int r = 0;
	if(argc < 2)
		printf("Create what?\n");
	else if(!strcmp(argv[1], "database"))
	{
		if(argc < 3)
			printf("OK, but what should I call it?\n");
		else
			r = toilet_new(argv[2]);
	}
	else if(!strcmp(argv[1], "gtable"))
	{
		if(!open_toilet)
			printf("You need to open a database first.\n");
		else if(argc < 3)
			printf("OK, but what should I call it?\n");
		else
			r = toilet_new_gtable(open_toilet, argv[2]);
	}
	else if(!strcmp(argv[1], "row"))
	{
		if(!open_gtable)
			printf("You need to open a gtable first.\n");
		else if(open_row)
			printf("You need to close the current row first.\n");
		else
		{
			t_row_id id;
			r = toilet_new_row(open_gtable, &id);
			if(r >= 0)
			{
				printf("New row ID is " ROW_FORMAT "\n", id);
				assert(open_toilet);
				open_row = toilet_get_row(open_gtable, id);
				if(!open_row)
					r = errno ? -errno : -ENOENT;
			}
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static int command_drop(int argc, const char * argv[])
{
	int r = 0;
	if(argc < 2)
		printf("Drop what?\n");
	else if(!strcmp(argv[1], "database"))
	{
		/* not implemented yet */
		r = -ENOSYS;
	}
	else if(!strcmp(argv[1], "gtable"))
	{
		if(argc < 3)
			printf("OK, but which one should I drop?\n");
		else if(!strcmp(argv[2], "."))
		{
		current_gtable:
			if(!open_gtable)
				printf("You need to open a gtable first.\n");
			else if(open_row)
				printf("You need to close the current row first.\n");
			else
			{
				r = toilet_drop_gtable(open_gtable);
				if(r >= 0)
					open_gtable = NULL;
			}
		}
		else if(open_toilet)
		{
			if(open_gtable && !strcmp(argv[2], toilet_gtable_name(open_gtable)))
				goto current_gtable;
			else
			{
				t_gtable * gtable = toilet_get_gtable(open_toilet, argv[2]);
				if(!gtable)
					r = errno ? -errno : -ENOENT;
				else
				{
					r = toilet_drop_gtable(gtable);
					if(r < 0)
						toilet_put_gtable(gtable);
				}
			}
		}
		else
			printf("You need to open a database first.\n");
	}
	else if(!strcmp(argv[1], "row"))
	{
		if(argc < 3)
			printf("OK, but which one should I drop?\n");
		else if(!strcmp(argv[2], "."))
		{
			if(!open_row)
				printf("You need to open a row first.\n");
			else
			{
			current_row:
				r = toilet_drop_row(open_row);
				if(r >= 0)
					open_row = NULL;
			}
		}
		else if(open_gtable)
		{
			t_row_id id;
			if(sscanf(argv[2], ROW_FORMAT, &id) != 1)
				r = -EINVAL;
			else
			{
				if(open_row && id == toilet_row_id(open_row))
					goto current_row;
				else
				{
					t_row * row = toilet_get_row(open_gtable, id);
					if(!row)
						r = errno ? -errno : -ENOENT;
					else
					{
						r = toilet_drop_row(row);
						if(r < 0)
							toilet_put_row(row);
					}
				}
			}
		}
		else
			printf("You need to open a gtable first.\n");
	}
	else if(!strcmp(argv[1], "value"))
	{
		if(!open_row)
			printf("You need to open a row first.\n");
		else
		{
			if(argc < 3)
				printf("OK, but which one should I drop?\n");
			else
				r = toilet_row_remove_key(open_row, argv[2]);
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static int command_open(int argc, const char * argv[])
{
	int r = 0;
	if(argc < 2)
		printf("Open what?\n");
	else if(!strcmp(argv[1], "database"))
	{
		if(open_toilet)
			printf("You need to close the current database first.\n");
		else if(argc < 3)
			printf("OK, but which one should I open?\n");
		else
		{
			open_toilet = toilet_open(argv[2], NULL);
			if(!open_toilet)
				r = errno ? -errno : -ENOENT;
		}
	}
	else if(!strcmp(argv[1], "gtable"))
	{
		if(open_gtable)
			printf("You need to close the current gtable first.\n");
		else if(!open_toilet)
			printf("You need to open a database first.\n");
		else if(argc < 3)
			printf("OK, but which one should I open?\n");
		else
		{
			open_gtable = toilet_get_gtable(open_toilet, argv[2]);
			if(!open_gtable)
				r = errno ? -errno : -ENOENT;
		}
	}
	else if(!strcmp(argv[1], "row"))
	{
		if(open_row)
			printf("You need to close the current row first.\n");
		else if(!open_gtable)
			printf("You need to open a gtable first.\n");
		else if(argc < 3)
			printf("OK, but which one should I open?\n");
		else
		{
			t_row_id id;
			if(sscanf(argv[2], ROW_FORMAT, &id) != 1)
				r = -EINVAL;
			else
			{
				assert(open_toilet);
				open_row = toilet_get_row(open_gtable, id);
				if(!open_row)
					r = errno ? -errno : -ENOENT;
			}
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static int command_close(int argc, const char * argv[])
{
	if(argc < 2)
		printf("Close what?\n");
	else if(!strcmp(argv[1], "database"))
	{
		if(open_gtable)
			printf("You need to close the current gtable first.\n");
		else if(open_toilet)
		{
			toilet_close(open_toilet);
			open_toilet = NULL;
		}
	}
	else if(!strcmp(argv[1], "gtable"))
	{
		if(open_row)
			printf("You need to close the current row first.\n");
		else if(open_gtable)
		{
			toilet_put_gtable(open_gtable);
			open_gtable = NULL;
		}
	}
	else if(!strcmp(argv[1], "row"))
	{
		if(open_row)
		{
			toilet_put_row(open_row);
			open_row = NULL;
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return 0;
}

static void print_value(t_type type, const t_value * value)
{
	switch(type)
	{
		case T_INT:
			printf("%u\n", value->v_int);
			break;
		case T_FLOAT:
			printf("%lg\n", value->v_float);
			break;
		case T_STRING:
			printf("%s\n", value->v_string);
			break;
		case T_BLOB:
			printf("(blob:%u)\n", value->v_blob.length);
			break;
	}
}

static int command_list(int argc, const char * argv[])
{
	int r = 0;
	if(argc < 2)
		printf("List what?\n");
	else if(!strcmp(argv[1], "gtables"))
	{
		if(!open_toilet)
			printf("You need to open a database first.\n");
		else
		{
			size_t i, max = toilet_gtables_count(open_toilet);
			for(i = 0; i < max; i++)
				printf("%s\n", toilet_gtables_name(open_toilet, i));
		}
	}
	else if(!strcmp(argv[1], "columns"))
	{
		if(!open_gtable)
			printf("You need to open a gtable first.\n");
		else
		{
			t_columns * columns = toilet_gtable_columns(open_gtable);
			printf("Name          Type       Rows\n");
			printf("------------  ---------  -----------\n");
			while(toilet_columns_valid(columns))
			{
				const char * name = toilet_columns_name(columns);
				const char * type = toilet_name_type(toilet_columns_type(columns));
				size_t count = toilet_columns_row_count(columns);
				printf("%-12s  %-9s  %11u\n", name, type, count);
				toilet_columns_next(columns);
			}
			toilet_put_columns(columns);
		}
	}
	else if(!strcmp(argv[1], "rows"))
	{
		t_simple_query query = {.name = NULL};
		t_rowset * rows = toilet_simple_query(open_gtable, &query);
		if(!rows)
			r = -ENOENT;
		else
		{
			size_t i, max = toilet_rowset_size(rows);
			for(i = 0; i < max; i++)
				printf(ROW_FORMAT "\n", toilet_rowset_row(rows, i));
			toilet_put_rowset(rows);
			printf("gtable %s holds %d row%s\n", toilet_gtable_name(open_gtable), i, (i == 1) ? "" : "s");
		}
	}
	else if(!strcmp(argv[1], "keys"))
	{
		if(!open_row)
			printf("You need to open a row first.\n");
		else
		{
			/* not implemented yet */
			r = -ENOSYS;
		}
	}
	else if(!strcmp(argv[1], "value"))
	{
		if(!open_row)
			printf("You need to open a row first.\n");
		else
		{
			if(argc < 3)
				printf("OK, but which one should I list?\n");
			else
			{
				t_type type;
				const t_value * value = toilet_row_value_type(open_row, argv[2], &type);
				if(!value)
					r = errno ? -errno : -ENOENT;
				else
					print_value(type, value);
			}
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static t_value * parse_value(t_type type, const char * string, t_value * value)
{
	char * end = NULL;
	switch(type)
	{
		case T_INT:
			value->v_int = strtoll(string, &end, 0);
			if(end && *end)
				return NULL;
			return value;
		case T_FLOAT:
			value->v_float = strtod(string, &end);
			if(end && *end)
				return NULL;
			return value;
		case T_STRING:
			return (t_value *) string;
		case T_BLOB:
			/* fall through */ ;
	}
	return NULL;
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

static int command_set(int argc, const char * argv[])
{
	int r = 0;
	if(!open_row)
		printf("You need to open a row first.\n");
	else
	{
		if(argc < 2)
			printf("OK, but what should I set?\n");
		else if(!strcmp(argv[1], "gtable"))
		{
			/* not implemented yet */
			r = -ENOSYS;
		}
		else if(!strcmp(argv[1], "row"))
		{
			/* not implemented yet */
			r = -ENOSYS;
		}
		else if(!strcmp(argv[1], "value"))
		{
			if(argc < 3)
				printf("OK, but which value should I set?\n");
			else
			{
				t_value local_value;
				t_value * value;
				t_type type;
				if(!toilet_gtable_column_row_count(open_gtable, argv[2]))
				{
					if(argc < 4)
						printf("You need to specify the type for a new column.\n");
					else if(argc < 5)
						printf("OK, but what value should I set it to?\n");
					else
					{
					user_type:
						r = parse_type(argv[3], &type);
						if(r < 0)
							printf("Unknown type: %s\n", argv[3]);
						else
						{
							value = parse_value(type, argv[4], &local_value);
							goto have_type;
						}
					}
				}
				else
				{
					if(argc < 4)
						printf("OK, but what value should I set it to?\n");
					else if(argc < 5)
					{
						type = toilet_gtable_column_type(open_gtable, argv[2]);
						value = parse_value(type, argv[3], &local_value);
					have_type:
						r = toilet_row_set_value(open_row, argv[2], type, value);
						/* TODO: print error message? */
					}
					else
						goto user_type;
				}
			}
		}
		else
			printf("Unknown object type: %s\n", argv[1]);
	}
	return r;
}

static int command_maintain(int argc, const char * argv[])
{
	int r = 0;
	if(!open_gtable)
		printf("You need to open a gtable first.\n");
	else
		r = toilet_gtable_maintain(open_gtable);
	return r;
}

static int command_query(int argc, const char * argv[])
{
	int r = 0;
	if(!open_gtable)
		printf("You need to open a gtable first.\n");
	else
	{
		if(argc < 2)
			printf("OK, but which column should I query?\n");
		else
		{
			if(!toilet_gtable_column_row_count(open_gtable, argv[1]))
				printf("Unknown column: %s\n", argv[1]);
			else
			{
				if(argc < 3)
					printf("OK, but what value should I query for?\n");
				else
				{
					t_value value;
					t_simple_query query;
					query.name = argv[1];
					query.type = toilet_gtable_column_type(open_gtable, argv[1]);
					query.values[0] = parse_value(query.type, argv[2], &value);
					query.values[1] = NULL;
					if(!query.values[0])
						r = -EINVAL;
					else
					{
						t_rowset * rows = toilet_simple_query(open_gtable, &query);
						if(!rows)
							r = -ENOENT;
						else
						{
							size_t i, max = toilet_rowset_size(rows);
							printf("Matching rows:\n");
							for(i = 0; i < max; i++)
								printf(ROW_FORMAT "\n", toilet_rowset_row(rows, i));
							toilet_put_rowset(rows);
							printf("%d row%s matched\n", i, (i == 1) ? "" : "s");
						}
					}
				}
			}
		}
	}
	return r;
}

static int command_tx(int argc, const char * argv[])
{
	int r;
	tx_fd fd, fd2;
	char buf[17];
	
	fd = tx_open(AT_FDCWD, "testfile", O_RDWR | O_CREAT, 0644);
	printf("tx_open(testfile) = %d\n", fd);
	r = tx_start();
	printf("tx_start() = %d\n", r);
	
	r = tx_write(fd, "0123456789ABCDEF", 16, 0);
	printf("tx_write(%d, 0123456789ABCDEF, 16, 0) = %d\n", fd, r);
	r = tx_write(fd, "FEDCBA9876543210", 16, 14);
	printf("tx_write(%d, FEDCBA9876543210, 16, 14) = %d\n", fd, r);
	r = tx_read(fd, buf, 16, 8);
	buf[r < 0 ? 0 : r > 16 ? 16 : r] = 0;
	printf("tx_read(%d, 16, 8) = %d \"%s\"\n", fd, r, buf);
	
	fd2 = tx_open(AT_FDCWD, "testfile", O_RDWR | O_CREAT, 0644);
	printf("tx_open(testfile) = %d\n", fd2);
	r = tx_close(fd2);
	printf("tx_close(%d) = %d\n", fd2, r);
	r = tx_read(fd, buf, 16, 7);
	buf[r < 0 ? 0 : r > 16 ? 16 : r] = 0;
	printf("tx_read(%d, 16, 7) = %d \"%s\"\n", fd, r, buf);
	
	r = tx_end(0);
	printf("tx_end() = %d\n", r);
	
	r = tx_read(fd, buf, 16, 6);
	buf[r < 0 ? 0 : r > 16 ? 16 : r] = 0;
	printf("tx_read(%d, 16, 6) = %d \"%s\"\n", fd, r, buf);
	
	r = tx_close(fd);
	printf("tx_close(%d) = %d\n", fd, r);
	
	r = tx_start();
	printf("tx_start() = %d\n", r);
	r = tx_unlink(AT_FDCWD, "testfile");
	printf("tx_unlink(testfile) = %d\n", r);
	r = tx_end(0);
	printf("tx_end() = %d\n", r);
	
	return 0;
}

int command_journal(int argc, const char * argv[]);
int command_dtable(int argc, const char * argv[]);
int command_ctable(int argc, const char * argv[]);
int command_stable(int argc, const char * argv[]);
int command_blob_cmp(int argc, const char * argv[]);
int command_performance(int argc, const char * argv[]);

static int command_script(int argc, const char * argv[]);
static int command_help(int argc, const char * argv[]);
static int command_quit(int argc, const char * argv[]);

struct {
	const char * command;
	const char * help;
	int (*execute)(int argc, const char * argv[]);
} commands[] = {
	{"create", "Create toilet objects: databases, gtables, and rows.", command_create},
	{"drop", "Drop toilet objects: databases, gtables, rows, and values.", command_drop},
	{"open", "Open toilet objects: databases, gtables, and rows.", command_open},
	{"close", "Close toilet objects: databases, gtables, and rows.", command_close},
	{"list", "List toilet objects: gtables, columns, rows, keys, and values.", command_list},
	{"set", "Modify toilet objects: gtables, rows, and values.", command_set},
	{"maintain", "Run maintenance on the open gtable.", command_maintain},
	{"query", "Query toilet!", command_query},
	{"help", "Displays help.", command_help},
	{"quit", "Quits the program.", command_quit},
	{"script", "Run a toilet script.", command_script},
	{"journal", "Test journal functionality: create, append, commit, playback, erase.", command_journal},
	{"tx", "Test transaction functionality.", command_tx},
	{"dtable", "Test dtable functionality.", command_dtable},
	{"ctable", "Test ctable functionality.", command_ctable},
	{"stable", "Test stable functionality.", command_stable},
	{"blob_cmp", "Test blob_cmp functionality.", command_blob_cmp},
	{"performance", "Test performance.", command_performance}
};
#define COMMAND_COUNT (sizeof(commands) / sizeof(commands[0]))

static int command_help(int argc, const char * argv[])
{
	int i;
	if(argc < 2)
	{
		printf("Commands:\n");
		for(i = 0; i < COMMAND_COUNT; i++)
			printf("  %s\n    %s\n", commands[i].command, commands[i].help);
	}
	else
		for(i = 0; i < COMMAND_COUNT; i++)
		{
			if(strcmp(commands[i].command, argv[1]))
				continue;
			printf("  %s\n    %s\n", commands[i].command, commands[i].help);
			break;
		}
	return 0;
}

static int command_quit(int argc, const char * argv[])
{
	return -EINTR;
}

static int command_line_execute(char * line, char ** error)
{
	int i, argc = 0;
	const char * argv[64];
	*error = NULL;
	do {
		while(*line == ' ' || *line == '\n')
			line++;
		if(!*line)
			break;
		argv[argc++] = line;
		while(*line && *line != ' ' && *line != '\n')
			line++;
		if(*line)
			*(line++) = 0;
		else
			break;
	} while(argc < 64);
	if(*line)
		return -E2BIG;
	if(!argc)
		return 0;
	for(i = 0; i < COMMAND_COUNT; i++)
		if(!strcmp(commands[i].command, argv[0]))
		{
			int r = commands[i].execute(argc, argv);
			if(r < 0)
			{
				/* try to compensate for return -1 */
				if(r == -1 && errno)
					r = -errno;
				*error = strerror(-r);
				/* these are not allowed here */
				if(r == -E2BIG || r == -ENOENT)
					r = -1;
			}
			return r;
		}
	return -ENOENT;
}

static int command_script(int argc, const char * argv[])
{
	if(argc < 2)
		printf("What script should I read?\n");
	else
	{
		FILE * script = fopen(argv[1], "r");
		char line[64];
		if(!script)
			return -errno;
		fgets(line, sizeof(line), script);
		while(!feof(script))
		{
			int i, r = 0;
			char * error;
			printf("> %s", line);
			for(i = 0; line[i] == ' '; i++);
			if(line[i] != '#')
				r = command_line_execute(line, &error);
			if(r == -E2BIG)
				printf("Too many tokens on command line!\n");
			else if(r == -ENOENT)
				printf("No such command.\n");
			else if(r == -EINTR)
				break;
			else if(r < 0)
				printf("Error: %s\n", error);
			fgets(line, sizeof(line), script);
		}
		fclose(script);
	}
	return 0;
}

int main(int argc, char * argv[])
{
	char * quit = "quit";
	char * home = getenv("HOME");
	char history[PATH_MAX];
	int r;
	/* toilet_init() calls tx_init() for us */
	if((r = toilet_init(".")) < 0)
	{
		if(r == -1 && errno > 0)
		{
			fprintf(stderr, "Error: toilet_init() = -1 (%s)\n", strerror(errno));
			r = -errno;
		}
		else
			fprintf(stderr, "Error: toilet_init() = %d (%s)\n", r, strerror(-r));
		if(r == -ENOENT)
			fprintf(stderr, "(Is there a journals directory?)\n");
		return 1;
	}
	snprintf(history, sizeof(history), "%s/.toilet_history", home ? home : ".");
	read_history(history);
	do {
		int i;
		char * error;
		char * line = readline("toilet> ");
		if(!line)
		{
			printf("\n");
			line = quit;
		}
		for(i = 0; line[i] == ' '; i++);
		if(line[i] == '#')
			/* commented out */
			r = 0;
		else
		{
			if(line[i] && strcmp(line, "quit"))
				add_history(line);
			r = command_line_execute(line, &error);
		}
		if(line != quit)
			free(line);
		if(r == -E2BIG)
			printf("Too many tokens on command line!\n");
		else if(r == -ENOENT)
			printf("No such command.\n");
		else if(r < 0 && r != -EINTR)
			printf("Error: %s\n", error);
	} while(r != -EINTR);
	write_history(history);
	
	if(open_row)
		toilet_put_row(open_row);
	if(open_gtable)
		toilet_put_gtable(open_gtable);
	if(open_toilet)
		toilet_close(open_toilet);
	return 0;
}
