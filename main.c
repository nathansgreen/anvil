/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "platform.h"
#include "hash_map.h"
#include "toilet.h"

#define HISTORY_FILE ".toilet_history"

static int old_main(int argc, const char * argv[])
{
	toilet * toilet;
	int r = hash_map_init();
	if(r < 0)
		fprintf(stderr, "Warning: failed to initialize hash map library!\n");
	r = toilet_new("test");
	if(r < 0)
	{
		fprintf(stderr, "Warning: failed to create toilet! ('test')\n");
		toilet = toilet_open("test", NULL);
		if(toilet)
		{
			t_gtable * gtable = toilet_get_gtable(toilet, "testgt");
			if(gtable)
			{
				t_query query;
				t_rowset * rows;
				query.name = "key";
				query.type = T_STRING;
				query.value = (t_value *) "value";
				rows = toilet_query(gtable, &query);
				if(rows)
				{
					size_t i;
					for(i = 0; i < ROWS(rows); i++)
					{
						t_row * row;
						t_row_id id = ROW(rows, i);
						printf("Matching row: 0x" ROW_FORMAT "\n", id);
						row = toilet_get_row(toilet, id);
						if(row)
						{
							char * value = (char *) toilet_row_value(row, "key", T_STRING);
							if(!value)
								fprintf(stderr, "Error: failed to get value!\n");
							else
								printf("Value is: %s\n", value);
							toilet_put_row(row);
						}
						else
							fprintf(stderr, "Error: failed to get row!\n");
					}
					toilet_put_rowset(rows);
				}
				else
					fprintf(stderr, "Error: no rows matched query!\n");
				toilet_put_gtable(gtable);
			}
			else
				fprintf(stderr, "Error: failed to open gtable! ('testgt')\n");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Error: failed to open toilet! ('test')\n");
	}
	else
	{
		toilet = toilet_open("test", NULL);
		if(toilet)
		{
			t_gtable * gtable;
			r = toilet_new_gtable(toilet, "testgt");
			if(r < 0)
				fprintf(stderr, "Warning: failed to create gtable! ('testgt')\n");
			gtable = toilet_get_gtable(toilet, "testgt");
			if(gtable)
			{
				t_row_id id;
				if(toilet_new_row(gtable, &id) < 0)
					fprintf(stderr, "Error: failed to create row!\n");
				else
				{
					t_row * row;
					printf("New row ID is 0x" ROW_FORMAT "\n", id);
					row = toilet_get_row(toilet, id);
					if(row)
					{
						r = toilet_row_set_value(row, "key", T_STRING, (t_value *) "value");
						if(r < 0)
							fprintf(stderr, "Warning: failed to set value!\n");
						toilet_put_row(row);
						row = toilet_get_row(toilet, id);
						if(row)
						{
							char * value = (char *) toilet_row_value(row, "key", T_STRING);
							if(!value)
								fprintf(stderr, "Error: failed to get value!\n");
							else
								printf("Value is: %s\n", value);
							toilet_put_row(row);
						}
						else
							fprintf(stderr, "Error: failed to get row again!\n");
					}
					else
						fprintf(stderr, "Error: failed to get row!\n");
				}
				toilet_put_gtable(gtable);
			}
			else
				fprintf(stderr, "Error: failed to open gtable! ('testgt')\n");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Error: failed to open toilet! ('test')\n");
	}
	return 0;
}

static toilet * open_toilet = NULL;
static t_gtable * open_gtable = NULL;

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
		/* not implemented yet */
		r = -ENOSYS;
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static int command_drop(int argc, const char * argv[])
{
	/* not implemented yet */
	return -ENOSYS;
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
		if(open_gtable)
		{
			toilet_put_gtable(open_gtable);
			open_gtable = NULL;
		}
	}
	return 0;
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
			/* not implemented yet */
			r = -ENOSYS;
		}
	}
	else
		printf("Unknown object type: %s\n", argv[1]);
	return r;
}

static int command_set(int argc, const char * argv[])
{
	/* not implemented yet */
	return -ENOSYS;
}

static int command_query(int argc, const char * argv[])
{
	/* not implemented yet */
	return -ENOSYS;
}

static int command_help(int argc, const char * argv[]);
static int command_quit(int argc, const char * argv[]);

struct {
	const char * command;
	const char * help;
	int (*execute)(int argc, const char * argv[]);
} commands[] = {
	{"test", "Run the old hand-written toilet test", old_main},
	{"create", "Create toilet objects: databases, gtables, and rows", command_create},
	{"drop", "Drop toilet objects: databases, gtables, and rows", command_drop},
	{"open", "Open toilet objects: databases and gtables", command_open},
	{"close", "Close toilet objects: databases and gtables", command_close},
	{"list", "List toilet objects: gtables", command_list},
	{"set", "Modify toilet objects: gtables and rows", command_set},
	{"query", "Query toilet!", command_query},
	{"help", "Displays help.", command_help},
	{"quit", "Quits the program.", command_quit}
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

int main(int argc, char * argv[])
{
	int r;
	read_history(HISTORY_FILE);
	do {
		int i;
		char * error;
		char * line = readline("toilet> ");
		if(!line)
		{
			printf("\n");
			line = strdup("quit");
			assert(line);
		}
		for(i = 0; line[i] == ' '; i++);
		if(line[i])
			add_history(line);
		r = command_line_execute(line, &error);
		free(line);
		if(r == -E2BIG)
			printf("Too many tokens on command line!\n");
		else if(r == -ENOENT)
			printf("No such command.\n");
		else if(r < 0 && r != -EINTR)
			printf("Error: %s\n", error);
	} while(r != -EINTR);
	write_history(HISTORY_FILE);
	
	return 0;
}
