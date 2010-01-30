/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

static enum {
	SPACE,
	PREAMBLE,
	BENCH
} log_state = SPACE;
static char * bench_name = NULL;
static char * bench_file = NULL;
static FILE * bench_out = NULL;
static int bench_patches = 0;
static int bench_count = 0;
static int bench_unpatches = 0;

static int prompt_overwrite(const char * file)
{
	static int all_never = 0;
	int value = 0;
	char line[256];
	FILE * term;
	if(all_never)
	{
		printf("%sverwriting '%s'\n", (all_never > 0) ? "O" : "Not o", file);
		return all_never > 0;
	}
	term = fopen("/dev/stderr", "r");
	printf("File '%s' exists, overwrite? [y|a|N|v] ", file);
	fflush(stdout);
	if(term)
	{
		if(fgets(line, sizeof(line), term))
		{
			if(line[0] == 'a' || line[0] == 'A')
			{
				all_never = 1;
				value = 1;
			}
			else if(line[0] == 'y' || line[0] == 'Y')
				value = 1;
			else if(line[0] == 'v' || line[0] == 'V')
				all_never = -1;
		}
		fclose(term);
	}
	else
		perror("/dev/stderr");
	if(!value)
		printf("Cancelled.\n");
	return value;
}

static void run_utility(char * file, const char * new_ext, char * const argv[])
{
	int in_fd, out_fd;
	char * out_name = file;
	char * ext = strrchr(file, '.');
	assert(strlen(new_ext) == 4);
	in_fd = open(file, O_RDONLY);
	if(in_fd < 0)
		perror(file);
	if(ext && !strcmp(ext, ".log"))
		strcpy(ext, new_ext);
	else
		asprintf(&out_name, "%s%s", file, new_ext);
	assert(out_name);
	out_fd = open(out_name, O_WRONLY | O_CREAT | O_EXCL, 0664);
	if(out_fd < 0 && errno == EEXIST && prompt_overwrite(out_name))
		out_fd = open(out_name, O_WRONLY | O_CREAT | O_TRUNC, 0664);
	if(out_fd < 0)
		perror(out_name);
	else
	{
		pid_t pid = fork();
		if(pid < 0)
			perror("fork()");
		else if(pid)
		{
			int status;
			close(in_fd);
			close(out_fd);
			waitpid(pid, &status, 0);
			if(!WIFEXITED(status))
				fprintf(stderr, "[Child did not exit normally]\n");
			else if(WEXITSTATUS(status))
				fprintf(stderr, "[Child exited with status %d]\n", WEXITSTATUS(status));
		}
		else
		{
			dup2(in_fd, 0);
			close(in_fd);
			dup2(out_fd, 1);
			close(out_fd);
			execvp(argv[0], argv);
			_exit(127);
		}
	}
	if(out_name != file)
		free(out_name);
}

static void run_average(char * file, int count)
{
	char n[32];
	char * argv[] = {"average", "-n", n, NULL};
	snprintf(n, sizeof(n), "%d", count);
	return run_utility(file, ".avg", argv);
}

static void run_derive(char * file)
{
	char * argv[] = {"derive", NULL};
	return run_utility(file, ".drv", argv);
}

static void process(char * line, const char * file, const char * ext)
{
	int i;
	switch(log_state)
	{
		case SPACE:
			if(strncmp(line, "========== ", 11))
				break;
			for(i = 11; line[i] && line[i] != ' '; i++);
			line[i] = 0;
			assert(!bench_name);
			bench_name = strdup(&line[11]);
			printf("Start of benchmark '%s'\n", bench_name);
			assert(!bench_file);
			if(file)
			{
				if(ext)
					asprintf(&bench_file, "%s-%s.%s", file, bench_name, ext);
				else
					asprintf(&bench_file, "%s-%s", file, bench_name);
			}
			else
				asprintf(&bench_file, "%s.log", bench_name);
			assert(bench_file);
			assert(!bench_out);
			bench_out = fopen(bench_file, "wx");
			if(!bench_out && errno == EEXIST && prompt_overwrite(bench_file))
				bench_out = fopen(bench_file, "w");
			if(!bench_out)
				perror(bench_file);
			log_state = PREAMBLE;
			bench_patches = 0;
			bench_count = 0;
			bench_unpatches = 0;
			break;
		case PREAMBLE:
			if(strncmp(line, "---------- ", 11))
			{
				if(!strncmp(line, "patching ", 9))
					bench_patches++;
				else if(bench_patches && !strcmp(line, "make: Nothing to be done for `all'.\n"))
				{
					bench_count = 1;
					log_state = BENCH;
				}
				break;
			}
			sscanf(line, "---------- %d/%d ", &i, &bench_count);
			if(i != 1 || bench_count < 1)
				fprintf(stderr, "Invalid log format: %s", line);
			log_state = BENCH;
			break;
		case BENCH:
			if(!strncmp(line, "---------- ", 11))
			{
				int count = 0;
				sscanf(line, "---------- %d/%d ", &i, &count);
				if(i < 2 || i > bench_count || count != bench_count)
					fprintf(stderr, "Invalid log format: %s", line);
				break;
			}
			if(!strncmp(line, "patching ", 9))
			{
				bench_unpatches++;
				break;
			}
			if(strncmp(line, "========== ", 11))
			{
				fprintf(bench_out, "%s", line);
				break;
			}
			if(strncmp(&line[11], bench_name, strlen(bench_name)))
				fprintf(stderr, "Invalid log format: %s", line);
			if(bench_patches != bench_unpatches)
				fprintf(stderr, "Invalid log format (bench %s)\n", bench_name);
			printf("End of benchmark '%s'\n", bench_name);
			fclose(bench_out);
			bench_out = NULL;
			if(bench_count > 1)
				run_average(bench_file, bench_count);
			else if(!strncmp(bench_name, "tpch_create_", 12))
				run_derive(bench_file);
			free(bench_file);
			bench_file = NULL;
			free(bench_name);
			bench_name = NULL;
			log_state = SPACE;
			break;
	}
}

static void finish(void)
{
	if(log_state != SPACE)
		fprintf(stderr, "Invalid log format (at end)\n");
}

int main(int argc, char * argv[])
{
	char line[1024];
	char * file = NULL;
	char * ext = NULL;
	FILE * input = stdin;
	if(argc > 1)
		file = argv[1];
	if(file)
	{
		input = fopen(file, "r");
		if(!input)
			perror(file);
		ext = strrchr(file, '.');
		if(ext)
			*(ext++) = 0;
	}
	while(fgets(line, sizeof(line), input))
		process(line, file, ext);
	finish();
	if(file)
		fclose(input);
	return 0;
}
