/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _XOPEN_SOURCE 600
#include <fcntl.h>
#include <ctype.h>
#include <stdio.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "main.h"
#include "blob_buffer.h"

void print(const dtype & x)
{
	switch(x.type)
	{
		case dtype::UINT32:
			printf("%u", x.u32);
			break;
		case dtype::DOUBLE:
			printf("%lg", x.dbl);
			break;
		case dtype::STRING:
			printf("%s", (const char *) x.str);
			break;
		case dtype::BLOB:
			size_t size = x.blb.size();
			printf("%zu[", size);
			for(size_t i = 0; i < size && i < 8; i++)
				printf("%02X%s", x.blb[i], (i < size - 1) ? " " : "");
			printf((size > 8) ? "...]" : "]");
			break;
	}
}

void print(const blob & x, const char * prefix, ...)
{
	va_list ap;
	va_start(ap, prefix);
	if(!x.exists())
	{
		if(prefix)
			vprintf(prefix, ap);
		printf("(non-existent)\n");
		va_end(ap);
		return;
	}
	for(size_t i = 0; i < x.size(); i += 16)
	{
		size_t m = i + 16;
		if(prefix)
			vprintf(prefix, ap);
		for(size_t j = i; j < m; j++)
		{
			if(j < x.size())
				printf("%02x ", x[j]);
			else
				printf("   ");
			if((i % 16) == 8)
				printf(" ");
		}
		printf(" |");
		for(size_t j = i; j < m; j++)
		{
			if(j < x.size())
				printf("%c", isprint(x[j]) ? x[j] : '.');
			else
				printf(" ");
		}
		printf("|\n");
	}
	va_end(ap);
}

void run_iterator(dtable::iter * iter)
{
	bool more = true;
	printf("dtable contents:\n");
	while(iter->valid())
	{
		if(!more)
		{
			EXPECT_NEVER("iter->next() returned false, but iter->valid() says there is more!");
			break;
		}
		print(iter->key());
		printf(":");
		print(iter->value(), "\t");
		more = iter->next();
	}
}

void run_iterator(const dtable * table, ATX_DEF)
{
	dtable::iter * iter = table->iterator(atx);
	run_iterator(iter);
	delete iter;
}

void run_iterator(const ctable * table)
{
	dtype old_key(0u);
	bool more = true, first = true;
	ctable::iter * iter = table->iterator();
	printf("ctable contents:\n");
	while(iter->valid())
	{
		dtype key = iter->key();
		if(!more)
		{
			EXPECT_NEVER("iter->next() returned false, but iter->valid() says there is more!");
			break;
		}
		if(first || key.compare(old_key))
		{
			printf("==> key ");
			print(key);
			printf("\n");
			old_key = key;
			first = false;
		}
		printf("%s:", (const char *) iter->name());
		print(iter->value(), "\t");
		more = iter->next();
	}
	delete iter;
}

void run_iterator(const stable * table)
{
	dtype old_key(0u);
	bool more = true, first = true;
	stable::column_iter * columns = table->columns();
	stable::iter * iter = table->iterator();
	printf("stable columns:\n");
	while(columns->valid())
	{
		size_t rows = columns->row_count();
		const char * type = dtype::name(columns->type());
		if(!more)
		{
			EXPECT_NEVER("columns->next() returned false, but columns->valid() says there is more!");
			break;
		}
		printf("%s:\t%s (%zu row%s)\n", (const char *) columns->name(), type, rows, (rows == 1) ? "" : "s");
		more = columns->next();
	}
	delete columns;
	more = true;
	printf("stable contents:\n");
	while(iter->valid())
	{
		dtype key = iter->key();
		if(!more)
		{
			EXPECT_NEVER("iter->next() returned false, but iter->valid() says there is more!");
			break;
		}
		if(first || key.compare(old_key))
		{
			printf("==> key ");
			print(key);
			printf("\n");
			old_key = key;
			first = false;
		}
		printf("%s:\t", (const char *) iter->column());
		print(iter->value());
		printf("\n");
		more = iter->next();
	}
	delete iter;
}

void time_iterator(const dtable * table, size_t count, ATX_DEF)
{
	struct timeval start;
	dtable::iter * iter = table->iterator(atx);
	printf("Iterate %zu time%s... ", count, (count == 1) ? "" : "s");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < count; i++)
	{
		iter->first();
		while(iter->valid())
		{
			dtype key = iter->key();
			dtype value = iter->value();
			iter->next();
		}
	}
	print_elapsed(&start);
	delete iter;
}

void timeval_subtract(struct timeval * end, const struct timeval * start)
{
	end->tv_sec -= start->tv_sec;
	if(end->tv_usec < start->tv_usec)
	{
		end->tv_usec += 1000000;
		end->tv_sec--;
	}
	end->tv_usec -= start->tv_usec;
}

void timeval_add(struct timeval * accumulator, const struct timeval * delta)
{
	accumulator->tv_sec += delta->tv_sec;
	accumulator->tv_usec += delta->tv_usec;
	if(accumulator->tv_usec >= 1000000)
	{
		accumulator->tv_usec -= 1000000;
		accumulator->tv_sec++;
	}
}

void timeval_divide(struct timeval * time, int denominator, bool round)
{
	uint64_t usec = time->tv_sec;
	usec *= 1000000;
	usec += time->tv_usec;
	if(round)
		usec += denominator / 2;
	if(denominator)
		usec /= denominator;
	else
		/* well, we hope it already is */
		usec = 0;
	time->tv_sec = usec / 1000000;
	time->tv_usec = usec % 1000000;
}

void print_timeval(const struct timeval * time, bool seconds, bool newline)
{
	printf("%d.%06d%s%s", (int) time->tv_sec, (int) time->tv_usec, seconds ? " seconds" : "", newline ? ".\n" : "");
}

void print_elapsed(const struct timeval * start, struct timeval * end, bool elapsed)
{
	timeval_subtract(end, start);
	printf("%d.%06d seconds%s.\n", (int) end->tv_sec, (int) end->tv_usec, elapsed ? " elapsed" : "");
}

void print_elapsed(const struct timeval * start, bool elapsed)
{
	struct timeval end;
	gettimeofday(&end, NULL);
	print_elapsed(start, &end, elapsed);
}

void print_progress(const struct timeval * start, struct timeval * now, int percent)
{
	timeval_subtract(now, start);
	printf("%d%% done after %d.%06d seconds.\n", percent, (int) now->tv_sec, (int) now->tv_usec);
}

void print_progress(const struct timeval * start, int percent)
{
	struct timeval now;
	gettimeofday(&now, NULL);
	print_progress(start, &now, percent);
}

void wait_digest(int seconds)
{
	printf("Waiting %d second%s for digest interval... ", seconds, (seconds == 1) ? "" : "s");
	fflush(stdout);
	sleep(seconds);
	printf("done.\n");
}

blob random_blob(size_t size)
{
	blob_buffer buffer;
	for(size_t i = 0; i < size; i++)
	{
		uint8_t byte = rand();
		buffer << byte;
	}
	return buffer;
}

int drop_cache(const char * path)
{
	struct stat st;
	int r = stat(path, &st);
	if(r < 0)
	{
		perror(path);
		return r;
	}
	if(S_ISDIR(st.st_mode))
	{
		struct dirent * entry;
		DIR * dir = opendir(path);
		if(!dir)
		{
			perror(path);
			return -1;
		}
		while((entry = readdir(dir)))
		{
			char * full = NULL;
			if(!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;
			r = asprintf(&full, "%s/%s", path, entry->d_name);
			if(r < 0)
			{
				perror(entry->d_name);
				continue;
			}
			drop_cache(full);
			free(full);
		}
		closedir(dir);
	}
	else
	{
		int fd = open(path, O_RDONLY);
		if(fd < 0)
		{
			perror(path);
			return fd;
		}
		r = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
		if(r < 0)
		{
			perror(path);
			return r;
		}
		close(fd);
	}
	return 0;
}
