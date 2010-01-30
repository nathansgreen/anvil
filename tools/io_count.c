/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <dlfcn.h>

struct io_stats
{
	uint64_t read_total, read_calls;
	uint64_t write_total, write_calls;
	uint64_t pread_total, pread_calls;
	uint64_t pwrite_total, pwrite_calls;
	uint64_t fread_total, fread_calls;
	uint64_t fwrite_total, fwrite_calls;
};

static int is_setup = 0;
static struct io_stats io_stats = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

static int want_stats(void)
{
	FILE * maps;
	char line[160];
	char * require = getenv("IO_COUNT_REQUIRE");
	if(!require)
		/* no reqirement; we want stats */
		return 1;
	snprintf(line, sizeof(line), "/proc/%d/maps", (int) getpid());
	maps = fopen(line, "r");
	if(!maps)
		/* can't open maps; assume we want stats */
		return 1;
	while(fgets(line, sizeof(line), maps))
	{
		if(strstr(line, require))
		{
			/* found the required library; we want stats */
			fclose(maps);
			return 1;
		}
	}
	/* required library not found; we don't want stats */
	fclose(maps);
	return 0;
}

#define MEGS_FRAC(total) (unsigned) ((total) / 1048576), (unsigned) (((total) % 1048576) * 10 / 1048576)

static void print_stats(void)
{
	uint64_t total;
	/* save the stats before we check to see whether we should
	 * print them, because checking will alter the stats */
	struct io_stats stats = io_stats;
	if(!want_stats())
		return;
	if(stats.read_calls)
		printf("read() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.read_total, MEGS_FRAC(stats.read_total), stats.read_calls);
	if(stats.write_calls)
		printf("write() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.write_total, MEGS_FRAC(stats.write_total), stats.write_calls);
	if(stats.pread_calls)
		printf("pread() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.pread_total, MEGS_FRAC(stats.pread_total), stats.pread_calls);
	if(stats.pwrite_calls)
		printf("pwrite() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.pwrite_total, MEGS_FRAC(stats.pwrite_total), stats.pwrite_calls);
	if(stats.fread_calls)
		printf("fread() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.fread_total, MEGS_FRAC(stats.fread_total), stats.fread_calls);
	if(stats.fwrite_calls)
		printf("fwrite() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", stats.fwrite_total, MEGS_FRAC(stats.fwrite_total), stats.fwrite_calls);
	total = stats.read_total + stats.pread_total + stats.fread_total;
	printf("Read total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", total, MEGS_FRAC(total), stats.read_calls + stats.pread_calls + stats.fread_calls);
	total = stats.write_total + stats.pwrite_total + stats.fwrite_total;
	printf("Write total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", total, MEGS_FRAC(total), stats.write_calls + stats.pwrite_calls + stats.fwrite_calls);
}

#ifdef __APPLE__
ssize_t (*__read)(int, void *, size_t);
ssize_t (*__write)(int, const void *, size_t);
ssize_t (*__libc_pread)(int, void *, size_t, off_t);
ssize_t (*__libc_pwrite)(int, const void *, size_t, off_t);
size_t (*_IO_fread)(void *, size_t, size_t, FILE *);
size_t (*_IO_fwrite)(const void *, size_t, size_t, FILE *);
#else
ssize_t __read(int, void *, size_t);
ssize_t __write(int, const void *, size_t);
ssize_t (*__libc_pread)(int, void *, size_t, off_t);
ssize_t __libc_pwrite(int, const void *, size_t, off_t);
size_t _IO_fread(void *, size_t, size_t, FILE *);
size_t _IO_fwrite(const void *, size_t, size_t, FILE *);
#endif

static inline void do_setup(void)
{
	if(!is_setup)
	{
		atexit(print_stats);
#ifdef __APPLE__
		__read = dlsym(RTLD_NEXT, "read");
		__write = dlsym(RTLD_NEXT, "write");
		__libc_pread = dlsym(RTLD_NEXT, "pread");
		__libc_pwrite = dlsym(RTLD_NEXT, "pwrite");
		_IO_fread = dlsym(RTLD_NEXT, "fread");
		_IO_fwrite = dlsym(RTLD_NEXT, "fwrite");
#else
		/* why is there no __libc_pread? */
 		__libc_pread = dlsym(RTLD_NEXT, "pread");
#endif
		is_setup = 1;
	}
}

ssize_t read(int fd, void * buf, size_t count)
{
	do_setup();
	if(!isatty(fd))
	{
		io_stats.read_total += count;
		io_stats.read_calls++;
	}
	return __read(fd, buf, count);
}

ssize_t write(int fd, const void * buf, size_t count)
{
	do_setup();
	if(!isatty(fd))
	{
		io_stats.write_total += count;
		io_stats.write_calls++;
	}
	return __write(fd, buf, count);
}

ssize_t pread(int fd, void * buf, size_t count, off_t offset)
{
	do_setup();
	if(!isatty(fd))
	{
		io_stats.pread_total += count;
		io_stats.pread_calls++;
	}
	return __libc_pread(fd, buf, count, offset);
}

ssize_t pwrite(int fd, const void * buf, size_t count, off_t offset)
{
	do_setup();
	if(!isatty(fd))
	{
		io_stats.pwrite_total += count;
		io_stats.pwrite_calls++;
	}
	return __libc_pwrite(fd, buf, count, offset);
}

size_t fread(void * ptr, size_t size, size_t nmemb, FILE * stream)
{
	do_setup();
	if(!isatty(fileno(stream)))
	{
		io_stats.fread_total += size * nmemb;
		io_stats.fread_calls++;
	}
	return _IO_fread(ptr, size, nmemb, stream);
}

size_t fwrite(const void * ptr, size_t size, size_t nmemb, FILE * stream)
{
	do_setup();
	if(!isatty(fileno(stream)))
	{
		io_stats.fwrite_total += size * nmemb;
		io_stats.fwrite_calls++;
	}
	return _IO_fwrite(ptr, size, nmemb, stream);
}
