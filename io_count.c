#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <dlfcn.h>

static int is_setup = 0;
static uint64_t read_total = 0, read_calls = 0;
static uint64_t write_total = 0, write_calls = 0;
static uint64_t pread_total = 0, pread_calls = 0;
static uint64_t pwrite_total = 0, pwrite_calls = 0;
static uint64_t fread_total = 0, fread_calls = 0;
static uint64_t fwrite_total = 0, fwrite_calls = 0;

#define MEGS_FRAC(total) (unsigned) ((total) / 1048576), (unsigned) (((total) % 1048576) * 10 / 1048576)

static void print_stats(void)
{
	uint64_t total;
	if(read_calls)
		printf("read() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", read_total, MEGS_FRAC(read_total), read_calls);
	if(write_calls)
		printf("write() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", write_total, MEGS_FRAC(write_total), write_calls);
	if(pread_calls)
		printf("pread() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", pread_total, MEGS_FRAC(pread_total), pread_calls);
	if(pwrite_calls)
		printf("pwrite() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", pwrite_total, MEGS_FRAC(pwrite_total), pwrite_calls);
	if(fread_calls)
		printf("fread() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", fread_total, MEGS_FRAC(fread_total), fread_calls);
	if(fwrite_calls)
		printf("fwrite() total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", fwrite_total, MEGS_FRAC(fwrite_total), fwrite_calls);
	total = read_total + pread_total + fread_total;
	printf("Read total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", total, MEGS_FRAC(total), read_calls + pread_calls + fread_calls);
	total = write_total + pwrite_total + fwrite_total;
	printf("Write total: %"PRIu64" (%u.%uM) calls: %"PRIu64"\n", total, MEGS_FRAC(total), write_calls + pwrite_calls + fwrite_calls);
}

ssize_t __read(int, void *, size_t);
ssize_t __write(int, const void *, size_t);
ssize_t (*__libc_pread)(int, void *, size_t, off_t);
ssize_t __libc_pwrite(int, const void *, size_t, off_t);
size_t _IO_fread(void *, size_t, size_t, FILE *);
size_t _IO_fwrite(const void *, size_t, size_t, FILE *);

static inline void do_setup(void)
{
	if(!is_setup)
	{
		atexit(print_stats);
		/* why is there no __libc_pread? */
		__libc_pread = dlsym(RTLD_NEXT, "pread");
		is_setup = 1;
	}
}

ssize_t read(int fd, void * buf, size_t count)
{
	do_setup();
	if(!isatty(fd))
	{
		read_total += count;
		read_calls++;
	}
	return __read(fd, buf, count);
}

ssize_t write(int fd, const void * buf, size_t count)
{
	do_setup();
	if(!isatty(fd))
	{
		write_total += count;
		write_calls++;
	}
	return __write(fd, buf, count);
}

ssize_t pread(int fd, void * buf, size_t count, off_t offset)
{
	do_setup();
	if(!isatty(fd))
	{
		pread_total += count;
		pread_calls++;
	}
	return __libc_pread(fd, buf, count, offset);
}

ssize_t pwrite(int fd, const void * buf, size_t count, off_t offset)
{
	do_setup();
	if(!isatty(fd))
	{
		pwrite_total += count;
		pwrite_calls++;
	}
	return __libc_pwrite(fd, buf, count, offset);
}

size_t fread(void * ptr, size_t size, size_t nmemb, FILE * stream)
{
	do_setup();
	if(!isatty(fileno(stream)))
	{
		fread_total += size * nmemb;
		fread_calls++;
	}
	return _IO_fread(ptr, size, nmemb, stream);
}

size_t fwrite(const void * ptr, size_t size, size_t nmemb, FILE * stream)
{
	do_setup();
	if(!isatty(fileno(stream)))
	{
		fwrite_total += size * nmemb;
		fwrite_calls++;
	}
	return _IO_fwrite(ptr, size, nmemb, stream);
}
