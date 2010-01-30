/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

/* MEDIC - Mostly Empty Device Image Compressor */

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64
#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#include "../md5.h"

#define static_assert(x) do { switch(0) { case 0: case (x): ; } } while(0)

#define MEDIC_MAGIC 0xC5FA9171
#define MEDIC_VERSION 0

#define BLOCK_SIZE 4096
static uint8_t buffer[BLOCK_SIZE];

struct medic_header {
	uint32_t magic;
	uint32_t version;
	uint16_t blocksize;
} __attribute__((packed));

struct medic_block {
	uint32_t number;
	uint16_t size;
} __attribute__((packed));

enum medic_mode {
	MEDIC_ZERO,
	MEDIC_PATTERN,
	MEDIC_SCRG, /* self-certifying random garbage */
};

/* requires MD5_CTX ctx in scope */
#define SCRG_SIZE (BLOCK_SIZE - sizeof(ctx.digest))

static void erase_help(int argc, char * argv[])
{
	printf("Usage: medic erase [options] <device>\n");
	printf("Options:\n");
	printf("  -z           Erase with zeroes (default)\n");
	printf("  -p <0xNN..>  Erase with the given pattern\n");
	printf("  -g           Erase with self-certifying random garbage\n");
}

static int erase_help_error(void)
{
	erase_help(0, NULL);
	return 1;
}

static int medic_erase(int argc, char * argv[])
{
	const char * device = NULL;
	enum medic_mode mode = MEDIC_ZERO;
	uint32_t pattern = 0;
	int i, fd;
	
	for(i = 1; i < argc; i++)
	{
		if(argv[i][0] == '-')
		{
			if(!strcmp(argv[i], "-z"))
				mode = MEDIC_ZERO;
			else if(!strcmp(argv[i], "-p"))
			{
				if(++i < argc)
				{
					char * end = NULL;
					pattern = strtol(argv[i], &end, 16);
					if(end && *end)
						return erase_help_error();
					mode = MEDIC_PATTERN;
				}
				else
					return erase_help_error();
			}
			else if(!strcmp(argv[i], "-g"))
				mode = MEDIC_SCRG;
			else
				return erase_help_error();
		}
		else if(!device)
			device = argv[i];
		else
			return erase_help_error();
	}
	if(!device)
		return erase_help_error();
	
	switch(mode)
	{
		MD5_CTX ctx;
		case MEDIC_ZERO:
			memset(buffer, 0, BLOCK_SIZE);
			break;
		case MEDIC_PATTERN:
			static_assert(!(BLOCK_SIZE % sizeof(uint32_t)));
			for(i = 0; i < BLOCK_SIZE / sizeof(uint32_t); i++)
				((uint32_t *) &buffer[0])[i] = pattern;
			break;
		case MEDIC_SCRG:
			fd = open("/dev/urandom", O_RDONLY);
			if(fd >= 0)
			{
				for(i = 0; i < SCRG_SIZE; )
				{
					int r = read(fd, &buffer[i], SCRG_SIZE - i);
					if(r <= 0)
					{
						close(fd);
						goto pseudo;
					}
					i += r;
				}
				close(fd);
			}
			else
			{
			pseudo:
				fprintf(stderr, "Warning: /dev/urandom not available or not working; using rand() instead\n");
				srand(time(NULL));
				for(i = 0; i < SCRG_SIZE; i++);
					buffer[i] = rand();
			}
			MD5Init(&ctx);
			MD5Update(&ctx, buffer, SCRG_SIZE);
			MD5Final(&buffer[SCRG_SIZE], &ctx);
			break;
	}
	
	fd = open(device, O_WRONLY);
	if(fd < 0)
	{
		perror(device);
		return 1;
	}
	do
		i = write(fd, buffer, BLOCK_SIZE);
	while(i == BLOCK_SIZE);
	if(i < 0)
	{
		perror(device);
		close(fd);
		return 1;
	}
	if(i)
	{
		/* verify that this is the end of the device */
		i = write(fd, buffer, BLOCK_SIZE);
		if(i < 0)
		{
			perror(device);
			close(fd);
			return 1;
		}
		if(i)
		{
			fprintf(stderr, "%s: weird I/O error\n", device);
			close(fd);
			return 1;
		}
	}
	close(fd);
	return 0;
}

static void backup_help(int argc, char * argv[])
{
	printf("Usage: medic backup [options] <device> <file>\n");
	printf("Options:\n");
	printf("  -z           Backup skips zeroes (default)\n");
	printf("  -p <0xNN..>  Backup skips the given pattern\n");
	printf("  -g           Backup skips self-certifying random garbage\n");
	printf("  -v           Increase verbosity\n");
}

static int backup_help_error(void)
{
	backup_help(0, NULL);
	return 1;
}

static int medic_backup(int argc, char * argv[])
{
	struct medic_header header;
	struct medic_block block;
	const char * device = NULL;
	const char * filename = NULL;
	enum medic_mode mode = MEDIC_ZERO;
	uint32_t pattern = 0, count = 0;
	int i, fd, file, verbose = 0;
	
	for(i = 1; i < argc; i++)
	{
		if(argv[i][0] == '-')
		{
			if(!strcmp(argv[i], "-z"))
				mode = MEDIC_ZERO;
			else if(!strcmp(argv[i], "-p"))
			{
				if(++i < argc)
				{
					char * end = NULL;
					pattern = strtol(argv[i], &end, 16);
					if(end && *end)
						return backup_help_error();
					mode = MEDIC_PATTERN;
				}
				else
					return backup_help_error();
			}
			else if(!strcmp(argv[i], "-g"))
				mode = MEDIC_SCRG;
			else if(!strcmp(argv[i], "-v"))
				verbose++;
			else
				return backup_help_error();
		}
		else if(!device)
			device = argv[i];
		else if(!filename)
			filename = argv[i];
		else
			return backup_help_error();
	}
	if(!device || !filename)
		return backup_help_error();
	
	fd = open(device, O_RDONLY);
	if(fd < 0)
	{
		perror(device);
		return 1;
	}
	file = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0664);
	if(file < 0)
	{
		perror(filename);
		close(fd);
		return 1;
	}
	header.magic = MEDIC_MAGIC;
	header.version = MEDIC_VERSION;
	header.blocksize = BLOCK_SIZE;
	if(write(file, &header, sizeof(header)) != sizeof(header))
		goto fail;
	
	block.number = -1;
	for(;;)
	{
		for(i = 0; i < BLOCK_SIZE; )
		{
			int r = read(fd, &buffer[i], BLOCK_SIZE - i);
			if(r < 0)
				goto fail;
			if(!r)
				break;
			i += r;
		}
		if(!i)
			break;
		block.number++;
		block.size = i;
		if(i == BLOCK_SIZE)
		{
			switch(mode)
			{
				MD5_CTX ctx;
				uint8_t digest[sizeof(ctx.digest)];
				case MEDIC_ZERO:
					for(i = 0; i < BLOCK_SIZE; i++)
						if(buffer[i])
							break;
					i = (i < BLOCK_SIZE);
					break;
				case MEDIC_PATTERN:
					for(i = 0; i < BLOCK_SIZE / sizeof(uint32_t); i++)
						if(((uint32_t *) &buffer[0])[i] != pattern)
							break;
					i = (i < BLOCK_SIZE / sizeof(uint32_t));
					break;
				case MEDIC_SCRG:
					MD5Init(&ctx);
					MD5Update(&ctx, buffer, SCRG_SIZE);
					MD5Final(digest, &ctx);
					i = memcmp(digest, &buffer[SCRG_SIZE], sizeof(digest));
					break;
			}
		}
		if(i)
		{
			if(verbose > 1)
				printf("Saving block %u (size %u)\n", block.number, block.size);
			if(write(file, &block, sizeof(block)) != sizeof(block))
				goto fail;
			if(write(file, buffer, block.size) != block.size)
				goto fail;
			count++;
		}
	}
	if(verbose)
		printf("%u/%u blocks saved\n", count, block.number + 1);
	close(file);
	close(fd);
	return 0;
	
fail:
	/* might be device */
	perror(filename);
	close(file);
	unlink(filename);
	close(fd);
	return 1;
}

static void restore_help(int argc, char * argv[])
{
	printf("Usage: medic restore [options] <file> <device>\n");
	printf("Options:\n");
	printf("  -v           Increase verbosity\n");
	printf("  -n           Only simulate restore (implies -v -v)\n");
}

static int restore_help_error(void)
{
	restore_help(0, NULL);
	return 1;
}

static int medic_restore(int argc, char * argv[])
{
	struct medic_header header;
	struct medic_block block;
	const char * filename = NULL;
	const char * device = NULL;
	uint32_t count = 0;
	int i, fd = -1, file, verbose = 0, dry = 0;
	
	for(i = 1; i < argc; i++)
	{
		if(argv[i][0] == '-' && argv[i][1])
		{
			if(!strcmp(argv[i], "-v"))
				verbose++;
			else if(!strcmp(argv[i], "-n"))
			{
				dry = 1;
				if(verbose < 2)
					verbose = 2;
			}
			else
				return restore_help_error();
		}
		else if(!filename)
			filename = argv[i];
		else if(!device)
			device = argv[i];
		else
			return restore_help_error();
	}
	if(!filename || (!device && !dry))
		return restore_help_error();
	
	if(strcmp(filename, "-"))
	{
		file = open(filename, O_RDONLY);
		if(file < 0)
		{
			perror(filename);
			return 1;
		}
	}
	else
		file = 0;
	if(read(file, &header, sizeof(header)) != sizeof(header))
	{
		perror(filename);
		goto close_file;
	}
	if(header.magic != MEDIC_MAGIC || header.version != MEDIC_VERSION || header.blocksize != BLOCK_SIZE)
	{
		fprintf(stderr, "%s: unrecognized file format\n", filename);
		goto close_file;
	}
	if(!dry)
	{
		fd = open(device, O_WRONLY);
		if(fd < 0)
		{
			perror(device);
			goto close_file;
		}
	}
	
	for(;;)
	{
		int r = read(file, &block, sizeof(block));
		if(!r)
			break;
		if(r != sizeof(block))
			goto fail;
		if(block.size > BLOCK_SIZE)
		{
			fprintf(stderr, "%s: file format error", filename);
			goto close_fd;
		}
		for(i = 0; i < block.size; )
		{
			r = read(file, &buffer[i], block.size - i);
			if(r < 0)
				goto fail;
			if(!r)
				break;
			i += r;
		}
		if(i != block.size)
		{
			fprintf(stderr, "%s: unexpected end-of-file", filename);
			goto close_fd;
		}
		if(verbose > 1)
			printf("Restoring block %u (size %u)\n", block.number, block.size);
		if(!dry && pwrite(fd, buffer, block.size, block.number * (off_t) BLOCK_SIZE) != block.size)
			goto fail;
		count++;
	}
	if(verbose)
		printf("%u blocks restored\n", count);
	if(!dry)
		close(fd);
	if(file)
		close(file);
	return 0;
	
fail:
	/* might be device */
	perror(filename);
close_fd:
	if(!dry)
		close(fd);
close_file:
	if(file)
		close(file);
	return 1;
}

static void help_help(int argc, char * argv[]);
static int medic_help(int argc, char * argv[]);

static struct {
	const char * command;
	const char * description;
	int (*handler)(int, char *[]);
	void (*help)(int, char *[]);
} commands[] = {
	{"help", "Displays help.", medic_help, help_help},
	{"erase", "Erases a device.", medic_erase, erase_help},
	{"backup", "Backs up a device.", medic_backup, backup_help},
	{"restore", "Restores a device.", medic_restore, restore_help},
};
#define COMMANDS (sizeof(commands) / sizeof(commands[0]))

static void help_help(int argc, char * argv[])
{
	printf("Usage: medic help [command [options]]\n");
}

static int medic_help(int argc, char * argv[])
{
	int i;
	if(argc > 1)
		for(i = 0; i < COMMANDS; i++)
			if(!strcmp(argv[1], commands[i].command))
			{
				commands[i].help(argc - 1, &argv[1]);
				return 0;
			}
	printf("Usage: medic <command> [options]\n");
	printf("Commands:\n");
	for(i = 0; i < COMMANDS; i++)
		printf("  %-10s %s\n", commands[i].command, commands[i].description);
	return 0;
}

int main(int argc, char * argv[])
{
	if(argc > 1)
	{
		int i;
		for(i = 0; i < COMMANDS; i++)
			if(!strcmp(argv[1], commands[i].command))
				return commands[i].handler(argc - 1, &argv[1]);
		printf("Unknown command: %s\n", argv[1]);
		return 1;
	}
	medic_help(0, NULL);
	return 0;
}
