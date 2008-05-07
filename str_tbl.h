/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STR_TBL_H
#define __STR_TBL_H

#include <stdint.h>
#include <sys/types.h>

#include "transaction.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A string table is a section of a file which maintains a collection of unique
 * strings in sorted order. String tables are immutable once created. */

#define ST_LRU 16

struct str_tbl {
	int fd;
	off_t start;
	ssize_t count;
	size_t size;
	uint8_t bytes[3];
	struct {
		ssize_t index;
		const char * string;
	} lru[ST_LRU];
	int lru_next;
};

int st_init(struct str_tbl * st, int fd, off_t start);
int st_kill(struct str_tbl * st);

/* The return value of st_get() is good until at least ST_LRU
 * more calls to st_get(), or one call to st_locate(). */
const char * st_get(const struct str_tbl * st, ssize_t index);
ssize_t st_locate(const struct str_tbl * st, const char * string);

const char ** st_read(struct str_tbl * st);
void st_array_free(const char ** array, ssize_t count);

/* leaves the input string array sorted */
int st_create(int fd, off_t * start, const char ** strings, ssize_t count);
int st_combine(int fd, off_t * start, struct str_tbl * st1, struct str_tbl * st2);

int st_create_tx(tx_fd fd, off_t * start, const char ** strings, ssize_t count);
int st_combine_tx(tx_fd fd, off_t * start, struct str_tbl * st1, struct str_tbl * st2);

#ifdef __cplusplus
}
#endif

#endif /* __STR_TBL_H */
