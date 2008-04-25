/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TRANSACTION_H
#define __TRANSACTION_H

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t tx_id;
typedef int tx_fd;

int tx_init(int dfd);
void tx_deinit(void);

int tx_start(void);
tx_id tx_end(int assign_id);

int tx_sync(tx_id id);
int tx_forget(tx_id id);

tx_fd tx_open(int dfd, const char * name, int flags, ...);
int tx_read_fd(tx_fd fd);
/* note the same signature as pwrite() */
int tx_write(tx_fd fd, const void * buf, size_t length, off_t offset);
int tx_vnprintf(tx_fd fd, off_t offset, size_t max, const char * format, va_list ap);
int tx_nprintf(tx_fd fd, off_t offset, size_t max, const char * format, ...);
int tx_close(tx_fd fd);

int tx_unlink(int dfd, const char * name);
int tx_rename(int old_dfd, const char * old_name, int new_dfd, const char * new_name);

#ifdef __cplusplus
}
#endif

#endif /* __TRANSACTION_H */
