/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TRANSACTION_H
#define __TRANSACTION_H

#include <sys/types.h>
#include <sys/stat.h>

typedef uint32_t tx_id;
typedef int tx_fd;

int tx_init(int dfd);
void tx_deinit(void);

int tx_start(void);
tx_id tx_end(int assign_id);

int tx_sync(tx_id id);
int tx_forget(tx_id id);

tx_fd tx_open(int dfd, const char * name, int flags, ...);
ssize_t tx_read(tx_fd fd, void * buf, size_t length);
int tx_write(tx_fd fd, const void * buf, off_t offset, size_t length, int copy);
int tx_fstat(tx_fd fd, struct stat * buf);
int tx_close(tx_fd fd);

#endif /* __TRANSACTION_H */
