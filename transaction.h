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

typedef int32_t tx_id;
typedef int tx_fd;

struct tx_pre_end {
	/* fill in the data and handle fields */
	void * data;
	void (*handle)(void * data);
	/* but leave this one alone */
	struct tx_pre_end * _next;
};

int tx_init(int dfd, size_t log_size);
void tx_deinit(void);

int tx_start(void);
/* adds a pre-end handler to the current transaction */
void tx_register_pre_end(struct tx_pre_end * handle);
/* tx_start_external() causes subsequent file operations until tx_end_external()
 * to become dependencies of this transaction - see journal::start_external() */
int tx_start_external(void);
int tx_end_external(int success);
tx_id tx_end(int assign_id);

int tx_sync(tx_id id);
int tx_forget(tx_id id);

tx_fd tx_open(int dfd, const char * name, int flags, ...);
int tx_emptyfile(tx_fd fd);
ssize_t tx_read(tx_fd fd, void * buf, size_t length, off_t offset);
/* while the same signature as pwrite(), only returns 0 vs. < 0 (not #bytes written) */
ssize_t tx_write(tx_fd fd, const void * buf, size_t length, off_t offset);
int tx_vnprintf(tx_fd fd, off_t offset, size_t max, const char * format, va_list ap);
int tx_nprintf(tx_fd fd, off_t offset, size_t max, const char * format, ...);
int tx_close(tx_fd fd);

/* see the detailed comments about this function in transaction.cpp before you use it */
int tx_unlink(int dfd, const char * name);

/* simple recursive transaction functions: the "real" transaction is the outermost one */

int tx_start_r(void);
int tx_end_r(void);

struct tx_handle {
	uint32_t in_tx;
};
#define TX_HANDLE_INIT(handle) do { (handle).in_tx = 0; } while(0)
#define IN_TX(handle) ((handle).in_tx > 0)
#define TX_START(handle) ({ int r = 0; if(!(handle).in_tx++) { r = tx_start_r(); if(r < 0) (handle).in_tx--; } assert((handle).in_tx); r; })
#define TX_END(handle) ({ int r = 0; assert((handle).in_tx); if(!--(handle).in_tx) { r = tx_end_r(); if(r < 0) (handle).in_tx++; } r; })
#define TX_CLEANUP(handle) do { if((handle).in_tx) { int r = tx_end_r(); assert(r >= 0); } (handle).in_tx = 0; } while(0)

#ifdef __cplusplus
}
#endif

#endif /* __TRANSACTION_H */
