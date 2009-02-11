/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __METAFILE_H
#define __METAFILE_H

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* metafile transactions */
typedef int32_t mftx_id;

int mftx_init(int dfd, size_t log_size);
void mftx_deinit(void);

int mftx_start(void);
/* mftx_start_external() causes subsequent file operations until mftx_end_external()
 * to become dependencies of this transaction - see journal::start_external() */
int mftx_start_external(void);
int mftx_end_external(int success);
mftx_id mftx_end(int assign_id);

int mftx_sync(mftx_id id);
int mftx_forget(mftx_id id);

/* metafiles */
typedef struct metafile * mf_fp;

mf_fp mf_open(int dfd, const char * name, int create);
size_t mf_size(const mf_fp file);
int mf_dirty(const mf_fp file);
/* reads some part of the file */
size_t mf_read(const mf_fp file, void * buf, size_t length, off_t offset);
/* truncates the file to 0 size */
int mf_truncate(mf_fp file);
/* writes some part of the metafile as part of the current transaction */
int mf_write(mf_fp file, const void * buf, size_t length, off_t offset);
int mf_close(mf_fp file);

int mf_unlink(int dfd, const char * name, int recursive);

/* simple recursive transactions: the "real" transaction is the outermost one */
int mftx_start_r(void);
int mftx_end_r(void);

struct mftx_handle {
	uint32_t in_tx;
};
#define MFTX_HANDLE_INIT(handle) do { (handle).in_tx = 0; } while(0)
#define MFTX_IN_TX(handle) ((handle).in_tx > 0)
#define MFTX_START(handle) ({ int r = 0; if(!(handle).in_tx++) { r = tx_start_r(); if(r < 0) (handle).in_tx--; } assert((handle).in_tx); r; })
#define MFTX_END(handle) ({ int r = 0; assert((handle).in_tx); if(!--(handle).in_tx) { r = tx_end_r(); if(r < 0) (handle).in_tx++; } r; })
#define MFTX_CLEANUP(handle) do { if((handle).in_tx) { int r = tx_end_r(); assert(r >= 0); } (handle).in_tx = 0; } while(0)

#ifdef __cplusplus
}
#endif

#endif /* __METAFILE_H */
