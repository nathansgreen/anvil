/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_H
#define __JOURNAL_H

#include <stdint.h>
#include <sys/uio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Featherstitch does not know about C++ so we include
 * its header file inside the extern "C" block. */
#include <patchgroup.h>

#define J_COMMIT_EXT ".commit"

struct journal;
typedef struct journal journal;

typedef int (*record_processor)(void * data, size_t length, void * param);

/* creates a new journal */
journal * journal_create(int dfd, const char * path, journal * prev);

/* appends a record to the journal */
int journal_append(journal * j, const void * data, size_t length);
int journal_appendv(journal * j, const struct iovec * iovp, size_t count);

/* adds a patchgroup dependency to the (future) commit record, so that this journal will depend on it */
int journal_add_depend(journal * j, patchgroup_id_t pid);

/* commits a journal atomically, but does not block waiting for it */
int journal_commit(journal * j);
/* blocks waiting for a committed journal to be written to disk */
int journal_wait(journal * j);

/* plays back a journal, possibly during recovery */
int journal_playback(journal * j, record_processor processor, void * param);

/* erases a journal after successful playback */
int journal_erase(journal * j);

/* frees the journal structure after erasure */
int journal_free(journal * j);

/* reopens an existing journal if it is committed, otherwise leaves it alone */
int journal_reopen(int dfd, const char * path, journal ** pj, journal * prev);

#ifdef __cplusplus
}

#include "istr.h"
#include "rwfile.h"

#define J_CHECKSUM_LEN 16

/* a commit record */
struct commit_record {
	off_t offset;
	size_t length;
	uint8_t checksum[J_CHECKSUM_LEN];
} __attribute__((packed));

/* a journal */
struct journal {
	istr path;
	int dfd, crfd;
	rwfile data_file;
	/* the records in this journal */
	patchgroup_id_t records;
	/* what will be the next commit record */
	patchgroup_id_t future;
	/* the most recent commit record */
	patchgroup_id_t last_commit;
	/* depends on all the playbacks; will be the erasure */
	patchgroup_id_t finished;
	/* the erasure of this journal */
	patchgroup_id_t erase;
	/* the previous journal */
	struct journal * prev;
	/* how many commits have been done on this journal */
	uint32_t commits;
	/* how many playbacks have been done on this journal */
	uint32_t playbacks;
	/* the most recent commit record data */
	struct commit_record prev_cr;
	/* usage count of this journal */
	int usage;
};

#endif

#endif /* __JOURNAL_H */
