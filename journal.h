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

#include <patchgroup.h>

/* a journal */
struct journal {
	int fd, dfd;
	char * path;
	patchgroup_id_t records;
	patchgroup_id_t commit;
	patchgroup_id_t playback;
	patchgroup_id_t erase;
	struct journal * prev;
	int usage;
};
typedef struct journal journal;

/* a pointer into a record in the journal */
struct journal_record {
	off_t offset;
	size_t length;
};
typedef struct journal_record journal_record;

typedef int (*record_processor)(void * data, size_t length, void * param);

/* creates a new journal */
journal * journal_create(int dfd, const char * path, journal * prev);

/* appends a record to the journal, optionally saving a pointer to it for later ammending */
int journal_append(journal * j, const void * data, size_t length, journal_record * location);
int journal_appendv4(journal * j, const struct iovec * iovp, size_t count, journal_record * location);
/* amends a record in the journal with new data */
int journal_amend(journal * j, const journal_record * location, const void * data);
int journal_amendv4(journal * j, const journal_record * location, const struct iovec * iovp, size_t count);

/* commits a journal atomically, but does not block waiting for it */
int journal_commit(journal * j);
/* blocks waiting for a committed journal to be written to disk */
int journal_wait(journal * j);

/* aborts a journal, erasing it before committing it and freeing the structure */
int journal_abort(journal * j);

/* plays back a journal, possibly during recovery */
int journal_playback(journal * j, record_processor processor, void * param);

/* erases a journal after successful playback */
int journal_erase(journal * j);
/* blocks waiting for an erased journal to be erased on disk */
int journal_flush(journal * j);

/* frees the journal structure after erasure */
int journal_free(journal * j);

/* reopens an existing journal if it is committed, otherwise leaves it alone */
int journal_reopen(int dfd, const char * path, journal ** pj, journal * prev);

#ifdef __cplusplus
}
#endif

#endif /* __JOURNAL_H */
