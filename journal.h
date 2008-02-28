/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_H
#define __JOURNAL_H

#ifdef __cplusplus
extern "C" {
#endif

/* a journal */
struct journal {
	int fd;
	patchgroup_id_t records;
	patchgroup_id_t commit;
	patchgroup_id_t playback;
	patchgroup_id_t erase;
};
typedef struct journal journal;

/* a pointer into a record in the journal */
struct journal_record {
	off_t offset;
	uint16_t length;
	uint16_t type;
};
typedef struct journal_record journal_record;

typedef int (*record_processor)(void * data, uint16_t length, uint16_t type, void * param);

/* creates a new journal */
journal * journal_create(int dfd, const char * path);

/* appends a record to the journal, optionally saving a pointer to it for later ammending */
int journal_append(journal * j, void * data, uint16_t length, uint16_t type, journal_record * location);
/* ammends a record in the journal with new data */
int journal_ammend(journal * j, const journal_record * location, void * data);

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

#ifdef __cplusplus
}
#endif

#endif /* __JOURNAL_H */
