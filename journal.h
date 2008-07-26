/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_H
#define __JOURNAL_H

#include <errno.h>
#include <stdint.h>
#include <sys/uio.h>

#ifndef __cplusplus
#error journal.h is a C++ header file
#endif

extern "C" {
/* Featherstitch does not know about C++ so we include
 * its header file inside an extern "C" block. */
#include <patchgroup.h>
}

#include "istr.h"
#include "rwfile.h"

#define J_COMMIT_EXT ".commit"
#define J_CHECKSUM_LEN 16

class journal
{
public:
	typedef int (*record_processor)(void * data, size_t length, void * param);
	
	/* appends a record to the journal */
	int appendv(const struct iovec * iovp, size_t count);
	inline int append(const void * data, size_t length)
	{
		struct iovec iov;
		iov.iov_base = (void *) data;
		iov.iov_len = length;
		return appendv(&iov, 1);
	}
	
	/* adds a patchgroup dependency to the (future) commit record, so that this journal will depend on it */
	int add_depend(patchgroup_id_t pid);
	
	/* commits a journal atomically, but does not block waiting for it */
	int commit();
	
	/* blocks waiting for a committed journal to be written to disk */
	inline int wait()
	{
		if(!last_commit)
			return -EINVAL;
		return patchgroup_sync(last_commit);
	}
	
	/* plays back a journal, possibly during recovery */
	int playback(record_processor processor, void * param);
	
	/* erases a journal after successful playback */
	int erase();
	
	/* releases the journal structure after erasure */
	int release();
	
	/* creates a new journal */
	static journal * create(int dfd, const char * path, journal * prev);
	
	/* reopens an existing journal if it is committed, otherwise leaves it alone */
	static int reopen(int dfd, const char * path, journal ** pj, journal * prev);
	
private:
	/* a commit record */
	struct commit_record {
		off_t offset;
		size_t length;
		uint8_t checksum[J_CHECKSUM_LEN];
	} __attribute__((packed));
	
	inline journal(const istr & path, int dfd, journal * prev)
		: path(path), dfd(dfd), crfd(-1), records(0), future(0), last_commit(0),
		  finished(0), erasure(0), prev(prev), commits(0), playbacks(0), usage(1)
	{
		prev_cr.offset = 0;
		prev_cr.length = 0;
		if(prev)
			prev->usage++;
	}
	inline ~journal() {}
	
	int checksum(off_t start, off_t end, uint8_t * checksum);
	int verify();
	
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
	patchgroup_id_t erasure;
	/* the previous journal */
	struct journal * prev;
	/* how many commits have been done on this journal */
	uint32_t commits;
	/* how many playbacks have been done on this journal */
	uint32_t playbacks;
	/* the most recent commit record data */
	commit_record prev_cr;
	/* usage count of this journal */
	int usage;
};

#endif /* __JOURNAL_H */
