/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_H
#define __JOURNAL_H

#include <errno.h>
#include <stdint.h>

#ifndef __cplusplus
#error journal.h is a C++ header file
#endif

#include "config.h"

#if HAVE_FSTITCH
extern "C" {
/* Featherstitch does not know about C++ so we include
 * its header file inside an extern "C" block. */
#include <patchgroup.h>
}
#endif

#include "istr.h"
#include "rwfile.h"

#define J_COMMIT_EXT ".commit"
#define J_CHECKSUM_LEN 16
#define J_ADD_N_COMMITS 50 // In thousands of commits

class journal
{
public:
	typedef int (*record_processor)(void * data, size_t length, void * param);
	typedef int (*commit_hook)(void * param);
	struct ovec {
		const void * ov_base;
		size_t ov_len;
	};
	
	/* appends a record to the journal */
	int appendv(const struct ovec * ovp, size_t count);
	inline int append(const void * data, size_t length)
	{
		struct ovec ov;
		ov.ov_base = data;
		ov.ov_len = length;
		return appendv(&ov, 1);
	}
	
	/* start_external() causes subsequent file operations until end_external() to become
	 * dependencies of the (future) commit record, so that this journal will depend on them */
	int start_external();
	int end_external(bool success = true);
	
	/* commits a journal atomically, but does not block waiting for it */
	int commit();
	
	/* blocks waiting for a committed journal to be written to disk */
	int wait();
	
	/* plays back a journal, possibly during recovery */
	int playback(record_processor processor, commit_hook commit, void * param);
	
	/* erases a journal after successful playback */
	int erase();
	
	/* releases the journal structure after erasure */
	int release();
	
	/* creates a new journal */
	static journal * create(int dfd, const istr & path, journal * prev);
	
	/* reopens an existing journal if it is committed, otherwise leaves it alone */
	static int reopen(int dfd, const istr & path, const istr & commit_name, journal ** pj, journal * prev);
	
	/* number of bytes currently occupied by the journal */
	inline size_t size() const { return data_file.end() + (commits * sizeof(commit_record));}
	
private:
	/* a commit record */
	struct commit_record {
		off_t offset;
		size_t length;
		uint8_t checksum[J_CHECKSUM_LEN];
	} __attribute__((packed));
	
	inline journal(const istr & path, int dfd, journal * prev)
		: path(path), dfd(dfd), crfd(-1), records(0), last_commit(0),
		  finished(0), erasure(0), prev(prev), commits(0), playbacks(0),
		  usage(1), handler(this), ext_count(0), ext_success(false), external(0)
	{
		prev_cr.offset = 0;
		prev_cr.length = 0;
		if(prev)
			prev->usage++;
	}
	/* TODO: should we put something in here? */
	inline ~journal() {}
	
	class flush_handler : public rwfile::flush_handler
	{
	public:
		virtual int pre()
		{
			return (j->records > 0) ? patchgroup_engage(j->records) : 0;
		}
		virtual void post()
		{
			if(j->records > 0)
				patchgroup_disengage(j->records);
		}
		journal * j;
		inline flush_handler(journal * j) : j(j) {}
	};
	
	int checksum(off_t start, off_t end, uint8_t * checksum);
	int init_crfd(const istr & commit_name);
	int verify();
	
	istr path;
	int dfd, crfd;
	rwfile data_file;
	/* the records in this journal */
	patchgroup_id_t records;
	/* the most recent commit record */
	//patchgroup_id_t last_commit;
	uint32_t last_commit;
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
	flush_handler handler;
	/* external dependency state */
	int ext_count;
	bool ext_success;
	/* TODO: this can actually just use "records" above */
	patchgroup_id_t external;
};

#endif /* __JOURNAL_H */
