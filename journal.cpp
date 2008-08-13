/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "md5.h"
#include "openat.h"
#include "journal.h"

#include "istr.h"
#include "rwfile.h"

/* This is the generic journal module. It uses Featherstitch dependencies to
 * keep a journal of uninterpreted records, which later can be played back
 * either to commit transactions or to recover them. The records do not
 * necessarily have to describe idempotent actions, but if they do not, the
 * client code must be able to figure out whether a record's action has already
 * been taken or not so as not to perform it twice in the event of recovery. */

struct data_header {
	size_t length;
} __attribute__((packed));

journal * journal::create(int dfd, const istr & path, journal * prev)
{
	journal * j;
	if(prev && !prev->last_commit)
		return NULL;
	j = new journal(path, dfd, prev);
	if(!j)
		return NULL;
	/* hmm... should we include this create() in the patchgroup? */
	if(!j->path || j->data_file.create(dfd, path) < 0)
	{
		int save = errno;
		delete j;
		errno = save;
		return NULL;
	}
	return j;
}

int journal::appendv(const struct ovec * ovp, size_t count)
{
	data_header header;
	uint8_t * data;
	off_t offset;
	size_t i, cursor;
	if(count < 1 || erasure)
		return -EINVAL;
	header.length = ovp[0].ov_len;
	for(i = 1; i < count; i++)
		header.length += ovp[i].ov_len;
	data = (uint8_t *) malloc(header.length + sizeof(header));
	cursor = sizeof(header);
	memcpy(data, &header, sizeof(header));
	for(i = 0; i < count; i++)
	{
		memcpy(&data[cursor], ovp[i].ov_base, ovp[i].ov_len);
		cursor += ovp[i].ov_len;
	}
	assert(cursor == sizeof(header) + header.length);
	
	if(!records)
	{
		records = patchgroup_create(0);
		if(records <= 0)
			return -1;
		patchgroup_label(records, "records");
		patchgroup_release(records);
		data_file.set_pid(records);
	}
	offset = data_file.end();
	if(data_file.append(data, header.length + sizeof(header)) != (ssize_t) (sizeof(header) + header.length))
	{
		int save = errno;
		data_file.truncate(offset);
		/* make sure the pointer is not past the end of the file */
		errno = save;
		free(data);
		return -1;
	}
	free(data);
	return 0;
}

int journal::add_depend(patchgroup_id_t pid)
{
	if(erasure)
		return -EINVAL;
	if(!future)
	{
		future = patchgroup_create(0);
		if(future <= 0)
			return -1;
		patchgroup_label(future, "future");
		patchgroup_label(pid, "external dependency");
	}
	return patchgroup_add_depend(future, pid);
}

int journal::checksum(off_t start, off_t end, uint8_t * checksum)
{
	/* this can be replaced with any other hash function;
	 * I already had MD5 lying around so I used it here */
	MD5_CTX ctx;
	uint8_t buffer[4096];
	ssize_t size;
	MD5Init(&ctx);
	size = data_file.read(start, buffer, sizeof(buffer));
	while(size > 0 && end > start)
	{
		if(size > end)
		{
			size = end;
			end = 0;
		}
		else
			end -= size;
		MD5Update(&ctx, buffer, size);
		size = data_file.read(start, buffer, sizeof(buffer));
	}
	if(size < 0)
		return -1;
	MD5Final(checksum, &ctx);
	return 0;
}

int journal::commit()
{
	commit_record cr;
	patchgroup_id_t commit;
	
	if(erasure)
		return -EINVAL;
	if(prev && !prev->last_commit)
		/* or commit it here? */
		return -EINVAL;
	if(commits > playbacks)
		/* must play back previous commit first */
		return -EINVAL;
	assert(playbacks == commits);
	
	if(!records)
		return 0;
	
	if(crfd < 0)
	{
		crfd = openat(dfd, path + J_COMMIT_EXT, O_CREAT | O_RDWR | O_APPEND, 0644);
		if(crfd < 0)
			return -1;
	}
	else
		lseek(crfd, 0, SEEK_END);
	cr.offset = prev_cr.offset + prev_cr.length;
	cr.length = data_file.end() - cr.offset;
	if(checksum(cr.offset, cr.offset + cr.length, cr.checksum) < 0)
		return -1;
	if(!future)
	{
		commit = patchgroup_create(0);
		if(commit <= 0)
			return -1;
	}
	else
		commit = future;
	patchgroup_label(commit, "commit");
	
	data_file.set_pid(0);
	if(patchgroup_add_depend(commit, records) < 0)
	{
	fail:
		data_file.set_pid(records);
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}
	if(last_commit > 0 && patchgroup_add_depend(commit, last_commit) < 0)
		goto fail;
	if(prev && !commits && patchgroup_add_depend(commit, prev->last_commit) < 0)
		goto fail;
	patchgroup_release(commit);
	
	if(patchgroup_engage(commit) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(commit);
		return -1;
	}
	if(write(crfd, &cr, sizeof(cr)) != sizeof(cr))
	{
		int save = errno;
		patchgroup_disengage(commit);
		/* the truncate() really should be part of records, but
		 * since commit depends on records, we'll substitute it */
		patchgroup_abandon(records);
		/* make sure the pointer is not past the end of the file */
		errno = save;
		return -1;
	}
	patchgroup_disengage(commit);
	future = 0;
	patchgroup_abandon(records);
	records = 0;
	if(last_commit)
		patchgroup_abandon(last_commit);
	last_commit = commit;
	commits++;
	prev_cr = cr;
	return 0;
}

int journal::playback(record_processor processor, void * param)
{
	patchgroup_id_t playback;
	data_header header;
	commit_record cr;
	uint8_t buffer[65536];
	int r = -1;
	if(erasure)
		return -EINVAL;
	if(playbacks == commits)
		/* nothing to play back */
		return 0;
	assert(playbacks < commits);
	playback = patchgroup_create(0);
	if(playback <= 0)
		return -1;
	patchgroup_label(playback, "playback");
	if(last_commit > 0 && patchgroup_add_depend(playback, last_commit) < 0)
	{
		patchgroup_release(playback);
		patchgroup_abandon(playback);
		return -1;
	}
	patchgroup_release(playback);
	if(last_commit > 0)
		/* only replay the records from the last commit */
		lseek(crfd, -sizeof(cr), SEEK_END);
	else
		/* if we haven't commited anything replay the whole journal */
		lseek(crfd, 0, SEEK_SET);
	
	if(patchgroup_engage(playback) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(playback);
		return -1;
	}
	while(read(crfd, &cr, sizeof(cr)) == sizeof(cr)) 
	{
		off_t curoff = cr.offset;
		while((size_t) (curoff - cr.offset) < cr.length)
		{
			if(data_file.read(curoff, &header, sizeof(header)) != sizeof(header))
			{
				patchgroup_disengage(playback);
				patchgroup_abandon(playback);
				return -1;
			}
			if(data_file.read(curoff + sizeof(header), buffer, header.length) != (ssize_t) header.length)
			{
				patchgroup_disengage(playback);
				patchgroup_abandon(playback);
				return -1;
			}
			curoff += sizeof(header) + header.length;
			r = processor(buffer, header.length, param);
			if(r < 0)
			{
				patchgroup_disengage(playback);
				patchgroup_abandon(playback);
				return r;
			}
		}
	}
	patchgroup_disengage(playback);
	if(!finished)
	{
		finished = patchgroup_create(0);
		if(finished <= 0)
		{
			patchgroup_abandon(playback);
			return -1;
		}
		patchgroup_label(finished, "finished");
	}
	r = patchgroup_add_depend(finished, playback);
	if(r < 0)
	{
		patchgroup_abandon(playback);
		return r;
	}
	patchgroup_abandon(playback);
	playbacks = commits;
	return 0;
}

int journal::erase()
{
	int r;
	if(records || future)
		return -EBUSY;
	if(commits > playbacks)
		/* must play back previous commit first */
		return -EBUSY;
	if(erasure && crfd < 0)
		return -EINVAL;
	if(prev && !prev->erasure)
		/* or erase it here? */
		return -EINVAL;
	if(!erasure)
	{
		if(finished)
			erasure = finished;
		else
		{
			erasure = patchgroup_create(0);
			if(erasure <= 0)
				return -1;
		}
		patchgroup_label(erasure, "delete");
	}
	if(prev && patchgroup_add_depend(erasure, prev->erasure) < 0)
		return -1;
	r = patchgroup_release(erasure);
	assert(r >= 0);
	r = patchgroup_engage(erasure);
	assert(r >= 0);
	
	unlinkat(dfd, path, 0);
	unlinkat(dfd, path + J_COMMIT_EXT, 0);
	
	r = patchgroup_disengage(erasure);
	assert(r >= 0);
	
	r = data_file.close();
	assert(r >= 0);
	close(crfd);
	crfd = -1;
	if(prev)
	{
		if(!finished)
		{
			assert(!last_commit);
			last_commit = prev->last_commit;
			prev->last_commit = 0;
		}
		prev->release();
	}
	return 0;
}

int journal::release()
{
	if(!erasure || crfd >= 0)
		return -EINVAL;
	if(--usage > 0)
		return 0;
	assert(!records);
	assert(!future);
	if(last_commit)
		patchgroup_abandon(last_commit);
	patchgroup_abandon(erasure);
	/* no need to unlink, since those were done in erase() */
	delete this;
	return 0;
}

/* returns 1 for OK, 0 for failure, and < 0 on I/O error */
int journal::verify()
{
	commit_record cr;
	uint8_t actual[J_CHECKSUM_LEN];
	if(crfd < 0)
		return -1;
	lseek(crfd, 0, SEEK_SET);
	for(uint32_t i = 0; i < commits; i++)
	{
		if(read(crfd, &cr, sizeof(cr)) != sizeof(cr))
			return -1;
		if(checksum(cr.offset, cr.offset + cr.length, actual) < 0)
			return -1;
		if(memcmp(cr.checksum, actual, J_CHECKSUM_LEN))
			return 0;
	}
	return 1;
}

int journal::reopen(int dfd, const istr & path, journal ** pj, journal * prev)
{
	int r;
	journal * j;
	off_t offset;
	
	if(prev && !prev->last_commit)
		return -EINVAL;
	j = new journal(path, dfd, prev);
	if(!j)
		return -1;
	if(!j->path)
		goto error;
	/* do we want to O_CREAT here? */
	j->crfd = openat(dfd, j->path + J_COMMIT_EXT, O_CREAT | O_RDWR, 0644);
	if(j->crfd < 0)
		goto error;
	lseek(j->crfd, -sizeof(j->prev_cr), SEEK_END);
	r = read(j->crfd, &j->prev_cr, sizeof(j->prev_cr));
	if(r != (int) sizeof(j->prev_cr))
		return (r < 0) ? r : -1;
	offset = lseek(j->crfd, 0, SEEK_END);
	j->commits = offset / sizeof(j->prev_cr);
	/* get rid of any uncommited records that might be in the journal */
	r = j->data_file.open(dfd, path, j->prev_cr.offset + j->prev_cr.length);
	if(r < 0 || (r = j->verify()) <= 0)
	{
		if(prev)
			prev->usage--;
		j->data_file.close();
		close(j->crfd);
		delete j;
		j = NULL;
	}
	*pj = j;
	return (r < 0) ? r : 0;
	
error:
	int save = errno;
	if(prev)
		prev->usage--;
	delete j;
	errno = save;
	return -1;
}
