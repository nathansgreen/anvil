/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/uio.h>

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

/* this is the record actually written into the journal file */
struct record {
	size_t length;
};

journal * journal_create(int dfd, const char * path, journal * prev)
{
	journal * j;
	if(prev && !prev->last_commit)
		return NULL;
	j = new journal;
	if(!j)
		return NULL;
	j->path = path;
	if(!j->path)
		goto error;
	j->dfd = dfd;
	j->crfd = -1;
	/* hmm... should we include the openat() in the patchgroup? */
	if(j->data_file.create(dfd, path) < 0)
		goto error;
	j->records = 0;
	j->future = 0;
	j->last_commit = 0;
	j->finished = 0;
	j->erase = 0;
	j->prev = prev;
	j->commits = 0;
	j->playbacks = 0;
	j->prev_cr.offset = 0;
	j->prev_cr.length = 0;
	j->usage = 1;
	if(prev)
		prev->usage++;
	return j;
	
error:
	int save = errno;
	delete j;
	errno = save;
	return NULL;
}

int journal_appendv(journal * j, const struct iovec * iovp, size_t count)
{
	struct record header;
	uint8_t * data;
	off_t offset;
	size_t i, cursor;
	if(count < 1 || j->erase)
		return -EINVAL;
	header.length = iovp[0].iov_len;
	for(i = 1; i < count; i++)
		header.length += iovp[i].iov_len;
	data = (uint8_t *) malloc(header.length + sizeof(header));
	cursor = sizeof(header);
	memcpy(data, &header, sizeof(header));
	for(i = 0; i < count; i++)
	{
		memcpy(&data[cursor], iovp[i].iov_base, iovp[i].iov_len);
		cursor += iovp[i].iov_len;
	}
	assert(cursor == sizeof(header) + header.length);
	
	if(!j->records)
	{
		j->records = patchgroup_create(0);
		if(j->records <= 0)
			return -1;
		patchgroup_label(j->records, "records");
		patchgroup_release(j->records);
		j->data_file.set_pid(j->records);
	}
	offset = j->data_file.end();
	if(j->data_file.append(data, header.length + sizeof(header)) != (ssize_t) (sizeof(header) + header.length))
	{
		int save = errno;
		j->data_file.truncate(offset);
		/* make sure the pointer is not past the end of the file */
		errno = save;
		free(data);
		return -1;
	}
	free(data);
	return 0;
}

int journal_append(journal * j, const void * data, size_t length)
{
	struct iovec iov;
	iov.iov_base = (void *) data;
	iov.iov_len = length;
	return journal_appendv(j, &iov, 1);
}

int journal_add_depend(journal * j, patchgroup_id_t pid)
{
	if(j->erase)
		return -EINVAL;
	if(!j->future)
	{
		patchgroup_id_t future = patchgroup_create(0);
		if(future <= 0)
			return -1;
		patchgroup_label(future, "future");
		patchgroup_label(pid, "external dependency");
		j->future = future;
	}
	return patchgroup_add_depend(j->future, pid);
}

static int journal_checksum(journal * j, off_t start, off_t end, uint8_t * checksum)
{
	/* this can be replaced with any other hash function;
	 * I already had MD5 lying around so I used it here */
	MD5_CTX ctx;
	uint8_t buffer[4096];
	ssize_t size;
	MD5Init(&ctx);
	size = j->data_file.read(start, buffer, sizeof(buffer));
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
		size = j->data_file.read(start, buffer, sizeof(buffer));
	}
	if(size < 0)
		return -1;
	MD5Final(checksum, &ctx);
	return 0;
}

int journal_commit(journal * j)
{
	struct commit_record cr;
	patchgroup_id_t commit;
	
	if(j->erase)
		return -EINVAL;
	if(j->prev && !j->prev->last_commit)
		/* or commit it here? */
		return -EINVAL;
	if(j->commits > j->playbacks)
		/* must play back previous commit first */
		return -EINVAL;
	assert(j->playbacks == j->commits);
	
	if(!j->records)
		return 0;
	
	if(j->crfd < 0)
	{
		j->crfd = openat(j->dfd, j->path + J_COMMIT_EXT, O_CREAT | O_RDWR | O_APPEND, 0644);
		if(j->crfd < 0)
			return -1;
	}
	else
		lseek(j->crfd, 0, SEEK_END);
	cr.offset = j->prev_cr.offset + j->prev_cr.length;
	cr.length = j->data_file.end() - cr.offset;
	if(journal_checksum(j, cr.offset, cr.offset + cr.length, cr.checksum) < 0)
		return -1;
	if(!j->future)
	{
		commit = patchgroup_create(0);
		if(commit <= 0)
			return -1;
	}
	else
		commit = j->future;
	patchgroup_label(commit, "commit");
	
	j->data_file.set_pid(0);
	if(patchgroup_add_depend(commit, j->records) < 0)
	{
	fail:
		j->data_file.set_pid(j->records);
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}
	if(j->last_commit > 0 && patchgroup_add_depend(commit, j->last_commit) < 0)
		goto fail;
	if(j->prev && !j->commits && patchgroup_add_depend(commit, j->prev->last_commit) < 0)
		goto fail;
	patchgroup_release(commit);
	
	if(patchgroup_engage(commit) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(commit);
		return -1;
	}
	if(write(j->crfd, &cr, sizeof(cr)) != sizeof(cr))
	{
		int save = errno;
		patchgroup_disengage(commit);
		/* the truncate() really should be part of j->records, but
		 * since commit depends on j->records, we'll substitute it */
		patchgroup_abandon(j->records);
		/* make sure the pointer is not past the end of the file */
		errno = save;
		return -1;
	}
	patchgroup_disengage(commit);
	j->future = 0;
	patchgroup_abandon(j->records);
	j->records = 0;
	if(j->last_commit)
		patchgroup_abandon(j->last_commit);
	j->last_commit = commit;
	j->commits++;
	j->prev_cr = cr;
	return 0;
}

int journal_wait(journal * j)
{
	if(!j->last_commit)
		return -EINVAL;
	return patchgroup_sync(j->last_commit);
}

int journal_playback(journal * j, record_processor processor, void * param)
{
	patchgroup_id_t playback;
	struct record header;
	struct commit_record cr;
	uint8_t buffer[65536];
	int r = -1;
	if(j->erase)
		return -EINVAL;
	if(j->playbacks == j->commits)
		/* nothing to play back */
		return 0;
	assert(j->playbacks < j->commits);
	playback = patchgroup_create(0);
	if(playback <= 0)
		return -1;
	patchgroup_label(playback, "playback");
	if(j->last_commit > 0 && patchgroup_add_depend(playback, j->last_commit) < 0)
	{
		patchgroup_release(playback);
		patchgroup_abandon(playback);
		return -1;
	}
	patchgroup_release(playback);
	if(j->last_commit > 0)
		/* only replay the records from the last commit */
		lseek(j->crfd, -sizeof(cr), SEEK_END);
	else
		/* if we haven't commited anything replay the whole journal */
		lseek(j->crfd, 0, SEEK_SET);
	
	if(patchgroup_engage(playback) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(playback);
		return -1;
	}
	while(read(j->crfd, &cr, sizeof(cr)) == sizeof(cr)) 
	{
		off_t curoff = cr.offset;
		while((size_t)(curoff - cr.offset) < cr.length)
		{
			if(j->data_file.read(curoff, &header, sizeof(header)) != sizeof(header))
			{
				patchgroup_disengage(playback);
				patchgroup_abandon(playback);
				return -1;
			}
			if(j->data_file.read(curoff + sizeof(header), buffer, header.length) != (ssize_t) header.length)
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
	if(!j->finished)
	{
		patchgroup_id_t finished = patchgroup_create(0);
		if(finished <= 0)
		{
			patchgroup_abandon(playback);
			return -1;
		}
		patchgroup_label(finished, "finished");
		r = patchgroup_add_depend(finished, playback);
		if(r < 0)
		{
			patchgroup_release(finished);
			patchgroup_abandon(finished);
			patchgroup_abandon(playback);
			return r;
		}
		j->finished = finished;
	}
	patchgroup_abandon(playback);
	j->playbacks = j->commits;
	return 0;
}

int journal_erase(journal * j)
{
	int r;
	if(j->records || j->future)
		return -EBUSY;
	if(j->commits > j->playbacks)
		/* must play back previous commit first */
		return -EBUSY;
	if(j->erase && j->crfd < 0)
		return -EINVAL;
	if(j->prev && !j->prev->erase)
		/* or erase it here? */
		return -EINVAL;
	if(!j->erase)
	{
		if(j->finished)
			j->erase = j->finished;
		else
		{
			j->erase = patchgroup_create(0);
			if(j->erase <= 0)
				return -1;
		}
		patchgroup_label(j->erase, "delete");
	}
	if(j->prev && patchgroup_add_depend(j->erase, j->prev->erase) < 0)
		return -1;
	r = patchgroup_release(j->erase);
	assert(r >= 0);
	r = patchgroup_engage(j->erase);
	assert(r >= 0);
	
	unlinkat(j->dfd, j->path, 0);
	unlinkat(j->dfd, j->path + J_COMMIT_EXT, 0);
	
	r = patchgroup_disengage(j->erase);
	assert(r >= 0);
	
	r = j->data_file.close();
	assert(r >= 0);
	close(j->crfd);
	j->crfd = -1;
	if(j->prev)
	{
		if(!j->finished)
		{
			assert(!j->last_commit);
			j->last_commit = j->prev->last_commit;
			j->prev->last_commit = 0;
		}
		journal_free(j->prev);
	}
	return 0;
}

int journal_free(journal * j)
{
	if(!j->erase || j->crfd >= 0)
		return -EINVAL;
	if(--j->usage > 0)
		return 0;
	assert(!j->records);
	assert(!j->future);
	if(j->last_commit)
		patchgroup_abandon(j->last_commit);
	patchgroup_abandon(j->erase);
	/* no need to unlink, since those were done in journal_erase() */
	delete j;
	return 0;
}

/* returns 1 for OK, 0 for failure, and < 0 on I/O error */
static int journal_verify(journal * j)
{
	struct commit_record cr;
	uint8_t checksum[J_CHECKSUM_LEN];
	if(j->crfd < 0)
		return -1;
	lseek(j->crfd, 0, SEEK_SET);
	for(uint32_t i = 0; i < j->commits; ++i)
	{
		if(read(j->crfd, &cr, sizeof(cr)) != sizeof(cr))
			return -1;
		if(journal_checksum(j, cr.offset, cr.offset + cr.length, checksum) < 0)
			return -1;
		if(memcmp(cr.checksum, checksum, J_CHECKSUM_LEN))
			return 0;
	}
	return 1;
}

int journal_reopen(int dfd, const char * path, journal ** pj, journal * prev)
{
	int r;
	journal * j;
	off_t offset;
	
	if(prev && !prev->last_commit)
		return -EINVAL;
	j = new journal;
	if(!j)
		return -1;
	j->dfd = dfd;
	j->path = path;
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
	j->playbacks = 0;
	j->records = 0;
	j->future = 0;
	j->last_commit = 0;
	j->finished = 0;
	j->erase = 0;
	j->prev = prev;
	j->usage = 1;
	if(prev)
		prev->usage++;
	/* get rid of any uncommited records that might be in the journal */
	r = j->data_file.open(dfd, path, j->prev_cr.offset + j->prev_cr.length);
	if(r < 0 || (r = journal_verify(j)) <= 0)
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
	delete j;
	errno = save;
	return -1;
}
