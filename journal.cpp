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
	j->dfd = dfd;
	j->path = path;
	if(!j->path)
		goto error;
	/* hmm... should we include the openat() in the patchgroup? */
	if(j->data_file.create(dfd, path) < 0)
		goto error;
	j->records = 0;
	j->crfd = -1;
	j->future = 0;
	j->last_commit = 0;
	j->playback = 0;
	j->erase = 0;
	j->prev = prev;
	j->commit_groups = 0;
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

int journal_appendv4(journal * j, const struct iovec * iovp, size_t count)
{
	struct record header;
	off_t offset;
	size_t i;
	if(count > 4)
		return -EINVAL;
	header.length = iovp[0].iov_len;
	for(i = 1; i < count; i++)
		header.length += iovp[i].iov_len;
	if(header.length == (size_t) -1)
		return -EINVAL;
	uint8_t * data = (uint8_t *)malloc(header.length + sizeof(header));
	uint8_t * cursor = data;
	memcpy(cursor, &header, sizeof(header));
	cursor += sizeof(header);
	for(i = 0; i < count; i++)
	{
		memcpy(cursor, iovp[i].iov_base, iovp[i].iov_len);
		cursor += iovp[i].iov_len;
	}
	assert((size_t)(cursor - data) == (header.length + sizeof(header)));

	offset = j->data_file.end();
	if(!j->records)
	{
		j->records = patchgroup_create(0);
		if(j->records <= 0)
			return -1;
		patchgroup_label(j->records, "records");
		patchgroup_release(j->records);
		j->data_file.set_pid(j->records);
	}
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
	return journal_appendv4(j, &iov, 1);
}

int journal_add_depend(journal * j, patchgroup_id_t pid)
{
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

#define CHECKSUM_LENGTH 16
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
	uint8_t checksum[CHECKSUM_LENGTH];
	struct commit_record cr;
	patchgroup_id_t commit;
	struct iovec iov[2];
	off_t offset;
	
	if(j->prev && !j->prev->last_commit)
		/* or commit it here? */
		return -EINVAL;
	
	if(j->crfd < 0)
	{
		j->crfd = openat(j->dfd, j->path + J_COMMIT_EXT, O_CREAT | O_RDWR | O_APPEND, 0644);
		if(j->crfd < 0)
			return -1;
	}
	else
		lseek(j->crfd, 0, SEEK_END);
	offset = j->data_file.end();
	cr.offset = j->prev_cr.offset + j->prev_cr.length;
	cr.length = offset - cr.offset;
	if(journal_checksum(j, cr.offset, cr.offset + cr.length, checksum) < 0)
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
	if(!j->records)
		return 0;
	
	j->data_file.set_pid(0);
	if((patchgroup_add_depend(commit, j->records) < 0) ||
	   ((j->last_commit > 0) && (patchgroup_add_depend(commit, j->last_commit) < 0)))
	{
	fail:
		j->data_file.set_pid(j->records);
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}
	
	if(j->prev && !j->commit_groups && patchgroup_add_depend(commit, j->prev->last_commit) < 0)
		goto fail;
	patchgroup_release(commit);
	iov[0].iov_base = &cr;
	iov[0].iov_len = sizeof(cr);
	iov[1].iov_base = checksum;
	iov[1].iov_len = sizeof(checksum);
	if(patchgroup_engage(commit) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(commit);
		return -1;
	}
	if(writev(j->crfd, iov, 2) != sizeof(cr) + sizeof(checksum))
	{
		int save = errno;
		j->data_file.truncate(offset);
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
	j->records = 0;
	j->last_commit = commit;
	j->commit_groups++;
	j->prev_cr = cr;
	return 0;
}

int journal_wait(journal * j)
{
	if(!j->last_commit)
		return -EINVAL;
	return patchgroup_sync(j->last_commit);
}

int journal_abort(journal * j)
{
	if(j->records)
		patchgroup_abandon(j->records);
	if(j->prev)
		journal_free(j->prev);
	j->data_file.close();
	unlinkat(j->dfd, j->path, 0);
	unlinkat(j->dfd, j->path + J_COMMIT_EXT, 0);
	delete j;
	return 0;
}

int journal_playback(journal * j, record_processor processor, void * param)
{
	patchgroup_id_t playback;
	struct record header;
	struct commit_record cr;
	uint8_t buffer[65536];
	int r = -1;
	if(!j->commit_groups)
		return -EINVAL;
	playback = patchgroup_create(0);
	if(playback <= 0)
		return -1;
	patchgroup_label(playback, "playback");
	if(j->last_commit && patchgroup_add_depend(playback, j->last_commit) < 0)
	{
		patchgroup_release(playback);
		patchgroup_abandon(playback);
		return -1;
	}
	patchgroup_release(playback);
	if(!j->last_commit)
		/* if we haven't commited anything replay the whole journal */
		lseek(j->crfd, 0, SEEK_SET);
	else
		/* only replay the records from the last commit */
		lseek(j->crfd, -(sizeof(cr) + CHECKSUM_LENGTH), SEEK_END);
	
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
				r = -1;
				goto fail;
			}
			if(j->data_file.read(curoff + sizeof(header), buffer, header.length) != (ssize_t) header.length)
			{
				r = -1;
				goto fail;
			}
			curoff += sizeof(header) + header.length;
			r = processor(buffer, header.length, param);
			if(r < 0)
				goto fail;
		}
		lseek(j->crfd, CHECKSUM_LENGTH, SEEK_CUR);
	}
	patchgroup_disengage(playback);
	j->playback = playback;
	return 0;
fail:
	patchgroup_disengage(playback);
	patchgroup_abandon(playback);
	return r;
}

int journal_erase(journal * j)
{
	int r;
	patchgroup_id_t erase;
	if(!j->playback)
		return -EINVAL;
	if(j->prev && !j->prev->erase)
		/* or erase it here? */
		return -EINVAL;
	erase = patchgroup_create(0);
	if(erase <= 0)
		return -1;
	patchgroup_label(erase, "delete");
	if(patchgroup_add_depend(erase, j->playback) < 0 ||
	   (j->prev && patchgroup_add_depend(erase, j->prev->erase) < 0))
	{
		patchgroup_release(erase);
		patchgroup_abandon(erase);
		return -1;
	}
	r = patchgroup_release(erase);
	assert(r >= 0);
	r = patchgroup_engage(erase);
	assert(r >= 0);
	
	unlinkat(j->dfd, j->path, 0);
	unlinkat(j->dfd, j->path + J_COMMIT_EXT, 0);
	
	r = patchgroup_disengage(erase);
	assert(r >= 0);
	
	j->data_file.close();
	close(j->crfd);
	j->crfd = -1;
	j->erase = erase;
	if(j->prev)
		journal_free(j->prev);
	return 0;
}

int journal_flush(journal * j)
{
	if(!j->erase)
		return -EINVAL;
	return patchgroup_sync(j->erase);
}

int journal_free(journal * j)
{
	if(!j->erase)
		return -EINVAL;
	if(--j->usage > 0)
		return 0;
	if(j->records != -1)
		patchgroup_abandon(j->records);
	if(j->future)
		patchgroup_abandon(j->future);
	patchgroup_abandon(j->playback);
	patchgroup_abandon(j->erase);
	/* no need to unlink, since those were done in journal_erase() */
	delete j;
	return 0;
}

/* returns 1 for OK, 0 for failure, and < 0 on I/O error */
static int journal_verify(journal * j)
{
	struct commit_record cr;
	uint8_t checksum[2][CHECKSUM_LENGTH];
	struct iovec iov[2];
	if(j->crfd < 0)
		return -1;
	lseek(j->crfd, 0, SEEK_SET);
	for(uint32_t i = 0; i < j->commit_groups; ++i)
	{
		iov[0].iov_base = &cr;
		iov[0].iov_len = sizeof(cr);
		iov[1].iov_base = checksum[0];
		iov[1].iov_len = sizeof(checksum[0]);
		if(readv(j->crfd, iov, 2) != sizeof(cr) + sizeof(checksum[0]))
			return -1;
		if(journal_checksum(j, cr.offset, cr.offset + cr.length, checksum[1]) < 0)
			return -1;
		if(memcmp(checksum[0], checksum[1], CHECKSUM_LENGTH))
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
	lseek(j->crfd, -(sizeof(j->prev_cr) + CHECKSUM_LENGTH), SEEK_END);
	r = read(j->crfd, &j->prev_cr, sizeof(j->prev_cr));
	if(r != (int) sizeof(j->prev_cr))
		return (r < 0) ? r : -1;
	offset = lseek(j->crfd, 0, SEEK_END);
	j->commit_groups = offset / (sizeof(j->prev_cr) + CHECKSUM_LENGTH);
	j->records = 0;
	j->future = 0;
	j->last_commit = 0;
	j->playback = 0;
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
