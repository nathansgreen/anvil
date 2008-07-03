/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/uio.h>

#include "md5.h"

#include "openat.h"
#include "journal.h"
#include "istr.h"

/* This is the generic journal module. It uses Featherstitch dependencies to
 * keep a journal of uninterpreted records, which later can be played back
 * either to commit the transaction or to recover it. The records do not
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
	j = (journal *)malloc(sizeof(*j));
	if(!j)
		return NULL;
	j->dfd = dfd;
	j->path = strdup(path);
	if(!j->path)
	{
		int save = errno;
		free(j);
		errno = save;
		return NULL;
	}
	/* hmm... should we include the openat() in the patchgroup? */
	j->fd = openat(dfd, path, O_CREAT | O_RDWR | O_EXCL, 0600);
	if(j->fd < 0)
	{
		int save = errno;
		free(j->path);
		free(j);
		errno = save;
		return NULL;
	}
	j->records = 0;
	j->crfd = -1;
	j->future = 0;
	j->last_commit = 0;
	j->playback = 0;
	j->erase = 0;
	j->prev = prev;
	j->commit_groups = 0;
	j->prev_commit_record.offset = 0;
	j->prev_commit_record.length = 0;
	j->usage = 1;
	if(prev)
		prev->usage++;
	return j;
}

int journal_appendv4(journal * j, const struct iovec * iovp, size_t count, journal_record * location)
{
	struct record header;
	struct iovec iov[5];
	off_t offset;
	size_t i;
	if(count > 4)
		return -EINVAL;
	header.length = iovp[0].iov_len;
	for(i = 1; i < count; i++)
		header.length += iovp[i].iov_len;
	if(header.length == (size_t) -1)
		return -EINVAL;
	offset = lseek(j->fd, 0, SEEK_END);
	if(location)
	{
		location->offset = offset;
		location->length = header.length;
	}
	iov[0].iov_base = &header;
	iov[0].iov_len = sizeof(header);
	memcpy(&iov[1], iovp, count * sizeof(*iovp));
	if(j->records == 0)
	{
		j->records = patchgroup_create(0);
		if(j->records <= 0)
			return -1;
		patchgroup_label(j->records, "records");
		patchgroup_release(j->records);
	}
	if(patchgroup_engage(j->records) < 0)
		return -1;
	if(writev(j->fd, iov, count + 1) != (ssize_t)(sizeof(header) + header.length))
	{
		int save = errno;
		ftruncate(j->fd, offset);
		patchgroup_disengage(j->records);
		/* make sure the pointer is not past the end of the file */
		lseek(j->fd, 0, SEEK_END);
		errno = save;
		return -1;
	}
	patchgroup_disengage(j->records);
	return 0;
}

int journal_append(journal * j, const void * data, size_t length, journal_record * location)
{
	struct iovec iov;
	iov.iov_base = (void *) data;
	iov.iov_len = length;
	return journal_appendv4(j, &iov, 1, location);
}

int journal_amendv4(journal * j, const journal_record * location, const struct iovec * iovp, size_t count)
{
	size_t i, length;
	/* artificial limit to match journal_appendv4() */
	if(count > 4 || location->offset < (off_t) (j->prev_commit_record.offset + j->prev_commit_record.length))
		return -EINVAL;
	length = iovp[0].iov_len;
	for(i = 1; i < count; i++)
		length += iovp[i].iov_len;
	if(location->length != length)
		return -EINVAL;
	lseek(j->fd, location->offset + sizeof(struct record), SEEK_SET);
	if(j->records == 0)
	{
		j->records = patchgroup_create(0);
		if(j->records <= 0)
			return -1;
		patchgroup_label(j->records, "records");
		patchgroup_release(j->records);
	}
	if(patchgroup_engage(j->records) < 0)
		return -1;
	if(write(j->fd, iovp, count) != (ssize_t)location->length)
	{
		int save = errno;
		patchgroup_disengage(j->records);
		errno = save;
		return -1;
	}
	patchgroup_disengage(j->records);
	return 0;
}

int journal_amend(journal * j, const journal_record * location, const void * data)
{
	struct iovec iov;
	iov.iov_base = (void *) data;
	iov.iov_len = location->length;
	return journal_amendv4(j, location, &iov, 1);
}

int journal_add_depend(journal * j, patchgroup_id_t pid)
{
	if(!j->future)
	{
		patchgroup_id_t external_dependency = patchgroup_create(0);
		if(external_dependency <= 0)
			return -1;
		patchgroup_label(external_dependency, "external_dependency");
		j->future = external_dependency;
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
	lseek(j->fd, start, SEEK_SET);
	size = read(j->fd, buffer, sizeof(buffer));
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
		size = read(j->fd, buffer, sizeof(buffer));
	}
	if(size < 0)
		return -1;
	MD5Final(checksum, &ctx);
	return 0;
}

int journal_commit(journal * j)
{
	uint8_t checksum[CHECKSUM_LENGTH];
	struct iovec iov[2];
	patchgroup_id_t commit;
	off_t offset;
	if(j->prev && !j->prev->last_commit)
		/* or commit it here? */
		return -EINVAL;

	commit_record cr;
	istr cr_name = j->path;
	cr_name += ".commit";
	if(j->crfd < 0)
	{
		j->crfd = openat(j->dfd, cr_name, O_CREAT | O_RDWR | O_APPEND, 0600);
		if(j->crfd < 0)
			return -1;
	}
	else
		lseek(j->crfd, 0, SEEK_END);
	offset = lseek(j->fd, 0, SEEK_END);
	cr.offset = j->prev_commit_record.offset + j->prev_commit_record.length;
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

	if(j->records == 0)
		return 0;

	if((patchgroup_add_depend(commit, j->records) < 0) ||
	   ((j->last_commit > 0) && (patchgroup_add_depend(commit, j->last_commit) < 0)))
	{
	fail:
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}

	if(j->prev && j->commit_groups == 0 && patchgroup_add_depend(commit, j->prev->last_commit) < 0)
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
		ftruncate(j->fd, offset);
		patchgroup_disengage(commit);
		/* the ftruncate() really should be part of j->records, but
		 * since commit depends on j->records, we'll substitute it */
		patchgroup_abandon(j->records);
		j->records = commit;
		/* make sure the pointer is not past the end of the file */
		lseek(j->fd, 0, SEEK_END);
		errno = save;
		return -1;
	}
	patchgroup_disengage(commit);
	j->future = 0;
	j->records = 0;
	j->last_commit = commit;
	++j->commit_groups;
	j->prev_commit_record = cr;
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
	close(j->fd);
	unlinkat(j->dfd, j->path, 0);
	istr crname = j->path;
	crname += ".commit";
	unlinkat(j->dfd, crname, 0);
	free(j->path);
	free(j);
	return 0;
}

int journal_playback(journal * j, record_processor processor, void * param)
{
	patchgroup_id_t playback;
	struct record header;
	struct commit_record cr;
	uint8_t buffer[65536];
	int r = -1;
	if(j->commit_groups == 0)
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
	if(j->last_commit == 0)
		// if we haven't commited anything replay the whole journal
		lseek(j->crfd, 0, SEEK_SET);
	else
		// only replay the records from the last commit
		lseek(j->crfd, -(sizeof(commit_record) + CHECKSUM_LENGTH), SEEK_END);

	if(patchgroup_engage(playback) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(playback);
		return -1;
	}
	while(read(j->crfd, &cr, sizeof(cr)) == sizeof(cr)) 
	{
		lseek(j->fd, cr.offset, SEEK_SET);
		size_t nbytes = 0;
		while(nbytes < cr.length)
		{
			if(read(j->fd, &header, sizeof(header)) != sizeof(header))
			{
				r = -1;
				goto fail;
			}
			if(read(j->fd, buffer, header.length) != (ssize_t)header.length)
			{
				r = -1;
				goto fail;
			}
			nbytes += sizeof(header) + header.length;
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
	uint8_t zero[sizeof(struct commit_record) + CHECKSUM_LENGTH];
	patchgroup_id_t erase;
	if(!j->playback)
		return -EINVAL;
	if(j->prev && !j->prev->erase)
		/* or erase it here? */
		return -EINVAL;
	erase = patchgroup_create(0);
	if(erase <= 0)
		return -1;
	patchgroup_label(erase, "erase");
	if(patchgroup_add_depend(erase, j->playback) < 0)
	{
	fail:
		patchgroup_release(erase);
		patchgroup_abandon(erase);
		return -1;
	}
	if(j->prev && patchgroup_add_depend(erase, j->prev->erase) < 0)
		goto fail;
	patchgroup_release(erase);
	if(patchgroup_engage(erase) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(erase);
		return -1;
	}
	//XXX This could be done more efficiently
	lseek(j->crfd, 0, SEEK_SET);
	memset(zero, 0, sizeof(zero));
	for(uint32_t i = 0; i < j->commit_groups; ++i)
	{
		if(write(j->crfd, zero, sizeof(zero)) != sizeof(zero))
		{
			int save = errno;
			patchgroup_disengage(erase);
			patchgroup_abandon(erase);
			errno = save;
			return -1;
		}
	}
	patchgroup_disengage(erase);
	close(j->fd);
	close(j->crfd);
	j->fd = -1;
	j->crfd = -1;
	j->erase = erase;
	erase = patchgroup_create(0);
	if(erase > 0)
	{
		patchgroup_label(erase, "delete");
		if(patchgroup_add_depend(erase, j->playback) >= 0)
			if(!j->prev || patchgroup_add_depend(erase, j->prev->erase) >= 0)
			{
				patchgroup_release(erase);
				if(patchgroup_engage(erase) >= 0)
				{
					unlinkat(j->dfd, j->path, 0);
					istr crname = j->path;
					crname += ".commit";
					unlinkat(j->dfd, crname, 0);
					patchgroup_disengage(erase);
				}
			}
		patchgroup_abandon(erase);
	}
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
	/* no need to close j->fd or unlink, since those were done in journal_erase() */
	free(j->path);
	free(j);
	return 0;
}

/* returns 1 for OK, 0 for failure, and < 0 on I/O error */
static int journal_verify(journal * j)
{
	commit_record cr;
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
	
	if(prev && !prev->last_commit)
		return -EINVAL;
	j = (journal *)malloc(sizeof(*j));
	if(!j)
		return -1;
	j->dfd = dfd;
	j->path = strdup(path);
	if(!j->path)
	{
		int save = errno;
		free(j);
		errno = save;
		return -1;
	}
	istr cr_name = j->path;
	cr_name += ".commit";
	/* do we want to O_CREAT here */
	j->crfd = openat(dfd, cr_name, O_CREAT | O_RDWR, 0600);
	j->fd = openat(dfd, path, O_RDWR);
	if(j->fd < 0 || j->crfd < 0)
	{
		int save = errno;
		free(j->path);
		free(j);
		errno = save;
		return -1;
	}
	lseek(j->crfd, -(sizeof(j->prev_commit_record) + CHECKSUM_LENGTH), SEEK_END);
	r = read(j->crfd, &j->prev_commit_record, sizeof(j->prev_commit_record));
	if(r != (int) sizeof(j->prev_commit_record))
		return (r < 0) ? r : -1;
	off_t offset = lseek(j->crfd, 0, SEEK_END);
	j->commit_groups = offset / (sizeof(j->prev_commit_record) + CHECKSUM_LENGTH);
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
	r = ftruncate(j->fd, j->prev_commit_record.offset + j->prev_commit_record.length);
	if(r < 0)
		goto error;
	/* check the checksum */
	r = journal_verify(j);
	if(r != 1)
	{
error:		if(prev)
			prev->usage--;
		close(j->fd);
		close(j->crfd);
		free(j->path);
		free(j);
		j = NULL;
	}
	*pj = j;
	return (r < 0) ? r : 0;
}
