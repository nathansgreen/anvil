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

#include <patchgroup.h>

#include "md5.h"
#include "openat.h"
#include "journal.h"

/* This is the generic journal module. It uses Featherstitch dependencies to
 * keep a journal of uninterpreted records, which later can be played back
 * either to commit the transaction or to recover it. The records do not
 * necessarily have to describe idempotent actions, but if they do not, the
 * client code must be able to figure out whether a record's action has already
 * been taken or not so as not to perform it twice in the event of recovery. */

/* this is the record actually written into the journal file */
struct record {
	uint16_t length;
	uint16_t type;
};

journal * journal_create(int dfd, const char * path, journal * prev)
{
	journal * j;
	if(prev && !prev->commit)
		return NULL;
	j = malloc(sizeof(*j));
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
	j->fd = openat(dfd, path, O_CREAT | O_RDWR | O_EXCL);
	if(j->fd < 0)
	{
		int save = errno;
		free(j->path);
		free(j);
		errno = save;
		return NULL;
	}
	j->records = patchgroup_create(0);
	if(j->records <= 0)
	{
		int save = errno;
		close(j->fd);
		unlinkat(dfd, path, 0);
		free(j->path);
		free(j);
		errno = save;
		return NULL;
	}
	patchgroup_release(j->records);
	j->commit = 0;
	j->playback = 0;
	j->erase = 0;
	j->prev = prev;
	return j;
}

int journal_append(journal * j, void * data, uint16_t length, uint16_t type, journal_record * location)
{
	struct record header;
	struct iovec iov[2];
	off_t offset;
	if(j->commit)
		return -EINVAL;
	if(length == (uint16_t) -1 && type == (uint16_t) -1)
		return -EINVAL;
	offset = lseek(j->fd, 0, SEEK_END);
	if(location)
	{
		location->offset = offset;
		location->length = length;
		location->type = type;
	}
	header.length = length;
	header.type = type;
	iov[0].iov_base = &header;
	iov[0].iov_len = sizeof(header);
	iov[1].iov_base = data;
	iov[1].iov_len = length;
	if(patchgroup_engage(j->records) < 0)
		return -1;
	if(writev(j->fd, iov, 2) != sizeof(header) + length)
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

int journal_ammend(journal * j, const journal_record * location, void * data)
{
	if(j->commit)
		return -EINVAL;
	lseek(j->fd, location->offset + sizeof(struct record), SEEK_SET);
	if(patchgroup_engage(j->records) < 0)
		return -1;
	if(write(j->fd, data, location->length) != location->length)
	{
		int save = errno;
		patchgroup_disengage(j->records);
		errno = save;
		return -1;
	}
	patchgroup_disengage(j->records);
	return 0;
}

#define CHECKSUM_LENGTH 16
static int journal_checksum(journal * j, off_t max, uint8_t * checksum)
{
	/* this can be replaced with any other hash function;
	 * I already had MD5 lying around so I used it here */
	MD5_CTX ctx;
	uint8_t buffer[4096];
	size_t size;
	MD5Init(&ctx);
	lseek(j->fd, 0, SEEK_SET);
	size = read(j->fd, buffer, sizeof(buffer));
	while(size > 0 && max > 0)
	{
		if(size > max)
		{
			size = max;
			max = 0;
		}
		else
			max -= size;
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
	struct record header;
	uint8_t checksum[CHECKSUM_LENGTH];
	struct iovec iov[2];
	patchgroup_id_t commit;
	off_t offset;
	if(j->commit)
		return -EINVAL;
	if(j->prev && !j->prev->commit)
		/* or commit it here? */
		return -EINVAL;
	offset = lseek(j->fd, 0, SEEK_END);
	if(journal_checksum(j, offset, checksum) < 0)
		return -1;
	commit = patchgroup_create(0);
	if(commit <= 0)
		return -1;
	if(patchgroup_add_depend(commit, j->records) < 0)
	{
	fail:
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}
	if(j->prev && patchgroup_add_depend(commit, j->prev->commit) < 0)
		goto fail;
	patchgroup_release(commit);
	header.length = -1;
	header.type = -1;
	iov[0].iov_base = &header;
	iov[0].iov_len = sizeof(header);
	iov[1].iov_base = checksum;
	iov[1].iov_len = sizeof(checksum);
	if(patchgroup_engage(commit) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(commit);
		return -1;
	}
	if(writev(j->fd, iov, 2) != sizeof(header) + sizeof(checksum))
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
	j->commit = commit;
	return 0;
}

int journal_wait(journal * j)
{
	if(!j->commit)
		return -EINVAL;
	return patchgroup_sync(j->commit);
}

int journal_abort(journal * j)
{
	if(j->commit)
		return -EINVAL;
	patchgroup_abandon(j->records);
	close(j->fd);
	unlinkat(j->dfd, j->path, 0);
	free(j->path);
	free(j);
	return 0;
}

int journal_playback(journal * j, record_processor processor, void * param)
{
	patchgroup_id_t playback;
	struct record header;
	uint8_t buffer[65536];
	int r = -1;
	if(!j->commit)
		return -EINVAL;
	playback = patchgroup_create(0);
	if(playback <= 0)
		return -1;
	if(j->commit != -1 && patchgroup_add_depend(playback, j->commit) < 0)
	{
		patchgroup_release(playback);
		patchgroup_abandon(playback);
		return -1;
	}
	lseek(j->fd, 0, SEEK_SET);
	if(patchgroup_engage(playback) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(playback);
		return -1;
	}
	if(read(j->fd, &header, sizeof(header)) != sizeof(header))
		goto fail;
	while(header.length != (uint16_t) -1 || header.type != (uint16_t) -1)
	{
		if(read(j->fd, buffer, header.length) != header.length)
		{
			r = -1;
			goto fail;
		}
		r = processor(buffer, header.length, header.type, param);
		if(r < 0)
			goto fail;
		if(read(j->fd, &header, sizeof(header)) != sizeof(header))
		{
			r = -1;
			goto fail;
		}
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
	patchgroup_id_t erase;
	if(!j->playback)
		return -EINVAL;
	if(j->prev && !j->prev->erase)
		/* or erase it here? */
		return -EINVAL;
	erase = patchgroup_create(0);
	if(erase <= 0)
		return -1;
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
	close(j->fd);
	j->fd = -1;
	if(patchgroup_engage(erase) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(erase);
		return -1;
	}
	if(unlinkat(j->dfd, j->path, 0) < 0)
	{
		int save = errno;
		patchgroup_disengage(erase);
		patchgroup_abandon(erase);
		errno = save;
		return -1;
	}
	patchgroup_disengage(erase);
	j->erase = erase;
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
	if(j->records != -1)
		patchgroup_abandon(j->records);
	if(j->commit != -1)
		patchgroup_abandon(j->commit);
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
	struct record header;
	uint8_t checksum[2][CHECKSUM_LENGTH];
	struct iovec iov[2];
	off_t offset = lseek(j->fd, -(sizeof(header) + sizeof(checksum[0])), SEEK_END);
	iov[0].iov_base = &header;
	iov[0].iov_len = sizeof(header);
	iov[1].iov_base = checksum[0];
	iov[1].iov_len = sizeof(checksum[0]);
	if(readv(j->fd, iov, 2) != sizeof(header) + sizeof(checksum[0]))
		return -1;
	if(header.length != (uint16_t) -1 || header.type != (uint16_t) -1)
		return 0;
	if(journal_checksum(j, offset, checksum[1]) < 0)
		return -1;
	return !memcmp(checksum[0], checksum[1], CHECKSUM_LENGTH);
}

int journal_reopen(int dfd, const char * path, journal ** pj, journal * prev)
{
	int r;
	journal * j;
	if(prev && !prev->commit)
		return -EINVAL;
	j = malloc(sizeof(*j));
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
	j->fd = openat(dfd, path, O_RDWR);
	if(j->fd < 0)
	{
		int save = errno;
		free(j->path);
		free(j);
		errno = save;
		return -1;
	}
	j->records = -1;
	j->commit = -1;
	j->playback = 0;
	j->erase = 0;
	j->prev = prev;
	/* check the checksum */
	r = journal_verify(j);
	if(r != 1)
	{
		close(j->fd);
		free(j->path);
		free(j);
		j = NULL;
	}
	*pj = j;
	return (r < 0) ? r : 0;
}
