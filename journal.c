/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

#include <patchgroup.h>

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
	uint8_t data[0];
};

journal * journal_create(int dfd, const char * path)
{
	journal * j = malloc(sizeof(*j));
	if(!j)
		return NULL;
	j->fd = openat(dfd, path, O_CREAT | O_RDWR | O_EXCL);
	if(j->fd < 0)
	{
		int save = errno;
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
		free(j);
		errno = save;
		return NULL;
	}
	j->commit = 0;
	j->playback = 0;
	j->erase = 0;
	return j;
}

int journal_append(journal * j, void * data, uint16_t length, uint16_t type, journal_record * location)
{
}

int journal_ammend(journal * j, const journal_record * location, void * data)
{
}

int journal_commit(journal * j)
{
}

int journal_wait(journal * j)
{
}

int journal_abort(journal * j)
{
}

int journal_playback(journal * j, record_processor processor, void * param)
{
}

int journal_erase(journal * j)
{
}

int journal_flush(journal * j)
{
}

int journal_free(journal * j)
{
}
