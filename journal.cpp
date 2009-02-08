/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

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
	journal * j = new journal(path, dfd, prev);
	if(!j)
		return NULL;
	if(!j->path || j->data_file.create(dfd, path) < 0)
	{
		int save = errno;
		delete j;
		errno = save;
		return NULL;
	}
#if HAVE_FSTITCH /* {{{ */
	j->data_file.set_handler(&j->handler);
#endif /* }}} */
	return j;
}

int journal::wait()
{
	if(!last_commit)
		return -EINVAL;
#if HAVE_FSTITCH /* {{{ */
	return patchgroup_sync(last_commit);
#else /* }}} */
	sync();
	return 0;
#endif
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
		data_file.flush();
#if HAVE_FSTITCH /* {{{ */
		records = patchgroup_create(0);
		if(records <= 0)
			return -1;
		patchgroup_label(records, "records");
		patchgroup_release(records);
#else /* }}} */
		records = 1;
#endif
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

int journal::start_external()
{
#if HAVE_FSTITCH /* }}} */
	if(!ext_count)
	{
		int r;
		if(external <= 0)
		{
			external = patchgroup_create(0);
			if(external <= 0)
				return -ENOMEM;
			patchgroup_label(external, "external dependency");
			r = patchgroup_release(external);
			assert(r >= 0);
			ext_success = false;
		}
		r = patchgroup_engage(external);
		assert(r >= 0);
	}
	ext_count++;
#endif /* }}} */
	return 0;
}

int journal::end_external(bool success)
{
#if HAVE_FSTITCH
	assert(ext_count > 0);
	ext_success |= success;
	if(!--ext_count)
	{
		int r = patchgroup_disengage(external);
		assert(r >= 0);
	}
#endif
	return 0;
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
	
	if(erasure)
		return -EINVAL;
	if(commits > playbacks)
		/* must play back previous commit first */
		return -EINVAL;
	assert(playbacks == commits);
	
	if(!records)
		return 0;
	
	/* initialize crfd or if it is almost full add more empty records to it */
	if(crfd < 0 || !(commits % (J_ADD_N_COMMITS * 1000)))
		if(init_crfd(istr::null) < 0)
			return -1;

	cr.offset = prev_cr.offset + prev_cr.length;
	cr.length = data_file.end() - cr.offset;
	if(checksum(cr.offset, cr.offset + cr.length, cr.checksum) < 0)
		return -1;
#if HAVE_FSTITCH /* {{{ */
	patchgroup_id_t commit;
	commit = patchgroup_create(0);
	if(commit <= 0)
		return -1;
	patchgroup_label(commit, "commit");
	
	/* add the external dependency, if any */
	assert(!ext_count);
	if(external > 0)
	{
		if(ext_success && patchgroup_add_depend(commit, external) < 0)
			goto fail;
		patchgroup_abandon(external);
		external = 0;
	}
	
	if(patchgroup_add_depend(commit, records) < 0)
	{
	fail:
		patchgroup_release(commit);
		patchgroup_abandon(commit);
		return -1;
	}
	if(last_commit > 0 && patchgroup_add_depend(commit, last_commit) < 0)
		goto fail;
	if(prev && prev->last_commit && !commits && patchgroup_add_depend(commit, prev->last_commit) < 0)
		goto fail;
	patchgroup_release(commit);
	
	if(patchgroup_engage(commit) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(commit);
		return -1;
	}
#endif /* }}} */
	data_file.flush();
	
	if(pwrite(crfd, &cr, sizeof(cr), commits * sizeof(cr)) != sizeof(cr))
	{
#if HAVE_FSTITCH /* {{{ */
		int save = errno;
		patchgroup_disengage(commit);
		/* the truncate() really should be part of records, but
		 * since commit depends on records, we'll substitute it */
		patchgroup_abandon(records);
		/* make sure the pointer is not past the end of the file */
		errno = save;
#endif /* }}} */
		return -1;
	}
#if HAVE_FSTITCH /* {{{ */
	patchgroup_disengage(commit);
	patchgroup_abandon(records);
	if(last_commit)
		patchgroup_abandon(last_commit);
	last_commit = commit;
#else /* }}} */
	char commit_number[16];
	istr old_commit, new_commit;
	last_commit = commits;
	snprintf(commit_number, sizeof(commit_number), "%d", last_commit);
	old_commit = path + J_COMMIT_EXT + commit_number;
	snprintf(commit_number, sizeof(commit_number), "%d", commits + 1);
	new_commit = path + J_COMMIT_EXT + commit_number;
	int r = renameat(dfd, old_commit, dfd, new_commit);
	if(r < 0)
		return r;
#endif
	records = 0;
	++commits;
	prev_cr = cr;
	return 0;
}

int journal::playback(record_processor processor, commit_hook commit, void * param)
{
	data_header header;
	commit_record cr;
	off_t readoff;
	uint8_t buffer[65536];
	uint8_t zero_checksum[J_CHECKSUM_LEN];
	int r = -1;
	if(erasure)
		return -EINVAL;
	if(playbacks == commits)
		/* nothing to play back */
		return 0;
	assert(playbacks < commits);
#if HAVE_FSTITCH /* {{{ */
	patchgroup_id_t playback;
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
#endif /* }}} */
	if(last_commit > 0 && commits)
		/* only replay the records from the last commit */
		readoff = (commits - 1) * sizeof(cr);
	else
		/* if we haven't commited anything replay the whole journal */
		readoff = 0;
	
#if HAVE_FSTITCH /* {{{ */
	if(patchgroup_engage(playback) < 0)
	{
		/* this basically can't happen */
		patchgroup_abandon(playback);
		return -1;
	}
#endif /* }}} */
	memset(zero_checksum, 0, sizeof(zero_checksum));
	while(pread(crfd, &cr, sizeof(cr), readoff) == sizeof(cr))
	{
		if(!cr.offset && !cr.length && !memcmp(&cr.checksum, &zero_checksum, J_CHECKSUM_LEN))
			break;
		
		off_t curoff = cr.offset;
		while((size_t) (curoff - cr.offset) < cr.length)
		{
			if(data_file.read(curoff, &header, sizeof(header)) != sizeof(header))
			{
				r = -1;
				goto playback_error;
			}
			if(data_file.read(curoff + sizeof(header), buffer, header.length) != (ssize_t) header.length)
			{
				r = -1;
				goto playback_error;
			}
			curoff += sizeof(header) + header.length;
			r = processor(buffer, header.length, param);
			if(r < 0)
				goto playback_error;
		}
		readoff += sizeof(cr);
		if(commit)
		{
			r = commit(param);
			if(r < 0)
				goto playback_error;
		}
	}
#if HAVE_FSTITCH /* {{{ */
	patchgroup_disengage(playback);
#endif /* }}} */
	if(!finished)
	{
#if HAVE_FSTITCH /* {{{ */
		finished = patchgroup_create(0);
		if(finished <= 0)
		{
			patchgroup_abandon(playback);
			return -1;
		}
		patchgroup_label(finished, "finished");
#else /* }}} */
		finished = 1;
#endif
	}
#if HAVE_FSTITCH /* {{{ */
	r = patchgroup_add_depend(finished, playback);
	if(r < 0)
	{
		patchgroup_abandon(playback);
		return r;
	}
	patchgroup_abandon(playback);
#endif /* }}} */
	playbacks = commits;
	return 0;

playback_error:
#if HAVE_FSTITCH /* {{{ */
	patchgroup_disengage(playback);
	patchgroup_abandon(playback);
#endif /* }}} */
	return r;

}

int journal::erase()
{
	int r;
	if(records || external)
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
#if HAVE_FSTITCH /* {{{ */
			erasure = patchgroup_create(0);
			if(erasure <= 0)
				return -1;
#else /* }}} */
			erasure = 1;
#endif
		}
#if HAVE_FSTITCH /* {{{ */
		patchgroup_label(erasure, "delete");
#endif /* }}} */
	}
#if HAVE_FSTITCH /* {{{ */
	if(prev && patchgroup_add_depend(erasure, prev->erasure) < 0)
		return -1;
	r = patchgroup_release(erasure);
	assert(r >= 0);
	r = patchgroup_engage(erasure);
	assert(r >= 0);
#endif /* }}} */
	unlinkat(dfd, path, 0);
#if HAVE_FSTITCH /* {{{ */
	r = patchgroup_disengage(erasure);
	assert(r >= 0);
#else /* }}} */
	char commit_number[16];
	snprintf(commit_number, sizeof(commit_number), "%d", commits);
	unlinkat(dfd, path + J_COMMIT_EXT + commit_number, 0);
#endif
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
	assert(!external);
#if HAVE_FSTITCH /* {{{ */
	if(last_commit)
		patchgroup_abandon(last_commit);
	patchgroup_abandon(erasure);
#endif /* }}} */
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
	for(uint32_t i = 0; i < commits; i++)
	{
		if(pread(crfd, &cr, sizeof(cr), commits * sizeof(cr)) != sizeof(cr))
			return -1;
		if(checksum(cr.offset, cr.offset + cr.length, actual) < 0)
			return -1;
		if(memcmp(cr.checksum, actual, J_CHECKSUM_LEN))
			return 0;
	}
	return 1;
}

int journal::init_crfd(const istr & commit_name)
{
	int r;
	off_t filesize, nextcr = 0;
	struct timeval settime[2] = {{0, 0}, {0, 0}};
	commit_record zero = {0, 0}, cr;
	
	/* Only append more empy records to the commit file if it is already open
	 * otherwise create a new commit record file. */
	if(crfd < 0)
	{
		istr cname = commit_name;
		if(!cname)
		{
			char commit_number[16];
			snprintf(commit_number, sizeof(commit_number), "%d", commits);
			cname = path + J_COMMIT_EXT + commit_number;
		}
		crfd = openat(dfd, cname, O_CREAT | O_RDWR, 0644);
		if(crfd < 0)
			return -1;
	}

	filesize = lseek(crfd, 0, SEEK_END);
	memset(zero.checksum, 0, sizeof(zero.checksum));
	/* find out where the last good commit record is */
	while((r = pread(crfd, &cr, sizeof(cr), nextcr)))
	{
		if(r < (int) sizeof(cr))
			break;
		if(!cr.offset && !cr.length && !memcmp(&cr.checksum, &zero.checksum, J_CHECKSUM_LEN))
			break;
		nextcr += r;
	}
	
	/* We set the mtime for the commit record file in the future to prevent
	 * the inode metadata being updated with every write - this uses a hack
	 * in Featherstitch to optimize for patchgroups. */
	
	/* atime */
	settime[0].tv_sec = time(NULL);
	/* mtime is current time plus 10 years, or the end of 31-bit time, whichever is later */
	settime[1].tv_sec = settime[0].tv_sec + 315360000;
	if(settime[1].tv_sec < 2147483647)
		settime[1].tv_sec = 2147483647;
	if((r = futimes(crfd, settime)) < 0)
		goto error;
	
	if(filesize < (nextcr + (int) sizeof(cr)))
	{
		/* zero out the rest of the file J_ADD_N_COMMITS records at a time */
		uint8_t zbuffer[1000 * sizeof(zero)];
		memset(zbuffer, 0, sizeof(zbuffer));
		while((filesize - nextcr) < J_ADD_N_COMMITS * (int) sizeof(zbuffer))
		{
			r = pwrite(crfd, zbuffer, sizeof(zbuffer), filesize);
			if(r <= 0)
				goto error;
			filesize += r;
		}
		/* necessary? */
		fsync(crfd);
	}
	
	return nextcr;
	
error:
	if(crfd > 0)
	{
		close(crfd);
		crfd = -1;
	}
	return r < 0 ? r : -1;
}

int journal::reopen(int dfd, const istr & path, const istr & commit_name, journal ** pj, journal * prev)
{
	int r;
	journal * j;
	off_t offset;
#if !(HAVE_FSTITCH)
	const char * commit_number;
#endif
	
	if(prev && !prev->last_commit)
		return -EINVAL;
	j = new journal(path, dfd, prev);
	if(!j)
		return -1;
	if(!j->path)
		goto error;
	offset = j->init_crfd(commit_name);
	if(offset < 0)
		goto error;
	if(!offset)
	{
		/* opening an empty journal file */
		j->commits = 0;
		*pj = j;
		return 0;
	}
	
	offset -= sizeof(j->prev_cr);
	r = pread(j->crfd, &j->prev_cr, sizeof(j->prev_cr), offset);
	if(r != (int) sizeof(j->prev_cr))
		return (r < 0) ? r : -1;
	j->commits = (offset / sizeof(j->prev_cr)) + 1;
#if !(HAVE_FSTITCH)
	/* get the last commit number from the file name */
	commit_number = strstr(commit_name, J_COMMIT_EXT);
	if(commit_number)
	{
		commit_number += strlen(J_COMMIT_EXT);
		j->last_commit = atoi(commit_number);
		assert(j->last_commit <= j->commits);
	}
#endif
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
#if HAVE_FSTITCH /* {{{ */
	if(j)
		j->data_file.set_handler(&j->handler);
#endif /* }}} */
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
