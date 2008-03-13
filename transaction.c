/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define _GNU_SOURCE

#include <errno.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#include "openat.h"
#include "vector.h"
#include "hash_map.h"
#include "journal.h"
#include "transaction.h"

/* The routines in this file implement a file system transaction interface on
 * top of a generic journal module. Here we keep a journal directory with the
 * journal transactions, and manage initiating recovery when necessary. There
 * are some limits to the allowed file system operations; in particular, all
 * access to the files must be done through this module since writes to files
 * will not actually be done until the journal commits and thus they will not be
 * available for reading directly from the file system. */

typedef uint32_t tx_fid;

/* these structures are easier for reading... */

struct tx_name_hdr {
	uint16_t dir_len;
	uint16_t name_len;
	mode_t mode;
	char strings[0];
};

struct tx_write_hdr {
	tx_fid fid;
	size_t length;
	off_t offset;
	union {
		struct tx_name_hdr name[0];
		uint8_t data[0];
	};
};

struct tx_hdr {
	enum { WRITE, UNLINK, RENAME } type;
	union {
		struct tx_write_hdr write[0];
		struct tx_name_hdr unlink[0];
	};
};

/* ...and these are better for writing */

struct tx_write {
	struct tx_hdr type;
	struct tx_write_hdr write;
};

struct tx_full_write {
	struct tx_write write;
	struct tx_name_hdr name;
};

struct tx_unlink {
	struct tx_hdr type;
	struct tx_name_hdr unlink;
};

static int journal_dir = -1;
static journal * last_journal = NULL;
static journal * current_journal = NULL;

static tx_id last_tx_id = -1;
static hash_map_t * tx_id_map = NULL;

/* TX_FDS should be a power of 2 */
#define TX_FDS 1024
static struct {
	int fd, usage;
	char * dir;
	char * name;
	mode_t mode;
	tx_id tid;
	tx_fid fid;
} tx_fds[TX_FDS];
static tx_fd last_tx_fd = -1;

#define FID_FD(x) ((x) % TX_FDS)

/* operations on transactions */

static int tx_playback(journal * j);

/* scans journal dir, recovers transactions */
int tx_init(int dfd)
{
	size_t i;
	tx_fd fd;
	DIR * dir;
	int copy, error = -1;
	struct dirent * ent;
	vector_t * entries;
	
	if(journal_dir >= 0)
		return -EBUSY;
	for(fd = 0; fd < TX_FDS; fd++)
	{
		tx_fds[fd].fd = -1;
		tx_fds[fd].fid = fd;
	}
	last_tx_fd = 0;
	copy = atexit(tx_deinit);
	if(copy < 0)
		return copy;
	
	journal_dir = openat(dfd, "journals", O_RDONLY);
	if(journal_dir < 0)
		return journal_dir;
	copy = dup(journal_dir);
	if(copy < 0)
	{
		error = copy;
		goto fail;
	}
	dir = fdopendir(copy);
	if(!dir)
	{
		close(copy);
		goto fail;
	}
	entries = vector_create();
	if(!entries)
	{
		closedir(dir);
		goto fail;
	}
	
	while((ent = readdir(dir)))
	{
		char * name;
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;
		name = strdup(ent->d_name);
		if(!name)
			break;
		if(vector_push_back(entries, name) < 0)
		{
			free(name);
			break;
		}
	}
	closedir(dir);
	if(ent)
	{
	fail_vector:
		while(!vector_empty(entries))
			free(vector_pop_back(entries));
		vector_destroy(entries);
		goto fail;
	}
	
	/* XXX currently we assume recovery of journals in lexicographic order */
	vector_sort(entries, (int (*)(const void *, const void *)) strcmp);
	
	for(i = 0; i < vector_size(entries); i++)
	{
		char * name = (char *) vector_elt(entries, i);
		last_tx_id = strtol(name, NULL, 16);
		error = journal_reopen(journal_dir, name, &current_journal, last_journal);
		if(error < 0)
			goto fail_vector;
		if(!current_journal)
		{
			/* uncommitted journal */
			error = unlinkat(journal_dir, name, 0);
			if(error < 0)
				goto fail_vector;
			continue;
		}
		error = tx_playback(current_journal);
		if(error < 0)
			goto fail_vector;
		for(fd = 0; fd < TX_FDS; fd++)
			if(tx_fds[fd].fd >= 0)
				tx_close(fd);
		error = journal_erase(current_journal);
		if(error < 0)
			goto fail_vector;
		if(last_journal)
			journal_free(last_journal);
		last_journal = current_journal;
		current_journal = NULL;
	}
	
	while(!vector_empty(entries))
		free(vector_pop_back(entries));
	vector_destroy(entries);
	tx_id_map = hash_map_create();
	if(!tx_id_map)
	{
		error = -ENOMEM;
		goto fail;
	}
	return 0;
	
fail:
	close(journal_dir);
	journal_dir = -1;
	return error;
}

void tx_deinit(void)
{
	hash_map_it2_t it;
	if(journal_dir < 0)
		return;
	if(current_journal)
		tx_end(0);
	it = hash_map_it2_create(tx_id_map);
	while(hash_map_it2_next(&it))
	{
		journal * j = (journal *) it.val;
		if(j != last_journal)
			journal_free(j);
	}
	hash_map_destroy(tx_id_map);
	if(last_journal)
		journal_free(last_journal);
	tx_id_map = NULL;
	close(journal_dir);
	journal_dir = -1;
}

int tx_start(void)
{
	char name[16];
	if(current_journal)
		return -EBUSY;
	snprintf(name, sizeof(name), "%08x.jnl", last_tx_id + 1);
	current_journal = journal_create(journal_dir, name, last_journal);
	if(!current_journal)
		return -1;
	if(last_journal && !hash_map_find_val(tx_id_map, (void *) last_tx_id))
	{
		journal_free(last_journal);
		last_journal = NULL;
	}
	last_tx_id++;
	return 0;
}

tx_id tx_end(int assign_id)
{
	int r;
	if(!current_journal)
		return -ENOENT;
	if(assign_id)
	{
		r = hash_map_insert(tx_id_map, (void *) last_tx_id, current_journal);
		if(r < 0)
			return r;
	}
	r = journal_commit(current_journal);
	if(r < 0)
		goto fail;
	r = tx_playback(current_journal);
	if(r < 0)
		/* not clear how to uncommit the journal... */
		goto fail;
	r = journal_erase(current_journal);
	if(r < 0)
		/* not clear how to unplayback the journal... */
		goto fail;
	last_journal = current_journal;
	current_journal = NULL;
	return 0;
	
fail:
	if(assign_id)
		hash_map_erase(tx_id_map, (void *) last_tx_id);
	return r;
}

int tx_sync(tx_id id)
{
	int r;
	journal * j = (journal *) hash_map_find_val(tx_id_map, (void *) id);
	if(!j)
		return -EINVAL;
	r = journal_flush(j);
	if(r < 0)
		return r;
	hash_map_erase(tx_id_map, (void *) id);
	if(j != last_journal)
		journal_free(j);
	return 0;
}

int tx_forget(tx_id id)
{
	journal * j = (journal *) hash_map_find_val(tx_id_map, (void *) id);
	if(!j)
		return -EINVAL;
	hash_map_erase(tx_id_map, (void *) id);
	if(j != last_journal)
		journal_free(j);
	return 0;
}

/* transaction playback */

/* When doing normal playback, the files will all be open already (but possibly
 * with negative usage counts, indicating they've been "closed" during the
 * transaction). We can thus just go directly to their tx_fds entries and use
 * the file descriptors there. The tx_fd of each file is unique since files are
 * not actually closed until the end of the transaction, which is useful for
 * playback so we don't need to keep any additional mappings. During recovery,
 * we may need to open the files, but the other nice properties still apply.
 * However, in that case we need to close them again afterward. This is handled
 * outside this function, by the recovery routines. */
static int tx_write_playback(struct tx_write_hdr * header, size_t length)
{
	void * data;
	tx_fd fd = FID_FD(header->fid);
	/* this condition should only be true during recovery... */
	if(tx_fds[fd].fd < 0)
	{
		int dfd;
		/* need to open the file; must be a full header with pathnames */
		assert(header->length + sizeof(struct tx_full_write) + header->name->dir_len + header->name->name_len == length);
		tx_fds[fd].dir = strndup(header->name->strings, header->name->dir_len);
		if(!tx_fds[fd].dir)
			return -ENOMEM;
		tx_fds[fd].name = strndup(&header->name->strings[header->name->dir_len], header->name->name_len);
		if(!tx_fds[fd].name)
		{
			free(tx_fds[fd].dir);
			return -ENOMEM;
		}
		dfd = open(tx_fds[fd].dir, 0);
		if(dfd < 0)
		{
			free(tx_fds[fd].name);
			free(tx_fds[fd].dir);
			return dfd;
		}
		tx_fds[fd].fd = openat(dfd, tx_fds[fd].name, O_RDWR | O_CREAT, header->name->mode);
		close(dfd);
		if(tx_fds[fd].fd < 0)
		{
			free(tx_fds[fd].name);
			free(tx_fds[fd].dir);
			return tx_fds[fd].fd;
		}
		tx_fds[fd].tid = last_tx_id;
		/* important so that tx_close() will work after recovery! */
		tx_fds[fd].usage = 0;
		data = &header->name->strings[header->name->dir_len + header->name->name_len];
	}
	else
	{
		if(header->length + sizeof(struct tx_write) == length)
			/* not a full header with pathnames */
			data = header->data;
		else
		{
			/* full header with pathnames */
			assert(header->length + sizeof(struct tx_full_write) + header->name->dir_len + header->name->name_len == length);
			data = &header->name->strings[header->name->dir_len + header->name->name_len];
		}
	}
	lseek(tx_fds[fd].fd, header->offset, SEEK_SET);
	write(tx_fds[fd].fd, data, header->length);
	return 0;
}

static int tx_unlink_playback(struct tx_name_hdr * header, size_t length)
{
	int r, dfd;
	char * dir;
	char * name;
	dir = strndup(header->strings, header->dir_len);
	if(!dir)
		return -ENOMEM;
	name = strndup(&header->strings[header->dir_len], header->name_len);
	if(!name)
	{
		free(dir);
		return -ENOMEM;
	}
	dfd = open(dir, 0);
	if(dfd < 0)
	{
		free(name);
		free(dir);
		return dfd;
	}
	r = unlinkat(dfd, name, 0);
	if(r < 0 && errno == ENOENT)
		r = 0;
	close(dfd);
	free(name);
	free(dir);
	return r;
}

static int tx_rename_playback(struct tx_hdr * header, size_t length)
{
	return -ENOSYS;
}

static int tx_record_processor(void * data, size_t length, void * param)
{
	struct tx_hdr * header = data;
	switch(header->type)
	{
		case WRITE:
			return tx_write_playback(header->write, length);
		case UNLINK:
			return tx_unlink_playback(header->unlink, length);
		case RENAME:
			return tx_rename_playback(header, length);
	}
	return -ENOSYS;
}

static int tx_playback(journal * j)
{
	return journal_playback(j, tx_record_processor, NULL);
}

/* operations on files within a transaction */

static tx_fd get_next_tx_fd(void)
{
	tx_fd fd = last_tx_fd;
	
	/* XXX necessary? probably not... */
	if(fd == -1)
	{
		for(fd = 0; fd < TX_FDS; fd++)
		{
			tx_fds[fd].fd = -1;
			tx_fds[fd].fid = fd;
		}
		last_tx_fd = 0;
		return 0;
	}
	
	do {
		if(tx_fds[last_tx_fd].fd < 0)
			return last_tx_fd;
		if(++last_tx_fd == TX_FDS)
			last_tx_fd = 0;
	} while(fd != last_tx_fd);
	
	return -1;
}

/* Note that using O_CREAT here will create the file immediately, rather than
 * during transaction playback. Also see the note below about tx_unlink(). */
tx_fd tx_open(int dfd, const char * name, int flags, ...)
{
	int fd = get_next_tx_fd();
	if(fd < 0)
		return fd;
	if(flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		tx_fds[fd].mode = va_arg(ap, int);
		va_end(ap);
	}
	else
		tx_fds[fd].mode = 0;
	tx_fds[fd].dir = getcwdat(dfd, NULL, 0);
	if(!tx_fds[fd].dir)
		return (errno > 0) ? -errno : -1;
	tx_fds[fd].name = strdup(name);
	if(!tx_fds[fd].name)
	{
		free(tx_fds[fd].dir);
		return -ENOMEM;
	}
	tx_fds[fd].fd = openat(dfd, name, flags, tx_fds[fd].mode);
	if(tx_fds[fd].fd < 0)
	{
		free(tx_fds[fd].name);
		free(tx_fds[fd].dir);
		return tx_fds[fd].fd;
	}
	/* i.e., not this tx ID for sure */
	tx_fds[fd].tid = last_tx_id - 1;
	return fd;
}

int tx_read_fd(tx_fd fd)
{
	return tx_fds[fd].fd;
}

int tx_write(tx_fd fd, const void * buf, off_t offset, size_t length)
{
	struct tx_full_write full;
	struct tx_write * header = &full.write;
	struct iovec iov[4];
	size_t count = 1;
	if(!current_journal)
		return -EBUSY;
	header->type.type = WRITE;
	header->write.length = length;
	header->write.offset = offset;
	iov[0].iov_base = header;
	iov[0].iov_len = sizeof(*header);
	if(tx_fds[fd].tid != last_tx_id)
	{
		iov[0].iov_len = sizeof(full);
		full.name.dir_len = strlen(tx_fds[fd].dir);
		iov[1].iov_base = tx_fds[fd].dir;
		iov[1].iov_len = full.name.dir_len;
		full.name.name_len = strlen(tx_fds[fd].name);
		iov[2].iov_base = tx_fds[fd].name;
		iov[2].iov_len = full.name.name_len;
		full.name.mode = tx_fds[fd].mode;
		count = 3;
		tx_fds[fd].tid = last_tx_id;
		tx_fds[fd].fid += TX_FDS;
		tx_fds[fd].usage = 0;
	}
	header->write.fid = tx_fds[fd].fid;
	iov[count].iov_base = (void *) buf;
	iov[count++].iov_len = length;
	/* XXX we should save the location and amend it later if overwritten */
	tx_fds[fd].usage++; /* XXX only if appending; amend does not increment usage */
	/* FIXME if this fails and count == 4, then fix tx_fds[fd].tid */
	return journal_appendv4(current_journal, iov, count, NULL);
}

int tx_close(tx_fd fd)
{
	if(tx_fds[fd].tid == last_tx_id && tx_fds[fd].usage)
	{
		if(tx_fds[fd].usage > 0)
			tx_fds[fd].usage = -tx_fds[fd].usage;
		else
		{
			fprintf(stderr, "Double close of tx_fd %d?\n", fd);
			return -EINVAL;
		}
	}
	else
	{
		int r = close(tx_fds[fd].fd);
		if(r < 0)
			return r;
		free(tx_fds[fd].name);
		free(tx_fds[fd].dir);
		tx_fds[fd].fd = -1;
	}
	return 0;
}

/* Note that you cannot unlink and then recreate a file in a single transaction.
 * Most parts of that will work, but since the old file will have been opened as
 * the new file during the transaction (since the unlink will not have been
 * played back), the unlink that occurs during playback will unlink the file
 * which is still open as the new file. Further writes to the file will occur on
 * the unlinked file, which will be lost once it is closed. */
int tx_unlink(int dfd, const char * name)
{
	struct tx_unlink header;
	struct iovec iov[3];
	char * dir;
	int r;
	if(!current_journal)
		return -EBUSY;
	header.type.type = UNLINK;
	iov[0].iov_base = &header;
	iov[0].iov_len = sizeof(header);
	dir = getcwdat(dfd, NULL, 0);
	if(!dir)
		return (errno > 0) ? -errno : -1;
	header.unlink.dir_len = strlen(dir);
	iov[1].iov_base = dir;
	iov[1].iov_len = header.unlink.dir_len;
	header.unlink.name_len = strlen(name);
	iov[2].iov_base = (void *) name;
	iov[2].iov_len = header.unlink.name_len;
	header.unlink.mode = 0;
	r = journal_appendv4(current_journal, iov, 3, NULL);
	free(dir);
	return r;
}

int tx_rename(int old_dfd, const char * old_name, int new_dfd, const char * new_name)
{
	return -ENOSYS;
}
