/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#include <map>
#include <vector>
#include <algorithm> /* for std::sort */

#include "openat.h"
#include "journal.h"
#include "transaction.h"

#include "istr.h"
#include "params.h"

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
	enum { WRITE, UNLINK } type;
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
static uint32_t tx_recursion = 0;
static int log_size = 0;

static tx_id last_tx_id = -1;
typedef std::map<tx_id, journal *> tx_map_t;
static tx_map_t * tx_map; 

struct write_log {
	struct write_log * next;
	size_t length;
	off_t offset;
	/* must be the last member */
	uint8_t data[0];
	
	/* wrap the malloc() memory trick */
	static inline write_log * alloc(size_t length, off_t offset, const void * data)
	{
		write_log * log = (write_log *) malloc(sizeof(write_log) + length);
		if(log)
		{
			log->next = NULL;
			log->length = length;
			log->offset = offset;
			memcpy(log->data, data, length);
		}
		return log;
	}
	
	inline void free()
	{
		::free(this);
	}
	
private:
	/* make sure these are never used */
	write_log(); ~write_log();
	write_log(const write_log & x);
	write_log & operator=(const write_log & x);
};

/* TX_FDS should be a power of 2 */
#define TX_FDS 1024
static struct {
	int fd, writes, usage;
	char * dir;
	char * name;
	mode_t mode;
	tx_id tid;
	tx_fid fid;
	write_log * log;
	write_log ** last;
} tx_fds[TX_FDS];

#define FID_FD(x) ((x) % TX_FDS)

static struct tx_pre_end * pre_end_handlers = NULL;

/* operations on transactions */

static int tx_record_processor(void * data, size_t length, void * param);

static int ends_with(const char * string, const char * suffix)
{
	size_t str_len = strlen(string);
	size_t suf_len = strlen(suffix);
	if(str_len < suf_len)
		return 0;
	return !strcmp(&string[str_len - suf_len], suffix);
}

static int recover_hook(void * param)
{
	for(tx_fd fd = 0; fd < TX_FDS; fd++)
		if(tx_fds[fd].fd >= 0)
			tx_close(fd);
	return 0;
}

/* scans journal dir, recovers transactions */
int tx_init(int dfd, const params & config)
{
	DIR * dir;
	int copy, error = -1;
	struct dirent * ent;
	std::vector<istr> entries;
	
	if(journal_dir >= 0)
		return -EBUSY;
	for(tx_fd fd = 0; fd < TX_FDS; fd++)
	{
		tx_fds[fd].fd = -1;
		tx_fds[fd].fid = fd;
		tx_fds[fd].log = NULL;
		tx_fds[fd].last = &tx_fds[fd].log;
	}
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
	
	while((ent = readdir(dir)))
	{
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..") || ends_with(ent->d_name, J_COMMIT_EXT))
			continue;
		entries.push_back(ent->d_name);
	}
	closedir(dir);
	if(ent)
		goto fail;
	
	/* XXX currently we assume recovery of journals in lexicographic order */
	std::sort(entries.begin(), entries.end(), strcmp_less());
	
	for(size_t i = 0; i < entries.size(); i++)
	{
		const char * name = entries[i];
		last_tx_id = strtol(name, NULL, 16);
		error = journal::reopen(journal_dir, name, &current_journal, last_journal);
		if(error < 0)
			goto fail;
		if(!current_journal)
		{
			/* uncommitted journal */
			error = unlinkat(journal_dir, name, 0);
			if(error < 0)
				goto fail;
			continue;
		}
		error = current_journal->playback(tx_record_processor, recover_hook, NULL);
		if(error < 0)
			goto fail;
		error = current_journal->erase();
		if(error < 0)
			goto fail;
		if(last_journal)
			last_journal->release();
		last_journal = current_journal;
		current_journal = NULL;
	}
	
	if(!config.get("log_size", &log_size, 0))
	{
		error = -EINVAL;
		goto fail;
	}

	tx_map = new tx_map_t;
	if(!tx_map)
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
	if(journal_dir < 0)
		return;
	if(current_journal)
		tx_end(0);
	for(tx_map_t::iterator itr = tx_map->begin(); itr != tx_map->end(); ++itr)
	{
		journal * j = itr->second;
		if(j != last_journal)
			j->release();
	}
	tx_map->clear();
	delete tx_map;
	if(last_journal)
		last_journal->release();
	tx_map = NULL;
	close(journal_dir);
	journal_dir = -1;
}

int tx_start(void)
{
	if(!current_journal)
	{
		char name[16];
		if(journal_dir < 0)
			return -EBUSY;
		snprintf(name, sizeof(name), "%08x.jnl", last_tx_id + 1);
		current_journal = journal::create(journal_dir, name, last_journal);
		if(!current_journal)
			return -1;
		if(last_journal && tx_map->find(last_tx_id) == tx_map->end())
		{
			last_journal->release();
			last_journal = NULL;
		}
	}
	last_tx_id++;
	tx_recursion++;
	return 0;
}

void tx_register_pre_end(struct tx_pre_end * handle)
{
	handle->_next = pre_end_handlers;
	pre_end_handlers = handle;
}

int tx_add_depend(patchgroup_id_t pid)
{
	if(!current_journal)
		return -ENOENT;
	return current_journal->add_depend(pid);
}

static int switch_journal(void)
{
	int r = current_journal->erase();
	if(r < 0)
		return r;
	last_journal = current_journal;
	current_journal = NULL;
	return 0;
}

tx_id tx_end(int assign_id)
{
	int r;
	if(!current_journal)
		return -ENOENT;
	if(tx_recursion != 1)
		return -EBUSY;
	while(pre_end_handlers)
	{
		pre_end_handlers->handle(pre_end_handlers->data);
		pre_end_handlers = pre_end_handlers->_next;
	}
	if(assign_id)
	{
		if(!tx_map->insert(std::make_pair(last_tx_id, current_journal)).second)
			return -ENOENT;
	}
	r = current_journal->commit();
	if(r < 0)
		goto fail;
	r = current_journal->playback(tx_record_processor, NULL, NULL);
	if(r < 0)
		/* not clear how to uncommit the journal... */
		goto fail;
	if(current_journal->size() >= log_size)
	{
		r = switch_journal();
		if(r < 0)
			goto fail;
	}
	tx_recursion--;
	return 0;
	
fail:
	if(assign_id) 
		tx_map->erase(last_tx_id);
	return r;
}

int tx_sync(tx_id id)
{
	int r;
	tx_map_t::iterator itr = tx_map->find(id);
	if(itr == tx_map->end())
		return -EINVAL;
	journal * j = itr->second;
	r = j->wait();
	if(r < 0)
		return r;
	tx_map->erase(id);
	if(j != last_journal)
		j->release();
	return 0;
}

int tx_forget(tx_id id)
{
	tx_map_t::iterator itr = tx_map->find(id);
	if(itr == tx_map->end())
		return -EINVAL;
	journal * j = itr->second;
	tx_map->erase(id);
	if(j != last_journal)
		j->release();
	return 0;
}

/* transaction playback */

/* When doing normal playback, the files will all be open already (but possibly
 * with negative write counts, indicating they've been "closed" during the
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
		tx_fds[fd].writes = 0;
		tx_fds[fd].usage = 1;
		data = &header->name->strings[header->name->dir_len + header->name->name_len];
		assert(!tx_fds[fd].log);
	}
	else
	{
		write_log * log = tx_fds[fd].log;
		if(header->length + sizeof(struct tx_write) == length)
			/* not a full header with pathnames */
			data = header->data;
		else
		{
			/* full header with pathnames */
			assert(header->length + sizeof(struct tx_full_write) + header->name->dir_len + header->name->name_len == length);
			data = &header->name->strings[header->name->dir_len + header->name->name_len];
		}
		if(log)
		{
			assert(log->length == header->length && log->offset == header->offset);
			if(!(tx_fds[fd].log = log->next))
				tx_fds[fd].last = &tx_fds[fd].log;
			log->free();
		}
		/* else we are doing a second or later write during recovery */
	}
	pwrite(tx_fds[fd].fd, data, header->length, header->offset);
	if(tx_fds[fd].writes < 0 && !++tx_fds[fd].writes)
		tx_close(fd);
	return 0;
}

static int tx_unlink_playback(struct tx_name_hdr * header, size_t length)
{
	int r, dfd;
	istr dir(header->strings, header->dir_len);
	istr name(&header->strings[header->dir_len], header->name_len);
	dfd = open(dir, 0);
	if(dfd < 0)
		return dfd;
	r = unlinkat(dfd, name, 0);
	if(r < 0 && errno == ENOENT)
		r = 0;
	close(dfd);
	return r;
}

static int tx_record_processor(void * data, size_t length, void * param)
{
	struct tx_hdr * header = (tx_hdr *) data;
	switch(header->type)
	{
		case tx_hdr::WRITE:
			return tx_write_playback(header->write, length);
		case tx_hdr::UNLINK:
			return tx_unlink_playback(header->unlink, length);
	}
	return -ENOSYS;
}

/* operations on files within a transaction */

static int same_path(const char * d1, const char * f1, const char * d2, const char * f2)
{
	size_t d1l = strlen(d1);
	size_t d2l = strlen(d2);
	/* support directory names with trailing / */
	if(d1[d1l - 1] == '/')
		d1l--;
	if(d2[d2l - 1] == '/')
		d2l--;
	/* already lined up? then just compare each part */
	if(d1l == d2l)
		return !strncmp(d1, d2, d1l) && !strcmp(f1, f2);
	/* handle d1l > d2l by switching 1<->2 */
	if(d1l > d2l)
	{
		const char * ss;
		size_t sl = d1l;
		d1l = d2l; d2l = sl;
		ss = d1; d1 = d2; d2 = ss;
		ss = f1; f1 = f2; f2 = ss;
	}
	assert(d1l < d2l);
	if(strncmp(d1, d2, d1l))
		return 0;
	if(d2[d1l] != '/')
		return 0;
	d2 = &d2[d1l + 1];
	d2l -= d1l + 1;
	if(strncmp(d2, f1, d2l))
		return 0;
	if(f1[d2l] != '/')
		return 0;
	f1 = &f1[d2l + 1];
	return !strcmp(f1, f2);
}

static tx_fd get_tx_fd(const char * dir, const char * name)
{
	tx_fd min = -1;
	for(tx_fd fd = 0; fd < TX_FDS; fd++)
		if(tx_fds[fd].fd < 0)
		{
			/* if it's currently closed, check if it's the lowest */
			if(min == -1)
				min = fd;
		}
		else
		{
			/* if it's currently open, check if it's the same file */
			if(same_path(tx_fds[fd].dir, tx_fds[fd].name, dir, name))
			{
				if(tx_fds[fd].writes < 0)
					tx_fds[fd].writes = -tx_fds[fd].writes;
				tx_fds[fd].usage++;
				return fd;
			}
		}
	return min;
}

/* XXX: flags must either be stored in the journal, or some flags like O_TRUNC and O_EXCL must be disallowed */
/* Note that using O_CREAT here will create the file immediately, rather than
 * during transaction playback. Also see the note below about tx_unlink(). */
tx_fd tx_open(int dfd, const char * name, int flags, ...)
{
	int fd;
	char * dir;
	if(journal_dir < 0)
		return -EBUSY;
	dir = getcwdat(dfd, NULL, 0);
	if(!dir)
		return (errno > 0) ? -errno : -1;
	fd = get_tx_fd(dir, name);
	if(fd < 0)
	{
		free(dir);
		return fd;
	}
	if(tx_fds[fd].fd >= 0)
	{
		free(dir);
		/* already open */
		return fd;
	}
	if((flags & O_WRONLY) && !(flags & O_RDWR))
	{
		/* we may want to read from this file descriptor later, before
		 * the transaction is over, but we'll cache this one to use then
		 * so make it O_RDWR even though O_WRONLY was requested */
		flags &= ~O_WRONLY;
		flags |= O_RDWR;
	}
	if(flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		tx_fds[fd].mode = va_arg(ap, int);
		va_end(ap);
	}
	else
		tx_fds[fd].mode = 0;
	tx_fds[fd].dir = dir;
	tx_fds[fd].name = strdup(name);
	if(!tx_fds[fd].name)
	{
		free(dir);
		return -ENOMEM;
	}
	tx_fds[fd].fd = openat(dfd, name, flags, tx_fds[fd].mode);
	if(tx_fds[fd].fd < 0)
	{
		free(tx_fds[fd].name);
		free(dir);
		return tx_fds[fd].fd;
	}
	tx_fds[fd].writes = 0;
	tx_fds[fd].usage = 1;
	/* i.e., not this tx ID for sure */
	tx_fds[fd].tid = last_tx_id - 1;
	return fd;
}

int tx_emptyfile(tx_fd fd)
{
	int r;
	struct stat st;
	if(tx_fds[fd].log)
		return 1;
	r = fstat(tx_fds[fd].fd, &st);
	if(r < 0)
		return r;
	return st.st_size > 0;
}

ssize_t tx_read(tx_fd fd, void * buf, size_t length, off_t offset)
{
	const off_t end = offset + length;
	ssize_t size = pread(tx_fds[fd].fd, buf, length, offset);
	if(size < 0)
		return size;
	if((size_t) size < length)
		/* can't use void * in arithmetic... */
		memset(&((uint8_t *) buf)[size], 0, length - size);
	/* this will build up the correct result by applying all uncommitted writes in order */
	for(write_log * log = tx_fds[fd].log; log; log = log->next)
	{
		if(log->offset >= offset)
		{
			if(log->offset < end)
			{
				off_t start = log->offset - offset;
				size_t min = length - start;
				if(log->length < min)
					min = log->length;
				/* can't use void * in arithmetic... */
				memcpy(&((uint8_t *) buf)[start], log->data, min);
				if(start + min > (size_t) size)
					size = start + min;
			}
		}
		else if(log->offset + (off_t) log->length > offset)
		{
			off_t start = offset - log->offset;
			size_t min = log->length - start;
			if(length < min)
				min = length;
			memcpy(buf, &log->data[start], min);
			if(min > (size_t) size)
				size = min;
		}
	}
	return size;
}

/* note that tx_write(), unlike write(), does not report the number of bytes written */
ssize_t tx_write(tx_fd fd, const void * buf, size_t length, off_t offset)
{
	struct tx_full_write full;
	struct tx_write * header = &full.write;
	struct journal::ovec ov[4];
	size_t count = 1;
	int r;
	if(!current_journal)
		return -EBUSY;
	assert(tx_fds[fd].fd >= 0);
	header->type.type = tx_hdr::WRITE;
	header->write.length = length;
	header->write.offset = offset;
	ov[0].ov_base = header;
	ov[0].ov_len = sizeof(*header);
	if(tx_fds[fd].tid != last_tx_id)
	{
		ov[0].ov_len = sizeof(full);
		full.name.dir_len = strlen(tx_fds[fd].dir);
		ov[1].ov_base = tx_fds[fd].dir;
		ov[1].ov_len = full.name.dir_len;
		full.name.name_len = strlen(tx_fds[fd].name);
		ov[2].ov_base = tx_fds[fd].name;
		ov[2].ov_len = full.name.name_len;
		full.name.mode = tx_fds[fd].mode;
		count = 3;
		tx_fds[fd].tid = last_tx_id;
		tx_fds[fd].fid += TX_FDS;
		tx_fds[fd].writes = 0;
	}
	header->write.fid = tx_fds[fd].fid;
	ov[count].ov_base = buf;
	ov[count++].ov_len = length;
	*tx_fds[fd].last = write_log::alloc(length, offset, buf);
	if(!*tx_fds[fd].last)
		return -ENOMEM;
	tx_fds[fd].writes++;
	r = current_journal->appendv(ov, count);
	if(r < 0)
	{
		tx_fds[fd].writes--;
		if(count == 4)
			/* i.e., not this tx ID for sure */
			tx_fds[fd].tid--;
	}
	else
		tx_fds[fd].last = &(*tx_fds[fd].last)->next;
	return r;
}

int tx_vnprintf(tx_fd fd, off_t offset, size_t max, const char * format, va_list ap)
{
	char buffer[512];
	int r;
       	uint32_t length = vsnprintf(buffer, sizeof(buffer), format, ap);
	if(length >= sizeof(buffer) || (max != (size_t) -1 && length > max))
		return -E2BIG;
	r = tx_write(fd, buffer, length, offset);
	if(r < 0)
		return r;
	return length;
}

int tx_nprintf(tx_fd fd, off_t offset, size_t max, const char * format, ...)
{
	int r;
	va_list ap;
	va_start(ap, format);
	r = tx_vnprintf(fd, offset, max, format, ap);
	va_end(ap);
	return r;
}

int tx_close(tx_fd fd)
{
	/* still in use by other openers? */
	if(--tx_fds[fd].usage > 0)
		return 0;
	if(tx_fds[fd].tid == last_tx_id && tx_fds[fd].writes && current_journal)
	{
		if(tx_fds[fd].writes > 0)
			tx_fds[fd].writes = -tx_fds[fd].writes;
		else
		{
			fprintf(stderr, "Extra close of tx_fd %d?\n", fd);
			return -EINVAL;
		}
	}
	else
	{
		int r = close(tx_fds[fd].fd);
		if(r < 0)
			return r;
		while(tx_fds[fd].log)
		{
			write_log * log = tx_fds[fd].log;
			tx_fds[fd].log = log->next;
			log->free();
		}
		tx_fds[fd].last = &tx_fds[fd].log;
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
	struct journal::ovec ov[3];
	char * dir;
	int r;
	if(!current_journal)
		return -EBUSY;
	header.type.type = tx_hdr::UNLINK;
	ov[0].ov_base = &header;
	ov[0].ov_len = sizeof(header);
	dir = getcwdat(dfd, NULL, 0);
	if(!dir)
		return (errno > 0) ? -errno : -1;
	header.unlink.dir_len = strlen(dir);
	ov[1].ov_base = dir;
	ov[1].ov_len = header.unlink.dir_len;
	header.unlink.name_len = strlen(name);
	ov[2].ov_base = name;
	ov[2].ov_len = header.unlink.name_len;
	header.unlink.mode = 0;
	r = current_journal->appendv(ov, 3);
	free(dir);
	return r;
}

int tx_start_r(void)
{
	if(!tx_recursion)
		return tx_start();
	tx_recursion++;
	assert(tx_recursion);
	return 0;
}

int tx_end_r(void)
{
	if(!tx_recursion)
		return -EBUSY;
	if(tx_recursion == 1)
		return tx_end(0);
	tx_recursion--;
	return 0;
}
