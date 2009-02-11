/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <stdlib.h>
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
#include "metafile.h"

#include "istr.h"
#include "util.h"
#include "blob_buffer.h"

/* The routines in this file implement a simple small-file transaction interface
 * on top of a generic journal module. Here we keep a journal directory with the
 * journal transactions, and manage initiating recovery when necessary. There
 * are important limits to the allowed file operations; in particular, all
 * access to the files must be done through this module since writes to files
 * will not actually be done until the journal commits and thus they will not be
 * available for reading directly from the file system. */

#define MF_TX_WRITE 1
#define MF_TX_UNLINK 2
#define MF_TX_RM_R 3

struct metafile
{
public:
	static metafile * open(int dfd, const char * name, bool create);
	
	inline size_t size() const
	{
		return data.size();
	}
	
	inline bool dirty() const
	{
		return is_dirty;
	}
	
	inline size_t read(void * buf, size_t length, size_t offset) const
	{
		if(offset >= data.size())
			return 0;
		if(offset + length > data.size())
			length = data.size() - offset;
		util::memcpy(buf, &data[offset], length);
		return length;
	}
	
	inline void truncate()
	{
		is_dirty = true;
		data.set_size(0);
	}
	
	inline int write(const void * buf, size_t length, size_t offset)
	{
		is_dirty = true;
		return data.overwrite(offset, buf, length);
	}
	
	int flush();
	
	inline void close()
	{
		if(!--usage)
			delete this;
	}
	
	static int unlink(int dfd, const char * name, bool recursive);
	
	/* transactions */
	typedef mftx_id tx_id;
	
	static int tx_init(int dfd, size_t log_size);
	static void tx_deinit();
	
	static int tx_start();
	static tx_id tx_end(bool assign_id);
	
	static int tx_start_external();
	static int tx_end_external(bool success);
	
	static int tx_sync(tx_id id);
	static int tx_forget(tx_id id);
	
	static int tx_start_r();
	static int tx_end_r();
	
private:
	inline metafile(const istr & path)
		: path(path), usage(1), is_dirty(false)
	{
		assert(path);
		mf_map[path] = this;
		prev = &first;
		next = first;
		first = this;
		if(next)
			next->prev = &next;
	}
	
	inline ~metafile()
	{
		int r = flush();
		assert(r >= 0);
		mf_map.erase(path);
		*prev = next;
		if(next)
			next->prev = prev;
	}
	
	const istr path;
	blob_buffer data;
	size_t usage;
	metafile * next;
	metafile ** prev;
	bool is_dirty;
	
	/* static stuff */
	static metafile * first;
	typedef std::map<istr, metafile *, strcmp_less> mf_map_t;
	static mf_map_t mf_map;
	
	static istr metafile::full_path(int dfd, const char * name);
	
	static inline int flush_all()
	{
		int r = 0;
		for(metafile * m = first; m && r >= 0; m = m->next)
			r = m->flush();
		return r;
	}
	
	/* transactions */
	static int journal_dir;
	static journal * last_journal;
	static journal * current_journal;
	static uint32_t tx_recursion;
	static size_t tx_log_size;
	
	static tx_id last_tx_id;
	typedef std::map<tx_id, journal *> tx_map_t;
	static tx_map_t tx_map; 
	
	static inline bool ends_with(const char * string, const char * suffix)
	{
		size_t str_len = strlen(string);
		size_t suf_len = strlen(suffix);
		if(str_len < suf_len)
			return false;
		return !strcmp(&string[str_len - suf_len], suffix);
	}
	
	static int switch_journal(void);
	static int tx_record_processor(void * data, size_t length, void * param);
};

metafile * metafile::first = NULL;
metafile::mf_map_t metafile::mf_map;

istr metafile::full_path(int dfd, const char * name)
{
	char * dir = getcwdat(dfd, NULL, 0);
	if(!dir)
		return NULL;
	istr path(dir, "/", name);
	free(dir);
	return path;
}

metafile * metafile::open(int dfd, const char * name, bool create)
{
	int fd;
	metafile * fp = NULL;
	mf_map_t::iterator itr;
	istr path = full_path(dfd, name);
	if(!path)
		return NULL;
	itr = mf_map.find(path);
	if(itr != mf_map.end())
	{
		itr->second->usage++;
		return itr->second;
	}
	/* open read-write just to make sure we can */
	fd = openat(dfd, name, O_RDWR);
	if(fd < 0)
	{
		/* side effect: may non-transactionally create an empty file */
		fd = openat(dfd, name, O_RDWR | O_CREAT, 0644);
		if(fd < 0)
			return NULL;
		fp = new metafile(path);
		if(!fp)
			goto fail_close;
	}
	else
	{
		struct stat st;
		if(fstat(fd, &st) < 0)
			goto fail_close;
		fp = new metafile(path);
		if(!fp)
			goto fail_close;
		if(fp->data.set_size(st.st_size, false) < 0)
			goto fail_delete;
		if(pread(fd, &fp->data[0], st.st_size, 0) != st.st_size)
			goto fail_delete;
	}
	
	::close(fd);
	return fp;
	
fail_delete:
	delete fp;
fail_close:
	::close(fd);
	return NULL;
}

int metafile::flush()
{
	int r = 0;
	if(is_dirty)
	{
		uint8_t type = MF_TX_WRITE;
		uint16_t path_len = path.length();
		uint32_t data_len = data.size();
		journal::ovec ov[5] = {{&type, sizeof(type)},
		                       {&path_len, sizeof(path_len)},
		                       {&data_len, sizeof(data_len)},
		                       {&path[0], path_len},
		                       {data_len ? &data[0] : NULL, data_len}};
		assert(current_journal);
		r = current_journal->appendv(ov, data_len ? 5 : 4);
		if(r >= 0)
			is_dirty = false;
	}
	return r;
}

int metafile::unlink(int dfd, const char * name, bool recursive)
{
	istr path = full_path(dfd, name);
	if(!path)
		return -1;
	uint8_t type = recursive ? MF_TX_RM_R : MF_TX_UNLINK;
	uint16_t path_len = path.length();
	journal::ovec ov[3] = {{&type, sizeof(type)},
	                       {&path_len, sizeof(path_len)},
	                       {&path[0], path_len}};
	assert(current_journal);
	return current_journal->appendv(ov, 3);
}

/* transactions */

int metafile::journal_dir = -1;
journal * metafile::last_journal = NULL;
journal * metafile::current_journal = NULL;
uint32_t metafile::tx_recursion = 0;
size_t metafile::tx_log_size = 0;

metafile::tx_id metafile::last_tx_id = -1;
metafile::tx_map_t metafile::tx_map;

int metafile::tx_record_processor(void * data, size_t length, void * param)
{
	int r, fd;
	uint8_t * type = (uint8_t *) data;
	switch(*type)
	{
		case MF_TX_WRITE:
		{
			/* tx_write journal entry:
			 * [0] type (MF_TX_WRITE)
			 * [1-2] path length
			 * [3-6] data length
			 * [7-...] path
			 * [...] data */
			uint16_t * path_len = (uint16_t *) &type[1];
			uint32_t * data_len = (uint32_t *) &path_len[1];
			char * path_data = (char *) &data_len[1];
			void * file_data = (void *) &path_data[*path_len];
			istr path = istr(path_data, *path_len);
			assert(path[0] == '/');
			fd = ::open(path, O_WRONLY | O_TRUNC | O_CREAT, 0644);
			if(fd < 0)
				return fd;
			r = ::write(fd, file_data, *data_len);
			::close(fd);
			return (r == (int) *data_len) ? 0 : -1;
		}
		case MF_TX_UNLINK:
		case MF_TX_RM_R:
		{
			/* tx_unlink journal entry:
			 * [0] type (MF_TX_UNLINK or MF_TX_RM_R)
			 * [1-2] path length
			 * [3-...] path */
			uint16_t * path_len = (uint16_t *) &type[1];
			char * path_data = (char *) &path_len[1];
			istr path = istr(path_data, *path_len);
			assert(path[0] == '/');
			if(*type == MF_TX_RM_R)
				r = util::rm_r(AT_FDCWD, path);
			else
				r = unlinkat(AT_FDCWD, path, 0);
			if(r < 0 && errno == ENOENT)
				r = 0;
			return r;
		}
	}
	return -ENOSYS;
}

/* scans journal dir, recovers transactions */
int metafile::tx_init(int dfd, size_t log_size)
{
	DIR * dir;
	int copy, error = -1;
	struct dirent * ent;
	std::vector<istr> entries;
	
	if(journal_dir >= 0)
		return -EBUSY;
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
		::close(copy);
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
	
	/* XXX: currently we assume recovery of journals in lexicographic order */
	std::sort(entries.begin(), entries.end(), strcmp_less());
	
	for(size_t i = 0; i < entries.size(); i++)
	{
		const char * name = entries[i];
		const char * commit_name = NULL;
		last_tx_id = strtol(name, NULL, 16);
		if(i + 1 < entries.size() && strstr(entries[i + 1], name) && strstr(entries[i + 1], J_COMMIT_EXT))
			commit_name = entries[++i];
		
		error = journal::reopen(journal_dir, name, commit_name, &current_journal, last_journal);
		if(error < 0)
			goto fail;
		if(!current_journal)
		{
			/* uncommitted journal */
			error = unlinkat(journal_dir, name, 0);
			if(error < 0)
				goto fail;
			/* unlink the commit file as well */
			error = unlinkat(journal_dir, commit_name, 0);
			if(error < 0)
				goto fail;
			continue;
		}
		error = current_journal->playback(tx_record_processor, NULL, NULL);
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
	
	tx_log_size = log_size;
	
	/* just be sure */
	tx_map.clear();
	return 0;
	
fail:
	::close(journal_dir);
	journal_dir = -1;
	return error;
}

void metafile::tx_deinit()
{
	if(journal_dir < 0)
		return;
	if(tx_recursion)
	{
		fprintf(stderr, "Warning: %s() ending and committing transaction (recursion %u)\n", __FUNCTION__, tx_recursion);
		tx_recursion = 1;
		tx_end(0);
	}
	for(tx_map_t::iterator itr = tx_map.begin(); itr != tx_map.end(); ++itr)
	{
		journal * j = itr->second;
		if(j != last_journal)
			j->release();
	}
	tx_map.clear();
	if(last_journal)
	{
		last_journal->release();
		last_journal = NULL;
	}
	if(current_journal)
	{
		current_journal->erase();
		current_journal->release();
		current_journal = NULL;
	}
	::close(journal_dir);
	journal_dir = -1;
}

int metafile::tx_start()
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
		if(last_journal && tx_map.find(last_tx_id) == tx_map.end())
		{
			last_journal->release();
			last_journal = NULL;
		}
	}
	last_tx_id++;
	tx_recursion++;
	return 0;
}

int metafile::switch_journal(void)
{
	int r = current_journal->erase();
	if(r < 0)
		return r;
	last_journal = current_journal;
	current_journal = NULL;
	return 0;
}

metafile::tx_id metafile::tx_end(bool assign_id)
{
	int r;
	if(!current_journal)
		return -ENOENT;
	if(tx_recursion != 1)
		return -EBUSY;
	r = flush_all();
	if(r < 0)
		return r;
	if(assign_id)
		if(!tx_map.insert(std::make_pair(last_tx_id, current_journal)).second)
			return -ENOENT;
	r = current_journal->commit();
	if(r < 0)
		goto fail;
	r = current_journal->playback(tx_record_processor, NULL, NULL);
	if(r < 0)
		/* not clear how to uncommit the journal... */
		goto fail;
	/* current_journal->done() renames commit record again? */
	if(current_journal->size() >= tx_log_size)
	{
		r = switch_journal();
		if(r < 0)
			goto fail;
	}
	tx_recursion--;
	return 0;
	
fail:
	if(assign_id) 
		tx_map.erase(last_tx_id);
	return r;
}

int metafile::tx_start_external()
{
	if(!current_journal)
		return -EINVAL;
	return current_journal->start_external();
}

int metafile::tx_end_external(bool success)
{
	if(!current_journal)
		return -EINVAL;
	return current_journal->end_external(success);
}

int metafile::tx_sync(tx_id id)
{
	int r;
	tx_map_t::iterator itr = tx_map.find(id);
	if(itr == tx_map.end())
		return -EINVAL;
	journal * j = itr->second;
	r = j->wait();
	if(r < 0)
		return r;
	tx_map.erase(id);
	if(j != last_journal)
		j->release();
	return 0;
}

int metafile::tx_forget(tx_id id)
{
	tx_map_t::iterator itr = tx_map.find(id);
	if(itr == tx_map.end())
		return -EINVAL;
	journal * j = itr->second;
	tx_map.erase(id);
	if(j != last_journal)
		j->release();
	return 0;
}

int metafile::tx_start_r()
{
	if(!tx_recursion)
		return tx_start();
	tx_recursion++;
	assert(tx_recursion);
	return 0;
}

int metafile::tx_end_r()
{
	if(!tx_recursion)
		return -EBUSY;
	if(tx_recursion == 1)
		return tx_end(0);
	tx_recursion--;
	return 0;
}

/* now the C interface to all this */

mf_fp mf_open(int dfd, const char * name, int create)
{
	return metafile::open(dfd, name, create);
}

size_t mf_size(const mf_fp file)
{
	return file->size();
}

int mf_dirty(const mf_fp file)
{
	return file->dirty();
}

size_t mf_read(const mf_fp file, void * buf, size_t length, off_t offset)
{
	/* off_t vs. size_t for offset? */
	return file->read(buf, length, offset);
}

int mf_truncate(mf_fp file)
{
	file->truncate();
	return 0;
}

int mf_write(mf_fp file, const void * buf, size_t length, off_t offset)
{
	/* off_t vs. size_t for offset? */
	return file->write(buf, length, offset);
}

int mf_close(mf_fp file)
{
	file->close();
	return 0;
}

int mf_unlink(int dfd, const char * name, int recursive)
{
	return metafile::unlink(dfd, name, recursive);
}

int mftx_init(int dfd, size_t log_size)
{
	return metafile::tx_init(dfd, log_size);
}

void mftx_deinit(void)
{
	metafile::tx_deinit();
}

int mftx_start(void)
{
	return metafile::tx_start();
}

mftx_id mftx_end(int assign_id)
{
	return metafile::tx_end(assign_id);
}

int mftx_start_external(void)
{
	return metafile::tx_start_external();
}

int mftx_end_external(int success)
{
	return metafile::tx_end_external(success);
}

int mftx_sync(mftx_id id)
{
	return metafile::tx_sync(id);
}

int mftx_forget(mftx_id id)
{
	return metafile::tx_forget(id);
}

int mftx_start_r(void)
{
	return metafile::tx_start_r();
}

int mftx_end_r(void)
{
	return metafile::tx_end_r();
}
