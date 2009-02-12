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

#include <vector>
#include <algorithm> /* for std::sort */

#include "util.h"
#include "openat.h"
#include "journal.h"
#include "transaction.h"

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

#define MF_TEMP_EXT ".mf-tmp"

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
	MF_S_DEBUG("%s", path.str());
	itr = mf_map.find(path);
	if(itr != mf_map.end())
	{
		MF_S_DEBUG("cached");
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
		if(st.st_size && pread(fd, &fp->data[0], st.st_size, 0) != st.st_size)
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
	MF_DEBUG("%d", is_dirty);
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
	MF_S_DEBUG("%s", path.str());
	mf_map_t::iterator itr = mf_map.find(path);
	if(itr != mf_map.end())
	{
		if(itr->second->usage)
		{
			fprintf(stderr, "Warning: tried to unlink open metafile %s\n", path.str());
			return -EBUSY;
		}
		/* flush it, in case something later fails */
		itr->second->flush();
		delete itr->second;
	}
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

tx_id metafile::last_tx_id = -1;
tx_pre_end * metafile::pre_end_handlers = NULL;
metafile::tx_map_t metafile::tx_map;

int metafile::record_processor(void * data, size_t length, void * param)
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
			istr temp_path = path + MF_TEMP_EXT;
			assert(path[0] == '/');
			MF_S_DEBUG("write %s", path.str());
			fd = ::open(temp_path, O_WRONLY | O_TRUNC | O_CREAT, 0644);
			if(fd < 0)
				return fd;
			r = ::write(fd, file_data, *data_len);
			::close(fd);
			if(r != (int) *data_len)
			{
				::unlink(temp_path);
				return -1;
			}
			r = rename(temp_path, path);
			if(r < 0)
				::unlink(temp_path);
			return r;
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
			MF_S_DEBUG("unlink %s", path.str());
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
		if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
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
		error = current_journal->playback(record_processor, NULL, NULL);
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
	MF_S_DEBUG("%d", tx_recursion);
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

int metafile::switch_journal()
{
	int r = current_journal->erase();
	if(r < 0)
		return r;
	last_journal = current_journal;
	current_journal = NULL;
	return 0;
}

tx_id metafile::tx_end(bool assign_id)
{
	int r;
	mf_map_t::iterator itr;
	if(!current_journal)
		return -ENOENT;
	if(tx_recursion != 1)
		return -EBUSY;
	while(pre_end_handlers)
	{
		pre_end_handlers->handle(pre_end_handlers->data);
		pre_end_handlers = pre_end_handlers->_next;
	}
	itr = mf_map.begin();
	while(itr != mf_map.end())
	{
		metafile * mf = itr->second;
		/* advance the iterator before the possible delete
		 * below, which will remove it from the map */
		++itr;
		r = mf->flush();
		if(r < 0)
			return r;
		if(!mf->usage)
			delete mf;
	}
	MF_S_DEBUG("%d", tx_recursion);
	if(assign_id)
		if(!tx_map.insert(std::make_pair(last_tx_id, current_journal)).second)
			return -ENOENT;
	r = current_journal->commit();
	if(r < 0)
		goto fail;
	r = current_journal->playback(record_processor, NULL, NULL);
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

void metafile::tx_register_pre_end(tx_pre_end * handle)
{
	handle->_next = pre_end_handlers;
	pre_end_handlers = handle;
}

void metafile::tx_unregister_pre_end(tx_pre_end * handle)
{
	struct tx_pre_end ** prev = &pre_end_handlers;
	while(*prev && *prev != handle)
		prev = &(*prev)->_next;
	if(*prev)
		*prev = handle->_next;
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

tx_fd tx_open(int dfd, const char * name, int create)
{
	return metafile::open(dfd, name, create);
}

size_t tx_size(const tx_fd file)
{
	return file->size();
}

int tx_dirty(const tx_fd file)
{
	return file->dirty();
}

size_t tx_read(const tx_fd file, void * buf, size_t length, off_t offset)
{
	/* off_t vs. size_t for offset? */
	return file->read(buf, length, offset);
}

int tx_truncate(tx_fd file)
{
	file->truncate();
	return 0;
}

int tx_write(tx_fd file, const void * buf, size_t length, off_t offset)
{
	/* off_t vs. size_t for offset? */
	return file->write(buf, length, offset);
}

int tx_close(tx_fd file)
{
	file->close();
	return 0;
}

int tx_unlink(int dfd, const char * name, int recursive)
{
	return metafile::unlink(dfd, name, recursive);
}

int tx_init(int dfd, size_t log_size)
{
	return metafile::tx_init(dfd, log_size);
}

void tx_deinit(void)
{
	metafile::tx_deinit();
}

int tx_start(void)
{
	return metafile::tx_start();
}

tx_id tx_end(int assign_id)
{
	return metafile::tx_end(assign_id);
}

int tx_start_external(void)
{
	return metafile::tx_start_external();
}

int tx_end_external(int success)
{
	return metafile::tx_end_external(success);
}

void tx_register_pre_end(struct tx_pre_end * handle)
{
	metafile::tx_register_pre_end(handle);
}

void tx_unregister_pre_end(struct tx_pre_end * handle)
{
	metafile::tx_unregister_pre_end(handle);
}

int tx_sync(tx_id id)
{
	return metafile::tx_sync(id);
}

int tx_forget(tx_id id)
{
	return metafile::tx_forget(id);
}

int tx_start_r(void)
{
	return metafile::tx_start_r();
}

int tx_end_r(void)
{
	return metafile::tx_end_r();
}
