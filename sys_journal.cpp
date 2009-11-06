/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <assert.h>
#include <fcntl.h>

#include "openat.h"
#include "transaction.h"

#include "sys_journal.h"

#define SYSJ_META_MAGIC 0xBAFE9BDA
#define SYSJ_META_VERSION 1

#define SYSJ_DATA_MAGIC 0x874C74FD
#define SYSJ_DATA_VERSION 0

struct meta_journal
{
	uint32_t magic;
	uint32_t version;
	uint32_t seq;
	size_t size;
} __attribute__((packed));

struct data_header
{
	uint32_t magic;
	uint32_t version;
} __attribute__((packed));

struct entry_header
{
	sys_journal::listener_id id;
	size_t length;
} __attribute__((packed));

#define DEBUG_SYSJ 0

#if DEBUG_SYSJ
#define SYSJ_DEBUG(format, args...) printf("%s @%p[%u %u/%u] (" format ")\n", __FUNCTION__, this, (unsigned) data.end(), (unsigned) data_size, (unsigned) info_size, ##args)
#else
#define SYSJ_DEBUG(format, args...)
#endif

int sys_journal::append(journal_listener * listener, void * entry, size_t length)
{
	int r;
	entry_header header;
	live_entry_map::iterator count;
	SYSJ_DEBUG("%d, %p, %u", listener->id(), entry, length);
	
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	assert(!discarded.count(header.id));
	if(length == (size_t) -1)
		return -EINVAL;
	header.length = length;
	
	assert(data_size == data.end());
	if(!dirty)
	{
		if(!registered)
		{
			tx_register_pre_end(&handle);
			registered = true;
		}
		dirty = true;
	}
	r = data.append(&header);
	if(r < 0)
		return r;
	r = data.append(entry, length);
	if(r != (int) length)
	{
		data.truncate(-sizeof(header));
		return (r < 0) ? r : -1;
	}
	data_size += sizeof(header) + length;
	
	count = live_entry_count.find(header.id);
	if(count != live_entry_count.end())
		count->second++;
	else
		live_entry_count[header.id] = 1;
	live_entries++;
	
	assert(data_size == data.end());
	return 0;
}

int sys_journal::discard(journal_listener * listener)
{
	int r;
	entry_header header;
	live_entry_map::iterator count;
	SYSJ_DEBUG("%d", listener->id());
	
	header.id = listener->id();
	count = live_entry_count.find(header.id);
	if(count == live_entry_count.end())
		/* no sense in actually writing a discard
		 * record if there's nothing to discard */
		return 0;
	
	assert(lookup_listener(header.id) == listener);
	header.length = (size_t) -1;
	
	assert(data_size == data.end());
	if(!dirty)
	{
		if(!registered)
		{
			tx_register_pre_end(&handle);
			registered = true;
		}
		dirty = true;
	}
	r = data.append(&header);
	if(r < 0)
		return r;
	data_size += sizeof(header);
	discarded.insert(header.id);
	
	live_entries -= count->second;
	live_entry_count.erase(count);
	if(!live_entries && filter_on_empty)
		filter();
	
	assert(data_size == data.end());
	return 0;
}

int sys_journal::filter()
{
	int r;
	char seq[16];
	meta_journal info;
	SYSJ_DEBUG("");
	
	if(dirty)
	{
		r = flush_tx();
		if(r < 0)
			return r;
		assert(!dirty);
	}
	
	/* no discarded IDs? no need to filter */
	if(!discarded.size())
		return 0;
	
	info.magic = SYSJ_META_MAGIC;
	info.version = SYSJ_META_VERSION;
	info.seq = sequence + 1;
	/* size will be filled in by filter() below */
	snprintf(seq, sizeof(seq), ".%u", info.seq);
	assert(info_size == data_size);
	assert(data_size == data.end());
	
	istr data_name = meta_name + seq;
	r = tx_start_external();
	if(r < 0)
		return r;
	r = filter(meta_dfd, data_name, &info.size);
	tx_end_external(r >= 0);
	if(r >= 0)
	{
		r = tx_write(meta_fd, &info, sizeof(info), 0);
		if(r >= 0)
		{
			/* switch to the new data file */
			r = data.close();
			assert(r >= 0);
			data_size = info.size;
			info_size = info.size;
			r = data.open(meta_dfd, data_name, data_size);
			assert(r >= 0);
			/* delete the old sys_journal data */
			snprintf(seq, sizeof(seq), ".%u", sequence);
			data_name = meta_name + seq;
			tx_unlink(meta_dfd, data_name, 0);
			sequence = info.seq;
			discarded.clear();
		}
		else
			unlinkat(meta_dfd, data_name, 0);
	}
	else
		unlinkat(meta_dfd, data_name, 0);
	assert(data_size == data.end());
	return r;
}

int sys_journal::filter(int dfd, const char * file, size_t * new_size)
{
	rwfile out;
	data_header header;
	SYSJ_DEBUG("%d, %s", dfd, file);
	int r = out.create(dfd, file);
	if(r < 0)
		return r;
	header.magic = SYSJ_DATA_MAGIC;
	header.version = SYSJ_DATA_VERSION;
	r = out.append(&header);
	if(r < 0)
		goto fail;
	/* if there are no live entries, don't waste time scanning */
	if(live_entries)
	{
		entry_header entry;
		size_t offset = sizeof(header);
		while(offset < data_size)
		{
			void * entry_data;
			r = data.read(offset, &entry);
			if(r < 0)
				goto fail;
			offset += sizeof(entry);
			if(entry.length == (size_t) -1)
				continue;
			if(discarded.count(entry.id))
			{
				/* skip this entry, it's been discarded */
				offset += entry.length;
				continue;
			}
			entry_data = malloc(entry.length);
			if(!entry_data)
			{
				r = -ENOMEM;
				goto fail;
			}
			if(data.read(offset, entry_data, entry.length) != (ssize_t) entry.length)
			{
				free(entry_data);
				r = -EIO;
				goto fail;
			}
			offset += entry.length;
			r = out.append(&entry);
			if(r < 0)
			{
				free(entry_data);
				goto fail;
			}
			r = out.append(entry_data, entry.length);
			free(entry_data);
			if(r != (int) entry.length)
				goto fail;
		}
		if(offset != data_size)
			goto fail;
	}
	*new_size = out.end();
	r = out.close();
	if(r < 0)
		goto fail;
	return 0;
	
fail:
	out.close();
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

int sys_journal::init(int dfd, const char * file, bool create, bool filter_on_empty, bool fail_missing)
{
	int r;
	meta_journal info;
	data_header header;
	bool do_playback = false;
	SYSJ_DEBUG("%d, %s, %d, %d", dfd, file, create, fail_missing);
	if(meta_fd)
		deinit();
	live_entries = 0;
	this->filter_on_empty = filter_on_empty;
	meta_fd = tx_open(dfd, file, 0);
	if(!meta_fd)
	{
		if(!create || errno != ENOENT)
			return -1;
		
		/* due to O_CREAT not being part of the transaction, we might
		 * get an empty file as a result of this, which later will
		 * cause an error below instead of here... hence create: */
		meta_fd = tx_open(dfd, file, 1);
		if(!meta_fd)
			return -1;
	create:
		istr data_name = istr(file) + ".0";
		
		r = data.create(dfd, data_name, true);
		if(r < 0)
			goto fail_create;
		header.magic = SYSJ_DATA_MAGIC;
		header.version = SYSJ_DATA_VERSION;
		r = data.append(&header);
		if(r < 0)
			goto fail_append;
		
		info.magic = SYSJ_META_MAGIC;
		info.version = SYSJ_META_VERSION;
		info.seq = 0;
		info.size = sizeof(header);
		r = tx_write(meta_fd, &info, sizeof(info), 0);
		if(r < 0)
		{
		fail_append:
			data.close();
			unlinkat(dfd, data_name, 0);
		fail_create:
			tx_close(meta_fd);
			tx_unlink(dfd, file, 0);
			meta_fd = NULL;
			return r;
		}
	}
	else
	{
		char seq[16];
		if(tx_read(meta_fd, &info, sizeof(info), 0) != sizeof(info))
		{
			if(!tx_size(meta_fd))
				goto create;
			tx_close(meta_fd);
			meta_fd = NULL;
			return -1;
		}
		if(info.magic != SYSJ_META_MAGIC || info.version != SYSJ_META_VERSION)
		{
			tx_close(meta_fd);
			meta_fd = NULL;
			return -EINVAL;
		}
		snprintf(seq, sizeof(seq), ".%u", info.seq);
		istr data_name = istr(file) + seq;
		r = data.open(dfd, data_name, info.size);
		if(r < 0)
		{
			tx_close(meta_fd);
			meta_fd = NULL;
			return r;
		}
		r = data.read(0, &header);
		if(r < 0)
		{
			data.close();
			tx_close(meta_fd);
			meta_fd = NULL;
			return r;
		}
		if(header.magic != SYSJ_DATA_MAGIC || header.version != SYSJ_DATA_VERSION)
		{
			data.close();
			tx_close(meta_fd);
			meta_fd = NULL;
			return -EINVAL;
		}
		do_playback = true;
	}
	data_size = info.size;
	info_size = info.size;
	sequence = info.seq;
	if(do_playback)
	{
		int r = playback(NULL, fail_missing, true);
		if(r < 0)
		{
			deinit();
			return r;
		}
	}
	meta_name = file;
	if(dfd != AT_FDCWD)
	{
		meta_dfd = dup(dfd);
		if(meta_dfd < 0)
		{
			deinit();
			return meta_dfd;
		}
	}
	else
		meta_dfd = AT_FDCWD;
	return 0;
}

/* TODO: allow filtering right here? store discards in a separate file? */
int sys_journal::playback(journal_listener * target, bool fail_missing, bool count_live)
{
	__gnu_cxx::hash_set<listener_id> missing, failed;
	size_t offset = sizeof(data_header);
	SYSJ_DEBUG("%d, %d", target ? target->id() : 0, fail_missing);
	assert(offset <= info_size);
	assert(info_size <= data_size);
	assert(data_size == data.end());
	if(count_live)
	{
		if(info_size != data_size)
			return -EINVAL;
		live_entries = 0;
		live_entry_count.clear();
	}
	while(offset < info_size)
	{
		int r;
		void * entry_data;
		entry_header entry;
		journal_listener * listener;
		if(data.read(offset, &entry) < 0)
			return -EIO;
		offset += sizeof(entry);
		if(entry.length == (size_t) -1)
		{
			if(count_live)
			{
				live_entry_map::iterator count = live_entry_count.find(entry.id);
				if(count != live_entry_count.end())
				{
					live_entries -= count->second;
					live_entry_count.erase(count);
				}
			}
			/* warn if entry.id == target->id() ? */
			if(!target)
			{
				missing.erase(entry.id);
				discarded.insert(entry.id);
			}
			continue;
		}
		if(count_live)
		{
			live_entry_map::iterator count = live_entry_count.find(entry.id);
			if(count != live_entry_count.end())
				count->second++;
			else
				live_entry_count[entry.id] = 1;
			live_entries++;
		}
		if(target)
		{
			/* skip other entries */
			if(target->id() != entry.id)
			{
				offset += entry.length;
				continue;
			}
			listener = target;
		}
		else
		{
			if(failed.count(entry.id))
			{
				offset += entry.length;
				continue;
			}
			listener = lookup_listener(entry.id);
			if(!listener)
			{
				missing.insert(entry.id);
				offset += entry.length;
				continue;
			}
		}
		entry_data = malloc(entry.length);
		if(!entry_data)
			return -ENOMEM;
		if(data.read(offset, entry_data, entry.length) != (ssize_t) entry.length)
		{
			free(entry_data);
			return -EIO;
		}
		offset += entry.length;
		/* data is passed by reference */
		r = listener->journal_replay(entry_data, entry.length);
		if(entry_data)
			free(entry_data);
		if(r < 0)
		{
			if(target)
				return r;
			failed.insert(entry.id);
		}
	}
	if(offset != info_size)
		return -EIO;
	if(fail_missing && !missing.empty())
		/* print warning message? */
		return -ENOENT;
	if(!failed.empty())
		return -EBUSY;
	assert(data_size == data.end());
	return 0;
}

void sys_journal::deinit(bool erase)
{
	SYSJ_DEBUG("");
	if(meta_fd)
	{
		int r;
		if(dirty)
			flush_tx();
		assert(!dirty);
		if(registered)
		{
			tx_unregister_pre_end(&handle);
			registered = false;
		}
		r = data.close();
		assert(r >= 0);
		tx_close(meta_fd);
		if(erase)
		{
			char seq[16];
			snprintf(seq, sizeof(seq), ".%u", sequence);
			istr data_name = meta_name + seq;
			tx_unlink(meta_dfd, data_name, 0);
			tx_unlink(meta_dfd, meta_name, 0);
		}
		meta_fd = NULL;
		meta_name = NULL;
		if(meta_dfd >= 0 && meta_dfd != AT_FDCWD)
		{
			close(meta_dfd);
			meta_dfd = -1;
		}
		discarded.clear();
	}
}

sys_journal sys_journal::global_journal;
__gnu_cxx::hash_map<sys_journal::listener_id, sys_journal::journal_listener *> sys_journal::listener_map;

sys_journal::journal_listener * sys_journal::lookup_listener(listener_id id)
{
	return listener_map.count(id) ? listener_map[id] : NULL;
}

int sys_journal::register_listener(journal_listener * listener)
{
	std::pair<listener_id, journal_listener *> pair(listener->id(), listener);
	if(!listener_map.insert(pair).second)
		return -EEXIST;
	return 0;
}

void sys_journal::unregister_listener(journal_listener * listener)
{
	listener_map.erase(listener->id());
}

sys_journal::unique_id sys_journal::id;

int sys_journal::set_unique_id_file(int dfd, const char * file, bool create)
{
	int r;
	if(id.fd)
		tx_close(id.fd);
	id.fd = tx_open(dfd, file, 0);
	if(!id.fd)
	{
		if(!create || errno != ENOENT)
			return -1;
		id.fd = tx_open(dfd, file, 1);
		if(!id.fd)
			return -1;
create_empty:
		id.next = 0;
		r = tx_write(id.fd, &id.next, sizeof(id.next), 0);
		if(r < 0)
		{
			tx_close(id.fd);
			id.fd = NULL;
			unlinkat(dfd, file, 0);
			return r;
		}
	}
	else
	{
		r = tx_read(id.fd, &id.next, sizeof(id.next), 0);
		if(r != sizeof(id.next))
		{
			if(!r && create)
				goto create_empty;
			tx_close(id.fd);
			id.fd = NULL;
			return (r < 0) ? r : -1;
		}
	}
	return 0;
}

sys_journal::listener_id sys_journal::get_unique_id()
{
	int r;
	listener_id next;
	if(id.fd < 0)
		return NO_ID;
	next = id.next++;
	r = tx_write(id.fd, &id.next, sizeof(id.next), 0);
	if(r < 0)
	{
		id.next = next;
		return NO_ID;
	}
	return next;
}

int sys_journal::flush_tx()
{
	int r;
	meta_journal info;
	SYSJ_DEBUG("");
	
	if(!dirty)
		return 0;
	assert(data_size == data.end());
	r = data.flush();
	if(r < 0)
		return r;
	
	info.magic = SYSJ_META_MAGIC;
	info.version = SYSJ_META_VERSION;
	info.seq = sequence;
	info.size = data_size;
	r = tx_write(meta_fd, &info, sizeof(info), 0);
	if(r < 0)
		return r;
	
	dirty = false;
	info_size = data_size;
	assert(data_size == data.end());
	return 0;
}

void sys_journal::flush_tx_static(void * data)
{
	((sys_journal *) data)->registered = false;
	int r = ((sys_journal *) data)->flush_tx();
	assert(r >= 0);
}
