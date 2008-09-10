/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
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
	off_t size;
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
#define SYSJ_DEBUG(format, args...) printf("%s @%p[%u %u] (" format ")\n", __FUNCTION__, this, (unsigned) data.end(), (unsigned) data_size, ##args)
#else
#define SYSJ_DEBUG(format, args...)
#endif

int sys_journal::append(journal_listener * listener, void * entry, size_t length)
{
	int r;
	entry_header header;
	SYSJ_DEBUG("%d, %p, %u", listener->id(), entry, length);
	
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	assert(!discarded.count(header.id));
	if(length == (size_t) -1)
		return -EINVAL;
	header.length = length;
	
	assert(data.end() == data_size);
	if(pid <= 0)
	{
		pid = patchgroup_create(0);
		if(pid <= 0)
			return pid ? (int) pid : -1;
		r = patchgroup_release(pid);
		assert(r >= 0);
		r = data.set_pid(pid);
		assert(r >= 0);
		tx_register_pre_end(&handle);
	}
	assert(data.get_pid() == pid);
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
	
	assert(data.end() == data_size);
	return 0;
}

int sys_journal::discard(journal_listener * listener)
{
	int r;
	entry_header header;
	SYSJ_DEBUG("%d", listener->id());
	
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	header.length = (size_t) -1;
	
	assert(data.end() == data_size);
	if(pid <= 0)
	{
		pid = patchgroup_create(0);
		if(pid <= 0)
			return pid ? (int) pid : -1;
		r = patchgroup_release(pid);
		assert(r >= 0);
		r = data.set_pid(pid);
		assert(r >= 0);
		tx_register_pre_end(&handle);
	}
	r = data.append(&header);
	if(r < 0)
		return r;
	data_size += sizeof(header);
	discarded.insert(header.id);
	
	assert(data.end() == data_size);
	return 0;
}

int sys_journal::get_entries(journal_listener * listener)
{
	return playback(listener);
}

int sys_journal::filter()
{
	int r;
	char seq[16];
	meta_journal info;
	SYSJ_DEBUG("");
	
	if(pid > 0)
	{
		r = flush_tx();
		if(r < 0)
			return r;
		assert(pid <= 0);
	}
	
	info.magic = SYSJ_META_MAGIC;
	info.version = SYSJ_META_VERSION;
	info.seq = sequence + 1;
	/* size will be filled in by filter() below */
	snprintf(seq, sizeof(seq), ".%u", info.seq);
	assert(data.end() == data_size);
	
	istr data_name = istr(meta_name) + seq;
	pid = patchgroup_create(0);
	if(pid <= 0)
		return pid ? (int) pid : -1;
	r = patchgroup_release(pid);
	assert(r >= 0);
	patchgroup_engage(pid);
	r = filter(meta_dfd, data_name, &info.size);
	patchgroup_disengage(pid);
	if(r >= 0)
	{
		tx_add_depend(pid);
		patchgroup_abandon(pid);
		r = tx_write(meta_fd, &info, sizeof(info), 0);
		if(r >= 0)
		{
			/* switch to the new data file */
			r = data.close();
			assert(r >= 0);
			data_size = info.size;
			r = data.open(meta_dfd, data_name, data_size);
			assert(r >= 0);
			/* delete the old sys_journal data */
			snprintf(seq, sizeof(seq), ".%u", sequence);
			data_name = meta_name + seq;
			tx_unlink(meta_dfd, data_name);
			sequence = info.seq;
		}
		else
			unlinkat(meta_dfd, data_name, 0);
	}
	else
	{
		patchgroup_abandon(pid);
		unlinkat(meta_dfd, data_name, 0);
	}
	pid = 0;
	assert(data.end() == data_size);
	return r;
}

int sys_journal::filter(int dfd, const char * file, off_t * new_size)
{
	rwfile out;
	off_t offset;
	data_header header;
	entry_header entry;
	SYSJ_DEBUG("%d, %s", dfd, file);
	int r = out.create(dfd, file);
	if(r < 0)
		return r;
	header.magic = SYSJ_DATA_MAGIC;
	header.version = SYSJ_DATA_VERSION;
	r = out.append(&header);
	if(r < 0)
		goto fail;
	offset = sizeof(header);
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

int sys_journal::init(int dfd, const char * file, bool create, bool fail_missing)
{
	bool do_playback = false;
	SYSJ_DEBUG("%d, %s, %d, %d", dfd, file, create, fail_missing);
	if(meta_fd >= 0)
		deinit();
	meta_fd = tx_open(dfd, file, O_RDWR);
	if(meta_fd < 0)
	{
		int r;
		meta_journal info;
		data_header header;
		
		if(!create || (meta_fd != -ENOENT && errno != ENOENT))
			return (int) meta_fd;
		
		/* due to O_CREAT not being part of the transaction, we might
		 * get an empty file as a result of this, which later will
		 * cause an error below instead of here... hence create: */
		meta_fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
		if(meta_fd < 0)
			return (int) meta_fd;
	create:
		istr data_name = istr(file) + ".0";
		pid = patchgroup_create(0);
		if(pid <= 0)
		{
			r = pid ? (int) pid : -1;
			goto fail_pid;
		}
		patchgroup_release(pid);
		patchgroup_engage(pid);
		
		r = data.create(dfd, data_name);
		if(r < 0)
		{
			patchgroup_disengage(pid);
			patchgroup_abandon(pid);
			pid = 0;
			goto fail_pid;
		}
		header.magic = SYSJ_DATA_MAGIC;
		header.version = SYSJ_DATA_VERSION;
		r = data.append(&header);
		patchgroup_disengage(pid);
		if(r < 0)
		{
			patchgroup_abandon(pid);
			pid = 0;
			goto fail_create;
		}
		tx_add_depend(pid);
		patchgroup_abandon(pid);
		pid = 0;
		
		info.magic = SYSJ_META_MAGIC;
		info.version = SYSJ_META_VERSION;
		info.seq = 0;
		info.size = sizeof(header);
		r = tx_write(meta_fd, &info, sizeof(info), 0);
		if(r < 0)
		{
		fail_create:
			data.close();
			unlinkat(dfd, data_name, 0);
		fail_pid:
			tx_close(meta_fd);
			tx_unlink(dfd, file);
			meta_fd = -1;
			return r;
		}
		data_size = info.size;
		sequence = info.seq;
	}
	else
	{
		char seq[16];
		meta_journal info;
		data_header header;
		int r;
		if(tx_read(meta_fd, &info, sizeof(info), 0) != sizeof(info))
		{
			if(tx_emptyfile(meta_fd))
				goto create;
			tx_close(meta_fd);
			meta_fd = -1;
			return -1;
		}
		if(info.magic != SYSJ_META_MAGIC || info.version != SYSJ_META_VERSION)
		{
			tx_close(meta_fd);
			meta_fd = -1;
			return -EINVAL;
		}
		snprintf(seq, sizeof(seq), ".%u", info.seq);
		istr data_name = istr(file) + seq;
		r = data.open(dfd, data_name, info.size);
		if(r < 0)
		{
			tx_close(meta_fd);
			meta_fd = -1;
			return r;
		}
		r = data.read(0, &header);
		if(r < 0)
		{
			data.close();
			tx_close(meta_fd);
			meta_fd = -1;
			return r;
		}
		if(header.magic != SYSJ_DATA_MAGIC || header.version != SYSJ_DATA_VERSION)
		{
			data.close();
			tx_close(meta_fd);
			meta_fd = -1;
			return -EINVAL;
		}
		data_size = info.size;
		sequence = info.seq;
		do_playback = true;
	}
	/* any other initialization here */
	if(do_playback)
	{
		int r = playback(NULL, fail_missing);
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

int sys_journal::playback(journal_listener * target, bool fail_missing)
{
	/* playback */
	std::set<listener_id> missing, failed;
	off_t offset = sizeof(data_header);
	SYSJ_DEBUG("%d, %d", target ? target->id() : 0, fail_missing);
	assert(data_size >= offset);
	assert(data.end() == data_size);
	while(offset < data_size)
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
			/* warn if entry.id == target->id() ? */
			if(!target)
			{
				missing.erase(entry.id);
				discarded.insert(entry.id);
			}
			continue;
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
	if(offset != data_size)
		return -EIO;
	if(fail_missing && !missing.empty())
		/* print warning message? */
		return -ENOENT;
	if(!failed.empty())
		return -EBUSY;
	assert(data.end() == data_size);
	return 0;
}

void sys_journal::deinit()
{
	SYSJ_DEBUG("");
	if(meta_fd >= 0)
	{
		int r;
		if(pid > 0)
			flush_tx();
		assert(pid <= 0);
		discarded.clear();
		r = data.close();
		assert(r >= 0);
		tx_close(meta_fd);
		meta_fd = -1;
		meta_name = NULL;
		if(meta_dfd >= 0 && meta_dfd != AT_FDCWD)
		{
			close(meta_dfd);
			meta_dfd = -1;
		}
	}
}

sys_journal sys_journal::global_journal;
std::map<sys_journal::listener_id, sys_journal::journal_listener *> sys_journal::listener_map;

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
	if(id.fd >= 0)
		tx_close(id.fd);
	id.fd = tx_open(dfd, file, O_RDWR);
	if(id.fd < 0)
	{
		if(!create || (id.fd != -ENOENT && errno != ENOENT))
			return (int) id.fd;
		id.next = 0;
		id.fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
		if(id.fd < 0)
			return (int) id.fd;
		r = tx_write(id.fd, &id.next, sizeof(id.next), 0);
		if(r < 0)
		{
			tx_close(id.fd);
			id.fd = -1;
			unlinkat(dfd, file, 0);
			return r;
		}
	}
	else
	{
		r = tx_read(id.fd, &id.next, sizeof(id.next), 0);
		if(r != sizeof(id.next))
		{
			tx_close(id.fd);
			id.fd = -1;
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
	
	if(pid <= 0)
		return 0;
	assert(data.get_pid() == pid);
	assert(data.end() == data_size);
	r = data.flush();
	if(r < 0)
		return r;
	tx_add_depend(pid);
	
	info.magic = SYSJ_META_MAGIC;
	info.version = SYSJ_META_VERSION;
	info.seq = sequence;
	info.size = data_size;
	r = tx_write(meta_fd, &info, sizeof(info), 0);
	if(r < 0)
		return r;
	
	patchgroup_abandon(pid);
	data.set_pid(0);
	pid = 0;
	
	assert(data.end() == data_size);
	return 0;
}

void sys_journal::flush_tx_static(void * data)
{
	int r = ((sys_journal *) data)->flush_tx();
	assert(r >= 0);
}
