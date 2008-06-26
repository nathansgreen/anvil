/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>

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

int sys_journal::append(journal_listener * listener, void * entry, size_t length)
{
	int r;
	entry_header header;
	
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	assert(!discarded.count(header.id));
	if(length == (size_t) -1)
		return -EINVAL;
	header.length = length;
	
	lseek(data_fd, data_size, SEEK_SET);
	if(pid <= 0)
	{
		pid = patchgroup_create(0);
		if(pid <= 0)
			return pid ? (int) pid : -1;
		r = patchgroup_release(pid);
		assert(r >= 0);
		tx_register_pre_end(&handle);
	}
	r = patchgroup_engage(pid);
	assert(r >= 0);
	if(write(data_fd, &header, sizeof(header)) != sizeof(header)
	   || write(data_fd, entry, length) != (ssize_t) length)
	{
		r = patchgroup_disengage(pid);
		assert(r >= 0);
		r = patchgroup_abandon(pid);
		assert(r >= 0);
		return -1;
	}
	patchgroup_disengage(pid);
	data_size += sizeof(header) + length;
	
	return 0;
}

int sys_journal::discard(journal_listener * listener)
{
	int r;
	entry_header header;
	
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	header.length = (size_t) -1;
	
	lseek(data_fd, data_size, SEEK_SET);
	if(pid <= 0)
	{
		pid = patchgroup_create(0);
		if(pid <= 0)
			return pid ? (int) pid : -1;
		r = patchgroup_release(pid);
		assert(r >= 0);
		tx_register_pre_end(&handle);
	}
	r = patchgroup_engage(pid);
	assert(r >= 0);
	if(write(data_fd, &header, sizeof(header)) != sizeof(header))
	{
		r = patchgroup_disengage(pid);
		assert(r >= 0);
		r = patchgroup_abandon(pid);
		assert(r >= 0);
		return -1;
	}
	patchgroup_disengage(pid);
	data_size += sizeof(header);
	discarded.insert(header.id);
	
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
	
	istr data = istr(meta_name) + seq;
	pid = patchgroup_create(0);
	if(pid <= 0)
		return pid ? (int) pid : -1;
	r = patchgroup_release(pid);
	assert(r >= 0);
	patchgroup_engage(pid);
	r = filter(meta_dfd, data, &info.size);
	patchgroup_disengage(pid);
	if(r >= 0)
	{
		tx_add_depend(pid);
		patchgroup_abandon(pid);
		r = tx_write(meta_fd, &info, sizeof(info), 0);
		if(r >= 0)
		{
			/* switch to the new data file */
			close(data_fd);
			data_fd = openat(meta_dfd, data, O_RDWR);
			assert(data_fd >= 0);
			data_size = info.size;
			lseek(data_fd, data_size, SEEK_SET);
			/* delete the old sys_journal data */
			snprintf(seq, sizeof(seq), ".%u", sequence);
			data = meta_name + seq;
			tx_unlink(meta_dfd, data);
			sequence = info.seq;
		}
		else
			unlinkat(meta_dfd, data, 0);
	}
	else
	{
		patchgroup_abandon(pid);
		unlinkat(meta_dfd, data, 0);
	}
	pid = 0;
	return r;
}

int sys_journal::filter(int dfd, const char * file, off_t * new_size)
{
	int r, out;
	off_t offset;
	data_header header;
	entry_header entry;
	out = openat(dfd, file, O_WRONLY | O_TRUNC | O_CREAT, 0644);
	if(out < 0)
		return out;
	header.magic = SYSJ_DATA_MAGIC;
	header.version = SYSJ_DATA_VERSION;
	r = write(out, &header, sizeof(header));
	if(r != sizeof(header))
		goto fail;
	offset = sizeof(header);
	r = lseek(data_fd, offset, SEEK_SET);
	if(r < 0)
		goto fail;
	while(offset < data_size)
	{
		void * data;
		r = read(data_fd, &entry, sizeof(entry));
		if(r != sizeof(entry))
			goto fail;
		offset += sizeof(entry);
		if(entry.length == (size_t) -1)
			continue;
		offset += entry.length;
		if(discarded.count(entry.id))
		{
			/* skip this entry, it's been discarded */
			lseek(data_fd, entry.length, SEEK_CUR);
			continue;
		}
		data = malloc(entry.length);
		if(!data)
		{
			r = -ENOMEM;
			goto fail;
		}
		if(read(data_fd, data, entry.length) != (ssize_t) entry.length)
		{
			free(data);
			r = -EIO;
			goto fail;
		}
		r = write(out, &entry, sizeof(entry));
		if(r != sizeof(entry))
		{
			free(data);
			goto fail;
		}
		r = write(out, data, entry.length);
		free(data);
		if(r != (int) entry.length)
			goto fail;
	}
	if(offset != data_size)
		goto fail;
	*new_size = lseek(out, 0, SEEK_END);
	close(out);
	lseek(data_fd, data_size, SEEK_SET);
	return 0;
	
fail:
	close(out);
	unlinkat(dfd, file, 0);
	lseek(data_fd, data_size, SEEK_SET);
	return (r < 0) ? r : -1;
}

int sys_journal::init(int dfd, const char * file, bool create, bool fail_missing)
{
	bool do_playback = false;
	if(data_fd >= 0)
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
		istr data = istr(file) + ".0";
		pid = patchgroup_create(0);
		if(pid <= 0)
		{
			tx_close(meta_fd);
			tx_unlink(dfd, file);
			return pid ? (int) pid : -1;
		}
		patchgroup_release(pid);
		patchgroup_engage(pid);
		
		data_fd = openat(dfd, data, O_RDWR | O_CREAT | O_TRUNC, 0644);
		header.magic = SYSJ_DATA_MAGIC;
		header.version = SYSJ_DATA_VERSION;
		r = write(data_fd, &header, sizeof(header));
		patchgroup_disengage(pid);
		if(r != sizeof(header))
		{
			patchgroup_abandon(pid);
			pid = 0;
			r = -1;
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
			close(data_fd);
			unlinkat(dfd, data, 0);
			tx_close(meta_fd);
			tx_unlink(dfd, file);
			meta_fd = -1;
			data_fd = -1;
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
		int fd = tx_read_fd(meta_fd);
		if(read(fd, &info, sizeof(info)) != sizeof(info))
		{
			struct stat st;
			int r = fstat(fd, &st);
			if(r >= 0 && !st.st_size)
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
		istr data = istr(file) + seq;
		data_fd = openat(dfd, data, O_RDWR);
		if(data_fd < 0)
		{
			tx_close(meta_fd);
			meta_fd = -1;
			return data_fd;
		}
		if(read(data_fd, &header, sizeof(header)) != sizeof(header))
		{
			close(data_fd);
			tx_close(meta_fd);
			meta_fd = -1;
			data_fd = -1;
			return -1;
		}
		if(header.magic != SYSJ_DATA_MAGIC || header.version != SYSJ_DATA_VERSION)
		{
			close(data_fd);
			tx_close(meta_fd);
			meta_fd = -1;
			data_fd = -1;
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
	std::set<listener_id> missing;
	off_t offset = sizeof(data_header);
	assert(data_size >= offset);
	lseek(data_fd, offset, SEEK_SET);
	while(offset < data_size)
	{
		int r;
		void * data;
		entry_header entry;
		journal_listener * listener;
		if(read(data_fd, &entry, sizeof(entry)) != sizeof(entry))
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
			listener = target;
			/* skip other entries */
			if(listener->id() != entry.id)
			{
				lseek(data_fd, entry.length, SEEK_CUR);
				offset += entry.length;
				continue;
			}
		}
		else
		{
			listener = lookup_listener(entry.id);
			if(!listener)
			{
				missing.insert(entry.id);
				lseek(data_fd, entry.length, SEEK_CUR);
				offset += entry.length;
				continue;
			}
		}
		data = malloc(entry.length);
		if(!data)
			return -ENOMEM;
		if(read(data_fd, data, entry.length) != (ssize_t) entry.length)
		{
			free(data);
			return -EIO;
		}
		offset += entry.length;
		/* data is passed by reference */
		r = listener->journal_replay(data, entry.length);
		if(data)
			free(data);
		if(r < 0)
			return r;
	}
	if(offset != data_size)
		return -EIO;
	if(fail_missing && !missing.empty())
		/* print warning message? */
		return -ENOENT;
	return 0;
}

void sys_journal::deinit()
{
	if(data_fd >= 0)
	{
		assert(meta_fd >= 0);
		if(pid > 0)
			flush_tx();
		assert(pid <= 0);
		discarded.clear();
		close(data_fd);
		tx_close(meta_fd);
		meta_fd = -1;
		data_fd = -1;
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
		r = read(tx_read_fd(id.fd), &id.next, sizeof(id.next));
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
	meta_journal info;
	
	if(pid <= 0)
		return 0;
	tx_add_depend(pid);
	
	info.magic = SYSJ_META_MAGIC;
	info.version = SYSJ_META_VERSION;
	info.seq = sequence;
	info.size = data_size;
	if(tx_write(meta_fd, &info, sizeof(info), 0) < 0)
		return -1;
	
	patchgroup_abandon(pid);
	pid = 0;
	
	return 0;
}

void sys_journal::flush_tx_static(void * data)
{
	int r = ((sys_journal *) data)->flush_tx();
	assert(r >= 0);
}
