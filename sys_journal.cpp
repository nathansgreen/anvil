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

/* Currently, sys_journal is built on top of transactions, which are built on
 * top of journal.c, which is built on top of Featherstitch patchgroups, which
 * may be implemented using a journal. There are too many journals here, but we
 * can optimize that later... */

#define SYSJ_MAGIC 0xBAFE9BDA
#define SYSJ_VERSION 0

struct file_header
{
	uint32_t magic, version;
} __attribute__((packed));

struct entry_header
{
	sys_journal::listener_id id;
	size_t length;
} __attribute__((packed));

int sys_journal::append(journal_listener * listener, void * entry, size_t length)
{
	entry_header header;
	header.id = listener->id();
	assert(lookup_listener(header.id) == listener);
	header.length = length;
	if(tx_write(fd, &header, offset, sizeof(header)) < 0)
		return -1;
	offset += sizeof(header);
	if(tx_write(fd, entry, offset, length) < 0)
	{
		/* need to unwrite the header */
		assert(0);
		return -1;
	}
	offset += length;
	return 0;
}

int sys_journal::get_entries(journal_listener * listener)
{
	return playback(listener);
}

int sys_journal::init(int dfd, const char * file, bool create)
{
	bool do_playback = false;
	if(fd >= 0)
		deinit();
	fd = tx_open(dfd, file, O_RDWR);
	if(fd < 0)
	{
		if(create && (fd == -ENOENT || errno == ENOENT))
		{
			int r;
			file_header header;
			/* XXX due to O_CREAT not being part of the transaction, we might get
			 * an empty file as a result of this, which later will cause an error
			 * below instead of here... we should handle that case somewhere */
			fd = tx_open(dfd, file, O_RDWR | O_CREAT, 0644);
			if(fd < 0)
				return (int) fd;
			header.magic = SYSJ_MAGIC;
			header.version = SYSJ_VERSION;
			r = tx_write(fd, &header, 0, sizeof(header));
			if(r < 0)
			{
				tx_close(fd);
				tx_unlink(dfd, file);
				fd = -1;
				return r;
			}
			offset = sizeof(header);
		}
		else
			return (int) fd;
	}
	else
		do_playback = true;
	/* any other initialization here */
	if(do_playback)
	{
		int r = playback();
		if(r < 0)
		{
			deinit();
			return r;
		}
	}
	return 0;
}

int sys_journal::playback(journal_listener * target)
{
	/* playback */
	file_header header;
	entry_header entry;
	int r, ufd = tx_read_fd(fd);
	assert(ufd >= 0);
	r = lseek(ufd, 0, SEEK_SET);
	if(r < 0)
		return r;
	if((r = read(ufd, &header, sizeof(header))) != sizeof(header))
		return (r < 0) ? r : -EIO;
	if(header.magic != SYSJ_MAGIC || header.version != SYSJ_VERSION)
		return -EINVAL;
	while((r = read(ufd, &entry, sizeof(entry))) == sizeof(entry))
	{
		void * data;
		journal_listener * listener;
		if(target)
		{
			listener = target;
			/* skip other entries */
			if(listener->id() != entry.id)
				continue;
		}
		else
		{
			listener = lookup_listener(entry.id);
			if(!listener)
				return -ENOENT;
		}
		data = malloc(entry.length);
		if(!data)
			return -ENOMEM;
		if(read(ufd, data, entry.length) != (ssize_t) entry.length)
		{
			free(data);
			return -EIO;
		}
		/* data is passed by reference */
		r = listener->journal_replay(data, entry.length);
		if(data)
			free(data);
		if(r < 0)
			return r;
	}
	assert(r <= 0);
	offset = lseek(ufd, 0, SEEK_END);
	return r;
}

void sys_journal::deinit()
{
	if(fd >= 0)
	{
		tx_close(fd);
		fd = -1;
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
		r = tx_write(id.fd, &id.next, 0, sizeof(id.next));
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
	r = tx_write(id.fd, &id.next, 0, sizeof(id.next));
	if(r < 0)
	{
		id.next = next;
		return NO_ID;
	}
	return next;
}
