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

#define DEBUG_SYSJ 0

/* The system journal stores bulk data in an append-only chronological log,
 * using the transaction layer in order to make sure that entire groups of
 * records are appended as atomic units together with any other changes made
 * during the same transaction. Log entries are tagged with a system journal
 * "listener" ID, which is used to demultiplex the records when reading them
 * back. Each listener may also (and must, at some point) "discard" all of its
 * records, allowing the system journal to remove them in the future during an
 * operation called "filtering."
 * 
 * There are two types of system journal listener IDs: normal and temporary.
 * (Normal IDs are even and temporary IDs are odd, to easily distinguish them.)
 * Normal IDs have the requirement just mentioned, namely, that the listener
 * must eventually discard its records. Temporary listener IDs however will
 * automatically be discarded during the next recovery (i.e. after system
 * shutdown or failure), unless they are "rolled over" into a normal ID. If they
 * are rolled over then during playback the rollover will be replayed as well.
 * 
 * This feature allows a temporary system journal client, for instance one
 * storing information that is part of an abortable transaction, to write its
 * data to disk right away. However, it can still abort (and it should
 * explicitly discard its ID in this case) without needing to "undo" its
 * changes, since they are separated from the "main" updates. If it commits
 * instead, it needs only to write a small record rolling the ID over, since the
 * data is already part of the system journal.
 * 
 * How does all this work? The system journal basically appends the entries to a
 * log file using the transaction library's API for ensuring that file updates
 * are made before transaction commits. (Actually this part is done by rwfile.)
 * Each record is preceeded by a small header containing the listener ID and the
 * length of the data, with the length -1 meaning that this listener ID is being
 * discarded and -2 indicating a rollover record. During playback, when an ID
 * that has not previously been seen is encountered, a listener is created for
 * that ID using a "warehouse" (which is like a factory, but it also stores the
 * objects it creates). When discard and rollover records are found, the same
 * actions are taken on the objects in the warehouse. At the end of playback the
 * warehouse should contain the same listeners it did during the last run. */

#if DEBUG_SYSJ
#define SYSJ_DEBUG(format, args...) printf("%s @%p[%u %u/%u] (" format ")\n", __FUNCTION__, this, (unsigned) data.end(), (unsigned) data_size, (unsigned) info_size, ##args)
#define SYSJ_DEBUG_IN(format, args...) printf("%s [%u %u/%u] " format "\n", __FUNCTION__, (unsigned) data.end(), (unsigned) data_size, (unsigned) info_size, ##args)
#else
#define SYSJ_DEBUG(format, args...)
#define SYSJ_DEBUG_IN(format, args...)
#endif

#define SYSJ_META_MAGIC 0xBAFE9BDA
#define SYSJ_META_VERSION 1

#define SYSJ_DATA_MAGIC 0x874C74FD
#define SYSJ_DATA_VERSION 1

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

int sys_journal::append(listening_dtable * listener, void * entry, size_t length)
{
	int r;
	entry_header header;
	live_entry_map::iterator count;
	SYSJ_DEBUG("%d, %p, %zu", listener->id(), entry, length);
	
	header.id = listener->id();
	assert(warehouse->lookup(header.id) == listener);
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

int sys_journal::discard(listener_id lid)
{
	int r;
	entry_header header;
	live_entry_map::iterator count;
	SYSJ_DEBUG("%d", lid);
	
	count = live_entry_count.find(lid);
	if(count == live_entry_count.end())
		/* no sense in actually writing a discard
		 * record if there's nothing to discard */
		return 0;
	
	header.id = lid;
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
	
	discard_rollover_ids(lid);
	
	live_entries -= count->second;
	live_entry_count.erase(count);
	if(!live_entries && filter_on_empty)
		filter();
	
	assert(data_size == data.end());
	return 0;
}

int sys_journal::rollover(listener_id from, listener_id to)
{
	int r;
	entry_header header;
	live_entry_map::iterator from_count, to_count;
	SYSJ_DEBUG("%u, %u", from, to);
	
	if(!is_temporary(from))
		return -EINVAL;
	
	from_count = live_entry_count.find(from);
	if(from_count == live_entry_count.end())
		/* no sense in actually writing a rollover
		 * record if there's nothing to roll over */
		return 0;
	
	header.id = from;
	header.length = (size_t) -2;
	
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
	r = data.append(&to);
	if(r < 0)
	{
		data.truncate(-sizeof(header));
		return (r < 0) ? r : -1;
	}
	data_size += sizeof(header) + sizeof(to);
	
	/* update the live entry counts */
	to_count = live_entry_count.find(to);
	if(to_count == live_entry_count.end())
		live_entry_count[to] = from_count->second;
	else
		to_count->second += from_count->second;
	live_entry_count.erase(from_count);
	
	/* update the rollover ID map */
	roll_over_rollover_ids(from, to);
	
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
			size_t entry_length;
			r = data.read(offset, &entry);
			if(r < 0)
				goto fail;
			offset += sizeof(entry);
			if(entry.length == (size_t) -1)
				continue;
			/* check for a rollover record */
			if(entry.length == (size_t) -2)
				entry_length = sizeof(listener_id);
			else
				entry_length = entry.length;
			if(discarded.count(entry.id))
			{
				/* skip this entry, it's been discarded */
				offset += entry_length;
				continue;
			}
			entry_data = malloc(entry_length);
			if(!entry_data)
			{
				r = -ENOMEM;
				goto fail;
			}
			if(data.read(offset, entry_data, entry_length) != (ssize_t) entry_length)
			{
				free(entry_data);
				r = -EIO;
				goto fail;
			}
			offset += entry_length;
			r = out.append(&entry);
			if(r < 0)
			{
				free(entry_data);
				goto fail;
			}
			r = out.append(entry_data, entry_length);
			free(entry_data);
			if(r != (int) entry_length)
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

int sys_journal::init(int dfd, const char * file, listening_dtable_warehouse * warehouse, bool create, bool filter_on_empty)
{
	int r;
	meta_journal info;
	data_header header;
	bool do_playback = false;
	SYSJ_DEBUG("%d, %s, %d", dfd, file, create);
	if(warehouse->size())
		return -EINVAL;
	if(meta_fd)
		deinit();
	live_entries = 0;
	this->warehouse = warehouse;
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
		int r = playback();
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

void sys_journal::roll_over_rollover_ids(listener_id from, listener_id to, listener_id_set * remove)
{
	rollover_multimap::iterator from_set, to_set;
	to_set = rollover_ids.find(to);
	if(to_set == rollover_ids.end())
	{
		rollover_multimap::value_type value(to, listener_id_set());
		to_set = rollover_ids.insert(value).first;
	}
	to_set->second.insert(from);
	if(remove)
		remove->erase(from);
	from_set = rollover_ids.find(from);
	if(from_set != rollover_ids.end())
	{
		listener_id_set::iterator from_ids;
		for(from_ids = from_set->second.begin(); from_ids != from_set->second.end(); ++from_ids)
		{
			to_set->second.insert(*from_ids);
			if(remove)
				remove->erase(*from_ids);
		}
		rollover_ids.erase(from_set);
	}
}

void sys_journal::discard_rollover_ids(listener_id lid)
{
	rollover_multimap::iterator it = rollover_ids.find(lid);
	discarded.insert(lid);
	if(it != rollover_ids.end())
	{
		listener_id_set::iterator ids;
		for(ids = it->second.begin(); ids != it->second.end(); ++ids)
			discarded.insert(*ids);
		rollover_ids.erase(it);
	}
}

int sys_journal::playback()
{
	listener_id_set temporary;
	size_t offset = sizeof(data_header);
	SYSJ_DEBUG("");
	
	assert(offset <= info_size);
	assert(info_size <= data_size);
	assert(data_size == data.end());
	
	if(info_size != data_size)
		return -EINVAL;
	live_entries = 0;
	live_entry_count.clear();
	
	while(offset < info_size)
	{
		int r;
		void * entry_data;
		entry_header entry;
		listening_dtable * listener;
		if(data.read(offset, &entry) < 0)
			return -EIO;
		offset += sizeof(entry);
		if(entry.length == (size_t) -1)
		{
			/* this is a discard record */
			SYSJ_DEBUG_IN("discard %d", entry.id);
			live_entry_map::iterator count = live_entry_count.find(entry.id);
			if(count != live_entry_count.end())
			{
				live_entries -= count->second;
				live_entry_count.erase(count);
			}
			discard_rollover_ids(entry.id);
			if(is_temporary(entry.id))
				temporary.erase(entry.id);
			listener = warehouse->lookup(entry.id);
			if(listener)
			{
				warehouse->remove(listener);
				delete listener;
			}
			continue;
		}
		if(entry.length == (size_t) -2)
		{
			/* this is a rollover record */
			listener_id to;
			if(data.read(offset, &to) < 0)
				return -EIO;
			offset += sizeof(to);
			assert(is_temporary(entry.id));
			SYSJ_DEBUG_IN("rollover %d -> %d", entry.id, to);
			listening_dtable * from_ldt = warehouse->lookup(entry.id);
			if(from_ldt)
			{
				listening_dtable * to_ldt = warehouse->lookup(to);
				if(to_ldt)
				{
					r = from_ldt->rollover(to_ldt);
					assert(r >= 0);
					warehouse->remove(from_ldt);
					delete from_ldt;
				}
				else
					from_ldt->set_id(to);
			}
			if(is_temporary(to))
			{
				temporary.insert(to);
				roll_over_rollover_ids(entry.id, to);
			}
			else
				roll_over_rollover_ids(entry.id, to, &temporary);
			continue;
		}
		SYSJ_DEBUG_IN("record for ID %d, length %zu", entry.id, entry.length);
		live_entry_map::iterator count = live_entry_count.find(entry.id);
		if(count != live_entry_count.end())
			count->second++;
		else
			live_entry_count[entry.id] = 1;
		live_entries++;
		
		if(is_temporary(entry.id))
			temporary.insert(entry.id);
		
		entry_data = malloc(entry.length);
		if(!entry_data)
			return -ENOMEM;
		if(data.read(offset, entry_data, entry.length) != (ssize_t) entry.length)
		{
			free(entry_data);
			return -EIO;
		}
		offset += entry.length;
		
		listener = warehouse->obtain(entry.id, entry_data, entry.length, this);
		if(!listener)
		{
			free(entry_data);
			return -EIO;
		}
		/* data is passed by reference */
		r = listener->journal_replay(entry_data, entry.length);
		if(entry_data)
			free(entry_data);
		if(r < 0)
			return r;
	}
	if(offset != info_size)
		return -EIO;
	if(!temporary.empty())
	{
		/* abandoned temporary IDs were found, discard them */
		listener_id_set::iterator it;
		for(it = temporary.begin(); it != temporary.end(); ++it)
		{
			listening_dtable * listener = warehouse->lookup(*it);
			assert(listener);
			discard(*it);
			assert(listener->get_warehouse() == warehouse);
			warehouse->remove(listener);
			delete listener;
		}
	}
	assert(data_size == data.end());
	return 0;
}

void sys_journal::deinit(bool erase)
{
	SYSJ_DEBUG("%d", erase);
	if(meta_fd)
	{
		int r;
		if(dirty)
			flush_tx();
		assert(!dirty);
		/* destroy all the listeners by clearing the warehouse */
		warehouse->clear();
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

sys_journal * sys_journal::spawn_init(const char * file, listening_dtable_warehouse * warehouse, bool create, bool filter_on_empty)
{
	int r;
	sys_journal * journal;
	if(!global_journal.meta_fd)
		return NULL;
	journal = new sys_journal;
	if(!journal)
		return NULL;
	r = journal->init(global_journal.meta_dfd, file, warehouse, create, filter_on_empty);
	if(r < 0)
	{
		delete journal;
		journal = NULL;
	}
	return journal;
}

sys_journal sys_journal::global_journal;
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
		id.next[0] = 0;
		id.next[1] = 1;
		r = tx_write(id.fd, id.next, sizeof(id.next), 0);
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
		r = tx_read(id.fd, id.next, sizeof(id.next), 0);
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

sys_journal::listener_id sys_journal::get_unique_id(bool temporary)
{
	int r, index = temporary ? 1 : 0;
	listener_id next;
	if(id.fd < 0)
		return NO_ID;
	next = id.next[index];
	id.next[index] += 2;
	r = tx_write(id.fd, id.next, sizeof(id.next), 0);
	if(r < 0)
	{
		id.next[index] = next;
		return NO_ID;
	}
	assert(!temporary == !is_temporary(next));
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

void sys_journal::listening_dtable::replay_pending()
{
	assert(blob_cmp);
	assert(!cmp_name || !strcmp(blob_cmp->name, cmp_name));
	ldt_unique.clear();
	while(!ldt_pending.empty())
	{
		const pending_entry & entry = ldt_pending.first();
		accept(entry.key, entry.value, entry.append);
		ldt_pending.pop();
	}
}

int sys_journal::listening_dtable::pending_rollover(listening_dtable * target) const
{
	unique_blob_store::iterator it;
	target->ldt_pending.append(ldt_pending);
	for(it = ldt_unique.begin(); it != ldt_unique.end(); ++it)
	{
		unique_blob_store::iterator ukey = target->ldt_unique.find(*it);
		if(ukey == target->ldt_unique.end())
			target->ldt_unique.insert(*it);
	}
	ldt_unique.clear();
	return 0;
}
