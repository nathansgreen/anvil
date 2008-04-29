/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MANAGED_DTABLE_H
#define __MANAGED_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "transaction.h"

#ifndef __cplusplus
#error managed_dtable.h is a C++ header file
#endif

#include <vector>

#include "writable_dtable.h"
#include "simple_dtable.h"
#include "overlay_dtable.h"
#include "journal_dtable.h"

/* A managed dtable is really a collection of dtables: zero or more disk dtables
 * (e.g. simple_dtable), a journal dtable, and an overlay dtable to connect
 * everything together. It supports merging together various numbers of these
 * constituent dtables into new, combined disk dtables with the same data. */

#define MDTABLE_MAGIC 0x784D3DB7
#define MDTABLE_VERSION 0

class managed_dtable : public writable_dtable
{
public:
	/* send to overlay_dtable */
	inline virtual dtable_iter * iterator() const
	{
		return overlay->iterator();
	}
	inline virtual blob lookup(dtype key, const dtable ** source) const
	{
		return overlay->lookup(key, source);
	}
	
	/* send to journal_dtable */
	inline virtual int append(dtype key, const blob & blob)
	{
		if(!blob.exists() && !find(key).exists())
			return 0;
		return journal->append(key, blob);
	}
	inline virtual int remove(dtype key)
	{
		if(!find(key).exists())
			return 0;
		return journal->remove(key);
	}
	
	/* return the number of disk dtables */
	inline size_t disk_dtables()
	{
		return disks.size();
	}
	
	/* combine some dtables; first and last are inclusive */
	/* the dtables are indexed starting at 0 (the "oldest"), with the last
	 * one being the journal dtable; disk_dtables() returns the number of
	 * non-journal dtables, so the journal's index is the value returned by
	 * disk_dtables() (the journal dtable being the "newest") */
	/* there is always a journal dtable - if it is combined, a new one is
	 * created to take its place */
	int combine(size_t first, size_t last);
	
	/* combine the last count dtables into two new ones: a combined disk
	 * dtable, and a new journal dtable */
	inline int combine(size_t count = 0)
	{
		if(--count > disks.size())
			return combine(0, disks.size());
		return combine(disks.size() - count, disks.size());
	}
	
	/* digests the journal dtable into a new disk dtable */
	inline int digest()
	{
		size_t journal = disk_dtables();
		return combine(journal, journal);
	}
	
	/* these are expected to be implemented by templatized subclasses */
	virtual dtable * disk_open(int dfd, const char * file) = 0;
	virtual int disk_create(int dfd, const char * file, const dtable * source, const dtable * shadow) = 0;
	
	static int create(int dfd, const char * name, dtype::ctype key_type);
	
	inline managed_dtable() : md_dfd(-1) {}
	int init(int dfd, const char * name, bool query_journal = false, sys_journal * sys_journal = NULL);
	void deinit();
	inline virtual ~managed_dtable()
	{
		if(md_dfd >= 0)
			deinit();
	}
	
private:
	struct mdtable_header
	{
		uint32_t magic;
		uint16_t version;
		uint8_t key_type;
		uint8_t reserved;
		sys_journal::listener_id journal_id;
		uint32_t ddt_count, ddt_next;
	} __attribute__((packed));
	
	typedef std::pair<dtable *, uint32_t> dtable_list_entry;
	typedef std::vector<dtable_list_entry> dtable_list;
	
	int md_dfd;
	mdtable_header header;
	
	dtable_list disks;
	overlay_dtable * overlay;
	journal_dtable * journal;
};

template<class T>
class managed_disk_dtable : public managed_dtable
{
public:
	virtual dtable * disk_open(int dfd, const char * file)
	{
		T * disk = new T;
		int r = disk->init(dfd, file);
		if(r < 0)
		{
			delete disk;
			disk = NULL;
		}
		return disk;
	}
	virtual int disk_create(int dfd, const char * file, const dtable * source, const dtable * shadow)
	{
		return T::create(dfd, file, source, shadow);
	}
};

typedef managed_disk_dtable<simple_dtable> managed_simple_dtable;

#endif /* __MANAGED_DTABLE_H */
