/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MANAGED_DTABLE_H
#define __MANAGED_DTABLE_H

#include <time.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error managed_dtable.h is a C++ header file
#endif

#include <vector>

#include "dtable.h"
#include "dtable_factory.h"
#include "overlay_dtable.h"
#include "journal_dtable.h"

/* A managed dtable is really a collection of dtables: zero or more disk dtables
 * (e.g. simple_dtable), a journal dtable, and an overlay dtable to connect
 * everything together. It supports merging together various numbers of these
 * constituent dtables into new, combined disk dtables with the same data. */

#define MDTABLE_MAGIC 0x784D3DB7
#define MDTABLE_VERSION 1

class managed_dtable : public dtable
{
public:
	/* send to overlay_dtable */
	inline virtual iter * iterator() const
	{
		/* returns overlay->iterator() */
		return iterator_chain_usage(&chain, overlay);
	}
	inline virtual bool present(const dtype & key, bool * found) const
	{
		return overlay->present(key, found);
	}
	inline virtual blob lookup(const dtype & key, bool * found) const
	{
		return overlay->lookup(key, found);
	}
	
	inline virtual bool writable() const { return true; }
	
	/* send to journal_dtable */
	inline virtual int insert(const dtype & key, const blob & blob, bool append = false)
	{
		int r;
		if(!blob.exists() && !contains(key))
			return 0;
		r = journal->insert(key, blob, append);
		if(r >= 0 && digest_size && journal->size() >= digest_size)
			r = digest();
		return r;
	}
	inline virtual int remove(const dtype & key)
	{
		int r;
		if(!find(key).exists())
			return 0;
		r = journal->remove(key);
		if(r >= 0 && digest_size && journal->size() >= digest_size)
			r = digest();
		return r;
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
	/* note that there is always a journal dtable - if it is combined, a new
	 * one is created to take its place */
	int combine(size_t first, size_t last, bool use_fastbase = false);
	
	/* combine the last count dtables into two new ones: a combined disk
	 * dtable, and a new journal dtable */
	inline int combine(size_t count = 0, bool use_fastbase = false)
	{
		size_t journal = disks.size();
		if(--count > journal)
			return combine(0, journal, use_fastbase);
		return combine(journal - count, journal, use_fastbase);
	}
	
	/* digests the journal dtable into a new disk dtable */
	inline int digest(bool use_fastbase = true)
	{
		size_t journal = disks.size();
		return combine(journal, journal, use_fastbase);
	}
	
	/* do maintenance based on parameters */
	virtual int maintain(bool force = false);
	
	static int create(int dfd, const char * name, const params & config, dtype::ctype key_type);
	DECLARE_RW_FACTORY(managed_dtable);
	
	inline managed_dtable() : md_dfd(-1), chain(this) {}
	int init(int dfd, const char * name, const params & config, sys_journal * sys_journal = NULL);
	void deinit();
	inline virtual ~managed_dtable()
	{
		if(md_dfd >= 0)
			deinit();
	}
	
	virtual int set_blob_cmp(const blob_comparator * cmp);
	
private:
	struct mdtable_header
	{
		uint32_t magic;
		uint16_t version;
		uint8_t key_type;
		uint8_t combine_count;
		sys_journal::listener_id journal_id;
		uint32_t ddt_count, ddt_next;
		time_t digest_interval, digested;
		/* we will need something more advanced than this */
		time_t combine_interval, combined;
		/* autocombining is better but still not enough */
		uint32_t autocombine_digests;
		uint32_t autocombine_digest_count;
		uint32_t autocombine_combine_count;
	} __attribute__((packed));
	
	struct mdtable_entry
	{
		uint32_t ddt_number;
		uint8_t is_fastbase;
	} __attribute__((packed));
	
	struct dtable_list_entry
	{
		dtable * disk;
		uint32_t ddt_number;
		bool is_fastbase;
		inline dtable_list_entry(dtable * dtable, const mdtable_entry & entry)
			: disk(dtable), ddt_number(entry.ddt_number), is_fastbase(entry.is_fastbase)
		{
		}
		inline dtable_list_entry(dtable * dtable, uint32_t number, bool fastbase)
			: disk(dtable), ddt_number(number), is_fastbase(fastbase)
		{
		}
	};
	typedef std::vector<dtable_list_entry> dtable_list;
	
	/* this class handles managed dtable combine operations */
	class combiner
	{
	public:
		inline combiner(managed_dtable * mdt, size_t first, size_t last, bool use_fastbase)
			: mdt(mdt), first(first), last(last), use_fastbase(use_fastbase), source(NULL), shadow(NULL), reset_journal(false)
		{
		}
		int prepare();
		int run() const;
		int finish();
		void fail();
		inline ~combiner()
		{
			if(source)
				fail();
		}
	private:
		managed_dtable * mdt;
		size_t first, last;
		const bool use_fastbase;
		overlay_dtable * source;
		overlay_dtable * shadow;
		bool reset_journal;
		char name[32];
	};
	
	int maintain_autocombine();
	
	int md_dfd;
	mdtable_header header;
	
	dtable_list disks;
	overlay_dtable * overlay;
	mutable chain_callback chain;
	journal_dtable * journal;
	const dtable_factory * base;
	const dtable_factory * fastbase;
	params base_config, fastbase_config;
	/* in case a blob comparator is needed for the journal, it may return
	 * -EBUSY and we will need to query it later when the blob comparator is
	 *  set; we set delayed_query when this case is detected in init() */
	sys_journal * delayed_query;
	size_t digest_size;
	bool digest_on_close, close_digest_fastbase, autocombine;
};

#endif /* __MANAGED_DTABLE_H */
