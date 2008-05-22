/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include "openat.h"
#include "transaction.h"

#include "managed_dtable.h"

int managed_dtable::init(int dfd, const char * name, const params & config, sys_journal * sys_journal)
{
	int r = -1, meta;
	bool query_journal;
	if(md_dfd >= 0)
		deinit();
	base = dt_factory_registry::lookup(config, "base");
	if(!base)
		return -EINVAL;
	if(!config.get("query_journal", &query_journal, true))
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
		return md_dfd;
	meta = openat(md_dfd, "md_meta", O_RDONLY);
	if(meta < 0)
	{
		r = meta;
		goto fail_meta;
	}
	if(read(meta, &header, sizeof(header)) != sizeof(header))
		goto fail_header;
	if(header.magic != MDTABLE_MAGIC || header.version != MDTABLE_VERSION)
		goto fail_header;
	switch(header.key_type)
	{
		case 1:
			ktype = dtype::UINT32;
			break;
		case 2:
			ktype = dtype::DOUBLE;
			break;
		case 3:
			ktype = dtype::STRING;
			break;
		default:
			goto fail_header;
	}
	for(uint32_t i = 0; i < header.ddt_count; i++)
	{
		char name[32];
		dtable * source;
		uint32_t ddt_value;
		r = read(meta, &ddt_value, sizeof(ddt_value));
		if(r != sizeof(ddt_value))
			goto fail_disks;
		sprintf(name, "md_data.%u", ddt_value);
		source = base->open(md_dfd, name, base_config);
		if(!source)
			goto fail_disks;
		disks.push_back(dtable_list_entry(source, ddt_value));
	}
	close(meta);
	
	journal = new journal_dtable;
	journal->init(ktype, header.journal_id, sys_journal);
	if(query_journal)
	{
		if(!sys_journal)
			sys_journal = sys_journal::get_global_journal();
		sys_journal->get_entries(journal);
	}
	
	/* force array scope to end */
	{
		size_t count = header.ddt_count;
		const dtable * array[count + 1];
		for(size_t i = 0; i < count; i++)
			array[count - i] = disks[i].first;
		array[0] = journal;
		overlay = new overlay_dtable;
		overlay->init(array, count + 1);
	}
	return 0;
	
fail_disks:
	for(size_t i = 0; i < disks.size(); i++)
		delete disks[i].first;
	disks.clear();
fail_header:
	close(meta);
fail_meta:
	close(md_dfd);
	md_dfd = -1;
	return r;
}

void managed_dtable::deinit()
{
	if(md_dfd < 0)
		return;
	delete overlay;
	delete journal;
	for(size_t i = 0; i < disks.size(); i++)
		delete disks[i].first;
	disks.clear();
	close(md_dfd);
	md_dfd = -1;
}

int managed_dtable::combine(size_t first, size_t last)
{
	sys_journal::listener_id old_id = sys_journal::NO_ID;
	overlay_dtable * shadow = NULL;
	overlay_dtable * source;
	bool reset_journal = false;
	patchgroup_id_t pid;
	dtable_list copy;
	dtable * result;
	char name[32];
	tx_fd fd;
	int r;
	
	if(last < first || last > disks.size())
		return -EINVAL;
	if(first)
	{
		const dtable * array[first];
		for(size_t i = 0; i < first; i++)
			array[first - i - 1] = disks[i].first;
		shadow = new overlay_dtable;
		shadow->init(array, first);
	}
	/* force array scope to end */
	{
		size_t count = last - first + 1;
		const dtable * array[count];
		if(last == disks.size())
		{
			array[0] = journal;
			reset_journal = true;
			last--;
		}
		if(last != (size_t) -1)
			for(size_t i = first; i <= last; i++)
				array[first + last - i + reset_journal] = disks[i].first;
		source = new overlay_dtable;
		source->init(array, count);
	}
	
	pid = patchgroup_create(0);
	if(pid <= 0)
		return (int) pid;
	r = patchgroup_release(pid);
	assert(r >= 0);
	r = patchgroup_engage(pid);
	assert(r >= 0);
	sprintf(name, "md_data.%u", header.ddt_next);
	r = base->create(md_dfd, name, base_config, source, shadow);
	{
		int r2 = patchgroup_disengage(pid);
		assert(r2 >= 0);
		/* make the transaction (which modifies the metadata below) depend on having written the new file */
		if(r >= 0)
		{
			r2 = tx_add_depend(pid);
			assert(r2 >= 0);
		}
		r2 = patchgroup_abandon(pid);
		assert(r2 >= 0);
	}
	delete source;
	if(shadow)
		delete shadow;
	if(r < 0)
		return r;
	
	/* now we've created the file, but we can still delete it easily if something fails */
	
	result = base->open(md_dfd, name, base_config);
	if(!result)
	{
		unlinkat(md_dfd, name, 0);
		return -1;
	}
	for(size_t i = 0; i < first; i++)
		copy.push_back(disks[i]);
	copy.push_back(dtable_list_entry(result, header.ddt_next));
	for(size_t i = last + 1; i < disks.size(); i++)
		copy.push_back(disks[i]);
	
	fd = tx_open(md_dfd, "md_meta", O_RDWR);
	if(fd < 0)
	{
		unlinkat(md_dfd, name, 0);
		return (int) fd;
	}
	
	if(reset_journal)
	{
		old_id = header.journal_id;
		header.journal_id = sys_journal::get_unique_id();
		assert(header.journal_id != sys_journal::NO_ID);
	}
	header.ddt_count = copy.size();
	header.ddt_next++;
	
	r = tx_write(fd, &header, sizeof(header), 0);
	if(r < 0)
	{
		header.ddt_next--;
		header.ddt_count = disks.size();
		if(reset_journal)
			header.journal_id = old_id;
		return r;
	}
	/* force array scope to end */
	{
		uint32_t array[header.ddt_count];
		for(uint32_t i = 0; i < header.ddt_count; i++)
			array[i] = copy[i].second;
		/* hmm... would sizeof(array) work here? */
		r = tx_write(fd, array, header.ddt_count * sizeof(array[0]), sizeof(header));
		if(r < 0)
		{
			/* umm... we are screwed? */
			abort();
			return r;
		}
	}
	/* TODO: really the file should be truncated, but it's not important */
	tx_close(fd);
	
	disks.swap(copy);
	/* unlink the source files in the transaction, which depends on writing the new data */
	if(last != (size_t) -1)
		for(size_t i = first; i <= last; i++)
		{
			sprintf(name, "md_data.%u", copy[i].second);
			tx_unlink(md_dfd, name);
		}
	/* force array scope to end */
	{
		const dtable * array[header.ddt_count + 1];
		for(uint32_t i = 0; i < header.ddt_count; i++)
			array[header.ddt_count - i] = disks[i].first;
		array[0] = journal;
		overlay->init(array, header.ddt_count + 1);
	}
	if(reset_journal)
		journal->reinit(header.journal_id, true);
	return 0;
}

int managed_dtable::maintain()
{
	time_t now = time(NULL);
	/* well, the journal probably doesn't really need maintenance, but just in case */
	int r = journal->maintain();
	for(size_t i = 0; i < disks.size(); i++)
		/* ditto on these theoretically read-only dtables */
		r |= disks[i].first->maintain();
	if(r < 0)
		return -1;
	if(header.digested + header.digest_interval <= now)
	{
		time_t old = header.digested;
		header.digested += header.digest_interval;
		/* if we'd still just do another digest, then reset the timestamp */
		if(header.digested + header.digest_interval <= now)
			header.digested = now;
		/* will rewrite header for us! */
		r = digest();
		if(r < 0)
		{
			header.digested = old;
			return r;
		}
	}
	if(header.combined + header.combine_interval <= now)
	{
		time_t old = header.combined;
		header.combined += header.combine_interval;
		/* if we'd still just do another combine, then reset the timestamp */
		if(header.combined + header.combine_interval <= now)
			header.combined = now;
		/* will rewrite header for us! */
		r = combine(header.combine_count);
		if(r < 0)
		{
			header.combined = old;
			return r;
		}
	}
	return 0;
}

int managed_dtable::create(int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	int r, sdfd;
	tx_fd fd;
	mdtable_header header;
	header.magic = MDTABLE_MAGIC;
	header.version = MDTABLE_VERSION;
	switch(key_type)
	{
		case dtype::UINT32:
			header.key_type = 1;
			break;
		case dtype::DOUBLE:
			header.key_type = 2;
			break;
		case dtype::STRING:
			header.key_type = 3;
			break;
		default:
			return -EINVAL;
	}
	/* default count: combine 5 dtables */
	if(!config.get("combine_count", &r, 5) || r < 2)
		return -EINVAL;
	header.combine_count = r;
	header.journal_id = sys_journal::get_unique_id();
	if(header.journal_id == sys_journal::NO_ID)
		return -EBUSY;
	header.ddt_count = 0;
	header.ddt_next = 0;
	/* default interval: 5 minutes (300 seconds) */
	if(!config.get("digest_interval", &r, 300) || r < 1)
		return -EINVAL;
	header.digest_interval = r;
	header.digested = time(NULL);
	/* default interval: 20 minutes (1200 seconds) */
	if(!config.get("combine_interval", &r, 1200) || r < 1)
		return -EINVAL;
	header.combine_interval = r;
	header.combined = header.digested;
	
	r = mkdirat(dfd, name, 0755);
	if(r < 0)
		return r;
	sdfd = openat(dfd, name, 0);
	if(sdfd < 0)
	{
		unlinkat(dfd, name, AT_REMOVEDIR);
		return sdfd;
	}
	fd = tx_open(sdfd, "md_meta", O_WRONLY | O_CREAT, 0644);
	if(fd < 0)
	{
		close(sdfd);
		unlinkat(dfd, name, AT_REMOVEDIR);
		return (int) fd;
	}
	r = tx_write(fd, &header, sizeof(header), 0);
	tx_close(fd);
	if(r < 0)
		unlinkat(sdfd, "md_meta", 0);
	close(sdfd);
	if(r < 0)
		unlinkat(dfd, name, AT_REMOVEDIR);
	return r;
}

DEFINE_RW_FACTORY(managed_dtable);
