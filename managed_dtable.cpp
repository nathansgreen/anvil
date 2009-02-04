/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include "openat.h"
#include "transaction.h"

#include "util.h"
#include "managed_dtable.h"

int managed_dtable::init(int dfd, const char * name, const params & config, sys_journal * sys_journal)
{
	istr fast_config = "fastbase_config";
	tx_fd meta;
	off_t meta_off;
	int r = -1, size;
	if(md_dfd >= 0)
		deinit();
	base = dtable_factory::lookup(config, "base");
	if(!base)
		return -EINVAL;
	fastbase = dtable_factory::lookup(config, "fastbase", "base");
	if(!fastbase)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.contains(fast_config))
		fast_config = "base_config";
	if(!config.get(fast_config, &fastbase_config, params()))
		return -EINVAL;
	/* NOTE: use of this feature causes iterators to potentially
	 * become invalid on every insert() or remove() */
	if(!config.get("digest_size", &size, 0))
		return -EINVAL;
	digest_size = size;
	if(!config.get("digest_on_close", &digest_on_close, false))
		return -EINVAL;
	if(!config.get("close_digest_fastbase", &close_digest_fastbase, true))
		return -EINVAL;
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
		return md_dfd;
	meta = tx_open(md_dfd, "md_meta", O_RDONLY);
	if(meta < 0)
	{
		r = meta;
		goto fail_meta;
	}
	
	if(tx_read(meta, &header, sizeof(header), 0) != sizeof(header))
		goto fail_header;
	meta_off = sizeof(header);
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
		case 4:
			ktype = dtype::BLOB;
			break;
		default:
			goto fail_header;
	}
	
	for(uint32_t i = 0; i < header.ddt_count; i++)
	{
		char name[32];
		dtable * source;
		mdtable_entry ddt;
		r = tx_read(meta, &ddt, sizeof(ddt), meta_off);
		if(r != sizeof(ddt))
			goto fail_disks;
		meta_off += sizeof(ddt);
		sprintf(name, "md_data.%u", ddt.ddt_number);
		if(ddt.is_fastbase)
			source = fastbase->open(md_dfd, name, fastbase_config);
		else
			source = base->open(md_dfd, name, base_config);
		if(!source)
			goto fail_disks;
		disks.push_back(dtable_list_entry(source, ddt));
	}
	
	journal = new journal_dtable;
	journal->init(ktype, header.journal_id, sys_journal);
	if(!sys_journal)
		sys_journal = sys_journal::get_global_journal();
	r = sys_journal->get_entries(journal);
	cmp_name = journal->get_cmp_name();
	if(r == -EBUSY && cmp_name)
		delayed_query = sys_journal;
	else if(r >= 0)
		delayed_query = NULL;
	else
		goto fail_query;
	/* if the journal did not provide it, try to get the comparator name
	 * from the first disk dtable - journal_dtable doesn't save the name if
	 * it's empty, since you could in principle change it at that point */
	if(!cmp_name && disks.size())
		cmp_name = disks[0].disk->get_cmp_name();
	
	/* no more failure possible */
	tx_close(meta);
	
	/* force array scope to end */
	{
		size_t count = header.ddt_count;
		const dtable * array[count + 1];
		for(size_t i = 0; i < count; i++)
			array[count - i] = disks[i].disk;
		array[0] = journal;
		overlay = new overlay_dtable;
		overlay->init(array, count + 1);
	}
	return 0;
	
fail_query:
	delete journal;
fail_disks:
	for(size_t i = 0; i < disks.size(); i++)
		delete disks[i].disk;
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
	if(digest_on_close)
	{
		int r = digest(close_digest_fastbase);
		if(r < 0)
		{
			/* failing silently is OK here; do we need to warn? */
			fprintf(stderr, "%s: digest(%s) failed (%d; %s)\n", __PRETTY_FUNCTION__, close_digest_fastbase ? "true" : "false", r, strerror(errno));
		}
	}
	delete overlay;
	delete journal;
	for(size_t i = 0; i < disks.size(); i++)
		delete disks[i].disk;
	disks.clear();
	close(md_dfd);
	md_dfd = -1;
	dtable::deinit();
}

int managed_dtable::set_blob_cmp(const blob_comparator * cmp)
{
	int value;
	const char * match;
	if(md_dfd < 0)
		return -EBUSY;
	/* first check the journal's required comparator name */
	match = journal->get_cmp_name();
	if(match && strcmp(match, cmp->name))
		return -EINVAL;
	/* then try to set our own comparator */
	value = dtable::set_blob_cmp(cmp);
	if(value < 0)
		return value;
	/* if we get here, everything else should work fine */
	value = overlay->set_blob_cmp(cmp);
	assert(value >= 0);
	value = journal->set_blob_cmp(cmp);
	assert(value >= 0);
	for(size_t i = 0; i < disks.size(); i++)
	{
		value = disks[i].disk->set_blob_cmp(cmp);
		assert(value >= 0);
	}
	if(delayed_query)
	{
		value = delayed_query->get_entries(journal);
		/* not sure how that could fail at this point,
		 * but if so don't clear delayed_query */
		if(value >= 0)
			delayed_query = NULL;
	}
	return value;
}

int managed_dtable::combine(size_t first, size_t last, bool use_fastbase)
{
	sys_journal::listener_id old_id = sys_journal::NO_ID;
	overlay_dtable * shadow = NULL;
	overlay_dtable * source;
	bool reset_journal = false;
	dtable_list copy;
	dtable * result;
	char name[32];
	tx_fd fd;
	int r;
	
	/* can't do combining if we don't have the requisite comparator */
	if(cmp_name && !blob_cmp)
		return -EBUSY;
	if(last < first || last > disks.size())
		return -EINVAL;
	if(first)
	{
		const dtable * array[first];
		for(size_t i = 0; i < first; i++)
			array[first - i - 1] = disks[i].disk;
		shadow = new overlay_dtable;
		shadow->init(array, first);
		if(blob_cmp)
			shadow->set_blob_cmp(blob_cmp);
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
				array[last - i + reset_journal] = disks[i].disk;
		source = new overlay_dtable;
		source->init(array, count);
		if(blob_cmp)
			source->set_blob_cmp(blob_cmp);
	}
	
	/* make the transaction (which modifies the metadata below) depend on having written the new file */
	r = tx_start_external();
	if(r < 0)
		return r;
	sprintf(name, "md_data.%u", header.ddt_next);
	/* there might be one around from a previous failed combine */
	util::rm_r(md_dfd, name);
	if(use_fastbase)
		r = fastbase->create(md_dfd, name, fastbase_config, source, shadow);
	else
		r = base->create(md_dfd, name, base_config, source, shadow);
	tx_end_external(r >= 0);
	delete source;
	if(shadow)
		delete shadow;
	if(r < 0)
		return r;
	
	/* now we've created the file, but we can still delete it easily if something fails */
	
	if(use_fastbase)
		result = fastbase->open(md_dfd, name, fastbase_config);
	else
		result = base->open(md_dfd, name, base_config);
	if(!result)
	{
		unlinkat(md_dfd, name, 0);
		return -1;
	}
	if(blob_cmp)
		result->set_blob_cmp(blob_cmp);
	for(size_t i = 0; i < first; i++)
		copy.push_back(disks[i]);
	copy.push_back(dtable_list_entry(result, header.ddt_next, use_fastbase));
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
		mdtable_entry array[header.ddt_count];
		for(uint32_t i = 0; i < header.ddt_count; i++)
		{
			array[i].ddt_number = copy[i].ddt_number;
			array[i].is_fastbase = copy[i].is_fastbase;
		}
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
			delete copy[i].disk;
			sprintf(name, "md_data.%u", copy[i].ddt_number);
			/* recursive unlink */
			tx_unlink(md_dfd, name, 1);
		}
	/* force array scope to end */
	{
		const dtable * array[header.ddt_count + 1];
		for(uint32_t i = 0; i < header.ddt_count; i++)
			array[header.ddt_count - i] = disks[i].disk;
		array[0] = journal;
		overlay->init(array, header.ddt_count + 1);
		if(blob_cmp)
			overlay->set_blob_cmp(blob_cmp);
	}
	if(reset_journal)
	{
		journal->reinit(header.journal_id);
		if(blob_cmp)
			journal->set_blob_cmp(blob_cmp);
	}
	return 0;
}

int managed_dtable::maintain()
{
	time_t now = time(NULL);
	/* well, the journal probably doesn't really need maintenance, but just in case */
	int r = journal->maintain();
	for(size_t i = 0; i < disks.size(); i++)
		/* ditto on these theoretically read-only dtables */
		r |= disks[i].disk->maintain();
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
		case dtype::BLOB:
			header.key_type = 4;
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
