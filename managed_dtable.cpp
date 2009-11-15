/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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

/* FIXME: we need to explicitly store the blob comparator name in the
 * managed_dtable; counting on subordinate dtables to store it is insufficient,
 * since we might have no disk dtables and an empty journal_dtable */

int managed_dtable::init(int dfd, const char * name, const params & config, sys_journal * sysj)
{
	istr fast_config = "fastbase_config";
	tx_fd meta;
	off_t meta_off;
	int r = -1, size;
	if(md_dfd >= 0)
		deinit();
	if(!sysj)
		sysj = sys_journal::get_global_journal();
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
	 * become stale on every insert() or remove() */
	if(!config.get("digest_size", &size, 0))
		return -EINVAL;
	digest_size = size;
	if(!config.get("digest_on_close", &digest_on_close, false))
		return -EINVAL;
	if(!config.get("close_digest_fastbase", &close_digest_fastbase, true))
		return -EINVAL;
	if(!config.get("autocombine", &autocombine, true))
		return -EINVAL;
	if(!config.get("bg_default", &bg_default, false))
		return -EINVAL;
	md_dfd = openat(dfd, name, O_RDONLY);
	if(md_dfd < 0)
		return md_dfd;
	meta = tx_open(md_dfd, "md_meta", 0);
	if(!meta)
		goto fail_meta;
	
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
		mdtable_entry ddt;
		r = tx_read(meta, &ddt, sizeof(ddt), meta_off);
		if(r != sizeof(ddt))
			goto fail_disks;
		meta_off += sizeof(ddt);
		if(ddt.type == MDTE_TYPE_JOURNAL)
		{
			sys_journal::listener_id jid = ddt.ddt_number;
			journal_dtable * source = journal_dtable::obtain(jid, ktype, sysj);
			if(!cmp_name)
				cmp_name = source->get_cmp_name();
			disks.push_back(dtable_list_entry(journal, jid));
		}
		else
		{
			char name[32];
			dtable * source;
			sprintf(name, "md_data.%u", ddt.ddt_number);
			if(ddt.type == MDTE_TYPE_FASTBASE)
				source = fastbase->open(md_dfd, name, fastbase_config);
			else
				source = base->open(md_dfd, name, base_config);
			if(!source)
				goto fail_disks;
			disks.push_back(dtable_list_entry(source, ddt));
		}
	}
	
	journal = journal_dtable::obtain(header.journal_id, ktype, sysj);
	if(!cmp_name)
	{
		cmp_name = journal->get_cmp_name();
		/* if the journal did not provide it, try to get the comparator name
		 * from the first disk dtable - journal_dtable doesn't save the name if
		 * it's empty, since you could in principle change it at that point */
		if(!cmp_name && disks.size())
			cmp_name = disks[0].disk->get_cmp_name();
	}
	
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
	
	digest_thread.start();
	
	return 0;
	
fail_disks:
	for(size_t i = 0; i < disks.size(); i++)
		disks[i].disk->destroy();
	disks.clear();
fail_header:
	tx_close(meta);
fail_meta:
	close(md_dfd);
	md_dfd = -1;
	return (r < 0) ? r : -1;
}

void managed_dtable::deinit()
{
	if(md_dfd < 0)
		return;
	if(bg_digesting)
		background_join();
	assert(!bg_digesting);
	digest_thread.request_stop();
	/* send a STOP message to the queue */
	digest_queue.send(digest_msg());
	digest_thread.wait_for_stop();
	if(!doomed_dtables.empty())
	{
		/* FIXME: handle doomed dtables */
	}
	/* no sense digesting on close if there's nothing to digest */
	if(digest_on_close && journal->size())
	{
		int r = digest(close_digest_fastbase);
		if(r < 0)
		{
			/* failing silently is OK here; do we need to warn? */
			fprintf(stderr, "%s: digest(%s) failed (%d; %s)\n", __PRETTY_FUNCTION__, close_digest_fastbase ? "true" : "false", r, strerror(errno));
		}
	}
	delete overlay;
	journal->destroy();
	for(size_t i = 0; i < disks.size(); i++)
		disks[i].disk->destroy();
	disks.clear();
	close(md_dfd);
	md_dfd = -1;
	dtable::deinit();
}

dtable::iter * managed_dtable::iterator(ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::const_iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
			/* bad abortable transaction ID */
			return NULL;
		return iterator_chain_usage(&chain, it->second.overlay);
	}
	/* returns overlay->iterator() */
	return iterator_chain_usage(&chain, overlay);
}

bool managed_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::const_iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
		{
			/* bad abortable transaction ID */
			*found = false;
			return false;
		}
		return it->second.overlay->present(key, found);
	}
	return overlay->present(key, found);
}

blob managed_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::const_iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
		{
			/* bad abortable transaction ID */
			*found = false;
			return blob();
		}
		return it->second.overlay->lookup(key, found);
	}
	return overlay->lookup(key, found);
}

int managed_dtable::insert(const dtype & key, const blob & blob, bool append, ATX_DEF)
{
	int r;
	if(!blob.exists() && !contains(key, atx))
		return 0;
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
			/* bad abortable transaction ID */
			return -EINVAL;
		return it->second.journal->insert(key, blob, append);
	}
	r = journal->insert(key, blob, append);
	if(r >= 0 && digest_size && journal->size() >= digest_size)
		r = digest();
	return r;
}

int managed_dtable::remove(const dtype & key, ATX_DEF)
{
	int r;
	if(!find(key, atx).exists())
		return 0;
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
			/* bad abortable transaction ID */
			return -EINVAL;
		return it->second.journal->remove(key);
	}
	r = journal->remove(key);
	if(r >= 0 && digest_size && journal->size() >= digest_size)
		r = digest();
	return r;
}

abortable_tx managed_dtable::create_tx()
{
	int r;
	atx_state * state;
	sys_journal::listener_id lid;
	sys_journal * sysj;
	abortable_tx atx;
	
	if(cmp_name && !blob_cmp)
		return NO_ABORTABLE_TX;
	
	sysj = sys_journal::get_global_journal();
	atx = create_tx_id();
	assert(atx != NO_ABORTABLE_TX);
	
	state = &open_atx_map[atx];
	
	lid = sys_journal::get_unique_id(true);
	assert(lid != sys_journal::NO_ID);
	state->journal = journal_dtable::obtain(lid, ktype, sysj);
	assert(state->journal);
	if(blob_cmp)
		state->journal->set_blob_cmp(blob_cmp);
	
	state->overlay = new overlay_dtable;
	assert(state->overlay);
	r = state->overlay->init(state->journal, overlay, NULL);
	assert(r >= 0);
	if(blob_cmp)
		state->overlay->set_blob_cmp(blob_cmp);
	
	return atx;
}

int managed_dtable::commit_tx(ATX_DEF)
{
	return commit_abort_tx(atx, true);
}

void managed_dtable::abort_tx(ATX_DEF)
{
	commit_abort_tx(atx, false);
}

int managed_dtable::commit_abort_tx(ATX_DEF, bool commit)
{
	atx_map::iterator it = open_atx_map.find(atx);
	if(it == open_atx_map.end())
		/* bad abortable transaction ID */
		return -EINVAL;
	if(commit)
		it->second.journal->rollover(journal);
	if(it->second.overlay->in_use())
	{
		doomed_dtable * doomed = new doomed_dtable(this, it->second.overlay);
		doomed_dtables.insert(doomed);
	}
	else
		it->second.overlay->destroy();
	if(it->second.journal->in_use())
	{
		doomed_dtable * doomed = new doomed_dtable(this, it->second.journal);
		doomed_dtables.insert(doomed);
	}
	else
		it->second.journal->discard();
	open_atx_map.erase(it);
	return 0;
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
	return value;
}

/* external version */
int managed_dtable::combine(size_t first, size_t last, bool use_fastbase, bool background)
{
	if(bg_digesting)
		return -EBUSY;
	int r = 0;
	if(background)
	{
		digest_msg msg;
		msg.init_combine(first, last, use_fastbase);
		digest_queue.send(msg);
		bg_digesting = true;
	}
	else
	{
		fg_token token;
		r = combine(first, last, use_fastbase, &token);
	}
	return r;
}

template <class T>
int managed_dtable::combine(size_t first, size_t last, bool use_fastbase, T * token)
{
	size_t holds;
	scopetoken<T> scope(token);
	combiner worker(this, first, last, use_fastbase);
	int r = worker.prepare(token);
	if(r < 0)
		return r;
	holds = scope.full_release();
	r = worker.run();
	scope.full_acquire(holds);
	if(r < 0)
		/* will call worker.fail() */
		return r;
	return worker.finish();
}

/* set up the source and shadow overlay dtables */
int managed_dtable::combiner::prepare(bool shift_journal)
{
	/* can't do combining if we don't have the requisite comparator */
	if(mdt->cmp_name && !mdt->blob_cmp)
		return -EBUSY;
	if(last < first || last > mdt->disks.size())
		return -EINVAL;
	
	if(shift_journal && last == mdt->disks.size())
	{
		int r;
		sys_journal * sj = mdt->journal->get_journal();
		
		/* We're about to do a digest that uses the current journal dtable, and it
		 * will be done in the background. We can't allow the journal dtable to be
		 * writable any more, or the digest will not have a consistent view of the
		 * data it contains. We create a new one instead, and shift the old one
		 * (hence the name shift_journal) into a "disk" dtable position. */
		/* FIXME: in the event of a later digest error, this will cause there to
		 * be an extra dtable that the digest schedule won't take into account,
		 * hurting future performance */
		mdt->disks.push_back(dtable_list_entry(mdt->journal, mdt->header.journal_id));
		mdt->header.journal_id = sys_journal::get_unique_id();
		assert(mdt->header.journal_id != sys_journal::NO_ID);
		mdt->header.ddt_count++;
		
		r = write_meta(mdt->disks);
		if(r < 0)
		{
			dtable_list_entry orig = mdt->disks.back();
			mdt->header.journal_id = orig.jid;
			mdt->journal = orig.journal;
			mdt->header.ddt_count--;
			mdt->disks.pop_back();
			return r;
		}
		
		mdt->journal = journal_dtable::obtain(mdt->header.journal_id, mdt->ktype, sj);
		if(mdt->blob_cmp)
			mdt->journal->set_blob_cmp(mdt->blob_cmp);
		
		/* force array scope to end */
		{
			const dtable * array[mdt->header.ddt_count + 1];
			for(uint32_t i = 0; i < mdt->header.ddt_count; i++)
				array[mdt->header.ddt_count - i] = mdt->disks[i].disk;
			array[0] = mdt->journal;
			if(mdt->overlay->in_use())
			{
				doomed_dtable * doomed = new doomed_dtable(mdt, mdt->overlay);
				mdt->doomed_dtables.insert(doomed);
				mdt->overlay = new overlay_dtable;
			}
			mdt->overlay->init(array, mdt->header.ddt_count + 1);
			if(mdt->blob_cmp)
				mdt->overlay->set_blob_cmp(mdt->blob_cmp);
		}
		
		/* now it should look like we're not digesting the journal after all */
		assert(last < mdt->disks.size());
	}
	
	if(first)
	{
		const dtable * array[first];
		for(size_t i = 0; i < first; i++)
			array[first - i - 1] = mdt->disks[i].disk;
		shadow = new overlay_dtable;
		shadow->init(array, first);
		if(mdt->blob_cmp)
			shadow->set_blob_cmp(mdt->blob_cmp);
	}
	
	/* force array scope to end */
	{
		size_t count = last - first + 1;
		const dtable * array[count];
		if(last == mdt->disks.size())
		{
			array[0] = mdt->journal;
			reset_journal = true;
			last--;
		}
		if(last != (size_t) -1)
			for(size_t i = first; i <= last; i++)
				array[last - i + reset_journal] = mdt->disks[i].disk;
		source = new overlay_dtable;
		source->init(array, count);
		if(mdt->blob_cmp)
			source->set_blob_cmp(mdt->blob_cmp);
	}
	
	sprintf(name, "md_data.%u", mdt->header.ddt_next);
	
	return 0;
}

/* create the combined dtable - this can optionally run in a background thread */
int managed_dtable::combiner::run() const
{
	int r;
	
	/* make the current transaction depend on having written the new file */
	r = tx_start_external();
	if(r < 0)
		return r;
	
	/* there might be one around from a previous failed combine */
	util::rm_r(mdt->md_dfd, name);
	if(use_fastbase)
		r = mdt->fastbase->create(mdt->md_dfd, name, mdt->fastbase_config, source, shadow);
	else
		r = mdt->base->create(mdt->md_dfd, name, mdt->base_config, source, shadow);
	
	tx_end_external(r >= 0);
	
	/* now we've created the file, but we can still delete it easily if something fails */
	return r;
}

/* update the metadata to refer to the new dtable, and remove the now-obsolete ones */
int managed_dtable::combiner::finish()
{
	sys_journal::listener_id old_id = sys_journal::NO_ID;
	dtable_list copy;
	dtable * result;
	int r;
	
	delete source;
	source = NULL;
	if(shadow)
		delete shadow;
	
	if(use_fastbase)
		result = mdt->fastbase->open(mdt->md_dfd, name, mdt->fastbase_config);
	else
		result = mdt->base->open(mdt->md_dfd, name, mdt->base_config);
	if(!result)
	{
		fail();
		return -1;
	}
	if(mdt->blob_cmp)
		result->set_blob_cmp(mdt->blob_cmp);
	for(size_t i = 0; i < first; i++)
		copy.push_back(mdt->disks[i]);
	copy.push_back(dtable_list_entry(result, mdt->header.ddt_next, use_fastbase));
	for(size_t i = last + 1; i < mdt->disks.size(); i++)
		copy.push_back(mdt->disks[i]);
	
	if(reset_journal)
	{
		old_id = mdt->header.journal_id;
		mdt->header.journal_id = sys_journal::get_unique_id();
		assert(mdt->header.journal_id != sys_journal::NO_ID);
	}
	mdt->header.ddt_count = copy.size();
	mdt->header.ddt_next++;
	
	r = write_meta(copy);
	if(r < 0)
	{
		mdt->header.ddt_next--;
		mdt->header.ddt_count = mdt->disks.size();
		if(reset_journal)
			mdt->header.journal_id = old_id;
		result->destroy();
		fail();
		return r;
	}
	
	mdt->disks.swap(copy);
	
	/* unlink the source files in the transaction, which depends on writing the new data */
	if(last != (size_t) -1)
		for(size_t i = first; i <= last; i++)
		{
			if(copy[i].disk->in_use())
			{
				doomed_dtable * doomed;
				if(copy[i].type == MDTE_TYPE_JOURNAL)
					doomed = new doomed_dtable(mdt, copy[i].journal);
				else
					doomed = new doomed_dtable(mdt, copy[i].disk, copy[i].ddt_number);
				mdt->doomed_dtables.insert(doomed);
			}
			else if(copy[i].type == MDTE_TYPE_JOURNAL)
				/* also destroys it */
				copy[i].journal->discard();
			else
			{
				copy[i].disk->destroy();
				sprintf(name, "md_data.%u", copy[i].ddt_number);
				/* recursive unlink */
				tx_unlink(mdt->md_dfd, name, 1);
			}
		}
	
	/* force array scope to end */
	{
		const dtable * array[mdt->header.ddt_count + 1];
		for(uint32_t i = 0; i < mdt->header.ddt_count; i++)
			array[mdt->header.ddt_count - i] = mdt->disks[i].disk;
		array[0] = mdt->journal;
		if(mdt->overlay->in_use())
		{
			doomed_dtable * doomed = new doomed_dtable(mdt, mdt->overlay);
			mdt->doomed_dtables.insert(doomed);
			mdt->overlay = new overlay_dtable;
		}
		mdt->overlay->init(array, mdt->header.ddt_count + 1);
		if(mdt->blob_cmp)
			mdt->overlay->set_blob_cmp(mdt->blob_cmp);
	}
	
	if(reset_journal)
	{
		if(mdt->journal->in_use())
		{
			sys_journal * sj = mdt->journal->get_journal();
			doomed_dtable * doomed = new doomed_dtable(mdt, mdt->journal);
			/* FIXME: we can actually discard the sysj entries now, as long as we keep them in memory */
			mdt->doomed_dtables.insert(doomed);
			mdt->journal = journal_dtable::obtain(mdt->header.journal_id, mdt->ktype, sj);
		}
		else
			mdt->journal->reinit(mdt->header.journal_id);
		if(mdt->blob_cmp)
			mdt->journal->set_blob_cmp(mdt->blob_cmp);
	}
	
	return 0;
}

/* actually write the md_meta file */
int managed_dtable::combiner::write_meta(const dtable_list & list) const
{
	tx_fd fd = tx_open(mdt->md_dfd, "md_meta", 0);
	if(!fd)
		return -1;
	
	/* TODO: really the file should be truncated, but it's not important */
	int r = tx_write(fd, &mdt->header, sizeof(mdt->header), 0);
	if(r < 0)
	{
		tx_close(fd);
		return r;
	}
	
	/* force array scope to end */
	{
		mdtable_entry array[mdt->header.ddt_count];
		for(uint32_t i = 0; i < mdt->header.ddt_count; i++)
		{
			if(list[i].type == MDTE_TYPE_JOURNAL)
			{
				array[i].ddt_number = (uint32_t) list[i].jid;
				array[i].type = MDTE_TYPE_JOURNAL;
			}
			else
			{
				array[i].ddt_number = list[i].ddt_number;
				array[i].type = list[i].type;
			}
		}
		/* hmm... would sizeof(array) work here? */
		r = tx_write(fd, array, mdt->header.ddt_count * sizeof(array[0]), sizeof(mdt->header));
		if(r < 0)
		{
			/* umm... we are screwed? */
			abort();
			tx_close(fd);
			return r;
		}
	}
	
	tx_close(fd);
	return 0;
}

void managed_dtable::combiner::fail()
{
	if(source)
	{
		delete source;
		source = NULL;
	}
	if(shadow)
		delete shadow;
	util::rm_r(mdt->md_dfd, name);
}

void managed_dtable::background_loan()
{
	if(bg_digesting)
	{
		reply_msg reply;
		if(digest_thread.wants_token())
			digest_thread.loan_token();
		if(reply_queue.try_receive(&reply))
			bg_digesting = false;
	}
}

int managed_dtable::background_join()
{
	if(!bg_digesting)
		return -EBUSY;
	reply_msg reply;
	while(!reply_queue.try_receive(&reply))
	{
		if(digest_thread.wants_token())
			digest_thread.loan_token();
		else
			usleep(50000); /* 1/20 sec */
	}
	bg_digesting = false;
	return reply.return_value;
}

void managed_dtable::digest_thread_main(bg_token * token)
{
	while(!digest_thread.stop_requested())
	{
		reply_msg reply;
		digest_msg message;
		digest_queue.receive(&message);
		switch(message.type)
		{
			case digest_msg::COMBINE:
				reply.return_value = combine(message.combine.first, message.combine.last, message.combine.use_fastbase, token);
				reply_queue.send(reply);
				break;
			case digest_msg::MAINTAIN:
				reply.return_value = maintain(message.maintain.force, token);
				reply_queue.send(reply);
				break;
			case digest_msg::STOP:
				/* fall out */ ;
		}
	}
}

void managed_dtable::doomed_dtable::invoke()
{
	switch(type)
	{
		case DISK:
		{
			char name[32];
			doomed.disk->destroy();
			sprintf(name, "md_data.%u", ddt_number);
			/* make sure we have a transaction */
			tx_start_r();
			/* recursive unlink */
			tx_unlink(mdt->md_dfd, name, 1);
			tx_end_r();
			break;
		}
		case JOURNAL:
			/* make sure we have a transaction */
			tx_start_r();
			/* also destroys it */
			doomed.journal->discard();
			tx_end_r();
			break;
		case OVERLAY:
			delete doomed.overlay;
			break;
	}
	mdt->doomed_dtables.erase(this);
	delete this;
}

template<class T>
int managed_dtable::maintain_autocombine(T * token)
{
	scopetoken<T> scope(token);
	size_t count = header.autocombine_digests;
	header.autocombine_combine_count++;
	count += ffs(header.autocombine_combine_count) - 1;
	if(count > disks.size())
		count = disks.size();
	if(count > 1)
	{
		int r = combine(disks.size() - count, disks.size(), false, token);
		if(r < 0)
		{
			header.autocombine_combine_count--;
			return r;
		}
	}
	return 0;
}

int managed_dtable::maintain(bool force, bool background)
{
	if(bg_digesting)
	{
		/* sync with the background thread and finish its work first */
		background_loan();
		if(bg_digesting)
			/* this is not an error */
			return 0;
	}
	int r = 0;
	if(background)
	{
		digest_msg msg;
		msg.init_maintain(force);
		digest_queue.send(msg);
		bg_digesting = true;
	}
	else
	{
		fg_token token;
		r = maintain(force, &token);
	}
	return r;
}

template<class T>
int managed_dtable::maintain(bool force, T * token)
{
	int r;
	time_t now = time(NULL);
	/* check if we even need to digest */
	if(!force && header.digested + header.digest_interval > now &&
	   (header.combined + header.combine_interval > now || autocombine))
		return 0;
	scopetoken<T> scope(token);
	/* get the time again in case it has changed significantly since acquiring the token */
	now = time(NULL);
	/* well, the journal probably doesn't really need maintenance, but just in case */
	r = journal->maintain(force);
	for(size_t i = 0; i < disks.size(); i++)
		/* ditto on these theoretically read-only dtables */
		r |= disks[i].disk->maintain(force);
	if(r < 0)
		return -1;
	if(force)
	{
		/* how long until the next digest? */
		time_t delay = header.digested + header.digest_interval - now;
		if(delay > 0)
		{
			/* push history backward by that amount */
			header.digested -= delay;
			header.combined -= delay;
		}
	}
	if(header.digested + header.digest_interval <= now)
	{
		time_t old = header.digested;
		header.digested += header.digest_interval;
		/* if we'd still just do another digest, then reset the timestamp */
		if(header.digested + header.digest_interval <= now)
			header.digested = now;
		/* don't bother if the journal is empty though */
		if(journal->size())
		{
			size_t size = disks.size();
			if(autocombine)
				header.autocombine_digest_count++;
			/* will rewrite header for us! */
			/* "digest()" */
			r = combine(size, size, true, token);
			if(r < 0)
			{
				header.autocombine_digest_count--;
				header.digested = old;
				return r;
			}
			if(autocombine && header.autocombine_digest_count == header.autocombine_digests)
			{
				header.autocombine_digest_count = 0;
				/* will rewrite header for us! */
				r = maintain_autocombine(token);
				if(r < 0)
				{
					header.autocombine_digest_count = header.autocombine_digests;
					return r;
				}
			}
		}
	}
	if(header.combined + header.combine_interval <= now && !autocombine)
	{
		time_t old = header.combined;
		header.combined += header.combine_interval;
		/* if we'd still just do another combine, then reset the timestamp */
		if(header.combined + header.combine_interval <= now)
			header.combined = now;
		/* don't bother if there aren't at least two dtables */
		if(disks.size() > 1)
		{
			size_t size = disks.size();
			size_t count = header.combine_count - 1;
			if(count > size)
				count = size;
			/* will rewrite header for us! */
			r = combine(size - count, size, false, token);
			if(r < 0)
			{
				header.combined = old;
				return r;
			}
		}
	}
	return 0;
}

int managed_dtable::create(int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	int r, md_dfd;
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
	/* default count: combine 6 dtables */
	if(!config.get("combine_count", &r, 6) || r < 2)
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
	if(!config.get("autocombine_digests", &r, 4))
		return -EINVAL;
	header.autocombine_digests = r;
	header.autocombine_digest_count = 0;
	header.autocombine_combine_count = 0;
	
	r = mkdirat(dfd, name, 0755);
	if(r < 0)
		return r;
	md_dfd = openat(dfd, name, O_RDONLY);
	if(md_dfd < 0)
	{
		unlinkat(dfd, name, AT_REMOVEDIR);
		return md_dfd;
	}
	fd = tx_open(md_dfd, "md_meta", 1);
	if(!fd)
	{
		close(md_dfd);
		unlinkat(dfd, name, AT_REMOVEDIR);
		return -1;
	}
	r = tx_write(fd, &header, sizeof(header), 0);
	tx_close(fd);
	if(r < 0)
		unlinkat(md_dfd, "md_meta", 0);
	close(md_dfd);
	if(r < 0)
		unlinkat(dfd, name, AT_REMOVEDIR);
	return r;
}

DEFINE_RW_FACTORY(managed_dtable);
