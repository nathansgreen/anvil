/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "rwatx_dtable.h"

dtable::iter * rwatx_dtable::iterator(ATX_DEF) const
{
	if(atx == NO_ABORTABLE_TX)
		/* use the underlying iterator directly; returns base->iterator() */
		return iterator_chain_usage(&chain, base, atx);
	dtable::iter * bit = base->iterator();
	if(!bit)
		return NULL;
	iter * it = new iter(bit, this, atx);
	if(!it)
		delete bit;
	return it;
}

bool rwatx_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
		/* probably best not to report conflicts when reading */
		note_read(key, atx);
	return base->present(key, found, atx);
}

blob rwatx_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
		/* probably best not to report conflicts when reading */
		note_read(key, atx);
	return base->lookup(key, found, atx);
}

int rwatx_dtable::insert(const dtype & key, const blob & blob, bool append, ATX_DEF)
{
	if(atx != NO_ABORTABLE_TX)
		if(!note_write(key, atx))
			return -EBUSY;
	return base->insert(key, blob, append, atx);
}

int rwatx_dtable::remove(const dtype & key, ATX_DEF)
{
	if(atx != NO_ABORTABLE_TX)
		if(!note_write(key, atx))
			return -EBUSY;
	return base->remove(key, atx);
}

bool rwatx_dtable::note_read(const dtype & key, ATX_DEF) const
{
	std::pair<key_set::iterator, bool> undo_info;
	atx_status_map::const_iterator it = rwatx.find(atx);
	if(it == rwatx.end() || it->second.aborted)
		return false;
	if(it->second.writes.find(key) != it->second.writes.end())
		/* we already have a write lock, no need to read lock */
		return true;
	undo_info = it->second.reads.insert(key);
	if(undo_info.second)
	{
		/* this read is new to this transaction, update the global keys map */
		key_status_map::value_type pair(key, key_status());
		std::pair<key_status_map::iterator, bool> result = keys.insert(pair);
		if(!result.second && result.first->second.write_tagged())
		{
			/* some other transaction has a write lock, conflict */
			it->second.reads.erase(undo_info.first);
			it->second.aborted = true;
			return false;
		}
		bool ok = result.first->second.read_tag();
		assert(ok);
	}
	return true;
}

bool rwatx_dtable::note_write(const dtype & key, ATX_DEF)
{
	std::pair<key_set::iterator, bool> undo_info;
	atx_status_map::iterator it = rwatx.find(atx);
	if(it == rwatx.end() || it->second.aborted)
		return false;
	undo_info = it->second.writes.insert(key);
	if(undo_info.second)
	{
		/* this write is new to this transaction, update the global keys map */
		key_set::iterator read = it->second.reads.find(key);
		if(read != it->second.reads.end())
		{
			/* this write is an upgrade from a previous read */
			key_status_map::iterator result = keys.find(key);
			if(!result->second.write_upgrade())
			{
				/* some other transaction has a read lock, conflict */
				it->second.writes.erase(undo_info.first);
				it->second.aborted = true;
				return false;
			}
			it->second.reads.erase(read);
		}
		else
		{
			/* this write is completely new to this transaction */
			key_status_map::value_type pair(key, key_status());
			std::pair<key_status_map::iterator, bool> result = keys.insert(pair);
			if(!result.second)
			{
				/* some other transaction has a read or write lock, conflict */
				it->second.writes.erase(undo_info.first);
				it->second.aborted = true;
				return false;
			}
			bool ok = result.first->second.write_tag();
			assert(ok);
		}
	}
	return true;
}

abortable_tx rwatx_dtable::create_tx()
{
	abortable_tx atx = base->create_tx();
	if(atx != NO_ABORTABLE_TX)
	{
		atx_status_map::value_type pair(atx, atx_status(blob_cmp));
		bool ok = rwatx.insert(pair).second;
		assert(ok);
	}
	return atx;
}

int rwatx_dtable::check_tx(ATX_DEF) const
{
	atx_status_map::const_iterator it = rwatx.find(atx);
	if(it == rwatx.end())
		return -ENOENT;
	if(it->second.aborted)
		return -EBUSY;
	return base->check_tx(atx);
}

int rwatx_dtable::commit_tx(ATX_DEF)
{
	int r;
	atx_status_map::iterator it = rwatx.find(atx);
	if(it == rwatx.end())
		return -ENOENT;
	if(it->second.aborted)
		return -EBUSY;
	r = base->commit_tx(atx);
	if(r >= 0)
		remove_tx(it);
	return r;
}

void rwatx_dtable::abort_tx(ATX_DEF)
{
	atx_status_map::iterator it = rwatx.find(atx);
	if(it != rwatx.end())
		remove_tx(it);
	base->abort_tx(atx);
}

void rwatx_dtable::remove_tx(const atx_status_map::iterator & it)
{
	key_set::iterator kit;
	/* remove all the read keys from the global map */
	for(kit = it->second.reads.begin(); kit != it->second.reads.end(); ++kit)
	{
		key_status_map::iterator ksit = keys.find(*kit);
		if(!ksit->second.read_untag())
			/* clear the read lock if we were the last reader */
			keys.erase(ksit);
	}
	/* remove all the written keys from the global map */
	for(kit = it->second.writes.begin(); kit != it->second.writes.end(); ++kit)
	{
		key_status_map::iterator ksit = keys.find(*kit);
		ksit->second.write_untag();
		/* clear the write lock; by definition it was exclusive */
		keys.erase(ksit);
	}
	rwatx.erase(it);
}

int rwatx_dtable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	const dtable_factory * factory;
	params base_config;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	base = factory->open(dfd, file, base_config, sysj);
	if(!base)
		return -1;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	return 0;
}

void rwatx_dtable::deinit()
{
	if(base)
	{
		base->destroy();
		base = NULL;
		dtable::deinit();
	}
}

DEFINE_WRAP_FACTORY(rwatx_dtable);
