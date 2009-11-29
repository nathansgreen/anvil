/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "temp_journal_dtable.h"

dtable::iter * temp_journal_dtable::iterator(ATX_DEF) const
{
	if(temporary)
	{
		/* Oh well. We've been asked for an iterator, so we'll have
		 * to go ahead and degrade to a normal journal_dtable. */
		temp_journal_dtable * mutable_this = const_cast<temp_journal_dtable *>(this);
		if(mutable_this->degrade() < 0)
			return NULL;
	}
	return journal_dtable::iterator(atx);
}

int temp_journal_dtable::insert(const dtype & key, const blob & blob, bool append, ATX_DEF)
{
	int r;
	if(!temporary)
		return journal_dtable::insert(key, blob, append, atx);
	if(key.type != ktype || (ktype == dtype::BLOB && !key.blb.exists()))
		return -EINVAL;
	r = log(key, blob, append);
	if(r < 0)
		return r;
	jdt_hash[key] = blob;
	return 0;
}

int temp_journal_dtable::init(dtype::ctype key_type, sys_journal::listener_id lid, sys_journal * sysj)
{
	if(lid == sys_journal::NO_ID)
		return -EINVAL;
	if(initialized)
		deinit();
	temporary = true;
	return journal_dtable::init(key_type, lid, sysj);
}

int temp_journal_dtable::reinit(sys_journal::listener_id lid)
{
	if(!initialized)
		return -EBUSY;
	if(lid == sys_journal::NO_ID)
		return -EINVAL;
	temporary = true;
	return journal_dtable::reinit(lid);
}

int temp_journal_dtable::accept(const dtype & key, const blob & value, bool append)
{
	if(!temporary)
		return journal_dtable::accept(key, value, append);
	jdt_hash[key] = value;
	return 0;
}

int temp_journal_dtable::degrade()
{
	journal_dtable_hash::iterator it;
	for(it = jdt_hash.begin(); it != jdt_hash.end(); ++it)
		jdt_map[it->first] = &it->second;
	temporary = false;
	return 0;
}

temp_journal_dtable * temp_journal_dtable::temp_journal_dtable_warehouse::create(sys_journal::listener_id lid, const void * entry, size_t length, sys_journal * journal) const
{
	dtype::ctype key_type;
	if(!entry_key_type(entry, length, &key_type))
		return NULL;
	temp_journal_dtable * jdt = new temp_journal_dtable;
	if(jdt->init(key_type, lid, journal) < 0)
	{
		delete jdt;
		jdt = NULL;
	}
	return jdt;
}

temp_journal_dtable * temp_journal_dtable::temp_journal_dtable_warehouse::create(sys_journal::listener_id lid, dtype::ctype key_type, sys_journal * journal) const
{
	temp_journal_dtable * jdt = new temp_journal_dtable;
	if(jdt->init(key_type, lid, journal) < 0)
	{
		delete jdt;
		jdt = NULL;
	}
	return jdt;
}

temp_journal_dtable::temp_journal_dtable_warehouse temp_journal_dtable::warehouse;
