/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include "openat.h"

#include "util.h"
#include "keydiv_dtable.h"

keydiv_dtable::iter::iter(const keydiv_dtable * source, ATX_DEF)
	: iter_source<keydiv_dtable>(source), current_index(0)
{
	subs = new sub[source->sub.size()];
	for(size_t i = 0; i < source->sub.size(); i++)
	{
		subs[i].iter = source->sub[i]->iterator(atx);
		subs[i].at_first = true;
		subs[i].at_end = !subs[i].iter->valid();
	}
	/* find the first nonempty iterator */
	while(current_index < source->sub.size() && subs[current_index].at_end)
		current_index++;
}

keydiv_dtable::iter::~iter()
{
	delete[] subs;
}

bool keydiv_dtable::iter::valid() const
{
	return current_index < dt_source->sub.size();
}

bool keydiv_dtable::iter::next()
{
	if(current_index >= dt_source->sub.size())
		return false;
	if(subs[current_index].iter->next())
	{
		subs[current_index].at_first = false;
		return true;
	}
	subs[current_index].at_end = true;
	while(++current_index < dt_source->sub.size())
	{
		if(!subs[current_index].at_first)
		{
			subs[current_index].at_first = true;
			subs[current_index].at_end = !subs[current_index].iter->first();
		}
		if(!subs[current_index].at_end)
			return true;
	}
	return false;
}

bool keydiv_dtable::iter::prev()
{
	if(!current_index && subs[0].at_first)
		return false;
	if(current_index < dt_source->sub.size())
	{
		if(subs[current_index].iter->prev())
			return true;
		subs[current_index].at_first = true;
	}
	while(current_index)
	{
		bool empty;
		if(subs[--current_index].at_end)
			empty = !subs[current_index].iter->prev();
		else
			empty = !subs[current_index].iter->last();
		subs[current_index].at_first = empty;
		subs[current_index].at_end = empty;
		if(!empty)
			return true;
	}
	/* There is a special case we have to handle here: if subs[0].iter is
	 * empty, then we are currently pointing before the first element, which
	 * is not allowed. So we have to move back to the first element. */
	if(subs[0].at_end)
		next();
	return false;
}

bool keydiv_dtable::iter::first()
{
	for(size_t i = 0; i < dt_source->sub.size(); i++)
	{
		subs[i].at_first = true;
		subs[i].at_end = !subs[i].iter->first();
	}
	current_index = 0;
	/* find the first nonempty iterator */
	while(current_index < dt_source->sub.size() && subs[current_index].at_end)
		current_index++;
	return current_index < dt_source->sub.size();
}

bool keydiv_dtable::iter::last()
{
	current_index = dt_source->sub.size();
	return prev();
}

dtype keydiv_dtable::iter::key() const
{
	assert(current_index < dt_source->sub.size());
	return subs[current_index].iter->key();
}

bool keydiv_dtable::iter::seek(const dtype & key)
{
	size_t target_index = dt_source->key_index(key);
	bool found = subs[target_index].iter->seek(key);
	bool valid = found || subs[target_index].iter->valid();
	current_index = target_index;
	subs[current_index].at_first = false;
	subs[current_index].at_end = !valid;
	if(found)
		return true;
	if(!valid)
		next();
	return false;
}

bool keydiv_dtable::iter::seek(const dtype_test & test)
{
	size_t target_index = dt_source->key_index(test);
	bool found = subs[target_index].iter->seek(test);
	bool valid = found || subs[target_index].iter->valid();
	current_index = target_index;
	subs[current_index].at_first = false;
	subs[current_index].at_end = !valid;
	if(found)
		return true;
	if(!valid)
		next();
	return false;
}

metablob keydiv_dtable::iter::meta() const
{
	assert(current_index < dt_source->sub.size());
	return subs[current_index].iter->meta();
}

blob keydiv_dtable::iter::value() const
{
	assert(current_index < dt_source->sub.size());
	return subs[current_index].iter->value();
}

const dtable * keydiv_dtable::iter::source() const
{
	return dt_source;
}

dtable::iter * keydiv_dtable::iterator(ATX_DEF) const
{
	if(atx != NO_ABORTABLE_TX)
	{
		atx_map::const_iterator it = open_atx_map.find(atx);
		if(it == open_atx_map.end())
			/* bad abortable transaction ID */
			return NULL;
		if(it->second.populate(this) < 0)
			return NULL;
	}
	return new iter(this, atx);
}

bool keydiv_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	size_t index = key_index(key);
	assert(index < sub.size());
	if(atx != NO_ABORTABLE_TX)
		if(map_atx(&atx, index) < 0)
		{
			*found = false;
			return false;
		}
	return sub[index]->present(key, found, atx);
}

blob keydiv_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	size_t index = key_index(key);
	assert(index < sub.size());
	if(atx != NO_ABORTABLE_TX)
		if(map_atx(&atx, index) < 0)
		{
			*found = false;
			return blob();
		}
	return sub[index]->lookup(key, found, atx);
}

int keydiv_dtable::insert(const dtype & key, const blob & blob, bool append, ATX_DEF)
{
	size_t index = key_index(key);
	assert(index < sub.size());
	if(atx != NO_ABORTABLE_TX)
	{
		int r = map_atx(&atx, index);
		if(r < 0)
			return r;
	}
	return sub[index]->insert(key, blob, append, atx);
}

int keydiv_dtable::remove(const dtype & key, ATX_DEF)
{
	size_t index = key_index(key);
	assert(index < sub.size());
	if(atx != NO_ABORTABLE_TX)
	{
		int r = map_atx(&atx, index);
		if(r < 0)
			return r;
	}
	return sub[index]->remove(key, atx);
}

/* keydiv_dtable does not need to do anything itself to support abortable
 * transactions; however, it needs to pass these methods through to the
 * underlying dtables. Since there may be many underlying dtables, and a
 * given transaction may only touch a few, we create only local state at
 * first and do the create_tx() calls on demand later. We have to do them
 * all if an iterator is created within the transaction, however. */
abortable_tx keydiv_dtable::create_tx()
{
	int r;
	atx_state * state;
	abortable_tx atx;
	
	if(!support_atx)
		return -ENOSYS;
	
	atx = create_tx_id();
	assert(atx != NO_ABORTABLE_TX);
	
	state = &open_atx_map[atx];
	r = state->init(sub.size());
	if(r < 0)
	{
		open_atx_map.erase(atx);
		return NO_ABORTABLE_TX;
	}
	
	return 0;
}

int keydiv_dtable::commit_tx(ATX_DEF)
{
	int r;
	atx_map::iterator it = open_atx_map.find(atx);
	if(it == open_atx_map.end())
		/* bad abortable transaction ID */
		return -EINVAL;
	r = it->second.commit_tx(this);
	if(r < 0)
		return r;
	open_atx_map.erase(it);
	return 0;
}

void keydiv_dtable::abort_tx(ATX_DEF)
{
	atx_map::iterator it = open_atx_map.find(atx);
	if(it == open_atx_map.end())
		/* bad abortable transaction ID */
		return;
	it->second.commit_tx(this);
	open_atx_map.erase(it);
}

int keydiv_dtable::atx_state::init(size_t size)
{
	assert(!atx);
	atx = new abortable_tx[size];
	if(!atx)
		return -ENOMEM;
	for(size_t i = 0; i < size; i++)
		atx[i] = NO_ABORTABLE_TX;
	return 0;
}

int keydiv_dtable::atx_state::populate(const keydiv_dtable * kddt) const
{
	for(size_t i = 0; i < kddt->sub.size(); i++)
		if(atx[i] == NO_ABORTABLE_TX)
		{
			atx[i] = kddt->sub[i]->create_tx();
			if(atx[i] == NO_ABORTABLE_TX)
				return -1;
		}
	return 0;
}

abortable_tx keydiv_dtable::atx_state::get(size_t index, const keydiv_dtable * kddt) const
{
	if(atx[index] == NO_ABORTABLE_TX)
		atx[index] = kddt->sub[index]->create_tx();
	return atx[index];
}

int keydiv_dtable::atx_state::commit_tx(const keydiv_dtable * kddt)
{
	for(size_t i = 0; i < kddt->sub.size(); i++)
		if(atx[i] != NO_ABORTABLE_TX)
		{
			int r = kddt->sub[i]->commit_tx(atx[i]);
			if(r < 0)
				return r;
			atx[i] = NO_ABORTABLE_TX;
		}
	delete[] atx;
	atx = NULL;
	return 0;
}

void keydiv_dtable::atx_state::abort_tx(const keydiv_dtable * kddt)
{
	for(size_t i = 0; i < kddt->sub.size(); i++)
		if(atx[i] != NO_ABORTABLE_TX)
			kddt->sub[i]->abort_tx(atx[i]);
	delete[] atx;
	atx = NULL;
}

int keydiv_dtable::map_atx(abortable_tx * atx, size_t index) const
{
	atx_map::const_iterator it = open_atx_map.find(*atx);
	if(it == open_atx_map.end())
		/* bad abortable transaction ID */
		return -EINVAL;
	*atx = it->second.get(index, this);
	assert(*atx != NO_ABORTABLE_TX);
	return 0;
}

int keydiv_dtable::maintain(bool force)
{
	int r = 0;
	if(!sub.size())
		return -EBUSY;
	for(size_t i = 0; i < sub.size(); i++)
	{
		r = sub[i]->maintain(force);
		if(r < 0)
			break;
	}
	return r;
}

int keydiv_dtable::init(int dfd, const char * name, const params & config, sys_journal * sysj)
{
	abortable_tx atx;
	int r, kdd_dfd, meta;
	const dtable_factory * base;
	params base_config;
	if(sub.size() >= 0)
		deinit();
	base = dtable_factory::lookup(config, "base");
	if(!base)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	kdd_dfd = openat(dfd, name, O_RDONLY);
	if(kdd_dfd < 0)
		return kdd_dfd;
	meta = openat(kdd_dfd, "kdd_meta", O_RDONLY);
	if(meta < 0)
		goto fail_meta;
	
	if(pread(meta, &header, sizeof(header), 0) != sizeof(header))
	{
		close(meta);
		goto fail_meta;
	}
	close(meta);
	if(header.magic != KDDTABLE_MAGIC || header.version != KDDTABLE_VERSION)
		goto fail_meta;
	if(!header.dt_count)
		goto fail_meta;
	switch(header.key_type)
	{
		case 1:
			ktype = dtype::UINT32;
			r = load_dividers<int, uint32_t>(config, header.dt_count, &dividers);
			break;
		case 2:
			ktype = dtype::DOUBLE;
			r = load_dividers<float, double>(config, header.dt_count, &dividers);
			break;
		case 3:
			ktype = dtype::STRING;
			r = load_dividers<istr, istr>(config, header.dt_count, &dividers);
			break;
		case 4:
			ktype = dtype::BLOB;
			r = load_dividers<blob, blob>(config, header.dt_count, &dividers, true);
			break;
		default:
			goto fail_meta;
	}
	if(r < 0)
		goto fail_meta;
	
	for(uint32_t i = 0; i < header.dt_count; i++)
	{
		char name[32];
		dtable * source;
		sprintf(name, "kdd_data.%u", i);
		source = base->open(kdd_dfd, name, base_config, sysj);
		if(!source)
			goto fail_sub;
		sub.push_back(source);
	}
	
	if(sub[0]->get_cmp_name())
		cmp_name = sub[0]->get_cmp_name();
	
	/* check for abortable transaction support */
	atx = sub[0]->create_tx();
	if((support_atx = (atx != NO_ABORTABLE_TX)))
		sub[0]->abort_tx(atx);
	
	return 0;
	
fail_sub:
	for(size_t i = 0; i < sub.size(); i++)
		sub[i]->destroy();
fail_meta:
	sub.clear();
	dividers.clear();
	close(kdd_dfd);
	return -1;
}

void keydiv_dtable::deinit()
{
	if(!sub.size())
		return;
	for(size_t i = 0; i < sub.size(); i++)
		sub[i]->destroy();
	sub.clear();
	dtable::deinit();
}

int keydiv_dtable::set_blob_cmp(const blob_comparator * cmp)
{
	int value;
	const char * match;
	if(!sub.size())
		return -EBUSY;
	/* first check the required comparator name */
	match = sub[0]->get_cmp_name();
	if(match && strcmp(match, cmp->name))
		return -EINVAL;
	/* then try to set our own comparator */
	value = dtable::set_blob_cmp(cmp);
	if(value < 0)
		return value;
	/* if we get here, everything else should work fine */
	for(size_t i = 0; i < sub.size(); i++)
	{
		value = sub[i]->set_blob_cmp(cmp);
		assert(value >= 0);
	}
	return value;
}

template<class T, class C>
int keydiv_dtable::load_dividers(const params & config, size_t dt_count, divider_list * list, bool skip_check)
{
	std::vector<T> data;
	if(!config.get_seq("divider_", NULL, 0, true, &data))
		return -1;
	/* if there are n dtables, there should be n - 1 dividers */
	if(dt_count && data.size() != dt_count - 1)
		return -EINVAL;
	list->clear();
	for(size_t i = 0; i < data.size(); i++)
		list->push_back(dtype((C) data[i]));
	/* dividers should be in increasing order, but we can't check blobs
	 * since they might need a comparator that we don't have yet */
	if(!skip_check)
		for(size_t i = 1; i < list->size(); i++)
			if((*list)[i - 1].compare((*list)[i]) >= 0)
				return -EINVAL;
	return 0;
}

/* The dividers are inclusive up: that is, if we have a keydiv dtable with a
 * single divider X, then sub[0] will contain all keys up to but not including
 * X, and sub[1] will contain X and up. This is mostly an arbitrary choice. */
template<class T>
size_t keydiv_dtable::key_index(const T & test) const
{
	/* binary search */
	ssize_t min = 0, max = dividers.size() - 1;
	assert(ktype != dtype::BLOB || !cmp_name == !blob_cmp);
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		int c = test(dividers[mid]);
		if(c < 0)
			min = mid + 1;
		else if(c > 0)
			max = mid - 1;
		else
			return mid + 1; /* arbitrary choice here */
	}
	/* max and min crossed: either we checked [min] and found it to be too
	 * large, thus decrementing max, or we checked [max] and found it to be
	 * too small, thus incrementing min. Either way the correct answer is
	 * min (= max + 1), since divider n separates dtables n and n + 1. */
	return min;
}

int keydiv_dtable::create(int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	int r, kdd_dfd, meta;
	divider_list dividers;
	const dtable_factory * base;
	params base_config;
	
	kddtable_header header;
	header.magic = KDDTABLE_MAGIC;
	header.version = KDDTABLE_VERSION;
	switch(key_type)
	{
		case dtype::UINT32:
			header.key_type = 1;
			r = load_dividers<int, uint32_t>(config, 0, &dividers);
			break;
		case dtype::DOUBLE:
			header.key_type = 2;
			r = load_dividers<float, double>(config, 0, &dividers);
			break;
		case dtype::STRING:
			header.key_type = 3;
			r = load_dividers<istr, istr>(config, 0, &dividers);
			break;
		case dtype::BLOB:
			header.key_type = 4;
			r = load_dividers<blob, blob>(config, 0, &dividers, true);
			break;
		default:
			return -EINVAL;
	}
	header.dt_count = dividers.size() + 1;
	/* make sure we don't overflow the header field */
	if(header.dt_count != dividers.size() + 1)
		return -EINVAL;
	
	base = dtable_factory::lookup(config, "base");
	if(!base)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	
	r = mkdirat(dfd, name, 0755);
	if(r < 0)
		return r;
	kdd_dfd = openat(dfd, name, O_RDONLY);
	if(kdd_dfd < 0)
	{
		unlinkat(dfd, name, AT_REMOVEDIR);
		return kdd_dfd;
	}
	
	for(uint32_t i = 0; i < header.dt_count; i++)
	{
		char name[32];
		sprintf(name, "kdd_data.%u", i);
		r = base->create(kdd_dfd, name, base_config, key_type);
		if(r < 0)
			goto fail;
	}
	
	meta = openat(kdd_dfd, "kdd_meta", O_WRONLY | O_CREAT, 0644);
	if(meta < 0)
	{
		r = meta;
		goto fail;
	}
	r = pwrite(meta, &header, sizeof(header), 0);
	close(meta);
	if(r != sizeof(header))
		goto fail;
	close(kdd_dfd);
	return 0;
	
fail:
	close(kdd_dfd);
	util::rm_r(dfd, name);
	return (r < 0) ? r : -1;
}

DEFINE_RW_FACTORY(keydiv_dtable);
