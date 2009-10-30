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

keydiv_dtable::iter::iter(const keydiv_dtable * source)
	: iter_source<keydiv_dtable>(source), current_index(0)
{
	subs = new sub[source->sub.size()];
	for(size_t i = 0; i < source->sub.size(); i++)
	{
		subs[i].iter = source->sub[i]->iterator();
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
			subs[current_index].at_end = subs[current_index].iter->first();
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

dtable::iter * keydiv_dtable::iterator() const
{
	return new iter(this);
}

bool keydiv_dtable::present(const dtype & key, bool * found) const
{
	size_t index = key_index(key);
	assert(index < sub.size());
	return sub[index]->present(key, found);
}

blob keydiv_dtable::lookup(const dtype & key, bool * found) const
{
	size_t index = key_index(key);
	assert(index < sub.size());
	return sub[index]->lookup(key, found);
}

int keydiv_dtable::insert(const dtype & key, const blob & blob, bool append)
{
	size_t index = key_index(key);
	assert(index < sub.size());
	return sub[index]->insert(key, blob, append);
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

int keydiv_dtable::init(int dfd, const char * name, const params & config)
{
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
		sprintf(name, "md_data.%u", i);
		source = base->open(kdd_dfd, name, base_config);
		if(!source)
			goto fail_sub;
		sub.push_back(source);
	}
	
	if(sub[0]->get_cmp_name())
		cmp_name = sub[0]->get_cmp_name();
	
	return 0;
	
fail_sub:
	for(size_t i = 0; i < sub.size(); i++)
		delete sub[i];
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
		delete sub[i];
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
		sprintf(name, "md_data.%u", i);
		r = base->create(kdd_dfd, name, base_config, key_type);
		if(r < 0)
			goto fail;
	}
	
	meta = openat(kdd_dfd, "kdd_meta", O_WRONLY);
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
