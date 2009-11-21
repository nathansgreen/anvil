/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include "util.h"
#include "uniq_dtable.h"

/* The uniq dtable works a little bit like a Lempel-Ziv data compressor: it
 * keeps a sliding window of previous values, and can refer back to them if a
 * later key has the same value. To accomplish this, it uses two underlying
 * dtables: one for the values, with consecutive integer keys, and another to
 * map the real keys onto the appropriate indices in the data dtable. With an
 * appropriate dtable to store the values (e.g. simple dtable or linear dtable),
 * the integer keys need not be stored; the only overhead is then the values
 * stored in the key dtable to point into the data dtable. If enough duplicate
 * values are eliminated, this overhead will be less than the savings. */

uniq_dtable::iter::iter(dtable::iter * base, const uniq_dtable * source)
	: iter_source<uniq_dtable, dtable_wrap_iter>(base, source)
{
	claim_base = true;
}

metablob uniq_dtable::iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob uniq_dtable::iter::value() const
{
	blob index_blob = base->value();
	if(!index_blob.exists())
		return index_blob;
	assert(index_blob.size() == sizeof(uint32_t));
	uint32_t index = index_blob.index<uint32_t>(0);
	return dt_source->valuebase->find(index);
}

dtable::iter * uniq_dtable::iterator(ATX_DEF) const
{
	iter * value;
	dtable::iter * source = keybase->iterator();
	if(!source)
		return NULL;
	value = new iter(source, this);
	if(!value)
		delete source;
	return value;
}

bool uniq_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	return keybase->present(key, found);
}

blob uniq_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	blob value = keybase->lookup(key, found);
	if(value.exists())
	{
		assert(value.size() == sizeof(uint32_t));
		uint32_t index = value.index<uint32_t>(0);
		value = valuebase->find(index);
	}
	return value;
}

blob uniq_dtable::index(size_t index) const
{
	blob value = keybase->index(index);
	if(value.exists())
	{
		assert(value.size() == sizeof(uint32_t));
		uint32_t index = value.index<uint32_t>(0);
		value = valuebase->find(index);
	}
	return value;
}

bool uniq_dtable::contains_index(size_t index) const
{
	return keybase->contains_index(index);
}

size_t uniq_dtable::size() const
{
	return keybase->size();
}

bool uniq_dtable::static_indexed_access(const params & config)
{
	const dtable_factory * factory;
	params base_config;
	factory = dtable_factory::lookup(config, "keybase");
	if(!factory)
		return false;
	if(!config.get("keybase_config", &base_config, params()))
		return false;
	return factory->indexed_access(base_config);
}

int uniq_dtable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	int u_dfd;
	const dtable_factory * kb_factory;
	const dtable_factory * vb_factory;
	params keybase_config, valuebase_config;
	if(keybase)
		deinit();
	kb_factory = dtable_factory::lookup(config, "keybase");
	vb_factory = dtable_factory::lookup(config, "valuebase");
	if(!kb_factory || !vb_factory)
		return -ENOENT;
	if(!config.get("keybase_config", &keybase_config, params()))
		return -EINVAL;
	if(!config.get("valuebase_config", &valuebase_config, params()))
		return -EINVAL;
	u_dfd = openat(dfd, file, O_RDONLY);
	if(u_dfd < 0)
		return u_dfd;
	keybase = kb_factory->open(u_dfd, "keys", keybase_config, sysj);
	if(!keybase)
		goto fail_keys;
	valuebase = vb_factory->open(u_dfd, "values", valuebase_config, sysj);
	if(!valuebase)
		goto fail_values;
	ktype = keybase->key_type();
	cmp_name = keybase->get_cmp_name();
	
	close(u_dfd);
	return 0;
	
fail_values:
	keybase->destroy();
	keybase = NULL;
fail_keys:
	close(u_dfd);
	return -1;
}

void uniq_dtable::deinit()
{
	if(keybase)
	{
		valuebase->destroy();
		valuebase = NULL;
		keybase->destroy();
		keybase = NULL;
		dtable::deinit();
	}
}

uniq_dtable::sliding_window::idx_ref * uniq_dtable::sliding_window::append(const blob & value, bool * store)
{
	idx_ref * ref;
	value_map::iterator it = values.find(value);
	if(it != values.end())
	{
		if(store)
			*store = false;
		return &it->second;
	}
	if(store)
		*store = true;
	queue.append(value);
	if(queue.size() == window_size)
	{
		values.erase(queue.first());
		queue.pop();
	}
	assert(queue.size() <= window_size);
	ref = &values[value];
	ref->index = next_index++;
	return ref;
}

bool uniq_dtable::rev_iter_key::first()
{
	window.reset();
	return advance(true);
}

metablob uniq_dtable::rev_iter_key::meta() const
{
	metablob meta = base->meta();
	if(meta.exists())
		meta = metablob(sizeof(uint32_t));
	return meta;
}

bool uniq_dtable::rev_iter_key::advance(bool do_first)
{
	bool ok;
	blob value;
	sliding_window::idx_ref * ref;
	if(do_first)
		ok = base->first();
	else
		ok = base->next();
	if(!ok)
	{
		current_value = blob();
		return false;
	}
	value = base->value();
	if(!value.exists())
	{
		current_value = value;
		return true;
	}
	ref = window.append(value);
	if(!current_value.exists() || ref->index != current_value.index<uint32_t>(0))
		current_value = blob(sizeof(ref->index), &ref->index);
	return true;
}

uniq_dtable::rev_iter_key::rev_iter_key(dtable::iter * base, size_t window_size)
	: dtable_wrap_iter(base), window(window_size)
{
	advance(true);
}

bool uniq_dtable::rev_iter_value::first()
{
	window.reset();
	current_key = dtype(0u);
	current_value = blob();
	current_valid = true;
	return advance(true);
}

bool uniq_dtable::rev_iter_value::reject(blob * replacement)
{
	/* OK, so the underlying dtable wants to reject this value. That's fine,
	 * but we need to make sure that future keys with this same value (which
	 * already refer back to it in the keybase) will be OK. We need to mark
	 * the value in the sliding window as having been rejected, and ensure
	 * that any future references to it are also rejected and also replaced
	 * by the same value. (So that the references will still be correct.) */
	/* We can do this in two ways: by hoping the underlying dtable will
	 * reject the values and just checking everything is OK, or by actively
	 * rejecting the values and only hoping the replacement is the same. We
	 * use the second approach since it is less fragile. See advance(). */
	/* TODO: currently each unique rejected value will be stored separately
	 * even when the replacement values are the same. This should be fixed. */
	sliding_window::idx_ref * ref;
	if(failed)
		return false;
	assert(current_value.exists());
	if(!base->reject(replacement))
		return false;
	assert(replacement->exists());
	ref = window.append(current_value);
	assert(ref->index == current_key.u32);
	ref->replacement = *replacement;
	return true;
}

bool uniq_dtable::rev_iter_value::advance(bool do_first)
{
	bool ok;
	if(do_first)
		ok = base->first();
	else
		ok = base->next();
	for(; ok; ok = base->next())
	{
		bool store;
		sliding_window::idx_ref * ref;
		blob value = base->value();
		if(!value.exists())
			continue;
		ref = window.append(value, &store);
		if(!store)
		{
			/* If this is a duplicate of a rejected value, reject it
			 * and make sure the replacement matches the original
			 * replacement. See reject() for an explanation of this. */
			if(ref->replacement.exists())
			{
				blob replacement;
				if(!base->reject(&replacement) || replacement.compare(ref->replacement))
					/* it's too bad we can't report this sooner */
					failed = true;
			}
			continue;
		}
		assert(ref->index >= current_key.u32);
		current_key = dtype(ref->index);
		current_value = value;
		current_valid = true;
		return true;
	}
	assert(!ok);
	current_value = blob();
	current_valid = false;
	return false;
}

uniq_dtable::rev_iter_value::rev_iter_value(dtable::iter * base, size_t window_size)
	: failed(false), base(base), window(window_size), current_key(0u), current_valid(true)
{
	advance(true);
}

int uniq_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int u_dfd, r;
	rev_iter_key * rev_key;
	rev_iter_value * rev_value;
	params keybase_config, valuebase_config;
	const dtable_factory * keybase = dtable_factory::lookup(config, "keybase");
	const dtable_factory * valuebase = dtable_factory::lookup(config, "valuebase");
	if(!keybase || !valuebase)
		return -ENOENT;
	if(!config.get("keybase_config", &keybase_config, params()))
		return -EINVAL;
	if(!config.get("valuebase_config", &valuebase_config, params()))
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	u_dfd = openat(dfd, file, O_RDONLY);
	if(u_dfd < 0)
		goto fail_open;
	
	rev_key = new rev_iter_key(source, 4096);
	if(!rev_key)
		goto fail_keys;
	r = keybase->create(u_dfd, "keys", keybase_config, rev_key, shadow);
	delete rev_key;
	if(r < 0)
		goto fail_keys;
	
	rev_value = new rev_iter_value(source, 4096);
	if(!rev_value)
		goto fail_iter;
	r = valuebase->create(u_dfd, "values", valuebase_config, rev_value, NULL);
	if(r < 0)
		goto fail_values;
	if(rev_value->failed)
		goto fail_undetect;
	delete rev_value;
	
	close(u_dfd);
	return 0;
	
fail_undetect:
	util::rm_r(u_dfd, "values");
fail_values:
	delete rev_value;
fail_iter:
	util::rm_r(u_dfd, "keys");
fail_keys:
	close(u_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(uniq_dtable);
