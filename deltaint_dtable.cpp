/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include "util.h"
#include "rwfile.h"
#include "deltaint_dtable.h"

/* TODO: I think we are not as tolerant of nonexistent values in the reference
 * dtable as we need to be, in case it gets any in place of rejected values */

deltaint_dtable::iter::iter(dtable::iter * base, const deltaint_dtable * source)
	: iter_source<deltaint_dtable, dtable_wrap_iter_noindex>(base, source), current(0), exists(false)
{
	claim_base = true;
	if(base->valid())
	{
		blob value = base->value();
		if(value.exists())
		{
			assert(value.size() == sizeof(uint32_t));
			current = value.index<uint32_t>(0);
			exists = true;
		}
	}
}

bool deltaint_dtable::iter::next()
{
	bool valid = base->next();
	if(valid)
	{
		blob value = base->value();
		exists = value.exists();
		if(exists)
		{
			assert(value.size() == sizeof(uint32_t));
			current += value.index<uint32_t>(0);
		}
	}
	else
		exists = false;
	return valid;
}

bool deltaint_dtable::iter::prev()
{
	blob value = base->valid() ? base->value() : blob();
	bool valid = base->prev();
	if(valid)
	{
		exists = base->meta().exists();
		if(value.exists())
		{
			assert(value.size() == sizeof(uint32_t));
			current -= value.index<uint32_t>(0);
		}
	}
	/* else don't change exists */
	return valid;
}

bool deltaint_dtable::iter::first()
{
	bool valid = base->first();
	if(valid)
	{
		blob value = base->value();
		exists = value.exists();
		if(exists)
		{
			assert(value.size() == sizeof(uint32_t));
			current = value.index<uint32_t>(0);
		}
		else
			current = 0;
	}
	else
		exists = false;
	return valid;
}

bool deltaint_dtable::iter::last()
{
	blob value;
	bool valid = dt_source->ref_iter->last();
	if(!valid)
		return false;
	value = dt_source->ref_iter->value();
	assert(value.size() == sizeof(uint32_t));
	current = value.index<uint32_t>(0);
	valid = base->seek(dt_source->ref_iter->key());
	assert(valid && base->meta().exists());
	exists = true;
	while(next());
	return prev();
}

bool deltaint_dtable::iter::seek(const dtype & key)
{
	int cmp;
	bool found;
	blob value;
	found = dt_source->ref_iter->seek(key);
	if(found)
	{
		/* we're lucky, it's in the reference dtable */
		value = dt_source->ref_iter->value();
		assert(value.size() == sizeof(uint32_t));
		found = base->seek(key);
		assert(found);
		current = value.index<uint32_t>(0);
		exists = true;
		return true;
	}
	/* didn't find it, so it went to the next element */
	found = dt_source->ref_iter->prev();
	if(!found)
	{
		/* the "next" element was itself the first: the key was too small */
		first();
		return false;
	}
	assert(dt_source->ref_iter->valid());
	/* now the ref iter points before the requested key */
	found = base->seek(dt_source->ref_iter->key());
	assert(found && base->valid());
	/* now the base iter points at the same place as the ref iter */
	value = dt_source->ref_iter->value();
	assert(value.size() == sizeof(uint32_t));
	current = value.index<uint32_t>(0);
	/* reconstruct the current value */
	do {
		found = base->next();
		if(!found)
			/* we ran out of elements: the key was too large */
			return false;
		cmp = base->key().compare(key, dt_source->blob_cmp);
		value = base->value();
		exists = value.exists();
		if(!exists)
			continue;
		assert(value.size() == sizeof(uint32_t));
		current += value.index<uint32_t>(0);
	} while(cmp < 0);
	return !cmp;
}

bool deltaint_dtable::iter::seek(const dtype_test & test)
{
	int cmp;
	bool found;
	blob value;
	found = dt_source->ref_iter->seek(test);
	if(found)
	{
		/* we're lucky, it's in the reference dtable */
		value = dt_source->ref_iter->value();
		assert(value.size() == sizeof(uint32_t));
		found = base->seek(test);
		assert(found);
		current = value.index<uint32_t>(0);
		exists = true;
		return true;
	}
	/* didn't find it, so it went to the next element */
	found = dt_source->ref_iter->prev();
	if(!found)
	{
		/* the "next" element was itself the first: the key was too small */
		first();
		return false;
	}
	assert(dt_source->ref_iter->valid());
	/* now the ref iter points before the requested key */
	found = base->seek(dt_source->ref_iter->key());
	assert(found && base->valid());
	/* now the base iter points at the same place as the ref iter */
	value = dt_source->ref_iter->value();
	assert(value.size() == sizeof(uint32_t));
	current = value.index<uint32_t>(0);
	/* reconstruct the current value */
	do {
		found = base->next();
		if(!found)
			/* we ran out of elements: the key was too large */
			return false;
		cmp = test(base->key());
		value = base->value();
		exists = value.exists();
		if(!exists)
			continue;
		assert(value.size() == sizeof(uint32_t));
		current += value.index<uint32_t>(0);
	} while(cmp < 0);
	return !cmp;
}

metablob deltaint_dtable::iter::meta() const
{
	return exists ? metablob(sizeof(uint32_t)) : metablob();
}

blob deltaint_dtable::iter::value() const
{
	return exists ? blob(sizeof(uint32_t), &current) : blob();
}

dtable::iter * deltaint_dtable::iterator() const
{
	iter * value;
	dtable::iter * source = base->iterator();
	if(!source)
		return NULL;
	value = new iter(source, this);
	if(!value)
	{
		delete source;
		return NULL;
	}
	return value;
}

bool deltaint_dtable::present(const dtype & key, bool * found) const
{
	return base->present(key, found);
}

blob deltaint_dtable::lookup(const dtype & key, bool * found) const
{
	uint32_t value;
	blob blob_value;
	/* make sure it even exists */
	if(!base->lookup(key, found).exists())
		return blob();
	*found = ref_iter->seek(key);
	if(*found)
		/* we're lucky, it's in the reference dtable */
		return ref_iter->value();
	/* didn't find it, so it went to the next element */
	*found = ref_iter->prev();
	assert(*found && ref_iter->valid());
	/* now the ref iter points before the requested key */
	*found = scan_iter->seek(ref_iter->key());
	assert(*found && scan_iter->valid());
	/* now the scan iter points at the same place as the ref iter */
	blob_value = ref_iter->value();
	assert(blob_value.size() == sizeof(value));
	value = blob_value.index<uint32_t>(0);
	/* reconstruct the value */
	do {
		*found = scan_iter->next();
		assert(*found);
		blob_value = scan_iter->value();
		if(!blob_value.exists())
			continue;
		assert(blob_value.size() == sizeof(value));
		value += blob_value.index<uint32_t>(0);
	} while(scan_iter->key().compare(key, blob_cmp));
	return blob(sizeof(value), &value);
}

int deltaint_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * base_factory;
	const dtable_factory * ref_factory;
	params base_config, ref_config;
	int di_dfd;
	if(base)
		deinit();
	base_factory = dtable_factory::lookup(config, "base");
	ref_factory = dtable_factory::lookup(config, "ref");
	if(!base_factory || !ref_factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("ref_config", &ref_config, params()))
		return -EINVAL;
	di_dfd = openat(dfd, file, 0);
	if(di_dfd < 0)
		return di_dfd;
	base = base_factory->open(di_dfd, "base", base_config);
	if(!base)
		goto fail_base;
	reference = ref_factory->open(di_dfd, "ref", ref_config);
	if(!reference)
		goto fail_reference;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	
	assert(ktype == reference->key_type());
	
	scan_iter = base->iterator();
	if(!scan_iter)
		goto fail_scan;
	ref_iter = reference->iterator();
	if(!ref_iter)
		goto fail_iter;
	
	close(di_dfd);
	return 0;
	
fail_iter:
	delete scan_iter;
	scan_iter = NULL;
fail_scan:
	delete reference;
	reference = NULL;
fail_reference:
	delete base;
	base = NULL;
fail_base:
	close(di_dfd);
	return -1;
}

void deltaint_dtable::deinit()
{
	if(base)
	{
		delete ref_iter;
		ref_iter = NULL;
		delete scan_iter;
		scan_iter = NULL;
		delete reference;
		reference = NULL;
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

deltaint_dtable::rev_iter_delta::rev_iter_delta(dtable::iter * base)
	: dtable_wrap_iter_noindex(base), failed(false), delta(0), previous(0), exists(false)
{
	if(base->valid())
	{
		blob value = base->value();
		if(value.exists() && reject_check(&value))
		{
			delta = value.index<uint32_t>(0);
			previous = delta;
			exists = true;
		}
	}
}

bool deltaint_dtable::rev_iter_delta::reject_check(blob * value)
{
	if(value->size() != sizeof(uint32_t))
	{
		if(!base->reject(value))
		{
			failed = true;
			return false;
		}
		if(!value->exists())
			return false;
		if(value->size() != sizeof(uint32_t))
		{
			failed = true;
			return false;
		}
	}
	return true;
}

bool deltaint_dtable::rev_iter_delta::next()
{
	bool valid = base->next();
	if(valid)
	{
		blob value = base->value();
		if(value.exists() && reject_check(&value))
		{
			uint32_t current = value.index<uint32_t>(0);
			delta = current - previous;
			previous = current;
			exists = true;
		}
		else
			exists = false;
	}
	else
		exists = false;
	return valid;
}

bool deltaint_dtable::rev_iter_delta::first()
{
	bool valid = base->first();
	if(valid)
	{
		blob value = base->value();
		if(value.exists() && reject_check(&value))
		{
			delta = value.index<uint32_t>(0);
			previous = delta;
			exists = true;
		}
		else
		{
			delta = 0;
			previous = 0;
			exists = false;
		}
	}
	else
		exists = false;
	return valid;
}

metablob deltaint_dtable::rev_iter_delta::meta() const
{
	return exists ? metablob(sizeof(delta)) : metablob();
}

blob deltaint_dtable::rev_iter_delta::value() const
{
	return exists ? blob(sizeof(delta), &delta) : blob();
}

deltaint_dtable::rev_iter_ref::rev_iter_ref(dtable::iter * base, size_t skip)
	: dtable_wrap_iter_noindex(base), skip(skip)
{
	bool valid = base->valid();
	/* find the first uint32_t-sized entry */
	while(valid && base->meta().size() != sizeof(uint32_t))
		valid = base->next();
}

bool deltaint_dtable::rev_iter_ref::next()
{
	bool valid = false;
	for(size_t i = 0; i < skip; i++)
		if(!(valid = base->next()))
			break;
	/* find the next uint32_t-sized entry */
	while(valid && base->meta().size() != sizeof(uint32_t))
		valid = base->next();
	return valid;
}

bool deltaint_dtable::rev_iter_ref::first()
{
	bool valid = base->first();
	/* find the first uint32_t-sized entry */
	while(valid && base->meta().size() != sizeof(uint32_t))
		valid = base->next();
	return valid;
}

bool deltaint_dtable::rev_iter_ref::reject(blob * replacement)
{
	/* store the nonexistent value instead; in reality, we don't need any
	 * value stored at all: this whole dtable is just an optimization */
	/* probably this will result in no value being stored, actually */
	*replacement = blob();
	return true;
}

int deltaint_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int r, di_dfd, skip;
	rev_iter_delta * rev;
	rev_iter_ref * ref;
	params base_config, ref_config;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	const dtable_factory * reference = dtable_factory::lookup(config, "ref");
	if(!base || !reference)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("ref_config", &ref_config, params()))
		return -EINVAL;
	if(!config.get("skip", &skip, 0) || skip < 2)
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	di_dfd = openat(dfd, file, 0);
	if(di_dfd < 0)
		goto fail_open;
	
	rev = new rev_iter_delta(source);
	if(!rev)
	{
		r = -ENOMEM;
		goto fail_iter_1;
	}
	r = base->create(di_dfd, "base", base_config, rev, shadow);
	if(r < 0)
		goto fail_create_1;
	if(rev->failed)
	{
		r = -ENOSYS;
		goto fail_iter_2;
	}
	
	ref = new rev_iter_ref(source, skip);
	if(!ref)
	{
		r = -ENOMEM;
		goto fail_iter_2;
	}
	r = reference->create(di_dfd, "ref", ref_config, ref, NULL);
	if(r < 0)
		goto fail_create_2;
	
	delete ref;
	delete rev;
	close(di_dfd);
	return r;
	
fail_create_2:
	delete ref;
fail_iter_2:
	util::rm_r(di_dfd, "base");
fail_create_1:
	delete rev;
fail_iter_1:
	close(di_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(deltaint_dtable);
