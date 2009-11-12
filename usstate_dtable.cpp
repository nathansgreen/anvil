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
#include "rwfile.h"
#include "usstate_dtable.h"

/* NOTE: this must be sorted, and we expect all codes to be length 2 */
const blob usstate_dtable::state_codes[USSTATE_COUNT] = {
	"AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
	"HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME",
	"MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM",
	"NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
	"TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"};

usstate_dtable::iter::iter(dtable::iter * base, const usstate_dtable * source)
	: iter_source<usstate_dtable, dtable_wrap_iter>(base, source)
{
	claim_base = true;
}

metablob usstate_dtable::iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob usstate_dtable::iter::value() const
{
	return unpack(base->value(), dt_source->passthrough_value);
}

dtable::iter * usstate_dtable::iterator() const
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

blob usstate_dtable::unpack(blob packed, const blob & passthrough_value)
{
	uint8_t value;
	if(packed.size() != 1)
		return blob();
	value = packed[0];
	if(value < USSTATE_COUNT)
		return state_codes[value];
	if(!packed.compare(passthrough_value))
		return passthrough_value;
	return blob();
}

bool usstate_dtable::pack(blob * unpacked, const blob & passthrough_value)
{
	uint8_t byte;
	ssize_t index = -1;
	if(unpacked->size() == 2)
		index = blob::locate(state_codes, USSTATE_COUNT, *unpacked);
	if(index < 0)
	{
		if(!unpacked->compare(passthrough_value))
		{
			*unpacked = passthrough_value;
			return true;
		}
		return !unpacked->exists();
	}
	assert(index < USSTATE_COUNT);
	byte = index;
	*unpacked = blob(sizeof(byte), &byte);
	return true;
}

bool usstate_dtable::present(const dtype & key, bool * found) const
{
	return base->present(key, found);
}

blob usstate_dtable::lookup(const dtype & key, bool * found) const
{
	blob value = base->lookup(key, found);
	if(value.exists())
		value = unpack(value, passthrough_value);
	return value;
}

blob usstate_dtable::index(size_t index) const
{
	blob value = base->index(index);
	if(value.exists())
		value = unpack(value, passthrough_value);
	return value;
}

bool usstate_dtable::contains_index(size_t index) const
{
	return base->contains_index(index);
}

size_t usstate_dtable::size() const
{
	return base->size();
}

bool usstate_dtable::static_indexed_access(const params & config)
{
	const dtable_factory * factory;
	params base_config;
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return false;
	if(!config.get("base_config", &base_config, params()))
		return false;
	return factory->indexed_access(base_config);
}

int usstate_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get_blob_or_string("passthrough_value", &passthrough_value))
		return -EINVAL;
	if(passthrough_value.exists() && passthrough_value.size() != 1)
		return -EINVAL;
	base = factory->open(dfd, file, base_config);
	if(!base)
		return -1;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	return 0;
}

void usstate_dtable::deinit()
{
	if(base)
	{
		passthrough_value = blob();
		base->destroy();
		base = NULL;
		dtable::deinit();
	}
}

usstate_dtable::rev_iter::rev_iter(dtable::iter * base, blob passthrough_value)
	: dtable_wrap_iter(base), failed(false), passthrough_value(passthrough_value)
{
}

metablob usstate_dtable::rev_iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob usstate_dtable::rev_iter::value() const
{
	blob value = base->value();
	if(value.exists())
		if(!pack(&value, passthrough_value))
			if(!base->reject(&value))
			{
				/* it's too bad we can't report this sooner */
				failed = true;
				value = blob();
			}
	return value;
}

bool usstate_dtable::rev_iter::reject(blob * replacement)
{
	if(failed)
		return false;
	return base->reject(replacement);
}

int usstate_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int r;
	rev_iter * rev;
	params base_config;
	blob passthrough_value;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get_blob_or_string("passthrough_value", &passthrough_value))
		return -EINVAL;
	if(passthrough_value.exists() && passthrough_value.size() != 1)
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	rev = new rev_iter(source, passthrough_value);
	if(!rev)
		return -ENOMEM;
	
	r = base->create(dfd, file, base_config, rev, shadow);
	if(rev->failed)
	{
		util::rm_r(dfd, file);
		r = -ENOSYS;
	}
	
	delete rev;
	return r;
}

DEFINE_RO_FACTORY(usstate_dtable);
