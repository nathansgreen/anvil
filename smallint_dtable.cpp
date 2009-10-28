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
#include "smallint_dtable.h"

smallint_dtable::iter::iter(dtable::iter * base, const smallint_dtable * source)
	: iter_source<smallint_dtable, dtable_wrap_iter>(base, source)
{
	claim_base = true;
}

metablob smallint_dtable::iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob smallint_dtable::iter::value() const
{
	return unpack(base->value(), dt_source->byte_count);
}

dtable::iter * smallint_dtable::iterator() const
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

blob smallint_dtable::unpack(blob packed, size_t byte_count)
{
	uint32_t value;
	if(packed.size() != byte_count)
		return blob();
	value = util::read_bytes(&packed[0], 0, byte_count);
	return blob(sizeof(value), &value);
}

#define VALUE_LIMIT(bytes) (1u << (8 * (bytes)))

bool smallint_dtable::pack(blob * unpacked, size_t byte_count)
{
	if(unpacked->size() == sizeof(uint32_t))
	{
		uint32_t value = unpacked->index<uint32_t>(0);
		if(value < VALUE_LIMIT(byte_count))
		{
			uint8_t bytes[sizeof(uint32_t)];
			util::layout_bytes(bytes, 0, value, byte_count);
			*unpacked = blob(byte_count, bytes);
			return true;
		}
	}
	return !unpacked->exists();
}

bool smallint_dtable::present(const dtype & key, bool * found) const
{
	return base->present(key, found);
}

blob smallint_dtable::lookup(const dtype & key, bool * found) const
{
	blob value = base->lookup(key, found);
	if(value.exists())
		value = unpack(value, byte_count);
	return value;
}

blob smallint_dtable::index(size_t index) const
{
	blob value = base->index(index);
	if(value.exists())
		value = unpack(value, byte_count);
	return value;
}

bool smallint_dtable::contains_index(size_t index) const
{
	return base->contains_index(index);
}

size_t smallint_dtable::size() const
{
	return base->size();
}

bool smallint_dtable::static_indexed_access(const params & config)
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

int smallint_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	int bytes;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("bytes", &bytes, 0) || bytes < 1 || bytes > 3)
		return -EINVAL;
	byte_count = bytes;
	base = factory->open(dfd, file, base_config);
	if(!base)
		return -1;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	return 0;
}

void smallint_dtable::deinit()
{
	if(base)
	{
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

smallint_dtable::rev_iter::rev_iter(dtable::iter * base, size_t byte_count)
	: dtable_wrap_iter(base), failed(false), byte_count(byte_count)
{
}

metablob smallint_dtable::rev_iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob smallint_dtable::rev_iter::value() const
{
	blob value = base->value();
	if(value.exists())
		if(!pack(&value, byte_count))
			if(!base->reject(&value) || !pack(&value, byte_count))
			{
				/* it's too bad we can't report this sooner */
				failed = true;
				value = blob();
			}
	return value;
}

bool smallint_dtable::rev_iter::reject(blob * replacement)
{
	if(failed)
		return false;
	return base->reject(replacement);
}

int smallint_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int r, bytes;
	rev_iter * rev;
	params base_config;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("bytes", &bytes, 0) || bytes < 1 || bytes > 3)
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	rev = new rev_iter(source, bytes);
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

DEFINE_RO_FACTORY(smallint_dtable);
