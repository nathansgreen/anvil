/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "util.h"
#include "memory_dtable.h"
#include "dtable_wrap_iter.h"
#include "exception_dtable.h"

/* Currently the exception dtable will check if a key exists in a base table and
 * if not it will check an alternative table. Eddie proposed a version where a
 * specific value is stored in the base table to let us know to check the
 * alternative table, rather than requiring all base tables to be able to store
 * nonexistent blobs explicitly. We don't do it that way - instead, the base
 * tables can be given a value to use to mean "nonexistent" and they do the
 * conversion for us. (See array_dtable, for example.) */

exception_dtable::iter::iter(const exception_dtable * source)
	: iter_source<exception_dtable>(source), lastdir(FORWARD)
{
	base_sub = new sub;
	alt_sub = new sub;
	base_sub->iter = dt_source->base->iterator();
	alt_sub->iter = dt_source->alt->iterator();
	first();
}

bool exception_dtable::iter::valid() const
{
	return current_sub->iter->valid();
}

bool exception_dtable::iter::next()
{
	if(lastdir != FORWARD)
	{
		if(current_sub != alt_sub)
			alt_sub->valid = alt_sub->iter->next();
		lastdir = FORWARD;
	}
	
	if(current_sub == alt_sub)
		alt_sub->valid = alt_sub->iter->next();
	base_sub->valid = base_sub->iter->next();
	if(base_sub->valid && alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c <= 0);
		current_sub = (c < 0) ? base_sub : alt_sub;
	}
	else
		current_sub = base_sub;
	return base_sub->valid;
}

bool exception_dtable::iter::prev()
{
	if(lastdir != BACKWARD)
	{
		if(current_sub != alt_sub)
			alt_sub->valid = alt_sub->iter->prev();
		lastdir = BACKWARD;
	}
	
	if(current_sub == alt_sub)
		alt_sub->valid = alt_sub->iter->prev();
	base_sub->valid = base_sub->iter->prev();
	if(base_sub->valid && alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c >= 0);
		current_sub = (c > 0) ? base_sub : alt_sub;
	}
	else
		current_sub = base_sub;
	return base_sub->valid;
}

bool exception_dtable::iter::first()
{
	base_sub->valid = base_sub->iter->first();
	alt_sub->valid = alt_sub->iter->first();
	current_sub = base_sub;
	lastdir = FORWARD;
	
	if(!base_sub->valid)
		return false;
	if(alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c <= 0);
		current_sub = (c < 0) ? base_sub : alt_sub;
	}
	return true;
}

bool exception_dtable::iter::last()
{
	base_sub->valid = base_sub->iter->last();
	alt_sub->valid = alt_sub->iter->last();
	current_sub = base_sub;
	lastdir = BACKWARD;
	
	if(!base_sub->valid)
		return false;
	if(alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c >= 0);
		current_sub = (c > 0) ? base_sub : alt_sub;
	}
	return true;
}

dtype exception_dtable::iter::key() const
{
	return current_sub->iter->key();
}

bool exception_dtable::iter::seek(const dtype & key)
{
	bool base_seek = base_sub->iter->seek(key);
	bool alt_seek = alt_sub->iter->seek(key);
	current_sub = base_sub;
	lastdir = FORWARD;
	
	if(base_seek)
		base_sub->valid = true;
	else
		base_sub->valid = base_sub->iter->valid();
	if(alt_seek)
		alt_sub->valid = true;
	else
		alt_sub->valid = alt_sub->iter->valid();
	
	if(!base_sub->valid)
		return false;
	if(alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c <= 0);
		current_sub = (c < 0) ? base_sub : alt_sub;
	}
	return base_seek;
}

bool exception_dtable::iter::seek(const dtype_test & test)
{
	bool base_seek = base_sub->iter->seek(test);
	bool alt_seek = alt_sub->iter->seek(test);
	current_sub = base_sub;
	lastdir = FORWARD;
	
	if(base_seek)
		base_sub->valid = true;
	else
		base_sub->valid = base_sub->iter->valid();
	if(alt_seek)
		alt_sub->valid = true;
	else
		alt_sub->valid = alt_sub->iter->valid();
	
	if(!base_sub->valid)
		return false;
	if(alt_sub->valid)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		int c = base_sub->iter->key().compare(alt_sub->iter->key(), blob_cmp);
		assert(c <= 0);
		current_sub = (c < 0) ? base_sub : alt_sub;
	}
	return base_seek;
}

metablob exception_dtable::iter::meta() const
{
	return current_sub->iter->meta();
}

blob exception_dtable::iter::value() const
{
	return current_sub->iter->value();
}

const dtable * exception_dtable::iter::source() const
{
	return current_sub->iter->source();
}

exception_dtable::iter::~iter()
{
	if(base_sub)
	{
		delete base_sub->iter;
		delete base_sub;
	}
	if(alt_sub)
	{
		delete alt_sub->iter;
		delete alt_sub;
	}
	current_sub = NULL;
}

dtable::iter * exception_dtable::iterator(ATX_DEF) const
{
	return new iter(this);
}

bool exception_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	bool result = base->present(key, found);
	if(*found)
		return result;
	return alt->present(key, found);
}

blob exception_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	blob value = base->lookup(key, found);
	/* we don't use the blob comparator here; it must match exactly */
	if(!*found || value.compare(reject_value))
		return value;
	value = alt->lookup(key, found);
	if(!*found)
	{
		/* we might have just been storing the reject value as itself */
		*found = true;
		return reject_value;
	}
	return value;
}

int exception_dtable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	const dtable_factory * base_factory;
	const dtable_factory * alt_factory;
	params base_config, alt_config;
	int excp_dfd;
	if(base || alt)
		deinit();
	base_factory = dtable_factory::lookup(config, "base");
	alt_factory = dtable_factory::lookup(config, "alt");
	if(!base_factory || !alt_factory)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("alt_config", &alt_config, params()))
		return -EINVAL;
	if(!config.get_blob_or_string("reject_value", &reject_value))
		return -EINVAL;
	/* the reject value must exist, because nonexistent values
	 * can get pruned out if the shadow does not require them */
	if(!reject_value.exists())
		return -EINVAL;
	excp_dfd = openat(dfd, file, O_RDONLY);
	if(excp_dfd < 0)
		return excp_dfd;
	base = base_factory->open(excp_dfd, "base", base_config, sysj);
	if(!base)
		goto fail_base;
	alt = alt_factory->open(excp_dfd, "alt", alt_config, sysj);
	if(!alt)
		goto fail_alt;
	ktype = base->key_type();
	if(ktype != alt->key_type())
		goto fail_ktype;
	cmp_name = base->get_cmp_name();
	
	close(excp_dfd);
	return 0;
	
fail_ktype:
	alt->destroy();
	alt = NULL;
fail_alt:
	base->destroy();
	base = NULL;
fail_base:
	close(excp_dfd);
	return -1;
}

void exception_dtable::deinit()
{
	if(base || alt)
	{
		reject_value = blob();
		alt->destroy();
		alt = NULL;
		base->destroy();
		base = NULL;
		dtable::deinit();
	}
}

class exception_dtable::reject_iter : public dtable_wrap_iter
{
public:
	inline reject_iter(dtable::iter * base, dtable * rejects, blob reject_value)
		: dtable_wrap_iter(base), rejects(rejects), reject_value(reject_value)
	{
		/* the reject value must exist, because nonexistent values
		 * can get pruned out if the shadow does not require them */
		assert(reject_value.exists());
	}
	
	virtual bool reject(blob * replacement)
	{
		blob value = base->value();
		if(replacement)
			*replacement = reject_value;
		return rejects->insert(base->key(), value) >= 0;
	}
	
private:
	dtable * rejects;
	blob reject_value;
};

int exception_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int excp_dfd, r;
	memory_dtable alt_mdt;
	reject_iter * handler;
	params base_config, alt_config;
	blob reject_value;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	const dtable_factory * alt = dtable_factory::lookup(config, "alt");
	if(!base || !alt)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("alt_config", &alt_config, params()))
		return -EINVAL;
	if(!config.get_blob_or_string("reject_value", &reject_value))
		return -EINVAL;
	/* the reject value must exist, because nonexistent values
	 * can get pruned out if the shadow does not require them */
	if(!reject_value.exists())
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	excp_dfd = openat(dfd, file, O_RDONLY);
	if(excp_dfd < 0)
		goto fail_open;
	/* we should really save the reject_value in a meta file here */
	
	/* we'll always be appending, but it's faster if we say false here */
	r = alt_mdt.init(source->key_type(), false, true);
	if(r < 0)
		goto fail_mdt;
	if(source->get_blob_cmp())
		alt_mdt.set_blob_cmp(source->get_blob_cmp());
	
	handler = new reject_iter(source, &alt_mdt, reject_value);
	if(!handler)
		goto fail_handler;
	
	r = base->create(excp_dfd, "base", base_config, handler, shadow);
	if(r < 0)
		goto fail_base;
	
	/* no shadow - this only has exceptions */
	r = alt->create(excp_dfd, "alt", alt_config, &alt_mdt, NULL);
	if(r < 0)
		goto fail_alt;
	
	delete handler;
	alt_mdt.deinit();
	close(excp_dfd);
	return 0;
	
fail_alt:
	util::rm_r(excp_dfd, "base");
fail_base:
	delete handler;
fail_handler:
	alt_mdt.deinit();
fail_mdt:
	close(excp_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_RO_FACTORY(exception_dtable);
