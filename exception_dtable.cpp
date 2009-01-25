/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "util.h"
#include "journal_dtable.h"
#include "exception_dtable.h"

/* Currently the exception dtable will check if a key exists in a base table and
 * if not it will check an alternative table. Eddie proposed a version where a
 * specific value is stored in the base table to let us know to check the
 * alternative table, rather than requiring all base tables to be able to store
 * nonexistent blobs explicitly. We don't currently support this, but it should
 * be simple to add. */

exception_dtable::iter::iter(const exception_dtable * source)
	: iter_source<exception_dtable>(source), lastdir(FORWARD)
{
	base_iter = new sub;
	alternatives_iter = new sub;
	base_iter->iter = dt_source->base->iterator();
	alternatives_iter->iter = dt_source->alternatives->iterator();
	first();
}

bool exception_dtable::iter::valid() const
{
	return current_iter->iter->valid();
}

bool exception_dtable::iter::next()
{
	const blob_comparator * blob_cmp = dt_source->blob_cmp;
	sub * other_iter = (current_iter == base_iter) ? alternatives_iter : base_iter;
	if(lastdir != FORWARD)
	{
		while(other_iter->iter->valid() &&
		      (other_iter->iter->key().compare(current_iter->iter->key(), blob_cmp) <= 0))
			other_iter->valid = other_iter->iter->next();
		lastdir = FORWARD;
	}
	current_iter->valid = current_iter->iter->next();
	if(current_iter->valid)
	{
		if(other_iter->valid)
		{
			int comp = current_iter->iter->key().compare(other_iter->iter->key(), blob_cmp);
			if(!comp)
				other_iter->valid = other_iter->iter->next();
			if(comp <= 0)
				return true;
		}
		else
			return true;
	}
	
	if(other_iter->valid)
	{
		current_iter = other_iter;
		return true;
	}
	
	return false;
}

bool exception_dtable::iter::prev()
{
	const blob_comparator * blob_cmp = dt_source->blob_cmp;
	sub * other_iter = (current_iter == base_iter) ? alternatives_iter : base_iter;
	if(lastdir != BACKWARD)
	{
		/* Are we at the last element ? */
		if(!other_iter->valid)
			other_iter->valid = other_iter->iter->prev();
		while(other_iter->valid &&
				 (other_iter->iter->key().compare(current_iter->iter->key(), blob_cmp) >= 0))
		{
			other_iter->valid = other_iter->iter->prev();
			if(!other_iter->valid)
				break;
		}
		lastdir = BACKWARD;
	}
	current_iter->valid = current_iter->iter->prev();
	if(current_iter->valid)
	{
		if(other_iter->valid)
		{
			int comp = current_iter->iter->key().compare(other_iter->iter->key(), blob_cmp);
			if(!comp)
				other_iter->valid = other_iter->iter->prev();
			if(comp >= 0)
				return true;
		}
		else
			return true;
	}
	
	if(other_iter->valid)
	{
		current_iter = other_iter;
		return true;
	}
	
	return false;
}

bool exception_dtable::iter::first()
{
	bool base_first = base_iter->iter->first();
	bool alternatives_first = alternatives_iter->iter->first();
	if(!(base_first || alternatives_first))
		return false;
	if(base_first && alternatives_first)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		if(base_iter->iter->key().compare(alternatives_iter->iter->key(), blob_cmp) <= 0)
			current_iter = base_iter;
		else
			current_iter = alternatives_iter;
	}
	else
		current_iter = base_first ? base_iter : alternatives_iter;
	
	base_iter->valid = base_first;
	alternatives_iter->valid = alternatives_first;
	
	lastdir = FORWARD;
	
	return true;
}

bool exception_dtable::iter::last()
{
	bool base_last = base_iter->iter->last();
	bool exception_last = alternatives_iter->iter->last();
	if(!(base_last || exception_last))
		return false;
	if(base_last && exception_last)
	{
		const blob_comparator * blob_cmp = dt_source->blob_cmp;
		if(base_iter->iter->key().compare(alternatives_iter->iter->key(), blob_cmp) >= 0)
			current_iter = base_iter;
		else
			current_iter = alternatives_iter;
	}
	else
		current_iter = base_last ? base_iter : alternatives_iter;
	
	lastdir = FORWARD;
	return true;
}

dtype exception_dtable::iter::key() const
{
	return current_iter->iter->key();
}

bool exception_dtable::iter::seek(const dtype & key)
{
	bool base_seek = base_iter->iter->seek(key);
	bool exception_seek = alternatives_iter->iter->seek(key);
	
	lastdir = FORWARD;
	
	if(base_seek)
		current_iter = base_iter;
	else if(exception_seek)
		current_iter = alternatives_iter;
	else
		return false;
	return true;
}

bool exception_dtable::iter::seek(const dtype_test & test)
{
	bool base_seek = base_iter->iter->seek(test);
	bool exception_seek = alternatives_iter->iter->seek(test);
	
	lastdir = FORWARD;
	
	if(base_seek)
		current_iter = base_iter;
	else if(exception_seek)
		current_iter = alternatives_iter;
	else
		return false;
	return true;
}

metablob exception_dtable::iter::meta() const
{
	return current_iter->iter->meta();
}

blob exception_dtable::iter::value() const
{
	return current_iter->iter->value();
}

const dtable * exception_dtable::iter::source() const
{
	return current_iter->iter->source();
}

exception_dtable::iter::~iter()
{
	if(base_iter)
	{
		delete base_iter->iter;
		delete base_iter;
	}
	if(alternatives_iter)
	{
		delete alternatives_iter->iter;
		delete alternatives_iter;
	}
	current_iter = NULL;
}

dtable::iter * exception_dtable::iterator() const
{
	return new iter(this);
}

blob exception_dtable::lookup(const dtype & key, bool * found) const
{
	blob value = base->lookup(key, found);
	if(*found)
		return value;
	value = alternatives->lookup(key, found);
	return value;
}

int exception_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * base_factory;
	const dtable_factory * alt_factory;
	params base_config, alt_config;
	int excp_dfd;
	if(base || alternatives)
		deinit();
	base_factory = dtable_factory::lookup(config, "base");
	alt_factory = dtable_factory::lookup(config, "alt");
	if(!base_factory || !alt_factory)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("alt_config", &alt_config, params()))
		return -EINVAL;
	excp_dfd = openat(dfd, file, 0);
	if(excp_dfd < 0)
		return excp_dfd;
	base = base_factory->open(excp_dfd, "base", base_config);
	if(!base)
		goto fail_base;
	alternatives = alt_factory->open(excp_dfd, "alt", alt_config);
	if(!alternatives)
		goto fail_alt;
	ktype = base->key_type();
	if(ktype != alternatives->key_type())
		goto fail_ktype;
	cmp_name = base->get_cmp_name();
	
	close(excp_dfd);
	return 0;
	
fail_ktype:
	delete alternatives;
	alternatives = NULL;
fail_alt:
	delete base;
	base = NULL;
fail_base:
	close(excp_dfd);
	return -1;
}

void exception_dtable::deinit()
{
	if(base || alternatives)
	{
		delete alternatives;
		alternatives = NULL;
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

int exception_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const dtable * shadow)
{
	int excp_dfd, r;
	sys_journal alt_journal;
	journal_dtable alt_jdt;
	dtable::iter * filtered;
	sys_journal::listener_id id;
	params base_config, alt_config, filter_config;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	const dtable_factory * alt = dtable_factory::lookup(config, "alt");
	if(!base || !alt)
		return -EINVAL;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("alt_config", &alt_config, params()))
		return -EINVAL;
	if(!config.get("filter_config", &filter_config, params()))
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	excp_dfd = openat(dfd, file, 0);
	if(excp_dfd < 0)
		goto fail_open;
	
	r = alt_journal.init(excp_dfd, "alt_journal", true);
	if(r < 0)
		goto fail_journal;
	id = sys_journal::get_unique_id();
	if(id == sys_journal::NO_ID)
		goto fail_id;
	/* we'll always be appending, since we'll be reading the data in order */
	r = alt_jdt.init(source->key_type(), id, true, &alt_journal);
	if(r < 0)
		goto fail_id;
	if(source->get_blob_cmp())
		alt_jdt.set_blob_cmp(source->get_blob_cmp());
	
	filtered = base->filter_iterator(source, filter_config, &alt_jdt);
	if(!filtered)
		goto fail_filter;
	
	r = base->create(excp_dfd, "base", base_config, filtered, shadow);
	if(r < 0)
		goto fail_base;
	
	/* no shadow - this only has exceptions */
	/* NOTE: this might need to change if we use something other than
	 * nonexistent blobs to signal the need to check this dtable */
	r = alt->create(excp_dfd, "alt", alt_config, &alt_jdt, NULL);
	if(r < 0)
		goto fail_alt;
	
	if(filtered != source)
		delete filtered;
	alt_jdt.deinit(true);
	alt_journal.deinit(true);
	close(excp_dfd);
	return 0;
	
fail_alt:
	util::rm_r(excp_dfd, "base");
fail_base:
	if(filtered != source)
		delete filtered;
fail_filter:
	alt_jdt.deinit(true);
fail_id:
	alt_journal.deinit(true);
fail_journal:
	close(excp_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_RO_FACTORY(exception_dtable);
