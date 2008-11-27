/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "util.h"
#include "btree_dtable.h"

btree_dtable::iter::iter(dtable::iter * base, const btree_dtable * source)
	: base_iter(base), bdt_source(source)
{
}

bool btree_dtable::iter::valid() const
{
	return base_iter->valid();
}

bool btree_dtable::iter::next()
{
	return base_iter->next();
}

bool btree_dtable::iter::prev()
{
	return base_iter->prev();
}

bool btree_dtable::iter::first()
{
	return base_iter->first();
}

bool btree_dtable::iter::last()
{
	return base_iter->last();
}

dtype btree_dtable::iter::key() const
{
	return base_iter->key();
}

bool btree_dtable::iter::seek(const dtype & key)
{
	/* XXX use the btree here */
	return base_iter->seek(key);
}

bool btree_dtable::iter::seek(const dtype_test & test)
{
	/* XXX use the btree here */
	return base_iter->seek(test);
}

bool btree_dtable::iter::seek(size_t index)
{
	return base_iter->seek(index);
}

metablob btree_dtable::iter::meta() const
{
	return base_iter->meta();
}

blob btree_dtable::iter::value() const
{
	return base_iter->value();
}

const dtable * btree_dtable::iter::source() const
{
	return base_iter->source();
}

dtable::iter * btree_dtable::iterator() const
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

blob btree_dtable::lookup(const dtype & key, bool * found) const
{
	/* XXX use the btree here */
	return base->lookup(key, found);
}

blob btree_dtable::index(size_t index) const
{
	return base->index(index);
}

size_t btree_dtable::size() const
{
	return base->size();
}

int btree_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	int bt_dfd;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -EINVAL;
	if(!factory->indexed_access())
		return -ENOSYS;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	bt_dfd = openat(dfd, file, 0);
	if(bt_dfd < 0)
		return bt_dfd;
	base = factory->open(bt_dfd, "base", base_config);
	if(!base)
	{
		close(bt_dfd);
		return -1;
	}
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	/* XXX open the btree here */
	close(bt_dfd);
	return 0;
}

void btree_dtable::deinit()
{
	if(base)
	{
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

int btree_dtable::create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow)
{
	int bt_dfd, r;
	params base_config;
	dtable * base_dtable;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -EINVAL;
	if(!base->indexed_access())
		return -ENOSYS;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	bt_dfd = openat(dfd, file, 0);
	if(bt_dfd < 0)
		goto fail_open;
	
	r = base->create(bt_dfd, "base", base_config, source, shadow);
	if(r < 0)
		goto fail_create;
	
	base_dtable = base->open(bt_dfd, "base", base_config);
	if(!base_dtable)
		goto fail_reopen;
	/* XXX make the btree from base_dtable */
	delete base_dtable;
	
	close(bt_dfd);
	return 0;
	
fail_reopen:
	util::rm_r(bt_dfd, "base");
fail_create:
	close(bt_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_RO_FACTORY(btree_dtable);
