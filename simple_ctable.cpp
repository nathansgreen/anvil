/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "sub_blob.h"
#include "dtable_factory.h"
#include "simple_ctable.h"

simple_ctable::iter::iter(dtable::iter * src)
	: source(src), columns(NULL)
{
	advance();
}

simple_ctable::iter::iter(const blob & value)
	: source(NULL), columns(NULL)
{
	if(value.exists())
	{
		row = sub_blob(value);
		columns = row.iterator();
	}
}

bool simple_ctable::iter::valid() const
{
	return columns ? columns->valid() : false;
}

bool simple_ctable::iter::next()
{
	if(columns)
	{
		if(columns->next())
			return true;
		delete columns;
		columns = NULL;
	}
	if(!source)
		return false;
	for(;;)
	{
		if(!source->next())
			return false;
		blob value = source->value();
		if(!value.exists())
			continue;
		row = sub_blob(value);
		columns = row.iterator();
		if(columns->valid())
			return true;
		delete columns;
		columns = NULL;
	}
}

bool simple_ctable::iter::prev()
{
	if(columns)
	{
		if(columns->prev())
			return true;
		delete columns;
		columns = NULL;
	}
	if(!source)
		return false;
	for(;;)
	{
		if(!source->prev())
			return false;
		blob value = source->value();
		if(!value.exists())
			continue;
		row = sub_blob(value);
		columns = row.iterator();
		if(!columns)
			return false;
		columns->last();
		if(columns->valid())
			return true;
		delete columns;
		columns = NULL;
	}
}

void simple_ctable::iter::advance()
{
	while(source->valid())
	{
		blob value = source->value();
		if(value.exists())
		{
			row = sub_blob(value);
			columns = row.iterator();
			if(columns->valid())
				break;
			delete columns;
			columns = NULL;
		}
		source->next();
	}
}

bool simple_ctable::iter::first()
{
	if(!source)
		return false;
	if(columns)
	{
		delete columns;
		columns = NULL;
	}
	if(!source->first())
		return false;
	advance();
	return columns ? columns->valid() : false;
}

bool simple_ctable::iter::last()
{
	if(!source)
		return false;
	if(columns)
	{
		delete columns;
		columns = NULL;
	}
	if(!source->last())
		return false;
	blob value = source->value();
	row = sub_blob(value);
	columns = row.iterator();
	if(!columns)
		return false;
	columns->last();
	if(columns->valid())
		return true;
	delete columns;
	columns = NULL;
	return false;
}

dtype simple_ctable::iter::key() const
{
	return source->key();
}

bool simple_ctable::iter::seek(const dtype & key)
{
	bool found;
	if(columns)
	{
		delete columns;
		columns = NULL;
	}
	found = source->seek(key);
	advance();
	return found;
}

bool simple_ctable::iter::seek(const dtype_test & test)
{
	bool found;
	if(columns)
	{
		delete columns;
		columns = NULL;
	}
	found = source->seek(test);
	advance();
	return found;
}

dtype::ctype simple_ctable::iter::key_type() const
{
	return source->key_type();
}

const istr & simple_ctable::iter::column() const
{
	return columns->column();
}

blob simple_ctable::iter::value() const
{
	return columns->value();
}

dtable::key_iter * simple_ctable::keys() const
{
	return base->iterator();
}

ctable::iter * simple_ctable::iterator() const
{
	return new iter(base->iterator());
}

ctable::iter * simple_ctable::iterator(const dtype & key) const
{
	return new iter(base->find(key));
}

blob simple_ctable::find(const dtype & key, const istr & column) const
{
	blob row = base->find(key);
	if(!row.exists())
		return row;
	/* not super efficient, but we can fix it later */
	sub_blob columns(row);
	return columns.get(column);
}

blob simple_ctable::find(const dtype & key, size_t column) const
{
	/* XXX */
	abort();
}

bool simple_ctable::contains(const dtype & key) const
{
	return base->find(key).exists();
}

/* if we made a better find(), this could avoid flattening every time */
int simple_ctable::insert(const dtype & key, const istr & column, const blob & value, bool append)
{
	int r = 0;
	/* TODO: improve this... it is probably killing us */
	blob row = base->find(key);
	if(row.exists() || value.exists())
	{
		sub_blob columns(row);
		columns.set(column, value);
		r = base->insert(key, columns.flatten(), append);
	}
	return r;
}

int simple_ctable::insert(const dtype & key, size_t column, const blob & value, bool append)
{
	/* XXX */
	abort();
}

int simple_ctable::remove(const dtype & key, const istr & column)
{
	int r = insert(key, column, blob());
	if(r >= 0)
	{
		/* TODO: improve this... it is probably killing us */
		blob row = base->find(key);
		sub_blob columns(row);
		sub_blob::iter * iter = columns.iterator();
		bool last = !iter->valid();
		delete iter;
		if(last)
			remove(key);
	}
	return r;
}

int simple_ctable::remove(const dtype & key, size_t column)
{
	/* XXX */
	abort();
}

int simple_ctable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	int ct_dfd;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	ct_dfd = openat(dfd, file, 0);
	if(ct_dfd < 0)
		return ct_dfd;
	base = factory->open(ct_dfd, "base", base_config);
	close(ct_dfd);
	if(!base)
		return -1;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	return 0;
}

void simple_ctable::deinit()
{
	if(base)
	{
		delete base;
		base = NULL;
		ctable::deinit();
	}
}

int simple_ctable::create(int dfd, const char * file, const params & config, dtype::ctype key_type)
{
	int ct_dfd, r;
	params base_config;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	ct_dfd = openat(dfd, file, 0);
	if(ct_dfd < 0)
		goto fail_open;
	
	r = base->create(ct_dfd, "base", base_config, key_type);
	if(r < 0)
		goto fail_create;
	close(ct_dfd);
	return 0;
	
fail_create:
	close(ct_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_CT_FACTORY(simple_ctable);
