/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>
#include <fcntl.h>

#include "openat.h"

#include "simple_stable.h"

inline simple_stable::citer::citer(dtable_iter * source)
	: key(0u), meta(source)
{
	while(meta->valid())
	{
		key = meta->key();
		assert(key.type == dtype::STRING);
		/* skip internal entries */
		if(key.str[0] == '_')
		{
			meta->next();
			continue;
		}
		value = meta->value();
		break;
	}
}

bool simple_stable::citer::valid() const
{
	return meta->valid();
}

bool simple_stable::citer::next()
{
	meta->next();
	while(meta->valid())
	{
		key = meta->key();
		assert(key.type == dtype::STRING);
		/* skip internal entries */
		if(key.str[0] == '_')
		{
			meta->next();
			continue;
		}
		value = meta->value();
		/* row count + type + column name*/
		assert(value.size() >= sizeof(size_t) + 2);
		break;
	}
	return meta->valid();
}

const char * simple_stable::citer::name() const
{
	return key.str;
}

size_t simple_stable::citer::row_count() const
{
	return *(size_t *) &value[0];
}

dtype::ctype simple_stable::citer::type() const
{
	switch(value[4])
	{
		case 1:
			return dtype::UINT32;
		case 2:
			return dtype::DOUBLE;
		case 3:
			return dtype::STRING;
	}
	abort();
}

inline simple_stable::siter::siter(ctable_iter * source)
	: data(source)
{
	/* XXX */
}

bool simple_stable::siter::valid() const
{
	/* XXX */
	return false;
}

bool simple_stable::siter::next()
{
	/* XXX */
	return false;
}

dtype simple_stable::siter::key() const
{
	/* XXX */
	return dtype(0u);
}

const char * simple_stable::siter::column() const
{
	/* XXX */
	return NULL;
}

dtype simple_stable::siter::value() const
{
	/* XXX */
	return dtype(0u);
}

column_iter * simple_stable::columns() const
{
	column_iter * columns;
	dtable_iter * source = dt_meta->iterator();
	if(!source)
		return NULL;
	columns = new citer(source);
	if(!columns)
		delete source;
	return columns;
}

stable_iter * simple_stable::iterator() const
{
	stable_iter * wrapper;
	ctable_iter * source = ct_data->iterator();
	if(!source)
		return NULL;
	wrapper = new siter(source);
	if(!wrapper)
		delete source;
	return wrapper;
}

stable_iter * simple_stable::iterator(dtype key) const
{
	stable_iter * wrapper;
	ctable_iter * source = ct_data->iterator(key);
	if(!source)
		return NULL;
	wrapper = new siter(source);
	if(!wrapper)
		delete source;
	return wrapper;
}

bool simple_stable::find(dtype key, const char * column, dtype * value) const
{
	const struct column * c = get_column(column);
	if(!c)
		return false;
	blob v = ct_data->find(key, column);
	if(!v.exists())
		return false;
	*value = dtype(v, c->type);
	return true;
}

bool simple_stable::writable() const
{
	return dt_meta->writable() && ct_data->writable();
}

const simple_stable::column * simple_stable::get_column(const char * column) const
{
	/* XXX */
	return NULL;
}

int simple_stable::adjust_column(const char * column, ssize_t delta, dtype::ctype type)
{
	/* refuse internal entries */
	if(column[0] == '_')
		return -EINVAL;
	/* XXX not finished yet */
	return -ENOSYS;
}

int simple_stable::append(dtype key, const char * column, const dtype & value)
{
	int r;
	bool increment = !ct_data->find(key, column).exists();
	if(increment)
	{
		r = adjust_column(column, 1, value.type);
		if(r < 0)
			return r;
	}
	r = ct_data->append(key, column, value.flatten());
	if(r < 0 && increment)
		adjust_column(column, -1, value.type);
	return r;
}

int simple_stable::remove(dtype key, const char * column)
{
	int r;
	dtype::ctype type;
	const struct column * c = get_column(column);
	/* does it even exist to begin with? */
	if(!c || !ct_data->find(key, column).exists())
		return 0;
	type = c->type;
	r = adjust_column(column, -1, type);
	if(r < 0)
		return r;
	r = ct_data->remove(key, column);
	if(r < 0)
		adjust_column(column, 1, type);
	return r;
}

int simple_stable::remove(dtype key)
{
	int r;
	ctable_iter * columns = ct_data->iterator(key);
	if(!columns)
		return 0;
	while(columns->valid())
	{
		const column * c = get_column(columns->column());
		r = adjust_column(columns->column(), -1, c->type);
		/* XXX improve this */
		assert(r >= 0);
		columns->next();
	}
	delete columns;
	r = ct_data->remove(key);
	/* XXX improve this */
	assert(r >= 0);
	return r;
}

int simple_stable::init(int dfd, const char * name, dtable_factory * meta, dtable_factory * data, ctable_factory * columns)
{
	if(md_dfd >= 0)
		deinit();
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
		goto fail_open;
	dt_meta = meta->open(md_dfd, "st_meta");
	if(!dt_meta)
		goto fail_meta;
	_dt_data = data->open(md_dfd, "st_data");
	if(!_dt_data)
		goto fail_data;
	ct_data = columns->open(_dt_data);
	if(!ct_data)
		goto fail_columns;
	
	/* check sanity? */
	
	columns->release();
	data->release();
	meta->release();
	return 0;
	
/*fail_check:
	delete ct_data;*/
fail_columns:
	delete _dt_data;
fail_data:
	delete dt_meta;
fail_meta:
	close(md_dfd);
	md_dfd = -1;
fail_open:
	columns->release();
	data->release();
	meta->release();
	return -1;
}

void simple_stable::deinit()
{
	if(md_dfd < 0)
		return;
	delete ct_data;
	delete _dt_data;
	delete dt_meta;
	ct_data = NULL;
	_dt_data = NULL;
	dt_meta = NULL;
	close(md_dfd);
	md_dfd = -1;
}

int simple_stable::create(int dfd, const char * name, dtable_factory * meta_factory, dtable_factory * data_factory, dtype::ctype key_type)
{
	int md_dfd, r = mkdirat(dfd, name, 0755);
	if(r < 0)
		goto fail_mkdir;
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
	{
		r = md_dfd;
		goto fail_open;
	}
	
	/* the metadata is keyed by named properties (strings) */
	r = meta_factory->create(md_dfd, "st_meta", dtype::STRING);
	if(r < 0)
		goto fail_meta;
	r = data_factory->create(md_dfd, "st_data", key_type);
	if(r < 0)
		goto fail_data;
	
	close(md_dfd);
	data_factory->release();
	meta_factory->release();
	return 0;
	
fail_data:
	/* kill st_meta */
	abort();
fail_meta:
	close(md_dfd);
fail_open:
	unlinkat(dfd, name, AT_REMOVEDIR);
fail_mkdir:
	data_factory->release();
	meta_factory->release();
	return r;
}
