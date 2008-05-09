/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>
#include <fcntl.h>

#include "openat.h"

#include "simple_stable.h"

bool simple_stable::citer::valid() const
{
	return meta != end;
}

bool simple_stable::citer::next()
{
	if(meta != end)
		meta++;
	return meta != end;
}

const char * simple_stable::citer::name() const
{
	return meta->first;
}

size_t simple_stable::citer::row_count() const
{
	return meta->second.row_count;
}

dtype::ctype simple_stable::citer::type() const
{
	return meta->second.type;
}

bool simple_stable::siter::valid() const
{
	return data->valid();
}

bool simple_stable::siter::next()
{
	return data->next();
}

dtype simple_stable::siter::key() const
{
	return data->key();
}

const char * simple_stable::siter::column() const
{
	return data->column();
}

dtype simple_stable::siter::value() const
{
	return dtype(data->value(), meta->col_type(data->column()));
}

stable::column_iter * simple_stable::columns() const
{
	return new citer(column_map.begin(), column_map.end());
}

size_t simple_stable::column_count() const
{
	return column_map.size();
}

size_t simple_stable::row_count(const char * column) const
{
	const column_info * c = get_column(column);
	return c ? c->row_count : 0;
}

dtype::ctype simple_stable::col_type(const char * column) const
{
	const column_info * c = get_column(column);
	assert(c);
	return c->type;
}

stable::iter * simple_stable::iterator() const
{
	stable::iter * wrapper;
	ctable::iter * source = ct_data->iterator();
	if(!source)
		return NULL;
	wrapper = new siter(source, this);
	if(!wrapper)
		delete source;
	return wrapper;
}

stable::iter * simple_stable::iterator(dtype key) const
{
	stable::iter * wrapper;
	ctable::iter * source = ct_data->iterator(key);
	if(!source)
		return NULL;
	wrapper = new siter(source, this);
	if(!wrapper)
		delete source;
	return wrapper;
}

bool simple_stable::find(dtype key, const char * column, dtype * value) const
{
	const column_info * c = get_column(column);
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

int simple_stable::load_columns()
{
	dtable::iter * source = dt_meta->iterator();
	assert(column_map.empty());
	if(!source)
		return -ENOMEM;
	while(source->valid())
	{
		dtype key = source->key();
		assert(key.type == dtype::STRING);
		/* skip internal entries */
		if(key.str[0] == '_')
		{
			source->next();
			continue;
		}
		
		blob value = source->value();
		const char * column = strdup(key.str);
		if(!column)
			break;
		column_info * c = &column_map[column];
		c->row_count = value.index<size_t>(0);
		switch(value[sizeof(size_t)])
		{
			case 1:
				c->type = dtype::UINT32;
				break;
			case 2:
				c->type = dtype::DOUBLE;
				break;
			case 3:
				c->type = dtype::STRING;
				break;
		}
		source->next();
	}
	if(source->valid())
		while(!column_map.empty())
		{
			column_map_full_iter it = column_map.begin();
			const char * column = it->first;
			column_map.erase(column);
			free((void *) column);
		}
	delete source;
	return 0;
}

const simple_stable::column_info * simple_stable::get_column(const char * column) const
{
	column_map_iter it = column_map.find(column);
	if(it == column_map.end())
		return NULL;
	return &it->second;
}

int simple_stable::adjust_column(const char * column, ssize_t delta, dtype::ctype type)
{
	int r;
	bool created = false, destroyed = false;
	column_map_full_iter it = column_map.find(column);
	column_info * c = (it == column_map.end()) ? NULL : &it->second;
	/* refuse internal entries */
	if(column[0] == '_')
		return -EINVAL;
	if(!delta)
		/* do we care about this type-checking side effect? */
		return c ? ((type == c->type) ? 0 : -EINVAL) : 0;
	if(!c)
	{
		/* decrement a nonexistent column? */
		if(delta < 0)
			return -EINVAL;
		column = strdup(column);
		if(!column)
			return -ENOMEM;
		c = &column_map[column];
		c->row_count = delta;
		c->type = type;
		created = true;
	}
	else
	{
		/* type mismatch */
		if(type != c->type)
			return -EINVAL;
		/* decrement more than possible? */
		if(delta < 0 && c->row_count < (size_t) -delta)
			return -EINVAL;
		c->row_count += delta;
		if(!c->row_count)
		{
			assert(delta < 0);
			column = it->first;
			column_map.erase(column);
			destroyed = true;
		}
	}
	/* create the column meta blob */
	blob meta(sizeof(size_t) + 1);
	*(size_t *) meta.memory() = c->row_count;
	switch(type)
	{
		case dtype::UINT32:
			meta.memory()[sizeof(size_t)] = 1;
			break;
		case dtype::DOUBLE:
			meta.memory()[sizeof(size_t)] = 2;
			break;
		case dtype::STRING:
			meta.memory()[sizeof(size_t)] = 3;
			break;
	}
	/* and write it */
	r = dt_meta->append(column, meta);
	if(r < 0)
	{
		/* clean up in case of error */
		if(created)
		{
			column_map.erase(column);
			free((void *) column);
		}
		else if(destroyed)
		{
			c = &column_map[column];
			c->row_count = -delta;
			c->type = type;
		}
	}
	return r;
}

int simple_stable::append(dtype key, const char * column, const dtype & value)
{
	int r;
	bool increment = !ct_data->find(key, column).exists();
	if(increment)
	{
		/* this will check that the type matches */
		r = adjust_column(column, 1, value.type);
		if(r < 0)
			return r;
	}
	else if(col_type(column) != value.type)
		return -EINVAL;
	r = ct_data->append(key, column, value.flatten());
	if(r < 0 && increment)
		adjust_column(column, -1, value.type);
	return r;
}

int simple_stable::remove(dtype key, const char * column)
{
	int r;
	dtype::ctype type;
	const column_info * c = get_column(column);
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
	ctable::iter * columns = ct_data->iterator(key);
	if(!columns)
		return 0;
	while(columns->valid())
	{
		const column_info * c = get_column(columns->column());
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

dtype::ctype simple_stable::key_type() const
{
	return ct_data->key_type();
}

int simple_stable::init(int dfd, const char * name, dtable_factory * meta, dtable_factory * data, ctable_factory * columns)
{
	int r = -1;
	if(md_dfd >= 0)
		deinit();
	assert(column_map.empty());
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
	r = load_columns();
	if(r < 0)
		goto fail_check;
	
	columns->release();
	data->release();
	meta->release();
	return 0;
	
fail_check:
	delete ct_data;
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
	return r;
}

void simple_stable::deinit()
{
	if(md_dfd < 0)
		return;
	while(!column_map.empty())
	{
		column_map_full_iter it = column_map.begin();
		const char * column = it->first;
		column_map.erase(column);
		free((void *) column);
	}
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
