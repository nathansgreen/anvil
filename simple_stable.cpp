/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>
#include <fcntl.h>

#include "openat.h"

#include "blob_buffer.h"
#include "simple_stable.h"

bool simple_stable::citer::valid() const
{
	return meta != columns->end();
}

bool simple_stable::citer::next()
{
	if(meta != columns->end())
		meta++;
	return meta != columns->end();
}

bool simple_stable::citer::prev()
{
	if(meta == columns->begin())
		return false;
	--meta;
	return true;
}

bool simple_stable::citer::first()
{
	meta = columns->begin();
	return meta != columns->end();
}

bool simple_stable::citer::last()
{
	meta = columns->end();
	if(meta == columns->end())
		return false;
	--meta;
	return true;
}

const istr & simple_stable::citer::name() const
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

ext_index * simple_stable::citer::index() const
{
	return meta->second.index;
}

bool simple_stable::siter::valid() const
{
	return data->valid();
}

bool simple_stable::siter::next()
{
	return data->next();
}

bool simple_stable::siter::prev()
{
	return data->prev();
}

bool simple_stable::siter::first()
{
	return data->first();
}

bool simple_stable::siter::last()
{
	return data->last();
}

dtype simple_stable::siter::key() const
{
	return data->key();
}

dtype::ctype simple_stable::siter::key_type() const
{
	return data->key_type();
}

bool simple_stable::siter::seek(const dtype & key)
{
	return data->seek(key);
}

bool simple_stable::siter::seek(const dtype_test & test)
{
	return data->seek(test);
}

const istr & simple_stable::siter::column() const
{
	return data->column();
}

dtype simple_stable::siter::value() const
{
	return dtype(data->value(), meta->column_type(data->column()));
}

stable::column_iter * simple_stable::columns() const
{
	return new citer(column_map.begin(), &column_map);
}

size_t simple_stable::column_count() const
{
	return column_map.size();
}

size_t simple_stable::row_count(const istr & column) const
{
	const column_info * c = get_column(column);
	return c ? c->row_count : 0;
}

dtype::ctype simple_stable::column_type(const istr & column) const
{
	const column_info * c = get_column(column);
	assert(c);
	return c->type;
}

ext_index * simple_stable::column_index(const istr & column) const
{
	const column_info * c = get_column(column);
	return c ? c->index : NULL;
}

int simple_stable::set_column_index(const istr & column, ext_index * index)
{
	column_map_full_iter it = column_map.find(column);
	if(it == column_map.end())
		return -ENOENT;
	it->second.index = index;
	return 0;
}

dtable::key_iter * simple_stable::keys() const
{
	return ct_data->keys();
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

stable::iter * simple_stable::iterator(const dtype & key) const
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

bool simple_stable::find(const dtype & key, const istr & column, dtype * value) const
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

bool simple_stable::contains(const dtype & key) const
{
	return ct_data->contains(key);
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
		source->next();
		/* skip removed columns */
		if(!value.exists())
			continue;
		column_info * c = &column_map[key.str];
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
			case 4:
				c->type = dtype::BLOB;
				break;
		}
	}
	if(source->valid())
		column_map.clear();
	delete source;
	return 0;
}

const simple_stable::column_info * simple_stable::get_column(const istr & column) const
{
	column_map_iter it = column_map.find(column);
	if(it == column_map.end())
		return NULL;
	return &it->second;
}

int simple_stable::adjust_column(const istr & column, ssize_t delta, dtype::ctype type)
{
	int r;
	ext_index * old_index = NULL;
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
		c = &column_map[column];
		c->row_count = delta;
		c->type = type;
		c->index = NULL;
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
			old_index = c->index;
			column_map.erase(column);
			destroyed = true;
		}
	}
	if(!destroyed)
	{
		/* create the column meta blob */
		blob_buffer meta(sizeof(size_t) + 1);
		meta << c->row_count;
		switch(type)
		{
			case dtype::UINT32:
				meta << (uint8_t) 1;
				break;
			case dtype::DOUBLE:
				meta << (uint8_t) 2;
				break;
			case dtype::STRING:
				meta << (uint8_t) 3;
				break;
			case dtype::BLOB:
				meta << (uint8_t) 4;
				break;
		}
		/* and write it */
		r = dt_meta->insert(column, meta);
	}
	else
		r = dt_meta->remove(column);
	if(r < 0)
	{
		/* clean up in case of error */
		if(created)
			column_map.erase(column);
		else if(destroyed)
		{
			c = &column_map[column];
			c->row_count = -delta;
			c->type = type;
			c->index = old_index;
		}
	}
	return r;
}

int simple_stable::insert(const dtype & key, const istr & column, const dtype & value, bool append)
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
	else if(column_type(column) != value.type)
		return -EINVAL;
	r = ct_data->insert(key, column, value.flatten(), append);
	if(r < 0 && increment)
		adjust_column(column, -1, value.type);
	return r;
}

int simple_stable::remove(const dtype & key, const istr & column)
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

int simple_stable::remove(const dtype & key)
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

int simple_stable::init(int dfd, const char * name, const params & config)
{
	int r = -1;
	params meta_config, data_config, columns_config;
	const dtable_factory * meta = dtable_factory::lookup(config, "meta");
	const dtable_factory * data = dtable_factory::lookup(config, "data");
	const ctable_factory * columns = ctable_factory::lookup(config, "columns");
	if(md_dfd >= 0)
		deinit();
	assert(column_map.empty());
	if(!meta || !data || !columns)
		return -EINVAL;
	if(!config.get("meta_config", &meta_config, params()))
		return -EINVAL;
	if(!config.get("data_config", &data_config, params()))
		return -EINVAL;
	if(!config.get("columns_config", &columns_config, params()))
		return -EINVAL;
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
		return md_dfd;
	dt_meta = meta->open(md_dfd, "st_meta", meta_config);
	if(!dt_meta)
		goto fail_meta;
	_dt_data = data->open(md_dfd, "st_data", data_config);
	if(!_dt_data)
		goto fail_data;
	ct_data = columns->open(_dt_data, columns_config);
	if(!ct_data)
		goto fail_columns;
	
	/* check sanity? */
	r = load_columns();
	if(r < 0)
		goto fail_check;
	
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
	return r;
}

void simple_stable::deinit()
{
	if(md_dfd < 0)
		return;
	column_map.clear();
	delete ct_data;
	delete _dt_data;
	delete dt_meta;
	ct_data = NULL;
	_dt_data = NULL;
	dt_meta = NULL;
	close(md_dfd);
	md_dfd = -1;
}

int simple_stable::create(int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	int md_dfd, r;
	params meta_config, data_config;
	const dtable_factory * meta = dtable_factory::lookup(config, "meta");
	const dtable_factory * data = dtable_factory::lookup(config, "data");
	if(!meta || !data)
		return -EINVAL;
	if(!config.get("meta_config", &meta_config, params()))
		return -EINVAL;
	if(!config.get("data_config", &data_config, params()))
		return -EINVAL;
	r = mkdirat(dfd, name, 0755);
	if(r < 0)
		return r;
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
	{
		r = md_dfd;
		goto fail_open;
	}
	
	/* the metadata is keyed by named properties (strings) */
	r = meta->create(md_dfd, "st_meta", meta_config, dtype::STRING);
	if(r < 0)
		goto fail_meta;
	r = data->create(md_dfd, "st_data", data_config, key_type);
	if(r < 0)
		goto fail_data;
	
	close(md_dfd);
	return 0;
	
fail_data:
	/* kill st_meta */
	abort();
fail_meta:
	close(md_dfd);
fail_open:
	unlinkat(dfd, name, AT_REMOVEDIR);
	return r;
}
