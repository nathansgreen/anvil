/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <unistd.h>
#include <fcntl.h>

#include "openat.h"

#include "schema_table.h"

inline schema_table::iter::iter(dtable_iter * source)
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

bool schema_table::iter::valid() const
{
	return meta->valid();
}

bool schema_table::iter::next()
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

const char * schema_table::iter::name() const
{
	return key.str;
}

size_t schema_table::iter::row_count() const
{
	return *(size_t *) &value[0];
}

dtype::ctype schema_table::iter::type() const
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

ctable_iter * schema_table::iterator() const
{
	return ct_data->iterator();
}

ctable_iter * schema_table::iterator(dtype key) const
{
	return ct_data->iterator(key);
}

blob schema_table::find(dtype key, const char * column) const
{
	return ct_data->find(key, column);
}

bool schema_table::writable() const
{
	return dt_meta->writable() && ct_data->writable();
}

int schema_table::adjust_column(const char * column, ssize_t delta)
{
	/* refuse internal entries */
	if(column[0] == '_')
		return -EINVAL;
	/* XXX not finished yet */
	return -ENOSYS;
}

int schema_table::append(dtype key, const char * column, const blob & value)
{
	int r;
	bool increment = !ct_data->find(key, column).exists();
	if(increment)
	{
		r = adjust_column(column, 1);
		if(r < 0)
			return r;
	}
	r = ct_data->append(key, column, value);
	if(r < 0 && increment)
		adjust_column(column, -1);
	return r;
}

int schema_table::remove(dtype key, const char * column)
{
	int r;
	/* does it even exist to begin with? */
	if(!ct_data->find(key, column).exists())
		return 0;
	r = adjust_column(column, -1);
	if(r < 0)
		return r;
	r = ct_data->remove(key, column);
	if(r < 0)
		adjust_column(column, 1);
	return r;
}

int schema_table::remove(dtype key)
{
	int r;
	ctable_iter * columns = ct_data->iterator(key);
	if(columns)
	{
		while(columns->valid())
		{
			r = adjust_column(columns->column(), -1);
			/* XXX improve this */
			assert(r >= 0);
			columns->next();
		}
		delete columns;
	}
	r = ct_data->remove(key);
	if(r < 0 && columns)
	{
		columns = ct_data->iterator(key);
		while(columns->valid())
		{
			adjust_column(columns->column(), 1);
			columns->next();
		}
		delete columns;
	}
	return r;
}

column_iter * schema_table::columns() const
{
	column_iter * columns;
	dtable_iter * source = dt_meta->iterator();
	if(!source)
		return NULL;
	columns = new iter(source);
	if(!columns)
		delete source;
	return columns;
}

int schema_table::init(int dfd, const char * name, dtable_factory * meta, dtable_factory * data, ctable_factory * columns)
{
	if(md_dfd >= 0)
		deinit();
	md_dfd = openat(dfd, name, 0);
	if(md_dfd < 0)
		goto fail_open;
	dt_meta = meta->open(md_dfd, "st_meta");
	if(!dt_meta)
		goto fail_meta;
	dt_source = data->open(md_dfd, "st_data");
	if(!dt_source)
		goto fail_data;
	ct_data = columns->open(dt_source);
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
	delete dt_source;
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

void schema_table::deinit()
{
	if(md_dfd < 0)
		return;
	delete ct_data;
	delete dt_source;
	delete dt_meta;
	ct_data = NULL;
	dt_source = NULL;
	dt_meta = NULL;
	close(md_dfd);
	md_dfd = -1;
}

int schema_table::create(int dfd, const char * name, dtable_factory * meta_factory, dtable_factory * data_factory, dtype::ctype key_type)
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
