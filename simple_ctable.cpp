/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <set>

#include "openat.h"

#include "rwfile.h"
#include "rofile.h"
#include "index_blob.h"
#include "dtable_factory.h"
#include "dtable_skip_iter.h"
#include "simple_ctable.h"

/* FIXME: there are probably some bugs/inconsistencies with nonexistent values */

simple_ctable::iter::iter(const simple_ctable * base, dtable::iter * source)
	: base(base), source(source), number(0)
{
	advance();
}

bool simple_ctable::iter::valid() const
{
	return number < base->column_count;
}

bool simple_ctable::iter::next(bool row)
{
	if(row)
		number = base->column_count;
	else if(next_column())
		return true;
	return advance(true);
}

bool simple_ctable::iter::prev(bool row)
{
	if(row)
		number = 0;
	else if(prev_column())
		return true;
	return retreat(true);
}

bool simple_ctable::iter::next_column(bool reset)
{
	if(reset)
		number = (size_t) -1;
	else if(number >= base->column_count)
		return false;
	while(++number < base->column_count)
		if(row.get(number).exists())
			return true;
	return false;
}

bool simple_ctable::iter::prev_column(bool reset)
{
	if(reset)
		number = base->column_count;
	else if(number >= base->column_count)
		return false;
	while(number)
		if(row.get(--number).exists())
			return true;
	return false;
}

bool simple_ctable::iter::advance(bool initial)
{
	for(;;)
	{
		if(!next_row(initial))
			return false;
		if(next_column(true))
			return true;
		if(!initial)
			source->next();
	}
}

bool simple_ctable::iter::retreat(bool initial)
{
	for(;;)
	{
		if(!prev_row(initial))
			break;
		if(prev_column(true))
			return true;
		if(!initial && !source->prev())
			break;
	}
	/* need to go back forward to where we were */
	advance();
	return false;
}

bool simple_ctable::iter::next_row(bool initial)
{
	bool valid;
	if(initial && !source->next())
		return false;
	valid = source->valid();
	while(valid && !source->meta().exists())
		valid = source->next();
	if(!valid)
	{
		row = index_blob();
		number = base->column_count;
	}
	else
		/* we'll set number later, in a call to next_column() */
		row = index_blob(base->column_count, source->value());
	return valid;
}

bool simple_ctable::iter::prev_row(bool initial)
{
	bool valid;
	if(initial && !source->prev())
		return false;
	valid = source->valid();
	while(valid && !source->meta().exists())
		valid = source->prev();
	if(!valid)
	{
		row = index_blob();
		number = base->column_count;
	}
	else
		/* we'll set number later, in a call to next_column() */
		row = index_blob(base->column_count, source->value());
	return valid;
}

bool simple_ctable::iter::first()
{
	source->first();
	return advance();
}

bool simple_ctable::iter::last()
{
	source->last();
	return retreat();
}

dtype simple_ctable::iter::key() const
{
	return source->key();
}

bool simple_ctable::iter::seek(const dtype & key)
{
	/* bug! we might advance past the key */
	source->seek(key);
	return advance();
}

bool simple_ctable::iter::seek(const dtype_test & test)
{
	/* bug! we might advance past the key */
	source->seek(test);
	return advance();
}

dtype::ctype simple_ctable::iter::key_type() const
{
	assert(source);
	return source->key_type();
}

size_t simple_ctable::iter::column() const
{
	assert(number < base->column_count);
	return number;
}

const istr & simple_ctable::iter::name() const
{
	assert(number < base->column_count);
	return base->column_name[number];
}

blob simple_ctable::iter::value() const
{
	assert(number < base->column_count);
	return row.get(number);
}

blob simple_ctable::iter::index(size_t column) const
{
	assert(column < base->column_count);
	return row.get(column);
}

simple_ctable::p_iter::p_iter(const simple_ctable * base, dtable::iter * source)
	: base(base), source(source)
{
	source = wrap_and_claim<dtable_skip_iter>(source);
	if(source->valid())
		row = index_blob(base->column_count, source->value());
}

bool simple_ctable::p_iter::valid() const
{
	return source->valid();
}

bool simple_ctable::p_iter::next()
{
	if(source->next())
	{
		row = index_blob(base->column_count, source->value());
		return true;
	}
	row = index_blob();
	return false;
}

bool simple_ctable::p_iter::prev()
{
	if(source->prev())
	{
		row = index_blob(base->column_count, source->value());
		return true;
	}
	return false;
}

bool simple_ctable::p_iter::first()
{
	if(source->first())
	{
		row = index_blob(base->column_count, source->value());
		return true;
	}
	row = index_blob();
	return false;
}

bool simple_ctable::p_iter::last()
{
	if(source->last())
	{
		row = index_blob(base->column_count, source->value());
		return true;
	}
	row = index_blob();
	return false;
}

dtype simple_ctable::p_iter::key() const
{
	return source->key();
}

bool simple_ctable::p_iter::seek(const dtype & key)
{
	bool found = source->seek(key);
	if(found || source->valid())
		row = index_blob(base->column_count, source->value());
	else
		row = index_blob();
	return found;
}

bool simple_ctable::p_iter::seek(const dtype_test & test)
{
	bool found = source->seek(test);
	if(found || source->valid())
		row = index_blob(base->column_count, source->value());
	else
		row = index_blob();
	return found;
}

dtype::ctype simple_ctable::p_iter::key_type() const
{
	assert(source);
	return source->key_type();
}

blob simple_ctable::p_iter::value(size_t column) const
{
	assert(column < base->column_count);
	return row.get(column);
}

dtable::key_iter * simple_ctable::keys() const
{
	return base->iterator();
}

ctable::iter * simple_ctable::iterator() const
{
	return new iter(this, base->iterator());
}

ctable::p_iter * simple_ctable::iterator(const size_t * columns, size_t count) const
{
	/* we ignore the columns and provide them all anyway; it's no more expensive */
	return new p_iter(this, base->iterator());
}

blob simple_ctable::find(const dtype & key, size_t column) const
{
	assert(column < column_count);
	blob row = base->find(key);
	if(!row.exists())
		return row;
	/* not super efficient, but we can fix it later */
	index_blob sub(column_count, row);
	return sub.get(column);
}

int simple_ctable::find(const dtype & key, colval * values, size_t count) const
{
	blob row = base->find(key);
	if(!row.exists())
	{
		for(size_t i = 0; i < count; i++)
			values[i].value = blob();
		return 0;
	}
	/* not super efficient, but we can fix it later */
	index_blob sub(column_count, row);
	for(size_t i = 0; i < count; i++)
	{
		assert(values[i].index < column_count);
		values[i].value = sub.get(values[i].index);
	}
	return 0;
}

bool simple_ctable::contains(const dtype & key) const
{
	return base->find(key).exists();
}

/* if we made a better find(), this could avoid flattening every time */
int simple_ctable::insert(const dtype & key, size_t column, const blob & value, bool append)
{
	int r = 0;
	assert(column < column_count);
	blob row = base->find(key);
	if(row.exists() || value.exists())
	{
		/* TODO: improve this... it is probably killing us */
		index_blob sub(column_count, row);
		sub.set(column, value);
		r = base->insert(key, sub.flatten(), append);
	}
	return r;
}

int simple_ctable::insert(const dtype & key, const colval * values, size_t count, bool append)
{
	int r = 0;
	bool exist = false;
	blob row = base->find(key);
	for(size_t i = 0; i < count; i++)
		if(values[i].value.exists())
		{
			exist = true;
			break;
		}
	if(row.exists() || exist)
	{
		/* TODO: improve this... it is probably killing us */
		index_blob sub(column_count, row);
		for(size_t i = 0; i < count; i++)
		{
			assert(values[i].index < column_count);
			sub.set(values[i].index, values[i].value);
		}
		r = base->insert(key, sub.flatten(), append);
	}
	return r;
}

int simple_ctable::remove(const dtype & key, size_t column)
{
	int r = insert(key, column, blob());
	if(r >= 0)
	{
		bool exist = false;
		blob row = base->find(key);
		/* TODO: improve this... it is probably killing us */
		index_blob sub(column_count, row);
		for(size_t i = 0; i < column_count; i++)
			if(sub.get(i).exists())
			{
				exist = true;
				break;
			}
		if(!exist)
			remove(key);
	}
	return r;
}

int simple_ctable::remove(const dtype & key, size_t * indices, size_t count)
{
	colval erase[count];
	for(size_t i = 0; i < count; i++)
	{
		erase[i].index = indices[i];
		erase[i].value = blob();
	}
	int r = insert(key, erase, count);
	if(r >= 0)
	{
		bool exist = false;
		blob row = base->find(key);
		/* TODO: improve this... it is probably killing us */
		index_blob sub(column_count, row);
		for(size_t i = 0; i < column_count; i++)
			if(sub.get(i).exists())
			{
				exist = true;
				break;
			}
		if(!exist)
			remove(key);
	}
	return r;
}

int simple_ctable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	const dtable_factory * factory;
	params base_config;
	int ct_dfd, r;
	
	off_t offset;
	ctable_header meta;
	rofile * meta_file;
	
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	ct_dfd = openat(dfd, file, O_RDONLY);
	if(ct_dfd < 0)
		return ct_dfd;
	
	meta_file = rofile::open<4, 2>(ct_dfd, "sct_meta");
	if(!meta_file)
		goto fail_open;
	r = meta_file->read_type(0, &meta);
	if(r < 0)
		goto fail_header;
	if(meta.magic != SIMPLE_CTABLE_MAGIC || meta.version != SIMPLE_CTABLE_VERSION)
		goto fail_header;
	column_count = meta.columns;
	
	column_name = new istr[column_count];
	if(!column_name)
		goto fail_header;
	
	offset = sizeof(meta);
	for(size_t i = 0; i < column_count; i++)
	{
		uint32_t length;
		r = meta_file->read_type(offset, &length);
		if(r < 0)
			goto fail_names;
		offset += sizeof(length);
		column_name[i] = meta_file->read_string(offset, length);
		if(!column_name[i])
			goto fail_names;
		offset += length;
		column_map[column_name[i]] = i;
	}
	
	base = factory->open(ct_dfd, "base", base_config, sysj);
	if(!base)
		goto fail_names;
	ktype = base->key_type();
	cmp_name = base->get_cmp_name();
	
	delete meta_file;
	close(ct_dfd);
	return 0;
	
fail_names:
	column_map.empty();
	delete[] column_name;
fail_header:
	delete meta_file;
fail_open:
	close(ct_dfd);
	return -1;
}

void simple_ctable::deinit()
{
	if(base)
	{
		delete[] column_name;
		column_map.empty();
		column_count = 0;
		base->destroy();
		base = NULL;
		ctable::deinit();
	}
}

int simple_ctable::create(int dfd, const char * file, const params & config, dtype::ctype key_type)
{
	int ct_dfd, columns, r;
	params base_config;
	std::set<istr, strcmp_less> names;
	
	ctable_header meta;
	rwfile meta_file;
	
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	
	if(!config.get("columns", &columns, 0))
		return -EINVAL;
	if(columns < 1)
		return -EINVAL;
	
	/* check that we have all the names */
	for(int i = 0; i < columns; i++)
	{
		char string[32];
		istr column_name;
		sprintf(string, "column%d_name", i);
		if(!config.get(string, &column_name) || !column_name)
			return -EINVAL;
		if(names.count(column_name))
			return -EEXIST;
		names.insert(column_name);
	}
	names.clear();
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	ct_dfd = openat(dfd, file, O_RDONLY);
	if(ct_dfd < 0)
		goto fail_open;
	
	meta.magic = SIMPLE_CTABLE_MAGIC;
	meta.version = SIMPLE_CTABLE_VERSION;
	meta.columns = columns;
	r = meta_file.create(ct_dfd, "sct_meta");
	if(r < 0)
		goto fail_meta;
	r = meta_file.append(&meta);
	if(r < 0)
		goto fail_create;
	
	/* record column names */
	for(int i = 0; i < columns; i++)
	{
		uint32_t length;
		char string[32];
		istr column_name;
		sprintf(string, "column%d_name", i);
		r = config.get(string, &column_name);
		assert(r && column_name);
		length = column_name.length();
		r = meta_file.append(&length);
		if(r < 0)
			goto fail_create;
		r = meta_file.append(column_name);
		if(r < 0)
			goto fail_create;
	}
	
	r = base->create(ct_dfd, "base", base_config, key_type);
	if(r < 0)
		goto fail_create;
	
	meta_file.close();
	close(ct_dfd);
	return 0;
	
fail_create:
	meta_file.close();
	unlinkat(dfd, "sct_meta", 0);
fail_meta:
	close(ct_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_CT_FACTORY(simple_ctable);
