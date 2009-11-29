/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "util.h"
#include "sys_journal.h"
#include "dtable_factory.h"
#include "ctable_factory.h"
#include "dtable_cache_iter.h"
#include "ctable_cache_iter.h"
#include "journal_dtable.h"
#include "temp_journal_dtable.h"
#include "anvil.h"

static void rename_gmon_out(void)
{
	struct stat gmon;
	int r = stat("gmon.out", &gmon);
	if(r >= 0)
	{
		char name[sizeof("gmon.out.") + 12];
		for(uint32_t seq = 0; seq != (uint32_t) -1; seq++)
		{
			snprintf(name, sizeof(name), "gmon.out.%u", seq);
			r = stat(name, &gmon);
			if(r < 0)
			{
				fprintf(stderr, "%s() renaming gmon.out to %s\n", __FUNCTION__, name);
				rename("gmon.out", name);
				break;
			}
		}
	}
}

int anvil_init(const char * path)
{
	int r, fd = open(path, O_RDONLY);
	if(fd < 0)
		return fd;
	rename_gmon_out();
	/* make maximum log size 4MB */
	r = tx_init(fd, 4194304);
	if(r >= 0)
	{
		r = tx_start_r();
		if(r >= 0)
		{
			sys_journal * global = sys_journal::get_global_journal();
			r = sys_journal::set_unique_id_file(fd, "sys_journal_id", true);
			if(r >= 0)
				r = global->init(fd, "sys_journal", &journal_dtable::warehouse, &temp_journal_dtable::warehouse, true);
			if(r >= 0)
			{
				/* maybe we should not always do this here? */
				r = global->filter();
				r = tx_end_r();
			}
			else
				tx_end_r();
		}
	}
	close(fd);
	return r;
}

static inline int init_anvil_istr(anvil_istr * c, const istr & value)
{
	anvil_istr_union safer(c);
	/* istr is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int anvil_istr_new(anvil_istr * c, const char * str)
{
	return init_anvil_istr(c, str);
}

int anvil_istr_copy(anvil_istr * c, const anvil_istr * src)
{
	anvil_istr_union_const src_safer(src);
	return init_anvil_istr(c, *src_safer);
}

void anvil_istr_kill(anvil_istr * c)
{
	anvil_istr_union safer(c);
	*safer = istr(NULL);
}

const char * anvil_istr_str(const anvil_istr * c)
{
	anvil_istr_union_const safer(c);
	return safer->str();
}

size_t anvil_istr_length(const anvil_istr * c)
{
	anvil_istr_union_const safer(c);
	return safer->length();
}

static inline int init_anvil_blob(anvil_blob * c, const blob & value)
{
	anvil_blob_union safer(c);
	/* blob is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int anvil_blob_new(anvil_blob * c, size_t size, const void * data)
{
	return init_anvil_blob(c, blob(size, data));
}

int anvil_blob_str(anvil_blob * c, const char * str)
{
	return init_anvil_blob(c, blob(str));
}

int anvil_blob_dne(anvil_blob * c)
{
	return init_anvil_blob(c, blob::dne);
}

int anvil_blob_empty(anvil_blob * c)
{
	return init_anvil_blob(c, blob::empty);
}

int anvil_blob_copy(anvil_blob * c, const anvil_blob * src)
{
	anvil_blob_union_const src_safer(src);
	return init_anvil_blob(c, *src_safer);
}

int anvil_blob_copy_buffer(anvil_blob * c, const anvil_blob_buffer * src)
{
	anvil_blob_buffer_union_const src_safer(src);
	return init_anvil_blob(c, blob(*src_safer));
}

void anvil_blob_kill(anvil_blob * c)
{
	anvil_blob_union safer(c);
	*safer = blob();
}

bool anvil_blob_exists(const anvil_blob * c)
{
	anvil_blob_union_const safer(c);
	return safer->exists();
}

size_t anvil_blob_size(const anvil_blob * c)
{
	anvil_blob_union_const safer(c);
	return safer->size();
}

const void * anvil_blob_data(const anvil_blob * c)
{
	anvil_blob_union_const safer(c);
	return safer->data();
}

int anvil_blob_overwrite(anvil_blob * c, size_t offset, const void * data, size_t length)
{
	anvil_blob_union safer(c);
	blob_buffer buffer(*safer);
	int r = buffer.overwrite(offset, data, length);
	if(r < 0)
		return r;
	*safer = blob(buffer);
	return 0;
}

int anvil_blob_append(anvil_blob * c, const void * data, size_t length)
{
	anvil_blob_union safer(c);
	blob_buffer buffer(*safer);
	int r = buffer.append(data, length);
	if(r < 0)
		return r;
	*safer = blob(buffer);
	return 0;
}

static inline void init_anvil_metablob(anvil_metablob * c, const metablob & value)
{
	c->_size = value.size();
	c->_exists = value.exists();
}

int anvil_metablob_copy(anvil_metablob * c, const anvil_metablob * src)
{
	*c = *src;
	return 0;
}

void anvil_metablob_kill(anvil_metablob * c)
{
	/* nothing to do */
}

bool anvil_metablob_exists(const anvil_metablob * c)
{
	return c->_exists;
}

size_t anvil_metablob_size(const anvil_metablob * c)
{
	return c->_size;
}

static inline int init_anvil_blob_buffer(anvil_blob_buffer * c, const blob_buffer & value)
{
	anvil_blob_buffer_union safer(c);
	/* blob_buffer is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int anvil_blob_buffer_new(anvil_blob_buffer * c)
{
	return init_anvil_blob_buffer(c, blob_buffer());
}

int anvil_blob_buffer_new_capacity(anvil_blob_buffer * c, size_t capacity)
{
	return init_anvil_blob_buffer(c, blob_buffer(capacity));
}

int anvil_blob_buffer_new_data(anvil_blob_buffer * c, size_t size, const void * data)
{
	return init_anvil_blob_buffer(c, blob_buffer(size, data));
}

int anvil_blob_buffer_copy(anvil_blob_buffer * c, const anvil_blob_buffer * src)
{
	anvil_blob_buffer_union_const src_safer(src);
	return init_anvil_blob_buffer(c, *src_safer);
}

int anvil_blob_buffer_copy_blob(anvil_blob_buffer * c, const anvil_blob * src)
{
	anvil_blob_union_const src_safer(src);
	return init_anvil_blob_buffer(c, blob_buffer(*src_safer));
}

void anvil_blob_buffer_kill(anvil_blob_buffer * c)
{
	anvil_blob_buffer_union safer(c);
	*safer = blob_buffer();
}

bool anvil_blob_buffer_exists(const anvil_blob_buffer * c)
{
	anvil_blob_buffer_union_const safer(c);
	return safer->exists();
}

size_t anvil_blob_buffer_size(const anvil_blob_buffer * c)
{
	anvil_blob_buffer_union_const safer(c);
	return safer->size();
}

size_t anvil_blob_buffer_capacity(const anvil_blob_buffer * c)
{
	anvil_blob_buffer_union_const safer(c);
	return safer->capacity();
}

const void * anvil_blob_buffer_data(const anvil_blob_buffer * c)
{
	anvil_blob_buffer_union_const safer(c);
	return safer->data();
}

int anvil_blob_buffer_set_size(anvil_blob_buffer * c, size_t size)
{
	anvil_blob_buffer_union safer(c);
	return safer->set_size(size);
}

int anvil_blob_buffer_set_capacity(anvil_blob_buffer * c, size_t capacity)
{
	anvil_blob_buffer_union safer(c);
	return safer->set_capacity(capacity);
}

int anvil_blob_buffer_overwrite(anvil_blob_buffer * c, size_t offset, const void * data, size_t length)
{
	anvil_blob_buffer_union safer(c);
	return safer->overwrite(offset, data, length);
}

int anvil_blob_buffer_append(anvil_blob_buffer * c, const void * data, size_t length)
{
	anvil_blob_buffer_union safer(c);
	return safer->append(data, length);
}

anvil_params * anvil_params_new(void)
{
	return anvil_params_union(new params);
}

int anvil_params_parse(anvil_params * c, const char * string)
{
	anvil_params_union safer(c);
	return params::parse(string, safer);
}

void anvil_params_kill(anvil_params * c)
{
	anvil_params_union safer(c);
	delete safer.cpp;
}

static inline int init_anvil_dtype(anvil_dtype * c, const dtype & value)
{
	anvil_dtype_union safer(c);
	/* dtype is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int anvil_dtype_int(anvil_dtype * c, uint32_t value)
{
	return init_anvil_dtype(c, dtype(value));
}

int anvil_dtype_dbl(anvil_dtype * c, double value)
{
	return init_anvil_dtype(c, dtype(value));
}

int anvil_dtype_str(anvil_dtype * c, const char * value)
{
	return init_anvil_dtype(c, dtype(value));
}

int anvil_dtype_istr(anvil_dtype * c, const anvil_istr * value)
{
	anvil_istr_union_const value_safer(value);
	return init_anvil_dtype(c, dtype(*value_safer));
}

int anvil_dtype_blb(anvil_dtype * c, const anvil_blob * value)
{
	anvil_blob_union_const value_safer(value);
	return init_anvil_dtype(c, dtype(*value_safer));
}

int anvil_dtype_copy(anvil_dtype * c, const anvil_dtype * src)
{
	anvil_dtype_union_const src_safer(src);
	return init_anvil_dtype(c, *src_safer);
}

void anvil_dtype_kill(anvil_dtype * c)
{
	anvil_dtype_union safer(c);
	*safer = dtype(0u);
}

anvil_dtype_type anvil_dtype_get_type(const anvil_dtype * c)
{
	anvil_dtype_union_const safer(c);
	/* same values; see dtype.h */
	return (anvil_dtype_type) safer->type;
}

int anvil_dtype_get_int(const anvil_dtype * c, uint32_t * value)
{
	anvil_dtype_union_const safer(c);
	assert(safer->type == dtype::UINT32);
	*value = safer->u32;
	return 0;
}

int anvil_dtype_get_dbl(const anvil_dtype * c, double * value)
{
	anvil_dtype_union_const safer(c);
	assert(safer->type == dtype::DOUBLE);
	*value = safer->dbl;
	return 0;
}

int anvil_dtype_get_str(const anvil_dtype * c, anvil_istr * value)
{
	anvil_dtype_union_const safer(c);
	assert(safer->type == dtype::STRING);
	return init_anvil_istr(value, safer->str);
}

int anvil_dtype_get_blb(const anvil_dtype * c, anvil_blob * value)
{
	anvil_dtype_union_const safer(c);
	assert(safer->type == dtype::BLOB);
	return init_anvil_blob(value, safer->blb);
}

int anvil_dtype_compare(const anvil_dtype * a, const anvil_dtype * b)
{
	anvil_dtype_union_const a_safer(a);
	anvil_dtype_union_const b_safer(b);
	return a_safer->compare(*b_safer);
}

int anvil_dtype_compare_blobcmp(const anvil_dtype * a, const anvil_dtype * b, const anvil_blobcmp * cmp)
{
	anvil_dtype_union_const a_safer(a);
	anvil_dtype_union_const b_safer(b);
	return a_safer->compare(*b_safer, cmp);
}

const char * anvil_dtype_type_name(anvil_dtype_type type)
{
	/* same values; see dtype.h */
	return dtype::name((dtype::ctype) type);
}

int anvil_dtable_create(const char * type, int dfd, const char * name, const anvil_params * config, const anvil_dtable * source, const anvil_dtable * shadow)
{
	anvil_params_union_const config_safer(config);
	anvil_dtable_union_const source_safer(source);
	anvil_dtable_union_const shadow_safer(shadow);
	return dtable_factory::setup(type, dfd, name, *config_safer, source_safer, shadow_safer);
}

int anvil_dtable_create_empty(const char * type, int dfd, const char * name, const anvil_params * config, anvil_dtype_type key_type)
{
	anvil_params_union_const config_safer(config);
	/* same values; see dtype.h */
	return dtable_factory::setup(type, dfd, name, *config_safer, (dtype::ctype) key_type);
}

anvil_dtable * anvil_dtable_open(const char * type, int dfd, const char * name, const anvil_params * config)
{
	anvil_params_union_const config_safer(config);
	dtable * cpp = dtable_factory::load(type, dfd, name, *config_safer, sys_journal::get_global_journal());
	return anvil_dtable_union(cpp);
}

void anvil_dtable_kill(anvil_dtable * c)
{
	anvil_dtable_union safer(c);
	safer.cpp->destroy();
}

int anvil_dtable_find(const anvil_dtable * c, const anvil_dtype * key, anvil_blob * value)
{
	anvil_dtable_union_const safer(c);
	anvil_dtype_union_const key_safer(key);
	return init_anvil_blob(value, safer->find(*key_safer));
}

bool anvil_dtable_writable(const anvil_dtable * c)
{
	anvil_dtable_union_const safer(c);
	return safer->writable();
}

int anvil_dtable_insert(anvil_dtable * c, const anvil_dtype * key, const anvil_blob * value, bool append)
{
	anvil_dtable_union safer(c);
	anvil_dtype_union_const key_safer(key);
	anvil_blob_union_const value_safer(value);
	return safer->insert(*key_safer, *value_safer, append);
}

int anvil_dtable_remove(anvil_dtable * c, const anvil_dtype * key)
{
	anvil_dtable_union safer(c);
	anvil_dtype_union_const key_safer(key);
	return safer->remove(*key_safer);
}

anvil_dtype_type anvil_dtable_key_type(const anvil_dtable * c)
{
	anvil_dtable_union_const safer(c);
	/* same values; see dtype.h */
	return (anvil_dtype_type) safer->key_type();
}

int anvil_dtable_set_blob_cmp(anvil_dtable * c, const anvil_blobcmp * cmp)
{
	anvil_dtable_union safer(c);
	return safer->set_blob_cmp(cmp);
}

const char * anvil_dtable_get_cmp_name(const anvil_dtable * c)
{
	anvil_dtable_union_const safer(c);
	return safer->get_cmp_name();
}

int anvil_dtable_maintain(anvil_dtable * c)
{
	anvil_dtable_union safer(c);
	return safer->maintain();
}

anvil_dtable_key_iter * anvil_dtable_keys(const anvil_dtable * c)
{
	anvil_dtable_union_const safer(c);
	dtable::key_iter * iter = safer->iterator();
	dtable::key_iter * cache = iter ? new dtable_cache_key_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return anvil_dtable_key_iter_union(cache);
}

bool anvil_dtable_key_iter_valid(const anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union_const safer(c);
	return safer->valid();
}

bool anvil_dtable_key_iter_next(anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union safer(c);
	return safer->next();
}

bool anvil_dtable_key_iter_prev(anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union safer(c);
	return safer->prev();
}

bool anvil_dtable_key_iter_first(anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union safer(c);
	return safer->first();
}

bool anvil_dtable_key_iter_last(anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union safer(c);
	return safer->last();
}

int anvil_dtable_key_iter_key(const anvil_dtable_key_iter * c, anvil_dtype * key)
{
	anvil_dtable_key_iter_union_const safer(c);
	return init_anvil_dtype(key, safer->key());
}

bool anvil_dtable_key_iter_seek(anvil_dtable_key_iter * c, const anvil_dtype * key)
{
	anvil_dtable_key_iter_union safer(c);
	anvil_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool anvil_dtable_key_iter_seek_test(anvil_dtable_key_iter * c, blob_test test, void * user)
{
	anvil_dtable_key_iter_union safer(c);
	return safer->seek(anvil_dtype_test(test, user));
}

void anvil_dtable_key_iter_kill(anvil_dtable_key_iter * c)
{
	anvil_dtable_key_iter_union safer(c);
	delete safer.cpp;
}

anvil_dtable_iter * anvil_dtable_iterator(const anvil_dtable * c)
{
	anvil_dtable_union_const safer(c);
	return anvil_dtable_iter_union(wrap_and_claim<dtable_cache_iter>(safer->iterator()));
}

bool anvil_dtable_iter_valid(const anvil_dtable_iter * c)
{
	anvil_dtable_iter_union_const safer(c);
	return safer->valid();
}

bool anvil_dtable_iter_next(anvil_dtable_iter * c)
{
	anvil_dtable_iter_union safer(c);
	return safer->next();
}

bool anvil_dtable_iter_prev(anvil_dtable_iter * c)
{
	anvil_dtable_iter_union safer(c);
	return safer->prev();
}

bool anvil_dtable_iter_first(anvil_dtable_iter * c)
{
	anvil_dtable_iter_union safer(c);
	return safer->first();
}

bool anvil_dtable_iter_last(anvil_dtable_iter * c)
{
	anvil_dtable_iter_union safer(c);
	return safer->last();
}

int anvil_dtable_iter_key(const anvil_dtable_iter * c, anvil_dtype * key)
{
	anvil_dtable_iter_union_const safer(c);
	return init_anvil_dtype(key, safer->key());
}

bool anvil_dtable_iter_seek(anvil_dtable_iter * c, const anvil_dtype * key)
{
	anvil_dtable_iter_union safer(c);
	anvil_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool anvil_dtable_iter_seek_test(anvil_dtable_iter * c, blob_test test, void * user)
{
	anvil_dtable_iter_union safer(c);
	return safer->seek(anvil_dtype_test(test, user));
}

void anvil_dtable_iter_meta(const anvil_dtable_iter * c, anvil_metablob * meta)
{
	anvil_dtable_iter_union_const safer(c);
	init_anvil_metablob(meta, safer->meta());
}

int anvil_dtable_iter_value(const anvil_dtable_iter * c, anvil_blob * value)
{
	anvil_dtable_iter_union_const safer(c);
	return init_anvil_blob(value, safer->value());
}

void anvil_dtable_iter_kill(anvil_dtable_iter * c)
{
	anvil_dtable_iter_union_const safer(c);
	delete safer.cpp;
}

int anvil_ctable_create(const char * type, int dfd, const char * name, const anvil_params * config, anvil_dtype_type key_type)
{
	anvil_params_union_const config_safer(config);
	/* same values; see dtype.h */
	return ctable_factory::setup(type, dfd, name, *config_safer, (dtype::ctype) key_type);
}

anvil_ctable * anvil_ctable_open(const char * type, int dfd, const char * name, const anvil_params * config)
{
	anvil_params_union_const config_safer(config);
	ctable * cpp = ctable_factory::load(type, dfd, name, *config_safer, sys_journal::get_global_journal());
	return anvil_ctable_union(cpp);
}

void anvil_ctable_kill(anvil_ctable * c)
{
	anvil_ctable_union safer(c);
	delete safer.cpp;
}

int anvil_ctable_find(const anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column, anvil_blob * value)
{
	anvil_ctable_union_const safer(c);
	anvil_dtype_union_const key_safer(key);
	anvil_istr_union_const column_safer(column);
	return init_anvil_blob(value, safer->find(*key_safer, *column_safer));
}

bool anvil_ctable_writable(const anvil_ctable * c)
{
	anvil_ctable_union_const safer(c);
	return safer->writable();
}

int anvil_ctable_insert(anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column, const anvil_blob * value, bool append)
{
	anvil_ctable_union safer(c);
	anvil_dtype_union_const key_safer(key);
	anvil_istr_union_const column_safer(column);
	anvil_blob_union_const value_safer(value);
	return safer->insert(*key_safer, *column_safer, *value_safer, append);
}

int anvil_ctable_remove(anvil_ctable * c, const anvil_dtype * key, const anvil_istr * column)
{
	anvil_ctable_union safer(c);
	anvil_dtype_union_const key_safer(key);
	anvil_istr_union_const column_safer(column);
	return safer->remove(*key_safer, *column_safer);
}

int anvil_ctable_remove_row(anvil_ctable * c, const anvil_dtype * key)
{
	anvil_ctable_union safer(c);
	anvil_dtype_union_const key_safer(key);
	return safer->remove(*key_safer);
}

anvil_dtype_type anvil_ctable_key_type(const anvil_ctable * c)
{
	anvil_ctable_union_const safer(c);
	/* same values; see dtype.h */
	return (anvil_dtype_type) safer->key_type();
}

int anvil_ctable_set_blob_cmp(anvil_ctable * c, const anvil_blobcmp * cmp)
{
	anvil_ctable_union safer(c);
	return safer->set_blob_cmp(cmp);
}

const char * anvil_ctable_get_cmp_name(const anvil_ctable * c)
{
	anvil_ctable_union_const safer(c);
	return safer->get_cmp_name();
}

int anvil_ctable_maintain(anvil_ctable * c)
{
	anvil_ctable_union safer(c);
	return safer->maintain();
}

anvil_dtable_key_iter * anvil_ctable_keys(const anvil_ctable * c)
{
	anvil_ctable_union_const safer(c);
	dtable::key_iter * iter = safer->keys();
	dtable::key_iter * cache = iter ? new dtable_cache_key_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return anvil_dtable_key_iter_union(cache);
}

anvil_ctable_iter * anvil_ctable_iterator(const anvil_ctable * c)
{
	anvil_ctable_union_const safer(c);
	ctable::iter * iter = safer->iterator();
	ctable::iter * cache = iter ? new ctable_cache_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return anvil_ctable_iter_union(cache);
}

bool anvil_ctable_iter_valid(const anvil_ctable_iter * c)
{
	anvil_ctable_iter_union_const safer(c);
	return safer->valid();
}

bool anvil_ctable_iter_next(anvil_ctable_iter * c)
{
	anvil_ctable_iter_union safer(c);
	return safer->next();
}

bool anvil_ctable_iter_prev(anvil_ctable_iter * c)
{
	anvil_ctable_iter_union safer(c);
	return safer->prev();
}

bool anvil_ctable_iter_first(anvil_ctable_iter * c)
{
	anvil_ctable_iter_union safer(c);
	return safer->first();
}

bool anvil_ctable_iter_last(anvil_ctable_iter * c)
{
	anvil_ctable_iter_union safer(c);
	return safer->last();
}

int anvil_ctable_iter_key(const anvil_ctable_iter * c, anvil_dtype * key)
{
	anvil_ctable_iter_union_const safer(c);
	return init_anvil_dtype(key, safer->key());
}

bool anvil_ctable_iter_seek(anvil_ctable_iter * c, const anvil_dtype * key)
{
	anvil_ctable_iter_union safer(c);
	anvil_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool anvil_ctable_iter_seek_test(anvil_ctable_iter * c, blob_test test, void * user)
{
	anvil_ctable_iter_union safer(c);
	return safer->seek(anvil_dtype_test(test, user));
}

size_t anvil_ctable_iter_column(const anvil_ctable_iter * c)
{
	anvil_ctable_iter_union_const safer(c);
	return safer->column();
}

const char * anvil_ctable_iter_name(const anvil_ctable_iter * c)
{
	anvil_ctable_iter_union_const safer(c);
	return safer->name();
}

int anvil_ctable_iter_value(const anvil_ctable_iter * c, anvil_blob * value)
{
	anvil_ctable_iter_union_const safer(c);
	return init_anvil_blob(value, safer->value());
}

void anvil_ctable_iter_kill(anvil_ctable_iter * c)
{
	anvil_ctable_iter_union safer(c);
	delete safer.cpp;
}

anvil_blobcmp * anvil_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill, bool free_user)
{
	anvil_blobcmp * blobcmp = new anvil_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = free_user;
	blobcmp->user = user;
	return blobcmp;
}

anvil_blobcmp * anvil_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill)
{
	anvil_blobcmp * blobcmp = new anvil_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = true;
	blobcmp->user = malloc(size);
	util::memcpy(blobcmp->user, user, size);
	return blobcmp;
}

const char * anvil_blobcmp_name(const anvil_blobcmp * blobcmp)
{
	return blobcmp->name;
}

void anvil_blobcmp_retain(anvil_blobcmp * blobcmp)
{
	blobcmp->retain();
}

void anvil_blobcmp_release(anvil_blobcmp ** blobcmp)
{
	(*blobcmp)->release();
	*blobcmp = NULL;
}

anvil_dtable * anvil_dtable_cache::open(int index, const anvil_params * config)
{
	char number[24];
	anvil_dtable * dtable;
	idx_map::iterator iter = index_map.find(index);
	if(iter != index_map.end())
	{
		iter->second.count++;
		return iter->second.dtable;
	}
	
	snprintf(number, sizeof(number), "%d", index);
	dtable = anvil_dtable_open(type, dir_fd, number, config);
	if(!dtable)
		return NULL;
	index_map[index].init(dtable);
	dtable_map[dtable] = index;
	return dtable;
}

void anvil_dtable_cache::close(anvil_dtable * dtable)
{
	dt_map::iterator index = dtable_map.find(dtable);
	assert(index != dtable_map.end());
	idx_map::iterator iter = index_map.find(index->second);
	assert(iter != index_map.end());
	
	if(!--(iter->second.count))
	{
		int i;
		for(i = 0; i < RECENT_OPEN_DTABLES; i++)
			if(recent[i] == dtable)
			{
				/* it's being closed from the recent list */
				dtable_map.erase(index);
				for(int j = 0; j < OPEN_DTABLE_ITERS; j++)
					if(iter->second.iters[j])
					{
						iter_map.erase(iter->second.iters[j]);
						anvil_dtable_iter_kill(iter->second.iters[j]);
						iter->second.iters[j] = NULL;
					}
				index_map.erase(iter);
				anvil_dtable_kill(dtable);
				recent[i] = NULL;
				return;
			}
		/* give it a second chance in the recent list */
		for(i = 0; i < RECENT_OPEN_DTABLES; i++)
			if(!recent[i])
				break;
		if(i == RECENT_OPEN_DTABLES)
			close(recent[--i]);
		for(; i; i--)
			recent[i] = recent[i - 1];
		recent[0] = dtable;
		iter->second.count++;
	}
}

bool anvil_dtable_cache::can_maintain(int index)
{
	idx_map::iterator iter = index_map.find(index);
	if(iter == index_map.end())
		return false; 
	return (ticks - iter->second.last_tick) > DELTA_TICKS;
}

int anvil_dtable_cache::maintain(int index)
{
	int r = -1;
	idx_map::iterator iter = index_map.find(index);
	if(iter == index_map.end())
		return r;
	for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
		if(iter->second.iters[i])
		{
			iter_map.erase(iter->second.iters[i]);
			anvil_dtable_iter_kill(iter->second.iters[i]);
			iter->second.iters[i] = NULL;
		}
	tx_start_r();
	anvil_dtable_union safer(iter->second.dtable);
	r = safer->maintain();
	tx_end_r();
	if(r < 0)
		return r;
	iter->second.last_tick = ticks;
	return 0;
}

anvil_dtable_iter * anvil_dtable_cache::iterator(int index)
{
	anvil_dtable_iter * dt_iter;
	idx_map::iterator iter = index_map.find(index);
	if(iter == index_map.end())
		return NULL;
	for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
		if(iter->second.iters[i])
		{
			dt_iter = iter->second.iters[i];
			iter->second.iters[i] = NULL;
			anvil_dtable_iter_union safer(dt_iter);
			safer->first();
			++ticks;
			return dt_iter;
		}
	dt_iter = anvil_dtable_iterator(iter->second.dtable);
	if(!dt_iter)
		return NULL;
	iter_map[dt_iter] = index;
	++ticks;
	return dt_iter;
}

void anvil_dtable_cache::close(anvil_dtable_iter * dt_iter)
{
	it_map::iterator index = iter_map.find(dt_iter);
	assert(index != iter_map.end());
	idx_map::iterator iter = index_map.find(index->second);
	assert(iter != index_map.end());
	for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
		if(!iter->second.iters[i])
		{
			iter->second.iters[i] = dt_iter;
			return;
		}
	iter_map.erase(index);
	anvil_dtable_iter_kill(dt_iter);
}

anvil_dtable_cache::~anvil_dtable_cache()
{
	for(int i = 0; i < RECENT_OPEN_DTABLES; i++)
		if(recent[i])
			close(recent[i]);
	assert(index_map.empty());
}

anvil_dtable_cache * anvil_dtable_cache_new(int dir_fd, const char * type)
{
	return new anvil_dtable_cache(dir_fd, type);
}

void anvil_dtable_cache_kill(anvil_dtable_cache * c)
{
	delete c;
}

int anvil_dtable_cache_create(anvil_dtable_cache * c, int index, const anvil_params * config, const anvil_dtable * source, const anvil_dtable * shadow)
{
	char number[24];
	snprintf(number, sizeof(number), "%d", index);
	c->ticks = 0;
	return anvil_dtable_create(c->type, c->dir_fd, number, config, source, shadow);
}

int anvil_dtable_cache_create_empty(anvil_dtable_cache * c, int index, const anvil_params * config, anvil_dtype_type key_type)
{
	char number[24];
	snprintf(number, sizeof(number), "%d", index);
	return anvil_dtable_create_empty(c->type, c->dir_fd, number, config, key_type);
}

anvil_dtable * anvil_dtable_cache_open(anvil_dtable_cache * c, int index, const anvil_params * config)
{
	return c->open(index, config);
}

void anvil_dtable_cache_close(anvil_dtable_cache * c, anvil_dtable * dtable)
{
	c->close(dtable);
}

int anvil_dtable_cache_maintain(anvil_dtable_cache * c, int index)
{
	return c->maintain(index);
}

bool anvil_dtable_cache_can_maintain(anvil_dtable_cache * c, int index)
{
	return c->can_maintain(index);
}

anvil_dtable_iter * anvil_dtable_cache_iter(anvil_dtable_cache * c, int index)
{
	return c->iterator(index);
}

void anvil_dtable_cache_close_iter(anvil_dtable_cache * c, anvil_dtable_iter * iter)
{
	c->close(iter);
}
