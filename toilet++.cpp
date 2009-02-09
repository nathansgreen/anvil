/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
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
#include "toilet++.h"

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

int tpp_init(const char * path)
{
	int r, fd = open(path, 0);
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
				r = global->init(fd, "sys_journal", true);
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

static inline int init_tpp_istr(tpp_istr * c, const istr & value)
{
	tpp_istr_union safer(c);
	/* istr is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int tpp_istr_new(tpp_istr * c, const char * str)
{
	return init_tpp_istr(c, str);
}

int tpp_istr_copy(tpp_istr * c, const tpp_istr * src)
{
	tpp_istr_union_const src_safer(src);
	return init_tpp_istr(c, *src_safer);
}

void tpp_istr_kill(tpp_istr * c)
{
	tpp_istr_union safer(c);
	*safer = istr(NULL);
}

const char * tpp_istr_str(const tpp_istr * c)
{
	tpp_istr_union_const safer(c);
	return safer->str();
}

size_t tpp_istr_length(const tpp_istr * c)
{
	tpp_istr_union_const safer(c);
	return safer->length();
}

static inline int init_tpp_blob(tpp_blob * c, const blob & value)
{
	tpp_blob_union safer(c);
	/* blob is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int tpp_blob_new(tpp_blob * c, size_t size, const void * data)
{
	return init_tpp_blob(c, blob(size, data));
}

int tpp_blob_str(tpp_blob * c, const char * str)
{
	return init_tpp_blob(c, blob(str));
}

int tpp_blob_dne(tpp_blob * c)
{
	return init_tpp_blob(c, blob::dne);
}

int tpp_blob_empty(tpp_blob * c)
{
	return init_tpp_blob(c, blob::empty);
}

int tpp_blob_copy(tpp_blob * c, const tpp_blob * src)
{
	tpp_blob_union_const src_safer(src);
	return init_tpp_blob(c, *src_safer);
}

int tpp_blob_copy_buffer(tpp_blob * c, const tpp_blob_buffer * src)
{
	tpp_blob_buffer_union_const src_safer(src);
	return init_tpp_blob(c, blob(*src_safer));
}

void tpp_blob_kill(tpp_blob * c)
{
	tpp_blob_union safer(c);
	*safer = blob();
}

bool tpp_blob_exists(const tpp_blob * c)
{
	tpp_blob_union_const safer(c);
	return safer->exists();
}

size_t tpp_blob_size(const tpp_blob * c)
{
	tpp_blob_union_const safer(c);
	return safer->size();
}

const void * tpp_blob_data(const tpp_blob * c)
{
	tpp_blob_union_const safer(c);
	return safer->data();
}

int tpp_blob_overwrite(tpp_blob * c, size_t offset, const void * data, size_t length)
{
	tpp_blob_union safer(c);
	blob_buffer buffer(*safer);
	int r = buffer.overwrite(offset, data, length);
	if(r < 0)
		return r;
	*safer = blob(buffer);
	return 0;
}

int tpp_blob_append(tpp_blob * c, const void * data, size_t length)
{
	tpp_blob_union safer(c);
	blob_buffer buffer(*safer);
	int r = buffer.append(data, length);
	if(r < 0)
		return r;
	*safer = blob(buffer);
	return 0;
}

static inline void init_tpp_metablob(tpp_metablob * c, const metablob & value)
{
	c->_size = value.size();
	c->_exists = value.exists();
}

int tpp_metablob_copy(tpp_metablob * c, const tpp_metablob * src)
{
	*c = *src;
	return 0;
}

void tpp_metablob_kill(tpp_metablob * c)
{
	/* nothing to do */
}

bool tpp_metablob_exists(const tpp_metablob * c)
{
	return c->_exists;
}

size_t tpp_metablob_size(const tpp_metablob * c)
{
	return c->_size;
}

static inline int init_tpp_blob_buffer(tpp_blob_buffer * c, const blob_buffer & value)
{
	tpp_blob_buffer_union safer(c);
	/* blob_buffer is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int tpp_blob_buffer_new(tpp_blob_buffer * c)
{
	return init_tpp_blob_buffer(c, blob_buffer());
}

int tpp_blob_buffer_new_capacity(tpp_blob_buffer * c, size_t capacity)
{
	return init_tpp_blob_buffer(c, blob_buffer(capacity));
}

int tpp_blob_buffer_new_data(tpp_blob_buffer * c, size_t size, const void * data)
{
	return init_tpp_blob_buffer(c, blob_buffer(size, data));
}

int tpp_blob_buffer_copy(tpp_blob_buffer * c, const tpp_blob_buffer * src)
{
	tpp_blob_buffer_union_const src_safer(src);
	return init_tpp_blob_buffer(c, *src_safer);
}

int tpp_blob_buffer_copy_blob(tpp_blob_buffer * c, const tpp_blob * src)
{
	tpp_blob_union_const src_safer(src);
	return init_tpp_blob_buffer(c, blob_buffer(*src_safer));
}

void tpp_blob_buffer_kill(tpp_blob_buffer * c)
{
	tpp_blob_buffer_union safer(c);
	*safer = blob_buffer();
}

bool tpp_blob_buffer_exists(const tpp_blob_buffer * c)
{
	tpp_blob_buffer_union_const safer(c);
	return safer->exists();
}

size_t tpp_blob_buffer_size(const tpp_blob_buffer * c)
{
	tpp_blob_buffer_union_const safer(c);
	return safer->size();
}

size_t tpp_blob_buffer_capacity(const tpp_blob_buffer * c)
{
	tpp_blob_buffer_union_const safer(c);
	return safer->capacity();
}

const void * tpp_blob_buffer_data(const tpp_blob_buffer * c)
{
	tpp_blob_buffer_union_const safer(c);
	return safer->data();
}

int tpp_blob_buffer_set_size(tpp_blob_buffer * c, size_t size)
{
	tpp_blob_buffer_union safer(c);
	return safer->set_size(size);
}

int tpp_blob_buffer_set_capacity(tpp_blob_buffer * c, size_t capacity)
{
	tpp_blob_buffer_union safer(c);
	return safer->set_capacity(capacity);
}

int tpp_blob_buffer_overwrite(tpp_blob_buffer * c, size_t offset, const void * data, size_t length)
{
	tpp_blob_buffer_union safer(c);
	return safer->overwrite(offset, data, length);
}

int tpp_blob_buffer_append(tpp_blob_buffer * c, const void * data, size_t length)
{
	tpp_blob_buffer_union safer(c);
	return safer->append(data, length);
}

tpp_params * tpp_params_new(void)
{
	return tpp_params_union(new params);
}

int tpp_params_parse(tpp_params * c, const char * string)
{
	tpp_params_union safer(c);
	return params::parse(string, safer);
}

void tpp_params_kill(tpp_params * c)
{
	tpp_params_union safer(c);
	delete safer.cpp;
}

static inline int init_tpp_dtype(tpp_dtype * c, const dtype & value)
{
	tpp_dtype_union safer(c);
	/* dtype is sufficiently initialized if it is zeroed out */
	util::memset(c, 0, sizeof(*c));
	*safer = value;
	return 0;
}

int tpp_dtype_int(tpp_dtype * c, uint32_t value)
{
	return init_tpp_dtype(c, dtype(value));
}

int tpp_dtype_dbl(tpp_dtype * c, double value)
{
	return init_tpp_dtype(c, dtype(value));
}

int tpp_dtype_str(tpp_dtype * c, const char * value)
{
	return init_tpp_dtype(c, dtype(value));
}

int tpp_dtype_istr(tpp_dtype * c, const tpp_istr * value)
{
	tpp_istr_union_const value_safer(value);
	return init_tpp_dtype(c, dtype(*value_safer));
}

int tpp_dtype_blb(tpp_dtype * c, const tpp_blob * value)
{
	tpp_blob_union_const value_safer(value);
	return init_tpp_dtype(c, dtype(*value_safer));
}

int tpp_dtype_copy(tpp_dtype * c, const tpp_dtype * src)
{
	tpp_dtype_union_const src_safer(src);
	return init_tpp_dtype(c, *src_safer);
}

void tpp_dtype_kill(tpp_dtype * c)
{
	tpp_dtype_union safer(c);
	*safer = dtype(0u);
}

tpp_dtype_type tpp_dtype_get_type(const tpp_dtype * c)
{
	tpp_dtype_union_const safer(c);
	/* same values; see dtype.h */
	return (tpp_dtype_type) safer->type;
}

int tpp_dtype_get_int(const tpp_dtype * c, uint32_t * value)
{
	tpp_dtype_union_const safer(c);
	assert(safer->type == dtype::UINT32);
	*value = safer->u32;
	return 0;
}

int tpp_dtype_get_dbl(const tpp_dtype * c, double * value)
{
	tpp_dtype_union_const safer(c);
	assert(safer->type == dtype::DOUBLE);
	*value = safer->dbl;
	return 0;
}

int tpp_dtype_get_str(const tpp_dtype * c, tpp_istr * value)
{
	tpp_dtype_union_const safer(c);
	assert(safer->type == dtype::STRING);
	return init_tpp_istr(value, safer->str);
}

int tpp_dtype_get_blb(const tpp_dtype * c, tpp_blob * value)
{
	tpp_dtype_union_const safer(c);
	assert(safer->type == dtype::BLOB);
	return init_tpp_blob(value, safer->blb);
}

int tpp_dtype_compare(const tpp_dtype * a, const tpp_dtype * b)
{
	tpp_dtype_union_const a_safer(a);
	tpp_dtype_union_const b_safer(b);
	return a_safer->compare(*b_safer);
}

int tpp_dtype_compare_blobcmp(const tpp_dtype * a, const tpp_dtype * b, const tpp_blobcmp * cmp)
{
	tpp_dtype_union_const a_safer(a);
	tpp_dtype_union_const b_safer(b);
	return a_safer->compare(*b_safer, cmp);
}

const char * tpp_dtype_type_name(tpp_dtype_type type)
{
	/* same values; see dtype.h */
	return dtype::name((dtype::ctype) type);
}

int tpp_dtable_create(const char * type, int dfd, const char * name, const tpp_params * config, const tpp_dtable * source, const tpp_dtable * shadow)
{
	tpp_params_union_const config_safer(config);
	tpp_dtable_union_const source_safer(source);
	tpp_dtable_union_const shadow_safer(shadow);
	return dtable_factory::setup(type, dfd, name, *config_safer, source_safer, shadow_safer);
}

int tpp_dtable_create_empty(const char * type, int dfd, const char * name, const tpp_params * config, tpp_dtype_type key_type)
{
	tpp_params_union_const config_safer(config);
	/* same values; see dtype.h */
	return dtable_factory::setup(type, dfd, name, *config_safer, (dtype::ctype) key_type);
}

tpp_dtable * tpp_dtable_open(const char * type, int dfd, const char * name, const tpp_params * config)
{
	tpp_params_union_const config_safer(config);
	dtable * cpp = dtable_factory::load(type, dfd, name, *config_safer);
	return tpp_dtable_union(cpp);
}

void tpp_dtable_kill(tpp_dtable * c)
{
	tpp_dtable_union safer(c);
	delete safer.cpp;
}

int tpp_dtable_find(const tpp_dtable * c, const tpp_dtype * key, tpp_blob * value)
{
	tpp_dtable_union_const safer(c);
	tpp_dtype_union_const key_safer(key);
	return init_tpp_blob(value, safer->find(*key_safer));
}

bool tpp_dtable_writable(const tpp_dtable * c)
{
	tpp_dtable_union_const safer(c);
	return safer->writable();
}

int tpp_dtable_insert(tpp_dtable * c, const tpp_dtype * key, const tpp_blob * value, bool append)
{
	tpp_dtable_union safer(c);
	tpp_dtype_union_const key_safer(key);
	tpp_blob_union_const value_safer(value);
	return safer->insert(*key_safer, *value_safer, append);
}

int tpp_dtable_remove(tpp_dtable * c, const tpp_dtype * key)
{
	tpp_dtable_union safer(c);
	tpp_dtype_union_const key_safer(key);
	return safer->remove(*key_safer);
}

tpp_dtype_type tpp_dtable_key_type(const tpp_dtable * c)
{
	tpp_dtable_union_const safer(c);
	/* same values; see dtype.h */
	return (tpp_dtype_type) safer->key_type();
}

int tpp_dtable_set_blob_cmp(tpp_dtable * c, const tpp_blobcmp * cmp)
{
	tpp_dtable_union safer(c);
	return safer->set_blob_cmp(cmp);
}

const char * tpp_dtable_get_cmp_name(const tpp_dtable * c)
{
	tpp_dtable_union_const safer(c);
	return safer->get_cmp_name();
}

int tpp_dtable_maintain(tpp_dtable * c)
{
	tpp_dtable_union safer(c);
	return safer->maintain();
}

tpp_dtable_key_iter * tpp_dtable_keys(const tpp_dtable * c)
{
	tpp_dtable_union_const safer(c);
	dtable::key_iter * iter = safer->iterator();
	dtable::key_iter * cache = iter ? new dtable_cache_key_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return tpp_dtable_key_iter_union(cache);
}

bool tpp_dtable_key_iter_valid(const tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union_const safer(c);
	return safer->valid();
}

bool tpp_dtable_key_iter_next(tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union safer(c);
	return safer->next();
}

bool tpp_dtable_key_iter_prev(tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union safer(c);
	return safer->prev();
}

bool tpp_dtable_key_iter_first(tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union safer(c);
	return safer->first();
}

bool tpp_dtable_key_iter_last(tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union safer(c);
	return safer->last();
}

int tpp_dtable_key_iter_key(const tpp_dtable_key_iter * c, tpp_dtype * key)
{
	tpp_dtable_key_iter_union_const safer(c);
	return init_tpp_dtype(key, safer->key());
}

bool tpp_dtable_key_iter_seek(tpp_dtable_key_iter * c, const tpp_dtype * key)
{
	tpp_dtable_key_iter_union safer(c);
	tpp_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool tpp_dtable_key_iter_seek_test(tpp_dtable_key_iter * c, blob_test test, void * user)
{
	tpp_dtable_key_iter_union safer(c);
	return safer->seek(tpp_dtype_test(test, user));
}

void tpp_dtable_key_iter_kill(tpp_dtable_key_iter * c)
{
	tpp_dtable_key_iter_union safer(c);
	delete safer.cpp;
}

tpp_dtable_iter * tpp_dtable_iterator(const tpp_dtable * c)
{
	tpp_dtable_union_const safer(c);
	dtable::iter * iter = safer->iterator();
	dtable::iter * cache = iter ? new dtable_cache_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return tpp_dtable_iter_union(cache);
}

bool tpp_dtable_iter_valid(const tpp_dtable_iter * c)
{
	tpp_dtable_iter_union_const safer(c);
	return safer->valid();
}

bool tpp_dtable_iter_next(tpp_dtable_iter * c)
{
	tpp_dtable_iter_union safer(c);
	return safer->next();
}

bool tpp_dtable_iter_prev(tpp_dtable_iter * c)
{
	tpp_dtable_iter_union safer(c);
	return safer->prev();
}

bool tpp_dtable_iter_first(tpp_dtable_iter * c)
{
	tpp_dtable_iter_union safer(c);
	return safer->first();
}

bool tpp_dtable_iter_last(tpp_dtable_iter * c)
{
	tpp_dtable_iter_union safer(c);
	return safer->last();
}

int tpp_dtable_iter_key(const tpp_dtable_iter * c, tpp_dtype * key)
{
	tpp_dtable_iter_union_const safer(c);
	return init_tpp_dtype(key, safer->key());
}

bool tpp_dtable_iter_seek(tpp_dtable_iter * c, const tpp_dtype * key)
{
	tpp_dtable_iter_union safer(c);
	tpp_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool tpp_dtable_iter_seek_test(tpp_dtable_iter * c, blob_test test, void * user)
{
	tpp_dtable_iter_union safer(c);
	return safer->seek(tpp_dtype_test(test, user));
}

void tpp_dtable_iter_meta(const tpp_dtable_iter * c, tpp_metablob * meta)
{
	tpp_dtable_iter_union_const safer(c);
	init_tpp_metablob(meta, safer->meta());
}

int tpp_dtable_iter_value(const tpp_dtable_iter * c, tpp_blob * value)
{
	tpp_dtable_iter_union_const safer(c);
	return init_tpp_blob(value, safer->value());
}

void tpp_dtable_iter_kill(tpp_dtable_iter * c)
{
	tpp_dtable_iter_union_const safer(c);
	delete safer.cpp;
}

int tpp_ctable_create(const char * type, int dfd, const char * name, const tpp_params * config, tpp_dtype_type key_type)
{
	tpp_params_union_const config_safer(config);
	/* same values; see dtype.h */
	return ctable_factory::setup(type, dfd, name, *config_safer, (dtype::ctype) key_type);
}

tpp_ctable * tpp_ctable_open(const char * type, int dfd, const char * name, const tpp_params * config)
{
	tpp_params_union_const config_safer(config);
	ctable * cpp = ctable_factory::load(type, dfd, name, *config_safer);
	return tpp_ctable_union(cpp);
}

void tpp_ctable_kill(tpp_ctable * c)
{
	tpp_ctable_union safer(c);
	delete safer.cpp;
}

int tpp_ctable_find(const tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column, tpp_blob * value)
{
	tpp_ctable_union_const safer(c);
	tpp_dtype_union_const key_safer(key);
	tpp_istr_union_const column_safer(column);
	return init_tpp_blob(value, safer->find(*key_safer, *column_safer));
}

bool tpp_ctable_writable(const tpp_ctable * c)
{
	tpp_ctable_union_const safer(c);
	return safer->writable();
}

int tpp_ctable_insert(tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column, const tpp_blob * value, bool append)
{
	tpp_ctable_union safer(c);
	tpp_dtype_union_const key_safer(key);
	tpp_istr_union_const column_safer(column);
	tpp_blob_union_const value_safer(value);
	return safer->insert(*key_safer, *column_safer, *value_safer, append);
}

int tpp_ctable_remove(tpp_ctable * c, const tpp_dtype * key, const tpp_istr * column)
{
	tpp_ctable_union safer(c);
	tpp_dtype_union_const key_safer(key);
	tpp_istr_union_const column_safer(column);
	return safer->remove(*key_safer, *column_safer);
}

int tpp_ctable_remove_row(tpp_ctable * c, const tpp_dtype * key)
{
	tpp_ctable_union safer(c);
	tpp_dtype_union_const key_safer(key);
	return safer->remove(*key_safer);
}

tpp_dtype_type tpp_ctable_key_type(const tpp_ctable * c)
{
	tpp_ctable_union_const safer(c);
	/* same values; see dtype.h */
	return (tpp_dtype_type) safer->key_type();
}

int tpp_ctable_set_blob_cmp(tpp_ctable * c, const tpp_blobcmp * cmp)
{
	tpp_ctable_union safer(c);
	return safer->set_blob_cmp(cmp);
}

const char * tpp_ctable_get_cmp_name(const tpp_ctable * c)
{
	tpp_ctable_union_const safer(c);
	return safer->get_cmp_name();
}

int tpp_ctable_maintain(tpp_ctable * c)
{
	tpp_ctable_union safer(c);
	return safer->maintain();
}

tpp_dtable_key_iter * tpp_ctable_keys(const tpp_ctable * c)
{
	tpp_ctable_union_const safer(c);
	dtable::key_iter * iter = safer->keys();
	dtable::key_iter * cache = iter ? new dtable_cache_key_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return tpp_dtable_key_iter_union(cache);
}

tpp_ctable_iter * tpp_ctable_iterator(const tpp_ctable * c)
{
	tpp_ctable_union_const safer(c);
	ctable::iter * iter = safer->iterator();
	ctable::iter * cache = iter ? new ctable_cache_iter(iter) : NULL;
	if(!cache && iter)
		delete iter;
	return tpp_ctable_iter_union(cache);
}

bool tpp_ctable_iter_valid(const tpp_ctable_iter * c)
{
	tpp_ctable_iter_union_const safer(c);
	return safer->valid();
}

bool tpp_ctable_iter_next(tpp_ctable_iter * c)
{
	tpp_ctable_iter_union safer(c);
	return safer->next();
}

bool tpp_ctable_iter_prev(tpp_ctable_iter * c)
{
	tpp_ctable_iter_union safer(c);
	return safer->prev();
}

bool tpp_ctable_iter_first(tpp_ctable_iter * c)
{
	tpp_ctable_iter_union safer(c);
	return safer->first();
}

bool tpp_ctable_iter_last(tpp_ctable_iter * c)
{
	tpp_ctable_iter_union safer(c);
	return safer->last();
}

int tpp_ctable_iter_key(const tpp_ctable_iter * c, tpp_dtype * key)
{
	tpp_ctable_iter_union_const safer(c);
	return init_tpp_dtype(key, safer->key());
}

bool tpp_ctable_iter_seek(tpp_ctable_iter * c, const tpp_dtype * key)
{
	tpp_ctable_iter_union safer(c);
	tpp_dtype_union_const key_safer(key);
	return safer->seek(*key_safer);
}

bool tpp_ctable_iter_seek_test(tpp_ctable_iter * c, blob_test test, void * user)
{
	tpp_ctable_iter_union safer(c);
	return safer->seek(tpp_dtype_test(test, user));
}

const char * tpp_ctable_iter_column(const tpp_ctable_iter * c)
{
	tpp_ctable_iter_union_const safer(c);
	return safer->column();
}

int tpp_ctable_iter_value(const tpp_ctable_iter * c, tpp_blob * value)
{
	tpp_ctable_iter_union_const safer(c);
	return init_tpp_blob(value, safer->value());
}

void tpp_ctable_iter_kill(tpp_ctable_iter * c)
{
	tpp_ctable_iter_union safer(c);
	delete safer.cpp;
}

tpp_blobcmp * tpp_new_blobcmp(const char * name, blobcmp_func cmp, void * user, blobcmp_free kill, bool free_user)
{
	tpp_blobcmp * blobcmp = new tpp_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = free_user;
	blobcmp->user = user;
	return blobcmp;
}

tpp_blobcmp * tpp_new_blobcmp_copy(const char * name, blobcmp_func cmp, const void * user, size_t size, blobcmp_free kill)
{
	tpp_blobcmp * blobcmp = new tpp_blobcmp(name);
	blobcmp->cmp = cmp;
	blobcmp->kill = kill;
	blobcmp->copied = true;
	blobcmp->user = malloc(size);
	util::memcpy(blobcmp->user, user, size);
	return blobcmp;
}

const char * tpp_blobcmp_name(const tpp_blobcmp * blobcmp)
{
	return blobcmp->name;
}

void tpp_blobcmp_retain(tpp_blobcmp * blobcmp)
{
	blobcmp->retain();
}

void tpp_blobcmp_release(tpp_blobcmp ** blobcmp)
{
	(*blobcmp)->release();
	*blobcmp = NULL;
}

tpp_dtable * tpp_dtable_cache::open(int index)
{
	char number[24];
	tpp_dtable * dtable;
	idx_map::iterator iter = index_map.find(index);
	if(iter != index_map.end())
	{
		iter->second.count++;
		return iter->second.dtable;
	}
	
	snprintf(number, sizeof(number), "%d", index);
	dtable = tpp_dtable_open(type, dir_fd, number, config);
	if(!dtable)
		return NULL;
	index_map[index].init(dtable);
	dtable_map[dtable] = index;
	return dtable;
}

void tpp_dtable_cache::close(tpp_dtable * dtable)
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
						tpp_dtable_iter_kill(iter->second.iters[j]);
						iter->second.iters[j] = NULL;
					}
				index_map.erase(iter);
				tpp_dtable_kill(dtable);
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

tpp_dtable_iter * tpp_dtable_cache::iterator(int index)
{
	tpp_dtable_iter * dt_iter;
	idx_map::iterator iter = index_map.find(index);
	if(iter == index_map.end())
		return NULL;
	for(int i = 0; i < OPEN_DTABLE_ITERS; i++)
		if(iter->second.iters[i])
		{
			dt_iter = iter->second.iters[i];
			iter->second.iters[i] = NULL;
			tpp_dtable_iter_union safer(dt_iter);
			safer->first();
			return dt_iter;
		}
	dt_iter = tpp_dtable_iterator(iter->second.dtable);
	if(!dt_iter)
		return NULL;
	iter_map[dt_iter] = index;
	return dt_iter;
}

void tpp_dtable_cache::close(tpp_dtable_iter * dt_iter)
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
	tpp_dtable_iter_kill(dt_iter);
}

tpp_dtable_cache::~tpp_dtable_cache()
{
	for(int i = 0; i < RECENT_OPEN_DTABLES; i++)
		if(recent[i])
			close(recent[i]);
	assert(index_map.empty());
}

tpp_dtable_cache * tpp_dtable_cache_new(int dir_fd, const char * type, const tpp_params * config)
{
	return new tpp_dtable_cache(dir_fd, type, config);
}

void tpp_dtable_cache_kill(tpp_dtable_cache * c)
{
	delete c;
}

int tpp_dtable_cache_create(tpp_dtable_cache * c, int index, const tpp_dtable * source, const tpp_dtable * shadow)
{
	char number[24];
	snprintf(number, sizeof(number), "%d", index);
	return tpp_dtable_create(c->type, c->dir_fd, number, c->config, source, shadow);
}

int tpp_dtable_cache_create_empty(tpp_dtable_cache * c, int index, tpp_dtype_type key_type)
{
	char number[24];
	snprintf(number, sizeof(number), "%d", index);
	return tpp_dtable_create_empty(c->type, c->dir_fd, number, c->config, key_type);
}

tpp_dtable * tpp_dtable_cache_open(tpp_dtable_cache * c, int index)
{
	return c->open(index);
}

void tpp_dtable_cache_close(tpp_dtable_cache * c, tpp_dtable * dtable)
{
	c->close(dtable);
}

tpp_dtable_iter * tpp_dtable_cache_iter(tpp_dtable_cache * c, int index)
{
	return c->iterator(index);
}

void tpp_dtable_cache_close_iter(tpp_dtable_cache * c, tpp_dtable_iter * iter)
{
	c->close(iter);
}
