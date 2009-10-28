/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "md5.h"
#include "openat.h"

#include "util.h"
#include "rofile.h"
#include "bloom_dtable.h"

/* The bloom filter dtable keeps a bloom filter of the keys in an underlying
 * dtable, and checks it before serving a key lookup. On a bloom filter miss,
 * the key is not looked up at all in the underlying dtable. Otherwise the
 * lookup is passed through. Since there can be no false negatives, this will
 * always work correctly. */

#define HASH_SIZE 16 /* MD5 */
#define HASH_BITS (HASH_SIZE * 8)

/* a little helper class to read bit arrays */
class bitreader
{
public:
	bitreader(const uint8_t * array, uint8_t bits)
		: array(array), bits(bits), byte(*array), left(8)
	{
	}
	uint32_t next()
	{
		uint8_t need = bits;
		uint32_t value = 0;
		while(need)
		{
			uint8_t take = (need > left) ? left : need;
			value <<= take;
			value |= byte & ((1 << take) - 1);
			byte >>= take;
			left -= take;
			need -= take;
			if(!left)
			{
				byte = *++array;
				left = 8;
			}
		}
		return value;
	}
private:
	const uint8_t * array;
	const uint8_t bits;
	uint8_t byte, left;
};

int bloom_dtable::bloom::init(int dfd, const char * file, size_t * m, size_t * k)
{
	ssize_t bytes;
	rofile * data;
	bloom_dtable_header header;
	if(filter)
		deinit();
	data = rofile::open<16, 1>(dfd, file);
	if(!data)
		return -1;
	if(data->read_type(0, &header) < 0)
		goto fail_close;
	if(header.magic != BLOOM_DTABLE_MAGIC || header.version != BLOOM_DTABLE_VERSION)
		goto fail_close;
	bytes = (header.m + 7) / 8;
	filter = new uint8_t[bytes];
	if(!filter)
		goto fail_close;
	if(data->read(sizeof(header), filter, bytes) != bytes)
		goto fail_free;
	*m = header.m;
	*k = header.k;
	delete data;
	return 0;

fail_free:
	delete[] filter;
	filter = NULL;
fail_close:
	delete data;
	return -1;
}

int bloom_dtable::bloom::write(int dfd, const char * file, size_t m, size_t k) const
{
	int fd;
	ssize_t r, bytes = (m + 7) / 8;
	bloom_dtable_header header;

	header.magic = BLOOM_DTABLE_MAGIC;
	header.version = BLOOM_DTABLE_VERSION;
	header.m = m;
	header.k = k;
	
	fd = openat(dfd, file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if(fd < 0)
		return fd;
	
	r = pwrite(fd, &header, sizeof(header), 0);
	if(r != sizeof(header))
		goto fail;
	r = pwrite(fd, filter, bytes, sizeof(header));
	if(r != bytes)
		goto fail;
	
	close(fd);
	return 0;

fail:
	close(fd);
	unlinkat(dfd, file, 0);
	return (r < 0) ? r : -1;
}

bool bloom_dtable::bloom::check(const uint8_t * hash, size_t k, size_t bits) const
{
	bitreader indices(hash, bits);
	for(size_t i = 0; i < k; i++)
		if(!check(indices.next()))
			return false;
	return true;
}

void bloom_dtable::bloom::add(const uint8_t * hash, size_t k, size_t bits)
{
	bitreader indices(hash, bits);
	for(size_t i = 0; i < k; i++)
		set(indices.next());
}

bool bloom_dtable::bloom::check(const dtype & key, size_t k, size_t bits) const
{
	MD5_CTX ctx;
	uint8_t hash[HASH_SIZE];
	MD5Init(&ctx);
	switch(key.type)
	{
		case dtype::UINT32:
			MD5Update(&ctx, (const uint8_t *) &key.u32, sizeof(key.u32));
			MD5Final(hash, &ctx);
			return check(hash, k, bits);
		case dtype::DOUBLE:
			MD5Update(&ctx, (const uint8_t *) &key.dbl, sizeof(key.dbl));
			MD5Final(hash, &ctx);
			return check(hash, k, bits);
		case dtype::STRING:
			if(key.str)
				MD5Update(&ctx, (const uint8_t *) key.str.str(), key.str.length());
			MD5Final(hash, &ctx);
			return check(hash, k, bits);
		case dtype::BLOB:
			if(key.blb.exists())
				MD5Update(&ctx, &key.blb[0], key.blb.size());
			MD5Final(hash, &ctx);
			return check(hash, k, bits);
	}
	abort();
}

void bloom_dtable::bloom::add(const dtype & key, size_t k, size_t bits)
{
	MD5_CTX ctx;
	uint8_t hash[HASH_SIZE];
	MD5Init(&ctx);
	switch(key.type)
	{
		case dtype::UINT32:
			MD5Update(&ctx, (const uint8_t *) &key.u32, sizeof(key.u32));
			MD5Final(hash, &ctx);
			return add(hash, k, bits);
		case dtype::DOUBLE:
			MD5Update(&ctx, (const uint8_t *) &key.dbl, sizeof(key.dbl));
			MD5Final(hash, &ctx);
			return add(hash, k, bits);
		case dtype::STRING:
			if(key.str)
				MD5Update(&ctx, (const uint8_t *) key.str.str(), key.str.length());
			MD5Final(hash, &ctx);
			return add(hash, k, bits);
		case dtype::BLOB:
			if(key.blb.exists())
				MD5Update(&ctx, &key.blb[0], key.blb.size());
			MD5Final(hash, &ctx);
			return add(hash, k, bits);
	}
	abort();
}

bool bloom_dtable::present(const dtype & key, bool * found) const
{
	if(!filter.check(key, k, bits))
	{
		*found = false;
		return false;
	}
	return base->present(key, found);
}

blob bloom_dtable::lookup(const dtype & key, bool * found) const
{
	if(!filter.check(key, k, bits))
	{
		*found = false;
		return blob();
	}
	return base->lookup(key, found);
}

bool bloom_dtable::static_indexed_access(const params & config)
{
	const dtable_factory * factory;
	params base_config;
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return false;
	if(!config.get("base_config", &base_config, params()))
		return false;
	return factory->indexed_access(base_config);
}

int bloom_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	int r, bf_dfd;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	bf_dfd = openat(dfd, file, 0);
	if(bf_dfd < 0)
		return bf_dfd;
	base = factory->open(bf_dfd, "base", base_config);
	if(!base)
		goto fail_base;
	ktype = base->key_type();
	assert(ktype == dtype::UINT32);
	cmp_name = base->get_cmp_name();
	
	r = filter.init(bf_dfd, "bloom", &m, &k);
	if(r < 0)
		goto fail_filter;
	bits = HASH_BITS / k;
	
	close(bf_dfd);
	return 0;
	
fail_filter:
	delete base;
	base = NULL;
fail_base:
	close(bf_dfd);
	return -1;
}

void bloom_dtable::deinit()
{
	if(base)
	{
		filter.deinit();
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

/* The "bloom_k" parameter should be a divisor of 128 (the size in bits of an
 * MD5 hash). An MD5 hash of the key will be taken, and divided into bloom_k
 * indices, each of which will be used to set a bit of an appropriately sized
 * bloom filter. Picking a value that divides 160 as well, for SHA1, will avoid
 * having to change anything if we switch to that hash instead. The default
 * value is 8, resulting in 16-bit indices and and an 8KiB bloom filter. (For
 * SHA1 it would be a 128KiB bloom filter with 20-bit indices.) */
/* FIXME: Should this be given as index_bits instead, and divide to get k? It
 * might be more stable in case of hash changes. */
int bloom_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	bool valid;
	bloom filter;
	int bf_dfd, r;
	size_t m, k, bits;
	params base_config;
	dtable::iter * iter;
	dtable * base_dtable;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("bloom_k", &r, 8) || r < 5 || r > 32)
		return -EINVAL;
	k = r;
	bits = HASH_BITS / k;
	m = 1 << bits;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	bf_dfd = openat(dfd, file, 0);
	if(bf_dfd < 0)
		goto fail_open;
	
	r = base->create(bf_dfd, "base", base_config, source, shadow);
	if(r < 0)
		goto fail_create;
	
	base_dtable = base->open(bf_dfd, "base", base_config);
	if(!base_dtable)
		goto fail_reopen;
	
	r = filter.init((m + 7) / 8);
	if(r < 0)
		goto fail_write;
	iter = base_dtable->iterator();
	if(!iter)
		goto fail_write;
	valid = iter->valid();
	while(valid)
	{
		/* add all keys, even ones with nonexistent values */
		filter.add(iter->key(), k, bits);
		valid = iter->next();
	}
	delete iter;
	r = filter.write(bf_dfd, "bloom", m, k);
	if(r < 0)
		goto fail_write;
	
	delete base_dtable;
	
	close(bf_dfd);
	return 0;
	
fail_write:
	delete base_dtable;
fail_reopen:
	util::rm_r(bf_dfd, "base");
fail_create:
	close(bf_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(bloom_dtable);
