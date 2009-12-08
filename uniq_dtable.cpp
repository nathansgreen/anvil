/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include <ext/hash_map>
#include <ext/pool_allocator.h>
#include "concat_queue.h"

#include "util.h"
#include "rwfile.h"
#include "blob_buffer.h"
#include "uniq_dtable.h"

/* The uniq dtable works a little bit like a Lempel-Ziv data compressor: it
 * keeps a sliding window of previous values, and can refer back to them if a
 * later key has the same value. To accomplish this, it uses two underlying
 * dtables: one for the values, with consecutive integer keys, and another to
 * map the real keys onto the appropriate indices in the data dtable. With an
 * appropriate dtable to store the values (e.g. simple dtable or linear dtable),
 * the integer keys need not be stored; the only overhead is then the values
 * stored in the key dtable to point into the data dtable. If enough duplicate
 * values are eliminated, this overhead will be less than the savings. */

uniq_dtable::iter::iter(dtable::iter * base, const uniq_dtable * source)
	: iter_source<uniq_dtable, dtable_wrap_iter>(base, source)
{
	claim_base = true;
}

metablob uniq_dtable::iter::meta() const
{
	/* can't really avoid reading the data */
	return metablob(value());
}

blob uniq_dtable::iter::value() const
{
	blob index_blob = base->value();
	if(!index_blob.exists())
		return index_blob;
	assert(index_blob.size() == sizeof(uint32_t));
	uint32_t index = index_blob.index<uint32_t>(0);
	return dt_source->valuebase->find(index);
}

dtable::iter * uniq_dtable::iterator(ATX_DEF) const
{
	iter * value;
	dtable::iter * source = keybase->iterator();
	if(!source)
		return NULL;
	value = new iter(source, this);
	if(!value)
		delete source;
	return value;
}

bool uniq_dtable::present(const dtype & key, bool * found, ATX_DEF) const
{
	return keybase->present(key, found);
}

blob uniq_dtable::lookup(const dtype & key, bool * found, ATX_DEF) const
{
	blob value = keybase->lookup(key, found);
	if(value.exists())
	{
		assert(value.size() == sizeof(uint32_t));
		uint32_t index = value.index<uint32_t>(0);
		value = valuebase->find(index);
	}
	return value;
}

blob uniq_dtable::index(size_t index) const
{
	blob value = keybase->index(index);
	if(value.exists())
	{
		assert(value.size() == sizeof(uint32_t));
		uint32_t index = value.index<uint32_t>(0);
		value = valuebase->find(index);
	}
	return value;
}

bool uniq_dtable::contains_index(size_t index) const
{
	return keybase->contains_index(index);
}

size_t uniq_dtable::size() const
{
	return keybase->size();
}

bool uniq_dtable::static_indexed_access(const params & config)
{
	const dtable_factory * factory;
	params base_config;
	factory = dtable_factory::lookup(config, "keybase");
	if(!factory)
		return false;
	if(!config.get("keybase_config", &base_config, params()))
		return false;
	return factory->indexed_access(base_config);
}

int uniq_dtable::init(int dfd, const char * file, const params & config, sys_journal * sysj)
{
	int u_dfd;
	const dtable_factory * kb_factory;
	const dtable_factory * vb_factory;
	params keybase_config, valuebase_config;
	if(keybase)
		deinit();
	kb_factory = dtable_factory::lookup(config, "keybase");
	vb_factory = dtable_factory::lookup(config, "valuebase");
	if(!kb_factory || !vb_factory)
		return -ENOENT;
	if(!config.get("keybase_config", &keybase_config, params()))
		return -EINVAL;
	if(!config.get("valuebase_config", &valuebase_config, params()))
		return -EINVAL;
	u_dfd = openat(dfd, file, O_RDONLY);
	if(u_dfd < 0)
		return u_dfd;
	keybase = kb_factory->open(u_dfd, "keys", keybase_config, sysj);
	if(!keybase)
		goto fail_keys;
	valuebase = vb_factory->open(u_dfd, "values", valuebase_config, sysj);
	if(!valuebase)
		goto fail_values;
	ktype = keybase->key_type();
	cmp_name = keybase->get_cmp_name();
	
	close(u_dfd);
	return 0;
	
fail_values:
	keybase->destroy();
	keybase = NULL;
fail_keys:
	close(u_dfd);
	return -1;
}

void uniq_dtable::deinit()
{
	if(keybase)
	{
		valuebase->destroy();
		valuebase = NULL;
		keybase->destroy();
		keybase = NULL;
		dtable::deinit();
	}
}

/* used in create() to wrap source iterators on the way down */
class rev_iter_key : public dtable_wrap_iter
{
public:
	inline virtual bool next() { return advance(false); }
	inline virtual bool first() { return advance(true); }
	virtual metablob meta() const;
	inline virtual blob value() const;
	
	/* the keybase is not allowed to reject: it will get nice
	 * predictable values and should be able to store them all */
	inline virtual bool reject(blob * replacement) { return false; }
	
	/* we *could* implement these, but it would be really inefficient and it's
	 * not really necessary as no create() methods actually use these methods
	 * (and this iterator is only ever passed to create() methods) */
	inline virtual bool prev() { abort(); }
	inline virtual bool last() { abort(); }
	inline virtual bool seek(const dtype & key) { abort(); }
	inline virtual bool seek(const dtype_test & test) { abort(); }
	
	/* these don't make sense for downward-passed iterators */
	inline virtual bool seek_index(size_t index) { abort(); }
	inline virtual size_t get_index() const { abort(); }
	
	rev_iter_key(dtable::iter * base, rwfile * tmp_keys);
	virtual ~rev_iter_key() {}
private:
	bool advance(bool do_first);
	
	rwfile * tmp_keys;
	off_t key_offset;
	bool value_exists;
	
	void operator=(const rev_iter_key &);
	rev_iter_key(const rev_iter_key &);
};

metablob rev_iter_key::meta() const
{
	if(value_exists)
		return metablob(sizeof(uint32_t));
	return metablob();
}

blob rev_iter_key::value() const
{
	if(value_exists)
	{
		uint32_t value_index = 0;
		int r = tmp_keys->read(key_offset, &value_index);
		assert(r >= 0);
		return blob(sizeof(uint32_t), &value_index);
	}
	return blob();
}

bool rev_iter_key::advance(bool do_first)
{
	bool ok;
	blob value;
	if(do_first)
	{
		ok = base->first();
		key_offset = 0;
		value_exists = false;
	}
	else
		ok = base->next();
	if(!ok)
	{
		value_exists = false;
		return false;
	}
	if(value_exists)
		key_offset += sizeof(uint32_t);
	value_exists = base->meta().exists();
	return true;
}

rev_iter_key::rev_iter_key(dtable::iter * base, rwfile * tmp_keys)
	: dtable_wrap_iter(base), tmp_keys(tmp_keys)
{
	advance(true);
}

/* The sliding_window class below actually does the work of finding duplicates,
 * but we only use it once when creating the temporary files in create(). Then
 * we scan those files to create the keybase and valuebase. It is then that
 * we'll get reject() calls (well, only for the valuebase), so we need to keep
 * the source iterator around and synchronized to pass rejections back to it.
 * This class keeps just enough of the same information that sliding_window
 * above keeps to let us do that, and to ensure that if a value is rejected, all
 * keys sharing that value get rejected in the source iterator. */
class reject_window
{
public:
	struct reject
	{
		uint32_t index;
		blob replacement;
	};
	inline reject_window(size_t window_size) : window_size(window_size) {}
	/* index is the key to the valuebase here */
	void append(uint32_t index, const blob & replacement);
	const blob * lookup(uint32_t index) const;
	inline void reset()
	{
		rejects.clear();
		queue.clear();
	}
private:
	typedef __gnu_cxx::__pool_alloc<std::pair<uint32_t, blob> > map_pool_allocator;
	typedef __gnu_cxx::__pool_alloc<reject> queue_pool_allocator;
	typedef __gnu_cxx::hash_map<uint32_t, blob, __gnu_cxx::hash<uint32_t>, std::equal_to<uint32_t>, map_pool_allocator> reject_map;
	typedef concat_queue<reject, queue_pool_allocator> reject_queue;
	
	reject_map rejects;
	reject_queue queue;
	size_t window_size;
	
	void operator=(const reject_window &);
	reject_window(const reject_window &);
};

void reject_window::append(uint32_t index, const blob & replacement)
{
	reject r = {index, replacement};
	queue.append(r);
	if(queue.size() == window_size)
	{
		rejects.erase(queue.first().index);
		queue.pop();
	}
	assert(queue.size() <= window_size);
	rejects[index] = replacement;
}

const blob * reject_window::lookup(uint32_t index) const
{
	reject_map::const_iterator it = rejects.find(index);
	if(it == rejects.end())
		return NULL;
	return &it->second;
}

/* used in create() to wrap source iterators on the way down */
class rev_iter_value : public dtable::iter
{
public:
	inline virtual bool valid() const { return current_valid; }
	inline virtual bool next() { return advance(false); }
	inline virtual bool first() { return advance(true); }
	inline virtual dtype key() const { return current_key; }
	virtual metablob meta() const;
	virtual blob value() const;
	
	/* we need to be in the loop for rejection */
	virtual bool reject(blob * replacement);
	
	/* we *could* implement these, but it would be really inefficient and it's
	 * not really necessary as no create() methods actually use these methods
	 * (and this iterator is only ever passed to create() methods) */
	inline virtual bool prev() { abort(); }
	inline virtual bool last() { abort(); }
	inline virtual bool seek(const dtype & key) { abort(); }
	inline virtual bool seek(const dtype_test & test) { abort(); }
	
	/* these don't make sense for downward-passed iterators */
	inline virtual bool seek_index(size_t index) { abort(); }
	inline virtual size_t get_index() const { abort(); }
	
	/* we always have the integer key type for the valuebase */
	inline virtual dtype::ctype key_type() const { return dtype::UINT32; }
	inline virtual const blob_comparator * get_blob_cmp() const { return NULL; }
	inline virtual const istr & get_cmp_name() const { return istr::null; }
	inline virtual const dtable * source() const { return base->source(); }
	
	rev_iter_value(dtable::iter * base, rwfile * tmp_keys, rwfile * tmp_sizes, rwfile * tmp_values, size_t window_size);
	virtual ~rev_iter_value() {}
	
	bool failed;
	
private:
	bool advance(bool do_first);
	void sync_base(bool do_next);
	
	/* we need to have base available so we can send rejections to it */
	dtable::iter * base;
	/* we need all three temp files */
	rwfile * tmp_keys;
	rwfile * tmp_sizes;
	rwfile * tmp_values;
	/* some state */
	reject_window window;
	off_t key_offset;
	off_t size_offset;
	off_t value_offset;
	uint32_t current_key;
	size_t current_size;
	bool current_valid;
	
	void operator=(const rev_iter_value &);
	rev_iter_value(const rev_iter_value &);
};

metablob rev_iter_value::meta() const
{
	if(current_size)
		return metablob(current_size - 1);
	return metablob();
}

blob rev_iter_value::value() const
{
	if(!current_size)
		return blob();
	if(current_size == 1)
		return blob::empty;
	size_t size = current_size - 1;
	blob_buffer value(size);
	value.set_size(size, false);
	assert(size == value.size());
	size = tmp_values->read(value_offset, &value[0], size);
	assert(size == value.size());
	return value;
}

bool rev_iter_value::reject(blob * replacement)
{
	/* OK, so the underlying dtable wants to reject this value. That's fine,
	 * but we need to make sure that future keys with this same value (which
	 * already refer back to it in the keybase) will be OK. We need to
	 * record this value as having been rejected, and ensure that any future
	 * references to it are also rejected and also replaced by the same
	 * value. (So that the references will still be correct.) */
	/* We can do this in two ways: by hoping the underlying dtable will
	 * reject the values and just checking everything is OK, or by actively
	 * rejecting the values and only hoping the replacement is the same. We
	 * use the second approach since it is less fragile. See sync_base(). */
	/* TODO: currently each unique rejected value will be stored separately
	 * even when the replacement values are the same. This should be fixed. */
	if(failed)
		return false;
	if(!base->reject(replacement))
		return false;
	assert(replacement->exists());
	window.append(current_key, *replacement);
	return true;
}

bool rev_iter_value::advance(bool do_first)
{
	int r;
	if(do_first)
	{
		base->first();
		window.reset();
		key_offset = 0;
		size_offset = 0;
		value_offset = 0;
		current_key = 0;
		current_size = 0;
	}
	else if(current_valid)
	{
		current_key++;
		size_offset += sizeof(size_t);
		/* move past the previous value */
		if(current_size > 1)
			value_offset += current_size - 1;
	}
	else
		return false;
	if(size_offset >= tmp_sizes->end())
	{
		current_size = 0;
		current_valid = false;
		sync_base(!do_first);
		return false;
	}
	current_valid = true;
	r = tmp_sizes->read(size_offset, &current_size);
	assert(r >= 0);
	sync_base(!do_first);
	return true;
}

void rev_iter_value::sync_base(bool do_next)
{
	/* move the source iterator forward to wherever it first references the
	 * current key, rejecting duplicates of previously-rejected values */
	if(do_next)
	{
		/* we assume we are moving past an actual key, otherwise it
		 * would not be stopped there at the beginning of sync_base() */
		key_offset += sizeof(uint32_t);
		base->next();
	}
	while(base->valid())
	{
		int r;
		uint32_t value_key;
		metablob meta = base->meta();
		const blob * replacement;
		if(!meta.exists())
		{
			/* omit all non-existent entries */
			base->next();
			continue;
		}
		r = tmp_keys->read(key_offset, &value_key);
		assert(r >= 0);
		assert(value_key <= current_key);
		if(value_key == current_key)
			break;
		/* this source key refers to a previous value, so check if it
		 * was rejected and reject it in the source if so */
		replacement = window.lookup(value_key);
		if(replacement)
		{
			blob dup_replacement;
			if(!base->reject(&dup_replacement) || replacement->compare(dup_replacement))
				/* it's too bad we can't report this sooner */
				failed = true;
		}
		key_offset += sizeof(uint32_t);
		base->next();
	}
	assert(base->valid() == current_valid);
}

rev_iter_value::rev_iter_value(dtable::iter * base, rwfile * tmp_keys, rwfile * tmp_sizes, rwfile * tmp_values, size_t window_size)
	: failed(false), base(base), tmp_keys(tmp_keys), tmp_sizes(tmp_sizes), tmp_values(tmp_values), window(window_size)
{
	advance(true);
}

class sliding_window
{
public:
	inline sliding_window(size_t window_size) : window_size(window_size), next_index(0) {}
	uint32_t append(const blob & value, bool * store = NULL);
	inline void reset()
	{
		values.clear();
		queue.clear();
		next_index = 0;
	}
private:
	typedef __gnu_cxx::__pool_alloc<std::pair<blob, uint32_t> > map_pool_allocator;
	typedef __gnu_cxx::__pool_alloc<blob> queue_pool_allocator;
	typedef __gnu_cxx::hash_map<blob, uint32_t, blob_hashing_comparator, blob_hashing_comparator, map_pool_allocator> value_map;
	typedef concat_queue<blob, queue_pool_allocator> value_queue;
	
	/* the slow case of append() above */
	inline uint32_t slow_append(const blob & value);
	
	value_map values;
	value_queue queue;
	size_t window_size;
	uint32_t next_index;
	
	void operator=(const sliding_window &);
	sliding_window(const sliding_window &);
};

uint32_t sliding_window::append(const blob & value, bool * store)
{
	value_map::iterator it = values.find(value);
	if(it != values.end())
	{
		if(store)
			*store = false;
		return it->second;
	}
	if(store)
		*store = true;
	return slow_append(value);
}

uint32_t sliding_window::slow_append(const blob & value)
{
	queue.append(value);
	if(queue.size() == window_size)
	{
		values.erase(queue.first());
		queue.pop();
	}
	assert(queue.size() <= window_size);
	values[value] = next_index;
	return next_index++;
}

int uniq_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int u_dfd, r;
	const size_t window_size = 4096;
	sliding_window window(window_size);
	rwfile tmp_keys, tmp_sizes, tmp_values;
	params keybase_config, valuebase_config;
	const dtable_factory * keybase = dtable_factory::lookup(config, "keybase");
	const dtable_factory * valuebase = dtable_factory::lookup(config, "valuebase");
	if(!keybase || !valuebase)
		return -ENOENT;
	if(!config.get("keybase_config", &keybase_config, params()))
		return -EINVAL;
	if(!config.get("valuebase_config", &valuebase_config, params()))
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	u_dfd = openat(dfd, file, O_RDONLY);
	if(u_dfd < 0)
		goto fail_open;
	
	/* The underlying create() methods might want to scan our iterators more
	 * than once. If we do compression in the iterators themselves, that can
	 * get pretty expensive. Instead, we create unlinked temporary files to
	 * store the compressed keys and values, and compress once - then scan
	 * the temporary files as many times as necessary. We trade additional
	 * disk space and I/O work for CPU time here, an unusual trade in Anvil
	 * as a whole (which often does the opposite). */
	r = tmp_keys.create(u_dfd, "tmp_keys");
	if(r < 0)
		goto fail_temp;
	unlinkat(u_dfd, "tmp_keys", 0);
	r = tmp_sizes.create(u_dfd, "tmp_sizes");
	if(r < 0)
		goto fail_temp;
	unlinkat(u_dfd, "tmp_sizes", 0);
	r = tmp_values.create(u_dfd, "tmp_values");
	if(r < 0)
		goto fail_temp;
	unlinkat(u_dfd, "tmp_values", 0);
	
	/* just to be sure */
	source->first();
	while(source->valid())
	{
		bool store = false;
		dtype key = source->key();
		blob value = source->value();
		uint32_t index;
		source->next();
		if(!value.exists())
			/* omit all non-existent entries */
			continue;
		index = window.append(value, &store);
		tmp_keys.append(&index);
		if(store)
		{
			size_t size = value.exists() ? value.size() + 1 : 0;
			tmp_sizes.append(&size);
			tmp_values.append(value);
		}
	}
	window.reset();
	
	/* now write the keys */
	{
		rev_iter_key rev_key(source, &tmp_keys);
		r = keybase->create(u_dfd, "keys", keybase_config, &rev_key, shadow);
		if(r < 0)
			goto fail_keys;
	}
	
	/* and the values */
	{
		rev_iter_value rev_value(source, &tmp_keys, &tmp_sizes, &tmp_values, window_size);
		r = valuebase->create(u_dfd, "values", valuebase_config, &rev_value, NULL);
		if(r < 0)
			goto fail_values;
		if(rev_value.failed)
			goto fail_undetect;
	}
	
	close(u_dfd);
	return 0;
	
fail_undetect:
	util::rm_r(u_dfd, "values");
fail_values:
	util::rm_r(u_dfd, "keys");
fail_keys:
fail_temp:
	close(u_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return (r < 0) ? r : -1;
}

DEFINE_RO_FACTORY(uniq_dtable);
