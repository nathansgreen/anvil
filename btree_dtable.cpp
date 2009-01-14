/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include "openat.h"

#include "util.h"
#include "btree_dtable.h"

/* A btree dtable doesn't actually store the data itself - that's up to some
 * other dtable. What btree dtables do is create optimal btrees for looking up
 * keys, giving better key lookup speed than a binary search. Rather than log
 * base 2 pages read from disk, we get log base m+1 where m is the number of
 * keys stored per page. What the btree actually stores is a map from keys to
 * indices in the underlying dtable. */

/* The first page of a btree dtable file has a header, btree_dtable_header, and
 * is otherwise empty. Each subsequent page of the btree has one of two forms;
 * either an internal page, like this:
 * 
 * | page# | <key, index> | page# | <key, index> | ... | page# | [ filled ]
 * 
 * or a leaf page, like this:
 * 
 * | <key, index> | <key, index> | ... | <key, index> | [ filled ]
 * 
 * As page numbers, keys, and indices are all 32 bits, we can fit 341 <key,
 * index> pairs per 4K internal page, with 342 page numbers between them, and
 * 512 <key, index> pairs per 4K leaf page. We can tell the difference between
 * the two page types by the (runtime-known) depth of the page from the root.
 * 
 * The last few pages in the file may not be entirely filled, in the event that
 * we run out of keys before filling the tree exactly. (This will usually be the
 * case.) We store in the header the last completely filled page number, and on
 * all pages after that page, we store the number of filled bytes in the last 32
 * bits of the page. (Since a page number or <key, index> pair would be at least
 * that size, and since the page wasn't full, we know we have room for it.) */

#define BTREE_PAGENO_SIZE sizeof(uint32_t)
#define BTREE_KEY_SIZE sizeof(uint32_t)
#define BTREE_INDEX_SIZE sizeof(uint32_t)

#define BTREE_KEY_INDEX_SIZE (BTREE_KEY_SIZE + BTREE_INDEX_SIZE)
#define BTREE_ENTRY_SIZE (BTREE_PAGENO_SIZE + BTREE_KEY_INDEX_SIZE)

/* integers round down */
#define BTREE_KEYS_PER_PAGE ((BTREE_PAGE_SIZE - BTREE_PAGENO_SIZE) / BTREE_ENTRY_SIZE)
#define BTREE_KEYS_PER_LEAF_PAGE (BTREE_PAGE_SIZE / BTREE_KEY_INDEX_SIZE)

btree_dtable::iter::iter(dtable::iter * base, const btree_dtable * source)
	: base_iter(base), bdt_source(source)
{
}

bool btree_dtable::iter::valid() const
{
	return base_iter->valid();
}

bool btree_dtable::iter::next()
{
	return base_iter->next();
}

bool btree_dtable::iter::prev()
{
	return base_iter->prev();
}

bool btree_dtable::iter::first()
{
	return base_iter->first();
}

bool btree_dtable::iter::last()
{
	return base_iter->last();
}

dtype btree_dtable::iter::key() const
{
	return base_iter->key();
}

bool btree_dtable::iter::seek(const dtype & key)
{
	bool found;
	base_iter->seek_index(bdt_source->btree_lookup(key, &found));
	return found;
}

bool btree_dtable::iter::seek(const dtype_test & test)
{
	bool found;
	base_iter->seek_index(bdt_source->btree_lookup(test, &found));
	return found;
}

bool btree_dtable::iter::seek_index(size_t index)
{
	return base_iter->seek_index(index);
}

size_t btree_dtable::iter::get_index() const
{
	return base_iter->get_index();
}

metablob btree_dtable::iter::meta() const
{
	return base_iter->meta();
}

blob btree_dtable::iter::value() const
{
	return base_iter->value();
}

const dtable * btree_dtable::iter::source() const
{
	return base_iter->source();
}

dtable::iter * btree_dtable::iterator() const
{
	iter * value;
	dtable::iter * source = base->iterator();
	if(!source)
		return NULL;
	value = new iter(source, this);
	if(!value)
	{
		delete source;
		return NULL;
	}
	return value;
}

blob btree_dtable::lookup(const dtype & key, bool * found) const
{
	size_t index = btree_lookup(key, found);
	if(!*found)
		return blob();
	return base->index(index);
}

blob btree_dtable::index(size_t index) const
{
	return base->index(index);
}

size_t btree_dtable::size() const
{
	return base->size();
}

int btree_dtable::init(int dfd, const char * file, const params & config)
{
	const dtable_factory * factory;
	params base_config;
	int r, bt_dfd;
	if(base)
		deinit();
	factory = dtable_factory::lookup(config, "base");
	if(!factory)
		return -EINVAL;
	if(!factory->indexed_access())
		return -ENOSYS;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	bt_dfd = openat(dfd, file, 0);
	if(bt_dfd < 0)
		return bt_dfd;
	base = factory->open(bt_dfd, "base", base_config);
	if(!base)
		goto fail_base;
	ktype = base->key_type();
	assert(ktype == dtype::UINT32);
	cmp_name = base->get_cmp_name();
	
	/* open the btree */
	btree = rofile::open<BTREE_PAGE_KB, 8>(bt_dfd, "btree");
	if(!btree)
		goto fail_open;
	r = btree->read(0, &header);
	if(r < 0)
		goto fail_format;
	/* check the header */
	if(header.magic != BTREE_DTABLE_MAGIC || header.version != BTREE_DTABLE_VERSION)
		goto fail_format;
	if(header.page_size != BTREE_PAGE_SIZE || header.pageno_size != BTREE_PAGENO_SIZE)
		goto fail_format;
	if(header.key_size != BTREE_KEY_SIZE || header.index_size != BTREE_INDEX_SIZE)
		goto fail_format;
	/* 1 -> uint32, and even with an empty table there will be a root page */
	if(header.key_type != 1 || !header.root_page || !header.last_full)
		goto fail_format;
	
	close(bt_dfd);
	return 0;
	
fail_format:
	delete btree;
fail_open:
	delete base;
	base = NULL;
fail_base:
	close(bt_dfd);
	return -1;
}

void btree_dtable::deinit()
{
	if(base)
	{
		delete btree;
		delete base;
		base = NULL;
		dtable::deinit();
	}
}

template<class T, class U>
size_t btree_dtable::find_key(const T & test, const U * entries, size_t count, bool * found)
{
	/* binary search */
	ssize_t min = 0, max = count - 1;
	dtype key(0u);
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		key.u32 = entries[mid].get_key();
		int c = test(key);
		if(c < 0)
			min = mid + 1;
		else if(c > 0)
			max = mid - 1;
		else
		{
			*found = true;
			return mid;
		}
	}
	*found = false;
	return min;
}

template<class T>
size_t btree_dtable::btree_lookup(const T & test, bool * found) const
{
	page_union page;
	size_t depth = 1;
	size_t keys, index;
	bool full = header.root_page <= header.last_full;
	page.page = btree->page(header.root_page);
	
	while(depth < header.depth)
	{
		/* scan the internal page */
		size_t pointer, pointers;
		if(!full)
		{
			uint32_t filled = page.filled();
			keys = filled / BTREE_ENTRY_SIZE;
			pointers = (filled + BTREE_KEY_INDEX_SIZE) / BTREE_ENTRY_SIZE;
		}
		else
		{
			keys = BTREE_KEYS_PER_PAGE;
			pointers = BTREE_KEYS_PER_PAGE + 1;
		}
		index = find_key(test, page.internal, keys, found);
		if(*found)
			return page.internal[index].rec.index;
		if(index >= pointers)
			return header.key_count;
		pointer = page.internal[index].lt_ptr;
		full = pointer <= header.last_full;
		page.page = btree->page(pointer);
		assert(page.page);
		depth++;
	}
	
	/* scan the leaf page */
	if(!full)
	{
		uint32_t filled = page.filled();
		keys = filled / BTREE_KEY_INDEX_SIZE;
	}
	else
		keys = BTREE_KEYS_PER_LEAF_PAGE;
	index = find_key(test, page.leaf, keys, found);
	return *found ? page.leaf[index].index : header.key_count;
}

uint32_t btree_dtable::page_union::filled() const
{
	return *(const uint32_t *) &bytes[BTREE_PAGE_SIZE - sizeof(uint32_t)];
}

/* returns true if the page fills */
bool btree_dtable::page_stack::page::append_pointer(size_t pointer)
{
	assert(filled + sizeof(uint32_t) <= BTREE_PAGE_SIZE);
	*(uint32_t *) &data[filled] = pointer;
	filled += sizeof(uint32_t);
	return filled == BTREE_PAGE_SIZE;
}

/* returns true if the page fills */
bool btree_dtable::page_stack::page::append_record(uint32_t key, size_t index)
{
	assert(filled + 2 * sizeof(uint32_t) <= BTREE_PAGE_SIZE);
	*(uint32_t *) &data[filled] = key;
	*(uint32_t *) &data[filled += sizeof(uint32_t)] = index;
	filled += sizeof(uint32_t);
	return filled == BTREE_PAGE_SIZE;
}

bool btree_dtable::page_stack::page::write(int fd, size_t page)
{
	assert(filled == BTREE_PAGE_SIZE);
	ssize_t r = pwrite(fd, data, BTREE_PAGE_SIZE, page * BTREE_PAGE_SIZE);
	if(r != BTREE_PAGE_SIZE)
		return false;
	filled = 0;
	return true;
}

void btree_dtable::page_stack::page::pad()
{
	assert(filled <= BTREE_PAGE_SIZE - sizeof(uint32_t));
	memset(&data[filled], 0, BTREE_PAGE_SIZE - filled);
	/* we store the amount the page is filled into the last 32 bits */
	*(uint32_t *) &data[BTREE_PAGE_SIZE - sizeof(uint32_t)] = filled;
	filled = BTREE_PAGE_SIZE;
}

btree_dtable::page_stack::page_stack(int fd, size_t key_count)
	: fd(fd), next_file_page(1), filled(false), flushed(false)
{
	depth = btree_depth(key_count);
	assert(depth > 0);
	pages = new page[depth];
	next_depth = depth - 1;
	
	header.magic = BTREE_DTABLE_MAGIC;
	header.version = BTREE_DTABLE_VERSION;
	header.page_size = BTREE_PAGE_SIZE;
	header.pageno_size = BTREE_PAGENO_SIZE;
	header.key_size = BTREE_KEY_SIZE;
	header.index_size = BTREE_INDEX_SIZE;
	header.key_type = 1; /* 1 -> uint32 */
	header.key_count = key_count;
	header.depth = depth;
	/* to be filled in later */
	header.root_page = 0;
	header.last_full = 0;
}

btree_dtable::page_stack::~page_stack()
{
	if(!flushed)
		flush();
	delete[] pages;
}

int btree_dtable::page_stack::add(uint32_t key, size_t index)
{
	assert(!filled && !flushed);
	if(pages[next_depth].append_record(key, index))
	{
		size_t pointer = next_file_page++;
		if(!pages[next_depth].write(fd, pointer))
			/* FIXME: do better than this */
			abort();
		add(pointer);
	}
	else
		/* this will usually be the case already */
		next_depth = depth - 1;
	return 0;
}

int btree_dtable::page_stack::add(size_t pointer)
{
	assert(!filled);
	if(!next_depth)
	{
		header.root_page = pointer;
		filled = true;
		return 0;
	}
	next_depth--; /* check for -1, bail out (full) */
	if(pages[next_depth].append_pointer(pointer))
	{
		pointer = next_file_page++;
		if(!pages[next_depth].write(fd, pointer))
			/* FIXME: do better than this */
			abort();
		add(pointer);
	}
	return 0;
}

int btree_dtable::page_stack::flush()
{
	ssize_t r;
	assert(!flushed);
	if(!filled)
	{
		if(pages[next_depth].empty() && next_depth)
			/* the next page is empty, so don't pad and write it */
			next_depth--;
		header.last_full = next_file_page - 1;
		while(!filled)
		{
			size_t pointer = next_file_page++;
			pages[next_depth].pad();
			pages[next_depth].write(fd, pointer);
			/* ultimately, this will set filled */
			add(pointer);
		}
	}
	else
		header.last_full = header.root_page;
	r = pwrite(fd, &header, sizeof(header), 0);
	if(r != sizeof(header))
		return (r < 0) ? r : -1;
	flushed = true;
	return 0;
}

size_t btree_dtable::page_stack::btree_depth(size_t key_count)
{
	size_t depth = 1;
	uint64_t internal = 0, leaf = 1;
	/* there's probably some neat closed form way to do this, but this loop
	 * should only run log(key_count) times, base BTREE_KEYS_PER_PAGE */
	while(internal * BTREE_KEYS_PER_PAGE + leaf * BTREE_KEYS_PER_LEAF_PAGE < key_count)
	{
		internal += leaf;
		leaf *= BTREE_KEYS_PER_PAGE + 1;
		depth++;
	}
	return depth;
}

int btree_dtable::write_btree(int dfd, const char * name, const dtable * base)
{
	/* OK, here's how this works. We find out how many keys there are, and
	 * figure out what the topology of the btree will be based on that.
	 * (That is, how deep it is.) Then we do an in-order traversal of the
	 * virtual btree, filling in the keys during a single iteration over the
	 * source dtable data.
	 * 
	 * We write pages out to the btree file as they fill; because pages
	 * closer to the root of the tree will only fill after all the pages
	 * they point at fill, all the downward page number pointers will be
	 * known when it is time to write out a page. Further, we'll only need
	 * to keep log(n) pages in memory: the ones from the current position in
	 * the btree traversal up to the root. (That's base BTREE_KEYS_PER_PAGE
	 * log, even.)
	 * 
	 * After all this is done, the last page written will be the root page,
	 * so we don't actually need to store its location to the file header.
	 * Nevertheless, we do anyway, as a precaution. */
	int r = -1, fd;
	size_t count = base->size();
	dtable::iter * base_iter;
	
	assert(count != (size_t) -1);
	
	/* for the moment, we only support integer keys */
	if(base->key_type() != dtype::UINT32)
		return -ENOSYS;
	
	fd = openat(dfd, name, O_WRONLY | O_CREAT, 0644);
	if(fd < 0)
		return fd;
	
	page_stack stack(fd, count);
	
	base_iter = base->iterator();
	if(!base_iter)
		goto fail_iter;
	
	while(base_iter->valid())
	{
		dtype key = base_iter->key();
		size_t index = base_iter->get_index();
		assert(key.type == dtype::UINT32);
		base_iter->next();
		r = stack.add(key.u32, index);
		if(r < 0)
			goto fail_write;
	}
	r = stack.flush();
	if(r < 0)
		goto fail_write;
	
	delete base_iter;
	close(fd);
	return 0;
	
fail_write:
	unlinkat(dfd, name, 0);
	delete base_iter;
fail_iter:
	close(fd);
	unlinkat(dfd, name, 0);
	return r;
}

int btree_dtable::create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow)
{
	int bt_dfd, r;
	params base_config;
	dtable * base_dtable;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	if(!base)
		return -EINVAL;
	if(!base->indexed_access())
		return -ENOSYS;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	bt_dfd = openat(dfd, file, 0);
	if(bt_dfd < 0)
		goto fail_open;
	
	r = base->create(bt_dfd, "base", base_config, source, shadow);
	if(r < 0)
		goto fail_create;
	
	base_dtable = base->open(bt_dfd, "base", base_config);
	if(!base_dtable)
		goto fail_reopen;
	
	r = write_btree(bt_dfd, "btree", base_dtable);
	if(r < 0)
		goto fail_write;
	
	delete base_dtable;
	
	close(bt_dfd);
	return 0;
	
fail_write:
	delete base_dtable;
fail_reopen:
	util::rm_r(bt_dfd, "base");
fail_create:
	close(bt_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return -1;
}

DEFINE_RO_FACTORY(btree_dtable);
