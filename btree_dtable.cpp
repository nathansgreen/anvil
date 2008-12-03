/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
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
 * | page# | <key, index> | page# | <key, index> | ... | page# |
 * 
 * or a leaf page, like this:
 * 
 * | <key, index> | <key, index> | ... | <key, index> |
 * 
 * As page numbers, keys, and indices are all 32 bits, we can fit 341 <key,
 * index> pairs per 4K internal page, with 342 page numbers between them, and
 * 512 <key, index> pairs per 4K leaf page. We can tell the difference between
 * the two page types by the (runtime-known) depth of the page from the root. */

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
	if(header.key_type != 1 || !header.root_page)
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

size_t btree_dtable::btree_lookup(const dtype & key, bool * found) const
{
	/* TODO: avoid the virtual function call overhead of dtype_test */
	return btree_lookup(dtype_fixed_test(key), found);
}

size_t btree_dtable::btree_lookup(const dtype_test & test, bool * found) const
{
	/* TODO: to do a binary search within the pages here, we need to know
	 * how many keys are actually on the page - normally they will be full,
	 * but some of the later pages may be only partially filled... */
	/* (note that to have the same number of actual key comparisons as a
	 * regular binary search would, we must do a binary search here) */
	struct record
	{
		uint32_t key;
		uint32_t index;
	} __attribute__((packed));
	struct entry
	{
		uint32_t lt_ptr;
		record rec;
		/* keeps gt_ptr out of sizeof() */
		uint32_t gt_ptr[0];
	} __attribute__((packed));
	union
	{
		const void * page;
		const record * leaf;
		const entry * internal;
	} page;
	int c;
	dtype key(0u);
	size_t depth = 1, index = header.key_count;
	page.page = btree->page(header.root_page);
	*found = false;
	
	while(depth < header.depth)
	{
		size_t pointer = 0;
		/* scan the internal page */
		for(size_t i = 0; i < BTREE_KEYS_PER_PAGE; i++)
		{
			if(!page.internal[i].lt_ptr)
				return index;
			if(!page.internal[i].rec.index)
			{
				/* there are no further keys on this internal page,
				 * so we should follow the last pointer down */
				pointer = page.internal[i].lt_ptr;
				break;
			}
			key.u32 = page.internal[i].rec.key;
			c = test(key);
			if(c > 0)
			{
				pointer = page.internal[i].lt_ptr;
				/* we found something larger, so if we don't find
				 * anything closer this will be the correct result */
				index = page.internal[i].rec.index - 1;
				break;
			}
			if(!c)
			{
				*found = true;
				return page.internal[i].rec.index - 1;
			}
		}
		if(!pointer)
		{
			pointer = page.internal[BTREE_KEYS_PER_PAGE - 1].gt_ptr[0];
			if(!pointer)
				return index;
		}
		page.page = btree->page(pointer);
		depth++;
	}
	
	/* scan the leaf page */
	for(size_t i = 0; i < BTREE_KEYS_PER_LEAF_PAGE; i++)
	{
		if(!page.leaf[i].index)
			return index;
		key.u32 = page.leaf[i].key;
		c = test(key);
		if(c >= 0)
		{
			*found = !c;
			return page.leaf[i].index - 1;
		}
	}
	
	return index;
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
	/* all indices are stored incremented by 1 so 0 can mean "no record" */
	*(uint32_t *) &data[filled += sizeof(uint32_t)] = index + 1;
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
	memset(&data[filled], 0, BTREE_PAGE_SIZE - filled);
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
	else if(next_depth < depth - 1)
		next_depth++;
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
		while(!filled)
		{
			size_t pointer = next_file_page++;
			pages[next_depth].pad();
			pages[next_depth].write(fd, pointer);
			/* ultimately, this will set filled */
			add(pointer);
		}
	}
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
		leaf *= BTREE_KEYS_PER_PAGE;
		depth++;
	}
	/* we expect this to be true if the integers did not overflow */
	assert(depth <= 4);
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
	
	for(size_t index = 0; index < count; index++)
	{
		assert(base_iter->valid());
		dtype key = base_iter->key();
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
