/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BTREE_DTABLE_H
#define __BTREE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error btree_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "rofile.h"
#include "dtable.h"
#include "dtable_factory.h"
#include "dtable_wrap_iter.h"

/* The btree dtable must be created with another read-only dtable, and builds a
 * btree key index for it. The base dtable must support indexed access. */

#define BTREE_DTABLE_MAGIC 0xB2815C66
#define BTREE_DTABLE_VERSION 1

#define BTREE_PAGE_KB 4
#define BTREE_PAGE_SIZE (BTREE_PAGE_KB * 1024)

class btree_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	virtual size_t size() const;
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		int value = base->set_blob_cmp(cmp);
		if(value >= 0)
		{
			value = dtable::set_blob_cmp(cmp);
			assert(value >= 0);
		}
		return value;
	}
	
	static inline bool static_indexed_access(const params & config) { return true; }
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(btree_dtable);
	
	inline btree_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	
protected:
	inline virtual ~btree_dtable()
	{
		if(base)
			deinit();
	}
	
private:
	struct btree_dtable_header
	{
		uint32_t magic;
		uint32_t version;
		uint32_t page_size;
		uint8_t pageno_size;
		uint8_t key_size;
		uint8_t index_size;
		uint8_t key_type;
		uint32_t key_count;
		uint32_t depth;
		uint32_t root_page;
		uint32_t last_full;
	} __attribute__((packed));
	struct record
	{
		uint32_t key;
		uint32_t index;
		inline uint32_t get_key() const { return key; }
	} __attribute__((packed));
	struct entry
	{
		uint32_t lt_ptr;
		record rec;
		inline uint32_t get_key() const { return rec.get_key(); }
	} __attribute__((packed));
	union page_union
	{
		const void * page;
		const uint8_t * bytes;
		const record * leaf;
		const entry * internal;
		/* gets the value of filled stored by page::pad() */
		inline uint32_t filled() const;
	};
	
	class page_stack
	{
	public:
		page_stack(int fd, size_t key_count);
		~page_stack();
		
		int add(uint32_t key, size_t index);
		int flush();
		
		static size_t btree_depth(size_t key_count);
		
	private:
		class page
		{
		public:
			inline page() : filled(0) {}
			inline bool append_pointer(size_t pointer);
			inline bool append_record(uint32_t key, size_t index);
			inline bool write(int fd, size_t page);
			inline bool empty() const { return !filled; }
			inline void pad();
		private:
			size_t filled;
			uint8_t data[BTREE_PAGE_SIZE];
		};
		
		int fd;
		size_t depth;
		size_t next_depth, next_file_page;
		page * pages;
		btree_dtable_header header;
		bool filled, flushed;
		
		int add(size_t pointer);
	};
	
	class iter : public iter_source<btree_dtable, dtable_wrap_iter>
	{
	public:
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		inline iter(dtable::iter * base, const btree_dtable * source);
		virtual ~iter() {}
	};
	
	dtable * base;
	rofile * btree;
	btree_dtable_header header;
	
	template<class T, class U>
	static size_t find_key(const T & test, const U * entries, size_t count, bool * found);
	
	inline size_t btree_lookup(const dtype & key, bool * found) const
	{
		return btree_lookup(dtype_static_test(key, blob_cmp), found);
	}
	template<class T>
	size_t btree_lookup(const T & test, bool * found) const;
	
	static int write_btree(int dfd, const char * name, const dtable * base);
};

#endif /* __BTREE_DTABLE_H */
