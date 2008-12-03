/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
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
#include "dtable.h"
#include "dtable_factory.h"

/* The btree dtable must be created with another read-only dtable, and builds a
 * btree key index for it. The base dtable must support indexed access. */

#define BTREE_DTABLE_MAGIC 0xB2815C66
#define BTREE_DTABLE_VERSION 0

#define BTREE_PAGE_SIZE 4096

class btree_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	virtual blob index(size_t index) const;
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
	
	inline btree_dtable() : base(NULL) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline virtual ~btree_dtable()
	{
		if(base)
			deinit();
	}
	
	static inline bool static_indexed_access() { return true; }
	
	static int create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow = NULL);
	DECLARE_RO_FACTORY(btree_dtable);
	
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
	} __attribute__((packed));
	
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
			inline page() : filled(0) { }
			inline bool append_pointer(size_t pointer);
			inline bool append_record(uint32_t key, size_t index);
			inline bool write(int fd, size_t page);
			inline bool empty() { return !filled; }
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
	
	class iter : public dtable::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool first();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual bool seek_index(size_t index);
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(dtable::iter * base, const btree_dtable * source);
		virtual ~iter() { }
		
	private:
		dtable::iter * base_iter;
		const btree_dtable * bdt_source;
	};
	
	dtable * base;
	
	static size_t btree_depth(size_t key_count);
	static int write_btree(int dfd, const char * name, const dtable * base);
};

#endif /* __BTREE_DTABLE_H */
