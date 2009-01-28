/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __USSTATE_DTABLE_H
#define __USSTATE_DTABLE_H

#ifndef __cplusplus
#error usstate_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "rofile.h"
#include "dtable_factory.h"

#define USSDTABLE_MAGIC 0xC8FFEFA3
#define USSDTABLE_VERSION 0

/* 50 states plus DC and Puerto Rico */
#define USSTATE_COUNT 52
#define USSTATE_INDEX_DNE USSTATE_COUNT
#define USSTATE_INDEX_HOLE (USSTATE_COUNT + 1)

class usstate_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	inline virtual size_t size() const { return key_count; }
	
	inline usstate_dtable() : fp(NULL), min_key(0), array_size(0), value_size(0) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	static inline bool static_indexed_access() { return true; }
	static dtable::iter * filter_iterator(dtable::iter * source, const params & config, dtable * rejects);
	inline virtual ~usstate_dtable()
	{
		if(fp)
			deinit();
	}
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(usstate_dtable);
	
private:
	static const blob state_codes[USSTATE_COUNT + 1];
	
	inline void static_asserts()
	{
		/* we're using one byte to store these things */
		static_assert(USSTATE_INDEX_HOLE <= 255);
	}
	
	/* we'll define it in the source file */
	class usstate_filter;
	
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t min_key;
		uint32_t key_count;
		uint32_t array_size;
	} __attribute__((packed));
	
	class iter : public iter_source<usstate_dtable>
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool last();
		virtual bool first();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual bool seek_index(size_t index);
		virtual size_t get_index() const;
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const usstate_dtable * source);
		virtual ~iter() {}
		
	private:
		size_t index;
	};
	
	dtype get_key(size_t index) const;
	blob get_value(size_t index, bool * found) const;
	int find_key(const dtype_test & test, size_t * index) const;
	uint8_t index_value(size_t index) const;
	inline bool is_hole(size_t index) const { return index_value(index) == USSTATE_INDEX_HOLE; }
	
	rofile * fp;
	uint32_t min_key;
	size_t key_count;
	size_t array_size;
	size_t value_size;
};

#endif /* __USSTATE_DTABLE_H */
