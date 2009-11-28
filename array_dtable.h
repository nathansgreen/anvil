/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ARRAY_DTABLE_H
#define __ARRAY_DTABLE_H

#ifndef __cplusplus
#error array_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"

class rofile;

#define ADTABLE_MAGIC 0x69AD02D3
#define ADTABLE_VERSION 2

/* The array_dtable stores an array of blobs, all the same size. The keys must
 * be integers, and they are used to index into the file to retrieve the blobs.
 * Gaps in the keys are supported either by reserving a special value to mean a
 * hole, or by storing an additional byte before each value indicating whether
 * it is a hole or not. Nonexistent values are handled similarly. */

/* values for the tag byte */
#define ARRAY_INDEX_HOLE 0
#define ARRAY_INDEX_DNE 1
#define ARRAY_INDEX_VALID 2


class array_dtable : public dtable
{
public:
	virtual iter * iterator(ATX_OPT) const;
	virtual bool present(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob lookup(const dtype & key, bool * found, ATX_OPT) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	inline virtual size_t size() const { return key_count; }
	
	static inline bool static_indexed_access(const params & config) { return true; }
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(array_dtable);
	
	inline array_dtable() : fp(NULL), min_key(0), array_size(0), value_size(0) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	void deinit();
	
protected:
	inline virtual ~array_dtable()
	{
		if(fp)
			deinit();
	}
	
private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t min_key;
		uint32_t key_count;
		uint32_t array_size;
		uint32_t value_size;
		uint8_t tag_byte;
		uint8_t hole, dne;
	} __attribute__((packed));
	
	class iter : public iter_source<array_dtable>
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
		inline iter(const array_dtable * source);
		virtual ~iter() {}
		
	private:
		size_t index;
	};
	
	blob get_value(size_t index, bool * found) const;
	int find_key(const dtype_test & test, size_t * index) const;
	uint8_t index_type(size_t index, off_t * offset = NULL) const;
	bool is_hole(size_t index) const;
	
	rofile * fp;
	uint32_t min_key;
	size_t key_count;
	size_t array_size;
	size_t value_size;
	bool tag_byte;
	/* these are used if tag_byte is false */
	blob hole_value;
	blob dne_value;
	/* the start of the data array */
	off_t data_start;
};

#endif /* __ARRAY_DTABLE_H */
