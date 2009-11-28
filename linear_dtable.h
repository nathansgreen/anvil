/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __LINEAR_DTABLE_H
#define __LINEAR_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error linear_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"

class rofile;

/* The linear dtable is like the simple dtable in that it can store values of
 * any size, but like the array dtable in that the keys must be contiguous
 * integers. It can store holes and nonexistent values without special help,
 * unlike the array dtable, since it does not store *only* the values. */

#define LDTABLE_MAGIC 0xCB001E65
#define LDTABLE_VERSION 0

class linear_dtable : public dtable
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
	DECLARE_RO_FACTORY(linear_dtable);
	
	inline linear_dtable() : fp(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	void deinit();
	
protected:
	inline virtual ~linear_dtable()
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
		uint8_t length_size;
		uint8_t offset_size;
	} __attribute__((packed));
	
	class iter : public iter_source<linear_dtable>
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
		virtual size_t get_index() const;
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const linear_dtable * source);
		virtual ~iter() {}
		
	private:
		size_t index;
	};
	
	bool get_index(size_t index, size_t * data_length = NULL, off_t * data_offset = NULL) const;
	blob get_value(size_t index, bool * found) const;
	int find_key(const dtype_test & test, size_t * index) const;
	bool is_hole(size_t index) const;
	
	rofile * fp;
	size_t min_key, key_count, array_size;
	uint8_t length_size, offset_size;
	off_t data_start_off;
};

#endif /* __LINEAR_DTABLE_H */
