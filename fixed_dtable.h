/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __FIXED_DTABLE_H
#define __FIXED_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>


#ifndef __cplusplus
#error fixed_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"
#include "stringtbl.h"

class rofile;

/* The fixed dtable stores blobs of a fixed size. This allows it to omit the
 * size and offset fields that are present in simple_dtable, and to store the
 * values in the same array with the keys. These dtables are read-only once they
 * are created with the ::create() method. */

#define FDTABLE_MAGIC 0x89B63A8E
#define FDTABLE_VERSION 1

class fixed_dtable : public dtable
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
	DECLARE_RO_FACTORY(fixed_dtable);
	
	inline fixed_dtable() : fp(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~fixed_dtable()
	{
		if(fp)
			deinit();
	}
	
private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t key_count;
		uint32_t value_size;
		uint8_t key_type;
		uint8_t key_size;
	} __attribute__((packed));
	
	class iter : public iter_source<fixed_dtable>
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
		inline iter(const fixed_dtable * source);
		virtual ~iter() {}
	private:
		size_t index;
	};
	
	dtype get_key(size_t index, bool * data_exists = NULL, off_t * data_offset = NULL) const;
	inline int find_key(const dtype & key, bool * data_exists, off_t * data_offset = NULL, size_t * index = NULL) const
	{
		return find_key(dtype_static_test(key, blob_cmp), index, data_exists, data_offset);
	}
	template<class T>
	int find_key(const T & test, size_t * index, bool * data_exists = NULL, off_t * data_offset = NULL) const;
	blob get_value(size_t index, off_t data_offset) const;
	blob get_value(size_t index) const;
	
	rofile * fp;
	size_t key_count;
	size_t value_size, record_size;
	stringtbl st;
	uint8_t key_size;
	off_t key_start_off;
};

#endif /* __FIXED_DTABLE_H */
