/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_DTABLE_H
#define __SIMPLE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "stringtbl.h"

#ifndef __cplusplus
#error simple_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "dtable_factory.h"
#include "rofile.h"

/* The simple dtable does nothing fancy to store the blobs efficiently. It just
 * stores the key and the blob literally, including size information. These
 * dtables are read-only once they are created with the ::create() method. */

/* Custom versions of this class are definitely expected, to store the data more
 * efficiently given knowledge of what it will probably be. If such a class
 * cannot store a requested value, it should use the reject() method on the
 * input iterator to attempt to get an alternate value to store. See dtable.h
 * for further information. */

#define SDTABLE_MAGIC 0xF029DDE3
#define SDTABLE_VERSION 1

class simple_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	virtual blob index(size_t index) const;
	virtual bool contains_index(size_t index) const;
	inline virtual size_t size() const { return key_count; }
	
	inline simple_dtable() : fp(NULL) {}
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline virtual ~simple_dtable()
	{
		if(fp)
			deinit();
	}
	
	static inline bool static_indexed_access(const params & config) { return true; }
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(simple_dtable);
	
private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t key_count;
		uint8_t key_type;
		uint8_t key_size;
		uint8_t length_size;
		uint8_t offset_size;
	} __attribute__((packed));
	
	class iter : public iter_source<simple_dtable>
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
		inline iter(const simple_dtable * source);
		virtual ~iter() {}
	private:
		size_t index;
	};
	
	dtype get_key(size_t index, size_t * data_length = NULL, off_t * data_offset = NULL, bool lock = true) const;
	inline int find_key(const dtype & key, size_t * data_length, off_t * data_offset = NULL, size_t * index = NULL) const
	{
		return find_key(dtype_static_test(key, blob_cmp), index, data_length, data_offset);
	}
	template<class T>
	int find_key(const T & test, size_t * index, size_t * data_length = NULL, off_t * data_offset = NULL) const;
	blob get_value(size_t data_length, off_t data_offset) const;
	blob get_value(size_t index) const;
	
	rofile * fp;
	size_t key_count;
	stringtbl st;
	uint8_t key_size, length_size, offset_size;
	off_t key_start_off, data_start_off;
};

#endif /* __SIMPLE_DTABLE_H */
