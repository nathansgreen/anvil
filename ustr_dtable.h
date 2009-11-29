/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __USTR_DTABLE_H
#define __USTR_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "stringtbl.h"

#ifndef __cplusplus
#error ustr_dtable.h is a C++ header file
#endif

#include <vector>

#include "dtable_factory.h"

class rofile;

/* The ustr (unique string) dtable is designed with simple_ctable in mind. It
 * scans the blobs for column names as stored by simple_ctable with sub_blob,
 * and attempts to store them in a unique string table once in the file rather
 * than once per key. It looks for a string length byte followed by a printable
 * string of that length; a table of such strings (when they are repeated a
 * sufficient number of times) is stored at the end of the file and references
 * to it are stored in the blobs in place of the original strings. Like
 * simple_dtable, these dtables are read-only once they are created with the
 * ::create() method. */

#define USDTABLE_MAGIC 0xABB9D449
#define USDTABLE_VERSION 1

class ustr_dtable : public dtable
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
	DECLARE_RO_FACTORY(ustr_dtable);
	
	inline ustr_dtable() : fp(NULL) {}
	int init(int dfd, const char * file, const params & config, sys_journal * sysj);
	
protected:
	void deinit();
	inline virtual ~ustr_dtable()
	{
		if(fp)
			deinit();
	}
	
private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t key_count;
		uint8_t key_type;
		uint8_t key_size;
		uint8_t length_size;
		uint8_t offset_size;
		uint32_t dup_offset;
		uint8_t dup_index_size;
		uint8_t dup_escape_len;
		uint8_t dup_escape[2];
	} __attribute__((packed));
	
	class iter : public iter_source<ustr_dtable>
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
		inline iter(const ustr_dtable * source);
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
	blob unpack_blob(const blob & source, size_t unpacked_size) const;
	blob get_value(size_t index, size_t data_length, off_t data_offset) const;
	blob get_value(size_t index) const;
	
	/* helpers for create() above */
	static size_t pack_size(const blob & source, const dtable_header & header, const std::vector<istr> & dups);
	static blob pack_blob(const blob & source, const dtable_header & header, const std::vector<istr> & dups);
	
	rofile * fp;
	size_t key_count;
	stringtbl st, dup;
	uint8_t key_size, length_size, offset_size;
	uint8_t dup_index_size, dup_escape_len, dup_escape[2];
	off_t key_start_off, data_start_off;
};

#endif /* __USTR_DTABLE_H */
