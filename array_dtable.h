/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ARRAY_DTABLE_H
#define __ARRAY_DTABLE_H

#ifndef __cplusplus
#error array_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "rofile.h"
#include "dtable_factory.h"

#define ADTABLE_MAGIC 0x69AD02D3
#define ADTABLE_VERSION 1

/* array tables */

class array_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	inline array_dtable() : fp(NULL), key_count(0) {}
	int init(int dfd, const char * file, const params & config);
	dtype get_key(size_t index) const;
	blob get_value(size_t index, bool * found) const;
	static inline bool static_indexed_access() { return true; }
	void deinit();
	inline virtual ~array_dtable() { deinit(); }
	static int create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow = NULL);
	DECLARE_RO_FACTORY(array_dtable);

private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t key_count;
		uint32_t value_size;
	} __attribute__((packed));

	class iter : public dtable::iter
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
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const array_dtable * source);
		virtual ~iter() {}

	private:
		size_t index;
		const array_dtable * bdt_source;
	};

	inline int find_key(const dtype & key, size_t * index) const
	{
		return find_key(dtype_static_test(key, blob_cmp), index);
	}
	template<class T>
	int find_key(const T & test, size_t * index) const;
	bool exists(size_t index) const;

	rofile * fp;
	uint32_t value_size;
	size_t key_count;
};

#endif /* __ARRAY_DTABLE_H */
