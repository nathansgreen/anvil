/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STRINGTBL_H
#define __STRINGTBL_H

#include <stdint.h>
#include <sys/types.h>

#ifndef __cplusplus
#error stringtbl.h is a C++ header file
#endif

#include "blob.h"
#include "rofile.h"
#include "rwfile.h"
#include "blob_comparator.h"

/* A string table is a section of a file which maintains a collection of unique
 * strings in sorted order. String tables are immutable once created. */

#define ST_LRU 16

class stringtbl
{
public:
	inline stringtbl() : fp(NULL) {}
	inline ~stringtbl()
	{
		if(fp)
			deinit();
	}
	
	int init(const rofile * fp, off_t start);
	void deinit();
	
	inline size_t get_size()
	{
		return size;
	}
	
	inline bool is_binary()
	{
		return binary;
	}
	
	/* The return value of get() is good until at least ST_LRU
	 * more calls to get(), or one call to locate(). */
	const char * get(ssize_t index) const;
	const blob & get_blob(ssize_t index) const;
	ssize_t locate(const char * string) const;
	ssize_t locate(const blob & search, const blob_comparator * blob_cmp = NULL) const;
	
	static void array_sort(const char ** array, ssize_t count);
	static void array_sort(blob * array, ssize_t count, const blob_comparator * blob_cmp);

	/* these functions leave the input arrays sorted */
	static int create(rwfile * fp, const char ** strings, ssize_t count, bool need_sort = false);
	/* makes a binary table */
	static int create(rwfile * fp, blob * blobs, ssize_t count, const blob_comparator * blob_cmp = NULL, bool need_sort = false);

private:
	struct lru_ent
	{
		ssize_t index;
		const char * string;
		blob binary;
	};
	
	const rofile * fp;
	off_t start;
	ssize_t count;
	size_t size;
	uint8_t bytes[3];
	bool binary;
	mutable lru_ent lru[ST_LRU];
	int lru_next;
};

#endif /* __STRINGTBL_H */
