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

/* A string table is a section of a file which maintains a collection of unique
 * strings in sorted order. String tables are immutable once created. */

#define ST_LRU 16

class stringtbl
{
public:
	inline stringtbl() : fd(-1) {}
	inline ~stringtbl()
	{
		if(fd >= 0)
			deinit();
	}
	
	int init(int fd, off_t start);
	void deinit();
	
	inline size_t get_size()
	{
		return size;
	}
	
	/* The return value of get() is good until at least ST_LRU
	 * more calls to get(), or one call to locate(). */
	const char * get(ssize_t index) const;
	ssize_t locate(const char * string) const;
	
	const char ** read() const;
	
	static void array_sort(const char ** array, ssize_t count);
	static void array_free(const char ** array, ssize_t count);

	/* leaves the input string array sorted */
	static int create(int fd, off_t * start, const char ** strings, ssize_t count);
	static int combine(int fd, off_t * start, const stringtbl * st1, const stringtbl * st2);

private:
	struct lru_ent
	{
		ssize_t index;
		const char * string;
	};
	
	int fd;
	off_t start;
	ssize_t count;
	size_t size;
	uint8_t bytes[3];
	mutable lru_ent lru[ST_LRU];
	int lru_next;
};

#endif /* __STRINGTBL_H */
