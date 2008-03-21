/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STRINGSET_H
#define __STRINGSET_H

#include <stdlib.h>
#include <stdint.h>

#include "hash_map.h"

#ifndef __cplusplus
#error stringset.h is a C++ header file
#endif

/* This class provides a simple wrapper around a string hash table to get unique
 * string instances. The add() and lookup() methods return a pointer to a copy
 * of the parameter string which is kept in an internal hash table; subsequent
 * calls with equivalent strings will return the same pointer. */

class stringset
{
public:
	inline stringset();
	inline ~stringset();
	
	int init(bool reverse = false);
	void deinit();
	bool ready();
	
	bool remove(const char * string);
	const char * add(const char * string, uint32_t * index = NULL);
	const char * lookup(const char * string, uint32_t * index = NULL);
	const char * lookup(uint32_t index);
	const char ** array(bool empty = true);
	size_t size();
	
private:
	uint32_t next_index;
	hash_map_t * string_map;
	hash_map_t * index_map;
};

inline stringset::stringset()
	: next_index(0), string_map(NULL), index_map(NULL)
{
}

inline stringset::~stringset()
{
	if(string_map)
		deinit();
}

#endif /* __STRINGSET_H */
