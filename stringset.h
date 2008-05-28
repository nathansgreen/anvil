/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STRINGSET_H
#define __STRINGSET_H

#include <stdint.h>
#include <string.h>

#ifndef __cplusplus
#error stringset.h is a C++ header file
#endif

#include <map>

#include "istr.h"

/* This class provides a simple wrapper around an std::map to get unique string
 * instances. The add() and lookup() methods return a reference to an internally
 * maintained istr instance; subsequent calls with equivalent strings will
 * return the same reference. */

class stringset
{
public:
	inline stringset() : reverse(false), next_index(0) {}
	
	int init(bool keep_reverse = false);
	
	bool remove(const istr & string);
	const istr & add(const istr & string, uint32_t * index = NULL);
	const istr & lookup(const istr & string, uint32_t * index = NULL) const;
	const istr & lookup(uint32_t index) const;
	/* returns an array of the strings; the array must be free()d, but the
	 * strings must be left alone */
	const char ** array() const;
	size_t size() const;
	
private:
	/* /me dislikes std::map immensely */
	typedef std::map<istr, uint32_t, strcmp_less> istr_map;
	typedef std::map<uint32_t, istr> idx_map;
	
	bool reverse;
	uint32_t next_index;
	istr_map string_map;
	idx_map index_map;
};

#endif /* __STRINGSET_H */
