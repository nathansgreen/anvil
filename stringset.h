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
#include <set>

#include "istr.h"
#include "blob.h"
#include "blob_comparator.h"

/* This class provides a simple wrapper around an std::map to get unique string
 * instances. The add() and lookup() methods return a reference to an internally
 * maintained istr instance; subsequent calls with equivalent strings will
 * return the same reference. Blobs are also supported, but not in reverse. */

class stringset
{
public:
	inline stringset() : reverse(false), next_index(0), blob_cmp(NULL), blobs(blob_cmp) {}
	
	int init(const blob_comparator * blob_cmp, bool keep_reverse = false);
	
	bool remove(const istr & string);
	bool remove(const blob & string);
	const istr & add(const istr & string, uint32_t * index = NULL);
	const blob & add(const blob & blob);
	const istr & lookup(const istr & string, uint32_t * index = NULL) const;
	const blob & lookup(const blob & blob) const;
	const istr & lookup(uint32_t index) const;
	
	/* returns an array of the strings; the array must be free()d, but the
	 * strings must be left alone */
	const char ** array() const;
	/* returns an array of the blobs; the array must be delete[]d */
	blob * blob_array() const;
	
	inline size_t size() const
	{
		return string_map.size();
	}
	
	inline size_t blob_size() const
	{
		return blobs.size();
	}
	
private:
	/* /me dislikes std::map immensely */
	typedef std::map<istr, uint32_t, strcmp_less> istr_map;
	typedef std::map<uint32_t, istr> idx_map;
	typedef std::set<blob, blob_comparator_refobject> blob_set;
	
	bool reverse;
	uint32_t next_index;
	const blob_comparator * blob_cmp;
	istr_map string_map;
	idx_map index_map;
	blob_set blobs;
};

#endif /* __STRINGSET_H */
