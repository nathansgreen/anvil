/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __COUNTED_STRINGSET_H
#define __COUNTED_STRINGSET_H

#include <stdint.h>
#include <string.h>

#ifndef __cplusplus
#error counted_stringset.h is a C++ header file
#endif

#include <map>
#include <set>

#include "istr.h"

/* This class provides a simple wrapper around an std::map to get unique string
 * instances. The add() and lookup() methods return a reference to an internally
 * maintained istr instance; subsequent calls with equivalent strings will
 * return the same reference. */

class counted_stringset
{
public:
	inline counted_stringset() : max(0) {}
	
	void init(size_t max_strings = 0);
	
	/* returns the number of times this string has been added */
	size_t add(const istr & string);
	size_t lookup(const istr & string) const;
	/* drop all entries with counts less than min */
	void ignore(size_t min = 2);
	/* returns an array of the strings; the array must be free()d, but the
	 * strings must be left alone */
	const char ** array() const;
	inline size_t size() const
	{
		return strings.size();
	}
	
private:
	struct count_istr
	{
		inline count_istr(size_t count, const istr & string) : count(count), string(string) {}
		/* default copy constructor and assignment operator are fine */
		inline bool operator==(const count_istr & x) const
		{
			if(count != x.count)
				return false;
			return !strcmp(string, x.string);
		}
		inline bool operator<(const count_istr & x) const
		{
			if(count == x.count)
				return strcmp(string, x.string) < 0;
			return count < x.count;
		}
		inline operator const istr &() const { return string; }
		inline operator size_t() const { return count; }
		private:
			size_t count;
			istr string;
	};
	
	/* /me dislikes std::map immensely */
	typedef std::map<istr, size_t, strcmp_less> string_map;
	typedef std::set<count_istr> count_set;
	
	size_t max;
	string_map strings;
	count_set counts;
};

#endif /* __COUNTED_STRINGSET_H */
