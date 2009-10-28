/* This file is part of Anvil. Anvil is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __STRING_COUNTER_H
#define __STRING_COUNTER_H

#include <stdint.h>
#include <string.h>

#ifndef __cplusplus
#error string_counter.h is a C++ header file
#endif

#include <map>
#include <set>
#include <vector>

#include "istr.h"

/* This class provides a simple way to get a list of "frequent" strings from
 * some set of input strings. It will not find exactly the most frequent
 * strings; this would require potentially linear space. Instead, a running
 * list of the most popular strings so far is kept, and once the size of that
 * list exceeds a set threshold, no new strings are added. There are obvious
 * inputs that will cause this approach to fail horribly, so a better approach
 * may be needed at some point. */

class string_counter
{
public:
	inline string_counter() : max(0) {}
	
	void init(size_t max_strings = 0);
	
	/* returns the number of times this string has been added */
	size_t add(const istr & string);
	size_t lookup(const istr & string) const;
	
	/* drop all entries with counts less than min */
	void ignore(size_t min = 2);
	
	/* fills a vector with the strings; it will be in sorted order */
	void vector(std::vector<istr> * vector) const;
	
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
	
	typedef std::map<istr, size_t, strcmp_less> string_map;
	typedef std::set<count_istr> count_set;
	
	size_t max;
	string_map strings;
	count_set counts;
};

#endif /* __STRING_COUNTER_H */
