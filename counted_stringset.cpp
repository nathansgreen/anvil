/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>

#include "counted_stringset.h"

void counted_stringset::init(size_t max_strings)
{
	strings.clear();
	counts.clear();
	max = max_strings;
}

size_t counted_stringset::add(const istr & string)
{
	string_map::iterator iter = strings.find(string);
	if(iter == strings.end())
	{
		/* new string; check max string count before adding it */
		if(max && strings.size() >= max)
		{
			count_set::iterator count = counts.begin();
			if(*count > 1)
				/* at max string count and lowest count is more than 1 */
				return 0;
			/* prefer more recent strings: dump the previous low string */
			strings.erase(*count);
			counts.erase(*count);
		}
		strings[string] = 1;
		counts.insert(count_istr(1, string));
		return 1;
	}
	counts.erase(count_istr((*iter).second, (*iter).first));
	counts.insert(count_istr(++(*iter).second, (*iter).first));
	return (*iter).second;
}

size_t counted_stringset::lookup(const istr & string) const
{
	string_map::const_iterator iter = strings.find(string);
	if(iter == strings.end())
		return 0;
	return (*iter).second;
}

void counted_stringset::ignore(size_t min)
{
	count_set::iterator count = counts.begin();
	while(counts.size() && *count < min)
	{
		strings.erase(*count);
		counts.erase(*count);
		count = counts.begin();
	}
}

void counted_stringset::vector(std::vector<istr> * vector) const
{
	string_map::const_iterator iter = strings.begin();
	string_map::const_iterator end = strings.end();
	vector->clear();
	while(iter != end)
	{
		vector->push_back((*iter).first);
		++iter;
	}
}
