/* This file is part of Anvil. Anvil is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>
#include <assert.h>

#include "stringset.h"

int stringset::init(bool keep_reverse)
{
	reverse = keep_reverse;
	next_index = 0;
	string_map.clear();
	index_map.clear();
	return 0;
}

bool stringset::remove(const istr & string)
{
	uint32_t index;
	if(!string_map.count(string))
		return false;
	index = string_map[string];
	string_map.erase(string);
	if(reverse)
	{
		index_map.erase(index);
		/* rewind the index if we've removed the most recently added string */
		if(next_index - 1 == index)
			next_index--;
	}
	return true;
}

const istr & stringset::add(const istr & string, uint32_t * index)
{
	istr_map::iterator iter = string_map.find(string);
	if(iter == string_map.end())
	{
		string_map[string] = next_index++;
		iter = string_map.find(string);
		if(reverse)
			index_map[(*iter).second] = (*iter).first;
	}
	if(index)
		*index = (*iter).second;
	return (*iter).first;
}

const istr & stringset::lookup(const istr & string, uint32_t * index) const
{
	istr_map::const_iterator iter = string_map.find(string);
	if(iter == string_map.end())
		return istr::null;
	if(index)
		*index = (*iter).second;
	return (*iter).first;
}

const istr & stringset::lookup(uint32_t index) const
{
	if(!reverse)
		return istr::null;
	idx_map::const_iterator iter = index_map.find(index);
	if(iter == index_map.end())
		return istr::null;
	return (*iter).second;
}

void stringset::vector(std::vector<istr> * vector) const
{
	istr_map::const_iterator iter = string_map.begin();
	istr_map::const_iterator end = string_map.end();
	vector->clear();
	while(iter != end)
	{
		vector->push_back((*iter).first);
		++iter;
	}
}
