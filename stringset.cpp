/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>

#include "hash_map.h"
#include "stringset.h"

int stringset::init(bool reverse)
{
	if(string_map)
		deinit();
	next_index = 0;
	string_map = hash_map_create_str();
	if(!string_map)
		return -ENOMEM;
	if(reverse)
	{
		index_map = hash_map_create();
		if(!index_map)
		{
			hash_map_destroy(string_map);
			string_map = NULL;
			return -ENOMEM;
		}
	}
	return 0;
}

void stringset::deinit()
{
	if(string_map)
	{
		hash_map_it2_t hm_it = hash_map_it2_create(string_map);
		while(hash_map_it2_next(&hm_it))
			free((void *) hm_it.key);
		hash_map_destroy(string_map);
		string_map = NULL;
		if(index_map)
		{
			hash_map_destroy(index_map);
			index_map = NULL;
		}
	}
}

bool stringset::ready()
{
	return string_map != NULL;
}

bool stringset::remove(const char * string)
{
	hash_map_elt_t elt = hash_map_find_elt(string_map, string);
	if(!elt.key)
		return false;
	hash_map_erase(string_map, string);
	if(index_map)
	{
		void * key = hash_map_erase(index_map, elt.val);
		assert(key == elt.key);
	}
	free((void *) elt.key);
	return true;
}

const char * stringset::add(const char * string, uint32_t * index)
{
	hash_map_elt_t elt = hash_map_find_elt(string_map, string);
	char * copy = (char *) elt.key;
	if(!copy)
	{
		copy = strdup(string);
		if(!copy)
			return NULL;
		if(hash_map_insert(string_map, copy, (void *) next_index) < 0)
		{
			free(copy);
			return NULL;
		}
		if(index_map)
			if(hash_map_insert(index_map, (void *) next_index, copy) < 0)
			{
				hash_map_erase(string_map, copy);
				free(copy);
				return NULL;
			}
		if(index)
			*index = next_index;
		/* assume that this does not wrap */
		next_index++;
	}
	else if(index)
		*index = (uint32_t) elt.val;
	return copy;
}

const char * stringset::lookup(const char * string, uint32_t * index)
{
	hash_map_elt_t elt = hash_map_find_elt(string_map, string);
	if(elt.key && index)
		*index = (uint32_t) elt.val;
	return (char *) elt.key;
}

const char * stringset::lookup(uint32_t index)
{
	if(!index_map)
		return NULL;
	return (char *) hash_map_find_val(index_map, (void *) index);
}

const char ** stringset::array(bool empty)
{
	hash_map_it2_t hm_it;
	size_t i = 0, count = hash_map_size(string_map);
	const char ** array = (const char **) malloc(sizeof(*array) * count);
	if(!array)
		return NULL;
	hm_it = hash_map_it2_create(string_map);
	while(hash_map_it2_next(&hm_it))
		array[i++] = (const char *) hm_it.key;
	assert(i == count);
	if(empty)
	{
		hash_map_clear(string_map);
		if(index_map)
			hash_map_clear(index_map);
	}
	return array;
}

size_t stringset::size()
{
	return hash_map_size(string_map);
}
