/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <string.h>

#include "blob.h"
#include "blob_comparator.h"

const blob blob::dne;
const blob blob::empty(0, NULL);

blob::blob(size_t size, const void * data)
{
	internal = (blob_internal *) malloc(sizeof(*internal) + size);
	assert(internal);
	internal->size = size;
	internal->shares = 1;
	memcpy(internal->bytes, data, size);
}

blob::blob(const blob & x)
{
	if((internal = x.internal))
		internal->shares++;
}

blob & blob::operator=(const blob & x)
{
	if(this == &x)
		return *this;
	if(internal && !--internal->shares)
		free(internal);
	if((internal = x.internal))
		internal->shares++;
	return *this;
}

ssize_t blob::locate(const std::vector<blob> & array, const blob & search, const blob_comparator * blob_cmp)
{
	/* binary search */
	ssize_t min = 0, max = array.size() - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		c = blob_cmp ? blob_cmp->compare(array[index], search) : array[index].compare(search);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}
