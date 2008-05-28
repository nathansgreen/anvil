/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <string.h>

#include "blob.h"

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

