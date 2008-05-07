/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <string.h>

#include "blob.h"

blob::blob(size_t size)
	: internal(NULL)
{
	set_size(size);
	assert(internal);
}

blob::blob(size_t size, const void * data)
	: internal(NULL)
{
	set_size(size);
	assert(internal);
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

int blob::set_size(size_t size)
{
	if(!internal)
	{
		internal = (blob_internal *) malloc(sizeof(*internal) + size);
		if(!internal)
			return -ENOMEM;
		internal->size = size;
		internal->shares = 1;
		memset(internal->bytes, 0, size);
		return 0;
	}
	if(internal->size == size)
		return 0;
	if(internal->shares > 1)
	{
		blob_internal * copy = (blob_internal *) malloc(sizeof(*internal) + size);
		if(!copy)
			return -ENOMEM;
		copy->size = size;
		copy->shares = 1;
		if(size >= internal->size)
		{
			memcpy(copy->bytes, internal->bytes, internal->size);
			memset(&copy->bytes[internal->size], 0, size - internal->size);
		}
		else
			memcpy(copy->bytes, internal->bytes, size);
		internal->shares--;
		internal = copy;
	}
	else
	{
		blob_internal * copy = (blob_internal *) realloc(internal, sizeof(*internal) + size);
		if(!copy)
			return -ENOMEM;
		if(copy->size < size)
			memset(&copy->bytes[copy->size], 0, size - copy->size);
		copy->size = size;
		internal = copy;
	}
	return 0;
}

int blob::touch()
{
	if(internal->shares > 1)
	{
		blob_internal * copy = (blob_internal *) malloc(sizeof(*internal) + internal->size);
		if(!copy)
			return -ENOMEM;
		memcpy(copy, internal, sizeof(*internal) + internal->size);
		copy->shares = 1;
		internal->shares--;
		internal = copy;
	}
	return 0;
}
