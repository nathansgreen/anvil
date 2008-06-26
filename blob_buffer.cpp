/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <string.h>

#include "blob_buffer.h"

blob_buffer::blob_buffer(size_t capacity)
	: buffer_capacity(0), internal(NULL)
{
	int r = set_capacity(capacity);
	assert(r >= 0);
}

blob_buffer::blob_buffer(size_t size, const void * data)
	: buffer_capacity(0), internal(NULL)
{
	int r = set_capacity(size);
	assert(r >= 0);
	memcpy(internal->bytes, data, size);
	internal->size = size;
}

blob_buffer::blob_buffer(const blob & x)
	: buffer_capacity(0), internal(NULL)
{
	*this = x;
}

blob_buffer::blob_buffer(const blob_buffer & x)
	: buffer_capacity(0), internal(NULL)
{
	*this = x;
}

blob_buffer & blob_buffer::operator=(const blob & x)
{
	if(internal && !--internal->shares)
		free(internal);
	if(x.internal)
	{
		buffer_capacity = x.internal->size;
		internal = x.internal;
		internal->shares++;
	}
	else
	{
		buffer_capacity = 0;
		internal = NULL;
	}
	return *this;
}

blob_buffer & blob_buffer::operator=(const blob_buffer & x)
{
	if(this == &x)
		return *this;
	if(internal && !--internal->shares)
		free(internal);
	if(x.internal)
	{
		buffer_capacity = x.buffer_capacity;
		internal = x.internal;
		internal->shares++;
	}
	else
	{
		buffer_capacity = 0;
		internal = NULL;
	}
	return *this;
}

int blob_buffer::overwrite(size_t offset, const void * data, size_t length)
{
	int r;
	if(!length)
		return 0;
	if(offset + length > buffer_capacity)
		r = set_capacity(offset + length);
	else
		r = touch();
	if(r < 0)
		return r;
	assert(internal->shares == 1);
	if(offset + length > internal->size)
	{
		if(offset > internal->size)
			memset(&internal->bytes[internal->size], 0, offset - internal->size);
		internal->size = offset + length;
	}
	/* unroll memcpy a little bit... this call right here is expensive! */
	switch(length)
	{
		case 4:
			internal->bytes[offset + 3] = ((uint8_t *) data)[3];
		case 3:
			internal->bytes[offset + 2] = ((uint8_t *) data)[2];
		case 2:
			internal->bytes[offset + 1] = ((uint8_t *) data)[1];
		case 1:
			internal->bytes[offset] = *(uint8_t *) data;
		case 0:
			break;
		default:
			memcpy(&internal->bytes[offset], data, length);
	}
	return 0;
}

int blob_buffer::set_size(size_t size, bool clear)
{
	if(size > buffer_capacity)
	{
		int r = set_capacity(size);
		if(r < 0)
			return r;
	}
	assert(!size || internal);
	if(internal)
	{
		int r = touch();
		if(r < 0)
			return r;
		if(size > internal->size && clear)
			memset(&internal->bytes[internal->size], 0, size - internal->size);
		internal->size = size;
	}
	return 0;
}

int blob_buffer::set_capacity(size_t capacity)
{
	blob::blob_internal * copy;
	if(buffer_capacity == capacity)
		return 0;
	if(!internal)
	{
		internal = (blob::blob_internal *) malloc(sizeof(*internal) + capacity);
		if(!internal)
			return -ENOMEM;
		internal->size = 0;
		internal->shares = 1;
		buffer_capacity = capacity;
		return 0;
	}
	if(internal->shares > 1)
	{
		copy = (blob::blob_internal *) malloc(sizeof(*internal) + capacity);
		if(!copy)
			return -ENOMEM;
		copy->size = (internal->size > capacity) ? capacity : internal->size;
		copy->shares = 1;
		memcpy(copy->bytes, internal->bytes, copy->size);
		internal->shares--;
	}
	else
	{
		copy = (blob::blob_internal *) realloc(internal, sizeof(*internal) + capacity);
		if(!copy)
			return -ENOMEM;
		if(internal->size > capacity)
			internal->size = capacity;
	}
	internal = copy;
	buffer_capacity = capacity;
	return 0;
}

int blob_buffer::touch()
{
	if(internal->shares > 1)
	{
		blob::blob_internal * copy = (blob::blob_internal *) malloc(sizeof(*internal) + internal->size);
		if(!copy)
			return -ENOMEM;
		memcpy(copy, internal, sizeof(*internal) + internal->size);
		copy->shares = 1;
		internal->shares--;
		internal = copy;
	}
	return 0;
}
