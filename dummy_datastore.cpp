/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <string.h>

#include "datastore.h"
#include "dummy_datastore.h"

off_t dummy_datastore::append_uint8(uint8_t i)
{
	off_t off = offset;
	offset += sizeof(i);
	return 0;
}

off_t dummy_datastore::append_uint16(uint16_t i)
{
	off_t off = offset;
	offset += sizeof(i);
	return 0;
}

off_t dummy_datastore::append_uint32(uint32_t i)
{
	off_t off = offset;
	offset += sizeof(i);
	return 0;
}

off_t dummy_datastore::append_uint64(uint64_t i)
{
	off_t off = offset;
	offset += sizeof(i);
	return 0;
}

off_t dummy_datastore::append_float(float f)
{
	off_t off = offset;
	offset += sizeof(f);
	return 0;
}

off_t dummy_datastore::append_double(double d)
{
	off_t off = offset;
	offset += sizeof(d);
	return 0;
}

off_t dummy_datastore::append_string255(const char * string)
{
	uint8_t length;
	off_t off = offset;
	size_t full_length = strlen(string);
	if(full_length > 0xFF)
		return INVAL_OFF_T;
	length = full_length;
	offset += sizeof(length) + full_length;
	return off;
}

off_t dummy_datastore::append_string65k(const char * string)
{
	uint16_t length;
	off_t off = offset;
	size_t full_length = strlen(string);
	if(full_length > 0xFFFF)
		return INVAL_OFF_T;
	length = full_length;
	offset += sizeof(length) + full_length;
	return off;
}

off_t dummy_datastore::append_stringX(const char * string)
{
	off_t off = offset;
	size_t length = strlen(string);
	offset += length;
	return off;
}

off_t dummy_datastore::append_blob255(const void * blob, uint8_t length)
{
	off_t off = offset;
	offset += sizeof(length) + length;
	return off;
}

off_t dummy_datastore::append_blob65k(const void * blob, uint16_t length)
{
	off_t off = offset;
	offset += sizeof(length) + length;
	return off;
}

off_t dummy_datastore::append_blob4g(const void * blob, uint32_t length)
{
	off_t off = offset;
	offset += sizeof(length) + length;
	return off;
}

off_t dummy_datastore::append_blobX(const void * blob, size_t length)
{
	off_t off = offset;
	offset += length;
	return off;
}

int dummy_datastore::init()
{
	offset = 0;
	return 0;
}
