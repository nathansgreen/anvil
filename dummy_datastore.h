/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DUMMY_DATASTORE_H
#define __DUMMY_DATASTORE_H

#include <stdint.h>
#include <stddef.h>

#ifndef __cplusplus
#error dummy_datastore.h is a C++ header file
#endif

/* This dummy datastore just gives you the same offsets you'd get if you
 * started appending data to a newly created real datastore. */

class dummy_datastore
{
public:
	off_t append_uint8(uint8_t i);
	off_t append_uint16(uint16_t i);
	off_t append_uint32(uint32_t i);
	off_t append_uint64(uint64_t i);
	
	off_t append_float(float f);
	off_t append_double(double d);
	
	off_t append_string255(const char * string);
	off_t append_string65k(const char * string);
	off_t append_stringX(const char * string);
	
	off_t append_blob255(const void * blob, uint8_t length);
	off_t append_blob65k(const void * blob, uint16_t length);
	off_t append_blob4g(const void * blob, uint32_t length);
	off_t append_blobX(const void * blob, size_t length);
	
	inline dummy_datastore();
	int init();
	
private:
	off_t offset;
};

inline dummy_datastore::dummy_datastore()
	: offset(0)
{
}

#endif /* __DUMMY_DATASTORE_H */
