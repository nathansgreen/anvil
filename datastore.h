/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DATASTORE_H
#define __DATASTORE_H

#include <stdint.h>
#include <stddef.h>

#include "transaction.h"

#ifndef __cplusplus
#error datastore.h is a C++ header file
#endif

/* A datastore is the actual place where all the data is stored. This initial
 * version just appends data; the data can be reordered by iterating through an
 * existing itable that points into the datastore and writing a new datastore at
 * the same time as an updated itable. Later more advanced mechanisms will be
 * supported, like using different datastores for different columns, or
 * combinations of column-oriented data and row-oriented data. */

/* not used yet... */
#define DATASTORE_MAGIC 0xDA7ABA5E
#define DATASTORE_VERSION 0x0000

#define INVAL_OFF_T ((off_t) -1)

class datastore
{
public:
	/* appending data */
	
	off_t append_uint8(uint8_t i);
	off_t append_uint16(uint16_t i);
	off_t append_uint32(uint32_t i);
	off_t append_uint64(uint64_t i);
	
	off_t append_float(float f);
	off_t append_double(double d);
	
	off_t append_string255(const char * string);
	off_t append_string65k(const char * string);
	/* X: you will know the length when you read it back */
	off_t append_stringX(const char * string);
	
	off_t append_blob255(const void * blob, uint8_t length);
	off_t append_blob65k(const void * blob, uint16_t length);
	off_t append_blob4g(const void * blob, uint32_t length);
	/* X: you will know the length when you read it back */
	off_t append_blobX(const void * blob, size_t length);
	
	/* reading data */
	
	int read_uint8(off_t off, uint8_t * i);
	int read_uint16(off_t off, uint16_t * i);
	int read_uint32(off_t off, uint32_t * i);
	int read_uint64(off_t off, uint64_t * i);
	
	int read_float(off_t off, float * f);
	int read_double(off_t off, double * d);
	
	/* for strings, the "length" parameter is the size of the buffer (including termination) */
	/* length is only needed if you provide a string instead of NULL */
	char * read_string255(off_t off, char * string = NULL, uint8_t length = 0);
	char * read_string65k(off_t off, char * string = NULL, uint16_t length = 0);
	char * read_string4g(off_t off, char * string = NULL, uint32_t length = 0);
	/* length is always required here */
	char * read_stringX(off_t off, char * string, size_t length);
	
	/* length is input/output if you provide a blob instead of NULL, else output only */
	void * read_blob255(off_t off, uint8_t * length, void * blob = NULL);
	void * read_blob65k(off_t off, uint16_t * length, void * blob = NULL);
	void * read_blob4g(off_t off, uint32_t * length, void * blob = NULL);
	/* length is always required here */
	void * read_blobX(off_t off, size_t length, void * blob);
	
	inline datastore();
	int init(int dfd, const char * file, bool create = false);
	void deinit();
	inline ~datastore();
	
private:
	tx_fd fd;
	int ufd;
	off_t offset;
};

inline datastore::datastore()
	: fd(-1), ufd(-1), offset(-1)
{
}

inline datastore::~datastore()
{
	if(fd >= 0)
		deinit();
}

#endif /* __DATASTORE_H */
