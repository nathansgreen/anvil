/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SINGLE_STORE_H
#define __SINGLE_STORE_H

#include <stdlib.h>
#include <string.h>

#include "itable.h"
#include "atable.h"
#include "itable_datamap.h"
#include "itable_overlay.h"
#include "datastore.h"

#ifndef __cplusplus
#error single_store.h is a C++ header file
#endif

class single_store
{
public:
	enum vtype {
		NONE,
		UINT8, UINT16, UINT32, UINT64,
		FLOAT, DOUBLE,
		STRING_SMALL, STRING, STRING_LARGE,
		BLOB_SMALL, BLOB_MEDIUM, BLOB
	};
	struct value {
		vtype type;
		union {
			uint8_t v_uint8;
			uint16_t v_uint16;
			uint32_t v_uint32;
			uint64_t v_uint64;
			float v_float;
			double v_double;
			const char * v_string;
			struct {
				const void * v_blob;
				size_t v_blob_len;
			};
		};
		inline value() : type(NONE) { }
		inline value(uint8_t x) : type(UINT8), v_uint8(x) { }
		inline value(uint16_t x) : type(UINT16), v_uint16(x) { }
		inline value(uint32_t x) : type(UINT32), v_uint32(x) { }
		inline value(uint64_t x) : type(UINT64), v_uint64(x) { }
		inline value(float x) : type(FLOAT), v_float(x) { }
		inline value(double x) : type(DOUBLE), v_double(x) { }
		/* note that these do not verify the lengths are valid */
		inline value(const char * x, bool small = false) : type(small ? STRING_SMALL : STRING), v_string(x) { }
		inline value(const void * x, size_t l, bool medium = false) : type(medium ? BLOB_MEDIUM : BLOB), v_blob(x), v_blob_len(l) { }
		/* definitions below */
		inline value(const value & x);
		inline ~value();
	};
	
	value get(iv_int k1, iv_int k2, vtype type);
	value get(iv_int k1, const char * k2, vtype type);
	value get(const char * k1, iv_int k2, vtype type);
	value get(const char * k1, const char * k2, vtype type);
	
	int put(iv_int k1, iv_int k2, value v);
	int put(iv_int k1, const char * k2, value v);
	int put(const char * k1, iv_int k2, value v);
	int put(const char * k1, const char * k2, value v);
	
	inline single_store();
	int init(int dfd, const char * meta);
	void deinit();
	inline virtual ~single_store();
	
private:
	int dfd;
	const char * meta;
	
	itable ** itables;
	itable_datamap * map;
	itable_overlay * overlay;
	atable * append;
	datastore * store;
	
	value read_data(off_t offset, vtype type);
};

inline single_store::value::value(const value & x) : type(x.type)
{
	switch(type)
	{
		case UINT8:
		case UINT16:
		case UINT32:
		case UINT64:
			/* uint64 is larger than uint<64 */
			v_uint64 = x.v_uint64;
			break;
		case FLOAT:
		case DOUBLE:
			/* double is larger than float */
			v_double = x.v_double;
			break;
		case STRING_SMALL:
		case STRING:
		case STRING_LARGE:
			v_string = strdup(x.v_string);
			if(!v_string)
				type = NONE;
			break;
		case BLOB_SMALL:
		case BLOB_MEDIUM:
		case BLOB:
			v_blob_len = x.v_blob_len;
			v_blob = malloc(v_blob_len);
			if(v_blob)
				memcpy((void *) v_blob, x.v_blob, v_blob_len);
			else
				type = NONE;
			break;
		default:
			type = NONE;
			break;
	}
}

inline single_store::value::~value()
{
	switch(type)
	{
		case STRING_SMALL:
		case STRING:
		case STRING_LARGE:
			free((void *) v_string);
			break;
		case BLOB_SMALL:
		case BLOB_MEDIUM:
		case BLOB:
			free((void *) v_blob);
			break;
		default:
			break;
	}
}

inline single_store::single_store()
{
}

inline single_store::~single_store()
{
}

#endif /* __SINGLE_STORE_H */
