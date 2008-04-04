/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "single_store.h"

single_store::value single_store::read_data(off_t offset, single_store::vtype type)
{
	value v;
	switch(type)
	{
		case UINT8:
			if(store->read_uint8(offset, &v.v_uint8) < 0)
				return v;
			break;
		case UINT16:
			if(store->read_uint16(offset, &v.v_uint16) < 0)
				return v;
			break;
		case UINT32:
			if(store->read_uint32(offset, &v.v_uint32) < 0)
				return v;
			break;
		case UINT64:
			if(store->read_uint64(offset, &v.v_uint64) < 0)
				return v;
			break;
		case FLOAT:
			if(store->read_float(offset, &v.v_float) < 0)
				return v;
			break;
		case DOUBLE:
			if(store->read_double(offset, &v.v_double) < 0)
				return v;
			break;
		case STRING_SMALL:
			if(!(v.v_string = store->read_string255(offset)))
				return v;
			break;
		case STRING:
			if(!(v.v_string = store->read_string65k(offset)))
				return v;
			break;
		case STRING_LARGE:
			if(!(v.v_string = store->read_string4g(offset)))
				return v;
			break;
		case BLOB_SMALL:
		{
			uint8_t size;
			if(!(v.v_blob = store->read_blob255(offset, &size)))
				return v;
			v.v_blob_len = size;
			break;
		}
		case BLOB_MEDIUM:
		{
			uint16_t size;
			if(!(v.v_blob = store->read_blob65k(offset, &size)))
				return v;
			v.v_blob_len = size;
			break;
		}
		case BLOB:
		{
			uint32_t size;
			if(!(v.v_blob = store->read_blob4g(offset, &size)))
				return v;
			v.v_blob_len = size;
			break;
		}
		default:
			return v;
	}
	v.type = type;
	return v;
}

single_store::value single_store::get(iv_int k1, iv_int k2, single_store::vtype type)
{
	value none;
	off_t off = overlay->get(k1, k2);
	if(off == INVAL_OFF_T)
		return none;
	return read_data(off, type);
}

single_store::value single_store::get(iv_int k1, const char * k2, single_store::vtype type)
{
	value none;
	off_t off = overlay->get(k1, k2);
	if(off == INVAL_OFF_T)
		return none;
	return read_data(off, type);
}

single_store::value single_store::get(const char * k1, iv_int k2, single_store::vtype type)
{
	value none;
	off_t off = overlay->get(k1, k2);
	if(off == INVAL_OFF_T)
		return none;
	return read_data(off, type);
}

single_store::value single_store::get(const char * k1, const char * k2, single_store::vtype type)
{
	value none;
	off_t off = overlay->get(k1, k2);
	if(off == INVAL_OFF_T)
		return none;
	return read_data(off, type);
}

int single_store::put(iv_int k1, iv_int k2, single_store::value v)
{
	return -ENOSYS;
}

int single_store::put(iv_int k1, const char * k2, single_store::value v)
{
	return -ENOSYS;
}

int single_store::put(const char * k1, iv_int k2, single_store::value v)
{
	return -ENOSYS;
}

int single_store::put(const char * k1, const char * k2, single_store::value v)
{
	return -ENOSYS;
}

int single_store::init(int dfd, const char * meta)
{
	return -ENOSYS;
}

void single_store::deinit()
{
}
