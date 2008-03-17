/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdarg.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>

#include "itable.h"
#include "itable_overlay.h"

int itable_overlay::init(itable * it1, ...)
{
	va_list ap;
	size_t length = 1;
	itable * table;
	
	k1t = it1->k1_type();
	k2t = it1->k2_type();
	
	va_start(ap, it1);
	while((table = va_arg(ap, itable *)))
	{
		if(table->k1_type() != k1t || table->k2_type() != k2t)
		{
			va_end(ap);
			return -EINVAL;
		}
		length++;
	}
	va_end(ap);
	
	tables = new itable *[length];
	if(!tables)
		return -ENOMEM;
	table_count = length;
	tables[0] = it1;
	
	va_start(ap, it1);
	for(length = 1; length < table_count; length++)
		tables[length] = va_arg(ap, itable *);
	va_end(ap);
	return 0;
}

int itable_overlay::init(itable ** array, size_t length)
{
	size_t i;
	if(length < 1)
		return -EINVAL;
	k1t = array[0]->k1_type();
	k2t = array[0]->k2_type();
	
	for(i = 1; i < length; i++)
		if(array[i]->k1_type() != k1t || array[i]->k2_type() != k2t)
			return -EINVAL;
	
	tables = new itable *[length];
	if(!tables)
		return -ENOMEM;
	table_count = length;
	memcpy(tables, array, sizeof(*array) * length);
	return 0;
}

void itable_overlay::deinit()
{
	if(!tables)
		return;
	delete tables;
	tables = NULL;
	table_count = 0;
}

bool itable_overlay::has(iv_int k1)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1))
			return true;
	return false;
}

bool itable_overlay::has(const char * k1)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1))
			return true;
	return false;
}

bool itable_overlay::has(iv_int k1, iv_int k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1, k2))
			return true;
	return false;
}

bool itable_overlay::has(iv_int k1, const char * k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1, k2))
			return true;
	return false;
}

bool itable_overlay::has(const char * k1, iv_int k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1, k2))
			return true;
	return false;
}

bool itable_overlay::has(const char * k1, const char * k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
		if(tables[i]->has(k1, k2))
			return true;
	return false;
}

off_t itable_overlay::get(iv_int k1, iv_int k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		off_t off = tables[i]->get(k1, k2);
		if(off != INVAL_OFF_T)
			return off;
	}
	return INVAL_OFF_T;
}

off_t itable_overlay::get(iv_int k1, const char * k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		off_t off = tables[i]->get(k1, k2);
		if(off != INVAL_OFF_T)
			return off;
	}
	return INVAL_OFF_T;
}

off_t itable_overlay::get(const char * k1, iv_int k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		off_t off = tables[i]->get(k1, k2);
		if(off != INVAL_OFF_T)
			return off;
	}
	return INVAL_OFF_T;
}

off_t itable_overlay::get(const char * k1, const char * k2)
{
	size_t i;
	for(i = 0; i < table_count; i++)
	{
		off_t off = tables[i]->get(k1, k2);
		if(off != INVAL_OFF_T)
			return off;
	}
	return INVAL_OFF_T;
}

int itable_overlay::iter(struct it * it)
{
}

int itable_overlay::iter(struct it * it, iv_int k1)
{
}

int itable_overlay::iter(struct it * it, const char * k1)
{
}

int itable_overlay::next(struct it * it, iv_int * k1, iv_int * k2, off_t * off)
{
}

int itable_overlay::next(struct it * it, iv_int * k1, const char ** k2, off_t * off)
{
}

int itable_overlay::next(struct it * it, const char ** k1, iv_int * k2, off_t * off)
{
}

int itable_overlay::next(struct it * it, const char ** k1, const char ** k2, off_t * off)
{
}

int itable_overlay::next(struct it * it, iv_int * k1, size_t * k2_count)
{
}

int itable_overlay::next(struct it * it, const char ** k1, size_t * k2_count)
{
}
