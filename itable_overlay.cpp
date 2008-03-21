/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
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
	size_t i;
	it->clear();
	it->ovr = new it::overlay[table_count];
	if(!it->ovr)
		return -ENOMEM;
	for(i = 0; i < table_count; i++)
	{
		int r = tables[i]->iter(&it->ovr[i].iter);
		if(r < 0)
		{
			delete[] it->ovr;
			return r;
		}
		it->ovr[i].r = 0;
		it->ovr[i].empty = 1;
		it->ovr[i].last_k1.s = NULL;
		it->ovr[i].last_k2.s = NULL;
	}
	it->table = this;
	return 0;
}

int itable_overlay::iter(struct it * it, iv_int k1)
{
	size_t i;
	it->clear();
	it->ovr = new it::overlay[table_count];
	if(!it->ovr)
		return -ENOMEM;
	for(i = 0; i < table_count; i++)
	{
		int r = tables[i]->iter(&it->ovr[i].iter, k1);
		if(r < 0 && r != -ENOENT)
		{
			delete[] it->ovr;
			return r;
		}
		it->ovr[i].r = 0;
		it->ovr[i].empty = 1;
		it->ovr[i].last_k1.s = NULL;
		it->ovr[i].last_k2.s = NULL;
	}
	it->table = this;
	return 0;
}

int itable_overlay::iter(struct it * it, const char * k1)
{
	size_t i;
	it->clear();
	it->ovr = new it::overlay[table_count];
	if(!it->ovr)
		return -ENOMEM;
	for(i = 0; i < table_count; i++)
	{
		int r = tables[i]->iter(&it->ovr[i].iter, k1);
		if(r < 0 && r != -ENOENT)
		{
			delete[] it->ovr;
			return r;
		}
		it->ovr[i].r = 0;
		it->ovr[i].empty = 1;
		it->ovr[i].last_k1.s = NULL;
		it->ovr[i].last_k2.s = NULL;
	}
	it->table = this;
	return 0;
}

void itable_overlay::kill_iter(struct it * it)
{
	if(k1t == STRING)
		for(size_t i = 0; i < table_count; i++)
			if(it->ovr[i].last_k1.s)
				free((void *) it->ovr[i].last_k1.s);
	if(k2t == STRING)
		for(size_t i = 0; i < table_count; i++)
			if(it->ovr[i].last_k2.s)
				free((void *) it->ovr[i].last_k2.s);
	delete[] it->ovr;
	it->table = NULL;
}

int itable_overlay::next(struct it * it, iv_int * k1, iv_int * k2, off_t * off)
{
	size_t i, min_idx = table_count;
	iv_int min_k1 = 0, min_k2 = 0;
	if(k1t != INT || k2t != INT)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.i, &it->ovr[i].last_k2.i, &it->ovr[i].last_off);
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || it->ovr[i].last_k1.i < min_k1 ||
		   (it->ovr[i].last_k1.i == min_k1 && it->ovr[i].last_k2.i < min_k2))
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.i;
			min_k2 = it->ovr[i].last_k2.i;
		}
		else if(it->ovr[i].last_k1.i == min_k1 && it->ovr[i].last_k2.i == min_k2)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.i;
	*k2 = it->ovr[min_idx].last_k2.i;
	*off = it->ovr[min_idx].last_off;
	return 0;
}

int itable_overlay::next(struct it * it, iv_int * k1, const char ** k2, off_t * off)
{
	size_t i, min_idx = table_count;
	iv_int min_k1 = 0;
	const char * min_k2 = NULL;
	if(k1t != INT || k2t != STRING)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		int c = 0;
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			if(it->ovr[i].last_k2.s)
				free((void *) it->ovr[i].last_k2.s);
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.i, &it->ovr[i].last_k2.s, &it->ovr[i].last_off);
			if(it->ovr[i].r >= 0)
			{
				it->ovr[i].last_k2.s = strdup(it->ovr[i].last_k2.s);
				if(!it->ovr[i].last_k2.s)
					it->ovr[i].r = -ENOMEM;
			}
			else
				it->ovr[i].last_k2.s = NULL;
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || it->ovr[i].last_k1.i < min_k1 ||
		   (it->ovr[i].last_k1.i == min_k1 && (c = strcmp(it->ovr[i].last_k2.s, min_k2)) < 0))
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.i;
			min_k2 = it->ovr[i].last_k2.s;
		}
		else if(it->ovr[i].last_k1.i == min_k1 && !c)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.i;
	*k2 = it->ovr[min_idx].last_k2.s;
	*off = it->ovr[min_idx].last_off;
	return 0;
}

int itable_overlay::next(struct it * it, const char ** k1, iv_int * k2, off_t * off)
{
	size_t i, min_idx = table_count;
	const char * min_k1 = NULL;
	iv_int min_k2 = 0;
	if(k1t != STRING || k2t != INT)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		int c;
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			if(it->ovr[i].last_k1.s)
				free((void *) it->ovr[i].last_k1.s);
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.s, &it->ovr[i].last_k2.i, &it->ovr[i].last_off);
			if(it->ovr[i].r >= 0)
			{
				it->ovr[i].last_k1.s = strdup(it->ovr[i].last_k1.s);
				if(!it->ovr[i].last_k1.s)
					it->ovr[i].r = -ENOMEM;
			}
			else
				it->ovr[i].last_k1.s = NULL;
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || (c = strcmp(it->ovr[i].last_k1.s, min_k1)) < 0 ||
		   (!c && it->ovr[i].last_k2.i < min_k2))
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.s;
			min_k2 = it->ovr[i].last_k2.i;
		}
		else if(!c && it->ovr[i].last_k2.i < min_k2)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.s;
	*k2 = it->ovr[min_idx].last_k2.i;
	*off = it->ovr[min_idx].last_off;
	return 0;
}

int itable_overlay::next(struct it * it, const char ** k1, const char ** k2, off_t * off)
{
	size_t i, min_idx = table_count;
	const char * min_k1 = NULL;
	const char * min_k2 = NULL;
	if(k1t != STRING || k2t != STRING)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		int c1, c2 = 0;
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			if(it->ovr[i].last_k1.s)
				free((void *) it->ovr[i].last_k1.s);
			if(it->ovr[i].last_k2.s)
				free((void *) it->ovr[i].last_k2.s);
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.s, &it->ovr[i].last_k2.s, &it->ovr[i].last_off);
			if(it->ovr[i].r >= 0)
			{
				it->ovr[i].last_k1.s = strdup(it->ovr[i].last_k1.s);
				it->ovr[i].last_k2.s = strdup(it->ovr[i].last_k2.s);
				if(!it->ovr[i].last_k1.s || it->ovr[i].last_k2.s)
					it->ovr[i].r = -ENOMEM;
			}
			else
			{
				it->ovr[i].last_k1.s = NULL;
				it->ovr[i].last_k2.s = NULL;
			}
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || (c1 = strcmp(it->ovr[i].last_k1.s, min_k1)) < 0 ||
		   (!c1 && (c2 = strcmp(it->ovr[i].last_k2.s, min_k2)) < 0))
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.s;
			min_k2 = it->ovr[i].last_k2.s;
		}
		else if(!c1 && !c2)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.s;
	*k2 = it->ovr[min_idx].last_k2.s;
	*off = it->ovr[min_idx].last_off;
	return 0;
}

int itable_overlay::next(struct it * it, iv_int * k1)
{
	size_t i, min_idx = table_count;
	iv_int min_k1 = 0;
	if(k1t != INT)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.i);
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || it->ovr[i].last_k1.i < min_k1)
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.i;
		}
		else if(it->ovr[i].last_k1.i == min_k1)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.i;
	return 0;
}

int itable_overlay::next(struct it * it, const char ** k1)
{
	size_t i, min_idx = table_count;
	const char * min_k1 = NULL;
	if(k1t != STRING)
		return -EINVAL;
	for(i = 0; i < table_count; i++)
	{
		int c;
		if(it->ovr[i].empty)
		{
			/* fill in empty slots */
			it->ovr[i].empty = 0;
			if(it->ovr[i].last_k1.s)
				free((void *) it->ovr[i].last_k1.s);
			it->ovr[i].r = tables[i]->next(&it->ovr[i].iter, &it->ovr[i].last_k1.s);
			if(it->ovr[i].r >= 0)
			{
				it->ovr[i].last_k1.s = strdup(it->ovr[i].last_k1.s);
				if(!it->ovr[i].last_k1.s)
					it->ovr[i].r = -ENOMEM;
			}
			else
				it->ovr[i].last_k1.s = NULL;
		}
		if(it->ovr[i].r == -ENOENT)
			/* skip exhausted tables */
			continue;
		else if(it->ovr[i].r < 0)
			return it->ovr[i].r;
		if(!i || (c = strcmp(it->ovr[i].last_k1.s, min_k1)) < 0)
		{
			min_idx = i;
			min_k1 = it->ovr[i].last_k1.s;
		}
		else if(!c)
			/* skip shadowed entry */
			it->ovr[i].empty = 1;
	}
	if(min_idx == table_count)
		return -ENOENT;
	it->ovr[min_idx].empty = 1;
	*k1 = it->ovr[min_idx].last_k1.s;
	return 0;
}
