/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdarg.h>

#include "itable.h"
#include "itable_overlay.h"

int itable_overlay::init(itable * it1, ...)
{
}

void itable_overlay::deinit()
{
}

bool itable_overlay::has(iv_int k1)
{
}

bool itable_overlay::has(const char * k1)
{
}

bool itable_overlay::has(iv_int k1, iv_int k2)
{
}

bool itable_overlay::has(iv_int k1, const char * k2)
{
}

bool itable_overlay::has(const char * k1, iv_int k2)
{
}

bool itable_overlay::has(const char * k1, const char * k2)
{
}

off_t itable_overlay::get(iv_int k1, iv_int k2)
{
}

off_t itable_overlay::get(iv_int k1, const char * k2)
{
}

off_t itable_overlay::get(const char * k1, iv_int k2)
{
}

off_t itable_overlay::get(const char * k1, const char * k2)
{
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
