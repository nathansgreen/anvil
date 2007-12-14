/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __PLATFORM_H
#define __PLATFORM_H

#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <stdint.h>
#include <stdbool.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/* Efficient min and max operations */ 
#define MIN(_a, _b)	\
	({		\
		typeof(_a) __a = (_a);	\
		typeof(_b) __b = (_b);	\
		__a <= __b ? __a : __b;	\
	})

#define MAX(_a, _b)	\
	({		\
		typeof(_a) __a = (_a);	\
		typeof(_b) __b = (_b);	\
		__a >= __b ? __a : __b;	\
	})

/* static_assert(x) will generate a compile-time error if 'x' is false. */
#define static_assert(x) switch (x) case 0: case (x):

#endif /* __PLATFORM_H */
