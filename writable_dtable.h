/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __WRITABLE_DTABLE_H
#define __WRITABLE_DTABLE_H

#ifndef __cplusplus
#error writable_dtable.h is a C++ header file
#endif

#include "dtable.h"

/* writable data tables */

class writable_dtable : public dtable
{
public:
	virtual int append(dtype key, const blob & blob) = 0;
	virtual int remove(dtype key) = 0;
};

#endif /* __WRITABLE_DTABLE_H */
