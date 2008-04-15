/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>

#include "transaction.h"

#include "simple_dtable.h"

simple_dtable::iter::iter(const simple_dtable * source)
	: source(source)
{
}

bool simple_dtable::iter::valid() const
{
}

bool simple_dtable::iter::next()
{
}

dtype simple_dtable::iter::key() const
{
}

blob simple_dtable::iter::value() const
{
}

const dtable * simple_dtable::iter::extra() const
{
	return source;
}

sane_iter3<dtype, blob, const dtable *> * simple_dtable::iterator() const
{
	return NULL;
}

blob simple_dtable::lookup(dtype key, const dtable ** source) const
{
	*source = NULL;
	return blob();
}

int simple_dtable::init(int dfd, const char * file)
{
	if(fd >= 0)
		deinit();
	return -ENOSYS;
}

void simple_dtable::deinit()
{
	if(fd >= 0)
	{
	}
}

int simple_dtable::create(int dfd, const char * file, const dtable * source, const dtable * shadow)
{
	return -ENOSYS;
}
