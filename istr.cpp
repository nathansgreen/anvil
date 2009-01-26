/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "istr.h"

const istr istr::null;

template<class T>
ssize_t istr::locate_generic(T array, size_t size, const istr & key)
{
	/* binary search */
	ssize_t min = 0, max = size - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		c = strcmp(array[index], key);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}

/* force the two variants of this template that we'll want to use to instantiate */
template ssize_t istr::locate_generic<const istr *>(const istr *, size_t, const istr &);
template ssize_t istr::locate_generic<const std::vector<istr> &>(const std::vector<istr> &, size_t, const istr &);
