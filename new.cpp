/* This file is part of Anvil. Anvil is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <new>

/* make the default operator new be the nothrow version */

void * operator new(size_t size)
{
	return operator new(size, std::nothrow);
}

void * operator new[](size_t size)
{
	return operator new[](size, std::nothrow);
}
