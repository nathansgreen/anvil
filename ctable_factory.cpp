/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <errno.h>
#include <stddef.h>

#include "ctable_factory.h"
#include "factory_impl.h"

/* force the template to instantiate */
template class factory<ctable_factory_base>;

ctable * ctable_factory_base::load(const istr & type, int dfd, const char * name, const params & config, sys_journal * sysj)
{
	const ctable_factory * factory = ctable_factory::lookup(type);
	return factory ? factory->open(dfd, name, config, sysj) : NULL;
}

int ctable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	const ctable_factory * factory = ctable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, key_type) : -ENOENT;
}
