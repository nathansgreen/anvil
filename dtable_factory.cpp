/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "dtable_factory.h"
#include "factory_impl.h"

/* force the template to instantiate */
template class factory<dtable_factory_base>;

dtable * dtable_factory_base::load(const istr & type, int dfd, const char * name, const params & config)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->open(dfd, name, config) : NULL;
}

int dtable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, key_type) : -ENOENT;
}

int dtable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, const dtable * source, const dtable * shadow)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, source, shadow) : -ENOENT;
}
