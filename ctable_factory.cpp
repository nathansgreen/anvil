/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "ctable_factory.h"
#include "factory_impl.h"

/* force the template to instantiate */
template class factory<ctable_factory_base>;

ctable * ctable_factory_base::load(const istr & type, const dtable * dt_source, const params & config)
{
	const ctable_factory * factory = ctable_factory::lookup(type);
	return factory ? factory->open(dt_source, config) : NULL;
}

ctable * ctable_factory_base::load(const istr & type, dtable * dt_source, const params & config)
{
	const ctable_factory * factory = ctable_factory::lookup(type);
	return factory ? factory->open(dt_source, config) : NULL;
}
