/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "dtable_factory.h"
#include "factory_impl.h"
#include "empty_dtable.h"

/* force the template to instantiate */
template class factory<dtable_factory_base>;

int dtable_factory_base::create(int dfd, const char * name, const params & config, dtype::ctype key_type) const
{
	empty_dtable empty(key_type);
	return create(dfd, name, config, &empty, NULL);
}

dtable * dtable_factory_base::load(const istr & type, int dfd, const char * name, const params & config, sys_journal * sysj)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->open(dfd, name, config, sysj) : NULL;
}

dtable * dtable_factory_base::load(int dfd, const char * name, const params & config, sys_journal * sysj)
{
	istr base;
	params base_config;
	if(!config.get("base", &base) || !config.get("base_config", &base_config))
		return NULL;
	return load(base, dfd, name, base_config, sysj);
}

int dtable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, key_type) : -ENOENT;
}

int dtable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, dtable::iter * source, const ktable * shadow)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, source, shadow) : -ENOENT;
}

int dtable_factory_base::setup(const istr & type, int dfd, const char * name, const params & config, const dtable * source, const ktable * shadow)
{
	const dtable_factory * factory = dtable_factory::lookup(type);
	return factory ? factory->create(dfd, name, config, source, shadow) : -ENOENT;
}

int dtable_factory_base::setup(int dfd, const char * name, const params & config, dtype::ctype key_type)
{
	istr base;
	params base_config;
	if(!config.get("base", &base) || !config.get("base_config", &base_config))
		return NULL;
	return setup(base, dfd, name, base_config, key_type);
}

int dtable_factory_base::setup(int dfd, const char * name, const params & config, dtable::iter * source, const ktable * shadow)
{
	istr base;
	params base_config;
	if(!config.get("base", &base) || !config.get("base_config", &base_config))
		return NULL;
	return setup(base, dfd, name, base_config, source, shadow);
}

int dtable_factory_base::setup(int dfd, const char * name, const params & config, const dtable * source, const ktable * shadow)
{
	istr base;
	params base_config;
	if(!config.get("base", &base) || !config.get("base_config", &base_config))
		return NULL;
	return setup(base, dfd, name, base_config, source, shadow);
}
