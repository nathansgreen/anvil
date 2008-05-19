/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>

#include "ctable_factory.h"

/* hmm... can we be guaranteed that this constructor will be called before the constructors that call add() below? */
ct_factory_registry::factory_map ct_factory_registry::factories;

int ct_factory_registry::add(const istr & class_name, const ctable_factory * factory)
{
	if(factories.count(class_name) > 0)
		fprintf(stderr, "Warning: replacing existing ctable factory \"%s\"\n", (const char *) class_name);
	factories[class_name] = factory;
	return 0;
}

const ctable_factory * ct_factory_registry::lookup(const istr & class_name)
{
	if(class_name && factories.count(class_name) > 0)
		return factories[class_name];
	return NULL;
}

const ctable_factory * ct_factory_registry::lookup(const params & config, const istr & config_name)
{
	istr class_name;
	if(!config.get(config_name, &class_name))
		return NULL;
	return lookup(class_name);
}

void ct_factory_registry::remove(const istr & class_name, const ctable_factory * factory)
{
	if(factories.count(class_name) > 0)
	{
		if(factories[class_name] != factory)
			fprintf(stderr, "Warning: attempt to remove mismatched ctable factory \"%s\"\n", (const char *) class_name);
		else
			factories.erase(class_name);
	}
	else
		fprintf(stderr, "Warning: attempt to remove nonexistent ctable factory \"%s\"\n", (const char *) class_name);
}
