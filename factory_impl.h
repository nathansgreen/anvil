/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __FACTORY_IMPL_H
#define __FACTORY_IMPL_H

#include <stdio.h>

#include "factory.h"
#include "params.h"

/* Can we be guaranteed that the constructor for this map will be called before
 * the factory constructors that call add() below? Not officially, but in
 * practice we can. We must list all the factory-constructible classes before
 * the corresponding factory registries in the Makefile, as the global
 * constructors are called in the opposite order as they are linked together. */
template<class T>
typename factory<T>::factory_map factory<T>::factories;

/* TODO: use factory_map::find()/insert() here instead of factory_map::count()/operator[]() */

template<class T>
int factory<T>::add(const istr & class_name, const factory * factory)
{
	if(factories.count(class_name) > 0)
		fprintf(stderr, "Warning: replacing existing factory \"%s\"\n", (const char *) class_name);
	factories[class_name] = factory;
	return 0;
}

template<class T>
const factory<T> * factory<T>::lookup(const istr & class_name)
{
	if(class_name && factories.count(class_name) > 0)
		return factories[class_name];
	return NULL;
}

template<class T>
const factory<T> * factory<T>::lookup(const params & config, const istr & config_name, const istr & alt_name)
{
	istr class_name;
	if(!config.get(config_name, &class_name))
		return NULL;
	/* if it wasn't there, try the alternate config name */
	if(!class_name && alt_name)
		if(!config.get(alt_name, &class_name))
			return NULL;
	return lookup(class_name);
}

template<class T>
void factory<T>::remove(const istr & class_name, const factory * factory)
{
	if(factories.count(class_name) > 0)
	{
		if(factories[class_name] != factory)
			fprintf(stderr, "Warning: attempt to remove mismatched factory \"%s\"\n", (const char *) class_name);
		else
			factories.erase(class_name);
	}
	else
		fprintf(stderr, "Warning: attempt to remove nonexistent factory \"%s\"\n", (const char *) class_name);
}

template<class T>
size_t factory<T>::list(const istr ** names)
{
	if(names)
	{
		typename factory_map::iterator iter = factories.begin();
		size_t i = 0, size = factories.size();
		istr * array = new istr[size];
		while(iter != factories.end())
		{
			array[i++] = (*iter).first;
			++iter;
		}
		*names = array;
	}
	return factories.size();
}

#endif /* __FACTORY_IMPL_H */
