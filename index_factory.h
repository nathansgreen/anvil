/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __INDEX_FACTORY_H
#define __INDEX_FACTORY_H

#ifndef __cplusplus
#error index_factory.h is a C++ header file
#endif

#include "dtype.h"
#include "factory.h"
#include "ext_index.h"

class dtable;
class params;

/* although ext_index itself does not suggest that it be implemented on top of dtables,
 * index_factory basically does require that for any ext_indexes built via factories */
class index_factory_base
{
public:
	virtual ext_index * open(const dtable * store, dtype::ctype pri_key_type, const params & config) const = 0;
	virtual ext_index * open(dtable * store, dtype::ctype pri_key_type, const params & config) const = 0;
	
	virtual ~index_factory_base() {}
	
	/* wrappers for open() that do lookup() */
	static ext_index * load(const istr & type, const dtable * dt_source, dtype::ctype pri_key_type, const params & config);
	static ext_index * load(const istr & type, dtable * dt_source, dtype::ctype pri_key_type, const params & config);
	static ext_index * load(const dtable * dt_source, dtype::ctype pri_key_type, const params & config);
	static ext_index * load(dtable * dt_source, dtype::ctype pri_key_type, const params & config);
};

typedef factory<index_factory_base> index_factory;

template<class T>
class index_static_factory : public index_factory
{
public:
	index_static_factory(const istr & class_name) : index_factory(class_name) {}
	
	virtual ext_index * open(const dtable * store, dtype::ctype pri_key_type, const params & config) const
	{
		T * index = new T;
		int r = index->init(store, pri_key_type, config);
		if(r < 0)
		{
			delete index;
			index = NULL;
		}
		return index;
	}
	
	virtual ext_index * open(dtable * store, dtype::ctype pri_key_type, const params & config) const
	{
		T * index = new T;
		int r = index->init(store, pri_key_type, config);
		if(r < 0)
		{
			delete index;
			index = NULL;
		}
		return index;
	}
};

#define DECLARE_EI_FACTORY(class_name) static const index_static_factory<class_name> factory;
#define DEFINE_EI_FACTORY(class_name) const index_static_factory<class_name> class_name::factory(#class_name);

#endif /* __INDEX_FACTORY_H */
