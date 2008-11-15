/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_FACTORY_H
#define __CTABLE_FACTORY_H

#ifndef __cplusplus
#error ctable_factory.h is a C++ header file
#endif

#include "istr.h"
#include "params.h"
#include "factory.h"
#include "ctable.h"

/* although ctable itself does not suggest that it be implemented on top of dtables,
 * ctable_factory basically does require that for any ctables built via factories */
class ctable_factory_base
{
public:
	//virtual ctable * open(int dfd, const char * name, const params & config) const = 0;
	virtual ctable * open(const dtable * dt_source, const params & config) const = 0;
	virtual ctable * open(dtable * dt_source, const params & config) const = 0;
	
	virtual ~ctable_factory_base() {}
	
	/* wrappers for open() that do lookup() */
	static ctable * load(const istr & type, const dtable * dt_source, const params & config);
	static ctable * load(const istr & type, dtable * dt_source, const params & config);
};

typedef factory<ctable_factory_base> ctable_factory;

template<class T>
class ctable_static_factory : public ctable_factory
{
public:
	ctable_static_factory(const istr & class_name) : ctable_factory(class_name) {}
	
	virtual ctable * open(const dtable * dt_source, const params & config) const
	{
		T * table = new T;
		int r = table->init(dt_source, config);
		if(r < 0)
		{
			delete table;
			table = NULL;
		}
		return table;
	}
	
	virtual ctable * open(dtable * dt_source, const params & config) const
	{
		T * table = new T;
		int r = table->init(dt_source, config);
		if(r < 0)
		{
			delete table;
			table = NULL;
		}
		return table;
	}
};

#define DECLARE_CT_FACTORY(class_name) static const ctable_static_factory<class_name> factory;
#define DEFINE_CT_FACTORY(class_name) const ctable_static_factory<class_name> class_name::factory(#class_name);

#endif /* __CTABLE_FACTORY_H */
