/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_FACTORY_H
#define __CTABLE_FACTORY_H

#include <stdio.h>

#ifndef __cplusplus
#error ctable_factory.h is a C++ header file
#endif

#include "istr.h"
#include "params.h"
#include "factory.h"
#include "ctable.h"

class ctable_factory_base
{
public:
	virtual ctable * open(int dfd, const char * name, const params & config) const { return NULL; }
	
	virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const { return -ENOSYS; }
	
	virtual ~ctable_factory_base() {}
	
	/* wrapper for open() that does lookup() */
	static ctable * load(const istr & type, int dfd, const char * name, const params & config);
	/* wrapper for create() that does lookup() */
	static int setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type);
};

typedef factory<ctable_factory_base> ctable_factory;

template<class T>
class ctable_open_factory : public ctable_factory
{
public:
	ctable_open_factory(const istr & class_name) : ctable_factory(class_name) {}
	
	inline virtual ctable * open(int dfd, const char * name, const params & config) const
	{
		T * disk = new T;
		int r = disk->init(dfd, name, config);
		if(r < 0)
		{
			delete disk;
			disk = NULL;
		}
		else
		{
			/* do we care about failure here? */
			r = disk->maintain();
			if(r < 0)
				fprintf(stderr, "Warning: failed to maintain \"%s\" after opening (%s)\n", name, this->name.str());
		}
		return disk;
	}
	
	inline virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const
	{
		return T::create(dfd, name, config, key_type);
	}
};

/* for ctables which are stored on disk in some way, like most dtables */
#define DECLARE_CT_FACTORY(class_name) static const ctable_open_factory<class_name> factory;
#define DEFINE_CT_FACTORY(class_name) const ctable_open_factory<class_name> class_name::factory(#class_name);

#endif /* __CTABLE_FACTORY_H */
