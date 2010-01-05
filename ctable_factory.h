/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CTABLE_FACTORY_H
#define __CTABLE_FACTORY_H

#include <stdio.h>
#include <errno.h>
#include <stddef.h>

#ifndef __cplusplus
#error ctable_factory.h is a C++ header file
#endif

#include "dtype.h"
#include "ctable.h"
#include "factory.h"

class params;
class sys_journal;

class ctable_factory_base
{
public:
	virtual ctable * open(int dfd, const char * name, const params & config, sys_journal * sysj) const { return NULL; }
	
	virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const { return -ENOSYS; }
	
	virtual ~ctable_factory_base() {}
	
	/* wrappers for open() that do lookup() */
	static ctable * load(const istr & type, int dfd, const char * name, const params & config, sys_journal * sysj);
	static ctable * load(int dfd, const char * name, const params & config, sys_journal * sysj);
	/* wrappers for create() that do lookup() */
	static int setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type);
	static int setup(int dfd, const char * name, const params & config, dtype::ctype key_type);
};

typedef factory<ctable_factory_base> ctable_factory;

template<class T>
class ctable_open_factory : public ctable_factory
{
public:
	ctable_open_factory(const istr & class_name) : ctable_factory(class_name) {}
	
	inline virtual ctable * open(int dfd, const char * name, const params & config, sys_journal * sysj) const
	{
		T * disk = new T;
		int r = disk->init(dfd, name, config, sysj);
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
