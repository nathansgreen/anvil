/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_FACTORY_H
#define __DTABLE_FACTORY_H

#ifndef __cplusplus
#error dtable_factory.h is a C++ header file
#endif

#include "istr.h"
#include "params.h"
#include "factory.h"
#include "dtable.h"
#include "empty_dtable.h"

/* override one or both create() methods in subclasses */
class dtable_factory_base
{
public:
	virtual dtable * open(int dfd, const char * name, const params & config) const = 0;
	
	/* create a new empty table with the given key type - this default implementation
	 * will use the other version of create() and an empty_dtable for the source */
	inline virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const
	{
		empty_dtable empty(key_type);
		return create(dfd, name, config, &empty, NULL);
	}
	
	/* non-existent entries in the source which are present in the shadow
	 * (as existent entries) will be kept as non-existent entries in the
	 * result, otherwise they will be omitted since they are not needed */
	/* shadow may also be NULL in which case it is treated as empty */
	inline virtual int create(int dfd, const char * name, const params & config, const dtable * source, const dtable * shadow) const
	{
		return -ENOSYS;
	}
	
	/* returns true if the class this factory will construct supports indexed access */
	virtual bool indexed_access() const = 0;
	
	virtual ~dtable_factory_base() {}
	
	/* wrapper for open() that does lookup() */
	static dtable * load(const istr & type, int dfd, const char * name, const params & config);
	/* wrappers for create() that do lookup() */
	static int setup(const istr & type, int dfd, const char * name, const params & config, dtype::ctype key_type);
	static int setup(const istr & type, int dfd, const char * name, const params & config, const dtable * source, const dtable * shadow);
};

typedef factory<dtable_factory_base> dtable_factory;

template<class T>
class dtable_open_factory : public dtable_factory
{
public:
	dtable_open_factory(const istr & class_name) : dtable_factory(class_name) {}
	
	virtual dtable * open(int dfd, const char * name, const params & config) const
	{
		T * disk = new T;
		int r = disk->init(dfd, name, config);
		if(r < 0)
		{
			delete disk;
			disk = NULL;
		}
		else
			/* do we care about failure here? */
			disk->maintain();
		return disk;
	}
	
	inline virtual bool indexed_access() const
	{
		return T::static_indexed_access();
	}
};

template<class T>
class dtable_ro_factory : public dtable_open_factory<T>
{
public:
	dtable_ro_factory(const istr & class_name) : dtable_open_factory<T>(class_name) {}
	
	inline virtual int create(int dfd, const char * name, const params & config, const dtable * source, const dtable * shadow) const
	{
		return T::create(dfd, name, config, source, shadow);
	}
};

template<class T>
class dtable_rw_factory : public dtable_open_factory<T>
{
public:
	dtable_rw_factory(const istr & class_name) : dtable_open_factory<T>(class_name) {}
	
	inline virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const
	{
		return T::create(dfd, name, config, key_type);
	}
};

template<class T>
class dtable_wrap_factory : public dtable_open_factory<T>
{
public:
	inline dtable_wrap_factory(const istr & class_name) : dtable_open_factory<T>(class_name) {}
	
	inline virtual int create(int dfd, const char * name, const params & config, const dtable * source, const dtable * shadow) const
	{
		params base_config;
		const dtable_factory * base = dtable_factory::lookup(config, "base");
		if(!base)
			return -EINVAL;
		if(!config.get("base_config", &base_config, params()))
			return -EINVAL;
		return base->create(dfd, name, base_config, source, shadow);
	}
	
	inline virtual int create(int dfd, const char * name, const params & config, dtype::ctype key_type) const
	{
		params base_config;
		const dtable_factory * base = dtable_factory::lookup(config, "base");
		if(!base)
			return -EINVAL;
		if(!config.get("base_config", &base_config, params()))
			return -EINVAL;
		return base->create(dfd, name, base_config, key_type);
	}
};

/* for dtables which can only be opened, perhaps data from some external source */
#define DECLARE_OPEN_FACTORY(class_name) static const dtable_open_factory<class_name> factory
#define DEFINE_OPEN_FACTORY(class_name) const dtable_open_factory<class_name> class_name::factory(#class_name)

/* for dtables which must be created with all the data they will ever contain */
#define DECLARE_RO_FACTORY(class_name) static const dtable_ro_factory<class_name> factory
#define DEFINE_RO_FACTORY(class_name) const dtable_ro_factory<class_name> class_name::factory(#class_name)

/* for dtables which generally are created empty and then populated */
#define DECLARE_RW_FACTORY(class_name) static const dtable_rw_factory<class_name> factory
#define DEFINE_RW_FACTORY(class_name) const dtable_rw_factory<class_name> class_name::factory(#class_name)

/* for dtables which wrap other dtables at runtime but not on disk */
#define DECLARE_WRAP_FACTORY(class_name) static const dtable_wrap_factory<class_name> factory
#define DEFINE_WRAP_FACTORY(class_name) const dtable_wrap_factory<class_name> class_name::factory(#class_name)

#endif /* __DTABLE_FACTORY_H */
