/* This file is part of Anvil. Anvil is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __FACTORY_H
#define __FACTORY_H

#ifndef __cplusplus
#error factory.h is a C++ header file
#endif

#include <map>
#include "istr.h"
#include "params.h"

template<class T>
class factory : public T
{
public:
	/* factory registry stuff */
	static int add(const istr & class_name, const factory * factory);
	static const factory<T> * lookup(const istr & class_name);
	static const factory<T> * lookup(const params & config, const istr & config_name, const istr & alt_name = NULL);
	static void remove(const istr & class_name, const factory * factory);
	static size_t list(const istr ** names = NULL);
	
	/* deregister */
	virtual ~factory()
	{
		remove(name, this);
	}
	
	const istr name;
	
protected:
	/* register */
	inline factory(const istr & class_name)
		: name(class_name)
	{
		add(name, this);
	}
	
private:
	typedef std::map<istr, const factory *, strcmp_less> factory_map;
	static factory_map factories;
};

#endif /* __FACTORY_H */
