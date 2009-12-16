/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __EXIST_DTABLE_H
#define __EXIST_DTABLE_H

#ifndef __cplusplus
#error exist_dtable.h is a C++ header file
#endif

#include "dtable_factory.h"

/* The exist_dtable is actually not a dtable class. At runtime, it is an
 * overlay dtable of the two underlying dtables. At create time, it creates
 * the underlying dtables. At open time, it sets up the overlay. As such it
 * looks quite a bit different than most dtables. */

class exist_dtable_factory : public dtable_factory
{
public:
	exist_dtable_factory() : dtable_factory("exist_dtable") {}
	virtual dtable * open(int dfd, const char * name, const params & config, sys_journal * sysj) const;
	virtual int create(int dfd, const char * name, const params & config, dtable::iter * source, const ktable * shadow = NULL) const;
	inline virtual bool indexed_access(const params & config) const { return false; }
	virtual ~exist_dtable_factory() {}
};

/* As above, there will never actually be an instance of this class. But
 * inherit from dtable anyway to get useful utility methods for our create()
 * method like source_shadow_ok(). */
class exist_dtable : private dtable
{
public:
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	static const exist_dtable_factory factory;
	
private:
	/* missing pure virtual methods, but private constructor anyway */
	inline exist_dtable() {}
	
	struct nonshadow_skip_test
	{
		inline nonshadow_skip_test(const ktable * shadow) : shadow(shadow) {}
		inline bool operator()(const dtable::iter * iter)
		{
			return iter->meta().exists() || !shadow->contains(iter->key());
		}
	private:
		const ktable * shadow;
	};
};

#endif /* __EXIST_DTABLE_H */
