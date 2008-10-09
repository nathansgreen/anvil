/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MAGIC_TEST_H
#define __MAGIC_TEST_H

#ifndef __cplusplus
#error magic_test.h is a C++ header file
#endif

template<class K>
class magic_test
{
public:
	/* return < 0 for "too small", 0 for "just right", and > 0 for "too big" */
	/* the behavior of this function must be consistent with the sort order of the map! */
	virtual int operator()(const K & key) const = 0;
	inline virtual ~magic_test() {}
};

template<class K, int (*F)(const K &, void *)>
class magic_test_fn : public magic_test<K>
{
public:
	magic_test_fn(void * user) : user(user) {}
	
	virtual int operator()(const K & key) const
	{
		return F(key, user);
	}
	
private:
	void * user;
};

template<class K>
class magic_test_fnp : public magic_test<K>
{
public:
	typedef int (*test_fnp)(const K & key, void * user);
	
	magic_test_fnp(test_fnp test, void * user) : test(test), user(user) {}
	
	virtual int operator()(const K & key) const
	{
		return test(key, user);
	}
	
private:
	test_fnp test;
	void * user;
};

#endif /* __MAGIC_TEST_H */
