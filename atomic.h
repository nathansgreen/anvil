/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ATOMIC_H
#define __ATOMIC_H

#ifndef __cplusplus
#error atomic.h is a C++ header file
#endif

/* this template class wraps some GCC builtins for atomic integer operations */

/* We deliberately do not overload all the operators so that code using this
 * class will be obviously different than code using a raw integer. That way it
 * will be easier to tell when this class is in use, and thus, hopefully, that
 * the code is written correctly. */

template<class T>
class atomic
{
public:
	inline atomic(T value = 0) : value(value) {}
	
	/* returns the old value */
	inline T inc()
	{
		return add(1);
	}
	
	/* returns the new value */
	inline T dec()
	{
		return sub(1);
	}
	
	/* returns the old value */
	inline T add(T delta)
	{
		return __sync_fetch_and_add(&value, delta);
	}
	
	/* returns the new value */
	inline T sub(T delta)
	{
		return __sync_sub_and_fetch(&value, delta);
	}
	
	inline T get() const
	{
		/* is there a better way? */
		return __sync_add_and_fetch(&value, 0);
	}
	
	/* returns the old value */
	inline T zero()
	{
		return __sync_fetch_and_and(&value, 0);
	}
	
private:
	/* mutable only for get() which technically writes to it */
	mutable T value;
	
	void operator=(const atomic &);
	atomic(const atomic &);
};

#endif /* __ATOMIC_H */
