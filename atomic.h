/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __ATOMIC_H
#define __ATOMIC_H

#include <pthread.h>

#ifndef __cplusplus
#error atomic.h is a C++ header file
#endif

/* a simple wrapper class to handle initializing a mutex when it
 * is allocated, and also shorten lock and unlock code for it */

class init_mutex
{
public:
	inline init_mutex()
	{
		pthread_mutex_init(&mutex, NULL);
	}
	
	inline ~init_mutex()
	{
		pthread_mutex_destroy(&mutex);
	}
	
	inline void lock()
	{
		pthread_mutex_lock(&mutex);
	}
	
	inline void unlock()
	{
		pthread_mutex_lock(&mutex);
	}
	
	inline operator pthread_mutex_t * ()
	{
		return &mutex;
	}
	
private:
	pthread_mutex_t mutex;
	void operator=(const init_mutex &);
	init_mutex(const init_mutex &);
};

/* a simple wrapper class to handle unlocking a mutex when exiting a
 * scope, and also shorten condition variable code using that mutex */

class atomic
{
public:
	inline atomic(pthread_mutex_t * mutex)
		: mutex(mutex), locked(true)
	{
		pthread_mutex_lock(mutex);
	}
	
	inline ~atomic()
	{
		if(locked)
			pthread_mutex_unlock(mutex);
	}
	
	inline void lock()
	{
		assert(!locked);
		locked = true;
		pthread_mutex_lock(mutex);
	}
	
	inline void unlock()
	{
		assert(locked);
		locked = false;
		pthread_mutex_unlock(mutex);
	}
	
	inline void wait(pthread_cond_t * cond)
	{
		assert(locked);
		pthread_cond_wait(cond, mutex);
	}
	
	inline void signal(pthread_cond_t * cond)
	{
		assert(locked);
		pthread_cond_signal(cond);
	}
	
	inline void broadcast(pthread_cond_t * cond)
	{
		assert(locked);
		pthread_cond_broadcast(cond);
	}
	
private:
	pthread_mutex_t * mutex;
	bool locked;
	void operator=(const atomic &);
	atomic(const atomic &);
};

#endif /* __ATOMIC_H */
