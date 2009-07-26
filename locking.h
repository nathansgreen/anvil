/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __LOCKING_H
#define __LOCKING_H

#include <assert.h>
#include <pthread.h>

#ifndef __cplusplus
#error locking.h is a C++ header file
#endif

#define LOCK_DEBUG 0

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
#if LOCK_DEBUG
		holder = pthread_self();
		locked = true;
#endif
	}
	
	inline void unlock()
	{
#if LOCK_DEBUG
		locked = false;
#endif
		pthread_mutex_unlock(&mutex);
	}
	
	inline void assert_locked()
	{
#if LOCK_DEBUG
		assert(locked);
		assert(holder == pthread_self());
#endif
	}
	
private:
	pthread_mutex_t mutex;
#if LOCK_DEBUG
	pthread_t holder;
	bool locked;
#endif
	void operator=(const init_mutex &);
	init_mutex(const init_mutex &);
	
	friend class init_cond;
};

/* a simple wrapper class to handle initializing a condition variable
 * when it is allocated, and also shorten wait and signal code for it */

class init_cond
{
public:
	inline init_cond()
	{
		pthread_cond_init(&cond, NULL);
	}
	
	inline ~init_cond()
	{
		pthread_cond_destroy(&cond);
	}
	
	inline void wait(init_mutex & lock)
	{
		pthread_cond_wait(&cond, &lock.mutex);
	}
	
	inline void signal()
	{
		pthread_cond_signal(&cond);
	}
	
	inline void broadcast()
	{
		pthread_cond_broadcast(&cond);
	}
	
private:
	pthread_cond_t cond;
	void operator=(const init_cond &);
	init_cond(const init_cond &);
};

/* a simple wrapper class to handle unlocking a mutex when exiting a
 * scope, and also shorten condition variable code using that mutex */

class scopelock
{
public:
	inline scopelock(init_mutex & lock, bool do_lock = true)
		: mutex(&lock), locked(do_lock)
	{
		if(do_lock)
			mutex->lock();
	}
	
	inline ~scopelock()
	{
		if(locked)
			mutex->unlock();
	}
	
	inline void lock()
	{
		assert(!locked);
		locked = true;
		mutex->lock();
	}
	
	inline void unlock()
	{
		assert(locked);
		locked = false;
		mutex->unlock();
	}
	
	inline void wait(init_cond & cond)
	{
		assert(locked);
		cond.wait(*mutex);
	}
	
	inline void signal(init_cond & cond)
	{
		assert(locked);
		cond.signal();
	}
	
	inline void broadcast(init_cond & cond)
	{
		assert(locked);
		cond.broadcast();
	}
	
private:
	init_mutex * mutex;
	bool locked;
	void operator=(const scopelock &);
	scopelock(const scopelock &);
};

#endif /* __LOCKING_H */
