/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BG_THREAD_H
#define __BG_THREAD_H

#include <assert.h>
#include <pthread.h>

#ifndef __cplusplus
#error bg_thread.h is a C++ header file
#endif

#include "locking.h"

/* a background token allows the main thread to temporarily
 * "lease" its lock on something to a background thread */
class bg_token
{
public:
	inline bg_token() : waiting(false), available(false), held(0) {}
	inline ~bg_token() { assert(!waiting && !held); }
	
	/* these should be called from the foreground thread */
	inline bool wanted()
	{
		/* no need to acquire the lock */
		return waiting;
	}
	void loan()
	{
		scopelock scope(lock);
		assert(waiting && !held);
		waiting = false;
		held = 1;
		scope.signal(wait);
		while(held)
			scope.wait(wait);
	}
	/* be careful with this: if the background thread never
	 * wants the token again, this will wait forever */
	void wait_to_loan()
	{
		scopelock scope(lock);
		assert(!held);
		if(waiting)
		{
			/* same as loan() above */
			waiting = false;
			held = 1;
			scope.signal(wait);
			while(held)
				scope.wait(wait);
		}
		else
		{
			available = true;
			while(available || held)
				scope.wait(wait);
		}
	}
	
	/* these should be called from background threads */
	void acquire()
	{
		if(held)
		{
			/* we already have the token */
			held++;
			return;
		}
		scopelock scope(lock);
		assert(!waiting);
		if(available)
		{
			available = false;
			held = 1;
			scope.signal(wait);
		}
		else
		{
			waiting = true;
			while(!held)
				scope.wait(wait);
		}
	}
	void release()
	{
		if(held > 1)
		{
			/* we will still have the token */
			held--;
			return;
		}
		scopelock scope(lock);
		assert(!waiting && held);
		held = 0;
		scope.signal(wait);
	}
private:
	bool waiting, available;
	size_t held;
	init_mutex lock;
	init_cond wait;
};

template<class T>
class bg_thread
{
public:
	void start()
	{
		pthread_t thread;
		scopelock scope(lock);
		if(running)
			return;
		stop_request = false;
		pthread_create(&thread, NULL, _start_static, this);
		pthread_detach(thread);
	}
	
	inline void request_stop()
	{
		stop_request = true;
	}
	
	void wait_for_stop()
	{
		scopelock scope(lock);
		if(!running)
			return;
		stop_request = true;
		scope.wait(wait);
		assert(!running);
	}
	
	/* should be called only from the thread */
	inline bool stop_requested()
	{
		return stop_request;
	}
	
	bool wants_token()
	{
		return token.wanted();
	}
	
	void loan_token()
	{
		token.loan();
	}
	
	/* this syntax means "pointer to member function of class T" */
	typedef void (T::*method_t)(bg_token *);
	
	bg_thread(T * object, method_t method)
		: stop_request(false), running(false), object(object), method(method)
	{
	}
	
	~bg_thread()
	{
		wait_for_stop();
	}
	
private:
	bool stop_request, running;
	
	T * const object;
	const method_t method;
	
	init_mutex lock;
	init_cond wait;
	bg_token token;
	
	void _start()
	{
		scopelock scope(lock);
		running = true;
		scope.unlock();
		/* call the method */
		(object->*method)(&token);
		scope.lock();
		running = false;
		scope.broadcast(wait);
	}
	
	static void * _start_static(void * arg)
	{
		((bg_thread *) arg)->_start();
		return NULL;
	}
	
	void operator=(const bg_thread &);
	bg_thread(const bg_thread &);
};

/* same background thread interface as bg_token but does nothing, for use
 * in template methods that can run in the foreground or background */
class fg_token
{
public:
	inline void acquire() {}
	inline void release() {}
};

/* a simple wrapper class to handle releasing a token when exiting a scope */

template<class T>
class scopetoken
{
public:
	inline scopetoken(T * token, bool acquire = true)
		: token(token), held(acquire)
	{
		if(acquire)
			token->acquire();
	}
	
	inline ~scopetoken()
	{
		if(held)
			token->release();
	}
	
	inline void acquire()
	{
		assert(!held);
		held = true;
		token->acquire();
	}
	
	inline void release()
	{
		assert(held);
		held = false;
		token->release();
	}
	
private:
	T * token;
	bool held;
	void operator=(const scopetoken &);
	scopetoken(const scopetoken &);
};

#endif /* __BG_THREAD_H */
