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
		scope.wait(&wait);
		assert(!running);
	}
	
	/* should be called only from the thread */
	inline bool stop_requested()
	{
		return stop_request;
	}
	
	/* this syntax means "pointer to member function of class T" */
	typedef void (T::*method_t)();
	
	bg_thread(T * object, method_t method)
		: stop_request(false), running(false), object(object), method(method)
	{
		pthread_cond_init(&wait, NULL);
	}
	
	~bg_thread()
	{
		wait_for_stop();
		pthread_cond_destroy(&wait);
	}
	
private:
	bool stop_request, running;
	
	T * const object;
	const method_t method;
	
	init_mutex lock;
	pthread_cond_t wait;
	
	void _start()
	{
		scopelock scope(lock);
		running = true;
		scope.unlock();
		/* call the method */
		(object->*method)();
		scope.lock();
		running = false;
		scope.broadcast(&wait);
	}
	
	static void * _start_static(void * arg)
	{
		((bg_thread *) arg)->_start();
		return NULL;
	}
	
	void operator=(const bg_thread &);
	bg_thread(const bg_thread &);
};

#endif /* __BG_THREAD_H */
