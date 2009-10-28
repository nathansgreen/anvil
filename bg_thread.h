/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BG_THREAD_H
#define __BG_THREAD_H

#ifndef __cplusplus
#error bg_thread.h is a C++ header file
#endif

#include "bg_token.h"

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

#endif /* __BG_THREAD_H */
