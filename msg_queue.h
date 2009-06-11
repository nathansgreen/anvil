/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MSG_QUEUE_H
#define __MSG_QUEUE_H

#include <pthread.h>

#ifndef __cplusplus
#error msg_queue.h is a C++ header file
#endif

#include "locking.h"

/* a simple message queue */

template<class T, size_t size = 8>
class msg_queue
{
public:
	inline msg_queue()
		: filled(0), next_filled(0), next_empty(0)
	{
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&not_filled, NULL);
		pthread_cond_init(&not_empty, NULL);
	}
	
	inline ~msg_queue()
	{
		pthread_cond_destroy(&not_empty);
		pthread_cond_destroy(&not_filled);
		pthread_mutex_destroy(&mutex);
	}
	
	inline void send(const T & msg)
	{
		scopelock scope(&mutex);
		while(filled == size)
			scope.wait(&not_filled);
		filled++;
		messages[next_empty] = msg;
		if(++next_empty == size)
			next_empty = 0;
		scope.signal(&not_empty);
	}
	
	inline void receive(T * msg)
	{
		scopelock scope(&mutex);
		while(!filled)
			scope.wait(&not_empty);
		filled--;
		*msg = messages[next_filled];
		if(++next_filled == size)
			next_filled = 0;
		scope.signal(&not_filled);
	}
	
	inline bool try_receive(T * msg)
	{
		scopelock scope(&mutex);
		if(!filled)
			return false;
		filled--;
		*msg = messages[next_filled];
		if(++next_filled == size)
			next_filled = 0;
		scope.signal(&not_filled);
		return true;
	}
	
	/* flush() is not called from ~msg_queue() in case T does not have a release() method */
	inline void flush()
	{
		scopelock scope(&mutex);
		while(filled)
		{
			messages[next_filled].release();
			if(++next_filled == size)
				next_filled = 0;
		}
		scope.signal(&not_filled);
	}
	
private:
	T messages[size];
	size_t filled, next_filled, next_empty;
	pthread_mutex_t mutex;
	pthread_cond_t not_filled, not_empty;
};

#endif /* __MSG_QUEUE_H */
