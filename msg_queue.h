/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MSG_QUEUE_H
#define __MSG_QUEUE_H

#ifndef __cplusplus
#error msg_queue.h is a C++ header file
#endif

#include "locking.h"

/* a simple message queue */

template<class T, size_t size = 8>
class msg_queue
{
public:
	inline msg_queue() : filled(0), next_filled(0), next_empty(0) {}
	
	inline void send(const T & msg)
	{
		scopelock scope(lock);
		while(filled == size)
			scope.wait(not_filled);
		filled++;
		messages[next_empty] = msg;
		if(++next_empty == size)
			next_empty = 0;
		scope.signal(not_empty);
	}
	
	inline void receive(T * msg)
	{
		scopelock scope(lock);
		while(!filled)
			scope.wait(not_empty);
		filled--;
		*msg = messages[next_filled];
		if(++next_filled == size)
			next_filled = 0;
		scope.signal(not_filled);
	}
	
	inline bool try_receive(T * msg)
	{
		scopelock scope(lock);
		if(!filled)
			return false;
		filled--;
		*msg = messages[next_filled];
		if(++next_filled == size)
			next_filled = 0;
		scope.signal(not_filled);
		return true;
	}
	
	/* flush() is not called from ~msg_queue() in case T does not have a release() method */
	inline void flush()
	{
		scopelock scope(lock);
		while(filled)
		{
			messages[next_filled].release();
			if(++next_filled == size)
				next_filled = 0;
		}
		scope.signal(not_filled);
	}
	
private:
	T messages[size];
	size_t filled, next_filled, next_empty;
	init_mutex lock;
	init_cond not_filled, not_empty;
};

#endif /* __MSG_QUEUE_H */
