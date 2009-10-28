/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __BG_TOKEN_H
#define __BG_TOKEN_H

#ifndef __cplusplus
#error bg_token.h is a C++ header file
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
	void loan();
	/* be careful with this: if the background thread never
	 * wants the token again, this will wait forever */
	void wait_to_loan();
	
	/* these should be called from background threads */
	void acquire();
	void release();
	/* these release and re-acquire any number of holds, for use
	 * deep inside background thread logic when a long-running,
	 * fully background task is about to start or has just ended */
	size_t full_release();
	void full_acquire(size_t original);
	
private:
	bool waiting, available;
	size_t held;
	init_mutex lock;
	init_cond wait;
};

/* same background thread interface as bg_token but does nothing, for use
 * in template methods that can run in the foreground or background */

class fg_token
{
public:
	inline void acquire() {}
	inline void release() {}
	inline size_t full_release() { return 0; }
	inline void full_acquire(size_t original) {}
};

/* a simple wrapper class to handle releasing a token when exiting a scope */

template<class T>
class scopetoken
{
public:
	inline scopetoken(T * token, bool acquire = true)
		: token(token), held(acquire), full(false)
	{
		if(acquire)
			token->acquire();
	}
	
	inline ~scopetoken()
	{
		assert(!full);
		if(held)
			token->release();
	}
	
	inline void acquire()
	{
		assert(!held && !full);
		held = true;
		token->acquire();
	}
	
	inline void release()
	{
		assert(held && !full);
		held = false;
		token->release();
	}
	
	inline size_t full_release()
	{
		assert(held && !full);
		held = false;
		full = true;
		return token->full_release();
	}
	
	inline void full_acquire(size_t original)
	{
		assert(!held && full);
		held = true;
		full = false;
		token->full_acquire(original);
	}
	
private:
	T * token;
	bool held, full;
	void operator=(const scopetoken &);
	scopetoken(const scopetoken &);
};

#endif /* __BG_TOKEN_H */
