/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "bg_token.h"

void bg_token::loan()
{
	scopelock scope(lock);
	assert(waiting && !held);
	waiting = false;
	held = 1;
	scope.signal(wait);
	while(held)
		scope.wait(wait);
}

void bg_token::wait_to_loan()
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

void bg_token::acquire()
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

void bg_token::release()
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

size_t bg_token::full_release()
{
	scopelock scope(lock);
	size_t old = held;
	assert(!waiting && held);
	held = 0;
	scope.signal(wait);
	return old;
}

void bg_token::full_acquire(size_t original)
{
	scopelock scope(lock);
	assert(!waiting && !held);
	if(available)
	{
		available = false;
		held = original;
		scope.signal(wait);
	}
	else
	{
		waiting = true;
		while(!held)
			scope.wait(wait);
		assert(held == 1);
		held = original;
	}
}
