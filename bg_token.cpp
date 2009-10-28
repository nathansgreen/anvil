/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "bg_token.h"

#define DEBUG_BGT 0

#if DEBUG_BGT
#include <stdio.h>
#define BGT_DEBUG(format, args...) printf("%s @%p[%s, %s, %zu] (" format ")\n", __FUNCTION__, this, waiting ? "W" : "w", available ? "A" : "a", held, ##args)
#else
#define BGT_DEBUG(format, args...)
#endif

void bg_token::loan()
{
	BGT_DEBUG("");
	scopelock scope(lock);
	assert(waiting && !held);
	waiting = false;
	held = 1;
	scope.signal(wait);
	while(held)
		scope.wait(wait);
	BGT_DEBUG("finished");
}

void bg_token::wait_to_loan()
{
	BGT_DEBUG("");
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
	BGT_DEBUG("finished");
}

void bg_token::acquire()
{
	BGT_DEBUG("");
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
	BGT_DEBUG("acquired");
}

void bg_token::release()
{
	BGT_DEBUG("");
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
	BGT_DEBUG("");
	scopelock scope(lock);
	size_t old = held;
	assert(!waiting && held);
	held = 0;
	scope.signal(wait);
	return old;
}

void bg_token::full_acquire(size_t original)
{
	BGT_DEBUG("%zu", original);
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
	BGT_DEBUG("acquired");
}
