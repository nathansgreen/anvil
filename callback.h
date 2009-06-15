/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CALLBACK_H
#define __CALLBACK_H

#ifndef __cplusplus
#error callback.h is a C++ header file
#endif

#include <set>

class callback
{
public:
	inline callback() {}
	
	/* called when the event this callback was waiting for has occurred */
	virtual void invoke() = 0;
	/* called when the object that would have generated the event this
	 * callback was waiting for is being destroyed, so it will never occur */
	virtual void release() = 0;
	
	virtual ~callback() {}
	
private:
	void operator=(const callback &);
	callback(const callback &);
};

class callbacks
{
public:
	inline callbacks() {}
	
	inline void add(callback * cb)
	{
		set.insert(cb);
	}
	
	inline void remove(callback * cb)
	{
		set.erase(cb);
	}
	
	inline void invoke()
	{
		callback_set::iterator it = set.begin();
		while(it != set.end())
		{
			callback * cb = *it;
			set.erase(it++);
			cb->invoke();
		}
	}
	
	inline void release()
	{
		callback_set::iterator it = set.begin();
		for(it = set.begin(); it != set.end(); ++it)
			(*it)->release();
		set.clear();
	}
	
	inline ~callbacks()
	{
		release();
	}
	
private:
	typedef std::set<callback *> callback_set;
	callback_set set;
	void operator=(const callbacks &);
	callbacks(const callbacks &);
};

#endif /* __CALLBACK_H */
