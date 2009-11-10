/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __CONCAT_QUEUE_H
#define __CONCAT_QUEUE_H

#include <assert.h>

#ifndef __cplusplus
#error concat_queue.h is a C++ header file
#endif

#include <ext/slist>

/* This is basically a queue: it provides constant time enqueueing and
 * dequeueing (append and first/pop, respectively), but also supports
 * appending an entire queue to another (also in constant time). */

template<class T, class A = std::allocator<T> >
class concat_queue
{
public:
	typedef __gnu_cxx::slist<T, A> list_type;
	
	inline concat_queue() {}
	inline size_t size() const { return elements.size(); }
	inline bool empty() const { return elements.empty(); }
	inline void clear() { elements.clear(); last = elements.begin(); }
	
	inline void append(const T & x)
	{
		if(last == elements.end())
			last = elements.insert(last, x);
		else
			last = elements.insert_after(last, x);
	}
	inline void append(concat_queue & x)
	{
		assert(&x != this);
		if(last == elements.end())
			elements.swap(x.elements);
		else if(x.last != x.elements.end())
			elements.splice_after(last, x.elements);
		else
			return;
		last = x.last;
		x.last = x.elements.begin();
	}
	
	inline const T & first() const { return elements.front(); }
	inline void pop() { elements.pop_front(); }
	
private:
	list_type elements;
	typename list_type::iterator last;
	
        void operator=(const concat_queue &);
        concat_queue(const concat_queue &);
};

#endif /* __CONCAT_QUEUE_H */
