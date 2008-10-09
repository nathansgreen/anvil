/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __HACK_MAP_H
#define __HACK_MAP_H

#ifndef __cplusplus
#error hack_map.h is a C++ header file
#endif

/* This file implements an ugly hack to add a new feature to std::map. The
 * feature is the ability to get iterators into the map based on a "magic test"
 * function rather than via a provided search key. The magic test function will
 * decide whether a given key passed to is is too large or too small (or the
 * right key), but it may not explicitly know what it is that it's looking for.
 * 
 * For instance, if you have a map of string keys sorted by their checksums, and
 * you want to find the key whose checksum is largest without exceeding some
 * specific checksum value, this ability will be useful. You don't actually have
 * a string to compare with when searching; the checksum function is not
 * reversible. Instead, the magic test function takes the checksum of the
 * candidate string and compares that against the target checksum.
 * 
 * To add this feature, we need access to some of the internals of std::map and
 * of the red-black tree that is used to implement it. We use the preprocessor
 * to redefine "private" temporarily to "protected" when including the <map>
 * header, and then subclass the relevant classes. We can then cast pointers to
 * regular std::map instances to pointers to the subclass, even though they are
 * not, since we do not add new fields and merely need the (non-virtual) methods
 * to have the right scope.
 * 
 * The downside of this is that this file must be included prior to <map> and
 * <set>, and it is reasonably likely that this will not be very portable. (It
 * works with g++ and the STL header files that come with it.) */

#if defined(_GLIBCXX_MAP) || defined(_GLIBCXX_SET) || defined(_RB_TREE)
#error hack_map.h must be included before <map>, <set>, and <ext/rb_tree>
#endif

#define private protected
#include <map>
#undef private

#include "magic_test.h"

namespace hack {

template<class K, class V, class C = std::less<K>, class A = std::allocator<std::pair<const K, V> > >
class std_map : public std::map<K, V, C, A>
{
public:
	typedef typename std::map<K, V, C, A>::iterator iterator;
	typedef typename std::map<K, V, C, A>::const_iterator const_iterator;
	
#define HACK_MAP_WRAPPERS(method) \
	inline iterator method(const magic_test<K> & test) \
	{ \
		return ((hack_tree *) &this->_M_t)->method(test); \
	} \
	inline const_iterator method(const magic_test<K> & test) const \
	{ \
		return ((hack_tree *) &this->_M_t)->method(test); \
	} \
	static inline iterator method(std::map<K, V, C, A> & map, const magic_test<K> & test) \
	{ \
		return ((std_map *) (&map))->method(test); \
	} \
	static inline const_iterator method(const std::map<K, V, C, A> & map, const magic_test<K> & test) \
	{ \
		return ((const std_map *) (&map))->method(test); \
	}
	
	/* generate functions with the preprocessor! whee! */
	HACK_MAP_WRAPPERS(find);
	HACK_MAP_WRAPPERS(lower_bound);
	HACK_MAP_WRAPPERS(upper_bound);
	
#undef HACK_MAP_WRAPPERS
	
private:
	std_map();
	std_map(const std_map & x);
	std_map & operator=(const std_map & x);
	
	typedef typename std::map<K, V, C, A>::_Rep_type _Rep_type;
	
	class hack_tree : public _Rep_type
	{
	public:
		/* find() */
		iterator find(const magic_test<K> & test)
		{
			_Link_type __x = _Rep_type::_M_begin();
			_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) >= 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			iterator __j = iterator(__y);
			return (__j == _Rep_type::end() || test(_S_key(__j._M_node))) ? _Rep_type::end() : __j;
		}
		const_iterator find(const magic_test<K> & test) const
		{
			_Const_Link_type __x = _Rep_type::_M_begin();
			_Const_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) >= 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			const_iterator __j = const_iterator(__y);
			return (__j == _Rep_type::end() || test(_S_key(__j._M_node))) ? _Rep_type::end() : __j;
		}
		
		/* lower_bound() */
		iterator lower_bound(const magic_test<K> & test)
		{
			_Link_type __x = _Rep_type::_M_begin();
			_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) >= 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			return iterator(__y);
		}
		const_iterator lower_bound(const magic_test<K> & test) const
		{
			_Const_Link_type __x = _Rep_type::_M_begin();
			_Const_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) >= 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			return const_iterator(__y);
		}
		
		/* lower_bound() */
		iterator upper_bound(const magic_test<K> & test)
		{
			_Link_type __x = _Rep_type::_M_begin();
			_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) > 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			return iterator(__y);
		}
		const_iterator upper_bound(const magic_test<K> & test) const
		{
			_Const_Link_type __x = _Rep_type::_M_begin();
			_Const_Link_type __y = _Rep_type::_M_end();
			while(__x != NULL)
				if(test(_S_key(__x)) > 0)
					__y = __x, __x = _S_left(__x);
				else
					__x = _S_right(__x);
			return const_iterator(__y);
		}
		
	private:
		typedef typename _Rep_type::_Link_type _Link_type;
		typedef typename _Rep_type::_Const_Link_type _Const_Link_type;
	};
};

}; /* namespace hack */

/* make all this hackery usable with minimal work - what would be map.find(key) becomes find(map, magic) */

#define HACK_MAP_GLOBALS(method) \
template<class K, class V, class C, class A> \
static inline typename std::map<K, V, C, A>::iterator method(std::map<K, V, C, A> & map, const magic_test<K> & test) \
{ \
	return hack::std_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename std::map<K, V, C, A>::const_iterator method(const std::map<K, V, C, A> & map, const magic_test<K> & test) \
{ \
	return hack::std_map<K, V, C, A>::method(map, test); \
} \
template<class K, int (*F)(const K &, void *), class V, class C, class A> \
static inline typename std::map<K, V, C, A>::iterator method(std::map<K, V, C, A> & map, void * user) \
{ \
	magic_test_fn<K, F> test(user); \
	return hack::std_map<K, V, C, A>::method(map, test); \
} \
template<class K, int (*F)(const K &, void *), class V, class C, class A> \
static inline typename std::map<K, V, C, A>::const_iterator method(const std::map<K, V, C, A> & map, void * user) \
{ \
	magic_test_fn<K, F> test(user); \
	return hack::std_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename std::map<K, V, C, A>::iterator method(std::map<K, V, C, A> & map, typename magic_test_fnp<K>::test_fnp fnp, void * user) \
{ \
	magic_test_fnp<K> test(fnp, user); \
	return hack::std_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename std::map<K, V, C, A>::const_iterator method(const std::map<K, V, C, A> & map, typename magic_test_fnp<K>::test_fnp fnp, void * user) \
{ \
	magic_test_fnp<K> test(fnp, user); \
	return hack::std_map<K, V, C, A>::method(map, test); \
}

/* generate templates with the preprocessor! whee! */
HACK_MAP_GLOBALS(find);
HACK_MAP_GLOBALS(lower_bound);
HACK_MAP_GLOBALS(upper_bound);

#undef HACK_MAP_GLOBALS

#endif /* __HACK_MAP_H */
