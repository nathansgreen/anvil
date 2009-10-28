/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __HACK_MAP_H
#define __HACK_MAP_H

#ifndef __cplusplus
#error hack_avl_map.h is a C++ header file
#endif

/* This file implements an ugly hack to add a new feature to avl::map. It is
 * just like hack_map.h, except for avl::map instead of std::map. Although it is
 * not necessary, since we include the full source for avl::map and could just
 * change it directly, this approach leaves it very easy to switch between
 * std::map and avl::map without significantly changing client code. */

#if defined(_AVL_MAP) || defined(_AVL_SET)
#error hack_avl_map.h must be included before avl/map.h and avl/set.h
#endif

#define private protected
#include "avl/map.h"
#undef private

#include "magic_test.h"

namespace hack {

template<class K, class V, class C = std::less<K>, class A = std::allocator<std::pair<const K, V> > >
class avl_map : public avl::map<K, V, C, A>
{
public:
	typedef typename avl::map<K, V, C, A>::iterator iterator;
	typedef typename avl::map<K, V, C, A>::const_iterator const_iterator;
	
#define HACK_MAP_WRAPPERS(method) \
	inline iterator method(const magic_test<K> & test) \
	{ \
		return ((hack_tree *) &this->_M_t)->method(test); \
	} \
	inline const_iterator method(const magic_test<K> & test) const \
	{ \
		return ((hack_tree *) &this->_M_t)->method(test); \
	} \
	static inline iterator method(avl::map<K, V, C, A> & map, const magic_test<K> & test) \
	{ \
		return ((avl_map *) (&map))->method(test); \
	} \
	static inline const_iterator method(const avl::map<K, V, C, A> & map, const magic_test<K> & test) \
	{ \
		return ((const avl_map *) (&map))->method(test); \
	}
	
	/* generate functions with the preprocessor! whee! */
	HACK_MAP_WRAPPERS(find);
	HACK_MAP_WRAPPERS(lower_bound);
	HACK_MAP_WRAPPERS(upper_bound);
	
#undef HACK_MAP_WRAPPERS
	
private:
	avl_map();
	avl_map(const avl_map & x);
	avl_map & operator=(const avl_map & x);
	
	typedef typename avl::map<K, V, C, A>::_Rep_type _Rep_type;
	
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
static inline typename avl::map<K, V, C, A>::iterator method(avl::map<K, V, C, A> & map, const magic_test<K> & test) \
{ \
	return hack::avl_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename avl::map<K, V, C, A>::const_iterator method(const avl::map<K, V, C, A> & map, const magic_test<K> & test) \
{ \
	return hack::avl_map<K, V, C, A>::method(map, test); \
} \
template<class K, int (*F)(const K &, void *), class V, class C, class A> \
static inline typename avl::map<K, V, C, A>::iterator method(avl::map<K, V, C, A> & map, void * user) \
{ \
	magic_test_fn<K, F> test(user); \
	return hack::avl_map<K, V, C, A>::method(map, test); \
} \
template<class K, int (*F)(const K &, void *), class V, class C, class A> \
static inline typename avl::map<K, V, C, A>::const_iterator method(const avl::map<K, V, C, A> & map, void * user) \
{ \
	magic_test_fn<K, F> test(user); \
	return hack::avl_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename avl::map<K, V, C, A>::iterator method(avl::map<K, V, C, A> & map, typename magic_test_fnp<K>::test_fnp fnp, void * user) \
{ \
	magic_test_fnp<K> test(fnp, user); \
	return hack::avl_map<K, V, C, A>::method(map, test); \
} \
template<class K, class V, class C, class A> \
static inline typename avl::map<K, V, C, A>::const_iterator method(const avl::map<K, V, C, A> & map, typename magic_test_fnp<K>::test_fnp fnp, void * user) \
{ \
	magic_test_fnp<K> test(fnp, user); \
	return hack::avl_map<K, V, C, A>::method(map, test); \
}

/* generate templates with the preprocessor! whee! */
HACK_MAP_GLOBALS(find);
HACK_MAP_GLOBALS(lower_bound);
HACK_MAP_GLOBALS(upper_bound);

#undef HACK_MAP_GLOBALS

#endif /* __HACK_MAP_H */
