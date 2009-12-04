/* AVL tree utilities implementation -*- C++ -*-

   Copyright (C) 2007 Daniel K. O. <danielosmari@users.sourceforge.net>
           Based on GCC's libstdc++ tree.cc

   This library is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this library; if not, write to the Free Software
   Software Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301,

   As a special exception, you may use this file as part of a free software
   library without restriction.  Specifically, if other files instantiate
   templates or use macros or inline functions from this file, or you compile
   this file and link it with other files to produce an executable, this
   file does not by itself cause the resulting executable to be covered by
   the GNU General Public License.  This exception does not however
   invalidate any other reasons why the executable file might be covered by
   the GNU General Public License.
*/

/*
 *
 * Copyright (c) 1996,1997
 * Silicon Graphics Computer Systems, Inc.
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.  Silicon Graphics makes no
 * representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 *
 *
 * Copyright (c) 1994
 * Hewlett-Packard Company
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.  Hewlett-Packard Company makes no
 * representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 *
 *
 */

#include <cassert>
#include <algorithm>
#include "avl/avl_tree.h"

namespace avl
{
  _avl_tree_node_base*
  _avl_tree_increment(_avl_tree_node_base* __x)
  {
    if (__x->_M_right != 0) 
      {
        __x = __x->_M_right;
        while (__x->_M_left != 0)
          __x = __x->_M_left;
      }
    else 
      {
        _avl_tree_node_base* __y = __x->_M_parent;
        while (__x == __y->_M_right) 
          {
            __x = __y;
            __y = __y->_M_parent;
          }
        if (__x->_M_right != __y)
          __x = __y;
      }
    return __x;
}


  const _avl_tree_node_base*
  _avl_tree_increment(const _avl_tree_node_base* __x)
  {
    return _avl_tree_increment(const_cast<_avl_tree_node_base*>(__x));
  }

  _avl_tree_node_base*
  _avl_tree_decrement(_avl_tree_node_base* __x)
  {
        if (__x->_M_parent->_M_parent == __x
                && __x->_M_bal_factor == -2 )
                __x = __x->_M_right;
        else if (__x->_M_left != 0)
        {
                _avl_tree_node_base* __y = __x->_M_left;
                while (__y->_M_right != 0)
                        __y = __y->_M_right;
                        __x = __y;
        }
        else
        {
                _avl_tree_node_base* __y = __x->_M_parent;
                while (__x == __y->_M_left)
                {
                        __x = __y;
                        __y = __y->_M_parent;
                }
                __x = __y;
        }
        return __x;
  }

const _avl_tree_node_base*
_avl_tree_decrement(const _avl_tree_node_base* __x)
{
        return _avl_tree_decrement(const_cast<_avl_tree_node_base*>(__x));
}





void
_avl_tree_rotate_left(_avl_tree_node_base* const __x,
                        _avl_tree_node_base*& __root)
{
        /*
                |                          |
                x(2)                       y(0)
               / \          ==>           / \
            n[a]  y(1)n+2          n+1(0)x  [c]n+1
                 / \                    / \
              n[b] [c]n+1            n[a] [b]n
        */
        _avl_tree_node_base * const __y = __x->_M_right;

        // switch
        __x->_M_right = __y->_M_left;
        __y->_M_left = __x;

        // rearrange parents
        __y->_M_parent = __x->_M_parent;
        __x->_M_parent = __y;
        // do we have [b]?
        if (__x->_M_right)
                __x->_M_right->_M_parent = __x;

        if (__x == __root)
                __root = __y;
        else
                // need to reparent y
                if (__y->_M_parent->_M_left == __x)
                        __y->_M_parent->_M_left = __y;
                else
                        __y->_M_parent->_M_right = __y;

        // reset the balancing factor
        if (__y->_M_bal_factor == 1) {
                __x->_M_bal_factor = 0;
                __y->_M_bal_factor = 0;

        } else {        // this doesn't happen during insertions
                __x->_M_bal_factor = 1;
                __y->_M_bal_factor = -1;
        }
}


  void
  _avl_tree_rotate_right(_avl_tree_node_base* const __x,
                        _avl_tree_node_base*& __root)
  {
        _avl_tree_node_base* const __y = __x->_M_left;

        __x->_M_left = __y->_M_right;
        __y->_M_right = __x;

        // rearrange parents
        __y->_M_parent = __x->_M_parent;
        __x->_M_parent = __y;

        if (__x->_M_left)
                __x->_M_left->_M_parent = __x;

        if (__x == __root)
                __root = __y;
        else
                // need to reparent y
                if (__y->_M_parent->_M_left == __x)
                        __y->_M_parent->_M_left = __y;
                else
                        __y->_M_parent->_M_right = __y;

        if (__y->_M_bal_factor == -1) {
                __x->_M_bal_factor = 0;
                __y->_M_bal_factor = 0;

        } else {        // this doesn't happen during insertions
                __x->_M_bal_factor = -1;
                __y->_M_bal_factor = 1;
        }
  }




  void
  _avl_tree_rotate_left_right(_avl_tree_node_base *a,
                        _avl_tree_node_base *&root)
  {
        /*
                |                               |
                a(-2)                           c
               / \                             / \
              /   \        ==>                /   \
          (1)b    [g]                        b     a
            / \                             / \   / \
          [d]  c                          [d] e  f  [g]
              / \
             e   f
        */

        _avl_tree_node_base     *b = a->_M_left,
                                *c = b->_M_right;

        // switch
        a->_M_left = c->_M_right;
        b->_M_right = c->_M_left;

        c->_M_right = a;
        c->_M_left = b;

        // set the parents
        c->_M_parent = a->_M_parent;
        a->_M_parent = b->_M_parent = c;

        if (a->_M_left)   // do we have f?
                a->_M_left->_M_parent = a;
        if (b->_M_right)    // do we have e?
                b->_M_right->_M_parent = b;

        if (a==root)
                root = c;
        else    // a had a parent, his child is now c
                if (a == c->_M_parent->_M_left)
                        c->_M_parent->_M_left = c;
                else
                        c->_M_parent->_M_right = c;

        // balancing...
        switch(c->_M_bal_factor) {
                case -1:
                        a->_M_bal_factor = 1;
                        b->_M_bal_factor = 0;
                        break;
                case 0:
                        a->_M_bal_factor = 0;
                        b->_M_bal_factor = 0;
                        break;
                case 1:
                        a->_M_bal_factor = 0;
                        b->_M_bal_factor = -1;
                        break;
                default:        assert(false);  // never reached
        }
        c->_M_bal_factor = 0;
  }


  void
  _avl_tree_rotate_right_left(_avl_tree_node_base *a,
                        _avl_tree_node_base *&root)
  {
        /*
                |                               |
                a(1)                            c
               / \                             / \
              /   \                           /   \
            [d]   b(-1)          ==>         a     b
                 / \                        / \   / \
                c  [g]                    [d] e  f  [g]
               / \
              e  f
        */

        _avl_tree_node_base     *b = a->_M_right,
                                *c = b->_M_left;

        // switch
        a->_M_right = c->_M_left;
        b->_M_left = c->_M_right;

        c->_M_left = a;
        c->_M_right = b;

        // fix the parents
        c->_M_parent = a->_M_parent;
        a->_M_parent = b->_M_parent = c;

        if (a->_M_right)   // have e?
                a->_M_right->_M_parent = a;
        if (b->_M_left)    // have f?
                b->_M_left->_M_parent = b;

        if (a==root)
                root = c;
        else    // a had a parent, his child is now c
                if (a == c->_M_parent->_M_left)
                        c->_M_parent->_M_left = c;
                else
                        c->_M_parent->_M_right = c;

        // balancing
        switch(c->_M_bal_factor) {
                case -1:
                        a->_M_bal_factor = 0;
                        b->_M_bal_factor = 1;
                        break;
                case 0:
                        a->_M_bal_factor = 0;
                        b->_M_bal_factor = 0;
                        break;
                case 1:
                        a->_M_bal_factor = -1;
                        b->_M_bal_factor = 0;
                        break;

                default:        assert(false);  // never reached
        }
        c->_M_bal_factor = 0;
  }



  void
  _avl_tree_insert_and_rebalance(const bool          __insert_left,
                                _avl_tree_node_base* __x,
                                _avl_tree_node_base* __p,
                                _avl_tree_node_base& __header)
  {

        _avl_tree_node_base *& __root = __header._M_parent;

        // Initialize fields in new node to insert.
        __x->_M_parent = __p;
        __x->_M_left = 0;
        __x->_M_right = 0;
        __x->_M_bal_factor = 0;

        // Insert.
        // Make new node child of parent and maintain root, leftmost and
        // rightmost nodes.
        // N.B. First node is always inserted left.
        if (__insert_left)
        {
                __p->_M_left = __x; // also makes leftmost = __x when __p == &__header

                if (__p == &__header)
                {
                        __header._M_parent = __x;
                        __header._M_right = __x;
                }
                else if (__p == __header._M_left)
                        __header._M_left = __x; // maintain leftmost pointing to min node
        }
        else
        {
                __p->_M_right = __x;

                if (__p == __header._M_right)
                        __header._M_right = __x; // maintain rightmost pointing to max node
        }

        // Rebalance.
        while (__x != __root) {
                switch (__x->_M_parent->_M_bal_factor) {
                        case 0:
                                // if x is left, parent will have parent->bal_factor = -1
                                // else, parent->bal_factor = 1
                                __x->_M_parent->_M_bal_factor =
                                        (__x == __x->_M_parent->_M_left) ? -1 : 1;
                                __x = __x->_M_parent;
                                break;
                        case 1:
                                // if x is a left child, parent->bal_factor = 0
                                if (__x == __x->_M_parent->_M_left)
                                        __x->_M_parent->_M_bal_factor = 0;
                                else {        // x is a right child, needs rebalancing
                                        if (__x->_M_bal_factor == -1)
                                                _avl_tree_rotate_right_left(__x->_M_parent, __root);
                                        else
                                                _avl_tree_rotate_left(__x->_M_parent, __root);
                                }
                                return;
                        case -1:
                                // if x is a left child, needs rebalancing
                                if (__x == __x->_M_parent->_M_left) {
                                        if (__x->_M_bal_factor == 1)
                                                _avl_tree_rotate_left_right(__x->_M_parent, __root);
                                        else
                                                _avl_tree_rotate_right(__x->_M_parent, __root);
                                } else
                                        __x->_M_parent->_M_bal_factor = 0;
                                return;

                        default:        assert(false);  // never reached
                }
        }
  }


  _avl_tree_node_base*
  _avl_tree_rebalance_for_erase(_avl_tree_node_base* const z,
			       _avl_tree_node_base& __header)
  {
        _avl_tree_node_base *& root = __header._M_parent;
        _avl_tree_node_base *& __leftmost = __header._M_left;
        _avl_tree_node_base *& __rightmost = __header._M_right;
        _avl_tree_node_base* y = z;
        _avl_tree_node_base* x = 0;
        _avl_tree_node_base* x_parent = 0;

        if (y->_M_left == 0)     // z has at most one non-null child. y == z.
                x = y->_M_right;     // x might be null.
        else
                if (y->_M_right == 0)  // z has exactly one non-null child. y == z.
                        x = y->_M_left;    // x is not null.
                else
                {
                        // z has two non-null children.  Set y to
                        y = y->_M_right;   //   z's successor.  x might be null.
                        while (y->_M_left)
                                y = y->_M_left;
                        x = y->_M_right;
                }
	
        if (y != z)
        {
                // relink y in place of z.  y is z's successor
                z->_M_left->_M_parent = y;
                y->_M_left = z->_M_left;
                if (y != z->_M_right)
                {
                        x_parent = y->_M_parent;
                        if (x) x->_M_parent = y->_M_parent;
                        y->_M_parent->_M_left = x;   // y must be a child of _M_left
                        y->_M_right = z->_M_right;
                        z->_M_right->_M_parent = y;
                }
        	else
                        x_parent = y;
                if (root == z)      // if we are deleting the root
                        root = y;   // the new root is y
                else if (z->_M_parent->_M_left == z)        // else, fix parent's child
                        z->_M_parent->_M_left = y;
                else
                        z->_M_parent->_M_right = y;
                y->_M_parent = z->_M_parent;

                y->_M_bal_factor = z->_M_bal_factor;

                y = z;
        	// y now points to node to be actually deleted
        }
        else
        {                        // y == z    --> z has only one child, or none
                x_parent = y->_M_parent;

                if (x)        // if z has at least one child
                  x->_M_parent = y->_M_parent;      // new parent is now y

                if (root == z)      // if we deleted the root
                        root = x;   // new root is x
                else    // else, fix the parent's child
                        if (z->_M_parent->_M_left == z)
                                z->_M_parent->_M_left = x;
                        else
                                z->_M_parent->_M_right = x;

                if (__leftmost == z)  // need to fix the header?
                {
                        if (z->_M_right == 0)           // z->_M_left must be null also
                                                        // because z is leftmost. if z had _M_left,
                                                        // z wouldn't be leftmost.
                                __leftmost = z->_M_parent;
                                // makes __leftmost == _M_header if z == root
                        else
                                __leftmost = _avl_tree_node_base::_S_minimum(x);
                }

                if (__rightmost == z) // if z is rightmost, z don't have _M_right
                {
                        if (z->_M_left == 0)         // z->_M_right must be null also
                                __rightmost = z->_M_parent;
                        // makes __rightmost == _M_header if z == root
                        else                      // x == z->_M_left
                                __rightmost = _avl_tree_node_base::_S_maximum(x);
                }
        }

        // Rebalancing
        // x: may be null

        while (x != root) {
                switch (x_parent->_M_bal_factor) {
                        case 0:
                                x_parent->_M_bal_factor = (x == x_parent->_M_right)?-1:1;
                                return y;       // the height didn't change, let's stop here
                        case -1:
                                if (x == x_parent->_M_left) {
                                        x_parent->_M_bal_factor = 0; // balanced
                                        
                                        x = x_parent;
                                        x_parent = x_parent->_M_parent;
                                } else {
                                        // x is right child
                                        // a is left child
                                        _avl_tree_node_base *a = x_parent->_M_left;
                                        assert(a);
                                        if (a->_M_bal_factor == 1) {
                                                // a MUST have a right child
                                                assert(a->_M_right);
                                                _avl_tree_rotate_left_right(x_parent, root);
                                                
                                                x = x_parent->_M_parent;
                                                x_parent = x_parent->_M_parent->_M_parent;
                                        }
                                        else {
                                                _avl_tree_rotate_right(x_parent, root);
                                                
                                                x = x_parent->_M_parent;
                                                x_parent = x_parent->_M_parent->_M_parent;
                                        }

                                        // if changed from -1 to 1, no need to check above
                                        if (x->_M_bal_factor == 1)
                                                return y;
                                }
                                break;
                        case 1:
                                if (x == x_parent->_M_right) {
                                        x_parent->_M_bal_factor = 0; // balanced
                                        
                                        x = x_parent;
                                        x_parent = x_parent->_M_parent;
                                } else {
                                        // x is left child
                                        // a is right child
                                        _avl_tree_node_base *a = x_parent->_M_right;
                                        assert(a);
                                        if (a->_M_bal_factor == -1) {
                                                // a MUST have then a left child
                                                assert(a->_M_left);
                                                _avl_tree_rotate_right_left(x_parent, root);

                                                x = x_parent->_M_parent;
                                                x_parent = x_parent->_M_parent->_M_parent;
                                        }
                                        else {
                                                _avl_tree_rotate_left(x_parent, root);

                                                x = x_parent->_M_parent;
                                                x_parent = x_parent->_M_parent->_M_parent;
                                        }
                                        // if changed from 1 to -1, no need to check above
                                        if (x->_M_bal_factor == -1)
                                                return y;
                                }
                                break;
                        default:        assert(false);  // never reached
                }
        }

        return y;
  }



namespace {

int height(const _avl_tree_node_base* root)
{
	int result = 0;
	if (root) {
		int hleft = height(root->_M_left);
		if (hleft < 0)
			return hleft;
		int hright = height(root->_M_right);
		if (hright < 0)
			return hright;

		result = std::max(hleft, hright) + 1;
		int balance = hright - hleft;
		if (balance < -1 || balance > 1)
			return -1;	// Magic value: violates AVL balancing
		if (balance != root->_M_bal_factor)
			return -2;	// Magic value: inconsistent balancing factor
	}
	return result;
}

}


  // check if all nodes are correctly balanced
  bool
  _avl_tree_check_balance(const _avl_tree_node_base* __root)
  {
	return height(__root) >= 0;
  }

} // namespace avl
