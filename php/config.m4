dnl  This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
dnl  of the University of California. It is distributed under the terms of
dnl  version 2 of the GNU GPL. See the file LICENSE for details.

PHP_ARG_WITH(toilet, for toilet support,
[  --with-toilet@<:@=DIR@:>@     Include toilet support from DIR @<:@default=..@:>@], ..)

if test "$PHP_TOILET" != "no"; then
	AC_DEFINE(HAVE_TOILET, 1, Whether you have toilet)
	PHP_ADD_LIBRARY_WITH_PATH(toilet, $PHP_TOILET, TOILET_SHARED_LIBADD)
	PHP_ADD_INCLUDE($PHP_TOILET)
	PHP_NEW_EXTENSION(toilet, php_toilet.c, $ext_shared)
	PHP_SUBST(TOILET_SHARED_LIBADD)
fi
