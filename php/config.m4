dnl PHP_ARG_ENABLE(toilet, whether to enable toilet support,
dnl [  --enable-toilet         Enable toilet support])

PHP_ARG_WITH(toilet, for toilet support,
[  --with-toilet[=DIR]     Include toilet support])

if test "$PHP_TOILET" != "no"; then
	AC_DEFINE(HAVE_TOILET, 1, [Whether you have toilet])
	if test -z "$PHP_TOILET"; then
		PHP_TOILET=/usr/local/toilet
	fi
	PHP_ADD_LIBRARY_WITH_PATH(toilet, $PHP_TOILET, TOILET_SHARED_LIBADD)
	PHP_ADD_INCLUDE($PHP_TOILET)
	PHP_NEW_EXTENSION(toilet, php_toilet.c, $ext_shared)
	PHP_SUBST(TOILET_SHARED_LIBADD)
fi
