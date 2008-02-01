#ifndef PHP_TOILET_H
#define PHP_TOILET_H 1

#include <toilet.h>

#define PHP_TOILET_VERSION "0.1"
#define PHP_TOILET_EXTNAME "toilet"

typedef struct php_toilet {
	t_toilet * toilet;
} php_toilet;
#define PHP_TOILET_RES_NAME "toilet database"

PHP_MINIT_FUNCTION(toilet);
PHP_MSHUTDOWN_FUNCTION(toilet);

PHP_FUNCTION(toilet_open);
PHP_FUNCTION(toilet_close);

extern zend_module_entry toilet_module_entry;
#define phpext_toilet_ptr &toilet_module_entry

#endif
