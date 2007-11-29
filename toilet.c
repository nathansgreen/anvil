#include <stdlib.h>

#include "toilet.h"

/* This function returns a toilet pointer. That is, a sign like this:
 * 
 * +------------------+
 * |                  |
 * |  Restrooms  -->  |
 * |                  |
 * +------------------+
 * 
 * The pointer part is the arrow, obviously. The rest of the sign constitutes
 * the type, and may actually indicate a subclass such as "men's room."
 */

toilet * toilet_open(const char * path)
{
}

int toilet_close(toilet * toilet)
{
}

/* gtables */

t_gtable * toilet_new_gtable(toilet * toilet, const char * name)
{
}

int toilet_drop_gtable(t_gtable * gtable)
{
}

t_gtable * toilet_get_gtable(toilet * toilet, const char * name)
{
}

int toilet_put_gtable(t_gtable * gtable)
{
}

/* rows */

t_row * toilet_new_row(t_gtable * gtable)
{
}

int toilet_drop_row(t_row * row)
{
}

t_row * toilet_get_row(toilet * toilet, t_row_id id)
{
}

int toilet_put_row(t_row * row)
{
}

/* values */

t_values * toilet_row_value(t_row * row, const char * key)
{
}

int toilet_row_remove_values(t_row * row, const char * key)
{
}

int toilet_row_append_value(t_row * row, const char * key, t_type type, t_value * value)
{
}

int toilet_row_replace_values(t_row * row, const char * key, t_type type, t_value * value)
{
}


int toilet_value_remove(t_values * values, int index)
{
}

int toilet_value_append(t_values * values, t_type type, t_value * value)
{
}

int toilet_value_update(t_values * values, int index, t_value * value)
{
}

/* queries */

t_rowset * toilet_query(t_gtable * gtable, t_query * query)
{
}

int toilet_put_rowset(t_rowset * rowset)
{
}

