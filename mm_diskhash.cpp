#include "multimap.h"
#include "mm_diskhash.h"

/* iterators */

int diskhash_it::next()
{
}

size_t diskhash_it::size()
{
}

diskhash_it::~diskhash_it()
{
}

/* disk hashes */

diskhash::diskhash(const char * store)
{
}

diskhash::~diskhash()
{
}

size_t diskhash::keys()
{
}

size_t diskhash::values()
{
}

size_t diskhash::count_values(mm_val_t * key)
{
}

diskhash_it * diskhash::get_values(mm_val_t * key)
{
}

diskhash_it * diskhash::get_range(mm_val_t * low_key, mm_val_t * high_key)
{
}

diskhash_it * diskhash::iterator()
{
}

int diskhash::remove_key(mm_val_t * key)
{
}

int diskhash::reset_key(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::append_value(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::remove_value(mm_val_t * key, mm_val_t * value)
{
}

int diskhash::update_value(mm_val_t * key, mm_val_t * old_value, mm_val_t * new_value)
{
}

/* create a new diskhash (on disk) using the specified store path */
int diskhash::init(const char * store, mm_type_t key_type, mm_type_t val_type)
{
}
