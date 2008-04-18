/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __DTABLE_H
#define __DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtype.h"

/* iterating through stuff seems like it is useful */

template<class T, class U = T> class sane_iter
{
public:
	virtual bool valid() const = 0;
	/* operator++ can suck it; we're using next() */
	virtual bool next() = 0;
	virtual T key() const = 0;
	virtual U value() const = 0;
	virtual ~sane_iter() {}
};

template<class T, class U, class V> class sane_iter3 : public sane_iter<T, U>
{
public:
	virtual V extra() const = 0;
	virtual ~sane_iter3() {}
};

/* data tables */

class dtable
{
public:
	static inline uint8_t byte_size(uint32_t value)
	{
		if(value < 0x100)
			return 1;
		if(value < 0x10000)
			return 2;
		if(value < 0x1000000)
			return 3;
		return 4;
	}
	
	static inline void layout_bytes(uint8_t * array, int * index, uint32_t value, int size)
	{
		uint8_t i = *index;
		*index += size;
		/* write big endian order */
		while(size-- > 0)
		{
			array[i + size] = value & 0xFF;
			value >>= 8;
		}
	}
	
	virtual sane_iter3<dtype, blob, const dtable *> * iterator() const = 0;
	virtual blob lookup(dtype key, const dtable ** source) const = 0;
	inline blob find(dtype key) const { const dtable * source; return lookup(key, &source); }
	inline dtype::ctype key_type() const { return ktype; }
	inline virtual ~dtable() {}
protected:
	dtype::ctype ktype;
};

#endif /* __DTABLE_H */
