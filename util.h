/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __UTIL_H
#define __UTIL_H

#include <string.h>
#include <stdint.h>

#ifndef __cplusplus
#error util.h is a C++ header file
#endif

#include "istr.h"

#define static_assert(x) do { switch(0) { case 0: case (x): ; } } while(0)

/* useful utility stuff */
class util
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
	
	template<class T>
	static inline void layout_bytes(uint8_t * array, T * index, uint32_t value, uint8_t size)
	{
		T i = *index;
		*index += size;
		/* write big endian order */
		while(size-- > 0)
		{
			array[i + size] = value & 0xFF;
			value >>= 8;
		}
	}
	
	static inline void layout_bytes(uint8_t * array, size_t index, uint32_t value, uint8_t size)
	{
		/* write big endian order */
		while(size-- > 0)
		{
			array[index + size] = value & 0xFF;
			value >>= 8;
		}
	}
	
	template<class T>
	static inline uint32_t read_bytes(const uint8_t * array, T * index, uint8_t size)
	{
		uint32_t value = 0;
		T max = size + *index;
		/* read big endian order */
		for(; *index < max; ++*index)
			value = (value << 8) | array[*index];
		return value;
	}
	
	template<class T>
	static inline uint32_t read_bytes(const uint8_t * array, T index, uint8_t size)
	{
		uint32_t value = 0;
		T max = size + index;
		/* read big endian order */
		for(; index < max; ++index)
			value = (value << 8) | array[index];
		return value;
	}
	
	/* a library call to memcpy() can be expensive, especially for small copies */
	static inline void memcpy(void * dst, const void * src, size_t size)
	{
		uint8_t * dstb = (uint8_t *) dst;
		const uint8_t * srcb = (const uint8_t *) src;
		switch(size)
		{
			case 8:
				dstb[7] = srcb[7];
			case 7:
				dstb[6] = srcb[6];
			case 6:
				dstb[5] = srcb[5];
			case 5:
				dstb[4] = srcb[4];
			case 4:
				dstb[3] = srcb[3];
			case 3:
				dstb[2] = srcb[2];
			case 2:
				dstb[1] = srcb[1];
			case 1:
				dstb[0] = srcb[0];
			case 0:
				break;
			default:
				::memcpy(dst, src, size);
		}
	}
	
	/* a library call to memset() can be expensive, especially for small buffers */
	static inline void memset(void * dst, int value, size_t size)
	{
		uint8_t * dstb = (uint8_t *) dst;
		switch(size)
		{
			case 8:
				dstb[7] = value;
			case 7:
				dstb[6] = value;
			case 6:
				dstb[5] = value;
			case 5:
				dstb[4] = value;
			case 4:
				dstb[3] = value;
			case 3:
				dstb[2] = value;
			case 2:
				dstb[1] = value;
			case 1:
				dstb[0] = value;
			case 0:
				break;
			default:
				::memset(dst, value, size);
		}
	}
	
	/* rm -r */
	static int rm_r(int dfd, const char * path);
	static istr tilde_home(const istr & path);
};

#endif /* __UTIL_H */
