/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TEMPFILE_H
#define __TEMPFILE_H

#include <stdio.h>
#include <stdint.h>

#ifndef __cplusplus
#error tempfile.h is a C++ header file
#endif

class tempfile
{
public:
	int create();
	int close();
	
	int append(uint32_t value);
	
	int rewind();
	int read(uint32_t * value);
	
	size_t count();
	
	inline tempfile();
	inline ~tempfile();
	
	static inline uint8_t byte_size(uint32_t value);
	
private:
	FILE * file;
	uint8_t max_size;
	size_t read_idx, write_idx, size_idx[4];
	enum { READ, WRITE } mode;
	fpos_t read_pos;
};

inline tempfile::tempfile()
	: file(NULL)
{
}

inline tempfile::~tempfile()
{
	if(file)
		close();
}

inline uint8_t tempfile::byte_size(uint32_t value)
{
	if(value < 0x100)
		return 1;
	if(value < 0x10000)
		return 2;
	if(value < 0x1000000)
		return 3;
	return 4;
}

#endif /* __TEMPFILE_H */
