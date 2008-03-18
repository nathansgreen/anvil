/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>
#include <errno.h>

#include "tempfile.h"

int tempfile::create()
{
	if(file)
		close();
	max_size = 0;
	read_idx = 0;
	write_idx = 0;
	mode = READ;
	file = tmpfile();
	if(!file)
		return -1;
	return 0;
}

int tempfile::close()
{
	fclose(file);
	file = NULL;
	return 0;
}

int tempfile::append(uint32_t value)
{
	uint8_t size, array[4];
	if(max_size < 4)
	{
		size = byte_size(value);
		while(size > max_size)
			size_idx[max_size++] = write_idx;
	}
	size = max_size;
	/* write big endian order */
	while(size-- > 0)
	{
		array[size] = value & 0xFF;
		value >>= 8;
	}
	if(mode == READ)
	{
		fgetpos(file, &read_pos);
		fseek(file, 0, SEEK_END);
		mode = WRITE;
	}
	if(fwrite(array, max_size, 1, file) != 1)
		return -1;
	write_idx++;
	return 0;
}

int tempfile::rewind()
{
	::rewind(file);
	mode = READ;
	read_idx = 0;
	return 0;
}

int tempfile::read(uint32_t * value)
{
	uint8_t size = 1, array[4];
	if(mode == WRITE)
	{
		fsetpos(file, &read_pos);
		mode = READ;
	}
	if(read_idx == write_idx)
		return -ENOENT;
	if(max_size > 3 && read_idx >= size_idx[3])
		size = 4;
	else if(max_size > 2 && read_idx >= size_idx[2])
		size = 3;
	else if(max_size > 1 && read_idx >= size_idx[1])
		size = 2;
	if(fread(array, size, 1, file) != 1)
		return -1;
	read_idx++;
	/* read big endian order */
	*value = 0;
	for(uint8_t i = 0; i < size; i++)
		*value = (*value << 8) | array[i];
	return 0;
}
