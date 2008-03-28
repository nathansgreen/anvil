/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "transaction.h"
#include "datastore.h"

off_t datastore::append_uint8(uint8_t i)
{
	off_t off = offset;
	if(tx_write(fd, &i, offset, sizeof(i)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(i);
	return 0;
}

off_t datastore::append_uint16(uint16_t i)
{
	off_t off = offset;
	if(tx_write(fd, &i, offset, sizeof(i)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(i);
	return 0;
}

off_t datastore::append_uint32(uint32_t i)
{
	off_t off = offset;
	if(tx_write(fd, &i, offset, sizeof(i)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(i);
	return 0;
}

off_t datastore::append_uint64(uint64_t i)
{
	off_t off = offset;
	if(tx_write(fd, &i, offset, sizeof(i)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(i);
	return 0;
}

off_t datastore::append_float(float f)
{
	off_t off = offset;
	if(tx_write(fd, &f, offset, sizeof(f)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(f);
	return 0;
}

off_t datastore::append_double(double d)
{
	off_t off = offset;
	if(tx_write(fd, &d, offset, sizeof(d)) < 0)
		return INVAL_OFF_T;
	offset += sizeof(d);
	return 0;
}

off_t datastore::append_string255(const char * string)
{
	uint8_t length;
	off_t off = offset;
	size_t full_length = strlen(string);
	if(full_length > 0xFF)
		return INVAL_OFF_T;
	length = full_length;
	if(tx_write(fd, &length, offset, sizeof(length)) < 0)
		return INVAL_OFF_T;
	if(tx_write(fd, string, offset + sizeof(length), full_length) < 0)
		return INVAL_OFF_T;
	offset += sizeof(length) + full_length;
	return off;
}

off_t datastore::append_string65k(const char * string)
{
	uint16_t length;
	off_t off = offset;
	size_t full_length = strlen(string);
	if(full_length > 0xFFFF)
		return INVAL_OFF_T;
	length = full_length;
	if(tx_write(fd, &length, offset, sizeof(length)) < 0)
		return INVAL_OFF_T;
	if(tx_write(fd, string, offset + sizeof(length), full_length) < 0)
		return INVAL_OFF_T;
	offset += sizeof(length) + full_length;
	return off;
}

off_t datastore::append_stringX(const char * string)
{
	off_t off = offset;
	size_t length = strlen(string);
	if(tx_write(fd, string, offset, length) < 0)
		return INVAL_OFF_T;
	offset += length;
	return off;
}

off_t datastore::append_blob255(const void * blob, uint8_t length)
{
	off_t off = offset;
	if(tx_write(fd, &length, offset, sizeof(length)) < 0)
		return INVAL_OFF_T;
	if(tx_write(fd, blob, offset + sizeof(length), length) < 0)
		return INVAL_OFF_T;
	offset += sizeof(length) + length;
	return off;
}

off_t datastore::append_blob65k(const void * blob, uint16_t length)
{
	off_t off = offset;
	if(tx_write(fd, &length, offset, sizeof(length)) < 0)
		return INVAL_OFF_T;
	if(tx_write(fd, blob, offset + sizeof(length), length) < 0)
		return INVAL_OFF_T;
	offset += sizeof(length) + length;
	return off;
}

off_t datastore::append_blob4g(const void * blob, uint32_t length)
{
	off_t off = offset;
	if(tx_write(fd, &length, offset, sizeof(length)) < 0)
		return INVAL_OFF_T;
	if(tx_write(fd, blob, offset + sizeof(length), length) < 0)
		return INVAL_OFF_T;
	offset += sizeof(length) + length;
	return off;
}

off_t datastore::append_blobX(const void * blob, size_t length)
{
	off_t off = offset;
	if(tx_write(fd, blob, offset, length) < 0)
		return INVAL_OFF_T;
	offset += length;
	return off;
}

int datastore::read_uint8(off_t off, uint8_t * i)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, i, sizeof(*i));
	if(r < 0 || r < sizeof(*i))
		return -1;
	return 0;
}

int datastore::read_uint16(off_t off, uint16_t * i)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, i, sizeof(*i));
	if(r < 0 || r < sizeof(*i))
		return -1;
	return 0;
}

int datastore::read_uint32(off_t off, uint32_t * i)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, i, sizeof(*i));
	if(r < 0 || r < sizeof(*i))
		return -1;
	return 0;
}

int datastore::read_uint64(off_t off, uint64_t * i)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, i, sizeof(*i));
	if(r < 0 || r < sizeof(*i))
		return -1;
	return 0;
}

int datastore::read_float(off_t off, float * f)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, f, sizeof(*f));
	if(r < 0 || r < sizeof(*f))
		return -1;
	return 0;
}

int datastore::read_double(off_t off, double * d)
{
	ssize_t r;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, d, sizeof(*d));
	if(r < 0 || r < sizeof(*d))
		return -1;
	return 0;
}

char * datastore::read_string255(off_t off, char * string, uint8_t length)
{
	ssize_t r;
	bool do_free = !string;
	uint8_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length + 1 > length)
			return NULL;
		string = (char *) malloc(real_length + 1);
		if(!string)
			return NULL;
	}
	r = read(ufd, string, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(string);
		return NULL;
	}
	string[real_length] = 0;
	return string;
}

char * datastore::read_string65k(off_t off, char * string, uint16_t length)
{
	ssize_t r;
	bool do_free = !string;
	uint16_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length + 1 > length)
			return NULL;
		string = (char *) malloc(real_length + 1);
		if(!string)
			return NULL;
	}
	r = read(ufd, string, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(string);
		return NULL;
	}
	string[real_length] = 0;
	return string;
}

char * datastore::read_string4g(off_t off, char * string, uint32_t length)
{
	ssize_t r;
	bool do_free = !string;
	uint32_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length + 1 > length)
			return NULL;
		string = (char *) malloc(real_length + 1);
		if(!string)
			return NULL;
	}
	r = read(ufd, string, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(string);
		return NULL;
	}
	string[real_length] = 0;
	return string;
}

char * datastore::read_stringX(off_t off, char * string, size_t length)
{
	ssize_t r;
	bool do_free = !string;
	lseek(ufd, off, SEEK_SET);
	if(do_free)
	{
		string = (char *) malloc(length);
		if(!string)
			return NULL;
	}
	r = read(ufd, string, --length);
	if(r < 0 || r < length)
	{
		if(do_free)
			free(string);
		return NULL;
	}
	string[length] = 0;
	return string;
}

void * datastore::read_blob255(off_t off, void * blob, uint8_t * length)
{
	ssize_t r;
	bool do_free = !blob;
	uint8_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length > *length)
			return NULL;
		*length = real_length;
		blob = malloc(real_length);
		if(!blob)
			return NULL;
	}
	r = read(ufd, blob, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(blob);
		return NULL;
	}
	return blob;
}

void * datastore::read_blob65k(off_t off, void * blob, uint16_t * length)
{
	ssize_t r;
	bool do_free = !blob;
	uint16_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length > *length)
			return NULL;
		*length = real_length;
		blob = malloc(real_length);
		if(!blob)
			return NULL;
	}
	r = read(ufd, blob, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(blob);
		return NULL;
	}
	return blob;
}

void * datastore::read_blob4g(off_t off, void * blob, uint32_t * length)
{
	ssize_t r;
	bool do_free = !blob;
	uint32_t real_length;
	lseek(ufd, off, SEEK_SET);
	r = read(ufd, &real_length, sizeof(real_length));
	if(r < 0 || r < sizeof(real_length))
		return NULL;
	if(do_free)
	{
		if(real_length > *length)
			return NULL;
		*length = real_length;
		blob = malloc(real_length);
		if(!blob)
			return NULL;
	}
	r = read(ufd, blob, real_length);
	if(r < 0 || r < real_length)
	{
		if(do_free)
			free(blob);
		return NULL;
	}
	return blob;
}

void * datastore::read_blobX(off_t off, void * blob, size_t length)
{
	ssize_t r;
	bool do_free = !blob;
	lseek(ufd, off, SEEK_SET);
	if(do_free)
	{
		blob = malloc(length);
		if(!blob)
			return NULL;
	}
	r = read(ufd, blob, length);
	if(r < 0 || r < length)
	{
		if(do_free)
			free(blob);
		return NULL;
	}
	return blob;
}

int datastore::init(int dfd, const char * file, bool create)
{
	int flags = O_RDWR;
	if(fd >= 0)
		deinit();
	if(create)
		flags |= O_CREAT;
	/* doesn't hurt to give 644 even without O_CREAT */
	fd = tx_open(dfd, file, flags, 0644);
	if(fd < 0)
		return fd;
	ufd = tx_read_fd(fd);
	offset = lseek(ufd, 0, SEEK_END);
	return 0;
}

void datastore::deinit()
{
	if(fd >= 0)
	{
		tx_close(fd);
		fd = -1;
		ufd = -1;
		offset = -1;
	}
}
