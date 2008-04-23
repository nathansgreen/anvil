/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_DTABLE_H
#define __SIMPLE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "stable.h"

#ifndef __cplusplus
#error simple_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"

/* The simple dtable does nothing fancy to store the blobs efficiently. It just
 * stores the key and the blob literally, including size information. These
 * dtables are read-only once they are created with the ::create() method. */

/* Custom versions of this class are definitely expected, to store the data more
 * efficiently given knowledge of what it will probably be. It is considered bad
 * form to have such a custom class outright reject data that it does not know
 * how to store conveniently, however. */

#define SDTABLE_MAGIC 0xF029DDE3
#define SDTABLE_VERSION 0

class simple_dtable : public dtable
{
public:
	virtual dtable_iter * iterator() const;
	virtual blob lookup(dtype key, const dtable ** source) const;
	
	inline simple_dtable() : fd(-1) {}
	int init(int dfd, const char * file);
	void deinit();
	inline virtual ~simple_dtable()
	{
		if(fd >= 0)
			deinit();
	}
	
	/* negative entries in the source which are present in the shadow (as
	 * positive entries) will be kept as negative entries in the result,
	 * otherwise they will be omitted since they are not needed */
	static int create(int dfd, const char * file, const dtable * source, const dtable * shadow = NULL);
	
private:
	struct dtable_header {
		uint32_t magic;
		uint32_t version;
		uint32_t key_count;
		uint8_t key_type;
		uint8_t key_size;
		uint8_t length_size;
		uint8_t offset_size;
	} __attribute__((packed));
	
	class iter : public dtable_iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const simple_dtable * source);
		virtual ~iter() { }
		
	private:
		size_t index;
		const simple_dtable * sdt_source;
	};
	
	dtype get_key(size_t index, size_t * data_length = NULL, off_t * data_offset = NULL) const;
	int find_key(dtype key, size_t * data_length, off_t * data_offset, size_t * index) const;
	blob get_value(size_t index, size_t data_length, off_t data_offset) const;
	blob get_value(size_t index) const;
	
	int fd;
	size_t key_count;
	struct stable st;
	uint8_t key_size, length_size, offset_size;
	off_t key_start_off, data_start_off;
};

#endif /* __SIMPLE_DTABLE_H */
