/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SIMPLE_DTABLE_H
#define __SIMPLE_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "blob.h"
#include "dtable.h"

#ifndef __cplusplus
#error simple_dtable.h is a C++ header file
#endif

/* The simple dtable does nothing fancy to store the blobs efficiently. It just
 * stores the key and the blob literally, including size information. These
 * dtables are read-only once they are created with the ::create() method. */

/* Custom versions of this class are definitely expected, to store the data more
 * efficiently given knowledge of what it will probably be. It is considered bad
 * form to have such a custom class outright reject data that it does not know
 * how to store conveniently, however. */

class simple_dtable : public dtable
{
	virtual sane_iter3<dtype, blob, const dtable *> * iterator() const;
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
	class iter : public sane_iter3<dtype, blob, const dtable *>
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual blob value() const;
		virtual const dtable * extra() const;
		inline iter(const simple_dtable * source);
		virtual ~iter() { }
		
	private:
		const simple_dtable * source;
	};
	
	int fd;
};

#endif /* __SIMPLE_DTABLE_H */
