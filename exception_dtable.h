/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __EXCEPTION_DTABLE_H
#define __EXCEPTION_DTABLE_H

#ifndef __cplusplus
#error exception_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "dtable.h"
#include "dtable_factory.h"

/* exception tables */

class exception_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	int init(int dfd, const char * file, const params & config);
	void deinit();
	inline exception_dtable() : base(NULL), alternatives(NULL) {}
	inline virtual ~exception_dtable()
	{
		if(base || alternatives)
			deinit();
	}
	
	static int create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow = NULL);
	DECLARE_RO_FACTORY(exception_dtable);
	
private:
	class iter : public iter_source<exception_dtable>
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool first();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual bool seek(const dtype_test & test);
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(const exception_dtable * source);
		virtual ~iter();
		
	private:
		struct sub
		{
			dtable::iter * iter;
			bool valid;
		};
		
		sub * base_iter;
		sub * alternatives_iter;
		sub * current_iter;
		enum direction {FORWARD, BACKWARD} lastdir;
	};
	
	const dtable * base;
	const dtable * alternatives;
};

#endif /* __EXCEPTION_DTABLE_H */
