/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __KEYDIV_DTABLE_H
#define __KEYDIV_DTABLE_H

#include <time.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error keydiv_dtable.h is a C++ header file
#endif

#include <vector>

#include "dtable.h"
#include "dtable_factory.h"

/* A keydiv dtable splits the keyspace among several underlying dtables. This
 * allows them to be maintained separately, although currently the maintain()
 * method for keydiv dtable just calls maintain() on all of them together. */

#define KDDTABLE_MAGIC 0x11720081
#define KDDTABLE_VERSION 1

class keydiv_dtable : public dtable
{
public:
	virtual iter * iterator() const;
	virtual bool present(const dtype & key, bool * found) const;
	virtual blob lookup(const dtype & key, bool * found) const;
	
	inline virtual bool writable() const { return sub[0]->writable(); }
	
	virtual int insert(const dtype & key, const blob & blob, bool append = false);
	virtual int remove(const dtype & key);
	
	/* do maintenance based on parameters */
	virtual int maintain(bool force = false);
	
	virtual int set_blob_cmp(const blob_comparator * cmp);
	
	static int create(int dfd, const char * name, const params & config, dtype::ctype key_type);
	DECLARE_RW_FACTORY(keydiv_dtable);
	
	inline keydiv_dtable() {}
	int init(int dfd, const char * name, const params & config);
	void deinit();
	
protected:
	inline virtual ~keydiv_dtable()
	{
		if(sub.size())
			deinit();
	}
	
private:
	struct kddtable_header
	{
		uint32_t magic;
		uint16_t version;
		uint8_t key_type;
		uint8_t dt_count;
	} __attribute__((packed));
	
	class iter : public iter_source<keydiv_dtable>
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
		inline iter(const keydiv_dtable * source);
		virtual ~iter();
		
	private:
		struct sub
		{
			dtable::iter * iter;
			bool at_first, at_end;
			inline sub() : iter(NULL) {}
			inline ~sub() { if(iter) delete iter; }
		};
		
		sub * subs;
		size_t current_index;
	};
	
	typedef std::vector<dtable *> dtable_list;
	typedef std::vector<dtype> divider_list;
	
	template<class T, class C>
	static int load_dividers(const params & config, size_t dt_count, divider_list * list, bool skip_check = false);
	
	/* return index into sub array */
	inline size_t key_index(const dtype & key) const
	{
		return key_index(dtype_static_test(key, blob_cmp));
	}
	template<class T>
	size_t key_index(const T & test) const;
	
	kddtable_header header;
	
	dtable_list sub;
	divider_list dividers;
};

#endif /* __KEYDIV_DTABLE_H */
