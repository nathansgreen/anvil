/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_DTABLE_H
#define __JOURNAL_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "blob.h"
#include "stringset.h"
#include "sys_journal.h"
#include "dtable.h"

#ifndef __cplusplus
#error journal_dtable.h is a C++ header file
#endif

/* The journal dtable doesn't have an associated file: all its data is stored in
 * a sys_journal. The only identifying part of journal dtables is their listener
 * ID, which must be chosen to be unique for each new journal dtable. */

class journal_dtable : public dtable, public sys_journal::journal_listener
{
	virtual sane_iter<dtype, blob> * iterator() const;
	virtual blob lookup(dtype key, bool * found) const;
	
	int append(dtype key, const blob & blob);
	int remove(dtype key);
	
	inline journal_dtable() : root(NULL), string_index(0), listener_id(sys_journal::NO_ID) {}
	int init(sys_journal::listener_id id);
	void deinit();
	inline virtual ~journal_dtable()
	{
		if(listener_id != sys_journal::NO_ID)
			deinit();
	}
	
	virtual int journal_replay(void *& entry, size_t length);
	virtual sys_journal::listener_id id();
	
private:
	struct node
	{
		const dtype key;
		blob value;
		node * up;
		node * left;
		node * right;
		inline node(const dtype key) : key(key) {}
	};
	
	node * find_node(dtype key) const;
	/* will reuse an existing node if possible */
	int add_node(dtype key, const blob & value);
	static void next_node(node ** n);
	static void kill_nodes(node * n);
	
	class iter : public sane_iter<dtype, blob>
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual dtype key() const;
		virtual blob value() const;
		inline iter(node * start) : jdt_next(start) {}
		virtual ~iter() {}
	private:
		node * jdt_next;
	};
	
	int add_string(const char * string, uint32_t * index);
	template<class T> inline int log(T * entry, const blob & blob);
	int log(dtype key, const blob & blob);
	
	node * root;
	stringset strings;
	uint32_t string_index;
	sys_journal::listener_id listener_id;
};

#endif /* __JOURNAL_DTABLE_H */
