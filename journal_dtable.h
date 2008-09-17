/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __JOURNAL_DTABLE_H
#define __JOURNAL_DTABLE_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#ifndef __cplusplus
#error journal_dtable.h is a C++ header file
#endif

#include "blob.h"
#include "istr.h"
#include "dtable.h"
#include "stringset.h"
#include "sys_journal.h"

/* The journal dtable doesn't have an associated file: all its data is stored in
 * a sys_journal. The only identifying part of journal dtables is their listener
 * ID, which must be chosen to be unique for each new journal dtable. */

class journal_dtable : public dtable, public sys_journal::journal_listener
{
public:
	virtual iter * iterator() const;
	virtual blob lookup(const dtype & key, bool * found) const;
	
	inline virtual bool writable() const { return true; }
	virtual int append(const dtype & key, const blob & blob);
	virtual int remove(const dtype & key);
	
	inline virtual int set_blob_cmp(const blob_comparator * cmp)
	{
		/* we merely add this assertion, but it's important */
		assert(!root);
		return dtable::set_blob_cmp(cmp);
	}
	
	inline journal_dtable() : root(NULL), string_index(0) {}
	int init(dtype::ctype key_type, sys_journal::listener_id lid, sys_journal * journal = NULL);
	/* reinitialize, optionally discarding the old entries from the journal */
	/* NOTE: also clears and releases the blob comparator, if one has been set */
	int reinit(sys_journal::listener_id lid, bool discard = true);
	void deinit();
	inline virtual ~journal_dtable()
	{
		if(id() != sys_journal::NO_ID)
			deinit();
	}
	
	virtual int journal_replay(void *& entry, size_t length);
	
private:
	struct node
	{
		const dtype key;
		blob value;
		node * up;
		node * left;
		node * right;
		inline node(const dtype & key) : key(key) {}
	};
	
	node * find_node(const dtype & key) const;
	node * find_node_next(const dtype & key, bool * found) const;
	/* will reuse an existing node if possible */
	int add_node(const dtype & key, const blob & value);
	static void next_node(node ** n);
	static void prev_node(node ** n);
	static void kill_nodes(node * n);
	
	class iter : public dtable::iter
	{
	public:
		virtual bool valid() const;
		virtual bool next();
		virtual bool prev();
		virtual bool last();
		virtual dtype key() const;
		virtual bool seek(const dtype & key);
		virtual metablob meta() const;
		virtual blob value() const;
		virtual const dtable * source() const;
		inline iter(node * start, const journal_dtable * source) : jdt_node(start), jdt_source(source) {}
		virtual ~iter() {}
	private:
		node * jdt_node;
		const journal_dtable * jdt_source;
	};
	
	int add_string(const istr & string, uint32_t * index);
	int log_blob_cmp();
	template<class T> inline int log(T * entry, const blob & blob, size_t offset = 0);
	int log(const dtype & key, const blob & blob);
	
	node * root;
	stringset strings;
	uint32_t string_index;
};

#endif /* __JOURNAL_DTABLE_H */
