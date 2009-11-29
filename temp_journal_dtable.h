/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TEMP_JOURNAL_DTABLE_H
#define __TEMP_JOURNAL_DTABLE_H

#ifndef __cplusplus
#error temp_journal_dtable.h is a C++ header file
#endif

#include "journal_dtable.h"

/* A normal journal dtable keeps its keys in sorted order, which is necessary to
 * efficiently implement iterators (which even if not used by the application
 * will be used to digest it). However, abortable transactions create temporary
 * journal dtables that don't get digested: they are rolled over into the normal
 * journal dtable when the transaction commits. The rollover operation does not
 * need to traverse the source data in sorted order. If the application does not
 * itself create an iterator, the work of maintaining a sorted tree is wasted.
 * This variant of the journal dtable defers the work of creating the sorted
 * tree until its iterator() method is called; until then it uses a hash table.
 * If the iterator() method is never called, then the extra work is avoided. */

class temp_journal_dtable : public journal_dtable
{
public:
	virtual dtable::iter * iterator(ATX_OPT) const;
	
	virtual int insert(const dtype & key, const blob & blob, bool append = false, ATX_OPT);
	
	/* for rollover */
	virtual int accept(const dtype & key, const blob & value, bool append = false);
	
	class temp_journal_dtable_warehouse : public sys_journal::listening_dtable_warehouse_impl<temp_journal_dtable>
	{
	protected:
		virtual temp_journal_dtable * create(sys_journal::listener_id lid, const void * entry, size_t length, sys_journal * journal) const;
		virtual temp_journal_dtable * create(sys_journal::listener_id lid, dtype::ctype key_type, sys_journal * journal) const;
	};
	
	static temp_journal_dtable_warehouse warehouse;
	
	/* clear memory state, discard the current listener ID, set a new listener
	 * ID, and clear and release the blob comparator (if one has been set) */
	virtual int reinit(sys_journal::listener_id lid);
	
protected:
	/* temp_journal_dtables should only be constructed by a temp_journal_dtable_warehouse */
	inline temp_journal_dtable() {}
	int init(dtype::ctype key_type, sys_journal::listener_id lid, sys_journal * sysj);
	inline virtual ~temp_journal_dtable() {}
	
private:
	int degrade();
	
	bool temporary;
};

#endif /* __TEMP_JOURNAL_DTABLE_H */
