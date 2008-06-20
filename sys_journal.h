/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __SYS_JOURNAL_H
#define __SYS_JOURNAL_H

#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "transaction.h"

#ifndef __cplusplus
#error journal++.h is a C++ header file
#endif

#include <map>
#include <set>

#include "istr.h"

class sys_journal
{
public:
	typedef uint32_t listener_id;
	static const listener_id NO_ID = (listener_id) -1;
	
	class journal_listener
	{
	public:
		virtual int journal_replay(void *& entry, size_t length) = 0;
		
		inline journal_listener() : local_id(NO_ID), journal(NULL) {}
		
		inline listener_id id()
		{
			return local_id;
		}
		
		inline virtual ~journal_listener()
		{
			if(local_id != NO_ID)
				unregister_listener(this);
		}
		
	protected:
		inline int set_id(listener_id lid)
		{
			if(local_id != NO_ID)
				unregister_listener(this);
			local_id = lid;
			if(lid != NO_ID)
			{
				int r = register_listener(this);
				if(r < 0)
					local_id = NO_ID;
				return r;
			}
			return 0;
		}
		
		inline void set_journal(sys_journal * journal)
		{
			this->journal = journal;
		}
		
		inline int journal_append(void * entry, size_t length)
		{
			sys_journal * j = journal ? journal : get_global_journal();
			return j->append(this, entry, length);
		}
		
		inline int journal_discard()
		{
			sys_journal * j = journal ? journal : get_global_journal();
			return j->discard(this);
		}
		
	private:
		listener_id local_id;
		sys_journal * journal;
	};
	
	int append(journal_listener * listener, void * entry, size_t length);
	/* make a note that this listener's entries are no longer needed */
	int discard(journal_listener * listener);
	/* gets only those entries that have been committed */
	int get_entries(journal_listener * listener);
	/* remove any discarded entries from this journal */
	int filter();
	
	inline sys_journal() : meta_dfd(-1), data_fd(-1), meta_fd(-1), pid(0) { handle.data = this; handle.handle = flush_tx_static; }
	int init(int dfd, const char * file, bool create = false, bool fail_missing = false);
	void deinit();
	inline ~sys_journal()
	{
		if(data_fd >= 0)
			deinit();
	}
	
	static inline sys_journal * get_global_journal()
	{
		return &global_journal;
	}
	static journal_listener * lookup_listener(listener_id id);
	static int register_listener(journal_listener * listener);
	static void unregister_listener(journal_listener * listener);
	static int set_unique_id_file(int dfd, const char * file, bool create = false);
	static listener_id get_unique_id();
	
private:
	int meta_dfd;
	istr meta_name;
	
	int data_fd;
	tx_fd meta_fd;
	off_t data_size;
	uint32_t sequence;
	patchgroup_id_t pid;
	tx_pre_end handle;
	
	std::set<listener_id> discarded;
	
	static sys_journal global_journal;
	static std::map<listener_id, journal_listener *> listener_map;
	
	struct unique_id
	{
		tx_fd fd;
		listener_id next;
		inline unique_id() : fd(-1), next(NO_ID) {}
		inline ~unique_id()
		{
			if(fd >= 0)
				tx_close(fd);
		}
	};
	static unique_id id;
	
	/* if no listener provided, all listeners, via global registry */
	int playback(journal_listener * target = NULL, bool fail_missing = false);
	/* copy the entries in this journal to a new one, omitting the discarded entries */
	int filter(int dfd, const char * file, off_t * new_size);
	/* tx_add_depend()s the patchgroup and tx_write()s the meta file */
	int flush_tx();
	/* actual function used for tx_register_pre_end */
	static void flush_tx_static(void * data);
};

#endif /* __SYS_JOURNAL_H */
