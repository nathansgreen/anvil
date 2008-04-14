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

class sys_journal
{
public:
	typedef uint32_t listener_id;
	static const listener_id NO_ID = (listener_id) -1;
	
	class journal_listener
	{
	public:
		virtual int journal_replay(void *& entry, size_t length) = 0;
		virtual listener_id id() = 0;
		
		journal_listener() : journal(NULL) {}
		
		virtual ~journal_listener()
		{
			sys_journal::unregister_listener(this);
		}
		
	protected:
		void set_journal(sys_journal * journal)
		{
			this->journal = journal;
		}
		
		int journal_append(void * entry, size_t length)
		{
			return journal->append(this, entry, length);
		}
		
	private:
		sys_journal * journal;
	};
	
	int append(journal_listener * listener, void * entry, size_t length);
	
	inline sys_journal() : fd(-1) {}
	int init(int dfd, const char * file, bool create = true);
	void deinit();
	inline ~sys_journal()
	{
		if(fd >= 0)
			deinit();
	}
	
	static journal_listener * lookup_listener(listener_id id);
	static int register_listener(journal_listener * listener);
	static void unregister_listener(journal_listener * listener);
private:
	tx_fd fd;
	off_t offset;
	static std::map<listener_id, journal_listener *> listener_map;
	
	int playback();
};

#endif /* __SYS_JOURNAL_H */
