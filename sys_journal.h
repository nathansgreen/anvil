/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
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

#include <ext/hash_set>
#include <ext/hash_map>
#include <ext/pool_allocator.h>
#include "concat_queue.h"

#include "istr.h"
#include "rwfile.h"
#include "dtable.h"

class sys_journal
{
public:
	typedef uint32_t listener_id;
	static const listener_id NO_ID = (listener_id) -1;
	
	class listening_dtable;
	
	class listening_dtable_warehouse
	{
	public:
		/* find a listener by ID, or NULL if none exists */
		virtual listening_dtable * lookup(listener_id id) const = 0;
		/* find a listener by ID, or create one that can handle the given data if none exists */
		/* doesn't actually handle the data given, just uses it to help create the listener */
		virtual listening_dtable * obtain(listener_id id, const void * entry, size_t length, sys_journal * journal) = 0;
		virtual listening_dtable * obtain(listener_id id, dtype::ctype key_type, sys_journal * journal) = 0;
		/* return the current number of listeners */
		virtual size_t size() const = 0;
	private:
		/* change the listener ID for the given listener */
		virtual bool reset(listening_dtable * listener, listener_id id) = 0;
		/* remove a listener from this warehouse */
		virtual bool remove(listening_dtable * listener) = 0;
		/* remove and destroy all listeners in this warehouse */
		virtual void clear() = 0;
		
		friend class sys_journal;
	};
	
	class listening_dtable : public dtable
	{
	public:
		/* copies all data from this listening dtable to another by writing a rollover
		 * record to the journal and then merging the in-memory state into the target */
		inline int rollover(listening_dtable * target) const
		{
			assert(is_temporary(id()));
			/* abuse source_shadow_ok() slightly and use it here; it will work fine */
			if(!source_shadow_ok(this, target))
				return -EINVAL;
			int r = journal->rollover(this, target);
			if(r < 0)
				return r;
			if(cmp_name && !blob_cmp)
				return pending_rollover(target);
			assert(ldt_pending.empty() && target->ldt_pending.empty());
			return real_rollover(target);
		}
		
		inline listening_dtable() : local_id(NO_ID), warehouse(NULL), journal(NULL) {}
		
		inline listener_id id() const { return local_id; }
		inline listening_dtable_warehouse * get_warehouse() const { return warehouse; }
		inline sys_journal * get_journal() const { return journal; }
		
		inline virtual int set_blob_cmp(const blob_comparator * cmp)
		{
			int r = dtable::set_blob_cmp(cmp);
			if(r >= 0 && !ldt_pending.empty())
				replay_pending();
			return r;
		}
		
		/* discard the journal entries, remove from the warehouse, and destroy */
		inline int discard()
		{
			int r = journal->discard(this);
			if(r < 0)
				return r;
			if(warehouse)
				warehouse->remove(this);
			destroy();
			return 0;
		}
		
		/* only actually destroy if we're not in a warehouse */
		inline virtual void destroy() const { if(!warehouse) delete this; }
		inline virtual ~listening_dtable() { assert(!warehouse); }
		
	protected:
		inline int set_id(listener_id lid)
		{
			assert(lid != NO_ID);
			if(warehouse)
				warehouse->reset(this, lid);
			local_id = lid;
			return 0;
		}
		
		inline void set_journal(sys_journal * journal)
		{
			this->journal = journal;
		}
		
		inline int journal_append(void * entry, size_t length)
		{
			return journal->append(this, entry, length);
		}
		
		inline int journal_discard()
		{
			return journal->discard(this);
		}
		
		inline int send(listening_dtable * target, const dtype & key, const blob & value, bool append = false) const
		{
			return target->accept(key, value, append);
		}
		
		/* see the note below about unique_blob_store */
		inline void add_pending(const blob & key, const blob & value, bool append)
		{
			unique_blob_store::iterator ukey = ldt_unique.find(key);
			if(ukey == ldt_unique.end())
				ukey = ldt_unique.insert(key).first;
			pending_entry pending(*ukey, value, append);
			ldt_pending.append(pending);
		}
		
	private:
		inline void set_warehouse(listening_dtable_warehouse * warehouse)
		{
			assert(!warehouse || !this->warehouse);
			this->warehouse = warehouse;
		}
		
		/* called by real_rollover() (via send() above) and replay_pending() */
		virtual int accept(const dtype & key, const blob & value, bool append) = 0;
		
		listener_id local_id;
		listening_dtable_warehouse * warehouse;
		sys_journal * journal;
		
		/* When playing back records before we have the required blob comparator, we store the
		 * keys and values in a concat_queue. But we'd like to store a single copy of each key,
		 * in case they are updated many times each. Even though we don't have the comparator,
		 * we know that blobs with exactly the same bytes are the same. We use a hash set to
		 * store the canonical versions of pending blob keys, and clear it when we later get
		 * the comparator. (We also, at that time, finish replaying the keys and values.) */
		typedef __gnu_cxx::__pool_alloc<blob> set_pool_allocator;
		typedef __gnu_cxx::hash_set<blob, blob_hashing_comparator, blob_hashing_comparator, set_pool_allocator> unique_blob_store;
		
		struct pending_entry
		{
			blob key, value;
			bool append;
			inline pending_entry(const blob & key, const blob & value, bool append) : key(key), value(value), append(append) {}
		};
		/* These are mutable for pending_rollover(), which must be const to be called from
		 * rollover(). It is called only when rolling over before we have the necessary blob
		 * comparator; in that case, even though we modify these fields, it won't affect the
		 * behavior of this instance. (And it will soon be destructed anyway, since that
		 * should only happen during sys_journal playback.) */
		mutable concat_queue<pending_entry> ldt_pending;
		mutable unique_blob_store ldt_unique;
		
		void replay_pending();
		virtual int journal_replay(void *& entry, size_t length) = 0;
		
		int pending_rollover(listening_dtable * target) const;
		virtual int real_rollover(listening_dtable * target) const = 0;
		
		friend class sys_journal;
	};
	
	template<class T>
	class listening_dtable_warehouse_impl : public listening_dtable_warehouse
	{
	public:
		virtual T * lookup(listener_id lid) const
		{
			typename id_ptr_map::const_iterator it = map.find(lid);
			return (it != map.end()) ? it->second : NULL;
		}
		
		virtual T * obtain(listener_id lid, const void * entry, size_t length, sys_journal * journal)
		{
			typename id_ptr_map::iterator it = map.find(lid);
			if(it != map.end())
				return it->second;
			T * add = create(lid, entry, length, journal);
			if(!add)
				return NULL;
			add->set_warehouse(this);
			map[lid] = add;
			return add;
		}
		
		virtual T * obtain(listener_id lid, dtype::ctype key_type, sys_journal * journal)
		{
			typename id_ptr_map::iterator it = map.find(lid);
			if(it != map.end())
			{
				assert(it->second->key_type() == key_type);
				return it->second;
			}
			T * add = create(lid, key_type, journal);
			if(!add)
				return NULL;
			add->set_warehouse(this);
			map[lid] = add;
			return add;
		}
		
		virtual size_t size() const { return map.size(); }
		
		virtual void clear()
		{
			typename id_ptr_map::iterator it = map.begin();
			while(it != map.end())
			{
				T * listener = it->second;
				++it;
				listener->set_warehouse(NULL);
				listener->destroy();
			}
			map.clear();
		}
		
		virtual ~listening_dtable_warehouse_impl()
		{
			clear();
		}
		
	protected:
		virtual bool reset(listening_dtable * listener, listener_id lid)
		{
			listener_id old = listener->id();
			typename id_ptr_map::iterator it = map.find(old);
			if(it == map.end())
				return false;
			T * actual = it->second;
			assert(actual == listener);
			map.erase(it);
			map[lid] = actual;
			return true;
		}
		
		virtual bool remove(listening_dtable * listener)
		{
			listener_id lid = listener->id();
			typename id_ptr_map::iterator it = map.find(lid);
			if(it == map.end())
				return false;
			assert(it->second == listener);
			listener->set_warehouse(NULL);
			map.erase(it);
			return true;
		}
		
		/* this is all you need to implement in subclasses */
		virtual T * create(listener_id lid, const void * entry, size_t length, sys_journal * journal) const = 0;
		virtual T * create(listener_id lid, dtype::ctype key_type, sys_journal * journal) const = 0;
		
	private:
		typedef __gnu_cxx::hash_map<listener_id, T *> id_ptr_map;
		id_ptr_map map;
	};
	
	/* remove any discarded entries from this journal */
	int filter();
	
	inline sys_journal() : meta_dfd(-1), meta_fd(NULL), dirty(false), registered(false), data_size(0), info_size(0)
	{
		handle.data = this;
		handle.handle = flush_tx_static;
	}
	/* the warehouse will be used to create the necessary listening dtables
	 * during playback, and will be populated with them for later retrieval */
	int init(int dfd, const char * file, listening_dtable_warehouse * warehouse, bool create = false, bool filter_on_empty = true);
	/* erase = true will tx_unlink() the journal - be sure you really want that */
	void deinit(bool erase = false);
	inline ~sys_journal()
	{
		if(meta_fd)
			deinit();
	}
	
	static inline sys_journal * get_global_journal()
	{
		return &global_journal;
	}
	/* allocates and initializes a new sys_journal in the same directory as the global journal */
	static sys_journal * spawn_init(const char * file, listening_dtable_warehouse * warehouse, bool create = false, bool filter_on_empty = true);
	
	static int set_unique_id_file(int dfd, const char * file, bool create = false);
	/* temporary IDs are odd rather than even and are automatically discarded
	 * during recovery unless they have been rolled into a non-temporary ID */
	static listener_id get_unique_id(bool temporary = false);
	static inline bool is_temporary(listener_id id) { return id & 1; }
	
private:
	int meta_dfd;
	istr meta_name;
	
	rwfile data;
	tx_fd meta_fd;
	
	bool dirty, registered, filter_on_empty;
	size_t data_size, info_size;
	uint32_t sequence;
	tx_pre_end handle;
	size_t live_entries;
	listening_dtable_warehouse * warehouse;
	
	int append(listening_dtable * listener, void * entry, size_t length);
	
	/* make a note that this listener's entries are no longer needed */
	inline int discard(listening_dtable * listener)
	{
		listener_id lid = listener->id();
		assert(warehouse->lookup(lid) == listener);
		return discard(lid);
	}
	int discard(listener_id lid);
	
	/* roll over the entries from a temporary listener to another; the
	 * listeners themselves must work out how to merge their runtime
	 * state (presumably with listening_dtable::rollover() above) */
	inline int rollover(const listening_dtable * from, listening_dtable * to)
	{
		listener_id from_id = from->id();
		listener_id to_id = to->id();
		assert(warehouse->lookup(from_id) == from);
		assert(warehouse->lookup(to_id) == to);
		assert(from->get_journal() == this);
		assert(to->get_journal() == this);
		return rollover(from_id, to_id);
	}
	int rollover(listener_id from, listener_id to);
	
	typedef __gnu_cxx::hash_map<listener_id, size_t> live_entry_map;
	live_entry_map live_entry_count;
	
	typedef __gnu_cxx::hash_set<listener_id> listener_id_set;
	listener_id_set discarded;
	
	typedef __gnu_cxx::hash_map<listener_id, listener_id_set> rollover_multimap;
	rollover_multimap rollover_ids;
	
	static sys_journal global_journal;
	
	struct unique_id
	{
		tx_fd fd;
		listener_id next[2];
		inline unique_id() : fd(NULL) { next[0] = NO_ID; next[1] = NO_ID; }
		inline ~unique_id()
		{
			if(fd)
				tx_close(fd);
		}
	};
	static unique_id id;
	
	/* if remove is not NULL, remove all the rolled over (temporary) IDs from it */
	void roll_over_rollover_ids(listener_id from, listener_id to, listener_id_set * remove = NULL);
	void discard_rollover_ids(listener_id lid);
	
	/* play back the entire journal, creating listeners as necessary */
	int playback();
	/* copy the entries in this journal to a new one, omitting the discarded entries */
	int filter(int dfd, const char * file, size_t * new_size);
	/* flushes the data file and tx_write()s the meta file */
	int flush_tx();
	/* actual function used for tx_register_pre_end */
	static void flush_tx_static(void * data);
};

#endif /* __SYS_JOURNAL_H */
