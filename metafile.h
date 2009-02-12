/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __METAFILE_H
#define __METAFILE_H

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* metafile transactions */
typedef int32_t tx_id;

struct tx_pre_end {
	/* fill in the data and handle fields */
	void * data;
	void (*handle)(void * data);
	/* but leave this one alone */
	struct tx_pre_end * _next;
};

int tx_init(int dfd, size_t log_size);
void tx_deinit(void);

int tx_start(void);
tx_id tx_end(int assign_id);

/* tx_start_external() causes subsequent file operations until tx_end_external()
 * to become dependencies of this transaction - see journal::start_external() */
int tx_start_external(void);
int tx_end_external(int success);

/* adds a pre-end handler to the current transaction */
void tx_register_pre_end(struct tx_pre_end * handle);
/* does a linear search; avoid if possible */
void tx_unregister_pre_end(struct tx_pre_end * handle);

int tx_sync(tx_id id);
int tx_forget(tx_id id);

/* metafiles */
typedef struct metafile * tx_fd;

tx_fd tx_open(int dfd, const char * name, int create);
size_t tx_size(const tx_fd file);
int tx_dirty(const tx_fd file);
/* reads some part of the file */
size_t tx_read(const tx_fd file, void * buf, size_t length, off_t offset);
/* truncates the file to 0 size */
int tx_truncate(tx_fd file);
/* writes some part of the metafile as part of the current transaction */
int tx_write(tx_fd file, const void * buf, size_t length, off_t offset);
int tx_close(tx_fd file);

int tx_unlink(int dfd, const char * name, int recursive);

/* simple recursive transactions: the "real" transaction is the outermost one */
int tx_start_r(void);
int tx_end_r(void);

struct tx_handle {
	uint32_t in_tx;
};
#define TX_HANDLE_INIT(handle) do { (handle).in_tx = 0; } while(0)
#define TX_IN_TX(handle) ((handle).in_tx > 0)
#define TX_START(handle) ({ int r = 0; if(!(handle).in_tx++) { r = tx_start_r(); if(r < 0) (handle).in_tx--; } assert((handle).in_tx); r; })
#define TX_END(handle) ({ int r = 0; assert((handle).in_tx); if(!--(handle).in_tx) { r = tx_end_r(); if(r < 0) (handle).in_tx++; } r; })
#define TX_CLEANUP(handle) do { if((handle).in_tx) { int r = tx_end_r(); assert(r >= 0); } (handle).in_tx = 0; } while(0)

#ifdef __cplusplus
}

#include <map>

#include "journal.h"
#include "blob_buffer.h"

#define DEBUG_MF 0

#if DEBUG_MF
#define MF_DEBUG(format, args...) printf("mf::%s @%p:%s (" format ")\n", __FUNCTION__, this, path.str(), ##args)
#define MF_S_DEBUG(format, args...) printf("mf::%s (" format ")\n", __FUNCTION__, ##args)
#else
#define MF_DEBUG(format, args...)
#define MF_S_DEBUG(format, args...)
#endif

struct metafile
{
public:
	static metafile * open(int dfd, const char * name, bool create);
	
	inline size_t size() const
	{
		return data.size();
	}
	
	inline bool dirty() const
	{
		return is_dirty;
	}
	
	inline size_t read(void * buf, size_t length, size_t offset) const
	{
		MF_DEBUG("%zu:%zu", offset, length);
		if(offset >= data.size())
			return 0;
		if(offset + length > data.size())
			length = data.size() - offset;
		util::memcpy(buf, &data[offset], length);
		return length;
	}
	
	inline void truncate()
	{
		is_dirty = true;
		data.set_size(0);
	}
	
	inline int write(const void * buf, size_t length, size_t offset)
	{
		MF_DEBUG("%zu:%zu", offset, length);
		is_dirty = true;
		return data.overwrite(offset, buf, length);
	}
	
	int flush();
	
	inline void close()
	{
		MF_DEBUG("%d", usage);
		if(!--usage && !is_dirty)
			delete this;
	}
	
	static int unlink(int dfd, const char * name, bool recursive);
	
	/* transactions */
	static int tx_init(int dfd, size_t log_size);
	static void tx_deinit();
	
	static int tx_start();
	static tx_id tx_end(bool assign_id);
	
	static int tx_start_external();
	static int tx_end_external(bool success);
	
	static void tx_register_pre_end(tx_pre_end * handle);
	static void tx_unregister_pre_end(tx_pre_end * handle);
	
	static int tx_sync(tx_id id);
	static int tx_forget(tx_id id);
	
	static int tx_start_r();
	static int tx_end_r();
	
private:
	inline metafile(const istr & path)
		: path(path), usage(1), is_dirty(false)
	{
		assert(path);
		mf_map[path] = this;
	}
	
	inline ~metafile()
	{
		MF_DEBUG("");
		if(is_dirty)
		{
			int r = flush();
			assert(r >= 0);
		}
		mf_map.erase(path);
	}
	
	const istr path;
	blob_buffer data;
	size_t usage;
	bool is_dirty;
	
	/* static stuff */
	typedef std::map<istr, metafile *, strcmp_less> mf_map_t;
	static mf_map_t mf_map;
	
	static int journal_dir;
	static journal * last_journal;
	static journal * current_journal;
	static uint32_t tx_recursion;
	static size_t tx_log_size;
	
	static tx_id last_tx_id;
	static tx_pre_end * pre_end_handlers;
	typedef std::map<tx_id, journal *> tx_map_t;
	static tx_map_t tx_map; 
	
	static int switch_journal();
	static istr full_path(int dfd, const char * name);
	static bool ends_with(const char * string, const char * suffix);
	static int record_processor(void * data, size_t length, void * param);
};
#endif

#endif /* __METAFILE_H */
