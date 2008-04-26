/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <ctype.h>
#include <stdio.h>

#include "openat.h"
#include "transaction.h"

#include "dtable.h"
#include "sys_journal.h"
#include "simple_dtable.h"
#include "overlay_dtable.h"
#include "journal_dtable.h"
#include "managed_dtable.h"

extern "C" {
int command_dtable(int argc, const char * argv[]);
};

static void print(dtype x)
{
	switch(x.type)
	{
		case dtype::UINT32:
			printf("%u", x.u32);
			break;
		case dtype::DOUBLE:
			printf("%lg", x.dbl);
			break;
		case dtype::STRING:
			printf("%s", x.str);
			break;
	}
}

static void print(blob x, const char * prefix = NULL)
{
	for(size_t i = 0; i < x.size(); i += 16)
	{
		size_t m = i + 16;
		if(prefix)
			printf("%s", prefix);
		for(size_t j = i; j < m; j++)
		{
			if(j < x.size())
				printf("%02x ", x[j]);
			else
				printf("   ");
			if((i % 16) == 8)
				printf(" ");
		}
		printf(" |");
		for(size_t j = i; j < m; j++)
		{
			if(j < x.size())
				printf("%c", isprint(x[j]) ? x[j] : '.');
			else
				printf(" ");
		}
		printf("|\n");
	}
}

static void run_iterator(dtable * table)
{
	bool more = true;
	dtable_iter * iter = table->iterator();
	while(iter->valid())
	{
		if(!more)
		{
			printf("iter->next() returned false, but iter->valid() says there is more!\n");
			break;
		}
		print(iter->key());
		printf(":");
		print(iter->value(), "\t");
		more = iter->next();
	}
	delete iter;
}

int command_dtable(int argc, const char * argv[])
{
	int r;
	managed_simple_dtable * mdt;
	sys_journal * journal = sys_journal::get_global_journal();
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = sys_journal::set_unique_id_file(AT_FDCWD, "sys_journal_id", true);
	printf("set_unique_id_file = %d\n", r);
	r = journal->init(AT_FDCWD, "sys_journal", true);
	printf("journal->init = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, "managed_dtable", dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	mdt = new managed_simple_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", journal);
	printf("mdt->init = %d, %d disk dtables\n", r, mdt->disk_dtables());
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->append((uint32_t) 4, blob(5, (const uint8_t *) "hello"));
	printf("mdt->append = %d\n", r);
	r = mdt->append((uint32_t) 2, blob(5, (const uint8_t *) "world"));
	printf("mdt->append = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	/* pass true to recover journal in this case */
	r = mdt->init(AT_FDCWD, "managed_dtable", true, journal);
	printf("mdt->init = %d, %d disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->digest();
	printf("mdt->digest = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", journal);
	printf("mdt->init = %d, %d disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->append((uint32_t) 6, blob(7, (const uint8_t *) "icanhas"));
	printf("mdt->append = %d\n", r);
	r = mdt->append((uint32_t) 0, blob(11, (const uint8_t *) "cheezburger"));
	printf("mdt->append = %d\n", r);
	run_iterator(mdt);
	r = mdt->digest();
	printf("mdt->digest = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", journal);
	printf("mdt->init = %d, %d disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->combine();
	printf("mdt->combine = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", journal);
	printf("mdt->init = %d, %d disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	delete mdt;
	
	return 0;
}
