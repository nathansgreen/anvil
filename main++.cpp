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
		printf(" ");
		for(size_t j = i; j < m; j++)
		{
			if(j < x.size())
				printf("%c", isprint(x[j]) ? x[j] : '.');
			else
				printf(" ");
			if((i % 16) == 8)
				printf(" ");
		}
		printf("\n");
	}
}

static void run_iterator(dtable * table)
{
	dtable_iter * iter = table->iterator();
	while(iter->valid())
	{
		print(iter->key());
		printf(":\n");
		print(iter->value(), "\t");
		iter->next();
	}
	delete iter;
}

int command_dtable(int argc, const char * argv[])
{
	managed_simple_dtable * mdt;
	sys_journal * journal = sys_journal::get_global_journal();
	
	tx_start();
	sys_journal::set_unique_id_file(AT_FDCWD, "sys_journal_id", true);
	journal->init(AT_FDCWD, "sys_journal", true);
	managed_dtable::create(AT_FDCWD, "managed_dtable", dtype::UINT32);
	tx_end(0);
	
	mdt = new managed_simple_dtable;
	mdt->init(AT_FDCWD, "managed_dtable", journal);
	tx_start();
	mdt->append((uint32_t) 4, blob(5, (const uint8_t *) "hello"));
	mdt->append((uint32_t) 2, blob(5, (const uint8_t *) "world"));
	run_iterator(mdt);
	tx_end(0);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	mdt->init(AT_FDCWD, "managed_dtable", true, journal);
	run_iterator(mdt);
	tx_start();
	/* I suspect the fact that we cannot read from a tx_written file until
	 * tx_end() is going to kick our ass now. That will have to be taken
	 * care of somehow... */
	mdt->digest();
	run_iterator(mdt);
	tx_end(0);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	mdt->init(AT_FDCWD, "managed_dtable", journal);
	run_iterator(mdt);
	tx_start();
	mdt->append((uint32_t) 8, blob(7, (const uint8_t *) "icanhas"));
	mdt->append((uint32_t) 6, blob(11, (const uint8_t *) "cheezburger"));
	run_iterator(mdt);
	/* ditto */
	mdt->digest();
	run_iterator(mdt);
	tx_end(0);
	delete mdt;
	
	mdt = new managed_simple_dtable;
	mdt->init(AT_FDCWD, "managed_dtable", journal);
	run_iterator(mdt);
	tx_start();
	/* ditto */
	mdt->combine();
	run_iterator(mdt);
	tx_end(0);
	delete mdt;
	
	return 0;
}
