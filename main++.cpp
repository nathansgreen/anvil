/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define __STDC_FORMAT_MACROS

#include <time.h>
#include <ctype.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>

#include "openat.h"
#include "transaction.h"

#include "dtable.h"
#include "ctable.h"
#include "stable.h"
#include "sys_journal.h"
#include "journal_dtable.h"
#include "simple_dtable.h"
#include "managed_dtable.h"
#include "usstate_dtable.h"
#include "memory_dtable.h"
#include "simple_stable.h"
#include "reverse_blob_comparator.h"

extern "C" {
int command_info(int argc, const char * argv[]);
int command_dtable(int argc, const char * argv[]);
int command_edtable(int argc, const char * argv[]);
int command_odtable(int argc, const char * argv[]);
int command_ldtable(int argc, const char * argv[]);
int command_ussdtable(int argc, const char * argv[]);
int command_bfdtable(int argc, const char * argv[]);
int command_sidtable(int argc, const char * argv[]);
int command_didtable(int argc, const char * argv[]);
int command_kddtable(int argc, const char * argv[]);
int command_ctable(int argc, const char * argv[]);
int command_cctable(int argc, const char * argv[]);
int command_consistency(int argc, const char * argv[]);
int command_durability(int argc, const char * argv[]);
int command_rollover(int argc, const char * argv[]);
int command_stable(int argc, const char * argv[]);
int command_iterator(int argc, const char * argv[]);
int command_blob_cmp(int argc, const char * argv[]);
int command_performance(int argc, const char * argv[]);
int command_bdbtest(int argc, const char * argv[]);
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
			printf("%s", (const char *) x.str);
			break;
		case dtype::BLOB:
			size_t size = x.blb.size();
			printf("%zu[", size);
			for(size_t i = 0; i < size && i < 8; i++)
				printf("%02X%s", x.blb[i], (i < size - 1) ? " " : "");
			printf((size > 8) ? "...]" : "]");
			break;
	}
}

static void print(blob x, const char * prefix = NULL, ...)
{
	va_list ap;
	va_start(ap, prefix);
	if(!x.exists())
	{
		if(prefix)
			vprintf(prefix, ap);
		printf("(non-existent)\n");
		va_end(ap);
		return;
	}
	for(size_t i = 0; i < x.size(); i += 16)
	{
		size_t m = i + 16;
		if(prefix)
			vprintf(prefix, ap);
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
	va_end(ap);
}

static void run_iterator(dtable * table)
{
	bool more = true;
	dtable::iter * iter = table->iterator();
	printf("dtable contents:\n");
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

static void run_iterator(ctable * table)
{
	dtype old_key(0u);
	bool more = true, first = true;
	ctable::iter * iter = table->iterator();
	printf("ctable contents:\n");
	while(iter->valid())
	{
		dtype key = iter->key();
		if(!more)
		{
			printf("iter->next() returned false, but iter->valid() says there is more!\n");
			break;
		}
		if(first || key.compare(old_key))
		{
			printf("==> key ");
			print(key);
			printf("\n");
			old_key = key;
			first = false;
		}
		printf("%s:", (const char *) iter->name());
		print(iter->value(), "\t");
		more = iter->next();
	}
	delete iter;
}

static void run_iterator(stable * table)
{
	dtype old_key(0u);
	bool more = true, first = true;
	stable::column_iter * columns = table->columns();
	stable::iter * iter = table->iterator();
	printf("stable columns:\n");
	while(columns->valid())
	{
		size_t rows = columns->row_count();
		const char * type = dtype::name(columns->type());
		if(!more)
		{
			printf("columns->next() returned false, but columns->valid() says there is more!\n");
			break;
		}
		printf("%s:\t%s (%zu row%s)\n", (const char *) columns->name(), type, rows, (rows == 1) ? "" : "s");
		more = columns->next();
	}
	delete columns;
	more = true;
	printf("stable contents:\n");
	while(iter->valid())
	{
		dtype key = iter->key();
		if(!more)
		{
			printf("iter->next() returned false, but iter->valid() says there is more!\n");
			break;
		}
		if(first || key.compare(old_key))
		{
			printf("==> key ");
			print(key);
			printf("\n");
			old_key = key;
			first = false;
		}
		printf("%s:\t", (const char *) iter->column());
		print(iter->value());
		printf("\n");
		more = iter->next();
	}
	delete iter;
}

int command_info(int argc, const char * argv[])
{
	params::print_classes();
	return 0;
}

int command_dtable(int argc, const char * argv[])
{
	int r;
	managed_dtable * mdt;
	const char * path;
	params config;
	
	if(argc > 1)
		path = argv[1];
	else
		path = "msdt_test";
	if(argc > 2)
		config.set("base", argv[2]);
	else
		config.set_class("base", simple_dtable);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, path, config, dtype::UINT32);
	printf("dtable::create(%s) = %d\n", path, r);
	if(r < 0)
	{
		tx_end(0);
		return r;
	}
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->insert(6u, blob("hello"));
	printf("mdt->insert = %d\n", r);
	r = mdt->insert(4u, blob("world"));
	printf("mdt->insert = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->digest();
	printf("mdt->digest = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->insert(8u, blob("icanhas"));
	printf("mdt->insert = %d\n", r);
	r = mdt->insert(2u, blob("cheezburger"));
	printf("mdt->insert = %d\n", r);
	run_iterator(mdt);
	r = mdt->digest();
	printf("mdt->digest = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->combine();
	printf("mdt->combine = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	delete mdt;
	
	return 0;
}

static int excp_perf(dtable * table)
{
	dtable::iter * it;
	blob fixed("aoife");
	blob exception("pandora");
	int sum[2] = {0, 0};
	struct timeval start, end;
	int r = tx_start();
	printf("tx_start = %d\n", r);
	
	printf("Populating table... ");
	fflush(stdout);
	for(int i = 0; i < 4000000; i++)
	{
		uint32_t key = i;
		if(rand() % 500)
			r = table->insert(key, fixed);
		else
			r = table->insert(key, exception);
		if(r < 0)
			goto fail_tx;
	}
	printf("done.\n");
	
	printf("Calculating verification data... ");
	fflush(stdout);
	for(it = table->iterator(); it->valid(); it->next())
	{
		dtype key = it->key();
		blob value = it->value();
		if(!value.compare(fixed))
			sum[0] += key.u32;
		else if(!value.compare(exception))
			sum[1] += key.u32;
		else
		{
			delete it;
			goto fail_tx;
		}
	}
	delete it;
	printf("(0x%08X, 0x%08X)\n", sum[0], sum[1]);
	
	printf("Waiting 2 seconds for digest interval...\n");
	sleep(2);
	printf("Maintaining... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	r = table->maintain();
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	printf("maintain = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	printf("Verifying data... ");
	fflush(stdout);
	for(it = table->iterator(); it->valid(); it->next())
	{
		dtype key = it->key();
		blob value = it->value();
		if(!value.compare(fixed))
			sum[0] -= key.u32;
		else if(!value.compare(exception))
			sum[1] -= key.u32;
		else
		{
			delete it;
			goto fail;
		}
	}
	if(sum[0] || sum[1])
		goto fail;
	printf("OK!\n");
	
	printf("Random lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 2000000; i++)
	{
		uint32_t key = rand() % 4000000;
		blob value = table->find(key);
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	return 0;
	
fail_tx:
	tx_end(0);
fail:
	printf("fail!\n");
	return -1;
}

int command_edtable(int argc, const char * argv[])
{
	int r;
	blob fixed("fixed");
	blob exception("exception");
	managed_dtable * mdt;
	params config;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) exception_dtable
		"base_config" config [
			"base" class(dt) array_dtable
			"alt" class(dt) simple_dtable
			"reject_value" string "_____"
		]
		"digest_interval" int 2
		"combine_interval" int 4
		"combine_count" int 4
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, "excp_test", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->insert(0u, fixed);
	printf("mdt->insert = %d\n", r);
	r = mdt->insert(1u, fixed);
	printf("mdt->insert = %d\n", r);
	r = mdt->insert(3u, fixed);
	printf("mdt->insert = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	printf("Waiting 3 seconds for digest interval...\n");
	sleep(3);
	r = tx_start();
	printf("tx_start = %d\n", r);
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config);
	printf("mdt->init = %d\n", r);
	r = mdt->maintain();
	printf("mdt->maintain = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = mdt->insert(2u, exception);
	printf("mdt->insert = %d\n", r);
	r = mdt->insert(8u, exception);
	printf("mdt->insert = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	printf("Waiting 2 seconds for digest interval...\n");
	sleep(2);
	r = tx_start();
	printf("tx_start = %d\n", r);
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config);
	printf("mdt->init = %d\n", r);
	r = mdt->maintain();
	printf("mdt->maintain = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
	{
		config = params();
		r = params::parse(LITERAL(
		config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) btree_dtable
				"base_config" config [
					"base" class(dt) fixed_dtable
				]
				"alt" class(dt) simple_dtable
				"reject_value" string "_____"
			]
			"digest_interval" int 2
			"combine_interval" int 12
			"combine_count" int 8
		]), &config);
		printf("params::parse = %d\n", r);
		config.print();
		printf("\n");
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		r = managed_dtable::create(AT_FDCWD, "excp_perf", config, dtype::UINT32);
		printf("dtable::create = %d\n", r);
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
		/* run a test with it */
		mdt = new managed_dtable;
		r = mdt->init(AT_FDCWD, "excp_perf", config);
		printf("mdt->init = %d\n", r);
		excp_perf(mdt);
		delete mdt;
		
		/* should we also run with exception_dtable but simple_dtable underneath, to
		 * isolate the overhead without the performance benefits of array_dtable? */
		config = params();
		r = params::parse(LITERAL(
		config [
			"base" class(dt) btree_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
			]
			"digest_interval" int 2
			"combine_interval" int 12
			"combine_count" int 8
		]), &config);
		printf("params::parse = %d\n", r);
		config.print();
		printf("\n");
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		r = managed_dtable::create(AT_FDCWD, "exbl_perf", config, dtype::UINT32);
		printf("dtable::create = %d\n", r);
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
		/* run the same test */
		mdt = new managed_dtable;
		r = mdt->init(AT_FDCWD, "exbl_perf", config);
		printf("mdt->init = %d\n", r);
		excp_perf(mdt);
		delete mdt;
	}
	
	return 0;
}

static int ovdt_perf(dtable * table)
{
	struct timeval start, end;
	dtable::iter * it;
	
	printf("Random lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 2000000; i++)
	{
		uint32_t key = rand() % 4000000;
		blob value = table->find(key);
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	printf("Linear scans... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	it = table->iterator();
	for(int i = 0; i < 4; i++)
	{
		/* forwards */
		for(; it->valid(); it->next())
			it->value();
		/* backwards */
		while(it->prev())
			it->value();
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	delete it;
	
	return 0;
}

int command_odtable(int argc, const char * argv[])
{
	params config, base_config;
	blob exception("pandora");
	blob fixed("aoife");
	dtable * dt;
	istr base;
	bool b;
	int r;
	
	config = params();
	r = params::parse(LITERAL(
	config [
		"base" class(dt) exception_dtable
		"base_config" config [
			"base" class(dt) array_dtable
			"alt" class(dt) simple_dtable
			"reject_value" string "_____"
		]
		"digest_interval" int 2
		"combine_interval" int 12
		"combine_count" int 8
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "ovdt_perf", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "ovdt_perf", config);
	printf("dtable_factory::load = %p\n", dt);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	printf("Populating table... ");
	fflush(stdout);
	for(int i = 0; i < 4000000; i++)
	{
		uint32_t key = i;
		if(rand() % 500)
			r = dt->insert(key, fixed);
		else
			r = dt->insert(key, exception);
		if(r < 0)
		{
			tx_end(0);
			printf("fail!\n");
			return -1;
		}
	}
	printf("done.\n");
	
	printf("Waiting 2 seconds for digest interval...\n");
	sleep(2);
	printf("Maintaining... ");
	fflush(stdout);
	r = dt->maintain();
	printf("done. (= %d)\n", r);
	/* insert one final key so that there's something in the journal for the overlay to look at */
	r = dt->insert(2000000u, fixed);
	printf("dt->insert = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	ovdt_perf(dt);
	delete dt;
	
	b = config.get("base", &base);
	assert(b);
	b = config.get("base_config", &base_config);
	assert(b);
	base_config.print();
	printf("\n");
	/* load the first disk dtable directly */
	dt = dtable_factory::load(base, AT_FDCWD, "ovdt_perf/md_data.0", base_config);
	printf("dtable_factory::load = %p\n", dt);
	ovdt_perf(dt);
	delete dt;
	
	return 0;
}

int command_ldtable(int argc, const char * argv[])
{
	/* we just compare simple dtable with linear dtable using the exception
	 * dtable performance test, and count on it to test functionality also */
	int r;
	dtable * dt;
	params config;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) linear_dtable
		"digest_interval" int 2
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "lldt_test", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "lldt_test", config);
	printf("dtable_factory::load = %p\n", dt);
	
	excp_perf(dt);
	delete dt;
	
	config = params();
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 2
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "lsdt_test", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "lsdt_test", config);
	printf("dtable_factory::load = %p\n", dt);
	
	excp_perf(dt);
	delete dt;
	
	return 0;
}

int command_ussdtable(int argc, const char * argv[])
{
	int r;
	params config;
	dtable * table;
	memory_dtable mdt;
	const dtable_factory * base = dtable_factory::lookup("usstate_dtable");
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) fixed_dtable
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	mdt.init(dtype::UINT32, true);
	mdt.insert(1u, "MA");
	mdt.insert(2u, "CA");
	run_iterator(&mdt);
	
	r = base->create(AT_FDCWD, "usst_test", config, &mdt);
	printf("uss::create = %d\n", r);
	table = base->open(AT_FDCWD, "usst_test", config);
	printf("uss::open = %p\n", table);
	run_iterator(table);
	delete table;
	
	table = dtable_factory::load("fixed_dtable", AT_FDCWD, "usst_test", params());
	printf("dtable_factory::load = %p\n", table);
	run_iterator(table);
	delete table;
	
	mdt.insert(3u, "other");
	r = base->create(AT_FDCWD, "usst_fail", config, &mdt);
	printf("uss::create = %d (expect failure)\n", r);
	
	return 0;
}

static int bfdt_perf(dtable * table)
{
	struct timeval start, end;
	
	printf("Extant lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 200000; i++)
	{
		uint32_t key = rand() % 4000000;
		blob value = table->find(key * 2);
		if(!value.exists() || value.index<uint32_t>(0) != key)
		{
			printf("!");
			fflush(stdout);
		}
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	printf("Nonexistent lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 200000; i++)
	{
		uint32_t key = rand() % 4000000;
		blob value = table->find(key * 2 + 1);
		if(value.exists())
		{
			printf("!");
			fflush(stdout);
		}
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	printf("Mixed lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 200000; i++)
	{
		uint32_t key = rand() % 8000000;
		blob value = table->find(key);
	}
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("%d.%06d seconds.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	return 0;
}

int command_bfdtable(int argc, const char * argv[])
{
	params config;
	dtable * dt;
	int r;
	
	config = params();
	r = params::parse(LITERAL(
	config [
		"base" class(dt) bloom_dtable
		"base_config" config [
			"bloom_k" int 5
			"base" class(dt) simple_dtable
		]
		"digest_interval" int 2
		"combine_interval" int 12
		"combine_count" int 8
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "bfdt_perf", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_perf", config);
	printf("dtable_factory::load = %p\n", dt);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	printf("Populating table... ");
	fflush(stdout);
	for(int i = 0; i < 4000000; i++)
	{
		uint32_t key = i * 2;
		uint32_t value = i;
		r = dt->insert(key, blob(sizeof(value), &value));
		if(r < 0)
		{
			tx_end(0);
			printf("fail!\n");
			return -1;
		}
	}
	printf("done.\n");
	
	printf("Waiting 2 seconds for digest interval...\n");
	sleep(2);
	printf("Maintaining... ");
	fflush(stdout);
	r = dt->maintain();
	printf("done. (= %d)\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	bfdt_perf(dt);
	delete dt;
	
	printf("Repeat with direct access...\n");
	dt = dtable_factory::load("simple_dtable", AT_FDCWD, "bfdt_perf/md_data.0/base", params());
	printf("dtable_factory::load = %p\n", dt);
	bfdt_perf(dt);
	delete dt;
	
	return 0;
}

int command_sidtable(int argc, const char * argv[])
{
	int r;
	params config;
	uint32_t value;
	dtable * table;
	memory_dtable mdt;
	const dtable_factory * base = dtable_factory::lookup("smallint_dtable");
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) array_dtable
		"base_config" config [
			"value_size" int 1
			"tag_byte" bool false
		]
		"bytes" int 1
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	mdt.init(dtype::UINT32, true);
	value = 42;
	mdt.insert(1u, blob(sizeof(value), &value));
	value = 192;
	mdt.insert(2u, blob(sizeof(value), &value));
	run_iterator(&mdt);
	
	r = base->create(AT_FDCWD, "sidt_test", config, &mdt);
	printf("sid::create = %d\n", r);
	table = base->open(AT_FDCWD, "sidt_test", config);
	printf("sid::open = %p\n", table);
	run_iterator(table);
	delete table;
	
	table = dtable_factory::load("array_dtable", AT_FDCWD, "sidt_test", params());
	printf("dtable_factory::load = %p\n", table);
	run_iterator(table);
	delete table;
	
	value = 320;
	mdt.insert(3u, blob(sizeof(value), &value));
	r = base->create(AT_FDCWD, "sidt_fail", config, &mdt);
	printf("sid::create = %d (expect failure)\n", r);
	
	return 0;
}

int command_didtable(int argc, const char * argv[])
{
	int r;
	params config;
	dtable * table;
	size_t count = 0;
	memory_dtable mdt;
	uint32_t key = 10, value = 0;
	const dtable_factory * base = dtable_factory::lookup("deltaint_dtable");
	bool verbose = false, ok = true;
	dtable::iter * ref_it;
	dtable::iter * test_it;
	
	if(argc > 1 && !strcmp(argv[1], "-v"))
	{
		verbose = true;
		argc--;
		argv++;
	}
	if(argc > 1)
		count = atoi(argv[1]);
	if(!count)
		count = 2000000;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"ref" class(dt) simple_dtable
		"skip" int 4
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	mdt.init(dtype::UINT32, true);
	for(int i = 0; i < 20; i++)
	{
		key += (rand() % 10) + 1;
		value += rand() % 20;
		mdt.insert(key, blob(sizeof(value), &value));
	}
	run_iterator(&mdt);
	
	r = base->create(AT_FDCWD, "didt_test", config, &mdt);
	printf("did::create = %d\n", r);
	table = base->open(AT_FDCWD, "didt_test", config);
	printf("did::open = %p\n", table);
	run_iterator(table);
	printf("Check random lookups... ");
	fflush(stdout);
	for(int i = 0; i < 300; i++)
	{
		/* we do it randomly so that no leftover
		 * internal state should help us out */
		key = rand() % 150;
		blob value_mdt = mdt.find(key);
		blob value_ddt = table->find(key);
		if(value_mdt.compare(value_ddt))
		{
			printf("failed!\n");
			print(value_mdt, "memory find %u: ", key);
			print(value_ddt, " delta find %u: ", key);
			delete table;
			table = NULL;
			break;
		}
	}
	if(table)
	{
		delete table;
		printf("OK!\n");
	}
	
	table = base->open(AT_FDCWD, "didt_test", config);
	printf("did::open = %p\n", table);
	
	printf("Checking iterator behavior... ");
	fflush(stdout);
	ref_it = mdt.iterator();
	test_it = table->iterator();
	for(size_t i = 0; i < count && ok; i++)
	{
		ok = false;
		if(ref_it->valid() != test_it->valid())
			break;
		if(test_it->valid())
		{
			if(ref_it->key().compare(test_it->key()))
				break;
			if(ref_it->value().compare(test_it->value()))
				break;
		}
		ok = true;
		switch(rand() % 5)
		{
			/* first() */
			case 0:
			{
				bool ref_b = ref_it->first();
				bool test_b = test_it->first();
				assert(ref_b && test_b);
				if(verbose)
				{
					printf("[");
					fflush(stdout);
				}
				break;
			}
			/* last() */
			case 1:
			{
				bool ref_b = ref_it->last();
				bool test_b = test_it->last();
				assert(ref_b && test_b);
				if(verbose)
				{
					printf("]");
					fflush(stdout);
				}
				break;
			}
			/* next() */
			case 2:
			{
				bool ref_b = ref_it->next();
				bool test_b = test_it->next();
				if(ref_b != test_b)
					ok = false;
				if(verbose)
				{
					printf(">");
					fflush(stdout);
				}
				break;
			}
			/* prev() */
			case 3:
			{
				bool ref_b = ref_it->prev();
				bool test_b = test_it->prev();
				if(ref_b != test_b)
					ok = false;
				if(verbose)
				{
					printf("<");
					fflush(stdout);
				}
				break;
			}
			/* seek() */
			case 4:
			{
				uint32_t key = rand() % 150;
				bool ref_b = ref_it->seek(key);
				bool test_b = test_it->seek(key);
				if(ref_b != test_b)
					ok = false;
				if(verbose)
				{
					printf("%02X", key);
					fflush(stdout);
				}
				break;
			}
		}
		if(!ok)
			printf(verbose ? " behavior" : "behavior ");
	}
	if(verbose)
		printf(" ");
	if(ok)
		printf("%zu operations OK!\n", count);
	else
		printf("failed!\n");
	delete test_it;
	delete ref_it;
	delete table;
	
	table = dtable_factory::load("simple_dtable", AT_FDCWD, "didt_test/base", params());
	printf("dtable_factory::load = %p\n", table);
	run_iterator(table);
	delete table;
	
	table = dtable_factory::load("simple_dtable", AT_FDCWD, "didt_test/ref", params());
	printf("dtable_factory::load = %p\n", table);
	run_iterator(table);
	delete table;
	
	return 0;
}

static void iterator_test(const istr & type, const char * name, const params & config, size_t count, bool verbose)
{
	int r;
	dtable * dt;
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup(type, AT_FDCWD, name, config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	const char * layers[] = {"358", "079", "", "2457", "1267"};
#define LAYERS (sizeof(layers) / sizeof(layers[0]))
#define VALUES 10
	blob values[VALUES];
	
	dt = dtable_factory::load(type, AT_FDCWD, name, config);
	printf("dtable_factory::load = %p\n", dt);
	for(size_t i = 0; i < LAYERS; i++)
	{
		int delay = i ? 2 : 3;
		r = tx_start();
		printf("tx_start = %d\n", r);
		for(const char * value = layers[i]; *value; value++)
		{
			uint32_t key = *value - '0';
			char content[16];
			snprintf(content, sizeof(content), "L%zu-K%u", i, key * 2);
			values[key] = blob(content);
			r = dt->insert(key * 2, values[key]);
			printf("dt->insert(%d, %s) = %d\n", key, content, r);
		}
		run_iterator(dt);
		
		printf("Waiting %d seconds for digest interval...\n", delay);
		sleep(delay);
		
		r = dt->maintain();
		printf("dt->maintain() = %d\n", r);
		run_iterator(dt);
		
		r = tx_end(0);
		printf("tx_end = %d\n", r);
	}
	delete dt;
	
	dt = dtable_factory::load(type, AT_FDCWD, name, config);
	printf("dtable_factory::load = %p\n", dt);
	run_iterator(dt);
	
	printf("Checking iterator behavior... ");
	fflush(stdout);
	dtable::iter * it = dt->iterator();
	uint32_t it_pos = 0;
	bool ok = true;
	for(size_t i = 0; i < count && ok; i++)
	{
		ok = false;
		if(it->valid())
		{
			if(it_pos >= VALUES)
				break;
			dtype key = it->key();
			assert(key.type == dtype::UINT32);
			if(key.u32 != it_pos * 2)
				break;
			if(values[it_pos].compare(it->value()))
				break;
		}
		else if(it_pos < VALUES)
			break;
		ok = true;
		switch(rand() % 5)
		{
			/* first() */
			case 0:
			{
				bool b = it->first();
				assert(b);
				it_pos = 0;
				if(verbose)
				{
					printf("[");
					fflush(stdout);
				}
				break;
			}
			/* last() */
			case 1:
			{
				bool b = it->last();
				assert(b);
				it_pos = VALUES - 1;
				if(verbose)
				{
					printf("]");
					fflush(stdout);
				}
				break;
			}
			/* next() */
			case 2:
			{
				bool b = it->next();
				if(it_pos < VALUES)
				{
					it_pos++;
					if(!b && it_pos < VALUES)
						ok = false;
				}
				else if(b)
					ok = false;
				if(verbose)
				{
					printf(">");
					fflush(stdout);
				}
				break;
			}
			/* prev() */
			case 3:
			{
				bool b = it->prev();
				if(it_pos)
				{
					if(!b)
						ok = false;
					it_pos--;
				}
				else if(b)
					ok = false;
				if(verbose)
				{
					printf("<");
					fflush(stdout);
				}
				break;
			}
			/* seek() */
			case 4:
			{
				uint32_t key = rand() % (VALUES * 2);
				bool b = it->seek(key);
				if((b && (key % 2)) || (!b && !(key % 2)))
					ok = false;
				it_pos = (key + 1) / 2;
				if(verbose)
				{
					printf("%02d", key);
					fflush(stdout);
				}
				break;
			}
		}
		if(!ok)
			printf(verbose ? " behavior" : "behavior ");
	}
	if(verbose)
		printf(" ");
	if(ok)
		printf("%zu operations OK!\n", count);
	else
	{
		printf("failed! (");
		if(it->valid())
		{
			if(it_pos >= VALUES)
				printf("should be invalid");
			else
			{
				dtype key = it->key();
				printf("@%02d", key.u32);
				if(key.u32 != it_pos * 2)
					printf(", expected %d * 2", it_pos);
				else if(values[it_pos].compare(it->value()))
					printf(", bad value");
			}
		}
		else if(it_pos < VALUES)
			printf("should be valid");
		printf(")\n");
	}
	delete it;
	delete dt;
}

int command_kddtable(int argc, const char * argv[])
{
	int r;
	size_t count = 0;
	bool verbose = false;
	params config;
	
	if(argc > 1 && !strcmp(argv[1], "-v"))
	{
		verbose = true;
		argc--;
		argv++;
	}
	if(argc > 1)
		count = atoi(argv[1]);
	if(!count)
		count = 2000000;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_interval" int 2
		]
		"divider_0" int 8
		"divider_1" int 11
		"divider_2" int 12
		"divider_3" int 15
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	iterator_test("keydiv_dtable", "kddt_test", config, count, verbose);
	
	return 0;
}

int command_ctable(int argc, const char * argv[])
{
	int r;
	ctable * sct;
	
	params config;
	r = params::parse(LITERAL(
	config [
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_interval" int 1
			"combine_interval" int 1
			"combine_count" int 3
		]
		"columns" int 3
		"column0_name" string "hello"
		"column1_name" string "world"
		"column2_name" string "foo"
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = ctable_factory::setup("simple_ctable", AT_FDCWD, "msct_test", config, dtype::UINT32);
	printf("setup = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sct = ctable_factory::load("simple_ctable", AT_FDCWD, "msct_test", config);
	printf("load = %p\n", sct);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = sct->insert(8u, "hello", blob("icanhas"));
	printf("sct->insert(8, hello) = %d\n", r);
	run_iterator(sct);
	r = sct->insert(8u, "world", blob("cheezburger"));
	printf("sct->insert(8, world) = %d\n", r);
	run_iterator(sct);
	printf("Waiting 1 second for digest interval...\n");
	sleep(1);
	r = sct->maintain();
	printf("sct->maintain() = %d\n", r);
	run_iterator(sct);
	r = sct->remove(8u, "hello");
	printf("sct->remove(8, hello) = %d\n", r);
	run_iterator(sct);
	r = sct->insert(10u, "foo", blob("bar"));
	printf("sct->insert(10, foo) = %d\n", r);
	run_iterator(sct);
	r = sct->remove(8u);
	printf("sct->remove(8) = %d\n", r);
	run_iterator(sct);
	r = sct->insert(12u, "foo", blob("zot"));
	printf("sct->insert(12, foo) = %d\n", r);
	run_iterator(sct);
	printf("Waiting 1 second for digest interval...\n");
	sleep(1);
	r = sct->maintain();
	printf("sct->maintain() = %d\n", r);
	run_iterator(sct);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sct;
	
	return 0;
}

int command_cctable(int argc, const char * argv[])
{
	int r;
	ctable * ct;
	ctable::colval values[3] = {{0}, {1}, {2}};
	const ctable_factory * base = ctable_factory::lookup("column_ctable");
	blob first[8] = {"Amy", "Bill", "Charlie", "Diana", "Edward", "Flora", "Gail", "Henry"};
	blob last[6] = {"Nobel", "O'Toole", "Patterson", "Quayle", "Roberts", "Smith"};
	
	params config;
	r = params::parse(LITERAL(
	config [
		"columns" int 3
		"base" class(dt) simple_dtable
		"column0_name" string "last"
		"column1_name" string "first"
		"column2_name" string "state"
		"column2_base" class(dt) usstate_dtable
		"column2_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"hole_value" blob FE
				"dne_value" blob FF
			]
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = base->create(AT_FDCWD, "cctr_test", config, dtype::UINT32);
	printf("cct::create = %d\n", r);
	
	ct = base->open(AT_FDCWD, "cctr_test", config);
	printf("cct::open = %p\n", ct);
	delete ct;
	
	config = params();
	r = params::parse(LITERAL(
	config [
		"columns" int 3
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_interval" int 2
			"combine_interval" int 8
			"combine_count" int 6
		]
		"column0_name" string "last"
		"column1_name" string "first"
		"column2_name" string "state"
		"column2_base" class(dt) managed_dtable
		"column2_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) usstate_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"base_config" config [
						"hole_value" blob FE
						"dne_value" blob FF
					]
					"passthrough_value" blob FD
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob FD
			]
			"digest_interval" int 2
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	
	r = base->create(AT_FDCWD, "cctw_test", config, dtype::UINT32);
	printf("cct::create = %d\n", r);
	
	ct = base->open(AT_FDCWD, "cctw_test", config);
	printf("cct::open = %p\n", ct);
	for(uint32_t i = 0; i < 20; i++)
	{
		values[0].value = last[rand() % 6];
		values[1].value = first[rand() % 8];
		values[2].value = usstate_dtable::state_codes[rand() % USSTATE_COUNT];
		if(!(rand() % 10))
			values[2].value = "Timbuktu";
		r = ct->insert(i, values, 3);
	}
	run_iterator(ct);
	delete ct;
	
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	printf("Waiting 3 seconds for digest interval...\n");
	sleep(3);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	ct = base->open(AT_FDCWD, "cctw_test", config);
	printf("cct::open = %p\n", ct);
	run_iterator(ct);
	delete ct;
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	return 0;
}

#define CONS_TEST_COLS 50
#define CONS_TEST_ROWS 500
#define CONS_TEST_SUM 100000000

#define CONS_TEST_CELLS (CONS_TEST_COLS * CONS_TEST_ROWS)
#define CONS_TEST_INITIAL (CONS_TEST_SUM / CONS_TEST_CELLS)

#define CONS_TEST_SPREAD 100
#define CONS_TEST_TX_SIZE 2000
#define CONS_TEST_ITERATIONS 1000000

#define CONS_TEST_BUCKET_SIZE 100
#define CONS_TEST_BUCKET_HEIGHT 20
#define CONS_TEST_BUCKET_ZOOM 4
#define CONS_TEST_BUCKET_MAX (CONS_TEST_INITIAL * 2)
#define CONS_TEST_BUCKETS ((CONS_TEST_BUCKET_MAX + CONS_TEST_BUCKET_SIZE - 1) / CONS_TEST_BUCKET_SIZE)
#define CONS_TEST_BUCKET_SCALE (CONS_TEST_CELLS / CONS_TEST_BUCKET_HEIGHT / CONS_TEST_BUCKET_ZOOM)

static bool consistency_check(const ctable * ct, size_t * buckets)
{
	ctable::iter * iter = ct->iterator();
	size_t total_entries = 0;
	uint32_t key_total = 0;
	uint32_t value_total = 0;
	memset(buckets, 0, sizeof(*buckets) * CONS_TEST_BUCKETS);
	while(iter->valid())
	{
		uint32_t number;
		dtype key = iter->key();
		blob value = iter->value();
		iter->next();
		assert(key.type == dtype::UINT32);
		total_entries++;
		key_total += key.u32;
		assert(value.size() == sizeof(uint32_t));
		number = value.index<uint32_t>(0);
		value_total += number;
		if(number > CONS_TEST_BUCKET_MAX)
			number = CONS_TEST_BUCKET_MAX;
		buckets[number / CONS_TEST_BUCKET_SIZE]++;
	}
	delete iter;
	if(total_entries != CONS_TEST_CELLS)
		return false;
	/* try to make sure the iterator is working properly while we're at it */
	if(key_total != (CONS_TEST_ROWS - 1) * CONS_TEST_ROWS / 2 * CONS_TEST_COLS)
		return false;
	return value_total == CONS_TEST_SUM;
}

static void print_buckets(const size_t * buckets)
{
	for(size_t j = CONS_TEST_BUCKET_HEIGHT; j; j--)
	{
		for(size_t i = 0; i < CONS_TEST_BUCKETS; i++)
			printf("%c", (buckets[i] >= j * CONS_TEST_BUCKET_SCALE) ? '*' : ' ');
		printf("\n");
	}
}

int command_consistency(int argc, const char * argv[])
{
	ctable * ct;
	params config;
	size_t buckets[CONS_TEST_BUCKETS];
	uint32_t initial = CONS_TEST_INITIAL;
	blob value(sizeof(initial), &initial);
	ctable::colval values[CONS_TEST_COLS] = {{0}, { 1}, { 2}, { 3}, { 4}, { 5}, { 6}, { 7}, { 8}, { 9},
	                                        {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19},
	                                        {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29},
	                                        {30}, {31}, {32}, {33}, {34}, {35}, {36}, {37}, {38}, {39},
	                                        {40}, {41}, {42}, {43}, {44}, {45}, {46}, {47}, {48}, {49}};
	int r = params::parse(LITERAL(
	config [
		"columns" int 50
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) array_dtable
			"digest_interval" int 5
			"combine_interval" int 20
			"combine_count" int 6
		]
		"column0_name" string "0" "column1_name" string "1" "column2_name" string "2" "column3_name" string "3" "column4_name" string "4"
		"column5_name" string "5" "column6_name" string "6" "column7_name" string "7" "column8_name" string "8" "column9_name" string "9"
		"column10_name" string "10" "column11_name" string "11" "column12_name" string "12" "column13_name" string "13" "column14_name" string "14"
		"column15_name" string "15" "column16_name" string "16" "column17_name" string "17" "column18_name" string "18" "column19_name" string "19"
		"column20_name" string "20" "column21_name" string "21" "column22_name" string "22" "column23_name" string "23" "column24_name" string "24"
		"column25_name" string "25" "column26_name" string "26" "column27_name" string "27" "column28_name" string "28" "column29_name" string "29"
		"column30_name" string "30" "column31_name" string "31" "column32_name" string "32" "column33_name" string "33" "column34_name" string "34"
		"column35_name" string "35" "column36_name" string "36" "column37_name" string "37" "column38_name" string "38" "column39_name" string "39"
		"column40_name" string "40" "column41_name" string "41" "column42_name" string "42" "column43_name" string "43" "column44_name" string "44"
		"column45_name" string "45" "column46_name" string "46" "column47_name" string "47" "column48_name" string "48" "column49_name" string "49"
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	if(argc > 1 && !strcmp(argv[1], "check"))
	{
		r = tx_start();
		printf("tx_start = %d\n", r);
		
		ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config);
		printf("load = %p\n", ct);
		
		printf("Consistency check: ");
		fflush(stdout);
		printf("%s\n", consistency_check(ct, buckets) ? "OK!" : "failed.");
		print_buckets(buckets);
		
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		return 0;
	}
	
	for(size_t i = 0; i < 50; i++)
		values[i].value = value;
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	
	r = ctable_factory::setup("column_ctable", AT_FDCWD, "cons_test", config, dtype::UINT32);
	printf("setup = %d\n", r);
	
	ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config);
	printf("load = %p\n", ct);
	for(uint32_t i = 0; i < 500; i++)
	{
		r = ct->insert(i, values, CONS_TEST_COLS);
		assert(r >= 0);
	}
	delete ct;
	
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	
	ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config);
	printf("load = %p\n", ct);
	
	printf("Consistency check: ");
	fflush(stdout);
	printf("%s\n", consistency_check(ct, buckets) ? "OK!" : "failed.");
	print_buckets(buckets);
	
	for(uint32_t i = 0; i < CONS_TEST_ITERATIONS; i++)
	{
		char column[24];
		uint32_t key = rand() % CONS_TEST_ROWS;
		sprintf(column, "%d", rand() % CONS_TEST_COLS);
		blob value = ct->find(key, column);
		assert(value.size() == sizeof(uint32_t));
		initial = value.index<uint32_t>(0) - CONS_TEST_SPREAD;
		value = blob(sizeof(initial), &initial);
		r = ct->insert(key, column, value);
		assert(r >= 0);
		for(int j = 0; j < CONS_TEST_SPREAD; j++)
		{
			key = rand() % CONS_TEST_ROWS;
			sprintf(column, "%d", rand() % CONS_TEST_COLS);
			value = ct->find(key, column);
			assert(value.size() == sizeof(uint32_t));
			initial = value.index<uint32_t>(0) + 1;
			value = blob(sizeof(initial), &initial);
			r = ct->insert(key, column, value);
			assert(r >= 0);
		}
		if(!(i % 1000))
			ct->maintain();
		if((i % CONS_TEST_TX_SIZE) == CONS_TEST_TX_SIZE - 1)
		{
			r = tx_end(0);
			assert(r >= 0);
			r = tx_start();
			assert(r >= 0);
			printf(".");
			fflush(stdout);
		}
		if((i % (CONS_TEST_ITERATIONS / 10)) == CONS_TEST_ITERATIONS / 10 - 1)
			printf(" %d%% done\n", (i + 1) / (CONS_TEST_ITERATIONS / 100));
	}
	
	printf("Consistency check: ");
	fflush(stdout);
	printf("%s\n", consistency_check(ct, buckets) ? "OK!" : "failed.");
	print_buckets(buckets);
	
	printf("Waiting 5 seconds for digest interval...\n");
	sleep(5);
	ct->maintain();
	delete ct;
	
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	return 0;
}

#include <signal.h>

static bool durability_stop = false;

static void durable_death(int signal)
{
	durability_stop = true;
}

int command_durability(int argc, const char * argv[])
{
	params config;
	tx_id transaction_id;
	uint32_t tx_seq;
	dtable * dt;
	bool check;
	int r;
	
	check = argc > 1 && !strcmp(argv[1], "check");
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 2
		"combine_interval" int 4
		"combine_count" int 4
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	
	if(!check)
	{
		r = dtable_factory::setup("managed_dtable", AT_FDCWD, "dura_test", config, dtype::UINT32);
		printf("dtable::create = %d\n", r);
	}
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "dura_test", config);
	printf("dtable_factory::load = %p\n", dt);
	
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	if(check)
	{
		blob value = dt ? dt->find(0u) : blob();
		if(value.exists())
		{
			tx_seq = value.index<uint32_t>(0);
			printf("Transaction ID: %u\n", tx_seq);
		}
		else
			printf("No transactions!\n");
	}
	else
	{
		void (*original_death)(int);
		
		durability_stop = false;
		original_death = signal(SIGALRM, durable_death);
		
		for(tx_seq = 0; tx_seq < 10000; tx_seq++)
		{
			bool stop = durability_stop;
			if(stop)
				printf("Durability test committing transaction ID: %u\n", tx_seq);
			
			r = tx_start();
			assert(r >= 0);
			
			r = dt->insert(0u, blob(sizeof(tx_seq), &tx_seq));
			assert(r >= 0);
			
			if((tx_seq % 1000) == 999)
			{
				r = dt->maintain();
				assert(r >= 0);
			}
			
			transaction_id = tx_end(1);
			r = tx_sync(transaction_id);
			assert(r >= 0);
			
			if(stop && original_death != SIG_DFL && original_death != SIG_IGN)
				original_death(SIGALRM);
		}
	}
	
	delete dt;
	
	return 0;
}

/* makes a dtype of the requested type from the numeric key */
static dtype idtype(uint32_t value, dtype::ctype key_type)
{
	switch(key_type)
	{
		case dtype::UINT32:
			return dtype(value);
		case dtype::DOUBLE:
			return dtype((double) value);
		case dtype::STRING:
			char string[32];
			sprintf(string, "%u", value);
			return dtype(string);
		case dtype::BLOB:
			/* endianness-sensitive... whatever */
			return dtype(blob(sizeof(value), &value));
	}
	abort();
}

#define PRINT_FAIL printf("\a\e[1m\e[31m    **** ERROR ****  (%s:%d)\e[0m\n", __FILE__, __LINE__)
#define EXPECT_NOFAIL(label, value) do { printf(label " = %d\n", value); if(value < 0) PRINT_FAIL; } while(0)
#define EXPECT_SIZET(label, expect, test) do { size_t __value = test; printf(label " = %zu (expect %zu)\n", __value, (size_t) expect); if(__value != (expect)) PRINT_FAIL; } while(0)
int command_rollover(int argc, const char * argv[])
{
	sys_journal * sysj;
	journal_dtable * normal;
	journal_dtable * temporary;
	sys_journal::listener_id normal_id, temp_id;
	dtype::ctype key_type = dtype::UINT32;
	bool use_reverse = false;
	const char * dir;
	int r, fd;
	
	reverse_blob_comparator reverse;
	/* let reverse know it's on the stack */
	reverse.on_stack();
	
	if(argc > 1 && !strcmp(argv[1], "-b"))
	{
		argc--; argv++;
		key_type = dtype::BLOB;
		if(argc > 1 && !strcmp(argv[1], "-r"))
		{
			argc--; argv++;
			use_reverse = true;
		}
	}
	
	dir = (argc > 1) ? argv[1] : ".";
	fd = open(dir, O_RDONLY);
	if(fd < 0)
	{
		perror(dir);
		return fd;
	}
	sysj = new sys_journal;
	EXPECT_SIZET("initial total", 0, journal_dtable::warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, true);
	EXPECT_NOFAIL("sysj init", r);
	normal_id = sys_journal::get_unique_id(false);
	temp_id = sys_journal::get_unique_id(true);
	printf("normal = %d, temp = %d\n", normal_id, temp_id);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	normal = journal_dtable::warehouse.obtain(normal_id, key_type, sysj);
	temporary = journal_dtable::warehouse.obtain(temp_id, key_type, sysj);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 2, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		r = normal->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
		r = temporary->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("temp set_cmp", r);
	}
	
	/* first test: insert records to normal and temporary, restart and see that they're gone */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = normal->insert(idtype(10, key_type), "key 10");
	EXPECT_NOFAIL("normal insert(10)", r);
	r = temporary->insert(idtype(20, key_type), "key 20");
	EXPECT_NOFAIL("temp insert(20)", r);
	delete temporary;
	delete normal;
	sysj->deinit();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, journal_dtable::warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, false);
	EXPECT_NOFAIL("sysj init", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = journal_dtable::warehouse.lookup(normal_id);
	temporary = journal_dtable::warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		r = normal->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	
	/* next test: create another temporary ID and roll it over, restart and see that it works */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	temp_id = sys_journal::get_unique_id(true);
	printf("temp = %d\n", temp_id);
	temporary = journal_dtable::warehouse.obtain(temp_id, key_type, sysj);
	printf("temp = %p\n", temporary);
	EXPECT_SIZET("total", 2, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		r = temporary->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("temp set_cmp", r);
	}
	r = temporary->insert(idtype(30, key_type), "key 30");
	EXPECT_NOFAIL("temp insert(30)", r);
	EXPECT_SIZET("normal size", 1, normal->size());
	r = sysj->rollover(temporary, normal);
	EXPECT_NOFAIL("sysj rollover", r);
	r = temporary->rollover(normal);
	EXPECT_NOFAIL("memory rollover", r);
	EXPECT_SIZET("normal size", 2, normal->size());
	delete temporary;
	delete normal;
	sysj->deinit();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, journal_dtable::warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, false);
	EXPECT_NOFAIL("sysj init", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = journal_dtable::warehouse.lookup(normal_id);
	temporary = journal_dtable::warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	
	/* final test: check that rollover records clobber existing records */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	temp_id = sys_journal::get_unique_id(true);
	printf("temp = %d\n", temp_id);
	temporary = journal_dtable::warehouse.obtain(temp_id, key_type, sysj);
	printf("temp = %p\n", temporary);
	EXPECT_SIZET("total", 2, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		r = temporary->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("temp set_cmp", r);
	}
	r = temporary->insert(idtype(10, key_type), "temp 10");
	EXPECT_NOFAIL("temp insert(10)", r);
	EXPECT_SIZET("temp size", 1, temporary->size());
	r = normal->insert(idtype(10, key_type), "normal 10");
	EXPECT_NOFAIL("normal insert(10)", r);
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 9, normal->find(idtype(10, key_type)).size());
	r = sysj->rollover(temporary, normal);
	EXPECT_NOFAIL("sysj rollover", r);
	r = temporary->rollover(normal);
	EXPECT_NOFAIL("memory rollover", r);
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	delete temporary;
	delete normal;
	sysj->deinit();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, journal_dtable::warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, false);
	EXPECT_NOFAIL("sysj init", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = journal_dtable::warehouse.lookup(normal_id);
	temporary = journal_dtable::warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	
	/* filter and check the results */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	delete normal;
	r = sysj->filter();
	EXPECT_NOFAIL("filter", r);
	sysj->deinit();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, false);
	EXPECT_NOFAIL("sysj init", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = journal_dtable::warehouse.lookup(normal_id);
	temporary = journal_dtable::warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, journal_dtable::warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(&reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	
	/* discard normal, filter and check the results */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	normal->deinit(true);
	delete normal;
	r = sysj->filter();
	EXPECT_NOFAIL("filter", r);
	sysj->deinit();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->init(fd, "test_journal", &journal_dtable::warehouse, false);
	EXPECT_NOFAIL("sysj init", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = journal_dtable::warehouse.lookup(normal_id);
	temporary = journal_dtable::warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 0, journal_dtable::warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj->deinit(true);
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	return 0;
}

int command_stable(int argc, const char * argv[])
{
	int r;
	simple_stable * sst;
	params config;
	
	r = params::parse(LITERAL(
	config [
		"meta" class(dt) cache_dtable
		"meta_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
				"digest_interval" int 2
			]
		]
		"data" class(ct) simple_ctable
		"data_config" config [
			"base" class(dt) cache_dtable
			"base_config" config [
				"cache_size" int 40000
				"base" class(dt) managed_dtable
				"base_config" config [
					"base" class(dt) btree_dtable
					"base_config" config [
						"base" class(dt) simple_dtable
					]
					"fastbase" class(dt) simple_dtable
					"digest_interval" int 2
				]
			]
			"columns" int 3
			"column0_name" string "twice"
			"column1_name" string "funky"
			"column2_name" string "zapf"
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "msst_test", config, dtype::UINT32);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "msst_test", config);
	printf("sst->init = %d\n", r);
	r = tx_start();
	printf("tx_start = %d\n", r);
	run_iterator(sst);
	r = sst->insert(5u, "twice", 10u);
	printf("sst->insert(5, twice) = %d\n", r);
	run_iterator(sst);
	r = sst->insert(6u, "funky", "face");
	printf("sst->insert(6, funky) = %d\n", r);
	run_iterator(sst);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "msst_test", config);
	printf("sst->init = %d\n", r);
	r = tx_start();
	printf("tx_start = %d\n", r);
	run_iterator(sst);
	r = sst->insert(6u, "twice", 12u);
	printf("sst->insert(5, twice) = %d\n", r);
	run_iterator(sst);
	r = sst->insert(5u, "zapf", "dingbats");
	printf("sst->insert(6, zapf) = %d\n", r);
	run_iterator(sst);
	r = sst->remove(6u);
	printf("sst->remove(6) = %d\n", r);
	run_iterator(sst);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sst;
	
	printf("Waiting 3 seconds for digest interval...\n");
	sleep(3);
	sst = new simple_stable;
	/* must start the transaction first since it will do maintenance */
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = sst->init(AT_FDCWD, "msst_test", config);
	printf("sst->init = %d\n", r);
	r = sst->maintain();
	printf("sst->maintain() = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sst;
	
	return 0;
}

int command_iterator(int argc, const char * argv[])
{
	int r;
	size_t count = 0;
	bool verbose = false;
	params config;
	
	if(argc > 1 && !strcmp(argv[1], "-v"))
	{
		verbose = true;
		argc--;
		argv++;
	}
	if(argc > 1)
		count = atoi(argv[1]);
	if(!count)
		count = 2000000;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 2
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	iterator_test("managed_dtable", "iter_test", config, count, verbose);
	
	return 0;
}

int command_bdbtest(int argc, const char * argv[])
{
	/* TODO: this isn't the complete bdb test; we need to try different durability checks */
	const uint32_t KEYSIZE = 8;
	const uint32_t VALSIZE = 32;
	struct timeval start, end;
	
	int r;
	sys_journal::listener_id jid;
	journal_dtable * jdt;
	
	char keybuf[KEYSIZE], valbuf[VALSIZE];
	memset(keybuf, 'a', KEYSIZE);
	memset(valbuf, 'b', VALSIZE);
	
	blob key(KEYSIZE, keybuf);
	blob value(VALSIZE, valbuf);
	
	printf("BerkeleyDB test timing!\n");
	gettimeofday(&start, NULL);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	jid = sys_journal::get_unique_id();
	if(jid == sys_journal::NO_ID)
		return -EBUSY;
	jdt = journal_dtable::create(dtype::BLOB, jid);
	printf("jdt = %p\n", jdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	for(int i = 0; i < 1000000; i++)
	{
		r = tx_start();
		if(r < 0)
		{
			printf("TX error\n");
			break;
		}
		r = jdt->insert(dtype(key), value);
		assert(r >= 0);
		if((i % 100000) == 99999)
		{
			gettimeofday(&end, NULL);
			end.tv_sec -= start.tv_sec;
			if(end.tv_usec < start.tv_usec)
			{
				end.tv_usec += 1000000;
				end.tv_sec--;
			}
			end.tv_usec -= start.tv_usec;
			printf("%d%% done after %d.%06d seconds.\n", (i + 1) / 10000, (int) end.tv_sec, (int) end.tv_usec);
			fflush(stdout);
		}
		r = tx_end(0);
		if(r < 0)
		{
			printf("TX error\n");
			break;
		}
	}
	
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("Timing finished! %d.%06d seconds elapsed.\n", (int) end.tv_sec, (int) end.tv_usec);
	
	tx_start();
	jdt->deinit(true);
	delete jdt;
	tx_end(0);
	return 0;
}

#define ROW_COUNT 50000
#define DT_ROW_COUNT 200000

int command_blob_cmp(int argc, const char * argv[])
{
	int r;
	struct timeval start, end;
	reverse_blob_comparator reverse;
	
	/* let reverse know it's on the stack */
	reverse.on_stack();
	
	if(argc < 2 || strcmp(argv[1], "perf"))
	{
		sys_journal::listener_id jid;
		journal_dtable * jdt;
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		jid = sys_journal::get_unique_id();
		if(jid == sys_journal::NO_ID)
			return -EBUSY;
		jdt = journal_dtable::create(dtype::BLOB, jid);
		printf("jdt = %p\n", jdt);
		r = jdt->set_blob_cmp(&reverse);
		printf("jdt->set_blob_cmp = %d\n", r);
		for(int i = 0; i < 10; i++)
		{
			uint32_t keydata = rand();
			uint8_t valuedata = i;
			blob key(sizeof(keydata), &keydata);
			blob value(sizeof(valuedata), &valuedata);
			jdt->insert(dtype(key), value);
		}
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
		run_iterator(jdt);
		
		r = jdt->reinit(jid, false);
		printf("jdt->reinit = %d\n", r);
		printf("current expected comparator: %s\n", (const char *) jdt->get_cmp_name());
		
		run_iterator(jdt);
		
		r = sys_journal::get_global_journal()->get_entries(jdt);
		printf("get_entries = %d (expect %d)\n", r, -EBUSY);
		if(r == -EBUSY)
		{
			printf("expect comparator: %s\n", (const char *) jdt->get_cmp_name());
			jdt->set_blob_cmp(&reverse);
			r = sys_journal::get_global_journal()->get_entries(jdt);
			printf("get_entries = %d\n", r);
		}
		
		run_iterator(jdt);
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		r = jdt->reinit(jid);
		printf("jdt->reinit = %d\n", r);
		jdt->deinit(true);
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
		delete jdt;
		return 0;
	}
	
	/* now do a test with stables */
	simple_stable * sst;
	params config;
	istr column("sum");
	uint32_t sum = 0, check = 0;
	dtype old_key(0u);
	bool first = true;
	stable::iter * iter;
	
	r = params::parse(LITERAL(
	config [
		"meta" class(dt) cache_dtable
		"meta_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
				"digest_interval" int 2
				"combine_interval" int 8
				"combine_count" int 6
			]
		]
		"data" class(ct) simple_ctable
		"data_config" config [
			"base" class(dt) cache_dtable
			"base_config" config [
				"cache_size" int 40000
				"base" class(dt) managed_dtable
				"base_config" config [
					"base" class(dt) simple_dtable
					"digest_interval" int 2
					"combine_interval" int 8
					"combine_count" int 6
				]
			]
			"columns" int 1
			"column0_name" string "sum"
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "cmp_test", config, dtype::BLOB);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmp_test", config);
	printf("sst->init = %d\n", r);
	r = sst->set_blob_cmp(&reverse);
	printf("sst->set_blob_cmp = %d\n", r);
	
	printf("Start timing! (5000000 reverse blob key inserts to %d rows)\n", ROW_COUNT);
	gettimeofday(&start, NULL);
	
	for(uint32_t i = 0; i < 5000000; i++)
	{
		uint32_t keydata = rand() % ROW_COUNT;
		blob key(sizeof(keydata), &keydata);
		dtype current(0u);
		if(!(i % 1000))
		{
			r = tx_start();
			if(r < 0)
				goto fail_tx_start;
		}
		if(sst->find(key, column, &current))
			current.u32 += i;
		else
			current.u32 = i;
		r = sst->insert(key, column, current);
		if(r < 0)
			goto fail_insert;
		if((i % 500000) == 499999)
		{
			gettimeofday(&end, NULL);
			end.tv_sec -= start.tv_sec;
			if(end.tv_usec < start.tv_usec)
			{
				end.tv_usec += 1000000;
				end.tv_sec--;
			}
			end.tv_usec -= start.tv_usec;
			printf("%d%% done after %d.%06d seconds.\n", (i + 1) / 50000, (int) end.tv_sec, (int) end.tv_usec);
			fflush(stdout);
		}
		if((i % 10000) == 9999)
		{
			r = sst->maintain();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 500000) == 499999)
		{
			r = sys_journal::get_global_journal()->filter();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 1000) == 999)
		{
			r = tx_end(0);
			assert(r >= 0);
		}
		/* this will overflow, but that's OK */
		sum += i;
	}
	
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("Timing finished! %d.%06d seconds elapsed.\n", (int) end.tv_sec, (int) end.tv_usec);
	printf("Average: %"PRIu64" inserts/second\n", 5000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	delete sst;
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmp_test", config);
	printf("sst->init = %d\n", r);
	r = sst->set_blob_cmp(&reverse);
	printf("sst->set_blob_cmp = %d\n", r);
	r = sst->maintain();
	printf("sst->maintain = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	printf("Verifying writes... ");
	fflush(stdout);
	iter = sst->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		dtype value = iter->value();
		if(!first)
		{
			if(key.compare(old_key) >= 0)
			{
				printf("key does not decrease (");
				print(key);
				printf(" >= ");
				print(old_key);
				printf(") ");
				break;
			}
		}
		else
			first = false;
		old_key = key;
		check += value.u32;
		iter->next();
	}
	delete iter;
	if(sum == check)
		printf("OK!\n");
	else
		printf("failed! (sum = %u, check = %u)\n", sum, check);
	
	delete sst;
	return (sum == check) ? 0 : -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete sst;
	return r;
}

static const istr column_names[] = {"c_one", "c_two", "c_three", "c_four", "c_five"};
#define COLUMN_NAMES (sizeof(column_names) / sizeof(column_names[0]))

static int command_performance_stable(int argc, const char * argv[])
{
	int r;
	simple_stable * sst;
	stable::iter * iter;
	struct timeval start, end;
	uint32_t table_copy[ROW_COUNT][COLUMN_NAMES];
	params config;
	
	r = params::parse(LITERAL(
	config [
		"meta" class(dt) cache_dtable
		"meta_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
				"digest_interval" int 2
				"combine_interval" int 8
				"combine_count" int 6
			]
		]
		"data" class(ct) simple_ctable
		"data_config" config [
			"base" class(dt) cache_dtable
			"base_config" config [
				"cache_size" int 40000
				"base" class(dt) managed_dtable
				"base_config" config [
					"base" class(dt) btree_dtable
					"base_config" config [
						"base" class(dt) simple_dtable
					]
					"fastbase" class(dt) simple_dtable
					"digest_interval" int 2
					"combine_interval" int 8
					"combine_count" int 6
				]
			]
			"columns" int 5
			"column0_name" string "c_one"
			"column1_name" string "c_two"
			"column2_name" string "c_three"
			"column3_name" string "c_four"
			"column4_name" string "c_five"
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "perf_test", config, dtype::UINT32);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perf_test", config);
	printf("sst->init = %d\n", r);
	
	for(uint32_t i = 0; i < ROW_COUNT; i++)
		for(uint32_t j = 0; j < COLUMN_NAMES; j++)
			table_copy[i][j] = (uint32_t) -1;
	
	printf("Start timing! (2000000 inserts to %d rows)\n", ROW_COUNT);
	gettimeofday(&start, NULL);
	
	for(int i = 0; i < 2000000; i++)
	{
		uint32_t row = rand() % ROW_COUNT;
		uint32_t column = rand() % COLUMN_NAMES;
		if(!(i % 1000))
		{
			r = tx_start();
			if(r < 0)
				goto fail_tx_start;
		}
		do {
			table_copy[row][column] = rand();
		} while(table_copy[row][column] == (uint32_t) -1);
		r = sst->insert(row, column_names[column], table_copy[row][column]);
		if(r < 0)
			goto fail_insert;
		if((i % 200000) == 199999)
		{
			gettimeofday(&end, NULL);
			end.tv_sec -= start.tv_sec;
			if(end.tv_usec < start.tv_usec)
			{
				end.tv_usec += 1000000;
				end.tv_sec--;
			}
			end.tv_usec -= start.tv_usec;
			printf("%d%% done after %d.%06d seconds.\n", (i + 1) / 20000, (int) end.tv_sec, (int) end.tv_usec);
			fflush(stdout);
		}
		if((i % 10000) == 9999)
		{
			r = sst->maintain();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 500000) == 499999)
		{
			r = sys_journal::get_global_journal()->filter();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 1000) == 999)
		{
			r = tx_end(0);
			assert(r >= 0);
		}
	}
	
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("Timing finished! %d.%06d seconds elapsed.\n", (int) end.tv_sec, (int) end.tv_usec);
	printf("Average: %"PRIu64" inserts/second\n", 2000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perf_test", config);
	printf("sst->init = %d\n", r);
	
	printf("Verifying writes... ");
	fflush(stdout);
	for(uint32_t i = 0; i < ROW_COUNT; i++)
		for(uint32_t j = 0; j < COLUMN_NAMES; j++)
		{
			dtype value(0u);
			if(sst->find(i, column_names[j], &value))
			{
				if(value.type != dtype::UINT32)
					goto fail_verify;
				if(table_copy[i][j] != value.u32)
					goto fail_verify;
			}
			else if(table_copy[i][j] != (uint32_t) -1)
				goto fail_verify;
		}
	printf("OK!\n");
	
	if(argc > 1 && !strcmp(argv[1], "seek"))
	{
		printf("Checking seeking (100000 seeks)... ");
		fflush(stdout);
		iter = sst->iterator();
		for(int i = 0; i < 100000; i++)
		{
			uint32_t row = rand() % ROW_COUNT;
			bool found = iter->seek(row);
			uint32_t expected_sum = 0;
			int expected_count = 0;
			for(uint32_t col = 0; col < COLUMN_NAMES; col++)
				if(table_copy[row][col] != (uint32_t) -1)
				{
					expected_count++;
					expected_sum += table_copy[row][col];
				}
			if(found == !expected_count)
				goto fail_iter;
			if(found)
			{
				if(!iter->valid())
					goto fail_iter;
				dtype key = iter->key();
				if(key.type != dtype::UINT32 || key.u32 != row)
					goto fail_iter;
				while(key.u32 == row)
				{
					dtype value = iter->value();
					if(value.type != dtype::UINT32)
						goto fail_iter;
					expected_count--;
					expected_sum -= value.u32;
					if(!iter->next())
						break;
					assert(iter->valid());
					key = iter->key();
				}
				if(expected_count || expected_sum)
					goto fail_iter;
			}
			else if(iter->valid())
			{
				dtype key = iter->key();
				if(key.type != dtype::UINT32 || key.u32 == row)
					goto fail_iter;
			}
		}
		delete iter;
		printf("OK!\n");
	}
	
	delete sst;
	return 0;
	
fail_iter:
	delete iter;
fail_verify:
	printf("failed!\n");
	delete sst;
	return -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete sst;
	return r;
}

static int command_performance_dtable(int argc, const char * argv[])
{
	int r;
	dtable * dt;
	dtable::iter * iter;
	struct timeval start, end;
	uint32_t table_copy[DT_ROW_COUNT];
	params config;
	
	r = params::parse(LITERAL(
	config [
		"cache_size" int 40000
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) btree_dtable
			"base_config" config [
				"base" class(dt) simple_dtable
			]
			"fastbase" class(dt) simple_dtable
			"digest_interval" int 2
			"combine_interval" int 8
			"combine_count" int 6
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = dtable_factory::setup("cache_dtable", AT_FDCWD, "dtpf_test", config, dtype::UINT32);
	printf("dtable_factory::setup = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	dt = dtable_factory::load("cache_dtable", AT_FDCWD, "dtpf_test", config);
	printf("dtable_factory::load = %p\n", dt);
	
	for(uint32_t i = 0; i < DT_ROW_COUNT; i++)
		table_copy[i] = (uint32_t) -1;
	
	printf("Start timing! (10000000 inserts to %d rows)\n", DT_ROW_COUNT);
	gettimeofday(&start, NULL);
	
	for(int i = 0; i < 10000000; i++)
	{
		uint32_t row = rand() % DT_ROW_COUNT;
		if(!(i % 1000))
		{
			r = tx_start();
			if(r < 0)
				goto fail_tx_start;
		}
		do {
			table_copy[row] = rand();
		} while(table_copy[row] == (uint32_t) -1);
		r = dt->insert(row, blob(sizeof(table_copy[row]), &table_copy[row]));
		if(r < 0)
			goto fail_insert;
		if((i % 1000000) == 999999)
		{
			gettimeofday(&end, NULL);
			end.tv_sec -= start.tv_sec;
			if(end.tv_usec < start.tv_usec)
			{
				end.tv_usec += 1000000;
				end.tv_sec--;
			}
			end.tv_usec -= start.tv_usec;
			printf("%d%% done after %d.%06d seconds.\n", (i + 1) / 100000, (int) end.tv_sec, (int) end.tv_usec);
			fflush(stdout);
		}
		if((i % 10000) == 9999)
		{
			r = dt->maintain();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 500000) == 499999)
		{
			r = sys_journal::get_global_journal()->filter();
			if(r < 0)
				goto fail_maintain;
		}
		if((i % 1000) == 999)
		{
			r = tx_end(0);
			assert(r >= 0);
		}
	}
	
	gettimeofday(&end, NULL);
	end.tv_sec -= start.tv_sec;
	if(end.tv_usec < start.tv_usec)
	{
		end.tv_usec += 1000000;
		end.tv_sec--;
	}
	end.tv_usec -= start.tv_usec;
	printf("Timing finished! %d.%06d seconds elapsed.\n", (int) end.tv_sec, (int) end.tv_usec);
	printf("Average: %"PRIu64" inserts/second\n", 10000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	delete dt;
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	dt = dtable_factory::load("cache_dtable", AT_FDCWD, "dtpf_test", config);
	printf("dtable_factory::load = %p\n", dt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	printf("Verifying writes... ");
	fflush(stdout);
	for(uint32_t i = 0; i < DT_ROW_COUNT; i++)
	{
		blob value = dt->find(i);
		if(value.exists())
		{
			if(value.size() != sizeof(uint32_t))
				goto fail_verify;
			dtype typed(value, dtype::UINT32);
			if(table_copy[i] != typed.u32)
				goto fail_verify;
		}
		else if(table_copy[i] != (uint32_t) -1)
			goto fail_verify;
	}
	printf("OK!\n");
	
	if(argc > 1 && !strcmp(argv[1], "seek"))
	{
		printf("Checking seeking (100000 seeks)... ");
		fflush(stdout);
		iter = dt->iterator();
		for(int i = 0; i < 100000; i++)
		{
			uint32_t row = rand() % DT_ROW_COUNT;
			if(iter->seek(row))
			{
				blob value = iter->value();
				if(value.size() != sizeof(uint32_t))
					goto fail_iter;
				dtype typed(value, dtype::UINT32);
				if(table_copy[row] != typed.u32)
					goto fail_iter;
			}
			else if(table_copy[row] != (uint32_t) -1)
				goto fail_iter;
		}
		delete iter;
		printf("OK!\n");
	}
	
	delete dt;
	return 0;
	
fail_iter:
	delete iter;
fail_verify:
	printf("failed!\n");
	delete dt;
	return -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete dt;
	return r;
}

int command_performance(int argc, const char * argv[])
{
	if(argc > 1 && !strcmp(argv[1], "stable"))
		return command_performance_stable(argc - 1, &argv[1]);
	return command_performance_dtable(argc, argv);
}
