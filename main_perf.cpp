/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define __STDC_FORMAT_MACROS

#include "main.h"
#include "openat.h"
#include "transaction.h"

#include "util.h"
#include "sys_journal.h"
#include "journal_dtable.h"
#include "managed_dtable.h"
#include "simple_stable.h"
#include "reverse_blob_comparator.h"

#define ROW_COUNT 50000
#define DT_ROW_COUNT 200000

int command_abort(int argc, const char * argv[])
{
	int r;
	dtable * dt;
	params config;
	abortable_tx atx;
	sys_journal * sysj;
	journal_dtable::journal_dtable_warehouse warehouse;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 2
		"combine_interval" int 4
		"combine_count" int 4
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "abtx_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, true);
	EXPECT_NONULL("sysj spawn", sysj);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "abtx_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = dt->insert(1u, blob("A"));
	EXPECT_NOFAIL("dt->insert(1)", r);
	r = dt->insert(2u, blob("B"));
	EXPECT_NOFAIL("dt->insert(2)", r);
	run_iterator(dt);
	EXPECT_SIZET("key 1 size", 1, dt->find(1u).size());
	EXPECT_SIZET("key 2 size", 1, dt->find(2u).size());
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	EXPECT_SIZET("total", 1, warehouse.size());
	atx = dt->create_tx();
	EXPECT_NOTU32("atx", NO_ABORTABLE_TX, atx);
	EXPECT_SIZET("total", 2, warehouse.size());
	r = dt->insert(2u, blob("B (atx)"), true, atx);
	EXPECT_NOFAIL("dt->insert(2, atx)", r);
	r = dt->insert(3u, blob("C (atx)"), true, atx);
	EXPECT_NOFAIL("dt->insert(3, atx)", r);
	r = dt->insert(4u, blob("D (atx)"), true, atx);
	EXPECT_NOFAIL("dt->insert(4, atx)", r);
	run_iterator(dt);
	run_iterator(dt, atx);
	EXPECT_SIZET("key 1 size", 1, dt->find(1u).size());
	EXPECT_SIZET("key 2 size", 1, dt->find(2u).size());
	EXPECT_SIZET("key 3 size", 0, dt->find(3u).size());
	EXPECT_SIZET("key 4 size", 0, dt->find(4u).size());
	EXPECT_SIZET("key 1 atx size", 1, dt->find(1u, atx).size());
	EXPECT_SIZET("key 2 atx size", 7, dt->find(2u, atx).size());
	EXPECT_SIZET("key 3 atx size", 7, dt->find(3u, atx).size());
	EXPECT_SIZET("key 4 atx size", 7, dt->find(4u, atx).size());
	EXPECT_SIZET("total", 2, warehouse.size());
	dt->abort_tx(atx);
	printf("abort_tx\n");
	EXPECT_SIZET("total", 1, warehouse.size());
	run_iterator(dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	EXPECT_SIZET("total", 1, warehouse.size());
	atx = dt->create_tx();
	EXPECT_NOTU32("atx", NO_ABORTABLE_TX, atx);
	EXPECT_SIZET("total", 2, warehouse.size());
	r = dt->insert(2u, blob("B (atx2)"), true, atx);
	EXPECT_NOFAIL("dt->insert(2, atx2)", r);
	r = dt->insert(3u, blob("C (atx2)"), true, atx);
	EXPECT_NOFAIL("dt->insert(3, atx2)", r);
	r = dt->insert(3u, blob("C"), true);
	EXPECT_NOFAIL("dt->insert(3)", r);
	run_iterator(dt);
	run_iterator(dt, atx);
	EXPECT_SIZET("key 1 size", 1, dt->find(1u).size());
	EXPECT_SIZET("key 2 size", 1, dt->find(2u).size());
	EXPECT_SIZET("key 3 size", 1, dt->find(3u).size());
	EXPECT_SIZET("key 1 atx size", 1, dt->find(1u, atx).size());
	EXPECT_SIZET("key 2 atx size", 8, dt->find(2u, atx).size());
	EXPECT_SIZET("key 3 atx size", 8, dt->find(3u, atx).size());
	EXPECT_SIZET("total", 2, warehouse.size());
	r = dt->commit_tx(atx);
	EXPECT_NOFAIL("commit_tx", r);
	EXPECT_SIZET("total", 1, warehouse.size());
	run_iterator(dt);
	EXPECT_SIZET("key 1 size", 1, dt->find(1u).size());
	EXPECT_SIZET("key 2 size", 8, dt->find(2u).size());
	EXPECT_SIZET("key 3 size", 8, dt->find(3u).size());
	dt->destroy();
	delete sysj;
	EXPECT_SIZET("total", 0, warehouse.size());
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	/* restart everything and make sure it's all still correct */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, false);
	EXPECT_NONULL("sysj spawn", sysj);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "abtx_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	EXPECT_SIZET("total", 1, warehouse.size());
	run_iterator(dt);
	EXPECT_SIZET("key 1 size", 1, dt->find(1u).size());
	EXPECT_SIZET("key 2 size", 8, dt->find(2u).size());
	EXPECT_SIZET("key 3 size", 8, dt->find(3u).size());
	EXPECT_SIZET("key 4 size", 0, dt->find(4u).size());
	dt->destroy();
	sysj->deinit(true);
	delete sysj;
	EXPECT_SIZET("total", 0, warehouse.size());
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
	{
		/* run the performance test as well */
		bool use_atx = false;
		
		config = params();
		r = params::parse(LITERAL(
		config [
			"base" class(dt) simple_dtable
			"digest_interval" int 2
			"combine_interval" int 8
			"combine_count" int 6
		]), &config);
		EXPECT_NOFAIL("params::parse", r);
		config.print();
		printf("\n");
			
		do {
			printf("Abortable transaction performance test: use_atx = %d\n", use_atx);
			struct timeval start, end;
			size_t atx_count = use_atx ? 1 : 0;
			
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			sysj = sys_journal::spawn_init("test_journal", &warehouse, true);
			EXPECT_NONULL("sysj spawn", sysj);
			r = dtable_factory::setup("managed_dtable", AT_FDCWD, "abtx_perf", config, dtype::UINT32);
			EXPECT_NOFAIL("dtable_factory::setup", r);
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
			
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			dt = dtable_factory::load("managed_dtable", AT_FDCWD, "abtx_perf", config, sysj);
			EXPECT_NONULL("dtable_factory::load", dt);
			if(use_atx)
			{
				atx = dt->create_tx();
				EXPECT_NOTU32("atx", NO_ABORTABLE_TX, atx);
			}
			else
				atx = NO_ABORTABLE_TX;
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
			
			printf("Start timing! (10000000 inserts to %d rows)\n", DT_ROW_COUNT);
			gettimeofday(&start, NULL);
			
			for(int i = 0; i < 10000000; i++)
			{
				uint32_t row = rand() % DT_ROW_COUNT;
				uint32_t value = rand();
				if(!(i % 1000))
				{
					r = tx_start();
					if(r < 0)
					{
						EXPECT_NEVER("tx_start failure");
						break;
					}
				}
				r = dt->insert(row, blob(sizeof(value), &value));
				assert(r >= 0);
				if((i % 1000000) == 999999)
				{
					print_progress(&start, (i + 1) / 100000);
					fflush(stdout);
				}
				if(use_atx && !(rand() % 100))
				{
					r = dt->commit_tx(atx);
					if(r < 0)
					{
						EXPECT_NEVER("commit_tx failure");
						break;
					}
					atx = dt->create_tx();
					if(atx == NO_ABORTABLE_TX)
					{
						EXPECT_NEVER("create_tx failure");
						break;
					}
					atx_count++;
				}
				if((i % 10000) == 9999)
				{
					r = dt->maintain();
					if(r < 0)
					{
						EXPECT_NEVER("maintain failure");
						break;
					}
				}
				if((i % 500000) == 499999)
				{
					r = sysj->filter();
					if(r < 0)
					{
						EXPECT_NEVER("filter failure");
						break;
					}
				}
				if((i % 1000) == 999)
				{
					r = tx_end(0);
					if(r < 0)
					{
						EXPECT_NEVER("tx_end failure");
						break;
					}
				}
			}
			if(use_atx)
			{
				r = tx_start();
				EXPECT_NOFAIL("tx_start", r);
				r = dt->commit_tx(atx);
				EXPECT_NOFAIL("commit_tx", r);
				r = tx_end(0);
				EXPECT_NOFAIL("tx_end", r);
			}
			
			gettimeofday(&end, NULL);
			printf("Timing finished! ");
			print_elapsed(&start, &end, true);
			printf("Average: %"PRIu64" inserts/second\n", 10000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
			printf("Total abortable transactions: %zu\n", atx_count);
			
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			dt->destroy();
			sysj->deinit(true);
			delete sysj;
			util::rm_r(AT_FDCWD, "abtx_perf");
			EXPECT_SIZET("total", 0, warehouse.size());
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
		} while((use_atx = !use_atx));
	}
	
	return 0;
}

static int excp_perf(dtable * table)
{
	dtable::iter * it;
	blob fixed("aoife");
	blob exception("pandora");
	int sum[2] = {0, 0};
	struct timeval start;
	int r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
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
	
	wait_digest(2);
	
	printf("Maintaining... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	r = table->maintain();
	print_elapsed(&start);
	EXPECT_NOFAIL("maintain", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
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
	print_elapsed(&start);
	
	return 0;
	
fail_tx:
	tx_end(0);
fail:
	EXPECT_NEVER("fail!");
	return -1;
}

int command_edtable(int argc, const char * argv[])
{
	int r;
	blob fixed("fixed");
	blob exception("exception");
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = managed_dtable::create(AT_FDCWD, "excp_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = mdt->insert(0u, fixed);
	EXPECT_NOFAIL("mdt->insert", r);
	r = mdt->insert(1u, fixed);
	EXPECT_NOFAIL("mdt->insert", r);
	r = mdt->insert(3u, fixed);
	EXPECT_NOFAIL("mdt->insert", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	wait_digest(3);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config, sysj);
	EXPECT_NOFAIL("mdt->init", r);
	r = mdt->maintain();
	EXPECT_NOFAIL_COUNT("mdt->maintain", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	r = mdt->insert(2u, exception);
	EXPECT_NOFAIL("mdt->insert", r);
	r = mdt->insert(8u, exception);
	EXPECT_NOFAIL("mdt->insert", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	wait_digest(2);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "excp_test", config, sysj);
	EXPECT_NOFAIL("mdt->init", r);
	r = mdt->maintain();
	EXPECT_NOFAIL_COUNT("mdt->maintain", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
	{
		/* run the performance test as well */
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
		EXPECT_NOFAIL("params::parse", r);
		config.print();
		printf("\n");
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		r = managed_dtable::create(AT_FDCWD, "excp_perf", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable::create", r);
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		/* run a test with it */
		mdt = new managed_dtable;
		r = mdt->init(AT_FDCWD, "excp_perf", config, sysj);
		EXPECT_NOFAIL("mdt->init", r);
		excp_perf(mdt);
		mdt->destroy();
		
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
		EXPECT_NOFAIL("params::parse", r);
		config.print();
		printf("\n");
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		r = managed_dtable::create(AT_FDCWD, "exbl_perf", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable::create", r);
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		/* run the same test */
		mdt = new managed_dtable;
		r = mdt->init(AT_FDCWD, "exbl_perf", config, sysj);
		EXPECT_NOFAIL("mdt->init", r);
		excp_perf(mdt);
		mdt->destroy();
	}
	
	return 0;
}

static int ovdt_perf(dtable * table)
{
	struct timeval start;
	dtable::iter * it;
	
	printf("Random lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 2000000; i++)
	{
		uint32_t key = rand() % 4000000;
		blob value = table->find(key);
	}
	print_elapsed(&start);
	
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
	print_elapsed(&start);
	delete it;
	
	return 0;
}

int command_odtable(int argc, const char * argv[])
{
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "ovdt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "ovdt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
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
			EXPECT_NEVER("fail!");
			return -1;
		}
	}
	printf("done.\n");
	
	wait_digest(2);
	
	printf("Maintaining... ");
	fflush(stdout);
	r = dt->maintain();
	EXPECT_NOFAIL("done. r", r);
	/* insert one final key so that there's something in the journal for the overlay to look at */
	r = dt->insert(2000000u, fixed);
	EXPECT_NOFAIL("dt->insert", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	ovdt_perf(dt);
	dt->destroy();
	
	b = config.get("base", &base);
	assert(b);
	b = config.get("base_config", &base_config);
	assert(b);
	base_config.print();
	printf("\n");
	/* load the first disk dtable directly */
	dt = dtable_factory::load(base, AT_FDCWD, "ovdt_perf/md_data.0", base_config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	ovdt_perf(dt);
	dt->destroy();
	
	return 0;
}

int command_ldtable(int argc, const char * argv[])
{
	/* we just compare simple dtable with linear dtable using the exception
	 * dtable performance test, and count on it to test functionality also */
	int r;
	dtable * dt;
	params config;
	sys_journal * sysj = sys_journal::get_global_journal();
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) linear_dtable
		"digest_interval" int 2
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "lldt_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "lldt_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	excp_perf(dt);
	dt->destroy();
	
	config = params();
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 2
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "lsdt_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "lsdt_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	excp_perf(dt);
	dt->destroy();
	
	return 0;
}

static int bfdt_perf(dtable * table)
{
	struct timeval start;
	
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
	print_elapsed(&start);
	
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
	print_elapsed(&start);
	
	printf("Mixed lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(int i = 0; i < 200000; i++)
	{
		uint32_t key = rand() % 8000000;
		blob value = table->find(key);
	}
	print_elapsed(&start);
	
	return 0;
}

int command_bfdtable(int argc, const char * argv[])
{
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "bfdt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
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
			EXPECT_NEVER("fail!");
			return -1;
		}
	}
	printf("done.\n");
	
	wait_digest(2);
	
	printf("Maintaining... ");
	fflush(stdout);
	r = dt->maintain();
	EXPECT_NOFAIL("done. r", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	bfdt_perf(dt);
	dt->destroy();
	
	printf("Repeat with direct access...\n");
	dt = dtable_factory::load("simple_dtable", AT_FDCWD, "bfdt_perf/md_data.0/base", params(), sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	bfdt_perf(dt);
	dt->destroy();
	
	return 0;
}

int command_blob_cmp(int argc, const char * argv[])
{
	int r;
	struct timeval start, end;
	sys_journal * sysj = sys_journal::get_global_journal();
	blob_comparator * reverse = new reverse_blob_comparator;
	
	if(argc < 2 || strcmp(argv[1], "perf"))
	{
		/* don't run the performance test, just do this fast test */
		sys_journal * sysj;
		journal_dtable * jdt;
		sys_journal::listener_id jid;
		journal_dtable::journal_dtable_warehouse warehouse;
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		sysj = sys_journal::spawn_init("test_journal", &warehouse, true);
		EXPECT_NONULL("sysj spawn", sysj);
		jid = sys_journal::get_unique_id();
		if(jid == sys_journal::NO_ID)
			return -EBUSY;
		jdt = warehouse.obtain(jid, dtype::BLOB, sysj);
		EXPECT_NONULL("jdt", jdt);
		r = jdt->set_blob_cmp(reverse);
		EXPECT_NOFAIL("jdt->set_blob_cmp", r);
		for(int i = 0; i < 10; i++)
		{
			uint32_t keydata = rand();
			uint8_t valuedata = i;
			blob key(sizeof(keydata), &keydata);
			blob value(sizeof(valuedata), &valuedata);
			r = jdt->insert(dtype(key), value);
			EXPECT_NOFAIL("insert", r);
		}
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		run_iterator(jdt);
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		delete sysj;
		sysj = sys_journal::spawn_init("test_journal", &warehouse, false);
		EXPECT_NONULL("sysj spawn", sysj);
		jdt = warehouse.lookup(jid);
		EXPECT_NONULL("jdt", jdt);
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		printf("expected comparator: %s\n", (const char *) jdt->get_cmp_name());
		EXPECT_SIZET("jdt size", 0, jdt->size());
		
		r = jdt->set_blob_cmp(reverse);
		EXPECT_NOFAIL("jdt->set_blob_cmp", r);
		EXPECT_SIZET("jdt size", 10, jdt->size());
		
		run_iterator(jdt);
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		r = jdt->discard();
		EXPECT_NOFAIL("discard", r);
		sysj->deinit(true);
		delete sysj;
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		reverse->release();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = simple_stable::create(AT_FDCWD, "cmp_test", config, dtype::BLOB);
	EXPECT_NOFAIL("stable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmp_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	r = sst->set_blob_cmp(reverse);
	EXPECT_NOFAIL("sst->set_blob_cmp", r);
	
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
			print_progress(&start, (i + 1) / 50000);
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
			r = sysj->filter();
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
	printf("Timing finished! ");
	print_elapsed(&start, &end, true);
	printf("Average: %"PRIu64" inserts/second\n", 5000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	delete sst;
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmp_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	r = sst->set_blob_cmp(reverse);
	EXPECT_NOFAIL("sst->set_blob_cmp", r);
	r = sst->maintain();
	EXPECT_NOFAIL("sst->maintain", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
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
		EXPECT_NEVER("failed! (sum = %u, check = %u)", sum, check);
	
	delete sst;
	reverse->release();
	return (sum == check) ? 0 : -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete sst;
	reverse->release();
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
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = simple_stable::create(AT_FDCWD, "perf_test", config, dtype::UINT32);
	EXPECT_NOFAIL("stable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perf_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	
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
			print_progress(&start, (i + 1) / 20000);
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
			r = sysj->filter();
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
	printf("Timing finished! ");
	print_elapsed(&start, &end, true);
	printf("Average: %"PRIu64" inserts/second\n", 2000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perf_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	
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
	delete sst;
	EXPECT_NEVER("failed!");
	return -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete sst;
	EXPECT_NEVER("failed!");
	return r;
}

static int command_performance_dtable(int argc, const char * argv[])
{
	int r;
	dtable * dt;
	dtable::iter * iter;
	struct timeval start, end;
	uint32_t table_copy[DT_ROW_COUNT];
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("cache_dtable", AT_FDCWD, "dtpf_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable_factory::setup", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	dt = dtable_factory::load("cache_dtable", AT_FDCWD, "dtpf_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
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
			print_progress(&start, (i + 1) / 100000);
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
			r = sysj->filter();
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
	printf("Timing finished! ");
	print_elapsed(&start, &end, true);
	printf("Average: %"PRIu64" inserts/second\n", 10000000 * (uint64_t) 1000000 / (end.tv_sec * 1000000 + end.tv_usec));
	
	dt->destroy();
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	dt = dtable_factory::load("cache_dtable", AT_FDCWD, "dtpf_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
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
	
	dt->destroy();
	return 0;
	
fail_iter:
	delete iter;
fail_verify:
	EXPECT_NEVER("failed!");
	dt->destroy();
	return -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	dt->destroy();
	return r;
}

int command_performance(int argc, const char * argv[])
{
	if(argc > 1 && !strcmp(argv[1], "stable"))
		return command_performance_stable(argc - 1, &argv[1]);
	return command_performance_dtable(argc, argv);
}

int command_bdbtest(int argc, const char * argv[])
{
	/* TODO: this isn't the complete bdb test; we need to try different durability checks */
	sys_journal * sysj = sys_journal::get_global_journal();
	const uint32_t KEYSIZE = 8;
	const uint32_t VALSIZE = 32;
	struct timeval start;
	
	int r;
	sys_journal::listener_id jid;
	journal_dtable * jdt;
	
	char keybuf[KEYSIZE], valbuf[VALSIZE];
	util::memset(keybuf, 'a', KEYSIZE);
	util::memset(valbuf, 'b', VALSIZE);
	
	blob key(KEYSIZE, keybuf);
	blob value(VALSIZE, valbuf);
	
	printf("BerkeleyDB test timing!\n");
	gettimeofday(&start, NULL);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	jid = sys_journal::get_unique_id();
	if(jid == sys_journal::NO_ID)
		return -EBUSY;
	jdt = journal_dtable::warehouse.obtain(jid, dtype::BLOB, sysj);
	EXPECT_NONULL("jdt", jdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	for(int i = 0; i < 1000000; i++)
	{
		r = tx_start();
		if(r < 0)
		{
			EXPECT_NEVER("tx_start failure");
			break;
		}
		r = jdt->insert(dtype(key), value);
		assert(r >= 0);
		if((i % 100000) == 99999)
		{
			print_progress(&start, (i + 1) / 10000);
			fflush(stdout);
		}
		r = tx_end(0);
		if(r < 0)
		{
			EXPECT_NEVER("tx_end failure");
			break;
		}
	}
	
	printf("Timing finished! ");
	print_elapsed(&start, true);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	/* also destroys it */
	jdt->discard();
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	return 0;
}
