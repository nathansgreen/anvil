/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define __STDC_FORMAT_MACROS

#include <stdlib.h>

#include "main.h"
#include "openat.h"
#include "transaction.h"

#include "util.h"
#include "sys_journal.h"
#include "bloom_dtable.h"
#include "journal_dtable.h"
#include "temp_journal_dtable.h"
#include "managed_dtable.h"
#include "simple_stable.h"

#define ROW_COUNT 50000
#define DT_ROW_COUNT 200000

void abort_perf(bool use_temp)
{
	int r;
	dtable * dt;
	params config;
	sys_journal * sysj;
	bool use_atx = false;
	abortable_tx atx = NO_ABORTABLE_TX;
	journal_dtable::journal_dtable_warehouse warehouse;
	temp_journal_dtable::temp_journal_dtable_warehouse temp_warehouse;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 20
		"combine_interval" int 160
		"combine_count" int 12
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	do {
		printf("Abortable transaction performance test: use_atx = %d, use_temp = %d\n", use_atx, use_temp);
		struct timeval start, end;
		size_t atx_count = use_atx ? 1 : 0;
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		sysj = sys_journal::spawn_init("test_journal", &warehouse, use_temp ? &temp_warehouse : NULL, true);
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
		
		printf("Start timing! (40000000 inserts to %d rows)\n", DT_ROW_COUNT);
		gettimeofday(&start, NULL);
		
		for(int i = 0; i < 40000000; i++)
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
			r = dt->insert(row, blob(sizeof(value), &value), false, atx);
			assert(r >= 0);
			if((i % 2000000) == 1999999)
			{
				print_progress(&start, (i + 1) / 400000);
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
			if((i % 1000000) == 999999)
			{
				r = dt->maintain(true);
				if(r < 0)
				{
					EXPECT_NEVER("maintain failure");
					break;
				}
			}
			if((i % 1000000) == 999999)
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
		
		sync();
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
		EXPECT_SIZET("temp total", 0, temp_warehouse.size());
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
	} while((use_atx = !use_atx));
}

void abort_effect(void)
{
	int r;
	dtable * dt;
	params config;
	sys_journal * sysj;
	abortable_tx atx = NO_ABORTABLE_TX;
	journal_dtable::journal_dtable_warehouse warehouse;
	temp_journal_dtable::temp_journal_dtable_warehouse temp_warehouse;
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) simple_dtable
		"digest_interval" int 20
		"combine_interval" int 160
		"combine_count" int 12
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	for(int slice = 0; slice <= 10; slice++)
	{
		printf("Abortable transaction fraction: %d/10\n", slice);
		struct timeval start, end;
		struct timeval begin, finish;
		struct timeval tx_sum = {0, 0}, atx_sum = {0, 0};
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		sysj = sys_journal::spawn_init("test_journal", &warehouse, &temp_warehouse, true);
		EXPECT_NONULL("sysj spawn", sysj);
		r = dtable_factory::setup("managed_dtable", AT_FDCWD, "abtx_fect", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable_factory::setup", r);
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		dt = dtable_factory::load("managed_dtable", AT_FDCWD, "abtx_fect", config, sysj);
		EXPECT_NONULL("dtable_factory::load", dt);
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		printf("Start timing! (1000000 inserts to %d rows)\n", DT_ROW_COUNT);
		gettimeofday(&start, NULL);
		
		for(int i = 0; i < 1000000; i++)
		{
			uint32_t row = rand() % DT_ROW_COUNT;
			uint32_t value = rand();
			if(!(i % 100))
			{
				gettimeofday(&begin, NULL);
				r = tx_start();
				if(r < 0)
				{
					EXPECT_NEVER("tx_start failure");
					break;
				}
				if((i / 100) % 10 < slice)
				{
					atx = dt->create_tx();
					if(atx == NO_ABORTABLE_TX)
					{
						EXPECT_NEVER("create_tx failure");
						break;
					}
				}
				else
					atx = NO_ABORTABLE_TX;
			}
			r = dt->insert(row, blob(sizeof(value), &value), false, atx);
			assert(r >= 0);
			if((i % 100000) == 99999)
			{
				print_progress(&start, (i + 1) / 10000);
				fflush(stdout);
			}
			if((i % 100000) == 99999)
			{
				r = dt->maintain(true);
				if(r < 0)
				{
					EXPECT_NEVER("maintain failure");
					break;
				}
			}
			if((i % 100000) == 99999)
			{
				r = sysj->filter();
				if(r < 0)
				{
					EXPECT_NEVER("filter failure");
					break;
				}
			}
			if((i % 100) == 99)
			{
				bool was_atx;
				if(atx != NO_ABORTABLE_TX)
				{
					r = dt->commit_tx(atx);
					if(r < 0)
					{
						EXPECT_NEVER("commit_tx failure");
						break;
					}
					atx = NO_ABORTABLE_TX;
					was_atx = true;
				}
				else
					was_atx = false;
				r = tx_end(0);
				if(r < 0)
				{
					EXPECT_NEVER("tx_end failure");
					break;
				}
				gettimeofday(&finish, NULL);
				timeval_subtract(&finish, &begin);
				if(was_atx)
					timeval_add(&atx_sum, &finish);
				else
					timeval_add(&tx_sum, &finish);
			}
		}
		assert(atx == NO_ABORTABLE_TX);
		
		sync();
		gettimeofday(&end, NULL);
		printf("Timing finished! ");
		print_elapsed(&start, &end, true);
		
		printf("Weighted basic transaction time: ");
		timeval_divide(&tx_sum, 10 - slice);
		print_timeval(&tx_sum, true, true);
		printf("Weighted abortable transaction time: ");
		timeval_divide(&atx_sum, slice);
		print_timeval(&atx_sum, true, true);
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		dt->destroy();
		sysj->deinit(true);
		delete sysj;
		util::rm_r(AT_FDCWD, "abtx_fect");
		EXPECT_SIZET("total", 0, warehouse.size());
		EXPECT_SIZET("temp total", 0, temp_warehouse.size());
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
	}
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

void edtable_perf(void)
{
	int r;
	params config;
	managed_dtable * mdt;
	sys_journal * sysj = sys_journal::get_global_journal();
	
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

#define POPULAR_BLOBS 10
static int uqdt_perf(dtable * dt, const char * name)
{
	int r;
	struct timeval start;
	blob popular[POPULAR_BLOBS];
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	printf("Populating journal dtable... ");
	fflush(stdout);
	srand(8675309);
	for(size_t i = 0; i < POPULAR_BLOBS; i++)
		popular[i] = random_blob(80);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < 2000000; i++)
	{
		uint32_t key = i * 2;
		if(!(rand() % 4))
			/* 25% chance of a random blob */
			r = dt->insert(key, random_blob(75 + (rand() % 11)));
		else
			/* 75% chance of a popular blob */
			r = dt->insert(key, popular[rand() % POPULAR_BLOBS]);
		if(r < 0)
		{
			EXPECT_NEVER("insert() fail!");
			return -1;
		}
	}
	print_elapsed(&start);
	printf("Converting to %s dtable... ", name);
	fflush(stdout);
	gettimeofday(&start, NULL);
	r = dt->maintain(true);
	print_elapsed(&start);
	EXPECT_NOFAIL("maintain", r);
	printf("done.\n");
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	time_iterator(dt, 3);
	
	printf("Random lookups... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < 1000000; i++)
	{
		uint32_t key = (rand() % 2000000) * 2;
		blob value = dt->find(key);
	}
	print_elapsed(&start);
	
	return 0;
}

int udtable_perf(void)
{
	int r;
	dtable * dt;
	params config;
	sys_journal * sysj = sys_journal::get_global_journal();
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) uniq_dtable
		"base_config" config [
			"keybase" class(dt) fixed_dtable
			"valuebase" class(dt) linear_dtable
		]
		"digest_interval" int 2
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "uqdt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "uqdt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	uqdt_perf(dt, "unique");
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
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "urdt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "urdt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	uqdt_perf(dt, "reference");
	dt->destroy();
	
	return 0;
}

int kddtable_perf(void)
{
	int r;
	dtable * dt;
	params config;
	sys_journal * sysj = sys_journal::get_global_journal();
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_interval" int 2
		]
		"divider_0" int 500000
		"divider_1" int 1000000
		"divider_2" int 1500000
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("keydiv_dtable", AT_FDCWD, "kddt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	dt = dtable_factory::load("keydiv_dtable", AT_FDCWD, "kddt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	/* the uniq_dtable test is fine here even thought it's not what we're testing */
	uqdt_perf(dt, "[keydiv] simple");
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
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "krdt_perf", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "krdt_perf", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	uqdt_perf(dt, "simple");
	dt->destroy();
	
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

#if BFDT_PERF_TEST
static int bfdt_mtrc(dtable * table)
{
	struct timeval start;
	size_t exist = 0, listed = 0;
	dtable::iter * iter = table->iterator();
	uint32_t next_key = 0;
	
	printf("Calculating bloom_dtable metrics... ");
	fflush(stdout);
	gettimeofday(&start, NULL);
	while(iter->valid())
	{
		dtype key = iter->key();
		assert(key.type == dtype::UINT32);
		
		/* look up intermediate keys */
		while(next_key < key.u32)
			table->contains(next_key++);
		next_key = key.u32 + 1;
		
		/* handle the current key */
		if(iter->value().exists())
			exist++;
		else
			table->contains(key);
		listed++;
		
		iter->next();
	}
	print_elapsed(&start, true);
	delete iter;
	
	assert(next_key);
	printf("%zu/%u keys exist (%lg%%), %zu listed\n", exist, next_key, 100 * exist / (double) next_key, listed);
	return 0;
}
#endif

static int bfdt_populate(dtable * dt, const size_t size, const size_t ops, const char * name, const dtable * ref)
{
	int r;
	printf("Populating %s table... ", name);
	fflush(stdout);
	srand(8675309);
	for(size_t i = 0; i < ops; i++)
	{
		uint32_t key = rand() % size;
		bool insert = !(rand() & 1);
		if(ref)
		{
			if(insert && ref->contains(key))
				r = dt->insert(key, blob(sizeof(key), &key));
			else
				r = 0;
		}
		else
		{
			if(insert)
				r = dt->insert(key, blob(sizeof(key), &key));
			else
				r = dt->remove(key);
		}
		if(r < 0)
		{
			EXPECT_NEVER("insert()/remove() fail!");
			return -1;
		}
		if(i == ops / 2 || (i > ops / 2 && !(i % (ops / 10))))
		{
			r = dt->maintain(true);
			if(r < 0)
			{
				EXPECT_NEVER("maintain() fail!");
				return -1;
			}
		}
	}
	r = dt->maintain(true);
	if(r < 0)
	{
		EXPECT_NEVER("maintain() fail!");
		return -1;
	}
	printf("done.\n");
	return 0;
}

static int bfdt_scan(dtable * table, const size_t size, const size_t lookups, const char * name, const dtable * ref)
{
	struct timeval start;
	size_t never = 0, reset = 0, set = 0;
	
	printf("Summarize %s key status... ", name);
	fflush(stdout);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < size; i++)
	{
		uint32_t key = i;
		bool found, has = table->present(key, &found);
		if(ref && has != ref->contains(key))
			EXPECT_NEVER("key %u mismatch\n", key);
		if(has)
			set++;
		else if(found)
			reset++;
		else
			never++;
	}
	print_elapsed(&start);
	printf("Results: %zu/%zu/%zu set/reset/never\n", set, reset, never);
	
	printf("Performance check... ");
	fflush(stdout);
	srand(9035768);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < lookups; i++)
	{
		uint32_t key = rand() % size;
		table->contains(key);
	}
	print_elapsed(&start);
	
#if BFDT_PERF_TEST
	printf("Effectiveness check... ");
	fflush(stdout);
	srand(9035768);
	gettimeofday(&start, NULL);
	for(size_t i = 0; i < lookups; i++)
	{
		uint32_t key = rand() % size;
		bool has = table->contains(key);
		if(!has)
		{
			bloom_dtable::perf_enable = true;
			table->contains(key);
			bloom_dtable::perf_enable = false;
		}
	}
	print_elapsed(&start);
#else
	printf("Not compiled with bloom_dtable metrics. Set BFDT_PERF_TEST.\n");
#endif
	
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
		"digest_interval" int 120
		"combine_interval" int 960
		"combine_count" int 10
		"autocombine" bool false
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	if(argc > 1 && !strcmp(argv[1], "metrics"))
	{
#if !BFDT_PERF_TEST
		EXPECT_NEVER("Not compiled with bloom_dtable metrics. Set BFDT_PERF_TEST.");
#else
		const int rounds = 20;
		for(int round = 0; round < rounds; round++)
		{
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			r = dtable_factory::setup("managed_dtable", AT_FDCWD, "bfdt_mtrc", config, dtype::UINT32);
			EXPECT_NOFAIL("dtable::create", r);
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
			
			dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_mtrc", config, sysj);
			EXPECT_NONULL("dtable_factory::load", dt);
			
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			printf("Populating table... ");
			fflush(stdout);
			for(int i = 0; i < 1000000; i++)
			{
				uint32_t key = i * 2 + 1;
				r = dt->insert(key, blob(sizeof(key), &key));
				if(r < 0)
				{
					tx_end(0);
					EXPECT_NEVER("fail!");
					return -1;
				}
			}
			printf("done.\n");
			
			printf("Maintaining... ");
			fflush(stdout);
			r = dt->maintain(true);
			EXPECT_NOFAIL("done. r", r);
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
			
			r = tx_start();
			EXPECT_NOFAIL("tx_start", r);
			printf("Depopulating table... ");
			fflush(stdout);
			for(int i = 0; i < 1000000; i++)
			{
				if(round <= i % rounds)
					continue;
				uint32_t key = i * 2 + 1;
				r = dt->remove(key);
				if(r < 0)
				{
					tx_end(0);
					EXPECT_NEVER("remove() fail!");
					return -1;
				}
			}
			printf("done.\n");
			
			printf("Maintaining... ");
			fflush(stdout);
			r = dt->maintain(true);
			EXPECT_NOFAIL("done. r", r);
			dt->destroy();
			r = sys_journal::get_global_journal()->filter();
			EXPECT_NOFAIL("filter", r);
			r = tx_end(0);
			EXPECT_NOFAIL("tx_end", r);
			
			bloom_dtable::perf_enable = true;
			dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_mtrc", config, sysj);
			EXPECT_NONULL("dtable_factory::load", dt);
			bfdt_mtrc(dt);
			dt->destroy();
			bloom_dtable::perf_enable = false;
			
			util::rm_r(AT_FDCWD, "bfdt_mtrc");
		}
#endif
		return 0;
	}
	
	if(argc > 1 && !strcmp(argv[1], "oracle"))
	{
		const size_t size = 4000000;
		const size_t ops = size * 3;
		dtable * ref;
		
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		
		r = dtable_factory::setup("managed_dtable", AT_FDCWD, "bfdt_refr", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable::create", r);
		ref = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_refr", config, sysj);
		EXPECT_NONULL("dtable_factory::load", ref);
		bfdt_populate(ref, size, ops, "reference", NULL);
		
		r = dtable_factory::setup("managed_dtable", AT_FDCWD, "bfdt_orcl", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable::create", r);
		dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_orcl", config, sysj);
		EXPECT_NONULL("dtable_factory::load", dt);
		bfdt_populate(dt, size, ops, "oracle", ref);
		
		ref->destroy();
		dt->destroy();
		
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		
		ref = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_refr", config, sysj);
		EXPECT_NONULL("dtable_factory::load", ref);
		dt = dtable_factory::load("managed_dtable", AT_FDCWD, "bfdt_orcl", config, sysj);
		EXPECT_NONULL("dtable_factory::load", dt);
		bfdt_scan(ref, size, ops, "reference", NULL);
		bfdt_scan(dt, size, ops, "oracle", ref);
		
#if BFDT_PERF_TEST
		bloom_dtable::perf_enable = true;
#endif
		ref->destroy();
		dt->destroy();
#if BFDT_PERF_TEST
		bloom_dtable::perf_enable = false;
#endif
		
		return 0;
	}
	
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
	
	printf("Maintaining... ");
	fflush(stdout);
	r = dt->maintain(true);
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

int blob_cmp_perf(blob_comparator * blob_cmp)
{
	int r;
	params config;
	bool first = true;
	dtype old_key(0u);
	istr column("sum");
	simple_stable * sst;
	stable::iter * iter;
	struct timeval start, end;
	uint32_t sum = 0, check = 0;
	sys_journal * sysj = sys_journal::get_global_journal();
	
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
	r = sst->set_blob_cmp(blob_cmp);
	EXPECT_NOFAIL("sst->set_blob_cmp", r);
	
	printf("Start timing! (5000000 compared blob key inserts to %d rows)\n", ROW_COUNT);
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
	r = sst->set_blob_cmp(blob_cmp);
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
	blob_cmp->release();
	return (sum == check) ? 0 : -1;
	
fail_maintain:
fail_insert:
	tx_end(0);
fail_tx_start:
	delete sst;
	blob_cmp->release();
	return r;
}

static const istr column_names[] = {"c_one", "c_two", "c_three", "c_four", "c_five"};
#define COLUMN_NAMES (sizeof(column_names) / sizeof(column_names[0]))

static int performance_stable(int argc, const char * argv[])
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

static int performance_dtable(int argc, const char * argv[])
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
		return performance_stable(argc - 1, &argv[1]);
	return performance_dtable(argc, argv);
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
