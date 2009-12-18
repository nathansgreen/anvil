/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <signal.h>

#include "main.h"
#include "openat.h"
#include "transaction.h"

#include "util.h"
#include "sys_journal.h"
#include "journal_dtable.h"
#include "simple_dtable.h"
#include "managed_dtable.h"
#include "usstate_dtable.h"
#include "memory_dtable.h"
#include "simple_stable.h"
#include "reverse_blob_comparator.h"

int command_info(int argc, const char * argv[])
{
	params::print_classes();
	return 0;
}

int command_dtable(int argc, const char * argv[])
{
	int r;
	managed_dtable * mdt;
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("tx_start", r);
	r = managed_dtable::create(AT_FDCWD, path, config, dtype::UINT32);
	EXPECT_NOFAIL_FORMAT("dtable::create(%s)", r, path);
	if(r < 0)
	{
		tx_end(0);
		return r;
	}
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = mdt->insert(6u, blob("hello"));
	EXPECT_NOFAIL("mdt->insert", r);
	r = mdt->insert(4u, blob("world"));
	EXPECT_NOFAIL("mdt->insert", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = mdt->digest();
	EXPECT_NOFAIL("mdt->digest", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = mdt->insert(8u, blob("icanhas"));
	EXPECT_NOFAIL("mdt->insert", r);
	r = mdt->insert(2u, blob("cheezburger"));
	EXPECT_NOFAIL("mdt->insert", r);
	run_iterator(mdt);
	r = mdt->digest();
	EXPECT_NOFAIL("mdt->digest", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = mdt->combine();
	EXPECT_NOFAIL("mdt->combine", r);
	run_iterator(mdt);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	mdt->destroy();
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, path, config, sysj);
	EXPECT_NOFAIL_COUNT("mdt->init", r, "disk dtables", mdt->disk_dtables());
	run_iterator(mdt);
	mdt->destroy();
	
	return 0;
}

int command_edtable(int argc, const char * argv[])
{
	sys_journal * sysj = sys_journal::get_global_journal();
	const size_t size = 10000;
	const size_t ops = size * 3;
	params config;
	dtable * dt;
	int r;
	
	/* TODO: It might be nice to have a dne_dtable that can only store
	 * nonexistent values. It's not clear that it could support rejection
	 * though, since the replacements would have to be nonexistent. Even so
	 * it would work for the dnebase of an exist_dtable. */
	r = params::parse(LITERAL(
	config [
		"base" class(dt) exist_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"dnebase" class(dt) simple_dtable
		]
		"digest_interval" int 240
		"combine_interval" int 1920
		"combine_count" int 10
		"autocombine" bool false
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "exst_test", config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "exst_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	printf("Populating exist_dtable... ");
	fflush(stdout);
	for(size_t i = 0; i < ops; i++)
	{
		uint32_t key = rand() % size;
		bool insert = !(rand() & 1);
		if(insert)
			r = dt->insert(key, blob(sizeof(key), &key));
		else
			r = dt->remove(key);
		EXPECT_NOFAIL_SILENT_BREAK("insert/remove", r);
		if(i == ops / 2 || (i > ops / 2 && !(i % (ops / 10))))
		{
			r = dt->maintain(true);
			EXPECT_NOFAIL_SILENT_BREAK("maintain", r);
		}
	}
	r = dt->maintain(true);
	if(r < 0)
		EXPECT_NEVER("maintain failure");
	else
	printf("done.\n");
	
	dt->destroy();
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	for(size_t i = 0; i <= 5; i++)
	{
		bool ok;
		char path[32];
		istr base, dnebase;
		dtable::iter * iter;
		params exist_config, base_config, dnebase_config;
		
		ok = config.get("base_config", &exist_config);
		assert(ok);
		ok = exist_config.get("base", &base);
		assert(ok);
		ok = exist_config.get("dnebase", &dnebase);
		assert(ok);
		ok = exist_config.get("base_config", &base_config);
		assert(ok);
		ok = exist_config.get("dnebase_config", &dnebase_config);
		assert(ok);
		
		sprintf(path, "exst_test/md_data.%zu/base", i);
		dt = dtable_factory::load(base, AT_FDCWD, path, base_config, sysj);
		EXPECT_NONULL("dtable_factory::load", dt);
		iter = dt->iterator();
		EXPECT_NONULL("iterator", iter);
		if(!iter->valid())
			EXPECT_NEVER("no values in base");
		while(iter->valid())
		{
			if(!iter->value().exists())
			{
				EXPECT_NEVER("nonexistent value in base");
				break;
			}
			iter->next();
		}
		delete iter;
		dt->destroy();
		
		sprintf(path, "exst_test/md_data.%zu/dnebase", i);
		dt = dtable_factory::load(dnebase, AT_FDCWD, path, dnebase_config, sysj);
		EXPECT_NONULL("dtable_factory::load", dt);
		iter = dt->iterator();
		EXPECT_NONULL("iterator", iter);
		if(!iter->valid())
			EXPECT_NEVER("no values in dnebase");
		while(iter->valid())
		{
			if(iter->value().exists())
			{
				EXPECT_NEVER("extant value in dnebase");
				break;
			}
			iter->next();
		}
		delete iter;
		dt->destroy();
	}
	
	util::rm_r(AT_FDCWD, "exst_test");
	
	return 0;
}

int command_exdtable(int argc, const char * argv[])
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
		/* run the performance test as well */
		exdtable_perf();
	
	return 0;
}

int command_ussdtable(int argc, const char * argv[])
{
	int r;
	params config;
	dtable * table;
	memory_dtable mdt;
	sys_journal * sysj = sys_journal::get_global_journal();
	const dtable_factory * base = dtable_factory::lookup("usstate_dtable");
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) fixed_dtable
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	mdt.init(dtype::UINT32, true);
	mdt.insert(1u, "MA");
	mdt.insert(2u, "CA");
	run_iterator(&mdt);
	
	r = base->create(AT_FDCWD, "usst_test", config, &mdt);
	EXPECT_NOFAIL("uss::create", r);
	table = base->open(AT_FDCWD, "usst_test", config, sysj);
	EXPECT_NONULL("uss::open", table);
	run_iterator(table);
	table->destroy();
	
	table = dtable_factory::load("fixed_dtable", AT_FDCWD, "usst_test", params(), sysj);
	EXPECT_NONULL("dtable_factory::load", table);
	run_iterator(table);
	table->destroy();
	
	mdt.insert(3u, "other");
	r = base->create(AT_FDCWD, "usst_fail", config, &mdt);
	EXPECT_FAIL("uss::create", r);
	
	return 0;
}

int command_sidtable(int argc, const char * argv[])
{
	int r;
	params config;
	uint32_t value;
	dtable * table;
	memory_dtable mdt;
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	mdt.init(dtype::UINT32, true);
	value = 42;
	mdt.insert(1u, blob(sizeof(value), &value));
	value = 192;
	mdt.insert(2u, blob(sizeof(value), &value));
	run_iterator(&mdt);
	
	r = base->create(AT_FDCWD, "sidt_test", config, &mdt);
	EXPECT_NOFAIL("sid::create", r);
	table = base->open(AT_FDCWD, "sidt_test", config, sysj);
	EXPECT_NONULL("sid::open", table);
	run_iterator(table);
	table->destroy();
	
	table = dtable_factory::load("array_dtable", AT_FDCWD, "sidt_test", params(), sysj);
	EXPECT_NONULL("dtable_factory::load", table);
	run_iterator(table);
	table->destroy();
	
	value = 320;
	mdt.insert(3u, blob(sizeof(value), &value));
	r = base->create(AT_FDCWD, "sidt_fail", config, &mdt);
	EXPECT_FAIL("sid::create", r);
	
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
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
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
	EXPECT_NOFAIL("did::create", r);
	table = base->open(AT_FDCWD, "didt_test", config, sysj);
	EXPECT_NONULL("did::open", table);
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
			EXPECT_NEVER("failed!");
			print(value_mdt, "memory find %u: ", key);
			print(value_ddt, " delta find %u: ", key);
			table->destroy();
			table = NULL;
			break;
		}
	}
	if(table)
	{
		table->destroy();
		printf("OK!\n");
	}
	
	table = base->open(AT_FDCWD, "didt_test", config, sysj);
	EXPECT_NONULL("did::open", table);
	
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
		EXPECT_NEVER("failed!");
	delete test_it;
	delete ref_it;
	table->destroy();
	
	table = dtable_factory::load("simple_dtable", AT_FDCWD, "didt_test/base", params(), sysj);
	EXPECT_NONULL("dtable_factory::load", table);
	run_iterator(table);
	table->destroy();
	
	table = dtable_factory::load("simple_dtable", AT_FDCWD, "didt_test/ref", params(), sysj);
	EXPECT_NONULL("dtable_factory::load", table);
	run_iterator(table);
	table->destroy();
	
	return 0;
}

static void iterator_test(const istr & type, const char * name, const params & config, size_t count, bool verbose)
{
	int r;
	dtable * dt;
	sys_journal * sysj = sys_journal::get_global_journal();
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = dtable_factory::setup(type, AT_FDCWD, name, config, dtype::UINT32);
	EXPECT_NOFAIL("dtable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	const char * layers[] = {"358", "079", "", "2457", "1267"};
#define LAYERS (sizeof(layers) / sizeof(layers[0]))
#define VALUES 10
	blob values[VALUES];
	
	dt = dtable_factory::load(type, AT_FDCWD, name, config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	for(size_t i = 0; i < LAYERS; i++)
	{
		int delay = i ? 2 : 3;
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		for(const char * value = layers[i]; *value; value++)
		{
			uint32_t key = *value - '0';
			char content[16];
			snprintf(content, sizeof(content), "L%zu-K%u", i, key * 2);
			values[key] = blob(content);
			r = dt->insert(key * 2, values[key]);
			EXPECT_NOFAIL_FORMAT("dt->insert(%d, %s)", r, key, content);
		}
		run_iterator(dt);
		
		wait_digest(delay);
		
		r = dt->maintain();
		EXPECT_NOFAIL("dt->maintain()", r);
		run_iterator(dt);
		
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
	}
	dt->destroy();
	
	dt = dtable_factory::load(type, AT_FDCWD, name, config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
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
		EXPECT_NEVER("failed!");
		printf("(");
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
	dt->destroy();
}

int command_kddtable(int argc, const char * argv[])
{
	int r;
	size_t count = 0;
	bool verbose = false;
	params config;
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
		return kddtable_perf();
	
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	iterator_test("keydiv_dtable", "kddt_test", config, count, verbose);
	
	return 0;
}

struct uniq_insert
{
	double key;
	size_t index;
};

struct uniq_key
{
	double key;
	size_t size;
};

static void verify_uniq_alts(const char ** strings, const uniq_insert * alts, size_t count, const char * type, const char * path, const params & config, sys_journal * sysj)
{
	dtable::iter * iter;
	dtable * dt = dtable_factory::load(type, AT_FDCWD, path, config, sysj);
	EXPECT_NONULL("dtable_factory::load alt", dt);
	run_iterator(dt);
	iter = dt->iterator();
	for(size_t i = 0; i < count; i++)
	{
		if(!iter->valid())
			EXPECT_NEVER("iterator not valid");
		dtype key = iter->key();
		blob value = iter->value();
		blob expect = blob(strings[alts[i].index]);
		assert(key.type == dtype::DOUBLE);
		if(key.dbl != alts[i].key)
			EXPECT_NEVER("incorrect key[%zu]", i);
		if(value.compare(expect))
			EXPECT_NEVER("incorrect value[%zu]", i);
		iter->next();
	}
	if(iter->valid())
		EXPECT_NEVER("iterator still valid");
	delete iter;
	dt->destroy();
}

static void verify_uniq_keys(const uniq_key * sizes, size_t count, const char * type, const char * path, const params & config, sys_journal * sysj)
{
	dtable::iter * iter;
	dtable * dt = dtable_factory::load(type, AT_FDCWD, path, config, sysj);
	EXPECT_NONULL("dtable_factory::load keys", dt);
	run_iterator(dt);
	iter = dt->iterator();
	for(size_t i = 0; i < count; i++)
	{
		if(!iter->valid())
			EXPECT_NEVER("iterator not valid");
		dtype key = iter->key();
		blob value = iter->value();
		assert(key.type == dtype::DOUBLE);
		if(key.dbl != sizes[i].key)
			EXPECT_NEVER("incorrect key[%zu]", i);
		if(sizes[i].size == (size_t) -1)
		{
			if(value.exists())
				EXPECT_NEVER("incorrect value[%zu]", i);
		}
		else
		{
			if(value.size() != sizes[i].size)
				EXPECT_NEVER("incorrect value[%zu]", i);
		}
		iter->next();
	}
	if(iter->valid())
		EXPECT_NEVER("iterator still valid");
	delete iter;
	dt->destroy();
}

static void verify_uniq_values(const char ** strings, const size_t * values, size_t count, const char * type, const char * path, const params & config, sys_journal * sysj)
{
	dtable::iter * iter;
	dtable * dt = dtable_factory::load(type, AT_FDCWD, path, config, sysj);
	EXPECT_NONULL("dtable_factory::load values", dt);
	run_iterator(dt);
	iter = dt->iterator();
	for(size_t i = 0; i < count; i++)
	{
		if(!iter->valid())
			EXPECT_NEVER("iterator not valid");
		dtype key = iter->key();
		blob value = iter->value();
		blob expect = blob(strings[values[i]]);
		assert(key.type == dtype::UINT32);
		if(key.u32 != i)
			EXPECT_NEVER("incorrect key %u (expected %zu)", key.u32, i);
		if(value.compare(expect))
			EXPECT_NEVER("incorrect value[%zu]", i);
		iter->next();
	}
	if(iter->valid())
		EXPECT_NEVER("iterator still valid");
	delete iter;
	dt->destroy();
}

int command_udtable(int argc, const char * argv[])
{
	int r;
	dtable * dt;
	params config;
	dtable::iter * iter;
	sys_journal * sysj = sys_journal::get_global_journal();
	const char * strings[] = {
		"1",              /* 0 */
		"* 5 *",          /* 1 */
		"### 10 ###",     /* 2 */
		"%%% 10 %%%",     /* 3 */
		"&&& 10 &&&",     /* 4 */
		"@@@ 10 @@@",     /* 5 */
		"===== 14 =====", /* 6 */
		"          ",     /* 7 (the reject value) */
	};
	const uniq_insert inserts[] = {
		{0.0, 2}, {0.5, 2}, {1.0, 2}, {1.5, 2},
		{2.0, 3}, {2.5, 2}, {3.0, 4}, {3.5, 2},
		{4.0, 5}, {4.5, 4}, {5.0, 3}, {5.5, 0},
		{6.0, 3}, {6.5, 4}, {7.0, 1}, {7.5, 5},
		{8.0, 6}, {8.5, 6}, {9.0, 1}, {9.5, 2},
	}, alts_0[] = {
		{5.5, 0}, {7.0, 1}, {8.0, 6}, {8.5, 6}, {9.0, 1},
	}, alts_1[] = { {7.25, 0} };
	const uniq_key keys_0[] = {
		{0.0, 4}, {0.5, 4}, {1.0, 4}, {1.5, 4},
		{2.0, 4}, {2.5, 4}, {3.0, 4}, {3.5, 4},
		{4.0, 4}, {4.5, 4}, {5.0, 4}, {5.5, 4},
		{6.0, 4}, {6.5, 4}, {7.0, 4}, {7.5, 4},
		{8.0, 4}, {8.5, 4}, {9.0, 4}, {9.5, 4},
	}, keys_1[] = {
		{0.5, -1}, {1.5, -1}, {7.25, 4}, {7.75, 4}, {8.25, 4},
	};
	/* TODO: there could be only one 7 here, but uniq_dtable
	 * needs to be improved before it will do that correctly */
	const size_t values_0[] = {2, 3, 4, 5, 7, 7, 7};
	const size_t values_1[] = {7, 4, 5};
#define INSERTS (sizeof(inserts) / sizeof(inserts[0]))
#define ALTS_0 (sizeof(alts_0) / sizeof(alts_0[0]))
#define ALTS_1 (sizeof(alts_1) / sizeof(alts_1[0]))
#define KEYS_0 (sizeof(keys_0) / sizeof(keys_0[0]))
#define KEYS_1 (sizeof(keys_1) / sizeof(keys_1[0]))
#define VALUES_0 (sizeof(values_0) / sizeof(values_0[0]))
#define VALUES_1 (sizeof(values_1) / sizeof(values_1[0]))
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
		return udtable_perf();
	
	r = params::parse(LITERAL(
	config [
		"base" class(dt) exception_dtable
		"base_config" config [
			"base" class(dt) uniq_dtable
			"base_config" config [
				"keybase" class(dt) fixed_dtable
				"valuebase" class(dt) array_dtable
				"valuebase_config" config [
					"value_size" int 10
				]
			]
			"alt" class(dt) simple_dtable
			"reject_value" blob 20202020202020202020
		]
		"digest_interval" int 2
		"combine_interval" int 12
		"combine_count" int 8
		"autocombine" bool false
	]), &config);
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	r = dtable_factory::setup("managed_dtable", AT_FDCWD, "uqdt_test", config, dtype::DOUBLE);
	EXPECT_NOFAIL("dtable::create", r);
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "uqdt_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	/* first, let's insert some values and digest */
	for(size_t i = 0; i < INSERTS; i++)
	{
		blob value = blob(strings[inserts[i].index]);
		r = dt->insert(inserts[i].key, value);
		if(r < 0)
			EXPECT_NEVER("insert(%lg)", inserts[i].key);
	}
	r = dt->maintain(true);
	EXPECT_NOFAIL("maintain", r);
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	/* print them to help diagnose errors */
	run_iterator(dt);
	
	/* and check for errors */
	iter = dt->iterator();
	for(size_t i = 0; i < INSERTS; i++)
	{
		if(!iter->valid())
			EXPECT_NEVER("iterator not valid");
		dtype key = iter->key();
		blob value = iter->value();
		blob expect = blob(strings[inserts[i].index]);
		assert(key.type == dtype::DOUBLE);
		if(key.dbl != inserts[i].key)
			EXPECT_NEVER("incorrect key[%zu]", i);
		if(value.compare(expect))
			EXPECT_NEVER("incorrect value[%zu]", i);
		iter->next();
	}
	if(iter->valid())
		EXPECT_NEVER("iterator still valid");
	delete iter;
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	/* insert a few more values and delete some of the old ones */
	/* NOTE: the _1 tables above are based on the following code */
	r = dt->insert(7.25, blob(strings[0]));
	EXPECT_NOFAIL("insert(7.25)", r);
	r = dt->insert(7.75, blob(strings[4]));
	EXPECT_NOFAIL("insert(7.75)", r);
	r = dt->insert(8.25, blob(strings[5]));
	EXPECT_NOFAIL("insert(8.25)", r);
	r = dt->insert(8.75, blob(strings[2]));
	EXPECT_NOFAIL("insert(8.75)", r);
	r = dt->remove(8.75);
	EXPECT_NOFAIL("remove(8.75)", r);
	r = dt->remove(0.5);
	EXPECT_NOFAIL("remove(0.5)", r);
	r = dt->remove(1.5);
	EXPECT_NOFAIL("remove(1.5)", r);
	r = dt->maintain(true);
	EXPECT_NOFAIL("maintain", r);
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	/* print them to help diagnose errors */
	run_iterator(dt);
	
	dt->destroy();
	
	printf("Check underlying dtables...\n");
	
	verify_uniq_alts(strings, alts_0, ALTS_0, "simple_dtable", "uqdt_test/md_data.0/alt", params(), sysj);
	verify_uniq_keys(keys_0, KEYS_0, "fixed_dtable", "uqdt_test/md_data.0/base/keys", params(), sysj);
	verify_uniq_values(strings, values_0, VALUES_0, "array_dtable", "uqdt_test/md_data.0/base/values", params(), sysj);
	
	verify_uniq_alts(strings, alts_1, ALTS_1, "simple_dtable", "uqdt_test/md_data.1/alt", params(), sysj);
	verify_uniq_keys(keys_1, KEYS_1, "fixed_dtable", "uqdt_test/md_data.1/base/keys", params(), sysj);
	verify_uniq_values(strings, values_1, VALUES_1, "array_dtable", "uqdt_test/md_data.1/base/values", params(), sysj);
	
	return 0;
}

int command_ctable(int argc, const char * argv[])
{
	int r;
	ctable * sct;
	sys_journal * sysj = sys_journal::get_global_journal();
	
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = ctable_factory::setup("simple_ctable", AT_FDCWD, "msct_test", config, dtype::UINT32);
	EXPECT_NOFAIL("setup", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	sct = ctable_factory::load("simple_ctable", AT_FDCWD, "msct_test", config, sysj);
	EXPECT_NONULL("load", sct);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sct->insert(8u, "hello", blob("icanhas"));
	EXPECT_NOFAIL("sct->insert(8, hello)", r);
	run_iterator(sct);
	r = sct->insert(8u, "world", blob("cheezburger"));
	EXPECT_NOFAIL("sct->insert(8, world)", r);
	run_iterator(sct);
	
	wait_digest(1);
	
	r = sct->maintain();
	EXPECT_NOFAIL("sct->maintain()", r);
	run_iterator(sct);
	r = sct->remove(8u, "hello");
	EXPECT_NOFAIL("sct->remove(8, hello)", r);
	run_iterator(sct);
	r = sct->insert(10u, "foo", blob("bar"));
	EXPECT_NOFAIL("sct->insert(10, foo)", r);
	run_iterator(sct);
	r = sct->remove(8u);
	EXPECT_NOFAIL("sct->remove(8)", r);
	run_iterator(sct);
	r = sct->insert(12u, "foo", blob("zot"));
	EXPECT_NOFAIL("sct->insert(12, foo)", r);
	run_iterator(sct);
	
	wait_digest(1);
	
	r = sct->maintain();
	EXPECT_NOFAIL("sct->maintain()", r);
	run_iterator(sct);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	delete sct;
	
	return 0;
}

int command_cctable(int argc, const char * argv[])
{
	int r;
	ctable * ct;
	ctable::colval values[3] = {{0}, {1}, {2}};
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = base->create(AT_FDCWD, "cctr_test", config, dtype::UINT32);
	EXPECT_NOFAIL("cct::create", r);
	
	ct = base->open(AT_FDCWD, "cctr_test", config, sysj);
	EXPECT_NONULL("cct::open", ct);
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	r = base->create(AT_FDCWD, "cctw_test", config, dtype::UINT32);
	EXPECT_NOFAIL("cct::create", r);
	
	ct = base->open(AT_FDCWD, "cctw_test", config, sysj);
	EXPECT_NONULL("cct::open", ct);
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
	EXPECT_NOFAIL("tx_end", r);
	
	wait_digest(3);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	ct = base->open(AT_FDCWD, "cctw_test", config, sysj);
	EXPECT_NONULL("cct::open", ct);
	run_iterator(ct);
	delete ct;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
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
	util::memset(buckets, 0, sizeof(*buckets) * CONS_TEST_BUCKETS);
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
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	if(argc > 1 && !strcmp(argv[1], "check"))
	{
		r = tx_start();
		EXPECT_NOFAIL("tx_start", r);
		
		ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config, sysj);
		EXPECT_NONULL("load", ct);
		
		printf("Consistency check: ");
		fflush(stdout);
		if(consistency_check(ct, buckets))
			printf("OK!\n");
		else
			EXPECT_NEVER("failed.");
		print_buckets(buckets);
		
		r = tx_end(0);
		EXPECT_NOFAIL("tx_end", r);
		return 0;
	}
	
	for(size_t i = 0; i < 50; i++)
		values[i].value = value;
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	r = ctable_factory::setup("column_ctable", AT_FDCWD, "cons_test", config, dtype::UINT32);
	EXPECT_NOFAIL("setup", r);
	
	ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config, sysj);
	EXPECT_NONULL("load", ct);
	for(uint32_t i = 0; i < 500; i++)
	{
		r = ct->insert(i, values, CONS_TEST_COLS);
		assert(r >= 0);
	}
	delete ct;
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	ct = ctable_factory::load("column_ctable", AT_FDCWD, "cons_test", config, sysj);
	EXPECT_NONULL("load", ct);
	
	printf("Consistency check: ");
	fflush(stdout);
	if(consistency_check(ct, buckets))
		printf("OK!\n");
	else
		EXPECT_NEVER("failed.");
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
	if(consistency_check(ct, buckets))
		printf("OK!\n");
	else
		EXPECT_NEVER("failed.");
	print_buckets(buckets);
	
	wait_digest(5);
	
	ct->maintain();
	delete ct;
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	return 0;
}

static bool durability_stop = false;

static void durable_death(int signal)
{
	durability_stop = true;
}

int command_durability(int argc, const char * argv[])
{
	params config;
	tx_id transaction_id;
	sys_journal * sysj = sys_journal::get_global_journal();
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	
	if(!check)
	{
		r = dtable_factory::setup("managed_dtable", AT_FDCWD, "dura_test", config, dtype::UINT32);
		EXPECT_NOFAIL("dtable::create", r);
	}
	
	dt = dtable_factory::load("managed_dtable", AT_FDCWD, "dura_test", config, sysj);
	EXPECT_NONULL("dtable_factory::load", dt);
	
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
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
	
	dt->destroy();
	
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

int command_rollover(int argc, const char * argv[])
{
	sys_journal * sysj;
	journal_dtable * normal;
	journal_dtable * temporary;
	journal_dtable::journal_dtable_warehouse warehouse;
	sys_journal::listener_id normal_id, temp_id;
	dtype::ctype key_type = dtype::UINT32;
	bool use_reverse = false;
	int r;
	
	blob_comparator * reverse = new reverse_blob_comparator;
	
	if(argc > 1 && !strcmp(argv[1], "-b"))
	{
		key_type = dtype::BLOB;
		if(argc > 2 && !strcmp(argv[2], "-r"))
			use_reverse = true;
	}
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
	EXPECT_NONULL("sysj spawn", sysj);
	normal_id = sys_journal::get_unique_id(false);
	temp_id = sys_journal::get_unique_id(true);
	printf("normal = %d, temp = %d\n", normal_id, temp_id);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	normal = warehouse.obtain(normal_id, key_type, sysj);
	temporary = warehouse.obtain(temp_id, key_type, sysj);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 2, warehouse.size());
	if(use_reverse)
	{
		r = normal->set_blob_cmp(reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
		r = temporary->set_blob_cmp(reverse);
		EXPECT_NOFAIL("temp set_cmp", r);
	}
	
	/* first test: insert records to normal and temporary, restart and see that they're gone */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = normal->insert(idtype(10, key_type), "key 10");
	EXPECT_NOFAIL("normal insert(10)", r);
	r = temporary->insert(idtype(20, key_type), "key 20");
	EXPECT_NOFAIL("temp insert(20)", r);
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, false);
	EXPECT_NONULL("sysj spawn", sysj);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = warehouse.lookup(normal_id);
	temporary = warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, warehouse.size());
	if(use_reverse)
	{
		r = normal->set_blob_cmp(reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	
	/* next test: create another temporary ID and roll it over, restart and see that it works */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	temp_id = sys_journal::get_unique_id(true);
	printf("temp = %d\n", temp_id);
	temporary = warehouse.obtain(temp_id, key_type, sysj);
	printf("temp = %p\n", temporary);
	EXPECT_SIZET("total", 2, warehouse.size());
	if(use_reverse)
	{
		r = temporary->set_blob_cmp(reverse);
		EXPECT_NOFAIL("temp set_cmp", r);
	}
	r = temporary->insert(idtype(30, key_type), "key 30");
	EXPECT_NOFAIL("temp insert(30)", r);
	EXPECT_SIZET("normal size", 1, normal->size());
	r = temporary->rollover(normal);
	EXPECT_NOFAIL("rollover", r);
	EXPECT_SIZET("normal size", 2, normal->size());
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
	EXPECT_NONULL("sysj spawn", sysj);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = warehouse.lookup(normal_id);
	temporary = warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	
	/* final test: check that rollover records clobber existing records */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	temp_id = sys_journal::get_unique_id(true);
	printf("temp = %d\n", temp_id);
	temporary = warehouse.obtain(temp_id, key_type, sysj);
	EXPECT_NONULL("temp", temporary);
	EXPECT_SIZET("total", 2, warehouse.size());
	if(use_reverse)
	{
		r = temporary->set_blob_cmp(reverse);
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
	r = temporary->rollover(normal);
	EXPECT_NOFAIL("rollover", r);
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	EXPECT_SIZET("total", 0, warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
	EXPECT_NONULL("sysj spawn", sysj);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = warehouse.lookup(normal_id);
	temporary = warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	
	/* filter and check the results */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sysj->filter();
	EXPECT_NOFAIL("filter", r);
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
	EXPECT_NONULL("sysj spawn", sysj);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = warehouse.lookup(normal_id);
	temporary = warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 1, warehouse.size());
	if(use_reverse)
	{
		EXPECT_SIZET("normal size", 0, normal->size());
		r = normal->set_blob_cmp(reverse);
		EXPECT_NOFAIL("normal set_cmp", r);
	}
	EXPECT_SIZET("normal size", 2, normal->size());
	run_iterator(normal);
	EXPECT_SIZET("key 10 size", 7, normal->find(idtype(10, key_type)).size());
	
	/* discard normal, filter and check the results */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	normal->discard();
	r = sysj->filter();
	EXPECT_NOFAIL("filter", r);
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
	EXPECT_NONULL("sysj spawn", sysj);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	normal = warehouse.lookup(normal_id);
	temporary = warehouse.lookup(temp_id);
	printf("normal = %p, temp = %p\n", normal, temporary);
	EXPECT_SIZET("total", 0, warehouse.size());
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj->deinit(true);
	delete sysj;
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	reverse->release();
	return 0;
}

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
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
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
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, false);
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
	util::rm_r(AT_FDCWD, "abtx_test");
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
	{
		/* run the performance test as well */
		bool use_temp = (argc > 2 && !strcmp(argv[2], "temp"));
		abort_perf(use_temp);
	}
	
	if(argc > 1 && !strcmp(argv[1], "effect"))
		/* run the performance effect test as well */
		abort_effect();
	
	return 0;
}

int command_stable(int argc, const char * argv[])
{
	int r;
	params config;
	simple_stable * sst;
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = simple_stable::create(AT_FDCWD, "msst_test", config, dtype::UINT32);
	EXPECT_NOFAIL("stable::create", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "msst_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	run_iterator(sst);
	r = sst->insert(5u, "twice", 10u);
	EXPECT_NOFAIL("sst->insert(5, twice)", r);
	run_iterator(sst);
	r = sst->insert(6u, "funky", "face");
	EXPECT_NOFAIL("sst->insert(6, funky)", r);
	run_iterator(sst);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "msst_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	run_iterator(sst);
	r = sst->insert(6u, "twice", 12u);
	EXPECT_NOFAIL("sst->insert(5, twice)", r);
	run_iterator(sst);
	r = sst->insert(5u, "zapf", "dingbats");
	EXPECT_NOFAIL("sst->insert(6, zapf)", r);
	run_iterator(sst);
	r = sst->remove(6u);
	EXPECT_NOFAIL("sst->remove(6)", r);
	run_iterator(sst);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
	delete sst;
	
	wait_digest(3);
	
	sst = new simple_stable;
	/* must start the transaction first since it will do maintenance */
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	r = sst->init(AT_FDCWD, "msst_test", config, sysj);
	EXPECT_NOFAIL("sst->init", r);
	r = sst->maintain();
	EXPECT_NOFAIL("sst->maintain()", r);
	r = tx_end(0);
	EXPECT_NOFAIL("tx_end", r);
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
	EXPECT_NOFAIL("params::parse", r);
	config.print();
	printf("\n");
	
	iterator_test("managed_dtable", "iter_test", config, count, verbose);
	
	return 0;
}

int command_blob_cmp(int argc, const char * argv[])
{
	int r;
	sys_journal * sysj;
	journal_dtable * jdt;
	sys_journal::listener_id jid;
	journal_dtable::journal_dtable_warehouse warehouse;
	blob_comparator * reverse = new reverse_blob_comparator;
	
	if(argc > 1 && !strcmp(argv[1], "perf"))
		return blob_cmp_perf(reverse);
	
	r = tx_start();
	EXPECT_NOFAIL("tx_start", r);
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, true);
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
	sysj = sys_journal::spawn_init("test_journal", &warehouse, NULL, false);
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
