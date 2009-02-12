/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define __STDC_FORMAT_MACROS

#include <time.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>

#include "openat.h"
#include "transaction.h"

#include "dtable.h"
#include "ctable.h"
#include "stable.h"
#include "sys_journal.h"
#include "ustr_dtable.h"
#include "simple_dtable.h"
#include "managed_dtable.h"
#include "usstate_dtable.h"
#include "memory_dtable.h"
#include "simple_ctable.h"
#include "simple_stable.h"
#include "reverse_blob_comparator.h"

extern "C" {
int command_info(int argc, const char * argv[]);
int command_dtable(int argc, const char * argv[]);
int command_edtable(int argc, const char * argv[]);
int command_ussdtable(int argc, const char * argv[]);
int command_ctable(int argc, const char * argv[]);
int command_cctable(int argc, const char * argv[]);
int command_consistency(int argc, const char * argv[]);
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

static void print(blob x, const char * prefix = NULL)
{
	if(!x.exists())
	{
		if(prefix)
			printf("%s", prefix);
		printf("(non-existent)\n");
		return;
	}
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
		printf("%s:", (const char *) iter->column());
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
		"base" class(dt) simple_dtable
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
	
	table = dtable_factory::load("simple_dtable", AT_FDCWD, "usst_test", params());
	printf("dtable_factory::load = %p\n", table);
	run_iterator(table);
	delete table;
	
	mdt.insert(3u, "other");
	r = base->create(AT_FDCWD, "usst_fail", config, &mdt);
	printf("uss::create = %d (expect failure)\n", r);
	
	return 0;
}

int command_ctable(int argc, const char * argv[])
{
	int r;
	simple_ctable * sct;
	
	params config;
	r = params::parse(LITERAL(
	config [
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) ustr_dtable
			"digest_interval" int 1
			"combine_interval" int 1
			"combine_count" int 3
		]
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_ctable::create(AT_FDCWD, "msct_test", config, dtype::UINT32);
	printf("ctable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sct = new simple_ctable;
	r = sct->init(AT_FDCWD, "msct_test", config);
	printf("sct->init = %d\n", r);
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
	ctable::colval values[3] = {{"last"}, {"first"}, {"state"}};
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
					"reject_value" blob FD
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
	ctable::colval values[CONS_TEST_COLS] = {{"0"}, {"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"},
	                               {"10"}, {"11"}, {"12"}, {"13"}, {"14"}, {"15"}, {"16"}, {"17"}, {"18"}, {"19"},
	                               {"20"}, {"21"}, {"22"}, {"23"}, {"24"}, {"25"}, {"26"}, {"27"}, {"28"}, {"29"},
	                               {"30"}, {"31"}, {"32"}, {"33"}, {"34"}, {"35"}, {"36"}, {"37"}, {"38"}, {"39"},
	                               {"40"}, {"41"}, {"42"}, {"43"}, {"44"}, {"45"}, {"46"}, {"47"}, {"48"}, {"49"}};
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
					"base" class(dt) ustr_dtable
					"fastbase" class(dt) simple_dtable
					"digest_interval" int 2
				]
			]
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
	managed_dtable * mdt;
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
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, "iter_test", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	const char * layers[] = {"358", "079", "", "2457", "1267"};
#define LAYERS (sizeof(layers) / sizeof(layers[0]))
#define VALUES 10
	blob values[VALUES];
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "iter_test", config);
	printf("mdt->init = %d\n", r);
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
			r = mdt->insert(key * 2, values[key]);
			printf("mdt->insert(%d, %s) = %d\n", key, content, r);
		}
		run_iterator(mdt);
		
		printf("Waiting %d seconds for digest interval...\n", delay);
		sleep(delay);
		
		r = mdt->maintain();
		printf("mdt->maintain() = %d\n", r);
		run_iterator(mdt);
		
		r = tx_end(0);
		printf("tx_end = %d\n", r);
	}
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "iter_test", config);
	printf("mdt->init = %d\n", r);
	run_iterator(mdt);
	
	printf("Checking iterator behavior... ");
	fflush(stdout);
	dtable::iter * it = mdt->iterator();
	uint32_t it_pos = 0, ok = 1;
	for(size_t i = 0; i < count && ok; i++)
	{
		ok = 0;
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
		ok = 1;
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
						ok = 0;
				}
				else if(b)
					ok = 0;
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
						ok = 0;
					it_pos--;
				}
				else if(b)
					ok = 0;
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
					ok = 0;
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
		printf("failed!\n");
	delete it;
	delete mdt;
	
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
	journal_dtable jdt;

	char * keybuf, * valbuf;
	keybuf = new char[KEYSIZE];
	valbuf = new char[VALSIZE];
	assert(keybuf && valbuf);
	memset(keybuf, 'a', KEYSIZE);
	memset(valbuf, 'b', VALSIZE);

	blob key(KEYSIZE, &keybuf);
	blob value(VALSIZE, &valbuf);

	printf("BekeleyDB test timing! \n");
	gettimeofday(&start, NULL);

	r = tx_start();
	printf("tx_start = %d\n", r);
	jid = sys_journal::get_unique_id();
	if(jid == sys_journal::NO_ID)
		return -EBUSY;
	r = jdt.init(dtype::BLOB, jid);
	printf("jdt.init = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);

	for(int i = 0; i < 1000000; i++)
	{
		r = tx_start();
		if(r)
		{
			printf("TX error\n");
			break;
		}
		assert(jdt.insert(dtype(key), value) == 0);
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
		if(r)
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
		journal_dtable jdt;
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		jid = sys_journal::get_unique_id();
		if(jid == sys_journal::NO_ID)
			return -EBUSY;
		r = jdt.init(dtype::BLOB, jid);
		printf("jdt.init = %d\n", r);
		r = jdt.set_blob_cmp(&reverse);
		printf("jdt.set_blob_cmp = %d\n", r);
		for(int i = 0; i < 10; i++)
		{
			uint32_t keydata = rand();
			uint8_t valuedata = i;
			blob key(sizeof(keydata), &keydata);
			blob value(sizeof(valuedata), &valuedata);
			jdt.insert(dtype(key), value);
		}
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
		run_iterator(&jdt);
		
		r = jdt.reinit(jid, false);
		printf("jdt.reinit = %d\n", r);
		printf("current expected comparator: %s\n", (const char *) jdt.get_cmp_name());
		
		run_iterator(&jdt);
		
		r = sys_journal::get_global_journal()->get_entries(&jdt);
		printf("get_entries = %d (expect %d)\n", r, -EBUSY);
		if(r == -EBUSY)
		{
			printf("expect comparator: %s\n", (const char *) jdt.get_cmp_name());
			jdt.set_blob_cmp(&reverse);
			r = sys_journal::get_global_journal()->get_entries(&jdt);
			printf("get_entries = %d\n", r);
		}
		
		run_iterator(&jdt);
		
		r = tx_start();
		printf("tx_start = %d\n", r);
		r = jdt.reinit(jid);
		printf("jdt.reinit = %d\n", r);
		r = tx_end(0);
		printf("tx_end = %d\n", r);
		
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
					"base" class(dt) ustr_dtable
					"fastbase" class(dt) simple_dtable
					"digest_interval" int 2
					"combine_interval" int 8
					"combine_count" int 6
				]
			]
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
					"base" class(dt) ustr_dtable
					"fastbase" class(dt) simple_dtable
					"digest_interval" int 2
					"combine_interval" int 8
					"combine_count" int 6
				]
			]
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
