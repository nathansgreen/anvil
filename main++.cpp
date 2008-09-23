/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <time.h>
#include <ctype.h>
#include <stdio.h>
#include <sys/time.h>

#include "openat.h"
#include "transaction.h"

#include "dtable.h"
#include "ctable.h"
#include "stable.h"
#include "sys_journal.h"
#include "cache_dtable.h"
#include "simple_dtable.h"
#include "managed_dtable.h"
#include "ustr_dtable.h"
#include "simple_ctable.h"
#include "simple_stable.h"
#include "reverse_blob_comparator.h"

extern "C" {
int command_info(int argc, const char * argv[]);
int command_dtable(int argc, const char * argv[]);
int command_ctable(int argc, const char * argv[]);
int command_stable(int argc, const char * argv[]);
int command_blob_cmp(int argc, const char * argv[]);
int command_performance(int argc, const char * argv[]);
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
	
	params config;
	config.set_class("base", simple_dtable);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, "managed_dtable", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->append(4u, blob(5, "hello"));
	printf("mdt->append = %d\n", r);
	r = mdt->append(2u, blob(5, "world"));
	printf("mdt->append = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", config);
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
	r = mdt->init(AT_FDCWD, "managed_dtable", config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = mdt->append(6u, blob(7, "icanhas"));
	printf("mdt->append = %d\n", r);
	r = mdt->append(0u, blob(11, "cheezburger"));
	printf("mdt->append = %d\n", r);
	run_iterator(mdt);
	r = mdt->digest();
	printf("mdt->digest = %d\n", r);
	run_iterator(mdt);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete mdt;
	
	mdt = new managed_dtable;
	r = mdt->init(AT_FDCWD, "managed_dtable", config);
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
	r = mdt->init(AT_FDCWD, "managed_dtable", config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	run_iterator(mdt);
	delete mdt;
	
	return 0;
}

int command_ctable(int argc, const char * argv[])
{
	int r;
	managed_dtable * mdt;
	simple_ctable * sct;
	
	params config;
	config.set_class("base", ustr_dtable);
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = managed_dtable::create(AT_FDCWD, "managed_ctable", config, dtype::UINT32);
	printf("dtable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	mdt = new managed_dtable;
	sct = new simple_ctable;
	r = mdt->init(AT_FDCWD, "managed_ctable", config);
	printf("mdt->init = %d, %zu disk dtables\n", r, mdt->disk_dtables());
	r = sct->init(mdt);
	printf("sct->init = %d\n", r);
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = sct->append(8u, "hello", blob(7, "icanhas"));
	printf("sct->append(8, hello) = %d\n", r);
	run_iterator(sct);
	r = sct->append(8u, "world", blob(11, "cheezburger"));
	printf("sct->append(8, world) = %d\n", r);
	run_iterator(sct);
	r = mdt->combine();
	printf("mdt->combine() = %d\n", r);
	run_iterator(sct);
	r = sct->remove(8u, "hello");
	printf("sct->remove(8, hello) = %d\n", r);
	run_iterator(sct);
	r = sct->append(10u, "foo", blob(3, "bar"));
	printf("sct->append(10, foo) = %d\n", r);
	run_iterator(sct);
	r = sct->remove(8u);
	printf("sct->remove(8) = %d\n", r);
	run_iterator(sct);
	r = sct->append(12u, "foo", blob(3, "zot"));
	printf("sct->append(10, foo) = %d\n", r);
	run_iterator(sct);
	r = mdt->combine();
	printf("mdt->combine() = %d\n", r);
	run_iterator(sct);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sct;
	delete mdt;
	
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
		"data" class(dt) cache_dtable
		"data_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class ustr_dtable
				"fastbase" class simple_dtable
				"digest_interval" int 2
			]
		]
		"columns" class(ct) simple_ctable
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "simple_stable", config, dtype::UINT32);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "simple_stable", config);
	printf("sst->init = %d\n", r);
	r = tx_start();
	printf("tx_start = %d\n", r);
	run_iterator(sst);
	r = sst->append(5u, "twice", 10u);
	printf("sst->append(5, twice) = %d\n", r);
	run_iterator(sst);
	r = sst->append(6u, "funky", "face");
	printf("sst->append(6, funky) = %d\n", r);
	run_iterator(sst);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "simple_stable", config);
	printf("sst->init = %d\n", r);
	r = tx_start();
	printf("tx_start = %d\n", r);
	run_iterator(sst);
	r = sst->append(6u, "twice", 12u);
	printf("sst->append(5, twice) = %d\n", r);
	run_iterator(sst);
	r = sst->append(5u, "zapf", "dingbats");
	printf("sst->append(6, zapf) = %d\n", r);
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
	r = sst->init(AT_FDCWD, "simple_stable", config);
	printf("sst->init = %d\n", r);
	r = sst->maintain();
	printf("sst->maintain() = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	delete sst;
	
	return 0;
}

#define ROW_COUNT 50000

int command_blob_cmp(int argc, const char * argv[])
{
	int r;
	struct timeval start, end;
	reverse_blob_comparator reverse;
	sys_journal::listener_id jid;
	journal_dtable jdt;
	
	/* let reverse know it's on the stack */
	reverse.on_stack();
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	jid = sys_journal::get_unique_id();
	if(jid == sys_journal::NO_ID)
		return -EBUSY;
	r = jdt.init(dtype::BLOB, jid, NULL);
	printf("jdt.init = %d\n", r);
	r = jdt.set_blob_cmp(&reverse);
	printf("jdt.set_blob_cmp = %d\n", r);
	for(int i = 0; i < 10; i++)
	{
		uint32_t keydata = rand();
		uint8_t valuedata = i;
		blob key(sizeof(keydata), &keydata);
		blob value(sizeof(valuedata), &valuedata);
		jdt.append(dtype(key), value);
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
				"combine_interval" int 6
				"combine_count" int 4
			]
		]
		"data" class(dt) cache_dtable
		"data_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class ustr_dtable
				"fastbase" class simple_dtable
				"digest_interval" int 2
				"combine_interval" int 8
				"combine_count" int 5
			]
		]
		"columns" class(ct) simple_ctable
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "cmptest", config, dtype::BLOB);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmptest", config);
	printf("sst->init = %d\n", r);
	r = sst->set_blob_cmp(&reverse);
	printf("sst->set_blob_cmp = %d\n", r);
	
	printf("Start timing! (400000 reverse blob key appends to %d rows)\n", ROW_COUNT);
	gettimeofday(&start, NULL);
	
	for(uint32_t i = 0; i < 400000; i++)
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
		r = sst->append(key, column, current);
		if(r < 0)
			goto fail_append;
		if((i % 40000) == 39999)
		{
			gettimeofday(&end, NULL);
			end.tv_sec -= start.tv_sec;
			if(end.tv_usec < start.tv_usec)
			{
				end.tv_usec += 1000000;
				end.tv_sec--;
			}
			end.tv_usec -= start.tv_usec;
			printf("%d%% done after %d.%06d seconds.\n", (i + 1) / 4000, (int) end.tv_sec, (int) end.tv_usec);
		}
		if((i % 10000) == 9999)
		{
			r = sst->maintain();
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
	
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "cmptest", config);
	printf("sst->init = %d\n", r);
	r = sst->set_blob_cmp(&reverse);
	printf("sst->set_blob_cmp = %d\n", r);
	r = sst->maintain();
	printf("sst->maintain = %d\n", r);
	
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
fail_append:
	tx_end(0);
fail_tx_start:
	delete sst;
	return r;
}

static const istr column_names[] = {"c_one", "c_two", "c_three", "c_four", "c_five"};
#define COLUMN_NAMES (sizeof(column_names) / sizeof(column_names[0]))

int command_performance(int argc, const char * argv[])
{
	int r;
	simple_stable * sst;
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
				"combine_count" int 5
			]
		]
		"data" class(dt) cache_dtable
		"data_config" config [
			"cache_size" int 40000
			"base" class(dt) managed_dtable
			"base_config" config [
				"base" class ustr_dtable
				"fastbase" class simple_dtable
				"digest_interval" int 2
				"combine_interval" int 8
				"combine_count" int 5
			]
		]
		"columns" class(ct) simple_ctable
	]), &config);
	printf("params::parse = %d\n", r);
	config.print();
	printf("\n");
	
	r = tx_start();
	printf("tx_start = %d\n", r);
	r = simple_stable::create(AT_FDCWD, "perftest", config, dtype::UINT32);
	printf("stable::create = %d\n", r);
	r = tx_end(0);
	printf("tx_end = %d\n", r);
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perftest", config);
	printf("sst->init = %d\n", r);
	
	for(uint32_t i = 0; i < ROW_COUNT; i++)
		for(uint32_t j = 0; j < COLUMN_NAMES; j++)
			table_copy[i][j] = (uint32_t) -1;
	
	printf("Start timing! (2000000 appends to %d rows)\n", ROW_COUNT);
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
		r = sst->append(row, column_names[column], table_copy[row][column]);
		if(r < 0)
			goto fail_append;
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
	
	delete sst;
	
	sst = new simple_stable;
	r = sst->init(AT_FDCWD, "perftest", config);
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
	
	delete sst;
	return 0;
	
fail_verify:
	printf("failed!\n");
	delete sst;
	return -1;
	
fail_maintain:
fail_append:
	tx_end(0);
fail_tx_start:
	delete sst;
	return r;
}
