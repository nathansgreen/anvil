/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <stdio.h>

#include "openat.h"
#include "itable.h"
#include "itable_overlay.h"
#include "atable.h"

extern "C" {
int command_itable(int argc, const char * argv[]);
int command_atable(int argc, const char * argv[]);
};

static int run_iter(itable * itable, const char * name)
{
	itable::it iter;
	const char * col;
	iv_int row = 0;
	off_t off;
	int r;
	r = itable->iter(&iter);
	printf("%s.iter() = %d\n", name, r);
	if(r < 0)
		return r;
	while(!(r = itable->next(&iter, &row, &col, &off)))
		printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
	printf("%s.next() = %d\n", name, r);
	r = itable->iter(&iter);
	printf("%s.iter() = %d\n", name, r);
	if(r < 0)
		return r;
	while(!(r = itable->next(&iter, &row)))
		printf("row = 0x%x\n", row);
	printf("%s.next() = %d\n", name, r);
	row /= 2;
	r = itable->iter(&iter, row);
	printf("%s.iter(0x%x) = %d\n", name, row, r);
	if(r >= 0)
	{
		while(!(r = itable->next(&iter, &row, &col, &off)))
			printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
		printf("%s.next() = %d\n", name, r);
	}
	return 0;
}
#define run_iter(it) run_iter(&(it), #it)

int command_itable(int argc, const char * argv[])
{
	itable_disk tbl;
	itable_overlay ovr;
	int r;
	if(argc < 2)
		return 0;
	r = tbl.init(AT_FDCWD, argv[1]);
	printf("tbl.init(%s) = %d\n", argv[1], r);
	if(r < 0)
		return r;
	r = run_iter(tbl);
	if(r < 0)
		return r;
	r = ovr.init(&tbl, NULL);
	printf("ovr.init(tbl) = %d\n", r);
	if(r >= 0)
	{
		r = run_iter(ovr);
		if(r < 0)
			return r;
	}
	if(argc > 2)
	{
		printf("%s -> %s\n", argv[1], argv[2]);
		r = tx_start();
		printf("tx_start() = %d\n", r);
		r = itable_disk::create(AT_FDCWD, argv[2], (argc % 2) ? (itable *) &tbl : (itable *) &ovr);
		printf("create(%s) = %d\n", (argc % 2) ? "tbl" : "ovr", r);
		r = tx_end(0);
		printf("tx_end() = %d\n", r);
		if(r >= 0)
		{
			argv[1] = argv[0];
			return command_itable(argc - 1, &argv[1]);
		}
	}
	return 0;
}

/* argv[1] is source itable name, argv[2] is new atable name, argv[3] is result itable name */
int command_atable(int argc, const char * argv[])
{
	atable atb;
	itable_disk tbl;
	itable_overlay ovr;
	itable::it iter;
	const char * col;
	iv_int row;
	off_t off;
	int r;
	if(argc < 3)
		return 0;
	r = tbl.init(AT_FDCWD, argv[1]);
	printf("tbl.init(%s) = %d\n", argv[1], r);
	if(r < 0)
		return r;
	r = tx_start();
	printf("tx_start() = %d\n", r);
	r = atb.init(AT_FDCWD, argv[2], tbl.k1_type(), tbl.k2_type());
	printf("atb.init(%s) = %d\n", argv[2], r);
	if(r < 0)
		return r;
	r = tx_end(0);
	printf("tx_end() = %d\n", r);
	r = ovr.init(&atb, &tbl, NULL);
	printf("ovr.init(atb, tbl) = %d\n", r);
	if(r < 0)
		return r;
	r = run_iter(tbl);
	if(r < 0)
		return r;
	r = run_iter(atb);
	if(r < 0)
		return r;
	r = run_iter(ovr);
	if(r < 0)
		return r;
	r = tx_start();
	printf("tx_start() = %d\n", r);
	r = atb.append(0x7777, "new", 0x10099);
	printf("atb.append(0x7777, new, 0x10099) = %d\n", r);
	r = atb.append(0x3333, "foo", 0x10000);
	printf("atb.append(0x3333, foo, 0x10000) = %d\n", r);
	r = atb.append(0x8888, "yum", 0x100AA);
	printf("atb.append(0x8888, yum, 0x100AA) = %d\n", r);
	r = tx_end(0);
	printf("tx_end() = %d\n", r);
	r = run_iter(atb);
	if(r < 0)
		return r;
	r = run_iter(ovr);
	if(r < 0)
		return r;
	r = atb.init(AT_FDCWD, argv[2], tbl.k1_type(), tbl.k2_type());
	printf("atb.init(%s) = %d\n", argv[2], r);
	if(r < 0)
		return r;
	r = run_iter(atb);
	if(r < 0)
		return r;
	if(argc > 3)
	{
		printf("(%s, %s) -> %s\n", argv[2], argv[1], argv[3]);
		r = tx_start();
		printf("tx_start() = %d\n", r);
		r = itable_disk::create(AT_FDCWD, argv[3], (itable *) &ovr);
		printf("create(ovr) = %d\n", r);
		r = tx_end(0);
		printf("tx_end() = %d\n", r);
		if(r < 0)
			return r;
		r = tbl.init(AT_FDCWD, argv[3]);
		printf("tbl.init(%s) = %d\n", argv[3], r);
		if(r < 0)
			return r;
		r = run_iter(tbl);
		if(r < 0)
			return r;
	}
	return 0;
}
