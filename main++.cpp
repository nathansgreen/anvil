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
};

int command_itable(int argc, const char * argv[])
{
	itable_disk tbl;
	itable_overlay ovr;
	itable::it iter;
	const char * col;
	iv_int row;
	off_t off;
	int r;
	if(argc < 2)
		return 0;
	r = tbl.init(AT_FDCWD, argv[1]);
	printf("tbl.init(%s) = %d\n", argv[1], r);
	if(r < 0)
		return r;
	r = tbl.iter(&iter);
	printf("tbl.iter() = %d\n", r);
	if(r < 0)
		return r;
	while(!(r = tbl.next(&iter, &row, &col, &off)))
		printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
	printf("tbl.next() = %d\n", r);
	r = tbl.iter(&iter);
	printf("tbl.iter() = %d\n", r);
	if(r < 0)
		return r;
	while(!(r = tbl.next(&iter, &row)))
		printf("row = 0x%x\n", row);
	printf("tbl.next() = %d\n", r);
	row /= 2;
	r = tbl.iter(&iter, row);
	printf("tbl.iter(0x%x) = %d\n", row, r);
	if(r >= 0)
	{
		while(!(r = tbl.next(&iter, &row, &col, &off)))
			printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
		printf("tbl.next() = %d\n", r);
	}
	r = ovr.init(&tbl, NULL);
	printf("ovr.init(tbl) = %d\n", r);
	if(r >= 0)
	{
		r = ovr.iter(&iter);
		printf("ovr.iter() = %d\n", r);
		if(r < 0)
			return r;
		while(!(r = ovr.next(&iter, &row, &col, &off)))
			printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
		printf("ovr.next() = %d\n", r);
		r = ovr.iter(&iter);
		printf("ovr.iter() = %d\n", r);
		if(r < 0)
			return r;
		while(!(r = ovr.next(&iter, &row)))
			printf("row = 0x%x\n", row);
		printf("ovr.next() = %d\n", r);
		row /= 2;
		r = ovr.iter(&iter, row);
		printf("ovr.iter(0x%x) = %d\n", row, r);
		if(r >= 0)
		{
			while(!(r = ovr.next(&iter, &row, &col, &off)))
				printf("row = 0x%x, col = %s, offset = 0x%x\n", row, col, (int) off);
			printf("ovr.next() = %d\n", r);
		}
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
