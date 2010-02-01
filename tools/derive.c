/* This file is part of Anvil. Anvil is copyright 2007-2010 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>

enum mode {
	UNKNOWN,  /* haven't seen the first data line yet */
	DERIVE,   /* take pseudo-derivative, first line had integer data */
	TIMEBASE  /* only rebase time to zero, first line had real data */
};

static int parse_line(const char * line, double * time, int * rows, enum mode * mode)
{
	/* skip lines that don't start with numbers */
	if(*line < '0' || '9' < *line)
		return 0;
	if(*mode == UNKNOWN)
	{
		const char * space = strchr(line, ' ');
		assert(space);
		if(strchr(space, '.'))
			*mode = TIMEBASE;
		else
			*mode = DERIVE;
	}
	if(*mode == TIMEBASE)
	{
		double krows = 0;
		int r = sscanf(line, "%lf %lf", time, &krows);
		*rows = rint(krows * 1000);
		return (r == 3) ? 1 : -1;
	}
	return (sscanf(line, "%lf %d", time, rows) == 2) ? 1 : -1;
}

int main(void)
{
	int last_rows = 0, first = 1;
	double last_time = 0, first_time = 0;
	enum mode mode = UNKNOWN;
	char line[64];
	
	while(fgets(line, sizeof(line), stdin))
	{
		int this_rows;
		double this_time;
		
		if(!parse_line(line, &this_time, &this_rows, &mode))
			continue;
		
		if(mode == TIMEBASE)
		{
			if(first)
			{
				first_time = this_time;
				first = 0;
			}
			printf("%lg %lg\n", this_time - first_time, (double) this_rows / 1000);
			continue;
		}
		
		if(first)
		{
			first_time = this_time;
			last_time = first_time;
			last_rows = this_rows;
			first = 0;
			continue;
		}
		
		last_rows = this_rows - last_rows;
		last_time = this_time - last_time;
		printf("%lg %lg\n", this_time - first_time, last_rows / (last_time * 1000));
		
		last_rows = this_rows;
		last_time = this_time;
	}
	return 0;
}
