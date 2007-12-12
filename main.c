/* This file is part of Toilet. Toilet is copyright 2005-2007 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>

#include "toilet.h"

int main(void)
{
	toilet * toilet = toilet_open("test");
	if(toilet)
	{
		int r;
		toilet_close(toilet);
		
		r = toilet_new("test.new");
		if(r < 0)
			fprintf(stderr, "Failed to create toilet! (test.new)\n");
		toilet = toilet_open("test.new");
		if(toilet)
		{
			t_gtable * gtable;
			toilet_new_gtable(toilet, "testgt");
			gtable = toilet_get_gtable(toilet, "testgt");
			if(gtable)
			{
				t_row_id id;
				if(toilet_new_row(toilet, gtable, &id) < 0)
					printf("New row ID is 0x%08x\n", id);
				else
					fprintf(stderr, "Failed to create row!\n");
				toilet_put_gtable(toilet, gtable);
			}
			else
				fprintf(stderr, "Failed to open gtable!\n");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Failed to open toilet! (test.new)\n");
	}
	else
		fprintf(stderr, "Failed to open toilet! (test)\n");
	return 0;
}
