/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>

#include "toilet.h"

int main(void)
{
	int r;
	toilet * toilet = toilet_open("hand-built", NULL);
	if(toilet)
		toilet_close(toilet);
	else
		fprintf(stderr, "Warning: failed to open toilet! ('hand-built')\n");
	
	r = toilet_new("test");
	if(r < 0)
		fprintf(stderr, "Error: failed to create toilet! ('test')\n");
	else
	{
		toilet = toilet_open("test", NULL);
		if(toilet)
		{
			t_gtable * gtable;
			r = toilet_new_gtable(toilet, "testgt");
			if(r < 0)
				fprintf(stderr, "Warning: failed to create gtable! ('testgt')\n");
			gtable = toilet_get_gtable(toilet, "testgt");
			if(gtable)
			{
				t_row_id id;
				if(toilet_new_row(toilet, gtable, &id) < 0)
					fprintf(stderr, "Failed to create row!\n");
				else
				{
					t_row * row;
					printf("New row ID is 0x%08x\n", id);
					row = toilet_get_row(toilet, id);
					if(row)
						toilet_put_row(toilet, row);
					else
						fprintf(stderr, "Error: failed to get row!\n");
				}
				toilet_put_gtable(toilet, gtable);
			}
			else
				fprintf(stderr, "Error: failed to open gtable!\n");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Error: failed to open toilet! ('test')\n");
	}
	
	return 0;
}
