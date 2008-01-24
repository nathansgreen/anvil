/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>
#include <stdlib.h>

#include "toilet.h"

int main(void)
{
	int r = toilet_new("test");
	if(r < 0)
		fprintf(stderr, "Error: failed to create toilet! ('test')\n");
	else
	{
		toilet * toilet = toilet_open("test", NULL);
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
				if(toilet_new_row(gtable, &id) < 0)
					fprintf(stderr, "Error: failed to create row!\n");
				else
				{
					t_row * row;
					printf("New row ID is 0x" ROW_FORMAT "\n", id);
					row = toilet_get_row(toilet, id);
					if(row)
					{
						r = toilet_row_set_value(row, "key", T_STRING, (t_value *) "value");
						if(r < 0)
							fprintf(stderr, "Warning: failed to set value!\n");
						toilet_put_row(row);
						row = toilet_get_row(toilet, id);
						if(row)
						{
							char * value = (char *) toilet_row_value(row, "key", T_STRING);
							if(!value)
								fprintf(stderr, "Error: failed to get value!\n");
							else
								printf("Value is: %s\n", value);
							toilet_put_row(row);
						}
						else
							fprintf(stderr, "Error: failed to get row again!\n");
					}
					else
						fprintf(stderr, "Error: failed to get row!\n");
				}
				toilet_put_gtable(gtable);
			}
			else
				fprintf(stderr, "Error: failed to open gtable! ('testgt')\n");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Error: failed to open toilet! ('test')\n");
	}
	
	return 0;
}
