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
			toilet_new_gtable(toilet, "testgt");
			toilet_close(toilet);
		}
		else
			fprintf(stderr, "Failed to open toilet! (test.new)\n");
	}
	else
		fprintf(stderr, "Failed to open toilet! (test)\n");
	return 0;
}
