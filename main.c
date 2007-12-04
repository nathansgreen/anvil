#include <stdlib.h>

#include "toilet.h"

int main(void)
{
	toilet * toilet = toilet_open("test");
	if(toilet)
		toilet_close(toilet);
	else
		fprintf(stderr, "Failed to open toilet!\n");
	return 0;
}
