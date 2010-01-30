#include <stdio.h>
#include <stdlib.h>

static int parse_line(const char * line, double * time, int * rows)
{
	/* skip lines that don't start with numbers */
	if(*line < '0' || '9' < *line)
		return 0;
	return (sscanf(line, "%lf %d", time, rows) == 2) ? 1 : -1;
}

int main(void)
{
	int last_rows = 0, first = 1;
	double last_time = 0, first_time = 0;
	char line[64];
	
	while(fgets(line, sizeof(line), stdin))
	{
		int this_rows;
		double this_time;
		
		if(!parse_line(line, &this_time, &this_rows))
			continue;
		
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
		printf("%lg %lg\n", this_time - first_time, last_rows / last_time);
		
		last_rows = this_rows;
		last_time = this_time;
	}
	return 0;
}
