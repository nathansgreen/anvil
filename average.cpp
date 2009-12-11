/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define __STDC_FORMAT_MACROS

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include <string>
#include <vector>

/* Many of the benchmarks in Anvil print messages that end with "N seconds." or
 * "N seconds elapsed.". We like to run benchmarks a bunch of times and take the
 * average, so we can either write benchmark code that does that, or postprocess
 * the output of multiple runs. This utility does the latter: it finds the lines
 * that contain timing results, matches them with the same lines in other runs,
 * and averages the times. It also reports min and max values so it is easier to
 * tell if one of the runs somehow took an unusually long (or short) time. */

static std::vector<std::string> messages;
static std::vector<uint64_t> times;
static std::vector<bool> elapsed;

static size_t trim_nl(char * line)
{
	size_t length;
	for(length = 0; line[length]; length++)
		if(line[length] == '\n' || line[length] == '\r')
		{
			line[length] = 0;
			break;
		}
	return length;
}

static void process_line(char * line)
{
	uint64_t usec;
	bool elapsed_found = false;
	size_t length = trim_nl(line);
	if(length < 12)
		return;
	if(strcmp(&line[length -= 9], " seconds."))
	{
		if(length < 11)
			return;
		if(strcmp(&line[length -= 8], " seconds elapsed."))
			return;
		elapsed_found = true;
	}
	line[length] = 0;
	while(length && line[length] != '.')
		length--;
	if(!length)
		return;
	usec = atoi(&line[length + 1]);
	while(length && line[length] != ' ')
		length--;
	if(!length)
		return;
	usec += atoi(&line[length + 1]) * 1000000;
	line[length] = 0;
	messages.push_back(line);
	times.push_back(usec);
	elapsed.push_back(elapsed_found);
}

static int print_summary(size_t count)
{
	size_t each = times.size() / count;
	if(times.size() % count)
	{
		fprintf(stderr, "Input time count (%zu) not a multiple of run count (%zu)!\n", times.size(), count);
		return -1;
	}
	for(size_t i = 0; i < each; i++)
	{
		uint64_t total = 0, min = 0, max = 0;
		for(size_t j = 0; j < count; j++)
		{
			size_t index = i + j * each;
			total += times[index];
			if(!j || times[index] < min)
				min = times[index];
			if(times[index] > max)
				max = times[index];
			if(messages[i] != messages[index])
			{
				fprintf(stderr, "Messages don't match:\n  %s\n  %s\n", messages[i].c_str(), messages[index].c_str());
				return -1;
			}
		}
		total /= count;
		printf("%s %"PRIu64".%06"PRIu64" seconds%s. ", messages[i].c_str(), total / 1000000, total % 1000000, elapsed[i] ? " elapsed" : "");
		printf("[%"PRIu64".%06"PRIu64", %"PRIu64".%06"PRIu64"]\n", min / 1000000, min % 1000000, max / 1000000, max % 1000000);
	}
	return 0;
}

int main(int argc, char * argv[])
{
	char line[256];
	int count = 5;
	FILE * input = stdin;
	if(argc > 1 && !strcmp(argv[1], "-n"))
	{
		if(argc > 2)
		{
			count = atoi(argv[2]);
			if(count < 2)
			{
				fprintf(stderr, "%s: argument to -n must be at least 2\n", argv[0]);
				return 1;
			}
		}
		else
		{
			fprintf(stderr, "%s: option -n requires an argument\n", argv[0]);
			return 1;
		}
		argc -= 2;
		argv += 2;
	}
	if(argc > 1)
	{
		input = fopen(argv[1], "r");
		if(!input)
		{
			perror(argv[1]);
			return 1;
		}
	}
	fgets(line, sizeof(line), input);
	while(!feof(input) && !ferror(input))
	{
		process_line(line);
		fgets(line, sizeof(line), input);
	}
	if(input != stdin)
		fclose(input);
	return (print_summary(count) < 0) ? 1 : 0;
}
