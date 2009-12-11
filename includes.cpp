/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>

#include <string>
#include <set>
#include <map>

/* This is a quick tool for scanning a set of header files and generating a
 * graph of which ones include what other ones. It outputs dot format. Note that
 * it does not scan source files, only header files. Source files, since they do
 * not (generally) transitively include each other, would just be leaves. */

using namespace std;

typedef set<string> file_set;
typedef map<string, file_set> inclusion_map;

static inclusion_map inclusions;
static inclusion_map sections;
static file_set included;
static file_set prunes;

static void undotdot(string * path)
{
	for(size_t find = path->find("/../"); find != string::npos; find = path->find("/../"))
	{
		if(!find)
			/* /.. is the same as / */
			path->erase(0, 3);
		else
		{
			size_t start = path->rfind('/', find - 1);
			if(start == string::npos)
				path->erase(0, find + 4);
			else
				path->erase(start, find + 3 - start);
		}
	}
}

static void add_file(const string & path)
{
	inclusions.insert(pair<string, file_set>(path, file_set()));
}

static void add_include(const string & path, const string & target)
{
	file_set * set = &inclusions[path];
	string relative = target;
	size_t find = path.rfind('/');
	if(find != string::npos)
	{
		/* fix target to be path-relative */
		relative = path;
		relative.resize(find + 1);
		relative += target;
		undotdot(&relative);
	}
	set->insert(relative);
	included.insert(relative);
}

static void scan_pp_line(const string & path, char * line)
{
	size_t index = 1, null;
	/* skip whitespace */
	while(line[index] == ' ' || line[index] == '\t')
		index++;
	/* only process include lines */
	if(strncmp(&line[index], "include", 7))
		return;
	index += 7;
	/* skip whitespace */
	while(line[index] == ' ' || line[index] == '\t')
		index++;
	/* skip system <> includes, only process local "" includes */
	if(line[index] != '"')
		return;
	null = ++index;
	/* find the matching quote */
	while(line[null] && line[null] != '"')
		null++;
	line[null] = 0;
	add_include(path, &line[index]);
}

static int scan_include(const string & path)
{
	char line[512];
	FILE * input;
	input = fopen(path.c_str(), "r");
	if(!input)
	{
		perror(path.c_str());
		return -1;
	}
	add_file(path);
	while(fgets(line, sizeof(line), input))
		/* only scan preprocessor lines */
		if(line[0] == '#')
			scan_pp_line(path, line);
	if(ferror(input))
	{
		perror(path.c_str());
		fclose(input);
		return -1;
	}
	fclose(input);
	return 0;
}

static int iterate_resolve(void)
{
	bool effect;
	do {
		effect = false;
		inclusion_map::iterator iit = inclusions.begin();
		for(iit = inclusions.begin(); iit != inclusions.end(); ++iit)
		{
			file_set::iterator fit;
			for(fit = iit->second.begin(); fit != iit->second.end(); ++fit)
			{
				inclusion_map::iterator has = inclusions.find(*fit);
				if(has == inclusions.end())
				{
					if(scan_include((*fit).c_str()) < 0)
						return -1;
					effect = true;
				}
			}
		}
	} while(effect);
	return 0;
}

/* .ini-style grouping of files into clusters */
static void read_hmap(const string & hmap)
{
	char line[512];
	string section;
	FILE * input = fopen(hmap.c_str(), "r");
	if(!input)
		return;
	while(fgets(line, sizeof(line), input))
	{
		for(size_t i = 0; i < sizeof(line) && line[i]; i++)
			if(line[i] == '\r' || line[i] == '\n')
			{
				line[i] = 0;
				break;
			}
		if(line[0] == '[')
		{
			size_t length = strlen(line);
			if(line[length - 1] == ']')
			{
				line[length - 1] = 0;
				section = &line[1];
			}
		}
		else if(line[0] && section.length())
			sections[section].insert(line);
	}
	fclose(input);
}

/* ignore leaf header files whose include list is exactly this */
static void read_hprune(const string & hprune)
{
	char line[512];
	string section;
	FILE * input = fopen(hprune.c_str(), "r");
	if(!input)
		return;
	while(fgets(line, sizeof(line), input))
	{
		for(size_t i = 0; i < sizeof(line) && line[i]; i++)
			if(line[i] == '\r' || line[i] == '\n')
			{
				line[i] = 0;
				break;
			}
		prunes.insert(line);
	}
	fclose(input);
}

static void generate_dot(void)
{
	printf("digraph includes {\n");
	printf("node [shape=ellipse,fontsize=9]\n");
	inclusion_map::iterator iit = inclusions.begin();
	/* list the nodes first, then the edges... dot seems to lay things out better that way */
	for(iit = inclusions.begin(); iit != inclusions.end(); ++iit)
	{
		file_set::iterator fit = included.find(iit->first);
		if(fit == included.end() && (iit->second.empty() || iit->second == prunes))
			continue;
		printf("file_%p [label=\"%s\"];\n", &iit->first, iit->first.c_str());
	}
	for(iit = inclusions.begin(); iit != inclusions.end(); ++iit)
	{
		file_set::iterator fit = included.find(iit->first);
		if(fit == included.end() && (iit->second.empty() || iit->second == prunes))
			continue;
		for(fit = iit->second.begin(); fit != iit->second.end(); ++fit)
		{
			inclusion_map::iterator target = inclusions.find(*fit);
			printf("file_%p -> file_%p;\n", &iit->first, &target->first);
		}
	}
	/* dot clusters for grouping */
	for(iit = sections.begin(); iit != sections.end(); ++iit)
	{
		bool exists = false;
		file_set::iterator fit;
		for(fit = iit->second.begin(); fit != iit->second.end(); ++fit)
		{
			inclusion_map::iterator target = inclusions.find(*fit);
			if(target == inclusions.end())
				continue;
			if(!exists)
			{
				printf("subgraph cluster_%p {\n", &iit->first);
				printf("label=\"%s\";\n", iit->first.c_str());
				exists = true;
			}
			printf("file_%p;\n", &target->first);
		}
		if(exists)
			printf("}\n");
	}
	printf("}\n");
}

int main(int argc, char * argv[])
{
	string dir_prefix;
	struct dirent * ent;
	const char * dir = (argc > 1) ? argv[1] : ".";
	DIR * cwd = opendir(dir);
	if(!cwd)
	{
		perror(dir);
		return 1;
	}
	dir_prefix = dir;
	undotdot(&dir_prefix);
	if(dir_prefix == ".")
		dir_prefix = "";
	else
		dir_prefix += "/";
	read_hmap(dir_prefix + "header.map");
	read_hprune(dir_prefix + "header.prune");
	while((ent = readdir(cwd)))
	{
		struct stat st;
		size_t length = strlen(ent->d_name);
		if(length < 3)
			continue;
		/* skip hidden files */
		if(ent->d_name[0] == '.')
			continue;
		/* only scan header files */
		if(strcmp(&ent->d_name[length - 2], ".h"))
			continue;
		if(stat(ent->d_name, &st))
		{
			perror(ent->d_name);
			break;
		}
		/* skip non-regular files (directories, etc.) */
		if(!(st.st_mode & S_IFREG))
			continue;
		if(scan_include(ent->d_name) < 0)
			break;
	}
	closedir(cwd);
	if(!ent && iterate_resolve() >= 0)
		generate_dot();
	return 0;
}
