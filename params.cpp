#include <stdlib.h>

#include "dtable_factory.h"
#include "ctable_factory.h"
#include "index_factory.h"
#include "params.h"

bool params::hate_std_map_get(const istr & name, const param ** p) const
{
	value_map::const_iterator elt = values.find(name);
	if(elt == values.end())
		return false;
	*p = &(*elt).second;
	return true;
}

bool params::get(const istr & name, bool * value, bool dfl) const
{
	const param * p;
	if(!hate_std_map_get(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::BOOL)
		return false;
	*value = p->i;
	return true;
}

bool params::get(const istr & name, int * value, int dfl) const
{
	const param * p;
	if(!hate_std_map_get(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::INT)
		return false;
	*value = p->i;
	return true;
}

bool params::get(const istr & name, float * value, float dfl) const
{
	const param * p;
	if(!hate_std_map_get(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::FLT)
		return false;
	*value = p->f;
	return true;
}

bool params::get(const istr & name, istr * value, const istr & dfl) const
{
	const param * p;
	if(!hate_std_map_get(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::STR)
		return false;
	*value = p->s;
	return true;
}

bool params::get(const istr & name, params * value, const params & dfl) const
{
	const param * p;
	if(!hate_std_map_get(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::PRM)
		return false;
	*value = p->p;
	return true;
}

void params::print() const
{
	value_map::const_iterator iter = values.begin();
	for(iter = values.begin(); iter != values.end(); ++iter)
	{
		printf("\"%s\" ", (const char *) (*iter).first);
		switch((*iter).second.type)
		{
			case param::BOOL:
				printf("bool %s ", (*iter).second.b ? "true" : "false");
				break;
			case param::INT:
				printf("int %d ", (*iter).second.i);
				break;
			case param::FLT:
				printf("flt %f ", (*iter).second.f);
				break;
			case param::STR:
				printf("str \"%s\" ", (const char *) (*iter).second.s);
				break;
			case param::PRM:
				printf("prm [ ");
				(*iter).second.p.print();
				printf(" ] ");
				break;
		}
	}
}

void params::print_classes()
{
	size_t count;
	const istr * names;
	
	count = dtable_factory::list(&names);
	printf("%zu available dtable%s:", count, (count == 1) ? "" : "s");
	for(size_t i = 0; i < count; i++)
		printf(" %s", (const char *) names[i]);
	printf("\n");
	delete[] names;
	
	count = ctable_factory::list(&names);
	printf("%zu available ctable%s:", count, (count == 1) ? "" : "s");
	for(size_t i = 0; i < count; i++)
		printf(" %s", (const char *) names[i]);
	printf("\n");
	delete[] names;
	
	count = index_factory::list(&names);
	printf("%zu available ind%s:", count, (count == 1) ? "ex" : "ices");
	for(size_t i = 0; i < count; i++)
		printf(" %s", (const char *) names[i]);
	printf("\n");
	delete[] names;
}

int params::parse(const char * input, params * result)
{
	token_stream tokens(input);
	const char * token = tokens.next();
	if(!token || tokens.quoted() || strcmp(token, "config"))
		return -tokens.line();
	if(parse(&tokens, result) < 0 || tokens.next())
		return -tokens.line();
	return 0;
}

params::keyword params::parse_type(const char * type)
{
	if(!strcmp(type, "bool"))
		return BOOL;
	if(!strcmp(type, "int"))
		return INT;
	if(!strcmp(type, "float"))
		return FLOAT;
	if(!strcmp(type, "string"))
		return STRING;
	if(!strcmp(type, "class"))
		return CLASS;
	if(!strcmp(type, "class(dt)"))
		return CLASS_DT;
	if(!strcmp(type, "class(ct)"))
		return CLASS_CT;
	if(!strcmp(type, "class(idx)"))
		return CLASS_IDX;
	if(!strcmp(type, "config"))
		return CONFIG;
	return ERROR;
}

int params::parse(token_stream * tokens, params * result)
{
	const char * token = tokens->next();
	if(!token || tokens->quoted() || strcmp(token, "["))
		return -1;
	token = tokens->next();
	if(!token)
		return -1;
	/* names must be quoted */
	while(tokens->quoted())
	{
		istr name(token);
		keyword type;
		/* get the type */
		token = tokens->next();
		if(!token || tokens->quoted())
			return -1;
		type = parse_type(token);
		if(type == ERROR)
			return -1;
		if(type == CONFIG)
		{
			params sub;
			parse(tokens, &sub);
			result->set(name, sub);
		}
		else
		{
			/* value */
			token = tokens->next();
			if(!token)
				return -1;
			/* strings, and only strings, should be quoted */
			if((type == STRING) != tokens->quoted())
				return -1;
			switch(type)
			{
				case BOOL:
					if(!strcmp(token, "true"))
						result->set(name, true);
					else if(!strcmp(token, "false"))
						result->set(name, false);
					else
						return -1;
					break;
				case INT:
				{
					char * end = NULL;
					long i = strtol(token, &end, 0);
					if(end && *end)
						return -1;
					result->set(name, (int) i);
					break;
				}
				case FLOAT:
				{
					char * end = NULL;
					double d = strtod(token, &end);
					if(end && *end)
						return -1;
					result->set(name, (float) d);
					break;
				}
				case STRING:
					result->set(name, token);
					break;
				case CLASS:
				case CLASS_DT:
				case CLASS_CT:
				case CLASS_IDX:
				{
					istr cppclass(token);
					if(type == CLASS)
					{
						size_t length = strlen(token);
						for(size_t i = 0; i < length; i++)
						{
							char c = token[i];
							if(i && '0' <= c && c <= '9')
								continue;
							if('A' <= c && c <= 'Z')
								continue;
							if('a' <= c && c <= 'z')
								continue;
							if(c == '_')
								continue;
							/* not a valid identifier */
							return -1;
						}
					}
					if(type == CLASS_DT && !dtable_factory::lookup(cppclass))
						return -1;
					if(type == CLASS_CT && !ctable_factory::lookup(cppclass))
						return -1;
					if(type == CLASS_IDX && !index_factory::lookup(cppclass))
						return -1;
					result->set(name, cppclass);
					break;
				}
				/* not used here, but keep the compiler from
				 * warning about unused enumeration values */
				case CONFIG:
				case ERROR:
					;
			}
		}
		/* next loop */
		token = tokens->next();
		if(!token)
			return -1;
	}
	return strcmp(token, "]") ? -1 : 0;
}
