/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <stdlib.h>

#include "dtable_factory.h"
#include "ctable_factory.h"
#include "index_factory.h"
#include "blob_buffer.h"
#include "params.h"
#include "util.h"

bool params::simple_find(const istr & name, const param ** p) const
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
	if(!simple_find(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::BOOL)
		return false;
	*value = p->b;
	return true;
}

bool params::get(const istr & name, int * value, int dfl) const
{
	const param * p;
	if(!simple_find(name, &p))
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
	if(!simple_find(name, &p))
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
	if(!simple_find(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::STR)
		return false;
	*value = p->s;
	return true;
}

bool params::get(const istr & name, blob * value, const blob & dfl) const
{
	const param * p;
	if(!simple_find(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::BLB)
		return false;
	*value = p->bl;
	return true;
}

bool params::get(const istr & name, params * value, const params & dfl) const
{
	const param * p;
	if(!simple_find(name, &p))
	{
		*value = dfl;
		return true;
	}
	if(p->type != param::PRM)
		return false;
	*value = p->p;
	return true;
}

bool params::has(const istr & name) const
{
	const param * p;
	return simple_find(name, &p);
}

bool params::get_blob_or_string(const istr & name, blob * value, const blob & dfl) const
{
	if(!get(name, value, dfl))
	{
		istr str;
		if(!get(name, &str))
			return false;
		*value = blob(str);
	}
	return true;
}

bool params::get_int_or_blob(const istr & name, int * value, int dfl) const
{
	if(!get(name, value, dfl))
	{
		blob blb;
		if(!get(name, &blb))
			return false;
		/* invalid size blobs count as type failures */
		if(!blb.size() || blb.size() > sizeof(uint32_t))
			return false;
		*value = util::read_bytes(&blb[0], 0, blb.size());
	}
	return true;
}

template<class T>
bool params::get_seq_impl(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<T> * value, const T & dfl) const
{
	size_t length = prefix.length() + postfix.length() + 32;
	const char * pre = prefix ? prefix.str() : "";
	const char * post = postfix ? postfix.str() : "";
	value->clear();
	for(size_t i = 0; variable || i < count; i++)
	{
		T index_value;
		char name[length];
		snprintf(name, length, "%s%zu%s", pre, i, post);
		istr iname(name);
		if(variable && !has(iname))
			break;
		/* std::vector<bool> strikes again! We can't just push_back a default T and then pass
		 * the address of the vector's copy, because that won't work with packed bools. */
		if(!get(iname, &index_value, dfl))
			return false;
		assert(value->size() == i);
		value->push_back(index_value);
	}
	return true;
}

/* force get_seq_impl to instantiate for the six types we need */
template bool params::get_seq_impl<bool>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<bool> * value, const bool & dfl) const;
template bool params::get_seq_impl<int>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<int> * value, const int & dfl) const;
template bool params::get_seq_impl<float>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<float> * value, const float & dfl) const;
template bool params::get_seq_impl<istr>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<istr> * value, const istr & dfl) const;
template bool params::get_seq_impl<blob>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<blob> * value, const blob & dfl) const;
template bool params::get_seq_impl<params>(const istr & prefix, const istr & postfix, size_t count, bool variable, std::vector<params> * value, const params & dfl) const;

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
			case param::BLB:
				printf("blb ");
				if((*iter).second.bl.exists())
				{
					if((*iter).second.bl.size())
						for(size_t i = 0; i < (*iter).second.bl.size(); i++)
							printf("%02X", (*iter).second.bl[i]);
					else
						printf("empty");
				}
				else
					printf("dne");
				printf(" ");
				break;
			case param::PRM:
				printf("prm [ ");
				(*iter).second.p.print();
				printf("] ");
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
	if(!strcmp(type, "blob"))
		return BLOB;
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
				case BLOB:
				{
					blob value;
					if(!strcmp(token, "empty"))
						value = blob::empty;
					else if(strcmp(token, "dne"))
					{
						size_t length = strlen(token);
						/* hex strings should have even length */
						if(length % 2)
							return -1;
						blob_buffer buffer(length / 2);
						for(size_t i = 0; i < length; i += 2)
						{
							uint8_t byte = 0;
							for(size_t b = 0; b < 2; b++)
							{
								char c = token[i + b];
								if('0' <= c && c <= '9')
									byte = (byte * 16) + c - '0';
								else if('A' <= c && c <= 'F')
									byte = (byte * 16) + 10 + c - 'A';
								else if('a' <= c && c <= 'f')
									byte = (byte * 16) + 10 + c - 'a';
								else
									/* not a valid hex digit */
									return -1;
							}
							buffer << byte;
						}
						value = buffer;
					}
					result->set(name, value);
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
