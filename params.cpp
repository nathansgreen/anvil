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
