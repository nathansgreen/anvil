/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <assert.h>

#include "token_stream.h"

const char * token_stream::next()
{
	bool in_quote = 0;
	size_t size = 0;
	if(!input[position] || token_error)
		return NULL;
	is_quoted = false;
	for(;;)
	{
		char next = input[position];
		if(!next)
			break;
		position++;
		if(next == '\n')
			on_line++;
		/* handle quotes */
		if(next == '"')
		{
			is_quoted = true;
			in_quote = !in_quote;
			continue;
		}
		/* skip leading whitespace */
		if(!size && !is_quoted)
			if(next == ' ' || next == '\t' || next == '\r' || next == '\n')
				continue;
		/* handle end of token */
		if(!in_quote && (next == ' ' || next == '\t' || next == '\r' || next == '\n'))
			break;
		/* handle token length errors */
		if(size == sizeof(token) - 1)
		{
			token_error = true;
			return NULL;
		}
		/* add to token */
		token[size++] = next;
	}
	
	if(in_quote)
	{
		token_error = true;
		return NULL;
	}
	
	assert(size < sizeof(token));
	token[size] = 0;
	return (size || is_quoted) ? token : NULL;
}
