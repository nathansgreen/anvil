/* This file is part of Anvil. Anvil is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TOKEN_STREAM_H
#define __TOKEN_STREAM_H

#include <stddef.h>

#ifndef __cplusplus
#error token_stream.h is a C++ header file
#endif

#define TOKEN_SIZE 128

class token_stream
{
public:
	inline token_stream() : input(NULL) {}
	inline token_stream(const char * source) { init(source); }
	
	inline void init(const char * source)
	{
		input = source;
		position = 0;
		on_line = 1;
		is_quoted = false;
		token_error = false;
		token[0] = 0;
	}
	
	const char * next();
	
	inline const char * get_token() { return token_error ? NULL : token; }
	inline bool quoted() { return is_quoted; }
	inline bool error() { return token_error; }
	inline size_t line() { return on_line; }
	
private:
	const char * input;
	size_t position, on_line;
	bool is_quoted, token_error;
	char token[TOKEN_SIZE];
};

#endif /* __TOKEN_STREAM_H */
