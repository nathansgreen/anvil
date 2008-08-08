/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include "rwfile.h"
#include "stringset.h"
#include "counted_stringset.h"
#include "blob_buffer.h"
#include "ustr_dtable.h"

#define DEBUG_USTR 0

/* ustr dtable file format:
 * byte 0-3: magic number
 * byte 4-7: format version
 * bytes 8-11: key count
 * byte 12: key type (0 -> invalid, 1 -> uint32, 2 -> double, 3 -> string)
 * byte 13: key size (for uint32/string; 1-4 bytes)
 * byte 14: data length size (1-4 bytes)
 * byte 15: offset size (1-4 bytes)
 * bytes 16-19: duplicate string table offset (relative to data start)
 * byte 20: duplicate string index size (1-4 bytes)
 * byte 21: duplicate string escape sequence length (1-2 bytes)
 * byte 22-23: duplicate string escape sequence
 * byte 24-n: if key type is string, a string table
 * byte 24 or n+1: main data tables
 * byte m: duplicate string table (unless offset = 0)
 * 
 * main data tables:
 * key array:
 * [] = byte 0-m: key
 * [] = byte m+1-n: (unpacked) data length
 *      byte n+1-o: data offset (relative to data start)
 * each data blob:
 * [] = byte 0-m: data bytes */

ustr_dtable::iter::iter(const ustr_dtable * source)
	: index(0), sdt_source(source)
{
}

bool ustr_dtable::iter::valid() const
{
	return index < sdt_source->key_count;
}

bool ustr_dtable::iter::next()
{
	return ++index < sdt_source->key_count;
}

bool ustr_dtable::iter::prev()
{
	return (index - 1 > sdt_source->key_count) ? false : --index >= 0;
}

bool ustr_dtable::iter::last()
{
	index = sdt_source->key_count - 1;
	return index < sdt_source->key_count;
}

dtype ustr_dtable::iter::key() const
{
	return sdt_source->get_key(index);
}

bool ustr_dtable::iter::seek(const dtype & key)
{
	return sdt_source->find_key(key, NULL, NULL, &index) >= 0;
}

metablob ustr_dtable::iter::meta() const
{
	size_t data_length;
	sdt_source->get_key(index, &data_length);
	return data_length ? metablob(data_length) : metablob();
}

blob ustr_dtable::iter::value() const
{
	return sdt_source->get_value(index);
}

const dtable * ustr_dtable::iter::source() const
{
	return sdt_source;
}

dtable::iter * ustr_dtable::iterator() const
{
	return new iter(this);
}

dtype ustr_dtable::get_key(size_t index, size_t * data_length, off_t * data_offset) const
{
	assert(index < key_count);
	int r;
	uint8_t size = key_size + length_size + offset_size;
	uint8_t bytes[size];
	
	r = fp->read(key_start_off + size * index, bytes, size);
	assert(r == size);
	
	if(data_length)
		/* all data lengths are stored incremented by 1, to free up 0 for non-existent entries */
		*data_length = read_bytes(bytes, key_size, length_size) - 1;
	if(data_offset)
		*data_offset = read_bytes(bytes, key_size + length_size, offset_size);
	
	switch(ktype)
	{
		case dtype::UINT32:
			return dtype(read_bytes(bytes, 0, key_size));
		case dtype::DOUBLE:
		{
			double value;
			memcpy(&value, bytes, sizeof(double));
			return dtype(value);
		}
		case dtype::STRING:
			return dtype(st.get(read_bytes(bytes, 0, key_size)));
		case dtype::BLOB:
			/* fall through */ ;
	}
	abort();
}

int ustr_dtable::find_key(const dtype & key, size_t * data_length, off_t * data_offset, size_t * index) const
{
	/* binary search */
	ssize_t min = 0, max = key_count - 1;
	while(min <= max)
	{
		/* watch out for overflow! */
		ssize_t mid = min + (max - min) / 2;
		dtype value = get_key(mid, data_length, data_offset);
		if(value < key)
			min = mid + 1;
		else if(value > key)
			max = mid - 1;
		else
		{
			if(index)
				*index = mid;
			return 0;
		}
	}
	if(index)
		*index = min;
	return -ENOENT;
}

blob ustr_dtable::unpack_blob(const blob & source, size_t unpacked_size) const
{
	size_t last = source.size() - dup_escape_len - dup_index_size + 1;
	blob_buffer buffer(unpacked_size);
	for(size_t i = 0; i < last && buffer.size() < unpacked_size; i++)
	{
		if(!memcmp(dup_escape, &source[i], dup_escape_len))
		{
			ssize_t index = read_bytes(&source[0], i += dup_escape_len, dup_index_size);
			const char * string = dup.get(index);
			if(string)
			{
				uint8_t length = strlen(string);
				buffer << length;
				buffer.append(string, length);
			}
			else
				for(size_t j = i - dup_escape_len; j < i; j++)
					buffer << source[j];
			i += dup_index_size - 1;
		}
		else
			buffer << source[i];
	}
	for(size_t i = last; i < source.size() && buffer.size() < unpacked_size; i++)
		buffer << source[i];
	assert(buffer.size() == unpacked_size);
	return buffer;
}

blob ustr_dtable::get_value(size_t index, size_t data_length, off_t data_offset) const
{
	ssize_t read_length;
	blob_buffer value(data_length);
	value.set_size(data_length, false);
	assert(data_length == value.size());
	read_length = fp->read(data_start_off + data_offset, &value[0], data_length);
	value.set_size(read_length);
	/* the data length stored in the key record is the unpacked size */
	return dup_index_size ? unpack_blob(value, data_length) : blob(value);
}

blob ustr_dtable::get_value(size_t index) const
{
	assert(index < key_count);
	size_t data_length;
	off_t data_offset;
	dtype key = get_key(index, &data_length, &data_offset);
	/* the data length stored in the key record is the unpacked size */
	return (data_length != (size_t) -1) ? get_value(index, data_length, data_offset) : blob();
}

blob ustr_dtable::lookup(const dtype & key, const dtable ** source) const
{
	size_t data_length, index;
	off_t data_offset;
	int r = find_key(key, &data_length, &data_offset, &index);
	if(r < 0)
	{
		*source = NULL;
		return blob();
	}
	*source = this;
	if(data_length == (size_t) -1)
		return blob();
	/* the data length stored in the key record is the unpacked size */
	return get_value(index, data_length, data_offset);
}

int ustr_dtable::init(int dfd, const char * file, const params & config)
{
	int r = -1;
	struct dtable_header header;
	if(fp)
		deinit();
	/* the larger the buffers, the more memory we use but the fewer read() system calls we'll make... */
	fp = rofile::open<16, 16>(dfd, file);
	if(!fp)
		return -1;
	if(fp->read(0, &header) < 0)
		goto fail;
	if(header.magic != USDTABLE_MAGIC || header.version != USDTABLE_VERSION)
		goto fail;
	key_count = header.key_count;
	key_start_off = sizeof(header);
	key_size = header.key_size;
	length_size = header.length_size;
	offset_size = header.offset_size;
	switch(header.key_type)
	{
		case 1:
			ktype = dtype::UINT32;
			if(key_size > 4)
				goto fail;
			break;
		case 2:
			ktype = dtype::DOUBLE;
			if(key_size != sizeof(double))
				goto fail;
			break;
		case 3:
			ktype = dtype::STRING;
			if(key_size > 4)
				goto fail;
			r = st.init(fp, key_start_off);
			if(r < 0)
				goto fail;
			key_start_off += st.get_size();
			break;
		default:
			goto fail;
	}
	data_start_off = key_start_off + (key_size + length_size + offset_size) * key_count;
	
	/* now the duplicate string table */
	if(header.dup_offset)
	{
		dup_index_size = header.dup_index_size;
		dup_escape_len = header.dup_escape_len;
		memcpy(dup_escape, header.dup_escape, sizeof(dup_escape));
		r = dup.init(fp, data_start_off + header.dup_offset);
		if(r < 0)
			goto fail_st;
	}
	else
	{
		dup_index_size = 0;
		dup_escape_len = 0;
	}
	
	return 0;
	
fail_st:
	if(ktype == dtype::STRING)
		st.deinit();
fail:
	delete fp;
	fp = NULL;
	return (r < 0) ? r : -1;
}

void ustr_dtable::deinit()
{
	if(fp)
	{
		if(dup_index_size)
			dup.deinit();
		if(ktype == dtype::STRING)
			st.deinit();
		delete fp;
		fp = NULL;
	}
}

ssize_t ustr_dtable::locate_string(const char ** array, ssize_t size, const char * string)
{
	/* binary search */
	ssize_t min = 0, max = size - 1;
	while(min <= max)
	{
		int c;
		/* watch out for overflow! */
		ssize_t index = min + (max - min) / 2;
		c = strcmp(array[index], string);
		if(c < 0)
			min = index + 1;
		else if(c > 0)
			max = index - 1;
		else
			return index;
	}
	return -1;
}

size_t ustr_dtable::pack_size(const blob & source, const dtable_header & header, const char ** dups, ssize_t dup_count)
{
	size_t size = 0, escape = header.dup_escape_len + header.dup_index_size;
	for(size_t i = 0; i < source.size(); i++)
	{
		uint8_t byte = source[i];
		/* string finding heuristic */
		if(3 <= byte && byte < 48 && i + byte < source.size())
		{
			size_t j;
			for(j = 1; j <= byte; j++)
				if(!isprint(source[i + j]))
					break;
			if(j > byte)
			{
				istr string(&source.index<char>(i + 1), byte);
				ssize_t index = locate_string(dups, dup_count, string);
				if(index >= 0)
				{
					size += escape;
					i += byte;
					continue;
				}
			}
		}
		/* no duplicate string here, but check for escape sequence needing escaping */
		if(i + escape <= source.size())
			if(!memcmp(&source[i], header.dup_escape, header.dup_escape_len))
			{
				size += escape;
				i += header.dup_escape_len - 1;
				continue;
			}
		size++;
	}
	return size;
}

blob ustr_dtable::pack_blob(const blob & source, const dtable_header & header, const char ** dups, ssize_t dup_count)
{
	size_t escape = header.dup_escape_len + header.dup_index_size;
	size_t packed_size = pack_size(source, header, dups, dup_count);
	blob_buffer buffer(packed_size);
	for(size_t i = 0; i < source.size(); i++)
	{
		uint8_t byte = source[i];
		/* string finding heuristic */
		if(3 <= byte && byte < 48 && i + byte < source.size())
		{
			size_t j;
			for(j = 1; j <= byte; j++)
				if(!isprint(source[i + j]))
					break;
			if(j > byte)
			{
				istr string(&source.index<char>(i + 1), byte);
				ssize_t index = locate_string(dups, dup_count, string);
				if(index >= 0)
				{
					for(size_t k = 0; k < header.dup_escape_len; k++)
						buffer << header.dup_escape[k];
					buffer.layout_append(index, header.dup_index_size);
					i += byte;
					continue;
				}
			}
		}
		/* no duplicate string here, but check for escape sequence needing escaping */
		if(i + escape <= source.size())
			if(!memcmp(&source[i], header.dup_escape, header.dup_escape_len))
			{
				for(size_t k = 0; k < header.dup_escape_len; k++)
					buffer << header.dup_escape[k];
				for(size_t k = 0; k < header.dup_index_size; k++)
					buffer << (uint8_t) 0xFF;
				i += header.dup_escape_len - 1;
				continue;
			}
		buffer << byte;
	}
	assert(packed_size == buffer.size());
	return buffer;
}

int ustr_dtable::create(int dfd, const char * file, const params & config, const dtable * source, const dtable * shadow)
{
	stringset strings;
	counted_stringset dups;
	dtable::iter * iter;
	dtype::ctype key_type = source->key_type();
	const char ** string_array = NULL;
	const char ** dup_array = NULL;
	size_t key_count = 0, max_data_size = 0, total_data_size = 0;
	uint32_t max_key = 0;
	dtable_header header;
	int r, size;
	rwfile out;
	if(shadow && shadow->key_type() != key_type)
		return -EINVAL;
	if(key_type == dtype::BLOB)
		return -EINVAL;
	if(key_type == dtype::STRING)
	{
		r = strings.init();
		if(r < 0)
			return r;
	}
	/* reserve one duplicate string index for the escape sequence itself */
	dups.init(255);
	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		blob value = iter->value();
		size_t blob_size = value.size();
		iter->next();
		if(!value.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		key_count++;
		assert(key.type == key_type);
		switch(key.type)
		{
			case dtype::UINT32:
				if(key.u32 > max_key)
					max_key = key.u32;
				break;
			case dtype::DOUBLE:
				/* nothing to do */
				break;
			case dtype::STRING:
				strings.add(key.str);
				break;
			case dtype::BLOB:
				abort();
		}
		if(blob_size)
		{
			if(blob_size > max_data_size)
				max_data_size = blob_size;
			total_data_size += blob_size;
			for(size_t i = 0; i < blob_size; i++)
			{
				uint8_t byte = value[i];
				/* string finding heuristic */
				if(3 <= byte && byte < 48 && i + byte < blob_size)
				{
					size_t j;
					for(j = 1; j <= byte; j++)
						if(!isprint(value[i + j]))
							break;
					if(j > byte)
					{
						const char * string = &value.index<char>(i + 1);
						dups.add(istr(string, byte));
					}
				}
			}
		}
	}
	delete iter;
	if(key_type == dtype::STRING)
	{
		string_array = strings.array();
		if(!string_array)
			return -ENOMEM;
	}
	
	/* duplicate string table setup */
	dups.ignore();
	if(dups.size())
	{
		uint8_t escape;
		uint32_t byte_counts[256];
		dup_array = dups.array();
		stringtbl::array_sort(dup_array, dups.size());
		
		/* hardcode these for a while */
		header.dup_index_size = 1;
		header.dup_escape_len = 1;
		escape = header.dup_index_size + header.dup_escape_len;
		
		/* make another pass over the data to recalculate total_data_size and byte_counts */
		for(size_t i = 0; i < 256; i++)
			byte_counts[i] = 0;
		iter = source->iterator();
		while(iter->valid())
		{
			dtype key = iter->key();
			blob value = iter->value();
			size_t blob_size = value.size();
			iter->next();
			if(!value.exists())
				/* omit non-existent entries no longer needed */
				if(!shadow || !shadow->find(key).exists())
					continue;
			if(blob_size)
			{
				for(size_t i = 0; i < blob_size; i++)
				{
					uint8_t byte = value[i];
					/* string finding heuristic */
					if(3 <= byte && byte < 48 && i + byte < blob_size)
					{
						size_t j;
						for(j = 1; j <= byte; j++)
							if(!isprint(value[i + j]))
								break;
						if(j > byte)
						{
							istr string(&value.index<char>(i + 1), byte);
							ssize_t index = locate_string(dup_array, dups.size(), string);
							if(index >= 0)
							{
								i += byte;
								total_data_size -= byte + 1;
								total_data_size += escape;
								continue;
							}
						}
					}
					byte_counts[byte]++;
				}
			}
		}
		delete iter;
		
		size_t min = 0;
		for(size_t i = 1; i < 256; i++)
			/* prefer higher escape characters for no real reason */
			if(byte_counts[i] <= byte_counts[min])
				min = i;
		memset(header.dup_escape, 0, sizeof(header.dup_escape));
		header.dup_escape[0] = min;
		
		total_data_size += byte_counts[min] * (escape - 1);
		header.dup_offset = total_data_size;
	}
	else
	{
		header.dup_offset = 0;
		header.dup_index_size = 0;
		header.dup_escape_len = 0;
		memset(header.dup_escape, 0, sizeof(header.dup_escape));
	}
	
	/* now write the file */
	header.magic = USDTABLE_MAGIC;
	header.version = USDTABLE_VERSION;
	header.key_count = key_count;
	switch(key_type)
	{
		case dtype::UINT32:
			header.key_type = 1;
			header.key_size = byte_size(max_key);
			break;
		case dtype::DOUBLE:
			header.key_type = 2;
			header.key_size = sizeof(double);
			break;
		case dtype::STRING:
			header.key_type = 3;
			header.key_size = byte_size(strings.size() - 1);
			break;
		case dtype::BLOB:
			abort();
	}
	/* we reserve size 0 for non-existent entries, so add 1 */
	header.length_size = byte_size(max_data_size + 1);
	header.offset_size = byte_size(total_data_size);
	size = header.key_size + header.length_size + header.offset_size;
	
	r = out.create(dfd, file);
	if(r < 0)
		goto out_strings;
	r = out.append(&header);
	if(r < 0)
	{
	fail_unlink:
		out.close();
		unlinkat(dfd, file, 0);
		if(r >= 0)
			r = -1;
		goto out_strings;
	}
	if(string_array)
	{
		r = stringtbl::create(&out, string_array, strings.size());
		if(r < 0)
			goto fail_unlink;
	}
	
	/* now the key array */
	total_data_size = 0;
	iter = source->iterator();
	while(iter->valid())
	{
		int i = 0;
		uint8_t bytes[size];
		dtype key = iter->key();
		blob value = iter->value();
		iter->next();
		if(!value.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
				continue;
		switch(key.type)
		{
			case dtype::UINT32:
				layout_bytes(bytes, &i, key.u32, header.key_size);
				break;
			case dtype::DOUBLE:
				memcpy(bytes, &key.dbl, sizeof(double));
				i += sizeof(double);
				break;
			case dtype::STRING:
				max_key = locate_string(string_array, strings.size(), key.str);
				layout_bytes(bytes, &i, max_key, header.key_size);
				break;
			case dtype::BLOB:
				abort();
		}
		layout_bytes(bytes, &i, value.exists() ? value.size() + 1 : 0, header.length_size);
		layout_bytes(bytes, &i, total_data_size, header.offset_size);
		r = out.append(bytes, i);
		if(r != i)
		{
			delete iter;
			goto fail_unlink;
		}
		/* with a duplicate string table, total_data_size should be the compressed size here */
		if(dup_array)
			total_data_size += pack_size(value, header, dup_array, dups.size());
		else
			total_data_size += value.size();
	}
	delete iter;
	
	/* and the data itself */
	iter = source->iterator();
	while(iter->valid())
	{
		blob value = iter->value();
		iter->next();
		if(!value.exists())
			continue;
		if(dup_array)
			value = pack_blob(value, header, dup_array, dups.size());
		r = out.append(&value[0], value.size());
		if(r != (int) value.size())
		{
			delete iter;
			goto fail_unlink;
		}
	}
	delete iter;
	
	/* the duplicate string table */
	if(dup_array)
	{
		r = stringtbl::create(&out, dup_array, dups.size());
		if(r < 0)
			goto fail_unlink;
	}
	
	r = out.close();
	if(r < 0)
		goto fail_unlink;
	r = 0;
	
#if DEBUG_USTR
	{
	ustr_dtable * new_ustr = new ustr_dtable;
	r = new_ustr->init(dfd, file, config);
	
	printf("%s: performing sanity check on new file\n", __PRETTY_FUNCTION__);
	dtable::iter * new_iter = new_ustr->iterator();
	iter = source->iterator();
	while(iter->valid())
	{
		dtype key = iter->key();
		blob value = iter->value();
		if(!value.exists())
			/* omit non-existent entries no longer needed */
			if(!shadow || !shadow->find(key).exists())
			{
				iter->next();
				continue;
			}
		if(!new_iter->valid())
		{
			printf("%s: ERROR: EOF on new dtable\n", __PRETTY_FUNCTION__);
			break;
		}
		dtype new_key = new_iter->key();
		blob new_value = new_iter->value();
		if(key.type != new_key.type)
		{
			printf("%s: ERROR: key type mismatch\n", __PRETTY_FUNCTION__);
			break;
		}
		if(key != new_key)
		{
			printf("%s: ERROR: key mismatch\n", __PRETTY_FUNCTION__);
			break;
		}
		if(value.size() != new_value.size())
		{
			printf("%s: ERROR: value size mismatch\n", __PRETTY_FUNCTION__);
			break;
		}
		
		size_t i;
		for(i = 0; i < value.size(); i++)
			if(value[i] != new_value[i])
				break;
		if(i < value.size())
		{
			printf("%s: ERROR: value mismatch\n", __PRETTY_FUNCTION__);
			printf("Original value:\n");
			for(i = 0; i < value.size(); i++)
				printf("%02x[%c] %c", value[i], isprint(value[i]) ? value[i] : '.', ((i % 16) == 15) ? '\n' : ' ');
			printf("\nNew value:\n");
			for(i = 0; i < new_value.size(); i++)
				printf("%02x[%c] %c", new_value[i], isprint(new_value[i]) ? new_value[i] : '.', ((i % 16) == 15) ? '\n' : ' ');
			value = pack_blob(value, header, dup_array, dups.size());
			printf("\nPacked as:\n");
			for(i = 0; i < value.size(); i++)
				printf("%02x[%c] %c", value[i], isprint(value[i]) ? value[i] : '.', ((i % 16) == 15) ? '\n' : ' ');
			printf("\n");
			break;
		}
		
		iter->next();
		new_iter->next();
	}
	if(!iter->valid() && new_iter->valid())
		printf("%s: ERROR: EOF on original dtable\n", __PRETTY_FUNCTION__);
	if(iter->valid() || new_iter->valid())
	{
		printf("Duplicate strings:\n");
		for(size_t i = 0; i < dups.size(); i++)
			printf("#%02d: %s\n", i, dup_array[i]);
		printf("Escape character: 0x%02x\n", header.dup_escape[0]);
	}
	delete iter;
	delete new_iter;
	delete new_ustr;
	}
#endif
	
out_strings:
	if(dup_array)
		free(dup_array);
	if(string_array)
		free(string_array);
	return r;
}

DEFINE_RO_FACTORY(ustr_dtable);
