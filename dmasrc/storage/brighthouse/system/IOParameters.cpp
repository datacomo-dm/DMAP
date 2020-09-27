/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "IOParameters.h"
#include "common/DataFormat.h"
#include "system/IBMemStream.h"
#include "system/IBFile.h"
#include "core/TransactionBase.h"
#include "boost/foreach.hpp"
#include "boost/scoped_array.hpp"

using namespace boost;
using namespace std;

void IOParameters::SetNoColumns(uint no_columns)
{
	this->no_columns = no_columns;
	no_outliers.resize(no_columns);
	std::fill(no_outliers.begin(), no_outliers.end(), -1);
}


EDF IOParameters::GetEDF() const
{
	return DataFormat::GetDataFormat(curr_output_mode)->GetEDF();
}

void IOParameters::PutParameters(IBFile& stream)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_columns != 0);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_outliers.size() == no_columns);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(columns_decompositions.size() == no_outliers.size());
	IBMemStream mem_stream;
	mem_stream.OpenCreateEmpty("memory");


	mem_stream.WriteExact(base_path.c_str(), uint(base_path.length()) + 1);
	mem_stream.WriteExact(table_name.c_str(), (ushort)table_name.length() + 1);
	mem_stream.WriteExact((void*)&no_columns, sizeof(no_columns));
	mem_stream.WriteExact(output_path, uint(strlen(output_path)) + 1);
	mem_stream.WriteExact(line_starter.c_str(), uint(line_starter.length()) + 1);
	mem_stream.WriteExact(line_terminator.c_str(), uint(line_terminator.length()) + 1);
	mem_stream.WriteExact((void*)&curr_output_mode, 1);
	mem_stream.WriteExact((void*)&delimiter, 1);
	mem_stream.WriteExact((void*)&string_qualifier, 1);
	mem_stream.WriteExact((void*)&opt_enclosed, 1);
	mem_stream.WriteExact((void*)&escape_character, 1);
	mem_stream.WriteExact((void*)&pipe_mode_tmp, 1);
	mem_stream.WriteExact((void*)&local_load, 1);
	mem_stream.WriteExact((void*)&lock_option, 1);

	mem_stream.WriteExact((void*)&timeout, sizeof(timeout));
	mem_stream.WriteExact(charsets_dir.c_str(), uint(charsets_dir.length()) + 1);
	mem_stream.WriteExact((void*)&charset_info_number, sizeof(charset_info_number));

	size_t size = columns_collations.size();
	mem_stream.WriteExact((void*)&size, sizeof(size));
	BOOST_FOREACH(const char& col, columns_collations)
		mem_stream.WriteExact((void*)&col, 1);

	size = columns_decompositions.size();
	mem_stream.WriteExact((void*)&size, sizeof(size));
	for(int i = 0; i < columns_decompositions.size(); i++) {
		mem_stream.WriteExact(columns_decompositions[i].c_str(), uint(columns_decompositions[i].length()) + 1);
		mem_stream.WriteExact((void*)&no_outliers[i], sizeof(int64));
	}

	mem_stream.WriteExact((void*)&skip_lines, sizeof(skip_lines));
	mem_stream.WriteExact((void*)&value_list_elements, sizeof(value_list_elements));

	mem_stream.WriteExact((void*)&sign, sizeof(sign));
	mem_stream.WriteExact((void*)&minute, sizeof(minute));

	mem_stream.WriteExact(null_str.c_str(), uint(null_str.length()) + 1);

	size = mem_stream.GetSize();
	stream.WriteExact((void*)&size, sizeof(size_t));
	mem_stream.FlushTo((IBMemStream&)stream);

}

void IOParameters::GetParameters(IBFile& stream)
{
	cur_len = 0;
	size_t length = 0;
	stream.ReadExact((void*)&length, sizeof(size_t));
	boost::scoped_array<char> buffer (new char [length]);
	char* src = buffer.get();

	stream.ReadExact(src, uint(length));

	base_path = src;
	cur_len += (ushort)base_path.length() + 1;

	table_name = src + cur_len;
	cur_len += (ushort)table_name.length() + 1;

	uint tmp_no_columns = *(int*)(src + cur_len);
	cur_len += sizeof(no_columns);

	SetNoColumns(tmp_no_columns);

	strcpy(output_path, src + cur_len);
	cur_len += (ushort)strlen(output_path) + 1;

	line_starter = src + cur_len;
	cur_len += (ushort)line_starter.length() + 1;

	line_terminator = src + cur_len;
	cur_len += (ushort)line_terminator.length() + 1;

	curr_output_mode = *(src + cur_len);
	cur_len++;

	delimiter = *(src + cur_len);
	cur_len++;

	string_qualifier = *(src + cur_len);
	cur_len++;

	opt_enclosed = *(src + cur_len);
	cur_len++;

	escape_character = *(src + cur_len);
	cur_len++;

	pipe_mode_tmp = *(src + cur_len);
	cur_len++;

	local_load = *(src + cur_len);
	cur_len++;

	lock_option = *(src + cur_len);
	cur_len++;

	timeout = *(int*)(src + cur_len);
	cur_len += 4;

	charsets_dir = src + cur_len;
	cur_len += (ushort)charsets_dir.length() + 1;

	charset_info_number = *(int*)(src + cur_len);
	cur_len += 4;

	size_t no_collations = *(size_t*)(src + cur_len);
	cur_len += sizeof(size_t);
	for(int i = 0; i < no_collations; i++) {
		columns_collations.push_back(*(uchar*)(src + cur_len));
		cur_len += sizeof(uchar);
	}

	size_t no_decompositions = *(size_t*)(src + cur_len);

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_columns == no_decompositions);
	cur_len += sizeof(size_t);
	for(int i = 0; i < no_decompositions; i++) {
		columns_decompositions.push_back(std::string(src + cur_len));
		cur_len += ushort(columns_decompositions.back().length()) + 1;
		no_outliers[i] = *(int64*)(src + cur_len);
		cur_len += sizeof(int64);
	}

	skip_lines = *(_int64*)(src + cur_len);
	cur_len += 8;

	value_list_elements = *(_int64*)(src + cur_len);
	cur_len += 8;

	sign = *(short*)(src + cur_len);
	cur_len += 2;

	minute = *(short*)(src + cur_len);
	cur_len += 2;

	null_str = src + cur_len;
	cur_len += (ushort)null_str.length() + 1;

}

void IOParameters::SetNoOutliers(int col, int64 no_outliers)
{
	this->no_outliers[col] = no_outliers;
}

