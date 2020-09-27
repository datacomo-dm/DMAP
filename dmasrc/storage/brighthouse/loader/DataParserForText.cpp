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

#include "types/DataConverter.h"
#include "core/tools.h"
#include "core/bintools.h"
#include "loader/NewValueSet.h"
#include "edition/loader/RCAttr_load.h"
#include "system/IOParameters.h"
#include "DataParserForText.h"
#include "types/ValueParserForText.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

DataParserForText::DataParserForText(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr)
	:	DataParser(attrs, buffer, iop, mpr), prepared(false), delimiter(iop.Delimiter()), delimiter_size(1),
	 	string_qualifier(iop.StringQualifier()), escape_char(iop.EscapeCharacter())
{

	ShiftBufferByBOM();
	qualifier_present = new bool * [no_attr];
	for(int i = 0; i < no_attr; i++)
		qualifier_present[i] = new bool [mpr];

	for(int i = 0; i < no_attr; i++)
		if(atis[i].Type() != RC_STRING)
			value_sizes[i] = new uint [mpr];

#ifndef PURE_LIBRARY
	cs_info = get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.CharsetInfoNumber(), 0);
	for(int i = 0; i < no_attr; i++)
		if(ATI::IsStringType(atis[i].Type()))
			atis[i].SetCollation(get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.ColumnsCollations()[i], 0));
#endif
	converted_values = 0;
	converted_values_size = 0;
	pos_in_converted_values = 0;
}

DataParserForText::~DataParserForText()
{
	for(int i = 0; i < no_attr; i++) {
		delete [] qualifier_present[i];
		if(atis[i].Type() != RC_STRING) {
			delete [] value_sizes[i];
			value_sizes[i] = 0;
		}
	}
	delete [] qualifier_present;
	free(converted_values);
	for( std::vector<char*>::iterator it=con_buffs.begin();
		it != con_buffs.end();
		it++)
			delete [] *it;
}

void 
DataParserForText::ReleaseCopies() 
{
        while( !con_buffs.empty() ) {
                delete [] con_buffs.back();
		con_buffs.pop_back();
	}
}

void DataParserForText::PrepareValuesPointers()
{
	if(oryg_vals_ptrs.size()) {
		if(cur_attr != 0) {
			for(int i = 0; i < no_prepared; i++) //restore original values pointers
				values_ptr[i] = oryg_vals_ptrs[i];
		}
		oryg_vals_ptrs.clear();
	}

	int opsbs = (delimiter ? 1 : 0);
	for(int i = 0; i < no_prepared; i++) {
		if (cur_attr != 0 ) {
			values_ptr[i] += objs_sizes[cur_attr-1][i] + opsbs ;
			if(qualifier_present[cur_attr - 1][i]) //previous column had string qualifier
				values_ptr[i]--; //undoing the following ++
		}
		if(string_qualifier && values_ptr[i][0] == string_qualifier) {
			values_ptr[i]++;
			qualifier_present[cur_attr][i]=true;
		} else
			qualifier_present[cur_attr][i]=false;
	}
}

void DataParserForText::FormatSpecificProcessing()
{
	if(ATI::IsBinType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
			int p = 0;
			uint os = GetObjSize(i);
			for(uint l = 0; l < os; l++) {
				int a = isalpha((uchar)values_ptr[i][l]) ? tolower(values_ptr[i][l]) - 'a' + 10 : values_ptr[i][l] - '0';
				int b = isalpha((uchar)values_ptr[i][l+1]) ? tolower(values_ptr[i][l+1]) - 'a' + 10 : values_ptr[i][l+1] - '0';
				values_ptr[i][p++] =  a * 16 + b;
				l++;
			}
		}
	} else if(ATI::IsTxtType(cur_attr_type)) {

		for(int i = 0; i < no_prepared; i++)
			ProcessEscChar(i);

#ifndef PURE_LIBRARY
		uint errors = 0;
		if(atis[cur_attr].CharsetInfo()->mbmaxlen <= cs_info->mbmaxlen) {
			for(int i = 0; i < no_prepared; i++)
				value_sizes[cur_attr][i] = copy_and_convert(values_ptr[i], GetObjSize(i), atis[cur_attr].CharsetInfo(), values_ptr[i], GetValueSize(i), cs_info, &errors);
		} else {
			SetPosInConvertedValues(0);
			oryg_vals_ptrs.clear();
			vector<size_t> positions;
			for(int i = 0; i < no_prepared; i++) {
				uint size_before = GetValueSize(i);
				uint size_after = 0;
				size_t new_max_size = size_before * atis[cur_attr].CharsetInfo()->mbmaxlen;
				size_t pos = GetBufferForConvertedValues(new_max_size + 1);
				char* v_ptr = ConvertedValue(pos);
				positions.push_back(pos);
				size_after = copy_and_convert(v_ptr, (uint32)new_max_size, atis[cur_attr].CharsetInfo(), values_ptr[i], size_before, cs_info, &errors);
				*(v_ptr + size_after) = 0;
				value_sizes[cur_attr][i] = size_after;
			}

			for(int i = 0; i < no_prepared; i++) {
				oryg_vals_ptrs.push_back(values_ptr[i]);
				values_ptr[i] = ConvertedValue(positions[i]);
			}
		}
#endif
	}
}

void DataParserForText::GetCRLF()
{
	bool inside_string = false;
	char* ptr = buf_start;
	while(ptr != buf_end) {
		if(!inside_string) {
			if(string_qualifier && *ptr == string_qualifier)
				inside_string = 1;
			else if(*ptr == '\r' && *(ptr+1) == '\n') {
				eol = "\r\n";
				crlf = 2;
				break;
			} else if(*ptr == '\n') {
				eol = "\n";
				crlf = 1;
				break;
			}
		} else if(inside_string && string_qualifier && *ptr == string_qualifier) {
			if(*(ptr + 1) == delimiter ||  *(ptr + 1) == '\n')
				inside_string = 0;
			else if((ptr + 2) <= buf_end && *(ptr + 1) == '\r' && *(ptr + 2) == '\n')
				inside_string = 0;
		}
		ptr++;
	}
	if (crlf==0) throw FormatRCException( BHERROR_DATA_ERROR, 1, -1 );
}

inline int DataParserForText::GetRowSize(char* row_ptr, int rno)
{
	row_incomplete = false;
	if(!prepared) {
		GetCRLF();
		prepared = true;
	}
	if(row_ptr != buf_end) {
		char* ptr = row_ptr;
		char* v_ptr = row_ptr;
		bool stop = false;
		bool inside_string = false;
		int no_attrs_tmp = 0;
		int tan = no_attr - 1;
		bool esc_char = false;
		bool new_value_beg = true;

		while(!stop) {
			if(esc_char) {
				new_value_beg = false;
				esc_char = false;
			} else if(escape_char && *ptr == escape_char &&
			    ATI::IsTxtType(atis[no_attrs_tmp].Type()) &&
			    !(escape_char == string_qualifier && new_value_beg) &&
			    (   !(escape_char == string_qualifier && !new_value_beg &&
				(((no_attrs_tmp != tan) && (*(ptr+1) == delimiter)) ||
				   ((no_attrs_tmp == tan) && IsEOR(ptr+1))
				))
                            ))
			{
				new_value_beg = false;
				esc_char = true;
			} else if(string_qualifier && *ptr == string_qualifier
			) {
				if(!inside_string) {
					inside_string = true;
				} else if ( ( ( ( ptr + 1 ) != buf_end ) && ( no_attrs_tmp != tan ) && ( *(ptr + 1) == delimiter ) )
						|| ( ( ( ptr + crlf ) < buf_end ) && ( no_attrs_tmp == tan ) && IsEOR( ptr + 1 ) ) ) {
					inside_string = false;
				} else if ( ( ( ( ptr + 1 ) != buf_end ) && ( no_attrs_tmp != tan ) ) || ( ( ( ptr + crlf ) < buf_end ) && ( no_attrs_tmp == tan ) ) )
					throw FormatRCException(BHERROR_DATA_ERROR, rno + 1, no_attrs_tmp + 1 );
				new_value_beg = false;
				esc_char = false;
			} else if(!inside_string) {
				if(
					((no_attrs_tmp != tan) && (*ptr == delimiter)) ||
					((no_attrs_tmp == tan) && IsEOR(ptr))
				) {
					objs_sizes[no_attrs_tmp][rno] = (uint)(ptr - v_ptr);
					if(no_attrs_tmp != 0)
						objs_sizes[no_attrs_tmp][rno]--;
					value_sizes[no_attrs_tmp][rno] = objs_sizes[no_attrs_tmp][rno];
					//no_esc_char[no_attrs_tmp][rno] = 0;
					v_ptr = ptr;
					no_attrs_tmp++;
					new_value_beg = true;
					if(no_attrs_tmp == no_attr)
						break;
				} else if(IsEOR(ptr)) {
					if(string_qualifier || (no_attrs_tmp != tan ))
						throw FormatRCException(BHERROR_DATA_ERROR, rno + 1, no_attrs_tmp + 1 );
				}
				esc_char = false;
			}
			if((ptr + 1) == buf_end) {
				row_incomplete = true;
				error_value.first = rno + 1;
				error_value.second = no_attrs_tmp + 1;
				break;
			}
			ptr++;
		}
		if(no_attrs_tmp == no_attr)
			return (int)(ptr - row_ptr) + crlf;
	}
 	return -1;
}

void DataParserForText::PrepareObjsSizes()
{
	objs_sizes_ptr = objs_sizes[cur_attr];
	for(int i = 0; i < no_prepared; i++) {
		if (string_qualifier ) {
			if(objs_sizes_ptr[i] && qualifier_present[cur_attr][i])
				value_sizes[cur_attr][i] -= 2;
		}
		//process escape chars

		// eliminate trailing spaces
		if(ATI::IsCharType(cur_attr_type)) {
			for (char* c = values_ptr[i] + value_sizes[cur_attr][i]- 1; c >= values_ptr[i]; c--) {
				if (*c == ' ')
					value_sizes[cur_attr][i]--;
				else
					break;
			}
		}
	}
}

void DataParserForText::ProcessEscChar(int i)
{
	if(!IsNull(i)) {
		uint s = GetObjSize(i);
		int p = 0;
		for(uint j = 0; j < s; j++)	{
			if(values_ptr[i][j] == escape_char && !(escape_char == string_qualifier && j == 0) && (j != s - 1)){
				values_ptr[i][p] = TranslateEscapedChar(values_ptr[i][j+1]);
				j++;
				if (value_sizes[cur_attr][i]) 
					value_sizes[cur_attr][i]--;
			} else
				values_ptr[i][p] = values_ptr[i][j];
			p++;
		}
	}
}

void DataParserForText::PrepareNulls()
{
	if(!IsLastColumn()) {
		for(int i = 0; i < no_prepared; i++) {
			int rs = row_sizes[i];
			if(
				*values_ptr[i] == delimiter ||
				(((values_ptr[i] + 5) < (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "NULL", 4) == 0
						&& (*(values_ptr[i] + 4) == delimiter)) ||
				(!qualifier_present[cur_attr][i] && ((values_ptr[i] + 3) < (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "\\N", 2) == 0
						&& (*(values_ptr[i] + 2) == delimiter)) ||
				(qualifier_present[cur_attr][i] && ((values_ptr[i] + 4) < (rows_ptr[i] + rs)) && strncmp(values_ptr[i], "\\N", 2) == 0
						&& (*(values_ptr[i] + 3) == delimiter))
			)
				nulls[i] = 1;
			else
				nulls[i] = 0;
		}
	} else {
		for(int i = 0; i < no_prepared; i++) {
			int rs = row_sizes[i];
			if(
				IsEOR(values_ptr[i]) ||
				(((values_ptr[i] + 4 + crlf) == (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "NULL", 4) == 0
						&& IsEOR(values_ptr[i] + 4)) ||
				(!qualifier_present[cur_attr][i] && ((values_ptr[i] + 2 + crlf) == (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "\\N", 2) == 0
						&& IsEOR(values_ptr[i] + 2)) ||
				(qualifier_present[cur_attr][i] && ((values_ptr[i] + 3 + crlf) == (rows_ptr[i] + rs)) && strncmp(values_ptr[i], "\\N", 2) == 0
						&& IsEOR(values_ptr[i] + 3))
			)
				nulls[i] = 1;
			else
				nulls[i] = 0;
		}
	}
}

bool DataParserForText::FormatSpecificDataCheck()
{

	if(ATI::IsTxtType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
#ifndef PURE_LIBRARY
			size_t char_len = atis[cur_attr].CharsetInfo()->cset->numchars(atis[cur_attr].CharsetInfo(), values_ptr[i], values_ptr[i] + GetValueSize(i));
#else
			size_t char_len = GetValueSize(i);
#endif
			if(!IsNull(i) && char_len > atis[cur_attr].CharLen()) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	} else if(ATI::IsBinType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
			if(!IsNull(i) && (GetObjSize(i)%2)) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	}
	return true;
}

uint DataParserForText::GetValueSize(int ono)
{
	if(ATI::IsBinType(CurrentAttributeType()))
		return value_sizes[cur_attr][ono] / 2;
	else
		return value_sizes[cur_attr][ono];

}

char* DataParserForText::GetObjPtr(int ono) const
{
	return oryg_vals_ptrs.size() ? oryg_vals_ptrs[ono] : values_ptr[ono];
}

char DataParserForText::TranslateEscapedChar(char c)
{
	static char in[] = {'0',  'b',  'n',  'r',  't',  char(26)};
	static char out[]= {'\0', '\b', '\n', '\r', '\t', char(26)};
	for(int i = 0; i < 6; i++)
		if(in[i] == c)
			return out[i];
	return c;
}

//bool DataParserForText::IsEOR(const char* ptr) const
//{
//	return (crlf == 2 && *ptr == '\r' && *(ptr + 1) == '\n') ||	(crlf == 1 && *ptr == '\n');
//}

bool DataParserForText::IsEOR(const char* ptr) const
{
	int i = 0;
	while(i < crlf && ptr[i] == eol[i])
		i++;
	return i == crlf;
}

void DataParserForText::ShiftBufferByBOM()
{
	// utf-8
	if((uchar)buf_start[0] == 0xEF && (uchar)buf_start[1] == 0xBB && (uchar)buf_start[2] == 0xBF) {
		buf_start += 3;
		buf_ptr += 3;
	} else if((uchar)buf_start[0] == 0xFE && (uchar)buf_start[1] == 0xFF || // UTF-16-BE
		(uchar)buf_start[0] == 0xFF && (uchar)buf_start[1] == 0xFE) {  // UTF-16-LE
		buf_start += 2;
		buf_ptr += 2;
	}
}

size_t DataParserForText::GetBufferForConvertedValues(size_t size)
{
	if(pos_in_converted_values + size > converted_values_size) {
		converted_values = (char*)realloc(converted_values, pos_in_converted_values + size);
		converted_values_size = pos_in_converted_values + size;
	}
	size_t tmp_pos = pos_in_converted_values;
	pos_in_converted_values += size;
	return  tmp_pos;
}

char ** DataParserForText::copy_values_ptr(int start_ono, int end_ono) {
#ifndef PURE_LIBRARY
	if( ATI::IsTxtType(cur_attr_type) && (atis[cur_attr].CharsetInfo()->mbmaxlen > cs_info->mbmaxlen) )
	{
		char **result = new char*[end_ono-start_ono];
		char *buf = new char[converted_values_size];
		memcpy(buf, converted_values, converted_values_size);
		con_buffs.push_back(buf);
		for( int i=0, j=start_ono; j<end_ono; i++,j++ )
			result[i] = values_ptr[j] - converted_values + buf;
			
		return result;		
	}
#endif
	return DataParser::copy_values_ptr(start_ono, end_ono);
}

bool DataParserForText::DoPreparedValuesHasToBeCoppied()
{
#ifndef PURE_LIBRARY
	return ATI::IsTxtType(cur_attr_type) && (atis[cur_attr].CharsetInfo()->mbmaxlen > cs_info->mbmaxlen);
#else
	return false;
#endif
}





DataParserForBinary::DataParserForBinary(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr)
	:	DataParser(attrs, buffer, iop, mpr)//, prepared(false), delimiter(iop.Delimiter()), delimiter_size(1),
	 	//string_qualifier(iop.StringQualifier()), escape_char(iop.EscapeCharacter())
{
//	nullinfo= new char * [mpr];
//	for(int i = 0; i < no_attr; i++)
//	if(atis[i].Type() != RC_STRING)

//	cs_info = get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.CharsetInfoNumber(), 0);
//	for(int i = 0; i < no_attr; i++)
//		if(ATI::IsStringType(atis[i].Type()))
//			atis[i].SetCollation(get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.ColumnsCollations()[i], 0));
}




#define GetVBit(bf,colid) (bf[colid/8]&(0x80>>(colid%8)))


 int	DataParserForBinary::GetRowSize(char* row_ptr, int rno) {
	row_incomplete = false;
	if(row_ptr != buf_end) {
		short rowlen=*(short *)row_ptr;
		row_header_byte_len=(no_attr+7)/8+2;
		char* ptr = row_ptr+row_header_byte_len;
		int no_attrs_tmp=0;
		int csize=0;
		if(buf_end-row_ptr < sizeof(short) || buf_end-row_ptr<rowlen) {
			row_incomplete = true;
			return -1;
		}
		while(true) {
			AttributeType at=atis[no_attrs_tmp];
			int prec=atis[no_attrs_tmp].Precision();
			//if(GetVBit(pnull,no_attrs_tmp)) {  null flag set every column ,not for all columns
			//
			//}
				value_sizes[no_attrs_tmp][rno]=0;
				switch(atis[no_attrs_tmp].Type()) {
					case RC_STRING: csize=atis[no_attrs_tmp].CharLen();break;//RC_STRING means fix length CHAR?but DataParser only alloc mem for RC_STRING ,not for VARCHAR
					case RC_VARCHAR : 
						csize=*((short *)ptr)+2;//v_ptr+=2;
						break;
						// merge 1-3 bytes int to 4bytes
					case RC_BYTEINT: //csize=1;break;
					case RC_SMALLINT://csize=2;break;
					case RC_MEDIUMINT://csize=3;break;
					case RC_INT:csize=4;break;
					case RC_BIGINT:csize=8;break;
					case RC_FLOAT:csize=8;break;//wdbi store float as double ,may be we must change wdbi to output float type
					case RC_REAL:csize=8;break;
					case RC_NUM:
						 if(prec<3) csize=1;
						 else if(prec<5) csize=2;
						 else if(prec<10) csize=4;
						 else csize=8;
						 break;
					case RC_DATE :
					case RC_TIME :
					case RC_YEAR :
						//not implement yet
						return -1;
					case RC_TIMESTAMP:
					case RC_DATETIME:
						csize=19;
						break;
				}
				//FIX :JIRA DMA-127 a error on field that just ended at buf_end
				// DMA-127: 前一个字段刚好在buf_end结束,用>buf_end的条件不能停止,同时no_attrs_tmp还没有达到no_attr,因此继续循环
				//   取出了buf_end以外的内容做为下一个字段(no_attrs_tmp被加1,指向了buf_end后面)
				// > buf_end ===> >= buf_end && no_attrs_tmp+1!=no_attr
				// DMA-131: 加上no_attrs_tmp+1!=no_attr的条件,如果最后一个字段在buf_end之前开始,但在buf_end处还没有结束,则出现错误处理:objs_sizes指向buf_end之外
				//   但没有处理row_incomplete为true
				if( ((ptr + csize) == buf_end && no_attrs_tmp+1!=no_attr) || (ptr + csize) > buf_end) {
					row_incomplete = true;
					error_value.first = rno + 1;
					error_value.second = no_attrs_tmp + 1;
					// a wrong data row,exceed maximum buffer in cache.
					int stophere=1; 
					// check row_ptr to see how this happen.
					break;
				}
				ptr+=csize;
				value_sizes[no_attrs_tmp][rno] = objs_sizes[no_attrs_tmp][rno]=csize;
				//if(at==RC_VARCHAR) value_sizes[no_attrs_tmp][rno]-=2;
				if(++no_attrs_tmp == no_attr)
				  break;
		}
		if(no_attrs_tmp == no_attr)
			return (int)(ptr - row_ptr) ;
	}
	return -1;
}


 void DataParserForBinary::PrepareObjsSizes()
{
	objs_sizes_ptr = objs_sizes[cur_attr];
}

 //prepare null flag for a column
  void DataParserForBinary::PrepareNulls()  
{
		if(!num_values64) {
			num_values64 = new _int64 [max_parse_rows];
		}

		AttributeType at=atis[cur_attr];
		for(int i = 0; i < no_prepared; i++) {
			char *nullinfo = rows_ptr[i]+2;
			// fix dma-512: null indicated error
			nulls[i]=GetVBit(nullinfo,cur_attr)==0?0:1;
					  //parser datatime to 64bit integer.
		  int prec=atis[cur_attr].Precision(),scale=atis[cur_attr].Scale();
		  if(nulls[i]) num_values64[i]=NULL_VALUE_64;
		  else	switch (atis[cur_attr].Type()) {
			  //parser for numberic/real/int value:
			case RC_NUM :
				//return boost::bind<BHReturnCode>(&ValueParserForText::ParseDecimal, _1, _2, at.Precision(), at.Scale());
				num_values64[i]=prec<3?*((char *)values_ptr[i]):(prec<5?*((short *)values_ptr[i]):
					(prec<10?*((int *)values_ptr[i]):*((_int64 *)values_ptr[i])));
				break;
			case RC_FLOAT :
				//wdbi store float as double ,may be we must change wdbi to output float type
				*(double *)(&num_values64[i])=*((double *)values_ptr[i]);
				break;
			case RC_REAL :
				*(double *)(&num_values64[i])=*((double *)values_ptr[i]);
				break;
				// wdbi simplified int type to tow class: int(4byte) and bigint(8byte),so we can merge 1/2/3 byte length type to 4bytes len
			case RC_BYTEINT :
			case RC_SMALLINT :
			case RC_MEDIUMINT :
			case RC_INT :
				num_values64[i]=*((int *)values_ptr[i]);
				break;
			case RC_BIGINT :
				num_values64[i]=*((_int64 *)values_ptr[i]);
				break;
			case RC_DATE :
			case RC_TIME :
			case RC_YEAR :
			case RC_DATETIME :
			case RC_TIMESTAMP:
				{
					//return boost::bind<BHReturnCode>(&ValueParserForText::ParseDateTimeAdapter, _1, _2, at.Type());
					int y,m,d,hh,mm,ss;
					sscanf(values_ptr[i],"%04d-%02d-%02d %02d:%02d:%02d", // len =20
						&y,&m,&d,&hh,&mm,&ss);
					RCDateTime rcv = RCDateTime((short) y, (short) m, (short) d, (short) hh, (short) mm,
						(short) ss, at);
					num_values64[i]=rcv.GetInt64();
				}
				break;
		  }
		}
}


  void 	DataParserForBinary::PrepareValuesPointers(){
	  AttributeType at=atis[cur_attr];
	  for(int i = 0; i < no_prepared; i++) {
		  // delay null value process to later(PrepareNulls called after PrepareValuesPointers in  DataParser::PrepareNextCol)
		  //char *nullinfo = rows_ptr[i];
		  if (cur_attr != 0 ) {
			  values_ptr[i] += objs_sizes[cur_attr-1][i];
		  }
			if(atis[cur_attr].Type()==RC_VARCHAR) {
				values_ptr[i]+=2;
			  value_sizes[cur_attr][i]-=2;
			}
	  }
  }

