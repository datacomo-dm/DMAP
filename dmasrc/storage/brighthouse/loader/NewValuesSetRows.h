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

#ifndef _NEW_VALUE_SUBSET_H_
#define _NEW_VALUE_SUBSET_H_

#include "core/bintools.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "system/LargeBuffer.h"
#include "loader/DataParser.h"
#include "common/DataFormat.h"
#include "types/RCDataTypes.h"
#include "loader/NewValuesSetBase.h"


class NewValuesSetRows : public NewValuesSetBase
{
public :
	enum STYPE {
		STYPE_STR,STYPE_REAL,STYPE_INT
	};
private:
	// simple column type
	STYPE stype;
	//
	// base type ,using one of these:
	std::vector<double> real_vals;
	std::vector<_int64> int_vals;
	std::vector<char *> str_vals;

	std::vector<char> isnulls;
	std::vector<short> sizes;
	int num_nulls;
	int num_rows;
	//double real_val;
	//_int64 int_val;
	//bool isnull;
	//void *pdata;
	//uint size;
public:
	NewValuesSetRows(STYPE _stype) //null value
	{
		stype=_stype;
		num_nulls=0;num_rows=0;
		if(stype==STYPE_STR) 
			AddStrNull();
		else if(stype==STYPE_REAL)
			AddRealNull();
		else AddIntNull();
	}
	void AddIntNull() {
		sizes.push_back(0);
		isnulls.push_back(1);
		int_vals.push_back(0);
		num_nulls++;
		num_rows++;
	}
	void AddRealNull() {
		sizes.push_back(0);
		isnulls.push_back(1);
		real_vals.push_back(0);
		num_nulls++;
		num_rows++;
	}
	void AddStrNull() {
		sizes.push_back(0);
		isnulls.push_back(1);
		str_vals.push_back(NULL);
		num_nulls++;
		num_rows++;
	}
	NewValuesSetRows(void *_pdata,uint _size )// for string value
	{
		num_nulls=0;num_rows=0;
		PushStr((char *)_pdata,_size);
		stype=STYPE_STR;
	}
	
	void PushStr(char *_pdata,uint _size) {
		sizes.push_back(_size);
		isnulls.push_back(0);
		char *newstr=new char[_size];
		memcpy(newstr,_pdata,_size);
		str_vals.push_back(newstr);
		num_rows++;
	}
	
	void PushReal(double v) {
		sizes.push_back(8);
		isnulls.push_back(0);
		real_vals.push_back(v);
		num_rows++;
	}

	void PushInt(_int64 v) {
		sizes.push_back(8);
		isnulls.push_back(0);
		int_vals.push_back(v);
		num_rows++;
	}

	NewValuesSetRows(double v)// for real value
	{
		stype=STYPE_REAL;
		num_nulls=0;num_rows=0;
		PushReal(v);
	}

	NewValuesSetRows(_int64 v)// for integer value
	{
		stype=STYPE_INT;
		num_nulls=0;num_rows=0;
		PushInt(v);
	}

	~NewValuesSetRows()
	{
		if(stype==STYPE_STR) {
			for(uint i = 0; i < num_rows; i++)
			{
				if(isnulls[i] != 1)
				{
					delete [] str_vals[i];
				}
			}
		}
	}

	bool IsNull(int ono)	{ return isnulls[ono]==1; }

	_uint64 SumarizedSize()
	{
		_uint64 size = 0;
		for(int i = 0; i < num_rows; i++)
			size += sizes[i];
		//TODO : check codes:
		//if(ATI::IsBinType(attr_type) && DataFormat::GetDataFormat(attr_edf)->BinaryAsHex())
		//	size /= 2;
		return size;
	}

	uint Size(int ono)
	{
		return sizes[ono];
	}

	void GetIntStats(_int64& min, _int64& max, _int64& sum)
	{
		min = PLUS_INF_64;
		max = MINUS_INF_64;
		sum = 0;
		_int64 v = 0;
		for(uint i = 0; i < num_rows; i++)
		{
			if(isnulls[i] != 1)
			{
				v = int_vals[i];
				sum += v;
				if(min > v)
					min = v;
				if(max < v)
					max = v;
			}
		}
	}

	void GetRealStats(double& min, double& max, double& sum)
	{
		min = PLUS_INF_DBL;
		max = MINUS_INF_DBL;
		sum = 0;
		double v = 0;
		int noo = num_rows;
		for(int i = 0; i < noo; i++)
		{
			if(isnulls[i] != 1)
			{
				v = real_vals[i];
				sum += v;
				if(min > v)
					min = v;
				if(max < v)
					max = v;
			}
		}
	}

	void GetStrStats(RCBString& min, RCBString& max, ushort& maxlen)
	{
		min = RCBString();
		max = RCBString();
		maxlen = 0;

		RCBString v = 0;
		int noo = num_rows;
		//bool UTFConversions = RequiresUTFConversions(col);
		for(int i = 0; i < noo; i++) {
			if(isnulls[i] != 1) {
				v.val = GetDataBytesPointer(i);
				v.len = Size(i);
				if(v.len > maxlen)
					maxlen = v.len;

				if(min.IsNull())
					min = v;
				//else if(UTFConversions) {
				//	if(CollationStrCmp(col, min, v) > 0)
				//		min = v;
				//} 
					else if(min > v)
					min = v;

				if(max.IsNull())
					max = v;
				//else if(UTFConversions) {
				//	if(CollationStrCmp(col, max, v) < 0)
				//		max = v;
				//} 
				  else if(max < v)
					max = v;
			}
		}
	}

	
	char*	GetDataBytesPointer(int ono)
	{
		if(stype==STYPE_STR) 
			return str_vals[ono];
		else if(stype==STYPE_REAL)
			return (char *)&real_vals[ono];
		else
			return (char *)&int_vals[ono];
	}
	uint GetObjSize(int ono) { return sizes[ono]; }
	int		NoValues()				{ return num_rows; }

};

#endif
