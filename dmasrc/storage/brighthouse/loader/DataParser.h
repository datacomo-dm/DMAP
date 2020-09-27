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

#ifndef DATAPARSER_H_
#define DATAPARSER_H_

#include <iostream>
#include <ctype.h>
#include <math.h>
#include <boost/utility.hpp>

#include "core/bintools.h"
#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "edition/local.h"
#include "edition/loader/RCAttr_load.h"
#include "system/IOParameters.h"
#include "common/bhassert.h"
#include "common/DataFormat.h"


class NewValuesSet;
class ValueParser;

class DataParser : public boost::noncopyable
{
	friend class NewValuesSet;
	bool simpbin;
public:
	DataParser();
	bool GetSimpBin() {return simpbin;}
	void SetSimpBin(bool v) {simpbin=v;}
	DataParser(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr=65536);
	virtual ~DataParser();
	void	PrepareNextCol();
	int		Prepare(int no_rows);
	void	PrepareNumValues();
	void	PrepareNumValuesForLookup();
	bool 	IsLastColumn();
	bool	CheckData();
	bool 	CheckPreparedValuesSizes();
	char*	GetValue(int ono);
//protected:
	virtual uint			GetObjSize(int ono);
	virtual uint 			GetValueSize(int ono);
	virtual bool 			IsNull(int ono);

	virtual int				GetRowSize(char* buf_ptr, int rno) = 0;
	virtual void 			PrepareValuesPointers() = 0;
	virtual void			PrepareNulls() = 0;
	virtual void 			PrepareObjsSizes() = 0;

	virtual bool 			FormatSpecificDataCheck() { return true; };
	virtual void			FormatSpecificProcessing() {};
	virtual void			ReleaseCopies() {};
//protected:

	virtual int				CurrentAttribute()	{ return cur_attr; }
	virtual int				NoPrepared() { return no_prepared; };
	virtual EDF				GetEDF() { return edf; };
	virtual AttributeType	CurrentAttributeType() { return cur_attr_type; };
	virtual void			InitAttributesTypeInfo();
	virtual bool			DoPreparedValuesHasToBeCoppied() { return false; }

	AttrPackType GetPackType(int attr) { return attrs[attr]->PackType(); }

	virtual char ** copy_values_ptr(int start_ono, int end_ono);
	char * copy_nulls(int start_ono, int end_ono);
	_int64* copy_num_values(int start_ono, int end_ono);
	int* copy_sizes(int start_ono, int end_ono);
	int* copy_obj_sizes(int start_ono, int end_ono);
	uint* copy_value_sizes(int attr, int start_ono, int end_ono);

protected:
	virtual char* GetObjPtr(int ono) const { return values_ptr[ono]; } //return the pointer to the value in input buffer, from before conversions like charsets conversion

public:
	std::vector<RCAttrLoad*> attrs;
	Buffer* buffer;
	EDF edf;
	char** values_ptr;
	char** rows_ptr;
	uint** objs_sizes;		//sizes in the source

	uint** value_sizes;		//sizes of parsed/extracted values

	uint* objs_sizes_ptr;
	_int64* num_values64;

	char* buf_start;
	char* buf_end;
	char* buf_ptr;

	int no_attr;
	int no_prepared;
	int row_header_byte_len;
	int cur_attr;
	AttributeType cur_attr_type;
	std::vector<AttributeTypeInfo> atis;

	ValueParserAutoPtr value_parser;

	char *nulls; //[65536];
	int *sizes; //[65536];
	int *row_sizes; //[65536];
	std::pair<_int64, int> error_value;
	_int64 loaderStartTime;

	bool row_incomplete;
	const uint max_parse_rows;
private:
	// TimeZone
	short sign;
	short minute;
};

#endif /*DATAPARSER_H_*/
