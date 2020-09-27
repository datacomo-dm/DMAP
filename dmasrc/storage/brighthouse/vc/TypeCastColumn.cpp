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

#include "TypeCastColumn.h"

#include "types/RCNum.h"

extern VirtualColumn* CreateVCCopy(VirtualColumn* vc);

using namespace std;

/*! \brief Converts UTC/GMT time given in seconds since beginning of the EPOCHE to TIME representation
 * \param tmp
 * \param t
 * \return void
 */
void GMTSec2GMTTime(MYSQL_TIME* tmp, my_time_t t)
{
#ifndef PURE_LIBRARY
	time_t tmp_t = (time_t)t;
	struct tm tmp_tm;
	gmtime_r(&tmp_t, &tmp_tm);
	localtime_to_TIME(tmp, &tmp_tm);
	tmp->time_type= MYSQL_TIMESTAMP_DATETIME;
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
}

bool IsTimeStampZero(MYSQL_TIME& t)
{
#ifndef PURE_LIBRARY
	return t.day == 0 && t.hour == 0 && t.minute == 0 && t.month == 0 && t.second == 0 && t.second_part == 0 && t.year == 0;
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#endif
}

TypeCastColumn::TypeCastColumn(VirtualColumn* from, ColumnType const& target_type)
	:	VirtualColumn(target_type, from->GetMultiIndex()), full_const(false), vc(from) 
{ 
	dim = from->GetDim(); 
}

TypeCastColumn::TypeCastColumn(const TypeCastColumn& c) : VirtualColumn(c)
{
	assert(CanCopy());
	vc = CreateVCCopy(c.vc);
}

void TypeCastColumn::LockSourcePacks(const MIIterator& mit, int th_no)
	{
		vc->LockSourcePacks( mit,th_no);
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
String2IntCastColumn::String2IntCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if (full_const) {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if (rs.IsNull()) {
			rcv = RCNum();
			val = NULL_VALUE_64;
		} else if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: ";
			s += (string)rs;
			push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
		rcv = rcn;
		val = rcn.GetValueInt64();
	}
#endif

}

_int64 String2IntCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(full_const)
		return val;
	RCBString rs;
	RCNum rcn;
	vc->GetValueString(rs, mit);
	if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
		std::string s = "Truncated incorrect numeric value: ";
		s += (string)rs;
		push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
	}
	return rcn.GetValueInt64();
#endif

}

_int64 String2IntCastColumn::DoGetValueInt64(const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if (full_const)
		return val;
	else {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if (rs.IsNull()) {
			return NULL_VALUE_64;
		} else {
			if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
				std::string s = "Truncated incorrect numeric value: ";
				s += (string)rs;
				push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
			}
			return rcn.GetValueInt64();
		}
	}
#endif

}

RCValueObject String2IntCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if(full_const)
		return rcv;
	else {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if (rs.IsNull()) {
			return RCNum();
		} else if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: ";
			s += (string)rs;
			push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
		return rcn;


	}
#endif
}

_int64 String2IntCastColumn::DoGetMinInt64(const MIIterator& m)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if (full_const)
		return val;
	else if(IsConst()) {
		// const with parameters
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, m);
		if (rs.IsNull()) {
			return NULL_VALUE_64;
		} else if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: ";
			s += (string)rs;
			push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
		return rcn.GetValueInt64();
	} else
	return MINUS_INF_64;
#endif
}

_int64 String2IntCastColumn::DoGetMaxInt64(const MIIterator& m)
{
	_int64 v = DoGetMinInt64(m);
	return v != MINUS_INF_64 ? v : PLUS_INF_64;
}

String2DateTimeCastColumn::String2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	full_const = vc->IsFullConst();
	if(full_const) {
		RCBString rbs;
		MIIterator mit(NULL);
		vc->GetValueString(rbs, mit);
		if (rbs.IsNull()) {
			val = NULL_VALUE_64;
			rcv = RCDateTime();
		} else {
			RCDateTime rcdt;
			if(BHReturn::IsError(RCDateTime::Parse(rbs, rcdt, to.GetTypeName()))) {
				std::string s = "Incorrect Date/Time value: ";
				s += (string)rbs;
				push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}
			if(to.GetTypeName() == RC_TIMESTAMP) {
				// needs to convert value to UTC
				MYSQL_TIME myt;
				memset(&myt, 0, sizeof(MYSQL_TIME));
				myt.year = rcdt.Year();
				myt.month = rcdt.Month();
				myt.day = rcdt.Day();
				myt.hour = rcdt.Hour();
				myt.minute = rcdt.Minute();
				myt.second = rcdt.Second();
				myt.time_type = MYSQL_TIMESTAMP_DATETIME;
				if(!IsTimeStampZero(myt)) {
					my_bool myb;
					// convert local time to UTC seconds from beg. of EPOCHE
					my_time_t secs_utc = ConnectionInfoOnTLS.Get().Thd().variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
					// UTC seconds converted to UTC TIME
					GMTSec2GMTTime(&myt, secs_utc);
				}
				rcdt = RCDateTime((short)myt.year, (short)myt.month, (short)myt.day, (short)myt.hour, (short)myt.minute, (short)myt.second, RC_TIMESTAMP);
			}
			val = rcdt.GetInt64();
			rcv = rcdt;
		}
	}
#endif
}

_int64 String2DateTimeCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
	if(full_const)
		return val;
	return ((RCDateTime*)DoGetValue(mit).Get())->GetInt64();
}

_int64 String2DateTimeCastColumn::DoGetValueInt64(const MIIterator& mit)
{
	if (full_const)
		return val;
	else {
		RCValueObject r(DoGetValue(mit));
		if (r.IsNull())
			return NULL_VALUE_64;
		else
			return ((RCDateTime*)r.Get())->GetInt64();
	}
}

RCValueObject String2DateTimeCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if (full_const)
		return rcv;
	else {
		RCDateTime rcdt;
		RCBString rbs;
		vc->GetValueString(rbs, mit);
		if (rbs.IsNull())
			return RCDateTime();
		else {
			if(BHReturn::IsError(RCDateTime::Parse(rbs, rcdt, TypeName()))) {
				std::string s = "Incorrect Date/Time value: ";
				s += (string)rbs;
				push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}
			return rcdt;
		}
	}
#endif

}

_int64 String2DateTimeCastColumn::DoGetMinInt64(const MIIterator& m)
{
	if(full_const)
		return val;
	else if(IsConst()) {
		// const with parameters
		return ((RCDateTime*)DoGetValue(m).Get())->GetInt64();
	} else
		return MINUS_INF_64;
}

_int64 String2DateTimeCastColumn::DoGetMaxInt64(const MIIterator& m)
{
	_int64 v = DoGetMinInt64(m);
	return v != MINUS_INF_64 ? v : PLUS_INF_64;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Int2DateTimeCastColumn::Int2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to) : String2DateTimeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if (full_const) {
		MIIterator mit(NULL);
		rcv = from->GetValue(mit);
		if (rcv.IsNull()) {
			val = NULL_VALUE_64;
		} else {
			RCDateTime rcdt;
			if(BHReturn::IsError(RCDateTime::Parse(rcv.ToRCString(), rcdt, TypeName()))) {
				std::string s = "Incorrect numeric value for Date/Time: ";
				s += (string)rcv.ToRCString();
				push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}
			val = rcdt.GetInt64();
			rcv = rcdt;
		}
	}
#endif

}

RCValueObject Int2DateTimeCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if (full_const)
		return rcv;
	else {
		RCDateTime rcdt;
		RCValueObject r(vc->GetValue(mit));
		if (!r.IsNull()) {
			if(BHReturn::IsError(RCDateTime::Parse(((RCNum)r).GetIntPart(), rcdt, TypeName()))) {
				std::string s = "Incorrect Date/Time value: ";
				s+= (string)r.ToRCString();
				push_warning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}
			return rcdt;
		} else
			return r;
	}
#endif

}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2VarcharCastColumn::DateTime2VarcharCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if (full_const){
		_int64 i = vc->GetValueInt64(mit);
		if (i == NULL_VALUE_64) {
			rcv = RCBString();
		} else {
			RCDateTime rcdt(i,TypeName());
			rcv = rcdt.ToRCString();
		}
	}
}

RCValueObject DateTime2VarcharCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if (full_const)
		return rcv;
	else {
		_int64 i = vc->GetValueInt64(mit);
		if (i == NULL_VALUE_64) {
			return RCBString();
		} else {
			RCDateTime rcdt(i,vc->TypeName());
			return rcdt.ToRCString();
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Num2VarcharCastColumn::Num2VarcharCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if (full_const){
		rcv = vc->GetValue(mit);
		if (rcv.IsNull()) {
			rcv = RCBString();
		} else {
			rcv = rcv.ToRCString();
		}
	}
}

RCValueObject Num2VarcharCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if (full_const)
		return rcv;
	else {
		RCValueObject r(vc->GetValue(mit));
		if (r.IsNull()) {
			return RCBString();
		} else {
			return r.ToRCString();
		}
	}
}

void Num2VarcharCastColumn::DoGetValueString(RCBString& s, const MIIterator& m)
{
	if(full_const)
		s = rcv.ToRCString();
	else
		s = vc->GetValue(m).ToRCString();
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2IntCastColumn::DateTime2IntCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if (full_const){
		rcv = vc->GetValue(mit);
		if (rcv.IsNull()) {
			rcv = RCNum();
			val = NULL_VALUE_64;
		} else {
			((RCDateTime*)rcv.Get())->ToInt64(val);
			rcv = RCNum(val,0,false,RC_BIGINT);
		}
	}
}

_int64 DateTime2IntCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
	if(full_const)
		return val;
	_int64 v;
	((RCDateTime*)vc->GetValue(mit).Get())->ToInt64(v);
	return v;
}

_int64 DateTime2IntCastColumn::DoGetValueInt64(const MIIterator& mit)
{
	if (full_const)
		return val;
	else {
		RCValueObject v(vc->GetValue(mit));
		if (v.IsNull()) {
			return NULL_VALUE_64;
		} else {
			_int64 r;
			((RCDateTime*)v.Get())->ToInt64(r);
			return r;
		}
	}
}

RCValueObject  DateTime2IntCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if (full_const)
		return rcv;
	else {
		RCValueObject v(vc->GetValue(mit));
		if (v.IsNull()) {
			return RCNum();
		} else {
			_int64 r;
			((RCDateTime*)v.Get())->ToInt64(r);
			return RCNum(r,0,false,RC_BIGINT);
		}
	}
}


