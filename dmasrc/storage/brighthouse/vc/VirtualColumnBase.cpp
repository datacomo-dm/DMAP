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

#include <assert.h>
#include "core/CompiledQuery.h"
#include "core/MysqlExpression.h"
#include "vc/VirtualColumnBase.h"
#include "InSetColumn.h"
#include "SubSelectColumn.h"
#include "core/RCAttr.h"

using namespace std;

VirtualColumnBase::VirtualColumnBase(ColumnType const& ct, MultiIndex* mind)
	: 	Column(ct), mind(mind), conn_info(ConnectionInfoOnTLS.Get()), first_eval(true), dim(-1)
{
	ResetLocalStatistics();
}

VirtualColumnBase::VirtualColumnBase(VirtualColumn const& vc)
	:	 Column(vc.ct), mind(vc.mind), conn_info(vc.conn_info), var_map(vc.var_map), params(vc.params), first_eval(true), dim(vc.dim)
{
	ResetLocalStatistics();
}

void  VirtualColumnBase::MarkUsedDims(DimensionVector& dims_usage)
{
	for(var_maps_t::const_iterator it = var_map.begin(), end = var_map.end(); it != end; ++it)
		dims_usage[it->dim] = true;
}

VirtualColumnBase::dimensions_t VirtualColumnBase::GetDimensions()
{
	std::set<int> d;
	for(var_maps_t::const_iterator it = var_map.begin(), end = var_map.end(); it != end;++ it)
		d.insert(it->dim);
	return d;
}

_int64 VirtualColumnBase::NoTuples(void)
{
	if(mind == NULL)							// constant
		return 1;
	DimensionVector dims(mind->NoDimensions());
	MarkUsedDims(dims);
	return mind->NoTuples(dims);
}

bool VirtualColumnBase::IsConstExpression(MysqlExpression* expr, int temp_table_alias, const vector<int>* aliases)
{
	MysqlExpression::SetOfVars& vars = expr->GetVars(); 		// get all variables from complex term

	for(MysqlExpression::SetOfVars::iterator iter = vars.begin(); iter != vars.end(); iter++) {
		vector<int>::const_iterator ndx_it = find(aliases->begin(), aliases->end(), iter->tab);
		if(ndx_it != aliases->end())
			return false;
		else
			if(iter->tab == temp_table_alias)
				return false;
	}
	return true;
}

void VirtualColumnBase::SetMultiIndex(MultiIndex* m, JustATablePtr t)
{
	mind = m;
	if(t)
		for(vector<VarMap>::iterator iter = var_map.begin(); iter != var_map.end(); iter++) {
			(*iter).tab = t;
		}
}

void VirtualColumnBase::ResetLocalStatistics()
{
	//if(Type().IsFloat()) {
	//	vc_min_val = MINUS_INF_64;
	//	vc_max_val = PLUS_INF_64;
	//} else {
	//	vc_min_val = MINUS_INF_64;
	//	vc_max_val = PLUS_INF_64;
	//}
	vc_min_val = NULL_VALUE_64;
	vc_max_val = NULL_VALUE_64;
	vc_nulls_possible = true;
	vc_dist_vals = NULL_VALUE_64;
	vc_max_len = 65536;
	nulls_only = false;
}

void VirtualColumnBase::SetLocalMinMax(_int64 loc_min, _int64 loc_max)
{
	if(loc_min == NULL_VALUE_64)
		loc_min = MINUS_INF_64;
	if(loc_max == NULL_VALUE_64)
		loc_max = PLUS_INF_64;
	if(Type().IsFloat()) {
		if(vc_min_val == NULL_VALUE_64 || (loc_min != MINUS_INF_64 && (*(double*)&loc_min > *(double*)&vc_min_val || vc_min_val == MINUS_INF_64)))
			vc_min_val = loc_min;
		if(vc_max_val == NULL_VALUE_64 || (loc_max != PLUS_INF_64 && (*(double*)&loc_max < *(double*)&vc_max_val || vc_max_val == PLUS_INF_64)))
			vc_max_val = loc_max;
	} else {
		if(vc_min_val == NULL_VALUE_64 || loc_min > vc_min_val)
			vc_min_val = loc_min;
		if(vc_max_val == NULL_VALUE_64 || loc_max < vc_max_val)
			vc_max_val = loc_max;
	}
}

_int64 VirtualColumnBase::RoughMax()
{
	_int64 res = DoRoughMax();
	if(Type().IsFloat()) {
		if(*(double*)&res > *(double*)&vc_max_val && vc_max_val != PLUS_INF_64 && vc_max_val != NULL_VALUE_64)
			return vc_max_val;
	} else if(res > vc_max_val && vc_max_val != NULL_VALUE_64)
		return vc_max_val;
	return res;
}

_int64 VirtualColumnBase::RoughMin()
{
	_int64 res = DoRoughMin();
	if(Type().IsFloat()) {
		if(*(double*)&res < *(double*)&vc_min_val && vc_min_val != MINUS_INF_64 && vc_min_val != NULL_VALUE_64)
			return vc_min_val;
	} else if(res < vc_min_val && vc_min_val != NULL_VALUE_64)
		return vc_min_val;
	return res;
}

_int64 VirtualColumnBase::GetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	_int64 res = DoGetApproxDistVals(incl_nulls, rough_mind);
	if(vc_dist_vals != NULL_VALUE_64) {
		_int64 local_res = vc_dist_vals;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			res = local_res;
	}
	if(!Type().IsFloat() && vc_min_val > (MINUS_INF_64/3) && vc_max_val < (PLUS_INF_64/3)) {
		_int64 local_res = vc_max_val - vc_min_val + 1;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			return local_res;
	}
	if(Type().IsFloat() && vc_min_val != NULL_VALUE_64 && vc_min_val == vc_max_val) {
		_int64 local_res = 1;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			return local_res;
	}
	return res;
}

_int64 VirtualColumnBase::GetMaxInt64(const MIIterator& mit)
{
	_int64 res = DoGetMaxInt64(mit);
	if(Type().IsFloat()) {
		if(*(double*)&res > *(double*)&vc_max_val && vc_max_val != PLUS_INF_64 && vc_max_val != NULL_VALUE_64)
			return vc_max_val;
	} else if((vc_max_val != NULL_VALUE_64 && res > vc_max_val) || res == NULL_VALUE_64)
		return vc_max_val;
	return res;
}

_int64 VirtualColumnBase::GetMinInt64(const MIIterator& mit)
{
	_int64 res = DoGetMinInt64(mit);
	if(Type().IsFloat()) {
		if(*(double*)&res < *(double*)&vc_min_val && vc_min_val != MINUS_INF_64 && vc_min_val != NULL_VALUE_64)
			return vc_min_val;
	} else if((vc_min_val != NULL_VALUE_64 && res < vc_min_val) || res == NULL_VALUE_64)
		return vc_min_val;
	return res;
}

_int64 VirtualColumnBase::DecodeValueAsDouble(_int64 code)
{
	if(ATI::IsRealType(ct.GetTypeName()))
		return code;				// no conversion
	double res = double(code) / PowOfTen(ct.GetScale());
	return *(_int64*)(&res);
}
