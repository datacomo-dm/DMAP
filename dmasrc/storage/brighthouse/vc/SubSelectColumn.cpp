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
#include "SubSelectColumn.h"
#include "core/MysqlExpression.h"
#include "ConstColumn.h"
#include "core/RCAttr.h"
#include "core/ValueSet.h"

using namespace std;
using namespace boost;

SubSelectColumn::SubSelectColumn(TempTable* subq, MultiIndex* mind, TempTable* temp_table, int temp_table_alias)
	:	MultiValColumn(subq->GetColumnType(0), mind), subq(static_pointer_cast<TempTable>(subq->shared_from_this())),
		min_max_uptodate(false), no_cached_values(0)
{
	const vector<JustATable*>* tables = &temp_table->GetTables();
	const vector<int>* aliases = &temp_table->GetAliases();

	col_idx = 0;
	for(uint i = 0; i < subq->NoAttrs(); i++)
		if(subq->IsDisplayAttr(i)) {
			col_idx = i;
			break;
		}
	ct = subq->GetColumnType(col_idx);
	MysqlExpression::SetOfVars all_params;

	for(uint i = 0; i < subq->NoVirtColumns(); i++) {
		MysqlExpression::SetOfVars params = subq->GetVirtualColumn(i)->GetParams();
		all_params.insert(params.begin(), params.end());
		MysqlExpression::bhfields_cache_t bhfields = subq->GetVirtualColumn(i)->GetBHItems();
		if(bhfields.size()) {
			for(MysqlExpression::bhfields_cache_t::const_iterator bhfield = bhfields.begin(); bhfield != bhfields.end(); ++bhfield) {
				MysqlExpression::bhfields_cache_t::iterator bhitem = bhitems.find(bhfield->first) ;
				if(bhitem == bhitems.end())
					bhitems.insert(*bhfield);
				else {
					bhitem->second.insert(bhfield->second.begin(), bhfield->second.end());
				}
			}
		}
	}

	for(MysqlExpression::SetOfVars::iterator iter = all_params.begin(); iter != all_params.end(); ++iter) {
		vector<int>::const_iterator ndx_it = find(aliases->begin(), aliases->end(), iter->tab);
		if(ndx_it != aliases->end()) {
			int ndx = (int)distance(aliases->begin(), ndx_it);

			var_map.push_back(VarMap(*iter,(*tables)[ndx], ndx));
			var_types[*iter] = (*tables)[ndx]->GetColumnType(var_map[var_map.size()-1].col_ndx);
			var_buf[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
		} else if(iter->tab == temp_table_alias) {
			var_map.push_back(VarMap(*iter, temp_table, 0));
			var_types[*iter] = temp_table->GetColumnType(var_map[var_map.size()-1].col_ndx);
			var_buf[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
		} else {
			//parameter
			params.insert(*iter);
		}
	}
	SetBufs(&var_buf);
	table.reset();
	table_for_rough.reset();

	if(var_map.size() == 1)
		dim = var_map[0].dim;
	else
		dim = -1;
	out_of_date_exact = true;
	out_of_date_rough = true;
}

SubSelectColumn::SubSelectColumn(const SubSelectColumn& c) : MultiValColumn(c), table(c.table), 
		col_idx(c.col_idx), subq(c.subq), min(c.min), max(c.max), min_max_uptodate(c.min_max_uptodate),
		expected_type(c.expected_type), var_buf(), 
		no_cached_values(c.no_cached_values), out_of_date_exact(c.out_of_date_exact),
		out_of_date_rough(c.out_of_date_rough)
{
	table_for_rough.reset();
	var_types.empty();
	if(c.cache.get()) {
		ValueSet* vs = new ValueSet(*c.cache.get());
		cache = boost::shared_ptr<ValueSet>(vs);
	}

}


SubSelectColumn::~SubSelectColumn( void )
{
//	if(table != subq)
//		delete table;
}

void SubSelectColumn::SetBufs(MysqlExpression::var_buf_t* bufs)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	assert(bufs);
	for( MysqlExpression::bhfields_cache_t::iterator it = bhitems.begin(); it != bhitems.end(); ++ it ) {
		MysqlExpression::var_buf_t::iterator buf_set = bufs->find(it->first);
		if(buf_set != bufs->end()) {
			//for each bhitem* in the set it->second put its buffer to buf_set.second
			for(std::set<Item_bhfield*>::iterator bhfield = it->second.begin(); bhfield != it->second.end(); ++bhfield) {
				ValueOrNull* von;
				(*bhfield)->SetBuf(von);
				buf_set->second.push_back(MysqlExpression::value_or_null_info_t(ValueOrNull(), von));
			}
		}
	}
#endif
}

RCBString SubSelectColumn::DoGetMinString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}

RCBString SubSelectColumn::DoGetMaxString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}

ushort SubSelectColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetPrecision();
}

PackOntologicalStatus SubSelectColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	return NORMAL;
}

void SubSelectColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	assert(0);
}

RSValue SubSelectColumn::DoRoughCheck(const MIIterator& mit, Descriptor& d)
{
	return RS_SOME; //not implemented
}

BHTribool SubSelectColumn::DoContains(MIIterator const& mit, RCDataType const& v)
{
	PrepareSubqCopy(mit, false);
	BHTribool res = false;
	if(!cache) {
		cache = boost::shared_ptr<ValueSet>(new ValueSet());
		cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
	}

	if(cache->Contains(v, GetCollation()))
		return true;
	else if(cache->ContainsNulls())
		res = BHTRIBOOL_UNKNOWN;

	bool added_new_value = false;
	if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
		for(_int64 i = no_cached_values; i < table->NoObj(); i++) {
			no_cached_values++;
			added_new_value = true;
			if(table->IsNull(i, col_idx)) {
				cache->AddNull();
				res = BHTRIBOOL_UNKNOWN;
			} else {
				RCBString val;
				table->GetTableString(val, i, col_idx);
				val.MakePersistent();
				cache->Add(val);
				if(CollationStrCmp(GetCollation(), val, v.ToRCString()) == 0) {
					res = true;
					break;
				}
			}
		}
	} else {
		for(_int64 i = no_cached_values; i < table->NoObj(); i++) {
			no_cached_values++;
			added_new_value = true;
			if(table->IsNull(i, col_idx)) {
				cache->AddNull();
				res = BHTRIBOOL_UNKNOWN;
			} else {
				RCValueObject value;
				if(Type().IsString()) {
					RCBString s ;
					table->GetTableString(s, i, col_idx);
					value = s;
					static_cast<RCBString*>(value.Get())->MakePersistent();
				} else
					value = table->GetTable(i, col_idx);
				cache->Add(value);
				if(value == v) {
					res = true;
					break;
				}
			}
		}
	}
	if(added_new_value && no_cached_values == table->NoObj())
		cache->ForcePreparation();				// completed for the first time => recalculate cache
	return res;
}

void SubSelectColumn::PrepareAndFillCache()
{
	if(!cache) {
		cache = boost::shared_ptr<ValueSet>(new ValueSet());
		cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
	}
	for(_int64 i = no_cached_values; i < table->NoObj(); i++) {
		no_cached_values++;
		if(table->IsNull(i, col_idx)) {
			cache->AddNull();
		} else {
			RCValueObject value;
			if(Type().IsString()) {
				RCBString s ;
				table->GetTableString(s, i, col_idx);
				value = s;
				static_cast<RCBString*>(value.Get())->MakePersistent();
			} else
				value = table->GetTable(i, col_idx);
			cache->Add(value);
		}
	}
	cache->ForcePreparation();
	cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
}

bool SubSelectColumn::IsSetEncoded(AttributeType at, int scale)	// checks whether the set is constant and fixed size equal to the given one
{
	if(!cache || !cache->EasyMode() || !table->IsMaterialized() || no_cached_values < table->NoObj())
		return false;
	return (scale == ct.GetScale() &&
			(at == expected_type.GetTypeName() ||
				(ATI::IsFixedNumericType(at) && ATI::IsFixedNumericType(expected_type.GetTypeName()))));
}

BHTribool SubSelectColumn::DoContains64(const MIIterator& mit, _int64 val)			// easy case for integers
{
	if(cache && cache->EasyMode()) {
		BHTribool contains = false;
		if(val == NULL_VALUE_64)
			return BHTRIBOOL_UNKNOWN;
		if(cache->Contains(val))
			contains = true;
		else if(cache->ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, RCNum(val, ct.GetScale()));
}

BHTribool SubSelectColumn::DoContainsString(const MIIterator& mit, RCBString &val)			// easy case for strings
{
	if(cache && cache->EasyMode()) {
		BHTribool contains = false;
		if(val.IsNull())
			return BHTRIBOOL_UNKNOWN;
		if(cache->Contains(val))
			contains = true;
		else if(cache->ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, val);
}

_int64 SubSelectColumn::DoNoValues(MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	if(!table->IsMaterialized())
		table->Materialize();
	return table->NoObj();
}

_int64 SubSelectColumn::DoAtLeastNoDistinctValues(MIIterator const& mit, _int64 const at_least)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(at_least > 0);
	PrepareSubqCopy(mit, false);
	ValueSet vals;
	vals.Prepare(expected_type.GetTypeName(), expected_type.GetScale(), expected_type.GetCollation());
	if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
		RCBString buf(NULL, CollationBufLen(GetCollation(), table->MaxStringSize(col_idx)), true);
		for(_int64 i = 0; vals.NoVals() < at_least && i < table->NoObj(); i++) {
			if(!table->IsNull(i, col_idx)) {
				RCBString s;
				table->GetTable_S(s, i, col_idx);
				ConvertToBinaryForm(s, buf, GetCollation());
				vals.Add(buf);
			}
		}
	} else {	
		for(_int64 i = 0; vals.NoVals() < at_least && i < table->NoObj(); i++) {
			if(!table->IsNull(i, col_idx)) {
				RCValueObject val = table->GetTable(i, col_idx);
				vals.Add(val);
			}
		}
	}
	return vals.NoVals();
}

bool SubSelectColumn::DoContainsNull(const MIIterator& mit)
{
	PrepareSubqCopy(mit, false);
	for(_int64 i = 0; i < table->NoObj(); ++i)
		if(table->IsNull(i, col_idx))
			return true;
	return false;
}

MultiValColumn::Iterator::impl_t SubSelectColumn::DoBegin(MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(table->begin(), expected_type));
}

MultiValColumn::Iterator::impl_t SubSelectColumn::DoEnd(MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(table->end(), expected_type));
}

void SubSelectColumn::PrepareSubqCopy(const MIIterator& mit, bool exists_only)
{
	MEASURE_FET("SubSelectColumn::PrepareSubqCopy(...)");
	bool cor = IsCorrelated();
	if(!cor) {
		if(!table)
			table = subq;
		if(table->IsFullyMaterialized())
			return;
	}

	if(cor && (FeedArguments(mit) || out_of_date_exact || !table)) {
		out_of_date_exact = false;
		table.reset();
		cache.reset();
		no_cached_values = 0;
		table = TempTable::Create(*subq, true);
		table->ResetVCStatistics();
		table->SuspendDisplay();
		try {
			table->ProcessParameters();		// exists_only is not needed here (limit already set)
		} catch(...) {
			table->ResumeDisplay();
			throw;
		}
		table->ResumeDisplay();
	}
	table->SuspendDisplay();
	try {
		if(exists_only)
			table->SetMode(TM_EXISTS);
		table->Materialize(true);
	} catch(...) {
		table->ResumeDisplay();
		throw;
	}
	table->ResumeDisplay();
}

void SubSelectColumn::RoughPrepareSubqCopy(const MIIterator& mit, SubSelectOptimizationType sot)
{
	MEASURE_FET("SubSelectColumn::RoughPrepareSubqCopy(...)");
	if((!IsCorrelated() && !table_for_rough) || 
		(IsCorrelated() && (FeedArguments(mit) || out_of_date_rough || !table_for_rough))) {
		out_of_date_rough = false;
		table_for_rough.reset();
		cache.reset();
		no_cached_values = 0;
		table_for_rough = TempTable::Create(*subq, true, true);
		table_for_rough->SuspendDisplay();
		try {
			table_for_rough->RoughProcessParameters();
		} catch(...) {
			table_for_rough->ResumeDisplay();
		}
		//table_for_rough->ResumeDisplay();
	} 
	table_for_rough->SuspendDisplay();
	try {
		table_for_rough->RoughMaterialize(true);
	} catch(...) {
		table_for_rough->ResumeDisplay();
	}
	table_for_rough->ResumeDisplay();
}

bool SubSelectColumn::IsCorrelated() const
{
	if(var_map.size() || params.size())
		return true;
	return false;
}

bool SubSelectColumn::DoIsNull(const MIIterator& mit)
{
	PrepareSubqCopy(mit, false);
	return table->IsNull(0, col_idx);
}

RCValueObject SubSelectColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num)
{
	PrepareSubqCopy(mit, false);
	RCValueObject val = table->GetTable(0, col_idx);
	if(expected_type.IsString())
		return val.ToRCString();
	return val;
}

_int64 SubSelectColumn::DoGetValueInt64(const MIIterator& mit)
{
	PrepareSubqCopy(mit, false);
	return table->GetTable64(0, col_idx);
}

RoughValue SubSelectColumn::RoughGetValue(const MIIterator& mit, SubSelectOptimizationType sot)
{
	RoughPrepareSubqCopy(mit, sot);
	return RoughValue(table_for_rough->GetTable64(0, col_idx), table_for_rough->GetTable64(1, col_idx));
}

double SubSelectColumn::DoGetValueDouble(const MIIterator& mit)
{
	PrepareSubqCopy(mit, false);
	_int64 v = table->GetTable64(0, col_idx);
	return *((double*)(&v));
}

void SubSelectColumn::DoGetValueString(RCBString& s, MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	table->GetTable_S(s, 0, col_idx);
}

RCValueObject SubSelectColumn::DoGetSetMin(MIIterator const& mit)
{
	// assert: this->params are all set
	PrepareSubqCopy(mit, false);
	if(!min_max_uptodate)
		CalculateMinMax();
	return min;
}

RCValueObject SubSelectColumn::DoGetSetMax(MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	if(!min_max_uptodate)
		CalculateMinMax();
	return max;
}
bool SubSelectColumn::CheckExists(MIIterator const& mit)
{
	PrepareSubqCopy(mit, true);		// true: exists_only
	return table->NoObj() > 0;
}

bool SubSelectColumn::DoIsEmpty(MIIterator const& mit)
{
	PrepareSubqCopy(mit, false);
	return table->NoObj() == 0;
}

BHTribool SubSelectColumn::RoughIsEmpty(MIIterator const& mit, SubSelectOptimizationType sot)
{
	RoughPrepareSubqCopy(mit, sot);
	return table_for_rough->RoughIsEmpty();
}

void SubSelectColumn::CalculateMinMax()
{
	if(!table->IsMaterialized())
		table->Materialize();
	if(table->NoObj() == 0) {
		min = max = RCValueObject();
		min_max_uptodate = true;
		return;
	}

	min = max = RCValueObject();
	RCValueObject val;
	bool found_not_null = false;
	if(RequiresUTFConversions(GetCollation()) && Type().IsString() && expected_type.IsString()) {
		RCBString val_s, min_s, max_s;
		for(_int64 i = 0; i < table->NoObj(); i++) {
			table->GetTable_S(val_s, i, col_idx);
			if(val_s.IsNull())
				continue;
			if(!found_not_null && !val_s.IsNull()) {
				found_not_null = true;
				min_s.PersistentCopy(val_s);
				max_s.PersistentCopy(val_s);
				continue;
			}
			if(CollationStrCmp(GetCollation(), val_s, max_s) > 0) {
				max_s.PersistentCopy(val_s);
			} else if(CollationStrCmp(GetCollation(), val_s, min_s) < 0) {
				min_s.PersistentCopy(val_s);
			}
		}
		min = min_s;
		max = max_s;
	} else {
		for(_int64 i = 0; i < table->NoObj(); i++) {
			val = table->GetTable(i, col_idx);
			if(expected_type.IsString()) {
				val = val.ToRCString();
				*static_cast<RCBString*>(val.Get())->MakePersistent();
			}
			else if(expected_type.IsNumeric() && ATI::IsStringType(val.Type())) {
				RCNum rc;
				RCNum::Parse(*static_cast<RCBString*>(val.Get()), rc, expected_type.GetTypeName());
				val = rc;
			}

			if(!found_not_null && !val.IsNull()) {
				found_not_null = true;
				min = max = val;
				continue;
			}
			if(val > max) {
				max = val;
			} else if(val < min) {
				min = val;
			}
		}
	}
	min_max_uptodate = true;
}

bool SubSelectColumn::FeedArguments(const MIIterator& mit)
{
	MEASURE_FET("SubSelectColumn::FeedArguments(...)");
	bool diff = first_eval;
	for(vector<VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); ++iter) {
		ValueOrNull v = iter->GetTabPtr()->GetComplexValue(mit[iter->dim], iter->col_ndx);
		v.MakeStringOwner();
		MysqlExpression::var_buf_t::iterator cache = var_buf.find(iter->var);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( cache != var_buf.end() );
		diff = diff || (v != cache->second.begin()->first);
		if(diff)
			for(std::vector<MysqlExpression::value_or_null_info_t>::iterator val_it = cache->second.begin(); val_it  != cache->second.end(); ++val_it)
				*((*val_it ).second) = (*val_it ).first = v;
	}
	first_eval = false;
	min_max_uptodate = false;
	if(diff) 
		out_of_date_exact = out_of_date_rough = true;
	return diff;
}

void SubSelectColumn::DoSetExpectedType(ColumnType const& ct)
{
	expected_type = ct;
}

bool SubSelectColumn::MakeParallelReady()
{
	MIDummyIterator mit(mind);
	PrepareSubqCopy(mit, false);
	if(!table->IsMaterialized())
		table->Materialize();
	if(table->NoObj() > table->GetPageSize())
		return false; //multipage Attrs - not thread safe
	// below assert doesn't take into account lazy field
	// NoMaterialized() tells how many rows in lazy mode are materialized 
	assert(table->NoObj() == table->NoMaterialized());
	PrepareAndFillCache();
	return true;
}
