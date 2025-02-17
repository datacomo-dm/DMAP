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

#include "ConditionEncoder.h"
#include "JustATable.h"
#include "CQTerm.h"
#include "edition/local.h"
#include "RCAttr.h"
#include "ValueSet.h"
#include "Descriptor.h"
#include "vc/SingleColumn.h"
#include "vc/InSetColumn.h"
#include "vc/ConstColumn.h"
#include "vc/SubSelectColumn.h"
#include "vc/ExpressionColumn.h"
#include "Query.h"

using namespace std;

ConditionEncoder::ConditionEncoder(bool additional_nulls) :
	additional_nulls(additional_nulls), in_type(ColumnType(RC_UNKNOWN)), sharp(false), encoding_done(false), attr(NULL), desc(NULL)
{
}

ConditionEncoder::~ConditionEncoder()
{
}

void ConditionEncoder::DoEncode()
{
	if(desc->IsType_AttrAttr())
		return;								// No actual encoding needed, just mark it as encoded. Type correspondence checked earlier.
	TransofrmWithRespectToNulls();
	if(!IsTransformationNeeded())
		return;

	DescriptorTransformation();
	if(!IsTransformationNeeded())
		return;

	if(ATI::IsStringType(AttrTypeName()))
		EncodeConditionOnStringColumn();
	else
 		EncodeConditionOnNumerics();
}

void ConditionEncoder::operator()(Descriptor& desc,Transaction *ptrans)
{
	MEASURE_FET("ConditionEncoder::operator()(...)");
	if(desc.encoded)
		return;

	desc.encoded = true;

/*	bool a = desc.attr.vc->GetVarMap()[0].col_ndx >= 0;
	bool b = desc.attr.vc->GetVarMap()[0].col_ndx <= (int)((desc.attr.vc->GetVarMap()[0]).GetTabPtr()->NoAttrs());
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( a && b); */

	this->attr = ((RCTable&)(*(desc.attr.vc->GetVarMap()[0].GetTabPtr()))).GetAttr(desc.attr.vc->GetVarMap()[0].col_ndx);
	attr->LoadPackInfo(ptrans==NULL?*ConnectionInfoOnTLS.Get().GetTransaction():*ptrans);
	in_type = attr->Type();
	this->desc = &desc;

	DoEncode();
}

bool ConditionEncoder::IsTransformationNeeded()
{
	return desc->encoded == true && 
		  !(encoding_done || desc->op == O_IS_NULL || desc->op == O_NOT_NULL || 
		    desc->op == O_FALSE || desc->op == O_TRUE);
}

void ConditionEncoder::DescriptorTransformation()
{
	MEASURE_FET("ConditionEncoder::DescriptorTransformation(...)");
	if(desc->op == O_IN || desc->op == O_NOT_IN) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<MultiValColumn*>(desc->val1.vc));
		return;
	}
	if(desc->op == O_EQ_ANY)
		desc->op = O_IN;

	if(desc->op == O_NOT_EQ_ALL)
		desc->op = O_NOT_IN;

	static MIIterator mit(0);
	if(desc->val1.IsNull() || (!IsSetOperator(desc->op) && 
		desc->val1.vc && desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit)))
		desc->op = O_FALSE;
	else if(desc->op == O_BETWEEN &&
		(desc->val1.IsNull() || (desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit)) ||
		desc->val2.IsNull() || (desc->val2.vc->IsConst() && desc->val2.vc->IsNull(mit))))
		desc->op = O_FALSE;
	else if(desc->op == O_NOT_BETWEEN) {		// O_NOT_BETWEEN may be changed to inequalities in case of nulls
		bool first_null  = (desc->val1.IsNull() || (desc->val1.vc->IsConst() && desc->val1.vc->IsNull(mit)));
		bool second_null = (desc->val2.IsNull() || (desc->val2.vc->IsConst() && desc->val2.vc->IsNull(mit)));
		if(first_null && second_null)
			desc->op = O_FALSE;
		else if(first_null) {				// a NOT BETWEEN null AND 5  <=>  a > 5
			desc->val1 = desc->val2;
			desc->val2 = CQTerm();
			desc->op = O_MORE;
		} else if(second_null) {			// a NOT BETWEEN 5 AND null  <=>  a < 5
			desc->op = O_LESS;
		}
	}

	if(IsSetAllOperator(desc->op) && (desc->val1.vc)->IsMultival() &&
			static_cast<MultiValColumn&>(*desc->val1.vc).NoValues(mit) == 0)
		desc->op = O_TRUE;
	else {

		if(desc->op == O_EQ_ALL && (desc->val1.vc)->IsMultival()) {
			MultiValColumn& mvc = static_cast<MultiValColumn&>(*desc->val1.vc);
			PrepareValueSet(mvc);
			if(mvc.NoValues(mit) == 0)
				desc->op = O_TRUE;
			else if(mvc.AtLeastNoDistinctValues(mit, 2) == 1 && !mvc.ContainsNull(mit)) {
				desc->op = O_EQ;
				desc->val1 = CQTerm();
				desc->val1.vc = new ConstColumn(mvc.GetSetMin(mit), mvc.Type());
				desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
			} else
				desc->op = O_FALSE;
		}

		if(desc->op == O_NOT_EQ_ANY && (desc->val1.vc)->IsMultival()) {
			MultiValColumn& mvc = static_cast<MultiValColumn&>(*desc->val1.vc);
			PrepareValueSet(mvc);
			if(mvc.NoValues(mit) == 0) {
				desc->op = O_FALSE;
				return;
			}
			int no_distinct = int(mvc.AtLeastNoDistinctValues(mit, 2));
			if(no_distinct == 0)
				desc->op = O_FALSE;
			else if(no_distinct == 2)
				desc->op = O_NOT_NULL;
			else {
				desc->op = O_NOT_EQ;
				desc->val1 = CQTerm();
				desc->val1.vc = new ConstColumn(mvc.GetSetMin(mit), mvc.Type());
				desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
			}
		}
		if(desc->op == O_FALSE || desc->op == O_TRUE)
			return;
		if(!IsSetOperator(desc->op) && (desc->val1.vc)->IsMultival()) {
			MultiValColumn& mvc = static_cast<MultiValColumn&>(*desc->val1.vc);
			VirtualColumn* vc = new ConstColumn(mvc.GetValue(mit), mvc.Type());
			desc->val1.vc = vc;
			desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
			desc->CoerceColumnType(desc->val1.vc);
		}
	}
}

void ConditionEncoder::TransofrmWithRespectToNulls()
{
	MEASURE_FET("ConditionEncoder::TransofrmWithRespectToNulls(...)");
	bool nulls_only = (attr->NoNulls() == attr->NoObj());
	bool nulls_possible = (additional_nulls || attr->NoNulls() > 0);

	if(desc->op == O_IS_NULL) {
		if(!nulls_possible || attr->NoObj() == 0)
			desc->op = O_FALSE;
		else if(nulls_only)
			desc->op = O_TRUE;
	} else if(desc->op == O_NOT_NULL) {
		if(!nulls_possible)
			desc->op = O_TRUE;
		else if(nulls_only)
			desc->op = O_FALSE;
	}

	if((IsSetAllOperator(desc->op) || desc->op == O_NOT_IN) && desc->val1.vc && desc->val1.vc->IsMultival()) { 
		MultiValColumn* mvc = static_cast<MultiValColumn*>(desc->val1.vc);
		MIIterator mit(0);
		// Change to O_FALSE for non-subselect columns, or non-correlated subselects (otherwise ContainsNulls needs feeding arguments)
		if(mvc->ContainsNull(mit))
			desc->op = O_FALSE;
	}
}

void ConditionEncoder::PrepareValueSet(MultiValColumn& mvc)
{
	mvc.SetExpectedType(attr->Type());
}

void ConditionEncoder::EncodeConditionOnStringColumn()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ATI::IsStringType(AttrTypeName()));

	TextTransformation();
	if(!IsTransformationNeeded())
		return;

	if(desc->op == O_IN || desc->op == O_NOT_IN)
		TransformINs();
	else if(!attr->Type().IsLookup())
		TransformOtherThanINsOnNotLookup();
	else
		TransformOtherThanINsOnNumerics();
}

void ConditionEncoder::EncodeConditionOnNumerics()
{
	if(desc->op == O_IN || desc->op == O_NOT_IN)
		TransformINs();
	else
		TransformOtherThanINsOnNumerics();
}

void ConditionEncoder::TransformOtherThanINsOnNumerics()
{
	MEASURE_FET("ConditionEncoder::TransformOtherThanINsOnNumerics(...)");
	_int64 v1, v2;
 	bool v1_rounded = false, v2_rounded = false;
	static MIIterator mit(0);

	MultiValColumn* mvc = 0;
	if(desc->val1.vc && (desc->val1.vc)->IsMultival()) {
		mvc = static_cast<MultiValColumn*>(desc->val1.vc);
		PrepareValueSet(*mvc); // else it was already done above
		// O_EQ_ANY = O_IN processed by other function, O_NOT_EQ_ANY processed on higher level
		if(desc->op == O_LESS_ANY || desc->op == O_LESS_EQ_ANY || desc->op == O_MORE_ALL || desc->op == O_MORE_EQ_ALL)
			v1 = attr->EncodeValue64(mvc->GetSetMax(mit).Get(), v1_rounded); // 1-level values
		else //	ANY && MORE or ALL && LESS
			v1 = attr->EncodeValue64(mvc->GetSetMin(mit).Get(), v1_rounded); // 1-level values
	} else if(desc->val1.vc->IsConst()) {
		BHReturnCode bhrc = BHRC_SUCCESS;
		v1 = attr->EncodeValue64(desc->val1.vc->GetValue(mit), v1_rounded, &bhrc); // 1-level values
		if(bhrc != BHRC_SUCCESS) {
			desc->encoded = false;
			throw DataTypeConversionRCException(BHERROR_DATACONVERSION);
		}
	}

	if(desc->val2.vc && (desc->val2.vc)->IsMultival()) {
		mvc = static_cast<MultiValColumn*>(desc->val2.vc);
		PrepareValueSet(*mvc);
		// only for BETWEEN
		if(IsSetAnyOperator(desc->op))
			v2 = attr->EncodeValue64(mvc->GetSetMax(mit).Get(), v2_rounded); // 1-level values
		else
			//	ALL
			v2 = attr->EncodeValue64(mvc->GetSetMin(mit).Get(), v2_rounded); // 1-level values
	} else {
		BHReturnCode bhrc = BHRC_SUCCESS;
		if(!desc->val2.IsNull() && desc->val2.vc && desc->val2.vc->IsConst()) {
			v2 = attr->EncodeValue64(desc->val2.vc->GetValue(mit), v2_rounded, &bhrc); // 1-level values
			if(bhrc != BHRC_SUCCESS) {
				desc->encoded = false;
				throw DataTypeConversionRCException(BHERROR_DATACONVERSION);
			}
		} else
			v2 = NULL_VALUE_64;
	}

	if(v1_rounded) {
		if(ISTypeOfEqualOperator(desc->op)) {
			desc->op = O_FALSE;
			return;
		}

		if(ISTypeOfNotEqualOperator(desc->op)) {
			desc->op = O_NOT_NULL;
			return;
		}

		if(ISTypeOfLessOperator(desc->op) && v1 >= 0)
			desc->op = O_LESS_EQ;

		if(ISTypeOfLessEqualOperator(desc->op) && v1 < 0)
			desc->op = O_LESS;

		if(ISTypeOfMoreOperator(desc->op) && v1 < 0)
			desc->op = O_MORE_EQ;

		if(ISTypeOfMoreEqualOperator(desc->op) && v1 >= 0)
			desc->op = O_MORE;

		if((desc->op == O_BETWEEN || desc->op == O_NOT_BETWEEN) && v1 >= 0)
			v1 += 1;
	}

	if(v2_rounded && (desc->op == O_BETWEEN || desc->op == O_NOT_BETWEEN) && v2 < 0)
		v2 -= 1;

	if(v1 == NULL_VALUE_64 || (desc->op == O_BETWEEN && v2 == NULL_VALUE_64)) { // any comparison with null values only
		desc->op = O_FALSE;
		return;
	}

	if(ISTypeOfEqualOperator(desc->op) || ISTypeOfNotEqualOperator(desc->op))
		v2 = v1;

	if(ISTypeOfLessOperator(desc->op)) {
		if(!ATI::IsRealType(AttrTypeName())) {
			if(v1 > MINUS_INF_64)
				v2 = v1 - 1;
			v1 = MINUS_INF_64;
		} else {
			if(*(double*) &v1 > MINUS_INF_DBL)
				v2 = (AttrTypeName() == RC_REAL) ? DoubleMinusEpsilon(v1) : FloatMinusEpsilon(v1);
			v1 = *(_int64 *)&MINUS_INF_DBL;
		}
	}

	if(ISTypeOfMoreOperator(desc->op)) {
		if(!ATI::IsRealType(AttrTypeName())) {
			if(v1 != PLUS_INF_64)
				v1 += 1;
			v2 = PLUS_INF_64;
		} else {
			if(*(double*) &(v1) != PLUS_INF_DBL)
				v1 = (AttrTypeName() == RC_REAL) ? DoublePlusEpsilon(v1) : FloatPlusEpsilon(v1);
			v2 = *(_int64 *)&PLUS_INF_DBL;
		}
	}

	if(ISTypeOfLessEqualOperator(desc->op)) {
		v2 = v1;
		if(!ATI::IsRealType(AttrTypeName()))
			v1 = MINUS_INF_64;
		else
			v1 = *(_int64*)&MINUS_INF_DBL;
	}

	if(ISTypeOfMoreEqualOperator(desc->op)) {
		if(!ATI::IsRealType(AttrTypeName()))
			v2 = PLUS_INF_64;
		else
			v2 = *(_int64*)&PLUS_INF_DBL;
	}

	desc->sharp = false;
	if(ISTypeOfNotEqualOperator(desc->op) || desc->op == O_NOT_BETWEEN)
		desc->op = O_NOT_BETWEEN;
	else
		desc->op = O_BETWEEN;

	desc->val1 = CQTerm();
	desc->val1.vc = new ConstColumn(ValueOrNull(RCNum(v1, attr->Type().GetScale(), ATI::IsRealType(AttrTypeName()), AttrTypeName())), attr->Type());
	desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);

	desc->val2 = CQTerm();
	desc->val2.vc = new ConstColumn(ValueOrNull(RCNum(v2, attr->Type().GetScale(), ATI::IsRealType(AttrTypeName()), AttrTypeName())), attr->Type());
	desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
}

void ConditionEncoder::TransformLIKEsPattern()
{
	MEASURE_FET("ConditionEncoder::TransformLIKEsPattern(...)");
	static MIIterator const mit(NULL);
	RCBString pattern;
	desc->val1.vc->GetValueString(pattern, mit);
	int min_len = 0;
	bool esc = false;
	for(uint i = 0; i < pattern.len; i++) {
		if(pattern[i] != '%')
			min_len++;
		else
			esc = true;
	}

	if(min_len == 0) {
		if(esc) {
			if(desc->op == O_LIKE)
				desc->op = O_NOT_NULL;
			else
				desc->op = O_FALSE;
		} else {
			if(desc->op == O_LIKE)
				desc->op = O_EQ;
			else
				desc->op = O_NOT_EQ;
		}
	} else if(min_len > attr->Type().GetPrecision()) {
		if(desc->op == O_LIKE)
			desc->op = O_FALSE;
		else
			desc->op = O_NOT_NULL;
	} else if(attr->Type().IsLookup())
		TransformLIKEsIntoINsOnLookup();
	else
		encoding_done = true;
}

void ConditionEncoder::TransformLIKEsIntoINsOnLookup()
{
	MEASURE_FET("ConditionEncoder::TransformLIKEsIntoINsOnLookup(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr->Type().IsLookup());

	if(desc->op == O_LIKE)
		desc->op = O_IN;
	else
		desc->op = O_NOT_IN;

	ValueSet valset;
	static MIIterator mid(0);
	in_type = ColumnType(RC_NUM);
	RCBString pattern;
	desc->val1.vc->GetValueString(pattern, mid);
	for(int i = 0; i < attr->dic->CountOfUniqueValues(); i++) {
		int res;
		if(RequiresUTFConversions(desc->GetCollation())) {
			RCBString s = attr->dic->GetRealValue(i);
			res = ! wildcmp(desc->GetCollation(), s.val, s.val + s.len,
					pattern.val, pattern.val + pattern.len,
					desc->like_esc, '_', '%');
		} else
			res = attr->dic->GetRealValue(i).Like(pattern, desc->like_esc);
		if(res)
			valset.Add64(i);
	}
	desc->val1.vc = new InSetColumn(in_type, NULL, valset);
	desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
	if(static_cast<InSetColumn&>(*desc->val1.vc).IsEmpty(mid)) {
		if(desc->op == O_IN)
			desc->op = O_FALSE;
		else
			desc->op = O_NOT_NULL;
	}
}

void ConditionEncoder::TransformLIKEs()
{
	MEASURE_FET("ConditionEncoder::TransformLIKEs(...)");
	TransformLIKEsPattern();
    if(!IsTransformationNeeded())
        return;

    if(attr->Type().IsLookup() && (desc->op == O_LIKE || desc->op == O_NOT_LIKE))
    	TransformLIKEsIntoINsOnLookup();
}

void ConditionEncoder::TransformINsOnLookup()
{
	MEASURE_FET("ConditionEncoder::TransformINsOnLookup(...)");
	static MIIterator mid(0);
	ValueSet valset;
    RCBString s;
    for(int i = 0; i < attr->dic->CountOfUniqueValues(); i++) {
        s = attr->dic->GetRealValue(i);
        if((static_cast<MultiValColumn*>(desc->val1.vc))->Contains(mid, s) == true)
			valset.Add64(i);
    }
    in_type = ColumnType(RC_NUM);
	desc->val1.vc = new InSetColumn(in_type, NULL, valset);
	desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
	if(static_cast<InSetColumn&>(*desc->val1.vc).IsEmpty(mid)) {
		if(desc->op == O_IN)
			desc->op = O_FALSE;
		else
			desc->op = O_NOT_NULL;
	}
}

void ConditionEncoder::TransformIntoINsOnLookup()
{
	MEASURE_FET("ConditionEncoder::TransformIntoINsOnLookup(...)");
	static MIIterator mit(0);
	ValueSet vset_positive;
	ValueSet vset_negative;
	RCBString s, vs1, vs2;
	RCValueObject vo;
	if(desc->val1.vc->IsMultival() && static_cast<MultiValColumn&>(*desc->val1.vc).NoValues(mit) > 0) {
		if((desc->op == O_LESS_ALL || desc->op == O_LESS_EQ_ALL) || (desc->op == O_MORE_ANY || desc->op == O_MORE_EQ_ANY))
			vo = static_cast<MultiValColumn&>(*desc->val1.vc).GetSetMin(mit);
		else if((desc->op == O_LESS_ANY || desc->op == O_LESS_EQ_ANY) || (desc->op == O_MORE_ALL || desc->op == O_MORE_EQ_ALL))
			vo = static_cast<MultiValColumn&>(*desc->val1.vc).GetSetMax(mit);
		if(vo.Get())
			vs1 = vo.Get()->ToRCString();
	} else if(desc->val1.vc->IsConst())
		desc->val1.vc->GetValueString(vs1, mit);

	if(desc->op == O_BETWEEN || desc->op == O_NOT_BETWEEN) {
		if(desc->val2.vc->IsMultival() && static_cast<MultiValColumn&>(*desc->val2.vc).NoValues(mit) > 0) {
			vo = static_cast<MultiValColumn&>(*desc->val2.vc).GetSetMin(mit);
			if(vo.Get())
				vs2 = vo.Get()->ToRCString();
		} else if(desc->val2.vc->IsConst())
			desc->val2.vc->GetValueString(vs2, mit);
	}

	if(vs1.IsNull() && vs2.IsNull()) {
		desc->op = O_FALSE;
	} else {
		in_type = ColumnType(RC_NUM);
		int cmp1 = 0, cmp2 = 0;
		int count1 = 0, count0 = 0;
		bool single_value_search = (ISTypeOfEqualOperator(desc->op) && !RequiresUTFConversions(desc->GetCollation()));
		bool utf = RequiresUTFConversions(desc->GetCollation());
		for(int i = 0; i < attr->dic->CountOfUniqueValues(); i++) {
			s = attr->dic->GetRealValue(i);
			if(utf)
				cmp1 = CollationStrCmp(desc->GetCollation(), s, vs1);
			else
				cmp1 = strcmp(s, vs1);

			if(desc->op == O_BETWEEN || desc->op == O_NOT_BETWEEN)
				if(utf)
					cmp2 = CollationStrCmp(desc->GetCollation(), s, vs2);
				else
					cmp2 = strcmp(s, vs2);

			if(	(ISTypeOfEqualOperator(desc->op) && cmp1 == 0) ||
				(ISTypeOfNotEqualOperator(desc->op) && cmp1 != 0) ||
				(ISTypeOfLessOperator(desc->op) && cmp1 < 0) ||
				(ISTypeOfMoreOperator(desc->op) && cmp1 > 0) ||
				(ISTypeOfLessEqualOperator(desc->op) && cmp1 <= 0) ||
				(ISTypeOfMoreEqualOperator(desc->op) && cmp1 >= 0) ||
				((desc->op == O_BETWEEN) && cmp1 >= 0 && cmp2 <= 0) ||
				((desc->op == O_NOT_BETWEEN) && (cmp1 < 0 || cmp2 > 0))	) {

				if(count1 <= attr->dic->CountOfUniqueValues() / 2 + 1)
					vset_positive.Add64(i);
				count1++;
				if(single_value_search) {
					count0 += attr->dic->CountOfUniqueValues() - i - 1;
					break;
				}
			} else {
				if(!single_value_search && count0 <= attr->dic->CountOfUniqueValues() / 2 + 1)
					vset_negative.Add64(i);
				count0++;
			}
		}

		if(count1 <= count0) {
			desc->op = O_IN;
			desc->val1.vc = new InSetColumn(in_type, NULL, vset_positive /* *vset.release()*/);
			desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
		} else {
			desc->op = O_NOT_IN;
			desc->val1.vc = new InSetColumn(in_type, NULL, vset_negative /* *vset_n.release() */);
			desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
		}
	}
}

void ConditionEncoder::TextTransformation()
{
	MEASURE_FET("ConditionEncoder::TextTransformation(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ATI::IsStringType(AttrTypeName()));

	if(desc->attr.vc) {
		ExpressionColumn* vcec = dynamic_cast<ExpressionColumn*>(desc->attr.vc);
		if(vcec && vcec->ExactlyOneLookup()) {
			LookupExpressionTransformation();
			return;
		}
	}
	if(desc->op == O_LIKE || desc->op == O_NOT_LIKE) {
		TransformLIKEs();
	} else if(attr->Type().IsLookup()) { // lookup - transform into IN (numbers)
		if(desc->op == O_IN || desc->op == O_NOT_IN)
			TransformINsOnLookup();
		else
			TransformIntoINsOnLookup();
	}
}

void ConditionEncoder::TransformINs()
{
	MEASURE_FET("ConditionEncoder::TransformINs(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<MultiValColumn*>(desc->val1.vc));

	static MIIterator mit(0);

	MultiValColumn& mvc = static_cast<MultiValColumn&>(*desc->val1.vc);
	mvc.SetExpectedType(in_type);

	_int64 no_dis_values = mvc.AtLeastNoDistinctValues(mit, 2);

    if(no_dis_values == 0) {
        if(desc->op == O_IN)
            desc->op = O_FALSE;
        else if(!mvc.ContainsNull(mit))
            desc->op = O_NOT_NULL;
        else
        	desc->op = O_FALSE;
    } else {
		if(no_dis_values == 1) {
			if(attr->PackType() == PackN && !attr->Type().IsLookup()) {
				desc->val2 = CQTerm();
				desc->val2.vc = new ConstColumn(ValueOrNull(RCNum(attr->EncodeValue64(mvc.GetSetMin(mit), sharp), in_type.GetTypeName())), in_type);
				desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
			} else {
				desc->val2 = CQTerm();
				desc->val2.vc = new ConstColumn(mvc.GetSetMin(mit), in_type);
				desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
			}

			if(sharp) {
				if(desc->op == O_IN)
					desc->op = O_FALSE;
				else
					desc->op = O_NOT_NULL;
			} else {
				desc->val1 = desc->val2;
				if(desc->op == O_IN)
					desc->op = O_BETWEEN;
				else if(!mvc.ContainsNull(mit))
					desc->op = O_NOT_BETWEEN;
				else
					desc->op = O_FALSE;
			}
		} else if(attr->PackType() == PackN && !mvc.ContainsNull(mit)) {
			_int64 val_min, val_max;
			if(attr->Type().IsLookup()) {
				val_min = _int64((RCNum&)mvc.GetSetMin(mit));
				val_max = _int64((RCNum&)mvc.GetSetMax(mit));
			} else {
				val_min = attr->EncodeValue64(mvc.GetSetMin(mit), sharp);
				val_max = attr->EncodeValue64(mvc.GetSetMax(mit), sharp);
			}
			_int64 span = val_max - val_min + 1;
			if(attr->Type().GetScale() < 1 && span > 0 && span < 65536		// otherwise AtLeast... will be too costly
				&& span == mvc.AtLeastNoDistinctValues(mit, span + 1)) {

				desc->val2.vc = new ConstColumn(ValueOrNull(RCNum(val_max, attr->Type().GetScale(), ATI::IsRealType(in_type.GetTypeName()), in_type.GetTypeName())), in_type);
				desc->val2.vc_id = desc->table->AddVirtColumn(desc->val2.vc);
				desc->val1.vc = new ConstColumn(ValueOrNull(RCNum(val_min, attr->Type().GetScale(), ATI::IsRealType(in_type.GetTypeName()), in_type.GetTypeName())), in_type);
				desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
				if(desc->op == O_IN)
					desc->op = O_BETWEEN;
				else
					desc->op = O_NOT_BETWEEN;
			}
		}
    }
}

void ConditionEncoder::TransformOtherThanINsOnNotLookup()
{
	MEASURE_FET("ConditionEncoder::TransformOtherThanINsOnNotLookup(...)");
	CQTerm v1 = desc->val1;
	CQTerm v2 = desc->val2;
	sharp = false;
	////////////// (SOME, ALL) //////////////////////

	MIIterator mit(0);
	MultiValColumn* mvc = 0;
	if(v1.vc && v1.vc->IsMultival()) {
		mvc = static_cast<MultiValColumn*>(v1.vc);
		PrepareValueSet(*mvc); // else it was already done above
		if(desc->op == O_LESS_ANY || desc->op == O_LESS_EQ_ANY || desc->op == O_MORE_ALL || desc->op == O_MORE_EQ_ALL) {
			v1.vc = new ConstColumn(mvc->GetSetMax(mit), mvc->Type());
			v1.vc_id = desc->table->AddVirtColumn(v1.vc);
		} else {
			//BHASSERT(desc->op != O_NOT_BETWEEN, "desc->op should not be O_NOT_BETWEEN at this point!");
			v1.vc = new ConstColumn(mvc->GetSetMin(mit), mvc->Type());
			v1.vc_id = desc->table->AddVirtColumn(v1.vc);
		}
	}

	if(v2.vc && v2.vc->IsMultival()) {
		mvc = static_cast<MultiValColumn*>(v2.vc);
		//BHASSERT(desc->op != O_NOT_BETWEEN, "desc->op should not be O_NOT_BETWEEN at this point!");
		PrepareValueSet(*mvc);
		// only for BETWEEN
		if(IsSetAnyOperator(desc->op)) {
			v2.vc = new ConstColumn(mvc->GetSetMax(mit), mvc->Type());
			v2.vc_id = desc->table->AddVirtColumn(v2.vc);
		} else { //	ALL
			v2.vc = new ConstColumn(mvc->GetSetMin(mit), mvc->Type());
			v2.vc_id = desc->table->AddVirtColumn(v2.vc);
		}
	}

	if(	(v1.IsNull() || (v1.vc && v1.vc->IsMultival() && v1.vc->IsNull(mit))) &&
		(v2.IsNull() || (v2.vc && v2.vc->IsMultival() && v2.vc->IsNull(mit))) ) {
		desc->op = O_FALSE;
	} else {
		if(v1.vc && v1.vc->IsMultival() && !v1.vc->IsNull(mit))
			desc->CoerceColumnType(v1.vc);

		if(v2.vc && v2.vc->IsMultival() && !v2.vc->IsNull(mit))
			desc->CoerceColumnType(v2.vc);

		if(ISTypeOfEqualOperator(desc->op) || ISTypeOfNotEqualOperator(desc->op))
			v2 = v1;

		if(ISTypeOfLessOperator(desc->op) || ISTypeOfLessEqualOperator(desc->op)) {
			v2 = v1;
			v1 = CQTerm();
			v1.vc = new ConstColumn(ValueOrNull(), attr->Type());
			v1.vc_id = desc->table->AddVirtColumn(v1.vc);
		} else if(ISTypeOfMoreOperator(desc->op) || ISTypeOfMoreEqualOperator(desc->op)) {
			v2 = CQTerm();
			v2.vc = new ConstColumn(ValueOrNull(), attr->Type());
			v2.vc_id = desc->table->AddVirtColumn(v2.vc);
		}

		if(ISTypeOfLessOperator(desc->op) || ISTypeOfMoreOperator(desc->op))
			sharp = true;

		if(ISTypeOfNotEqualOperator(desc->op) || desc->op == O_NOT_BETWEEN)
			desc->op = O_NOT_BETWEEN;
		else
			desc->op = O_BETWEEN; // O_IN, O_LIKE etc. excluded earlier

		desc->sharp = sharp;
		desc->val1 = v1;
		desc->val2 = v2;
	}
}

void ConditionEncoder::EncodeIfPossible(Descriptor& desc, bool for_rough_query, bool additional_nulls,Transaction *ptrans)
{
	MEASURE_FET("ConditionEncoder::EncodeIfPossible(...)");
	if(desc.done || desc.IsDelayed())
		return;
	if(desc.IsType_OrTree()) {
		desc.tree->root->EncodeIfPossible(for_rough_query, additional_nulls);
		desc.Simplify();
		return;
	}
	if(!desc.attr.vc || desc.attr.vc->GetDim() == -1)
		return;

	SingleColumn* vcsc = (desc.attr.vc->IsSingleColumn() ? static_cast<SingleColumn*>(desc.attr.vc) : NULL);

	bool encode_now = false;
	if(desc.IsType_AttrAttr() && IsSimpleEqualityOperator(desc.op) && vcsc) {
		// special case: simple operator on two compatible numerical columns
		SingleColumn* vcsc2 = NULL;
		if(desc.val1.vc->IsSingleColumn())
			vcsc2 = static_cast<SingleColumn*>(desc.val1.vc);
		if(vcsc2 == NULL ||	vcsc->GetVarMap()[0].GetTabPtr()->TableType() != RC_TABLE || 
			vcsc2->GetVarMap()[0].GetTabPtr()->TableType() != RC_TABLE)
			return;
		if(	vcsc->Type().IsString()  || vcsc->Type().IsLookup()  || 
			vcsc2->Type().IsString() || vcsc2->Type().IsLookup())					// excluding strings
			return;
		bool is_timestamp1 = (vcsc->Type().GetTypeName() == RC_TIMESTAMP);
		bool is_timestamp2 = (vcsc2->Type().GetTypeName() == RC_TIMESTAMP);
		if(is_timestamp1 || is_timestamp2 && !(is_timestamp1 && is_timestamp2))		// excluding timestamps compared with something else
			return;

		encode_now =(vcsc->Type().IsDateTime() && vcsc2->Type().IsDateTime()) ||
					(vcsc->Type().IsFloat() && vcsc2->Type().IsFloat()) ||
					(vcsc->Type().IsFixed() && vcsc2->Type().IsFixed() && 
					 vcsc->Type().GetScale() == vcsc2->Type().GetScale());			// excluding floats
	}

	if(!encode_now) {
		ExpressionColumn* vcec = dynamic_cast<ExpressionColumn*>(desc.attr.vc);
		if(vcec == NULL && (vcsc == NULL || vcsc->GetVarMap()[0].GetTabPtr()->TableType() != RC_TABLE))
			return;
		if(vcec != NULL) { 
			encode_now = (vcec->ExactlyOneLookup() && 
							(desc.op == O_IS_NULL || desc.op == O_NOT_NULL || 
							(desc.val1.vc && desc.val1.vc->IsConst() && 
							(desc.val2.vc == NULL || desc.val2.vc->IsConst()))));
		} else {
			encode_now = (desc.IsType_AttrValOrAttrValVal() ||
						  desc.IsType_AttrMultiVal() || 
						  desc.op == O_IS_NULL || desc.op == O_NOT_NULL )	&&
						  desc.attr.vc->GetVarMap()[0].GetTabPtr()->TableType() == RC_TABLE &&
						  (!for_rough_query || !desc.IsType_Subquery());
		}
	}
	if(!encode_now)
		return;
	/////////////////////////////////////////////////////////////////////////////////////
	// Encoding itself
	ConditionEncoder ce(additional_nulls);
	ce(desc,ptrans);
	desc.Simplify();
}

void ConditionEncoder::LookupExpressionTransformation()
{
	MEASURE_FET("ConditionEncoder::LookupExpressionTransformation(...)");
	ExpressionColumn* vcec = dynamic_cast<ExpressionColumn*>(desc->attr.vc);
	VirtualColumnBase::VarMap col_desc = vcec->GetLookupCoordinates();
	MILookupIterator mit;
	mit.Set(NULL_VALUE_64);
	bool null_positive = desc->CheckCondition(mit);
	ValueSet valset;
	in_type = ColumnType(RC_NUM);
	int code = 0;
	do {
		mit.Set(code);
		if(desc->CheckCondition(mit)) {
			if(mit.IsValid())
				valset.Add64(code);
		}
		code++;
	} while(mit.IsValid());

	if(!null_positive) {
		PhysicalColumn* col = col_desc.GetTabPtr()->GetColumn(col_desc.col_ndx);
		desc->attr.vc = new SingleColumn(col, vcec->GetMultiIndex(), col_desc.var.tab, col_desc.col_ndx, col_desc.GetTabPtr().get(), vcec->GetDim());
		desc->attr.vc_id = desc->table->AddVirtColumn(desc->attr.vc);
		desc->op = O_IN;
		desc->val1.vc = new InSetColumn(in_type, NULL, valset);
		desc->val1.vc_id = desc->table->AddVirtColumn(desc->val1.vc);
	} else if(valset.IsEmpty()) {
		PhysicalColumn* col = col_desc.GetTabPtr()->GetColumn(col_desc.col_ndx);
		desc->attr.vc = new SingleColumn(col, vcec->GetMultiIndex(), col_desc.var.tab, col_desc.col_ndx, col_desc.GetTabPtr().get(), vcec->GetDim());
		desc->attr.vc_id = desc->table->AddVirtColumn(desc->attr.vc);
		desc->op = O_IS_NULL;
	} else {	// both nulls and not-nulls are positive - no single operator possible
		// do not encode
		desc->encoded = false;
	}
}
