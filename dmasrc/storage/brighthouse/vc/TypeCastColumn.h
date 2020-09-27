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

#ifndef TYPECASTCOLUMN_H_
#define TYPECASTCOLUMN_H_

#include "edition/vc/VirtualColumn.h"
#include "core/MIIterator.h"

class TypeCastColumn : public VirtualColumn {

public:
	TypeCastColumn(VirtualColumn* from, ColumnType const& to);
	TypeCastColumn(const TypeCastColumn &c);

	const std::vector<VarMap>& GetVarMap() const {return vc->GetVarMap();}
//	void SetParam(VarID var, ValueOrNull val) {vc->SetParam(var, val);}
	void LockSourcePacks(const MIIterator& mit) {vc->LockSourcePacks( mit);}
	void LockSourcePacks(const MIIterator& mit, int th_no);
	void UnlockSourcePacks() {vc->UnlockSourcePacks();}
	void MarkUsedDims(DimensionVector& dims_usage) {vc->MarkUsedDims(dims_usage);}
	dimensions_t GetDimensions() { return vc->GetDimensions();}
	_int64 NoTuples() { return vc->NoTuples(); }
	bool IsConstExpression(MysqlExpression* expr, int temp_table_alias, const std::vector<int>* aliases) {return vc->IsConstExpression(expr, temp_table_alias, aliases);}
	int GetDim() {return vc->GetDim();}

	virtual bool IsConst() const { return vc->IsConst(); }
	virtual bool RoughNullsOnly(const MIIterator &mit) { return vc->RoughNullsOnly(mit); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &m) { return vc->GetNotNullValueString(s, m); }
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit)				{ return vc->GetNotNullValueInt64(mit); }
	virtual bool IsDistinctInTable()				{ return vc->IsDistinctInTable(); }	// sometimes overriden in subclasses
	bool IsDeterministic() { return vc->IsDeterministic();}
	bool IsParameterized() { return vc->IsParameterized(); }
	vector<VarMap>& GetVarMap() { return vc->GetVarMap(); }
	virtual bool IsTypeCastColumn() const { return true; }
	const MysqlExpression::bhfields_cache_t& GetBHItems() {return vc->GetBHItems();}

	bool CanCopy() const {return vc->CanCopy();}
	bool IsThreadSafe() {return vc->IsThreadSafe();}


protected:
	_int64 DoGetValueInt64(const MIIterator& mit) { return vc->GetValueInt64(mit); }
	bool DoIsNull(const MIIterator& mit) { return vc->IsNull(mit); }
	void DoGetValueString(RCBString& s, const MIIterator& m) { return vc->GetValueString(s, m); }
	double DoGetValueDouble(const MIIterator& m) { return vc->GetValueDouble(m); }
	RCValueObject DoGetValue(const MIIterator& m, bool b) { return vc->GetValue(m,b); }
	_int64 DoGetMinInt64(const MIIterator& m) { return MINUS_INF_64; }
	_int64 DoGetMaxInt64(const MIIterator& m) { return PLUS_INF_64; }
	RCBString DoGetMinString(const MIIterator &m) { return RCBString(); }
	RCBString DoGetMaxString(const MIIterator &m) { return RCBString(); }
	_int64 DoRoughMin() { return MINUS_INF_64 ; }
	_int64 DoRoughMax() { return PLUS_INF_64; }
	_int64 DoGetNoNulls(const MIIterator& m, bool val_nulls_possible) { return vc->GetNoNulls(m); }
	bool DoNullsPossible(bool val_nulls_possible) { return vc->NullsPossible(); }
	_int64 DoGetSum(const MIIterator& m, bool &nonnegative) { return vc->GetSum(m, nonnegative); }
	bool DoIsDistinct() { return vc->IsDistinct(); }
	_int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind) { return vc->GetApproxDistVals(incl_nulls, rough_mind); }
	ushort DoMaxStringSize() { return vc->MaxStringSize(); }		// maximal byte string length in column
	PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& m) { return vc->GetPackOntologicalStatus(m); }
	RSValue DoRoughCheck(const MIIterator& m, Descriptor&d ) { return vc->RoughCheck(m,d); }
	RSValue DoRoughPackIndexCheck(const MIIterator& m, Descriptor&d ,std::vector<int> &packs) { return vc->RoughPackIndexCheck(m,d,packs); }
	void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& d) { return vc->EvaluatePack(mit,d); }
	const MysqlExpression::bhfields_cache_t& GetBHItems() const { return vc->GetBHItems(); }

	bool full_const;
	VirtualColumn* vc;
};

//////////////////////////////////////////////////////
class String2IntCastColumn : public TypeCastColumn
{
public:
	String2IntCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool = true);
	_int64 DoGetMinInt64(const MIIterator& m);
	_int64 DoGetMaxInt64(const MIIterator& m);
	virtual bool IsDistinctInTable()				{ return false; }		// cast may make distinct strings equal

private:
	mutable _int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class String2DateTimeCastColumn : public TypeCastColumn
{
public:
	String2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool = true);
	_int64 DoGetMinInt64(const MIIterator& m);
	_int64 DoGetMaxInt64(const MIIterator& m);
	virtual bool IsDistinctInTable()				{ return false; }		// cast may make distinct strings equal

private:
	_int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Int2DateTimeCastColumn : public String2DateTimeCastColumn
{
public:
	Int2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to);
	RCValueObject DoGetValue(const MIIterator&, bool = true);

private:
	_int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class DateTime2VarcharCastColumn : public TypeCastColumn
{
public:
	DateTime2VarcharCastColumn(VirtualColumn* from, ColumnType const& to);
	RCValueObject DoGetValue(const MIIterator&, bool = true);

private:
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Num2VarcharCastColumn : public TypeCastColumn
{
public:
	Num2VarcharCastColumn(VirtualColumn* from, ColumnType const& to);
	void GetNotNullValueString(RCBString& s, const MIIterator &m) { DoGetValueString(s, m); }
protected:
	RCValueObject DoGetValue(const MIIterator&, bool = true);
	void DoGetValueString(RCBString& s, const MIIterator& m);
private:
	mutable RCValueObject rcv;
};


//////////////////////////////////////////////////////
class DateTime2IntCastColumn : public TypeCastColumn
{
public:
	DateTime2IntCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool = true);

private:
	mutable _int64 val;
	mutable RCValueObject rcv;
};

#endif /* TYPECASTCOLUMN_H_ */
