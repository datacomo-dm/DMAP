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

#include "GroupTable.h"
#include "GroupDistinctTable.h"
#include "MIIterator.h"
#include "system/fet.h"

GroupTable::GroupTable()
{
	initialized = false;
	occupied = NULL;
	occupied_table = NULL;
	no_of_occupied = 0;
	input_buffer = NULL;
	column_offset = NULL;
	t = NULL;
	grouping_buf_width = 0;
	grouping_and_UTF_width = 0;
	no_hash_needed = false;
	total_width = 0;
	operation = NULL;
	distinct = NULL;
	max_total_size = 0;

	aggregator = NULL;

	no_attr = 0;
	no_grouping_attr = 0;

	gdistinct = NULL;
	encoder = NULL;
	vc = NULL;
	is_full = false;
	search_depth = 0;
	declared_max_no_groups = 0;
	distinct_present = false;
	no_rows = 0;
	rows_limit = 0;
}

GroupTable::~GroupTable()
{
	delete occupied;
	delete [] occupied_table;
	dealloc(t);
	delete [] input_buffer;
	delete [] column_offset;
	delete [] distinct;

	delete [] operation;

	if(aggregator) {
		for(int i=0; i<no_attr; i++) {
			if(aggregator[i])
				delete aggregator[i];
		}
	}
	delete [] aggregator;

	for(int i = 0; i < no_attr; i++) {
		if(gdistinct[i])
			delete gdistinct[i];
		delete encoder[i];
	}
	delete [] encoder;
	delete [] gdistinct;
	delete [] vc;
}

GroupTable::GroupTable(GroupTable &sec)
{
	assert(sec.initialized);	// can only copy initialized GroupTables!
	////// Some fields are ommitted (empty vectors), as they are used only for Initialize()
	initialized = true;

	no_of_occupied = sec.no_of_occupied;
	grouping_buf_width = sec.grouping_buf_width;
	grouping_and_UTF_width = sec.grouping_and_UTF_width;
	no_hash_needed = sec.no_hash_needed;
	total_width = sec.total_width;
	no_attr = sec.no_attr;
	no_grouping_attr = sec.no_grouping_attr;
	is_full = sec.is_full;
	distinct_present = sec.distinct_present;	
	no_rows = sec.no_rows;
	rows_limit = sec.rows_limit;
	search_depth = sec.search_depth;
	declared_max_no_groups = sec.declared_max_no_groups;
	max_total_size = sec.max_total_size;

	if(sec.occupied)
		occupied = new Filter(*sec.occupied);
	else
		occupied = NULL;

	if(sec.occupied_table) {
		occupied_table = new int [no_rows];
		memcpy(occupied_table, sec.occupied_table, no_rows * sizeof(int));
	} else 
		occupied_table = NULL;

	if(sec.input_buffer) {
		input_buffer = new unsigned char [grouping_and_UTF_width];
		memcpy(input_buffer, sec.input_buffer, grouping_and_UTF_width);		// copy buffer, as it may already have some constants
	} else
		input_buffer = NULL;

	operation		= new GT_Aggregation [no_attr];
	distinct		= new bool [no_attr];
	column_offset 	= new int [no_attr];
	gdistinct		= new GroupDistinctTable * [no_attr];
	if(sec.aggregator)
		aggregator		= new Aggregator * [no_attr];
	else
		aggregator = NULL;
	encoder			= new ColumnBinEncoder * [no_attr];
	vc				= new VirtualColumn * [no_attr];

	for(int i = 0; i < no_attr; i++) {
		operation[i] = sec.operation[i];
		distinct[i] = sec.distinct[i];
		column_offset[i] = sec.column_offset[i];
		vc[i] = sec.vc[i];

		assert(sec.gdistinct[i] == NULL);		// cannot copy distinct! (I.e. there is no point to do it.)
		gdistinct[i] = NULL;

		if(aggregator) {
			if(sec.aggregator[i])
				aggregator[i] = sec.aggregator[i]->Copy();			// get a new object of appropriate subclass
			else
				aggregator[i] = NULL;
		}
		if(sec.encoder[i])
			encoder[i] = new ColumnBinEncoder(*sec.encoder[i]);
		else
			encoder[i] = NULL;
	}

	t = (unsigned char*)alloc(total_width * no_rows, BLOCK_TEMPORARY);
	memcpy(t, sec.t, total_width * no_rows);
}

//////////////////////////////////////////////////////////////////////////////////////

void GroupTable::AddGroupingColumn(VirtualColumn *vc)
{
	GroupTable::ColTempDesc desc;
	desc.vc = vc;
	grouping_desc.push_back(desc);
}

void GroupTable::AddAggregatedColumn( VirtualColumn *vc, GT_Aggregation operation, bool distinct,
									  AttributeType type, int size, int precision, DTCollation in_collation)
{
	GroupTable::ColTempDesc desc;
	desc.type = type;						// put defaults first, then check in Initialize() what will be actual result definition
	// Overwriting size in some cases:
	switch(type) {
		case RC_INT:
		case RC_MEDIUMINT:
		case RC_BYTEINT:
		case RC_SMALLINT:
			size = 4;
			break;				// minimal field is one int (4 bytes)
		case RC_STRING:
		case RC_VARCHAR:
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			// left as is
			break;
		// Note: lookup strings will have type RC_STRING or similar, and size 4 (stored as int!)
		default:
			size = 8;
	}
	desc.vc = vc;
	desc.size = size;
	desc.precision = precision;
	desc.operation = operation;
	desc.distinct = distinct;
	desc.collation = in_collation;
	aggregated_desc.push_back(desc);
}

void GroupTable::Initialize(_int64 max_no_groups)
{
	MEASURE_FET("GroupTable::Initialize(...)");
	declared_max_no_groups = max_no_groups;
	if(initialized)
		return;
	no_grouping_attr = int(grouping_desc.size());
	no_attr = no_grouping_attr + int(aggregated_desc.size());
	if(no_attr==0)
		return;

	operation		= new GT_Aggregation [no_attr];
	distinct		= new bool [no_attr];
	column_offset 	= new int [no_attr];
	gdistinct		= new GroupDistinctTable * [no_attr];
	aggregator		= new Aggregator * [no_attr];
	encoder			= new ColumnBinEncoder * [no_attr];
	vc				= new VirtualColumn * [no_attr];

	// rewrite column descriptions (defaults, to be verified)
	int no_columns_with_distinct = 0;
	GroupTable::ColTempDesc desc;
	for(int i = 0; i < no_attr; i++) {
		if(i < no_grouping_attr) {
			desc = grouping_desc[i];
			vc[i] = desc.vc;
			aggregator[i] = NULL;
			encoder[i] = new ColumnBinEncoder(ColumnBinEncoder::ENCODER_DECODABLE);	// TODO: not always decodable? (hidden cols)
			encoder[i]->PrepareEncoder(desc.vc);
		} else {
			encoder[i] = NULL;
			desc = aggregated_desc[i - no_grouping_attr];
			vc[i] = desc.vc;
			//////////////////////// Aggregations: /////////////////////////////////

				    // COUNT(...)
			if(desc.operation == GT_COUNT || desc.operation == GT_COUNT_NOT_NULL) {
				if(desc.max_no_values > 0x7FFFFFFF)
					aggregator[i] = new AggregatorCount64(desc.max_no_values);
				else
					aggregator[i] = new AggregatorCount32(int(desc.max_no_values));

			} else	// SUM(...)				Note: strings are parsed to double
				if(desc.operation == GT_SUM) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorSumD;
					else
						aggregator[i] = new AggregatorSum64;

			} else	// AVG(...)				Note: strings are parsed to double
				if(desc.operation == GT_AVG) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorAvgD;
					else if(desc.type == RC_YEAR)
						aggregator[i] = new AggregatorAvgYear;
					else
						aggregator[i] = new AggregatorAvg64(desc.precision);

			} else // MIN(...)
				if(desc.operation == GT_MIN) {
					if(ATI::IsStringType(desc.type)) {
						if(RequiresUTFConversions(desc.collation))
							aggregator[i] = new AggregatorMinT_UTF(desc.size, desc.collation);
						else
							aggregator[i] = new AggregatorMinT(desc.size);
					} else if(ATI::IsRealType(desc.type))
						aggregator[i] = new AggregatorMinD;
					else if(desc.min < -(0x7FFFFFFF) || desc.max > 0x7FFFFFFF)
						aggregator[i] = new AggregatorMin64;
					else
						aggregator[i] = new AggregatorMin32;

			} else // MAX(...)
				if(desc.operation == GT_MAX) {
					if(ATI::IsStringType(desc.type)) {
						if(RequiresUTFConversions(desc.collation))
							aggregator[i] = new AggregatorMaxT_UTF(desc.size, desc.collation);
						else
							aggregator[i] = new AggregatorMaxT(desc.size);
					} else if(ATI::IsRealType(desc.type))
						aggregator[i] = new AggregatorMaxD;
					else if(desc.min < -(0x7FFFFFFF) || desc.max > 0x7FFFFFFF)
						aggregator[i] = new AggregatorMax64;
					else
						aggregator[i] = new AggregatorMax32;
			} else	// LIST - just a first value found
				if(desc.operation == GT_LIST) {
					if(ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorListT(desc.size);
					else if(ATI::IsRealType(desc.type) || (desc.min < -(0x7FFFFFFF) || desc.max > 0x7FFFFFFF))
						aggregator[i] = new AggregatorList64;
					else
						aggregator[i] = new AggregatorList32;

			} else	// VAR_POP(...)
				if(desc.operation == GT_VAR_POP) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorVarPopD;
					else
						aggregator[i] = new AggregatorVarPop64(desc.precision);

			} else	// VAR_SAMP(...)
				if(desc.operation == GT_VAR_SAMP) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorVarSampD;
					else
						aggregator[i] = new AggregatorVarSamp64(desc.precision);

			} else	// STD_POP(...)
				if(desc.operation == GT_STD_POP) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorStdPopD;
					else
						aggregator[i] = new AggregatorStdPop64(desc.precision);

			} else	// STD_SAMP(...)
				if(desc.operation == GT_STD_SAMP) {
					if(ATI::IsRealType(desc.type) || ATI::IsStringType(desc.type))
						aggregator[i] = new AggregatorStdSampD;
					else
						aggregator[i] = new AggregatorStdSamp64(desc.precision);

			} else	// BIT_AND(...)
				if(desc.operation == GT_BIT_AND) {
					aggregator[i] = new AggregatorBitAnd;

			} else	// BIT_Or(...)
				if(desc.operation == GT_BIT_OR) {
					aggregator[i] = new AggregatorBitOr;

			} else	// BIT_XOR(...)
				if(desc.operation == GT_BIT_XOR) {
					aggregator[i] = new AggregatorBitXor;

			}

			if(aggregator[i]->IgnoreDistinct())
				desc.distinct = false;
		}
		operation[i]	= desc.operation;
		distinct[i]		= desc.distinct;
		gdistinct[i]	= NULL;					// for now...
		if(distinct[i])
			no_columns_with_distinct++;
	}
	if(no_columns_with_distinct > 0)
		distinct_present = true;
	// calculate column byte sizes
	grouping_buf_width = 0;
	grouping_and_UTF_width = 0;
	total_width = 0;
	for(int i = 0; i < no_grouping_attr; i++) {
		encoder[i]->SetPrimaryOffset(grouping_buf_width);
		grouping_buf_width += encoder[i]->GetPrimarySize();
	}
	grouping_and_UTF_width = grouping_buf_width;

	// UTF originals, if any
	for(int i = 0; i < no_grouping_attr; i++) {
		if(encoder[i]->GetSecondarySize() > 0) {
			encoder[i]->SetSecondaryOffset(grouping_and_UTF_width);
			grouping_and_UTF_width += encoder[i]->GetSecondarySize();
		}
	}
	total_width = grouping_and_UTF_width;

	// Aggregators
	for(int i = no_grouping_attr; i < no_attr; i++)	{
		// round up to 4-byte alignment (for numerical counters)
		total_width = 4 * ( (total_width + 3) / 4);				// e.g. 1->4, 12->12, 19->20
		column_offset[i] = total_width;
		total_width += aggregator[i]->BufferByteSize();
	}
	total_width = 4 * ( (total_width + 3) / 4);

	/////////// create buffers ////////////////////
	// Memory settings
	if(total_width < 1)
		total_width = 1;		// rare case: all constants, no actual buffer needed (but we create one anyway, just to avoid a special execution path for this boundary case)

	max_total_size = 64 * MBYTE;
	if(no_columns_with_distinct > 0 || max_no_groups * total_width > max_total_size)
		max_total_size = TrackableObject::MaxBufferSize(no_columns_with_distinct > 0 ? 1 : 0);	
		// Check memory only for larger groupings. More aggressive memory settings for distinct.

	// calculate sizes
	_int64 primary_total_size = max_total_size;	// size of grouping table, not distinct;
	if(no_columns_with_distinct > 0) {
		// naive algorithm: a bit more memory for every distinct than for grouping itself
		primary_total_size = max_total_size/(no_columns_with_distinct + 1);
	}

	// grouping table sizes
	no_rows = primary_total_size / total_width;		// memory limitation
	double overspace = 1.3;
	search_depth = 9;								// default search depth
	if(max_no_groups < no_rows / 100) {	// for small groupings we may assume more space and infinite search to prevent hash conflicts
		overspace = 3;
		search_depth = no_rows;			// assure one-pass
	} else if(max_no_groups < no_rows / 16) {
		overspace = 2;
		search_depth = no_rows;			// assure one-pass
	}	// for small groupings (buffer in memory) we may assume more space and deeper search to prevent hash conflicts
	if((max_no_groups + 1) * overspace < no_rows)
		no_rows = _int64((max_no_groups + 1) * overspace);	// make sure that rows_limit>=max_no_groups

	if(max_no_groups == 1) {		// special case: no grouping, just aggregations
		no_rows = 1;
		rows_limit = no_rows;
		no_hash_needed = true;
	} else if(grouping_buf_width == 1 && encoder[0]->MaxCode() > 0 && encoder[0]->MaxCode() < max(67LL, no_rows)) {
		no_rows = encoder[0]->MaxCode() + 1;	// easy case: the row number is just the one-byte coded value
		rows_limit = no_rows;
		no_hash_needed = true;
	} else if(grouping_buf_width == 2 && no_rows > 40000) {
		no_rows = 65536;						// easy case: the row number is just the two-byte coded value
		rows_limit = no_rows;
		no_hash_needed = true;
	} else {
		// calculate vertical size (not dividable by 17)
		if( no_rows < 67 )								// too less groups => high collision probability; 67 is prime.
			no_rows = 67;
		if( no_rows % 17 == 0 )
			no_rows++;
		rows_limit = _int64(no_rows * 0.9);		// rows_limit is used to determine whether the table is full
	}
	if(no_hash_needed)
		search_depth = no_rows;			// assure one-pass

	t = (unsigned char*)alloc(total_width * no_rows, BLOCK_TEMPORARY);
	if(grouping_and_UTF_width > 0)
		input_buffer = new unsigned char [grouping_and_UTF_width];

	// initialize distinct tables
	_int64 total_size_left = max_total_size - total_width * no_rows;
	for(int i = no_grouping_attr; i < no_attr; i++)
		if(distinct[i]) {
			desc = aggregated_desc[i - no_grouping_attr];
			assert(total_size_left>0);
			gdistinct[i] = new GroupDistinctTable();
			gdistinct[i]->InitializeVC(	no_rows, 
										vc[i], 
										desc.max_no_values * no_rows, 
										total_size_left/no_columns_with_distinct, 
										(operation[i] != GT_COUNT_NOT_NULL));	// otherwise must be decodable
			total_size_left -= gdistinct[i]->BytesTaken();
			no_columns_with_distinct--;
	}

	// initialize everything
	std::map<int,int> nfblocks;
	occupied = new Filter(no_rows,nfblocks);
	occupied_table = new int [no_rows];

	ConnectionInfo *m_conn = &ConnectionInfoOnTLS.Get();
	double size_mb = (double)total_width * no_rows;
	size_mb = (size_mb > 1000 ? (size_mb > 10000000 ? _int64(size_mb / 1000000) : _int64(size_mb / 1000) / 1000.0) : 0.001);
	rccontrol.lock(m_conn->GetThreadID()) << "GroupTable initialized for up to " << max_no_groups << " groups, " 
										  << grouping_buf_width << "+" << total_width - grouping_buf_width << " bytes (" 
										  << size_mb << " MB)" << unlock;
	ClearAll();
	initialized = true;
}

//////////////////////////////////////////////////////////////////////////////////////////

bool GroupTable::FindCurrentRow(_int64 &row)	// a position in the current GroupTable, row==NULL_VALUE_64 if not found and not added
{												// return value: true if already existing, false if put as a new row
	//MEASURE_FET("GroupTable::FindCurrentRow(...)");
	_int64 step = 1;
	int local_no_of_checks = 0;
	if(no_hash_needed)	{	// special case: no grouping columns
		row = 0;
		if(no_rows > 1)
			memcpy(&row, input_buffer, grouping_buf_width);
	} else {
		unsigned int crc_code = HashValue(input_buffer, grouping_buf_width);
		row = crc_code % no_rows;
		step = 3 + crc_code % 8;
	}
	do {
		if(occupied->Get(row)) {
			if(no_hash_needed ||
				memcmp( t + row * total_width, input_buffer, grouping_buf_width) == 0) {
				// i.e. identical row found
				return true;
			}
			local_no_of_checks++;
			row += step + local_no_of_checks;
			if(row >= no_rows)
				row = row % no_rows;
		} else {
			if(is_full) {
				row = NULL_VALUE_64;			// when is_full is set, do not add new tuples
				return false;
			}
			if(grouping_buf_width > 0) {
				memcpy(t + row * total_width, input_buffer, grouping_and_UTF_width);
				for(int col = 0; col < no_grouping_attr; col++)
					encoder[col]->UpdateStatistics(input_buffer);
			}
			occupied->Set(row);
			occupied_table[no_of_occupied] = (int)row;
			no_of_occupied++;
			if(no_of_occupied >= rows_limit || no_of_occupied == declared_max_no_groups)
				is_full = true;
			return false;
		}
	} while(local_no_of_checks < search_depth);
	row = NULL_VALUE_64;			// no place found in reasonable depth!
	return false;
}

//////////////////////////////////////////////////////////////////////////////////////////

void GroupTable::Merge(GroupTable &sec)
{
	assert(total_width == sec.total_width);
	for(int i = 0; i < sec.no_of_occupied; i++) {
		_int64 row = sec.occupied_table[i];
		unsigned char *t_sec = sec.t + row * total_width;
		bool row_occupied = occupied->Get(row);
		if(no_hash_needed) {
			if(!row_occupied) {			// add as new, otherwise already found
				if(grouping_buf_width > 0) {
					memcpy(t + row * total_width, t_sec, grouping_and_UTF_width);
					for(int col = 0; col < no_grouping_attr; col++)
						encoder[col]->UpdateStatistics(t_sec);
				}
				occupied->Set(row);
				occupied_table[no_of_occupied] = (int)row;
				no_of_occupied++;
				if(no_of_occupied >= rows_limit || no_of_occupied == declared_max_no_groups)
					is_full = true;
			}
		} else if(!row_occupied || memcmp(t + row * total_width, t_sec, grouping_buf_width) != 0) {
			// value not present yet or other value found in the same row
			// note that the same value may be somewhere else in hash table
			memcpy(input_buffer, t_sec, grouping_and_UTF_width);
			FindCurrentRow(row);		// find the value on another position or add as a new one
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(row != NULL_VALUE_64);
		}
		for(int col = no_grouping_attr; col < no_attr; col++) {
			unsigned char* p1 = t + row * total_width + column_offset[col];
			unsigned char* p2 = t_sec + column_offset[col];
			aggregator[col]->Merge(p1, p2);
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////

_int64 GroupTable::GetValue64(int col, _int64 row)
{
	unsigned char* p = t + row * total_width;
	if(col >= no_grouping_attr) {
		return aggregator[col]->GetValue64(p + column_offset[col]);
	}
	// Grouping column
	MIDummyIterator mit(1);
	bool is_null;
	return encoder[col]->GetValue64(p, mit, is_null);
}

RCBString GroupTable::GetValueT(int col, _int64 row)
{
	unsigned char* p = t + row * total_width;
	if(col >= no_grouping_attr) {
		return aggregator[col]->GetValueT(p + column_offset[col]);
	}
	// Grouping column
	MIDummyIterator mit(1);
	return encoder[col]->GetValueT(p, mit);
}


void GroupTable::ClearAll()
{
	is_full = false;
	memset( t, 0, total_width * no_rows );
	if(grouping_and_UTF_width > 0)
		memset(input_buffer, 0, grouping_and_UTF_width);

	// initialize statistics
	for(int i = 0; i < no_grouping_attr; i++)
		encoder[i]->ClearStatistics();

	// initialize aggregated values
	for(int i = no_grouping_attr; i < no_attr; i++) {
		aggregator[i]->ResetStatistics();
		for(_int64 row = 0; row < no_rows; row++)
			aggregator[i]->Reset(t + row * total_width + column_offset[i]);
		if(gdistinct[i])
			gdistinct[i]->Clear();
	}
	occupied->Reset();
	no_of_occupied = 0;
}

void GroupTable::ClearUsed()
{
	is_full = false;
	if(grouping_and_UTF_width > 0)
		memset(input_buffer, 0, grouping_and_UTF_width);

	FilterOnesIterator it(occupied);
	it.Rewind();
	while(it.IsValid()) {
		if(grouping_and_UTF_width > 0)
			memset(t + *it * total_width, 0, grouping_and_UTF_width);
		for(int i = no_grouping_attr; i < no_attr; i++)
			aggregator[i]->Reset(t + *it * total_width + column_offset[i]);
		++it;
	}
	occupied->Reset();
	no_of_occupied = 0;

	// initialize statistics

	for(int i = 0; i < no_grouping_attr; i++)
		encoder[i]->ClearStatistics();

	for(int i = no_grouping_attr; i < no_attr; i++) {
		aggregator[i]->ResetStatistics();
		if(gdistinct[i])
			gdistinct[i]->Clear();
	}
}


void GroupTable::ClearDistinct()							// reset the table for distinct values
{
	for(int i = no_grouping_attr; i < no_attr; i++) {
		aggregator[i]->ResetStatistics();
		if(gdistinct[i])
			gdistinct[i]->Clear();
	}
}

//////////////////////////////////////////////////////////////////////////////////////////
void GroupTable::AddCurrentValueToCache(int col, GroupDistinctCache &cache)
{
	cache.SetCurrentValue(gdistinct[col]->InputBuffer(), gdistinct[col]->InputBufferSize());
}

///////////////
bool GroupTable::AggregatePack(int col, _int64 row)	// aggregate based on parameters stored in the aggregator
{
	unsigned char* p = t + row * total_width + column_offset[col];
	return 	aggregator[col]->AggregatePack(p);
}

bool GroupTable::PutAggregatedValue(int col, _int64 row, _int64 factor)			// for agregations which do not need any value
{
	if(factor == NULL_VALUE_64 && aggregator[col]->FactorNeeded())
		throw NotImplementedRCException("Aggregation overflow.");
	unsigned char* p = t + row * total_width + column_offset[col];
	aggregator[col]->PutAggregatedValue(p, factor);
	return true;
}

GDTResult GroupTable::FindDistinctValue(int col, _int64 row, _int64 v)	// for all numerical values
{
	assert(gdistinct[col]);
	if(v == NULL_VALUE_64)		// works also for double
		return GDT_EXISTS;		// null omitted
	return gdistinct[col]->Find(row, v);
}

GDTResult GroupTable::AddDistinctValue(int col, _int64 row, _int64 v)	// for all numerical values
{
	assert(gdistinct[col]);
	if(v == NULL_VALUE_64)		// works also for double
		return GDT_EXISTS;			// null omitted
	return gdistinct[col]->Add(row, v);
}

bool GroupTable::PutAggregatedValue(int col, _int64 row, MIIterator &mit, _int64 factor, bool as_string)
{
	if(distinct[col]) {
		// Repetition? Return without action.
		assert(gdistinct[col]);
		if(vc[col]->IsNull(mit))
			return true;			// omit nulls
		GDTResult res = gdistinct[col]->Add(row, mit);
		if(res == GDT_EXISTS)
			return true;	// value found, do not aggregate it again
		if(res == GDT_FULL) {
			if(gdistinct[col]->AlreadyFull())
				is_full = true;	// disable also the main grouping table (if it is a persistent rejection)
			return false;	// value not found in DISTINCT buffer, which is already full
		}
		factor = 1;			// ignore repetitions for distinct
	}
	Aggregator* cur_aggr = aggregator[col];
	if(factor == NULL_VALUE_64 && cur_aggr->FactorNeeded())
		throw NotImplementedRCException("Aggregation overflow.");
	if(as_string) {
		RCBString v;
		vc[col]->GetValueString(v, mit);
		if(v.IsNull() && cur_aggr->IgnoreNulls())
			return true;			// null omitted
		cur_aggr->PutAggregatedValue(t + row * total_width + column_offset[col], v, factor);
	} else {
		// note: it is too costly to check nulls separately (e.g. for complex expressions)
		_int64 v = vc[col]->GetValueInt64(mit);
		if(v == NULL_VALUE_64 && cur_aggr->IgnoreNulls())
			return true;
		cur_aggr->PutAggregatedValue(t + row * total_width + column_offset[col], v, factor);
	}
	return true;
}

bool GroupTable::PutAggregatedNull(int col, _int64 row, bool as_string)
{
	if(distinct[col])
		return true;			// null omitted
	if(aggregator[col]->IgnoreNulls())
		return true;			// null omitted
	unsigned char* p = t + row * total_width + column_offset[col];
	if(as_string) {
		RCBString v;
		aggregator[col]->PutAggregatedValue(p, v, 1);
	} else
		aggregator[col]->PutAggregatedValue(p, NULL_VALUE_64, 1);
	return true;
}

bool GroupTable::PutCachedValue(int col, GroupDistinctCache &cache, bool as_text)	// for all numerical values
{
	assert(distinct[col]);
	GDTResult res = gdistinct[col]->AddFromCache(cache.GetCurrentValue());
	if(res == GDT_EXISTS)
		return true;	// value found, do not aggregate it again
	if(res == GDT_FULL) {
		if(gdistinct[col]->AlreadyFull())
			is_full = true;	// disable also the main grouping table (if it is a persistent rejection)
		return false;	// value not found in DISTINCT buffer, which is already full
	}
	_int64 row = gdistinct[col]->GroupNoFromInput();	// restore group number
	unsigned char* p = t + row * total_width + column_offset[col];
	if(operation[col] == GT_COUNT_NOT_NULL)
		aggregator[col]->PutAggregatedValue(p, 1);			// factor = 1, because we are just after distinct
	else {
		if(as_text) {
			RCBString v;
			gdistinct[col]->ValueFromInput(v);
			aggregator[col]->PutAggregatedValue(p, v, 1);
		} else {
			_int64 v = gdistinct[col]->ValueFromInput();
			aggregator[col]->PutAggregatedValue(p, v, 1);
		}
	}
	return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

void GroupTable::UpdateAttrStats(int col)	// update the current statistics for a column, if needed
{
	MEASURE_FET("GroupTable::UpdateAttrStats(...)");
	if(!aggregator[col]->StatisticsNeedsUpdate())
		return;
	FilterOnesIterator it(occupied);
	bool stop_updating = false;
	aggregator[col]->ResetStatistics();
	while(!stop_updating && it.IsValid()) {
		stop_updating = aggregator[col]->UpdateStatistics(t + *it * total_width + column_offset[col]);
		++it;
	}
	aggregator[col]->SetStatisticsUpdated();
}

bool GroupTable::AttrMayBeUpdatedByPack(int col, MIIterator &mit)	// get the current statistics for a column
{
	if(vc[col]->Type().IsFloat() || vc[col]->Type().IsString())
		return true;	// min/max not calculated properly for these types, see Mojo 782681
	_int64 local_min = vc[col]->GetMinInt64(mit);
	_int64 local_max = vc[col]->GetMaxInt64(mit);
	if(encoder[col]->ImpossibleValues(local_min, local_max))
		return false;
	return true;
}

void GroupTable::InvalidateAggregationStatistics()
{
	for(int i = no_grouping_attr; i < no_attr; i++) {
		aggregator[i]->ResetStatistics();
	}
}
