/* Copyright (C)  2005-2009 Infobright Inc.

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

#ifndef _GROUPTABLE_H_
#define _GROUPTABLE_H_

#include "bintools.h"
#include "Filter.h"
#include "GroupDistinctTable.h"
#include "GroupDistinctCache.h"
#include "AggregatorBasic.h"
#include "AggregatorAdvanced.h"
#include "ColumnBinEncoder.h"


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GroupTable - a tool for storing values and counters, implementing hash table mechanism
//
//	Usage:
//
//
//
////////////////////////////////////////////////////////////////////////////////////////////////////////
enum GT_Aggregation {	GT_LIST,						// GT_LIST - just store the first value
						GT_COUNT, GT_COUNT_NOT_NULL,
						GT_SUM, GT_MIN, GT_MAX, GT_AVG,
						GT_STD_POP, GT_STD_SAMP, GT_VAR_POP, GT_VAR_SAMP,
						GT_BIT_AND, GT_BIT_OR, GT_BIT_XOR,
						GT_GROUP_CONCAT};

class GroupTable : public TrackableObject
{
public:
	GroupTable();
	GroupTable(GroupTable &sec);
	~GroupTable();

	////////// Group table construction ///////////

	void AddGroupingColumn(VirtualColumn *vc);
	void AddAggregatedColumn(VirtualColumn *vc, GT_Aggregation operation, bool distinct,
							 AttributeType type, int b_size, int precision, DTCollation in_collation);
	void AggregatedColumnStatistics( int ag_col, _int64 max_no_vals, _int64 min_v = MINUS_INF_64, _int64 max_v = PLUS_INF_64 )
	{
		aggregated_desc[ag_col].max_no_values	= max_no_vals;
		aggregated_desc[ag_col].min				= min_v;
		aggregated_desc[ag_col].max				= max_v;
	}

	void Initialize(_int64 max_no_groups);			// initialize buffers basing on the previously defined grouping/aggregated columns
																// and the best possible upper approximation of number of groups;
																// note: max_no_groups may also be used for "select ... limit n", because we will not produce more groups

	///////// Group table usage //////////////////
	// put values to a temporary buffer (note that it will contain the previous values, which may be reused
	void PutGroupingValue(int col, MIIterator &mit)
	{
		// Encoder statistics are not updated here
		encoder[col]->Encode(input_buffer, mit);
	}

	bool FindCurrentRow(_int64 &row);					// a position in the current GroupTable, row==NULL_VALUE_64 if not found
														// return value: true if already existing, false if put as a new row
	GDTResult FindDistinctValue(int col, _int64 row, _int64 v);	// just check whether exists, do not add
	GDTResult AddDistinctValue(int col, _int64 row, _int64 v);	// just add to a list of distincts, not to aggregators

	// columns have common numbering, both grouping and aggregated ones
	// return value: true if value checked in, false if not (possible only on DISTINCT buffer overflow)

	bool PutAggregatedValue(int col, _int64 row, _int64 factor);			// for aggregations which do not need any value
	bool PutAggregatedValue(int col, _int64 row, MIIterator &mit, _int64 factor, bool as_string);
	bool PutAggregatedNull(int col, _int64 row, bool as_string);
	bool PutCachedValue(int col, GroupDistinctCache &cache, bool as_text);	// mainly for numerics, and only some aggregators

	bool AggregatePack(int col, _int64 row);	// aggregate based on parameters stored in the aggregator

	void AddCurrentValueToCache(int col, GroupDistinctCache &cache);
	void Merge(GroupTable &sec);				// merge values from another (compatible) GroupTable
	///////// Group table output and info //////////////////

	int NoAttr()				{ return no_attr; }				// total number of columns
	int NoGroupingAttr()		{ return no_grouping_attr; }	// number of grouping columns
	bool IsFull()				{ return is_full; }				// no place left?
	bool IsDistinctFull(int col) { return (distinct[col] ? gdistinct[col]->AlreadyFull() : false); }				// no place left in distinct buffer?
	bool IsOnePass()			{ return !distinct_present && (search_depth >= no_rows); }	// assured one-pass scan

	int GetOccupiedRow(int n)		{ assert(n < no_of_occupied); return occupied_table[n]; }
	int GetNoOfOccupiedRows()		{ return no_of_occupied; }

	_int64 GetValue64(int col, _int64 row);			// columns have common numbering
	RCBString GetValueT(int col, _int64 row);

	void ClearAll();								// initialize all
	void ClearUsed();								// initialize only used rows (for next pass)
	void ClearDistinct();							// reset the tables of distinct values, if present

	void UpdateAttrStats(int col);							// update the current statistics for a column, if needed
	bool AttrMayBeUpdatedByPack(int col, MIIterator &mit);	// false if a grouping attribute pack is out of scope of the current contents
	void InvalidateAggregationStatistics();					// force recalculate rough statistics for aggregators

	GT_Aggregation AttrOper(int col)	{ return operation[col]; }
	Aggregator* AttrAggregator(int col)	{ return aggregator[col]; }
	bool AttrDistinct(int col)			{ return distinct[col];  }
	_int64 UpperApproxOfGroups()		{ return declared_max_no_groups; }
	int MaxNoAggregators()				{ int res = (int)((max_total_size * 1.2) / (total_width * no_rows)); return (res > 1 ? res : 1); }	// allow 20% more for better parallelism

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

private:

	Filter*				occupied;
	int 				no_of_occupied;
	int*				occupied_table;

	// content and additional buffers

	unsigned char*		input_buffer;
	int*				column_offset;		// a table of offsets (bytes) of column beginnings wrt. the beginning of a row

	unsigned char*		t;					// common buffer for grouping values and results of aggregations
	int 				grouping_buf_width;	// in bytes
	int 				grouping_and_UTF_width;	// in bytes, total buffer size for grouping and UTF originals
	int 				total_width;		// in bytes
	bool				no_hash_needed;		// true, if the actual grouping value may be used instead of CRC (limited interval of values)

	_int64 				no_rows;			// actual buffer size (vertical)
	_int64 				rows_limit;			// a capacity of one memory buffer
	_int64				search_depth;		// how many steps are we doing before giving up
	_int64				declared_max_no_groups;	// maximal number of groups calculated from KNs etc.

	// column/operation descriptions

	bool 				initialized;
	struct ColTempDesc					// temporary description - prior to buffers initialization, then sometimes used for complex aggregations
	{
		ColTempDesc()					// defaults:
		{ vc = NULL; min = MINUS_INF_64; max = PLUS_INF_64; max_no_values = 0; operation = GT_LIST; distinct = false; size = 0; precision = 0; type = RC_INT; }

		VirtualColumn	*vc;
		GT_Aggregation	operation;		// not used for grouping columns
		bool			distinct;		// not used for grouping columns
		AttributeType	type;
		int				size;
		int				precision;
		// optimization statistics, not used for grouping columns:
		_int64			min;
		_int64			max;
		_int64			max_no_values;	// upper approximation of a number of distinct values (without null), 0 - don't know
		DTCollation		collation;
	};
	std::vector<ColTempDesc> grouping_desc;	// input fields description
	std::vector<ColTempDesc> aggregated_desc;

	GT_Aggregation*		operation;		// Note: these tables will be created in Initialize(); using vectors inside grouper may be too slow
	bool*				distinct;

	VirtualColumn**		vc;
	Aggregator**		aggregator;		// a table of actual aggregators
	ColumnBinEncoder**	encoder;		// encoders for grouping columns

	// "distinct" part
	GroupDistinctTable** gdistinct;		// NULL if not used

	// general descriptions

	int					no_attr;
	int					no_grouping_attr;
	bool				is_full;		// true if hash conflicts persisted
	bool				distinct_present;

	// some memory managing
	_int64		max_total_size;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#endif
