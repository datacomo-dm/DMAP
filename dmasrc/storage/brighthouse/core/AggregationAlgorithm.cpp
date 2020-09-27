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

/////////////////////////////////////////////////////////////////////////////////////////////////////
// This is a part of TempTable implementation concerned with the query execution low-level mechanisms
/////////////////////////////////////////////////////////////////////////////////////////////////////

#include "edition/local.h"
#include "AggregationAlgorithm.h"
#include "MIIterator.h"
#include "PackGuardian.h"
#include "system/RCSystem.h"
#include "system/ConnectionInfo.h"
#include "system/fet.h"
#include "RCEngine.h"
#ifndef __BH_COMMUNITY__
#include "enterprise/edition/core/AggregationWorkerEnt.h"
#endif

using namespace std;

void AggregationAlgorithm::Aggregate(bool just_distinct, _int64& limit, _int64& offset, ResultSender* sender)
{
	MEASURE_FET("TempTable::Aggregate(...)");

	////////////////////////////////////////////
	bool group_by_found			= false;

	GroupByWrapper gbw(t->NoAttrs(), just_distinct, *m_conn);
	_uint64 upper_approx_of_groups = 1;		// will remain 1 if there is no grouping columns (aggreg. only)

	map<int,vector<PackOrderer::OrderingInfo> > oi;	//for each dimension, what column should be taken into account for pack ordering
	for(uint i = 0; i < t->NoAttrs(); i++) {			// first pass: find all grouping attributes
		TempTable::Attr& cur_a = *(t->GetAttrP(i));
		if(cur_a.mode == DELAYED)						// delayed column (e.g. complex exp. on aggregations)
			continue;
		if(( just_distinct && cur_a.alias) || cur_a.mode == GROUP_BY) {
			if(cur_a.mode == GROUP_BY)
				group_by_found = true;
			bool already_added = false;
			for(uint j = 0; j < i; j++) {
				if(*(t->GetAttrP(j)) == cur_a) {
					already_added = true;
					gbw.DefineAsEquivalent(i,j);
					break;
				}
			}
			if(already_added == false) {
				int new_attr_number = gbw.NoGroupingAttr();
				gbw.AddGroupingColumn(new_attr_number, i, *(t->GetAttrP(i)));	// GetAttrP(i) is needed

				// approximate a number of groups
				if(upper_approx_of_groups < _uint64(mind->NoTuples())) {
					_uint64 dist_vals = (_uint64)gbw.ApproxDistinctVals(new_attr_number, mind);
					if(dist_vals > _uint64(mind->NoTuples()) || upper_approx_of_groups * dist_vals > _uint64(mind->NoTuples()))
						upper_approx_of_groups = _uint64(mind->NoTuples());
					else
						upper_approx_of_groups *= dist_vals;
				}
			}
		}
	}

	for(uint i = 0; i < t->NoAttrs(); i++)	{		// second pass: find all aggregated attributes
		TempTable::Attr& cur_a = *(t->GetAttrP(i));
		if(cur_a.mode == DELAYED)	{			// delayed column (e.g. complex exp. on aggregations)
			MIDummyIterator m(1);
			cur_a.term.vc->LockSourcePacks(m);
			continue;
		}
		if( (!just_distinct && cur_a.mode != GROUP_BY) ||	// aggregation
			( just_distinct && cur_a.alias == NULL )) {		// special case: hidden column for DISTINCT
				bool already_added = false;
				for(uint j = 0; j < i; j++) {
					if(*(t->GetAttrP(j)) == cur_a) {
						already_added = true;
						gbw.DefineAsEquivalent(i, j);
						break;
					}
				}
				if(already_added)
					continue;
				_int64 max_no_of_distinct = mind->NoTuples();
				_int64 min_v = MINUS_INF_64;
				_int64 max_v = PLUS_INF_64;
				ushort max_size = cur_a.Type().GetInternalSize();

				if(cur_a.term.vc) {
					max_size = cur_a.term.vc->MaxStringSize();
					min_v = cur_a.term.vc->RoughMin();
					max_v = cur_a.term.vc->RoughMax();
					if(cur_a.distinct && cur_a.term.vc->IsDistinct() && cur_a.mode != LISTING)
						cur_a.distinct = false;			// "distinct" not needed, as values are distinct anyway
					else if(cur_a.distinct) {
						max_no_of_distinct = cur_a.term.vc->GetApproxDistVals(false);	// no nulls included
						if(rccontrol.isOn())
							rccontrol.lock(m_conn->GetThreadID()) << "Adding dist. column, min = " << min_v << ",  max = " << max_v << ",  dist = " << max_no_of_distinct << unlock;
					}
				}
				if(max_no_of_distinct == 0)
					max_no_of_distinct = 1;		// special case: aggregations on empty result (should not be 0, because it triggers max. buffer settings)
				gbw.AddAggregatedColumn(i, cur_a, max_no_of_distinct, min_v, max_v, max_size);

				//setup pack order for each dim
				if(cur_a.term.vc && cur_a.term.vc->GetDim() != -1)
					oi[cur_a.term.vc->GetDim()].push_back(PackOrderer::OrderingInfo(cur_a.term.vc, cur_a.mode, cur_a.distinct));
		}
	}

	t->SetAsMaterialized();
	t->SetNoMaterialized(0);
	if((just_distinct || group_by_found) && mind->ZeroTuples())
		return;
	if(limit != -1 && (_int64)upper_approx_of_groups > offset + limit && !(t->HasHavingConditions()))	// HAVING should disable this optimization
		upper_approx_of_groups = offset + limit;
	////////////////////////////////////////////////////////////////////////////

	gbw.Initialize(upper_approx_of_groups);
	if((gbw.IsCountOnly() || gbw.IsCountDistinctOnly()) && (offset > 0 || limit == 0)) {
		--offset;
		return;			// one row, already omitted
	}

	// TODO: do all these special cases here in one loop, and left the unresolved aggregations for normal run
	// Special cases: SELECT COUNT(*) FROM ...,    SELECT 1, 2, MIN(a), SUM(b) WHERE false
	bool all_done_in_one_row = false;
	_int64 row = 0;
	if(gbw.IsCountOnly() || mind->ZeroTuples()) {
		DimensionVector dims(mind->NoDimensions());
		dims.SetAll();
		MIIterator mit(mind, dims);
		gbw.AddAllGroupingConstants(mit);
		gbw.AddAllAggregatedConstants(mit);
		gbw.FindCurrentRow(row);		// needed to initialize grouping buffer
		// minus all not full pack, not just last pack
		// for DimensionGroupMaterialized,no any filter at all.
		_int64 rrows=0;
		if(mind->GetFilter(0)) {
			rrows=mind->GetFilter(0)->NoObj();
			//some filter?
			// select count(1) from ... where length(x)=1...
			if(mind->NoTuples()<rrows) rrows=mind->NoTuples();
		}
		else
			rrows=mind->NoTuples();
		gbw.AddAllCountStar(row, mit, rrows);
		//gbw.AddAllCountStar(row, mit, mind->NoTuples());
		all_done_in_one_row = true;
	} // Special case 2, if applicable: SELECT COUNT(DISTINCT col) FROM .....;
	else if(gbw.IsCountDistinctOnly()) {
		_int64 count_distinct = t->GetAttrP(0)->term.vc->GetExactDistVals();	// multiindex checked inside
		if(count_distinct != NULL_VALUE_64) {
			_int64 row = 0;
			gbw.FindCurrentRow(row);		// needed to initialize grouping buffer
			gbw.PutAggregatedValueForCount(0, row, count_distinct);
			all_done_in_one_row = true;
		}
	}
	if(all_done_in_one_row) {
		for(uint i = 0; i < t->NoAttrs(); i++) {		// left as uninitialized (NULL or 0)
			t->GetAttrP(i)->page_size = 1;
			t->GetAttrP(i)->CreateBuffer(1);
		}
		if(limit == -1 || (offset == 0 && limit >= 1)) {		// limit is -1 (off), or a positive number, 0 means nothing should be displayed.
			--limit;
			AggregateFillOutput(gbw, row, offset);
			if(sender) {
				TempTable::RecordIterator iter = t->begin();	
				sender->Send(iter);
			}
		}
	} else {
		_int64 local_limit = limit == -1 ? upper_approx_of_groups : limit;
		MultiDimensionalGroupByScan(gbw, local_limit, offset, oi, sender);
		if(limit != -1)
			limit = local_limit;

	}
	t->ClearMultiIndexP();					// cleanup (i.e. regarded as materialized, one-dimensional)
	if(t->HasHavingConditions())
		t->ClearHavingConditions();			// to prevent another execution of HAVING on DISTINCT+GROUP BY
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////

void AggregationAlgorithm::MultiDimensionalGroupByScan(GroupByWrapper &gbw, _int64& limit, _int64& offset, map<int,vector<PackOrderer::OrderingInfo> > &oi, ResultSender* sender)
{
	MEASURE_FET("TempTable::MultiDimensionalGroupByScan(...)");
	bool first_pass = true;
	_int64 cur_tuple = 0;				// tuples are numbered according to tuple_left filter (not used, if tuple_left is null)
	_int64 displayed_no_groups = 0;

	// Determine dimensions to be iterated
	bool no_dims_found = true;
	DimensionVector dims(mind->NoDimensions());
	gbw.FillDimsUsed(dims);
	for(int i = 0; i < mind->NoDimensions(); i++)
		if(dims[i]) {
			no_dims_found = false;
			break;
		}
	if(no_dims_found)
		dims[0] = true;							// at least one dimension is needed

	vector<PackOrderer> po(mind->NoDimensions());
	map<int,vector<PackOrderer::OrderingInfo> >::iterator oi_it;
	for(oi_it = oi.begin(); oi_it!= oi.end(); oi_it++)
		PackOrderer::ChoosePackOrderer(po[(*oi_it).first],(*oi_it).second);

	MIIterator mit(mind, dims, po);

	factor = mit.Factor();
	if(mit.NoTuples() == NULL_VALUE_64 || mit.NoTuples() > MAX_ROW_NUMBER) {	// 2^47, a limit for filter below
		throw OutOfMemoryRCException("Aggregation is too large.");
	}
	gbw.SetDistinctTuples(mit.NoTuples());
	bool multipass_no_parallel = !gbw.IsOnePass();

#ifndef __BH_COMMUNITY__
	AggregationWorkerEnt ag_worker(gbw, this);
	if(!multipass_no_parallel) {
		if(!ag_worker.MayBeParallel(mit, !multipass_no_parallel) || ag_worker.CheckThreads(mit) == 1)	// CheckThreads() must be executed if we want to be parallel
			multipass_no_parallel = true;
	}
#else
	AggregationWorker ag_worker(gbw, this);
#endif

	::Filter* tuple_left = NULL;
	if(multipass_no_parallel) {
		// DMA-678: group by dead!
		//tuple_left = new ::Filter(mit.NoTuples(),t->GetNFBlocks());
		std::map<int,int> nfblocks;
		tuple_left = new ::Filter(mit.NoOrigTuples(),nfblocks);
		tuple_left->Set();
	}
	bool rewind_needed = false;
	try {
		do {
			if(rccontrol.isOn())  {
				if(gbw.UpperApproxOfGroups() == 1 || !multipass_no_parallel)
					rccontrol.lock(m_conn->GetThreadID()) << "Aggregating: " << mit.NoTuples() << " tuples left." << unlock;
				else
					rccontrol.lock(m_conn->GetThreadID()) << "Aggregating: " << tuple_left->NoOnes() << " tuples left, " << t->NoObj() << " gr. found so far" << unlock;
			}
			cur_tuple = 0;
			gbw.ClearNoGroups();			// count groups locally created in this pass
			gbw.ClearDistinctBuffers();		// reset buffers for a new contents
			gbw.AddAllGroupingConstants(mit);
			ag_worker.Init(mit);
			if(rewind_needed)
				mit.Rewind();	// aggregated rows will be massively omitted packrow by packrow
			rewind_needed = true;
			if(multipass_no_parallel)
				for(int i = 0; i < gbw.NoAttr(); i++)
					if(gbw.GetColumn(i))
						//use default depth of prefetch...
						gbw.GetColumn(i)->InitPrefetching(mit);

			for(uint i = 0; i < t->NoAttrs(); i++) {		// left as uninitialized (NULL or 0)
				if(t->GetAttrP(i)->mode == DELAYED) {
					MIDummyIterator m(1);
					t->GetAttrP(i)->term.vc->LockSourcePacks(m);
				}
			}

			while(mit.IsValid()) { ///////////////////////// First stage - some distincts may be delayed
				if(m_conn->killed())
					throw KilledRCException();

				/////////////// Grouping on a packrow ////////////
				bool continue_grouping = true;
				if(multipass_no_parallel)
					continue_grouping = AggregatePackrow(gbw, mit, tuple_left, cur_tuple);
				else {
					packrows_found++;					// for statistics
					int grouping_result = ag_worker.AggregatePackrowOnePass(mit);
					if(grouping_result == 1)
						continue_grouping = false;
					if(grouping_result == 2)
						throw KilledRCException();
					if(grouping_result >= 3)
						throw NotImplementedRCException("Aggregation overflow.");
					if(continue_grouping) {
						if(mit.BarrierAfterPackrow())
							ag_worker.Barrier();
						mit.NextPackrow();
					}
				}

				if(!continue_grouping)
					break;
			}
			if(tuple_left)
				tuple_left->Commit();					// committing delayed resets
			MultiDimensionalDistinctScan(gbw, dims);		// if not needed, no effect
			ag_worker.Commit();

			////////////////////////////////////////////////////////////////////////////
			// Now it is time to prepare output values
			if(first_pass) {
				first_pass = false;
				_int64 upper_groups = gbw.NoGroups() + (tuple_left ? tuple_left->NoOnes() : 0);		// upper approximation: the current size + all other possible rows (if any)
				t->CalculatePageSize(upper_groups);
				if(upper_groups > gbw.UpperApproxOfGroups())
					upper_groups = gbw.UpperApproxOfGroups();					// another upper limitation: not more than theoretical number of combinations

				MIDummyIterator m(1);
				for(uint i = 0; i < t->NoAttrs(); i++) {
					t->GetAttrP(i)->CreateBuffer(upper_groups);					// note: may be more than needed
					if(t->GetAttrP(i)->mode == DELAYED)
						t->GetAttrP(i)->term.vc->LockSourcePacks(m);
				}
			}
			for(int occupied_row = 0; occupied_row < gbw.GetNoOfOccupiedRows(); occupied_row++) {		
				// copy GroupTable into TempTable, row by row
				if(t->NoObj() >= limit)
					break;
				AggregateFillOutput(gbw, gbw.GetOccupiedRow(occupied_row), offset);		// offset is decremented for each row, if positive
				if(sender && t->NoObj() > 65535) {
					TempTable::RecordIterator iter = t->begin();	
					for(_int64 i = 0; i < t->NoObj(); i++) {
						sender->Send(iter);
						++iter;
					}
					displayed_no_groups += t->NoObj();
					limit -= t->NoObj();
					t->SetNoObj(0);
				}
			}
			if(sender) {
				TempTable::RecordIterator iter = t->begin();	
				for(_int64 i = 0; i < t->NoObj(); i++) {
					sender->Send(iter);
					++iter;
				}
				displayed_no_groups += t->NoObj();
				limit -= t->NoObj();
				t->SetNoObj(0);
			} else 
				displayed_no_groups = t->NoObj();
			if(t->NoObj() >= limit)
				break;
			if(tuple_left && !tuple_left->IsEmpty())
				gbw.ClearUsed();				// prepare for the next pass, if needed
		} while(tuple_left && !tuple_left->IsEmpty());			// do the next pass, if anything left
	} catch(...) {
		ag_worker.Commit();
		delete tuple_left;
		throw;
	}
	delete tuple_left;
	if(rccontrol.isOn())
		rccontrol.lock(m_conn->GetThreadID()) << "Aggregated (" << displayed_no_groups
									<< " gr). Omitted packrows: " << gbw.packrows_omitted << " + "
									<< gbw.packrows_part_omitted << " partially, out of " << packrows_found << " total." << unlock;
}

void AggregationAlgorithm::MultiDimensionalDistinctScan(GroupByWrapper& gbw, DimensionVector &dims)
{
	MEASURE_FET("TempTable::MultiDimensionalDistinctScan(GroupByWrapper& gbw)");
	while(gbw.AnyOmittedByDistinct()) {	/////////// any distincts omitted? => another pass needed
		///// Some displays
		_int64 max_size_for_display = 0;
		for(int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
			if(gbw.distinct_watch.OmittedFilter(i) && gbw.distinct_watch.OmittedFilter(i)->NoOnes() > max_size_for_display)
				max_size_for_display = gbw.distinct_watch.OmittedFilter(i)->NoOnes();
		rccontrol.lock(m_conn->GetThreadID()) << "Next distinct pass: " << max_size_for_display << " rows left" << unlock;

		gbw.RewindDistinctBuffers();			// reset buffers for a new contents, rewind cache
		MIIterator mit(mind, dims);	// NOTE: to maintain distinct cache compatibility, rows must be visited in the same order!
		for(int distinct_attr = gbw.NoGroupingAttr(); distinct_attr < gbw.NoAttr(); distinct_attr++) {
			::Filter *omit_filter = gbw.distinct_watch.OmittedFilter(distinct_attr);
			if(omit_filter && !omit_filter->IsEmpty()) {
				mit.Rewind();
				_int64 cur_tuple = 0;
				_int64 uniform_pos = NULL_VALUE_64;
				bool require_locking = true;
				while(mit.IsValid()) {
					if(mit.PackrowStarted()) {
						if(m_conn->killed())
							throw KilledRCException();
						/////////////// All packrow-level operations /////////
						omit_filter->Commit();
						gbw.ResetPackrow();
						bool skip_packrow = false;
						bool packrow_done = false;
						bool part_omitted = false;
						_int64 packrow_length = mit.GetPackSizeLeft();
						// Check whether the packrow contain any not aggregated rows
						if(omit_filter->IsEmptyBetween(cur_tuple, cur_tuple + packrow_length - 1))
							skip_packrow = true;
						else {
							_int64 rows_in_pack = omit_filter->NoOnesBetween(cur_tuple, cur_tuple + packrow_length - 1);
							bool agg_not_changeable = false;
							AggregateRough(gbw, mit, packrow_done, part_omitted, agg_not_changeable, uniform_pos, rows_in_pack, 1, distinct_attr);
							if(packrow_done) // This packrow will not be needed any more
								omit_filter->ResetBetween(cur_tuple, cur_tuple + packrow_length - 1);
						}
						if(skip_packrow) {
							mit.NextPackrow();
							cur_tuple += packrow_length;
							continue;
						}
						require_locking  = true;					// a new packrow, so locking will be needed
					}

					/////////////// All row-level operations ////////////
					if(omit_filter->Get(cur_tuple)) {
						bool value_successfully_aggregated = false;
						if(gbw.CacheValid(distinct_attr)) {
							value_successfully_aggregated = gbw.PutCachedValue(distinct_attr);
						}
						else {
							// Locking etc.
							if(require_locking) {
								gbw.LockPack(distinct_attr, mit);
								if(uniform_pos != PLUS_INF_64)
									for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
										gbw.LockPack(gr_a, mit);
								require_locking = false;
							}

							_int64 pos = 0;
							bool existed = true;
							if(uniform_pos != PLUS_INF_64)
								pos = uniform_pos; // existed == true, as above
							else { // Construct the grouping vector
								for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++) {
									if(gbw.ColumnNotOmitted(gr_a))
										gbw.PutGroupingValue(gr_a, mit);
								}
								existed = gbw.FindCurrentRow(pos);
							}
							BHASSERT_WITH_NO_PERFORMANCE_IMPACT(existed && pos!=NULL_VALUE_64);
							value_successfully_aggregated = gbw.PutAggregatedValue(distinct_attr, pos, mit);
						}
						if(value_successfully_aggregated)
							omit_filter->ResetDelayed(cur_tuple);
						gbw.distinct_watch.gd_cache[distinct_attr].NextRead();
					}
					cur_tuple++;
					++mit;
				}
				omit_filter->Commit();	// committing delayed resets
			}
		}
		gbw.UpdateDistinctCaches();					// take into account values already counted
	}
}


bool AggregationAlgorithm::AggregatePackrow(GroupByWrapper &gbw, MIIterator &mit, Filter* tuple_left, _int64 &cur_tuple)
{
	gbw.ResetPackrow();
	_int64 uniform_pos = NULL_VALUE_64;
	bool skip_packrow = false;
	bool packrow_done = false;
	bool part_omitted = false;
	bool aggregations_not_changeable = false;
	//在当前包中剩余的行数(有效的/满足过滤条件的).
	_int64 packrow_length = mit.GetPackSizeLeft();
	// Check whether the packrow contain any not aggregated rows
	if(tuple_left->IsEmptyBetween(cur_tuple, cur_tuple + packrow_length - 1))
		skip_packrow = true;
	else {
		_int64 rows_in_pack = tuple_left->NoOnesBetween(cur_tuple, cur_tuple + packrow_length - 1);
		packrows_found++;					// for statistics
		skip_packrow = AggregateRough(gbw, mit, packrow_done, part_omitted,	aggregations_not_changeable, 
									  uniform_pos, rows_in_pack, factor);
		if(t->NoObj() + gbw.NoGroups() == gbw.UpperApproxOfGroups()) { // no more groups!
			gbw.SetNoMoreGroups();
			if(skip_packrow)			// no aggr. changeable and no new groups possible?
				packrow_done = true;
			if(gbw.NoGroupingAttr() == gbw.NoAttr()) {	// just DISTINCT without grouping
				tuple_left->Reset();	// no more rows needed, just produce output
				return false;
			}
		}
		if(skip_packrow)
			gbw.packrows_omitted++;
		else if(part_omitted)
			gbw.packrows_part_omitted++;
		if(packrow_done) { // This packrow will not be needed any more
			tuple_left->Commit();		// commit the last pack to prevent mixing delayed and non-delayed filter access
			tuple_left->ResetBetween(cur_tuple, cur_tuple + packrow_length - 1);
		}
	}
	if(packrow_done || skip_packrow) {
		cur_tuple += packrow_length;
		mit.NextPackrow();
		// skip not full block
		//if(packrow_length<65536) 
		//	cur_tuple+=(65536-packrow_length);
		return true;
	}

	bool require_locking_ag  = true;					// a new packrow, so locking will be needed
	bool require_locking_gr	= (uniform_pos == NULL_VALUE_64);	// do not lock if the grouping row is uniform
	while(mit.IsValid()) {				// becomes invalid on pack end
		if(tuple_left->Get(cur_tuple)) {
			if(require_locking_gr) {
				for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
					gbw.LockPack(gr_a, mit);	// note: ColumnNotOmitted checked inside
				require_locking_gr = false;
			}

			_int64 pos = 0;
			bool existed = true;
			if(uniform_pos != NULL_VALUE_64)		// either uniform because of KNs, or = 0, because there is no grouping columns
				pos = uniform_pos; // existed == true, as above
			else {
				for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
					if(gbw.ColumnNotOmitted(gr_a))
						gbw.PutGroupingValue(gr_a, mit);
				existed = gbw.FindCurrentRow(pos);
			}

			if(pos != NULL_VALUE_64) {						// Any place left? If not, just omit the tuple.
				tuple_left->ResetDelayed(cur_tuple);			// delayed for optimization purposes - must be committed at the end
				if(!existed) {
					aggregations_not_changeable = false;
					gbw.AddGroup();								// successfully added
					if(t->NoObj() + gbw.NoGroups() == gbw.UpperApproxOfGroups()) { // no more groups!
						gbw.SetNoMoreGroups();
						if(gbw.NoGroupingAttr() == gbw.NoAttr()) {	// just DISTINCT without grouping
							tuple_left->Reset();	// no more rows needed, just produce output
							return false;
						}
					}
				}
				if(!aggregations_not_changeable) {
					// Lock packs if needed
					if(require_locking_ag) {
						for(int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
							gbw.LockPack(gr_a, mit);	// note: ColumnNotOmitted checked inside
						require_locking_ag = false;
					}

					// Prepare packs for aggregated columns
					for(int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
						if(gbw.ColumnNotOmitted(gr_a)) {
							bool value_successfully_aggregated = gbw.PutAggregatedValue(gr_a, pos, mit, factor);
							if(!value_successfully_aggregated)
								gbw.DistinctlyOmitted(gr_a, cur_tuple);
						}
				}
			}
		}
		cur_tuple++;
		++mit;
		if(mit.PackrowStarted()) {
			// skip not full block
			//if(packrow_length<65536) 
			//	cur_tuple+=(65536-packrow_length);
			break;
		}
	}
	return true;
}

bool AggregationAlgorithm::AggregatePackrowOnePass(GroupByWrapper &gbw, MIInpackIterator& loc_mit)
{
	gbw.ResetPackrow();
	bool skip_packrow = false;
	bool packrow_done = false;
	bool part_omitted = false;
	bool aggregations_not_changeable = false;
	_int64 uniform_pos;	
	_int64 rows_in_pack = loc_mit.GetPackSizeLeft();
	skip_packrow = AggregateRough(gbw, loc_mit, packrow_done, part_omitted,
								  aggregations_not_changeable, uniform_pos, rows_in_pack, factor);
	if(gbw.NoGroups() == gbw.UpperApproxOfGroups()) { // no more groups!
		gbw.SetNoMoreGroups();
		if(skip_packrow)			// no aggr. changeable and no new groups possible?
			packrow_done = true;
		if(gbw.NoGroupingAttr() == gbw.NoAttr()) {	// just DISTINCT without grouping
			return false;		// do not continue grouping
		}
	}
	if(skip_packrow)
		gbw.packrows_omitted++;
	else if(part_omitted)
		gbw.packrows_part_omitted++;
	if(skip_packrow || packrow_done)
		return true;

	bool require_locking_ag  = true;					// a new packrow, so locking will be needed
	bool require_locking_gr	= (uniform_pos == NULL_VALUE_64);	// do not lock if the grouping row is uniform
	while(loc_mit.IsValid()) {				// becomes invalid on pack end
		if(require_locking_gr) {
			for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
				gbw.LockPack(gr_a, loc_mit);	// note: ColumnNotOmitted checked inside
			require_locking_gr = false;
		}

		_int64 pos = 0;
		bool existed = true;
		if(uniform_pos != NULL_VALUE_64)		// either uniform because of KNs, or = 0, because there is no grouping columns
			pos = uniform_pos; // existed == true, as above
		else {
			for(int gr_a = 0; gr_a < gbw.NoGroupingAttr(); gr_a++)
				if(gbw.ColumnNotOmitted(gr_a))
					gbw.PutGroupingValue(gr_a, loc_mit);
			existed = gbw.FindCurrentRow(pos);
		}

		if(pos != NULL_VALUE_64) {						// Any place left? If not, just omit the tuple.
			if(!existed) {
				aggregations_not_changeable = false;
				gbw.AddGroup();								// successfully added
				if(gbw.NoGroups() == gbw.UpperApproxOfGroups()) { // no more groups!
					gbw.SetNoMoreGroups();
					if(gbw.NoGroupingAttr() == gbw.NoAttr()) {	// just DISTINCT without grouping
						// no more rows needed, just produce output
						return false;
					}
				}
			}
			if(!aggregations_not_changeable) {
				// Lock packs if needed
				if(require_locking_ag) {
					for(int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
						gbw.LockPack(gr_a, loc_mit);	// note: ColumnNotOmitted checked inside
					require_locking_ag = false;
				}

				// Prepare packs for aggregated columns
				for(int gr_a = gbw.NoGroupingAttr(); gr_a < gbw.NoAttr(); gr_a++)
					if(gbw.ColumnNotOmitted(gr_a))
						gbw.PutAggregatedValue(gr_a, pos, loc_mit, factor);
			}
		}
		++loc_mit;
	}
	return true;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

void AggregationAlgorithm::AggregateFillOutput(GroupByWrapper &gbw, _int64 gt_pos, _int64 &omit_by_offset)
{
	MEASURE_FET("TempTable::AggregateFillOutput(...)");
	// OFFSET without HAVING
	if(!(t->HasHavingConditions()) && omit_by_offset > 0) {		// note that the rows not meeting conditions should not count in offset
		omit_by_offset--;
		return;
	}
	_int64 cur_output_tuple = t->NoObj();
	t->SetNoMaterialized(cur_output_tuple + 1);				// needed to allow value reading from this TempTable

	// Fill aggregations and grouping columns
	for(uint i = 0; i < t->NoAttrs(); i++) {
		TempTable::Attr *a = t->GetAttrP(i);								// change to pointer - for speed
		int gt_column = gbw.AttrMapping(i);
		if(gt_column == -1)							// delayed column (e.g. complex exp. on aggregations)
			continue;
		if(ATI::IsStringType(a->TypeName()))
			a->SetValueString(cur_output_tuple, gbw.GetValueT(gt_column, gt_pos));
		else {
			_int64 v = gbw.GetValue64( gt_column, gt_pos );
			a->SetValueInt64(cur_output_tuple, v);
		}
	}

	// Materialize delayed attrs (e.g. expressions on aggregation results)
	MIDummyIterator it(1);		// one-dimensional dummy iterator to iterate the result
	it.Set(0, cur_output_tuple);
	RCBString vals;
	for(uint i = 0; i < t->NoAttrs(); i++) {
		TempTable::Attr *a = t->GetAttrP(i);
		if(a->mode != DELAYED)
			continue;
		VirtualColumn *vc = a->term.vc;
		switch(a->TypeName()) {
			case RC_STRING:
			case RC_VARCHAR:
				vc->GetValueString(vals, it);
				a->SetValueString(cur_output_tuple, vals);
				break;
			case RC_BIN:
			case RC_BYTE:
			case RC_VARBYTE:
				if(!vc->IsNull(it))
					vc->GetNotNullValueString(vals, it);
				else
					vals = RCBString();
				a->SetValueString(cur_output_tuple, vals);
				break;
			default:
				a->SetValueInt64(cur_output_tuple, vc->GetValueInt64(it));
				break;
		}
	}

	// HAVING
	if(t->HasHavingConditions()) {
		if(!(t->CheckHavingConditions(it))) {		// condition not met - forget about this row (will be overwritten soon)
			t->SetNoMaterialized(cur_output_tuple);		// i.e. no_obj--;
			for(uint i = 0; i < t->NoAttrs(); i++) 
				t->GetAttrP(i)->InvalidateRow(cur_output_tuple);								// change to pointer - for speed
		} else {			
			// OFFSET with HAVING
			if(omit_by_offset > 0) {
				omit_by_offset--;
				t->SetNoMaterialized(cur_output_tuple);		// i.e. no_obj--;
			}
		}
	}
}



bool AggregationAlgorithm::AggregateRough(GroupByWrapper &gbw, MIIterator &mit,
								bool &packrow_done, bool &part_omitted, bool &aggregations_not_changeable,
								_int64 &uniform_pos, _int64 rows_in_pack, _int64 local_factor,
								int just_one_aggr)
{
	MEASURE_FET("TempTable::AggregateRough(...)");
	//// Return situations:
	//// a) ignore the packrow and check it in future (next grouping pass) =>  return true;
	//// b) ignore the packrow forever =>  packrow_done = true; return true;
	//// c) ignore just some columns =>    gbw.OmitColumnForPackrow(i); return false;
	////
	//// If just_one_aggr > -1, then it is just for one aggregation column, plus all grouping columns (next distinct pass)
	packrow_done = false;
	part_omitted = false;
	aggregations_not_changeable = false;
	bool grouping_packrow_uniform = true;
	for(int i = 0; i < gbw.NoGroupingAttr(); i++) {		// first test: whether the grouping values are not all uniform
		if(gbw.AddPackIfUniform(i, mit)) {
			gbw.OmitColumnForPackrow(i);				// rowpack may be partially uniform; grouping value already in buffer
			part_omitted = true;
		}
		else
			grouping_packrow_uniform = false;
	}
	uniform_pos = NULL_VALUE_64;					// not changed => no uniform packrow
	if(grouping_packrow_uniform) {					// the whole packrow is defined by uniform values
		bool existed = gbw.FindCurrentRow(uniform_pos);	// the row is fully prepared
		if(uniform_pos != NULL_VALUE_64) {	// Successfully found? The whole packrow goes to one group.
			if(!existed) {
				gbw.AddGroup();				// successfully added
				gbw.InvalidateAggregationStatistics();
			}
			packrow_done = true;
			for(int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
				if(just_one_aggr == -1 || just_one_aggr == i) {
					if(gbw.AggregatePackInOneGroup(i, mit, uniform_pos, rows_in_pack, local_factor)) {
						gbw.OmitColumnForPackrow(i);		// the column is marked as already done
						part_omitted = true;
					}
					else
						packrow_done = false;
				}
			if(packrow_done)
				return true;	// pack done, get the next one
		} else
			return true;		// no space to add uniform values in this pass, omit it for now
	}
	assert(!packrow_done);

	// the next step: check if anything may be changed (when no new groups can be added))
	bool all_done = false;

	if(gbw.IsFull()) {
		all_done = false;
		bool any_gr_column_left = false;		// no gr. column left => all uniform, do not ignore them
		for(int i = 0; i < gbw.NoGroupingAttr(); i++)
			if( gbw.ColumnNotOmitted(i) ) {
				any_gr_column_left = true;
				if(!gbw.AttrMayBeUpdatedByPack(i, mit)) {	// grouping values out of scope?
					all_done = true;
					break;
				}
			}
		if(all_done && any_gr_column_left)
			return true;	// packrow is done for now (to be checked in the future), get the next one

		all_done = true;
		if( gbw.NoGroupingAttr() == gbw.NoAttr() )		// no aggregations, i.e. select distinct ... - cannot omit
			all_done = false;
	}

	aggregations_not_changeable = true;		// note that it is for current groups only, the flag will be ignored if any new group is found
	for(int i = gbw.NoGroupingAttr(); i < gbw.NoAttr(); i++)
		if(just_one_aggr == -1 || just_one_aggr == i) {
			if(gbw.ColumnNotOmitted(i)) {
				if(gbw.PackWillNotUpdateAggregation(i, mit)) {
					if(gbw.IsFull()) {
						gbw.OmitColumnForPackrow(i);
						part_omitted = true;
					}
				} else
					aggregations_not_changeable = false;
			}
			if(gbw.ColumnNotOmitted(i))
				all_done = false;	// i.e. there is something to be done in case of existing grouping values
		}

	// note that we may need to open the packrow to localize groups for future scans, except:
	if( all_done && gbw.IsFull() && gbw.NoMoreGroups()) {	// no aggregation may be changed by this packrow?
		packrow_done = true;
		return true;	// packrow is excluded from the search, get the next one
	}
	return all_done;
}

