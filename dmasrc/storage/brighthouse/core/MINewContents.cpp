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

#include "MultiIndex.h"
#include "MIRoughSorter.h"
#include "Joiner.h"
#include "DimensionGroupVirtual.h"

using namespace std;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

MINewContents::MINewContents(MultiIndex *m, JoinTips &tips) 
:	mind(m), t_new(NULL), optimized_dim_stay(-1), f_opt(NULL), t_opt(NULL)
{
	no_dims = mind->NoDimensions();
	dim_involved = DimensionVector(no_dims);		// init as empty
	nulls_possible = new bool [no_dims];
	forget_now = new bool [no_dims];
	t_new = new IndexTable * [no_dims];
	new_value = new _int64 [no_dims];
	ignore_repetitions_dim = -1;
	for(int i = 0; i < no_dims; i++) {
		t_new[i] = NULL;
		nulls_possible[i] = false;
		forget_now[i] = tips.forget_now[i];
		if(tips.distinct_only[i])
			ignore_repetitions_dim = i;
	}
	obj = 0;
	roughsorter = NULL;
	f_opt_max_ones = 0;
	content_type = MCT_UNSPECIFIED;
	max_filter_val = -1;
	min_block_shift = 0;
}

MINewContents::~MINewContents()
{
	for(int i = 0; i < no_dims; i++)
		delete t_new[i];
	delete [] t_new;
	delete [] nulls_possible;
	delete [] forget_now;
	delete f_opt;
	delete t_opt;
	delete roughsorter;
	delete [] new_value;
}

void MINewContents::SetDimensions(DimensionVector &dims)	// mark all these dimensions as to be involved
{
	dim_involved.Plus(dims);
	mind->MarkInvolvedDimGroups(dim_involved);
}

void MINewContents::Init(_int64 initial_size)		// initialize temporary structures (set approximate size)
{
	MEASURE_FET("MINewContents::Init(...)");
	// check for special (filter + forgotten) case
	for(int dim = 0; dim < no_dims; dim++) {
		if(dim_involved[dim] && !forget_now[dim]) {
			if(optimized_dim_stay != -1) {		// more than one unforgotten found
				optimized_dim_stay = -1;
				break;
			}
			optimized_dim_stay = dim;
		}	// optimized_dim_stay > -1 if there is exactly one unforgotten dimension
	}
	if(optimized_dim_stay != -1 && mind->GetFilter(optimized_dim_stay) == NULL)
		optimized_dim_stay = -1;				// filter case only

	if(optimized_dim_stay != -1)
		content_type = MCT_FILTER_FORGET;
	else {
		// check for Virtual Dimension case
		for(int dim = 0; dim < no_dims; dim++) {
			if(dim_involved[dim] && !forget_now[dim] && mind->MaxNoPacks(dim) > 1) {
				if(optimized_dim_stay != -1) {		// more than one large found
					optimized_dim_stay = -1;
					break;
				}
				optimized_dim_stay = dim;
			}	// optimized_dim_stay > -1 if there is exactly one unforgotten dimension
		}
		if(optimized_dim_stay != -1 && mind->GetFilter(optimized_dim_stay) == NULL)
			optimized_dim_stay = -1;				// filter case only
		if(optimized_dim_stay != -1)
			content_type = MCT_VIRTUAL_DIM;
	}
	if(content_type == MCT_UNSPECIFIED)
		content_type = MCT_MATERIAL;

	for(int dim = 0; dim < no_dims; dim++)
		if(dim_involved[dim])
			mind->LockForGetIndex(dim);				// locking for creation

	// general case
	bool roughsort_needed = false;
	min_block_shift = 64;
	for(int dim = 0; dim < no_dims; dim++) {
		if(dim_involved[dim]) {
			delete t_new[dim];
			t_new[dim] = NULL;
			if(forget_now[dim])
				continue;
			if(dim != optimized_dim_stay) {
				InitTnew(dim, initial_size);
				min_block_shift = min(min_block_shift, t_new[dim]->BlockShift());
			}
			else {
				InitTnew(dim, 64);		// minimal initial size - maybe will not be used at all
				min_block_shift = min(min_block_shift, t_new[dim]->BlockShift());
				t_opt = t_new[dim];
				t_new[dim] = NULL;
				std::map<int,int> &nfblocks=mind->GetFilter(dim)->GetNFBlocks();
				f_opt = new Filter(mind->GetFilter(dim)->NoObjOrig(),nfblocks);
				f_opt_max_ones = mind->GetFilter(dim)->NoOnes();
				f_opt->Reset();
			}
			if(mind->OrigSize(dim) > 65536 && dim != optimized_dim_stay)			// more than one material
				roughsort_needed = true;
		}
	}
	obj = 0;
	// Prepare rough sorting, if needed
	if(roughsort_needed)
		roughsorter = new MINewContentsRSorter(mind, t_new, min_block_shift);
}

void MINewContents::InitTnew(int dim, _int64 initial_size)
{
	if(initial_size < 8)
		initial_size = 8;
	t_new[dim] = new IndexTable(initial_size, mind->OrigSize(dim), 0);
	t_new[dim]->SetNoLocks(mind->group_for_dim[dim]->NoLocks(dim));
	nulls_possible[dim] = false;
}

void MINewContents::CommitCountOnly(_int64 joined_tuples)		// commit changes to multiindex - must be called at the end, or changes will be lost
{
	MEASURE_FET("MINewContents::CommitCountOnly(...)");
	mind->MakeCountOnly(joined_tuples, dim_involved);	// inside: UpdateNoTuples();
	for(int dim = 0; dim < no_dims; dim++)
		if(dim_involved[dim])
			mind->UnlockFromGetIndex(dim);
}

void MINewContents::Commit(_int64 joined_tuples)		// commit changes to multiindex - must be called at the end, or changes will be lost
{
	MEASURE_FET("MINewContents::Commit(...)");
	vector<int> no_locks(no_dims);
	for(int dim = 0; dim < no_dims; dim++) if(dim_involved[dim]) {
		no_locks[dim] = mind->group_for_dim[dim]->NoLocks(dim);
	}

	// dim_involved contains full original groups (to be deleted)
	for(int dim = 0; dim < no_dims; dim++) if(dim_involved[dim]) {
		int group_no = mind->group_num_for_dim[dim];
		if(mind->dim_groups[group_no]) { // otherwise already deleted
			delete mind->dim_groups[group_no];
			mind->dim_groups[group_no] = NULL;
		}
	}	
	// Now all involved groups must be replaced by a new contents

	if(content_type == MCT_FILTER_FORGET) {	// optimized version: just exchange filters
		DimensionGroupFilter* nf = new DimensionGroupFilter(optimized_dim_stay, f_opt, 2);	// mode 2: pass Filter ownership to the DimensionGroup
		f_opt = NULL;
		nf->Lock(optimized_dim_stay, no_locks[optimized_dim_stay]);
		mind->dim_groups.push_back(nf);
		DimensionVector dims_to_forget(dim_involved);
		dims_to_forget[optimized_dim_stay] = false;
		DimensionGroupMaterialized* ng = new DimensionGroupMaterialized(dims_to_forget);		// forgotten dimensions
		mind->dim_groups.push_back(ng);
		ng->SetNoObj(1);		// set a dummy size 1 for a group containing forgotten dimensions only
	} else if(content_type == MCT_VIRTUAL_DIM) {	// optimized version: virtual dimension group
		DimensionGroupVirtual* nv = new DimensionGroupVirtual(dim_involved, optimized_dim_stay, f_opt, 2);	// mode 2: pass Filter ownership to the DimensionGroup
		f_opt = NULL;
		nv->Lock(optimized_dim_stay, no_locks[optimized_dim_stay]);
		mind->dim_groups.push_back(nv);
		for(int dim = 0; dim < no_dims; dim++) {
			if(dim_involved[dim] && !forget_now[dim] && dim != optimized_dim_stay) {
				t_new[dim]->SetNoLocks(no_locks[dim]);
				nv->NewDimensionContent(dim, t_new[dim], nulls_possible[dim]);
				t_new[dim] = NULL;		// ownership transferred to the DimensionGroup
			}
		}
	} else {

		// now we should exchange existing joined dimensions into the newly calculated ones
		if(roughsorter) {
			roughsorter->Commit(obj);		// sort roughly the current t_new contents, if needed
			roughsorter->Barrier();
		}

		DimensionGroupMaterialized* ng = new DimensionGroupMaterialized(dim_involved);		// involving also forgotten
		mind->dim_groups.push_back(ng);
		ng->SetNoObj(obj);
		for(int dim = 0; dim < no_dims; dim++) {
			if(dim_involved[dim] && !forget_now[dim]) {
				t_new[dim]->SetNoLocks(no_locks[dim]);
				ng->NewDimensionContent(dim, t_new[dim], nulls_possible[dim]);
				t_new[dim] = NULL;		// ownership transferred to the DimensionGroup
			}
		}
	}
	mind->FillGroupForDim();
	mind->UpdateNoTuples();
	for(int dim = 0; dim < no_dims; dim++)
		if(dim_involved[dim] && !forget_now[dim])
			mind->UnlockFromGetIndex(dim);
}

void MINewContents::DisableOptimized()
{
	MEASURE_FET("MINewContents::DisableOptimized(...)");
	assert(optimized_dim_stay != -1);
	if(!forget_now[optimized_dim_stay]) {
		assert(t_new[optimized_dim_stay] == NULL);
		t_new[optimized_dim_stay] = t_opt;
		t_opt = NULL;
		FilterOnesIterator it(f_opt);
		t_new[optimized_dim_stay]->ExpandTo(f_opt->NoOnes());
		_int64 loc_obj = 0;
		while(it.IsValid()) {
			t_new[optimized_dim_stay]->Set64(loc_obj++, *it + 1);
			++it;
		}
		if(roughsorter == NULL && mind->OrigSize(optimized_dim_stay) > 65536)
			roughsorter = new MINewContentsRSorter(mind, t_new, min_block_shift);
		else if(roughsorter) {
			roughsorter->Barrier();
			roughsorter->AddColumn(t_new[optimized_dim_stay], optimized_dim_stay);
		}
	}
	delete f_opt;
	f_opt = NULL;
	optimized_dim_stay = -1;
	content_type = MCT_MATERIAL;
}

bool MINewContents::CommitPack(int pack)	// in case of single filter as a result: set a pack as not changed (return false if cannot do it)
{
	if(content_type != MCT_FILTER_FORGET)
		return false;
	::Filter* orig_filter = mind->GetFilter(optimized_dim_stay);
	if(!f_opt->IsEmpty(pack) || orig_filter == NULL)
		return false;
	f_opt->CopyBlock(*orig_filter, pack);
	return true;
}

void MINewContents::CommitNewTableValues()	// set a value (NULL_VALUE_64 is a null object index); if the index is larger than the current size, enlarge table automatically
{
	if(content_type == MCT_FILTER_FORGET) {
		_int64 val = new_value[optimized_dim_stay];
		if(val == NULL_VALUE_64 || 
			(optimized_dim_stay != ignore_repetitions_dim && f_opt->Get(val))) {
			// repetition or null object found
			DisableOptimized();
			CommitNewTableValues();				// again, content_type is changed now
			return;
		}
		f_opt->Set(val);
	} else if(content_type == MCT_VIRTUAL_DIM) {
		_int64 val = new_value[optimized_dim_stay];
		if(val == NULL_VALUE_64 || val <= max_filter_val) {	// can only forward iterate in virtual case
			// repetition or null object found
			DisableOptimized();
			CommitNewTableValues();			// again, content_type is changed now
			return;
		}
		f_opt->Set(val);
		max_filter_val = val;					// always forward
	}
	if(content_type != MCT_FILTER_FORGET) {
		if(roughsorter)
			roughsorter->CommitValues(new_value, obj);		// analyze whether to sort roughly the current t_new contents

		for(int dim = 0; dim < mind->NoDimensions(); dim++) {
			if(t_new[dim]) {					// == !forget_now[dim] && dim_involved[dim] && optimized_dim_stay != dim
				if((_uint64)obj >= t_new[dim]->N()) {
					if(roughsorter)
						roughsorter->Barrier();
					t_new[dim]->ExpandTo(obj < 2048 ? 2048 : obj * 4);	// enlarge with a safe backup
				}
				if(new_value[dim] == NULL_VALUE_64) {
					t_new[dim]->Set64(obj, 0);
					nulls_possible[dim] = true;
				} else
					t_new[dim]->Set64(obj, new_value[dim] + 1);
			}
		}
	}
	obj++;
}

bool MINewContents::NoMoreTuplesPossible()
{
	return	(content_type == MCT_FILTER_FORGET || content_type == MCT_VIRTUAL_DIM) && 
		optimized_dim_stay == ignore_repetitions_dim &&
		f_opt->NoOnes() == f_opt_max_ones;
}
