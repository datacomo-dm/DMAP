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

#include <iostream>
#include <assert.h>
#include <fcntl.h>
#ifdef __GNUC__
#include <sys/stat.h>
#endif

#include "Filter.h"
#include "loader/Loader.h"
#include "system/IBFile.h"
#include "system/ib_system.h"
#include "common/bhassert.h"
#include "tools.h"

using namespace std;
using namespace bh;

#define RANGEFULL 0x0000ffff

const int Filter::bitBlockSize = BIT_BLOCK_SIZE;
const int Filter::compressedBitBlockSize = COMPRESSED_BIT_BLOCK_SIZE;

IBMutex IBHeapAllocator::mutex;
TheFilterBlockOwner* the_filter_block_owner = 0;
// 存储在列属性文件中的no_obj,是按照实际的pack数量计算的,因此,和nf_blocks是一致的
//   但是,经过过滤的临时结果表中的no_obj,则无法用nf_blocks来衡量
// 无论是分区改造nf_block以前,还是改造以后,通过临时表传递的no_obj,无法与原始表的pack通过no_obj来计算!
Filter::Filter(_int64 no_obj, std::map<int,int> &notfullblocks,bool all_ones, bool shallow) :
no_blocks(0), block_status(0), blocks(0), block_allocator(0), no_of_bits_in_last_block(0),
track_changes(false), shallow(shallow),
bit_block_pool(0), delayed_stats(-1), delayed_block(-1), bit_mut(0)
{
	MEASURE_FET("Filter::Filter(int, bool, bool)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_obj >= 0);
	if(no_obj > MAX_ROW_NUMBER)
		throw OutOfMemoryRCException("Too many tuples.    (48)");
	blocks_nf=notfullblocks;
	no_blocks = bh::common::NoObj2NoPacks(no_obj);
	//no_of_bits_in_last_block should be equal to blocks_nf[no_blocks-1]
	no_of_bits_in_last_block = (no_obj & 0xFFFF) == 0 ? 0x10000 : int(no_obj & 0xFFFF);
		
	if(no_obj == 0)
		no_of_bits_in_last_block = 0;
  std::map<int,int>::iterator iter;
  nfobjs=0;
  for(iter=blocks_nf.begin();iter!=blocks_nf.end();iter++)
  {
  	if(iter->first>no_blocks-1) {
		std::map<int,int>::iterator iter_e=iter;
		iter++;
		blocks_nf.erase(iter_e);
		if(blocks_nf.size()==0) break;
		iter--;
  	}
  	else 
		nfobjs+=(65536-iter->second);
  }
  if(blocks_nf.size()>0) {
  	iter=blocks_nf.end();
	--iter;
	if(no_blocks-1==iter->first)
		no_of_bits_in_last_block=iter->second;
	//else if(iter->first>no_blocks-1)
		
	//else if(no_blocks-1<iter->first) 
		// input a wrong no_obj! try to repair
	//	no_blocks=iter->first+1;
  }
  
  if(blocks_nf.size()==0 && no_of_bits_in_last_block!=0) {
  	blocks_nf[no_blocks-1]=no_of_bits_in_last_block;
  	nfobjs=65536-no_of_bits_in_last_block;
  }
  
	blocks = 0;
	cached_b=cached_n=-1;
	Construct(all_ones);
}

Filter::Filter(const Filter& filter) :
no_blocks(0), block_status(0), blocks(0), no_of_bits_in_last_block(0),
track_changes(filter.track_changes), shallow(false),
bit_block_pool(0),  delayed_stats(-1), delayed_block(-1), bit_mut(0)
{
	MEASURE_FET("Filter::Filter(const Filter&)");
	Filter& tmp_filter = const_cast<Filter&> (filter);
	no_blocks = filter.NoBlocks();
	no_of_bits_in_last_block = tmp_filter.NoAddBits();
	blocks = new Block *[no_blocks];
	blocks_nf=tmp_filter.blocks_nf;
	nfobjs=tmp_filter.nfobjs;
	ConstructPool();

	for(int i = 0; i < no_blocks; i++) {
		if(tmp_filter.GetBlock(i)) {
			blocks[i] = block_allocator->Alloc(false);
			new(blocks[i]) Block(*(tmp_filter.GetBlock(i)), this); //TODO: JB check copying to use pool
		} else
			blocks[i] = NULL;
	}
	block_status = new uchar[no_blocks];
	memcpy(block_status, filter.block_status, no_blocks);
	cached_b=cached_n=-1;
	if(track_changes)
		was_block_changed = filter.was_block_changed;
}

Filter::Filter() :
no_blocks(0), block_status(0), blocks(0), block_allocator(0), no_of_bits_in_last_block(0),
track_changes(false), shallow(false),
bit_block_pool(0),  delayed_stats(-1), delayed_block(-1), bit_mut(0)
{
	cached_b=cached_n=-1;
}


void Filter::Construct(bool all_ones)
{
	try {
		if(no_blocks > 0) {
			blocks = new Block *[no_blocks];
			for(int i = 0; i < no_blocks; i++)
				blocks[i] = NULL;
			block_status = new uchar[no_blocks];
			memset(block_status, (all_ones ? (int) FB_FULL : (int) FB_EMPTY), no_blocks); // set the filter to all empty
			//No idea how to create an allocator for the pool below to use BH heap, due to static methods in allocator
		}
		if(!shallow)
			ConstructPool();
		//FIXME if we put throw statement here (which is handled by the catch statement below) then we have crash
		//			on many sql statements e.g. show tables, select * from t - investigate this
	} catch (...) {
		this->~Filter();
		throw OutOfMemoryRCException();
	}
}

void Filter::ConstructPool()
{
	bit_block_pool = new boost::pool<IBHeapAllocator>(bitBlockSize);
	bit_mut = new IBMutex();
	block_allocator = new BlockAllocator();
}

Filter* Filter::ShallowCopy(Filter& f)
{
	Filter* sc = new Filter(f.NoObjOrig(),f.blocks_nf, true, true); //shallow construct - no pool
	sc->shallow = true;
	sc->block_allocator = f.block_allocator;
	sc->track_changes = f.track_changes;
	sc->bit_block_pool = f.bit_block_pool;
	sc->bit_mut = f.bit_mut;
	if(sc->track_changes)
		sc->was_block_changed.resize(sc->no_blocks, false);
	sc->delayed_stats = -1;
	sc->delayed_block = -1;
	sc->cached_b=f.cached_b;
	sc->cached_n=f.cached_n;
	sc->blocks_nf=f.blocks_nf;
	sc->nfobjs=f.nfobjs;
	for(int i = 0; i < sc->no_blocks; i++)
		sc->blocks[i] = f.blocks[i];
	memcpy(sc->block_status, f.block_status, sc->no_blocks);
	return sc;
}


Filter::~Filter()
{
	MEASURE_FET("Filter::~Filter()");
	if(blocks) {
		delete[] blocks;
	}
	delete[] block_status;

	if(!shallow) {
		delete bit_block_pool;
		delete bit_mut;
		delete block_allocator;
	}
}

std::auto_ptr<Filter> Filter::Clone() const
{
	return std::auto_ptr<Filter>(new Filter(*this) );
}

void Filter::ResetDelayed(int b, int pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	// for parallel process of blocks ,remove delayed reset. do reset directly
	Reset(b, pos);
	/*
	if(block_status[b] == FB_MIXED) {
		Reset(b, pos);
		return;
	}
	if(block_status[b] == FB_FULL) {
		if(delayed_block != b) {
			Commit();
			delayed_block = b;
			delayed_stats = -1;
		}
		if(pos == delayed_stats + 1) {
			delayed_stats++;
		} else if(pos > delayed_stats + 1) {// then we can't delay
			if(delayed_stats >= 0)
				ResetBetween(b, 0, b, delayed_stats);
			Reset(b, pos);
			delayed_stats = -2; // not to use any longer
		}
		// else do nothing
	}*/
}

void Filter::Commit()
{
	if(delayed_block > -1) {
		if(block_status[delayed_block] == FB_FULL && delayed_stats >= 0)
			ResetBetween(delayed_block, 0, delayed_block, delayed_stats);
		delayed_block = -1;
		delayed_stats = -1;
	}
}

void Filter::Set()
{
	MEASURE_FET("voFilter::Set()");
	memset(block_status, FB_FULL, no_blocks); // set the filter to all full

	for(int b = 0; b < no_blocks; b++) {
		if(blocks[b]) {
			blocks[b] = NULL;
		}
	}
	delayed_stats = -1;

	if(track_changes)
		was_block_changed.set();
}

void Filter::SetBlock(int b)
{
	MEASURE_FET("Filter::SetBlock(int)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	block_status[b] = FB_FULL; // set the filter to all full
	if(blocks[b]) {
		//		block_object_pool->destroy(blocks[b]);
		blocks[b] = NULL;
	}
	if(track_changes)
		was_block_changed.set(b);
}

void Filter::Set(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	if(block_status[b] == FB_EMPTY) {
		block_status[b] = FB_MIXED;
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, BlockObj(b));
/*
		if(b == no_blocks - 1) {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, no_of_bits_in_last_block);
		} else {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this);
		}
*/
		if(blocks[b] == NULL)
			throw OutOfMemoryRCException();
	}
	if(blocks[b]) {
		bool full = blocks[b]->Set(n);
		if(full)
			SetBlock(b);
	}
	if(track_changes)
		was_block_changed.set(b);
}

void Filter::SetBetween(_int64 n1, _int64 n2)
{
	if(n1 == n2)
		Set(int(n1 >> 16), int(n1 & 65535));
	else
		SetBetween(int(n1 >> 16), int(n1 & 65535), int(n2 >> 16), int(n2 & 65535));
}

void Filter::SetBetween(int b1, int n1, int b2, int n2)
{
	MEASURE_FET("Filter::SetBetween(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b2 < no_blocks);
	if(b1 == b2) {
		if(block_status[b1] == FB_EMPTY) {
			// if full block
			if(  n2 >= BlockObj(b1)-1 )
//			if( (b1 == no_blocks - 1 && n1 == 0 && n2 == no_of_bits_in_last_block - 1) ||
//				(b1 < no_blocks - 1 && n1 == 0 && n2 == 65535) )
				block_status[b1] = FB_FULL;
			else {
				block_status[b1] = FB_MIXED;
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, BlockObj(b1)-1);
/*				
				if(b1 == no_blocks - 1) {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, no_of_bits_in_last_block);
				} else {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this);
				}
*/
				if(blocks[b1] == NULL)
					throw OutOfMemoryRCException();
			}
		}
		if(blocks[b1]) {
			bool full = blocks[b1]->Set(n1, n2);
			if(full)
				SetBlock(b1);
		}
		if(track_changes)
			was_block_changed.set(b1);
	} else {
		if(n1 == 0)
			SetBlock(b1);
		else
			SetBetween(b1, n1, b1, BlockObj(b1)-1); // note that b1 is never the last block
		for(int i = b1 + 1; i < b2; i++)
			SetBlock(i);
		SetBetween(b2, 0, b2, n2);
	}
}

void Filter::Reset()
{
	MEASURE_FET("Filter::Reset()");
	memset(block_status, FB_EMPTY, no_blocks); // set the filter to all empty
	for(int b = 0; b < no_blocks; b++) {
		if(blocks[b]) {
			//			block_object_pool->destroy(blocks[b]);
			blocks[b] = NULL;
		}
	}
	delayed_stats = -1;
	if(track_changes)
		was_block_changed.set();
}

void Filter::ResetBlock(int b)
{
	MEASURE_FET("Filter::ResetBlock(int)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	block_status[b] = FB_EMPTY;
	if(blocks[b]) {
		blocks[b] = NULL;
	}
	if(track_changes)
		was_block_changed.set(b);
}
/*
void Filter::Reset(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	if(block_status[b] == FB_FULL) {
		block_status[b] = FB_MIXED;
		if(b == no_blocks - 1) {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, no_of_bits_in_last_block, true); // set as full, then reset a part of it
		} else {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, 65536, true);
		}
		if(blocks[b] == NULL)
			throw OutOfMemoryRCException();
	}
	if(blocks[b]) {
		if(blocks[b]->Reset(n))
			ResetBlock(b);
	}
	if(track_changes)
		was_block_changed.set(b);
}
*/
void Filter::ResetBetween(_int64 n1, _int64 n2)
{
	if(n1 == n2)
		Reset(int(n1 >> 16), int(n1 & 65535));
	else
		ResetBetween(int(n1 >> 16), int(n1 & 65535), int(n2 >> 16), int(n2 & 65535));
}

void Filter::ResetBetween(int b1, int n1, int b2, int n2)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b2 <= no_blocks);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL) {
			// if full block
/*			
			if( (b1 == no_blocks - 1 && n1 == 0 && n2 == no_of_bits_in_last_block - 1) ||
				(b1 < no_blocks - 1 && n1 == 0 && n2 == 65535) )
*/
			if(n1==0 && n2>=BlockObj(b1)-1)
				ResetBlock(b1);
			else {
				block_status[b1] = FB_MIXED;
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, BlockObj(b1), true); // set as full, then reset a part of it
/*
				if(b1 == no_blocks - 1) {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, no_of_bits_in_last_block, true); // set as full, then reset a part of it
				} else {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, 65536, true);
				}
*/
				if(blocks[b1] == NULL)
					throw OutOfMemoryRCException();
			}
		}
		if(blocks[b1]) {
			bool empty = blocks[b1]->Reset(n1, n2);
			if(empty)
				ResetBlock(b1);
		}
		if(track_changes)
			was_block_changed.set(b1);
	} else {
		if(n1 == 0)
			ResetBlock(b1);
		else
			ResetBetween(b1, n1, b1, BlockObj(b1)-1); // note that b1 is never the last block
		for(int i = b1 + 1; i < b2; i++)
			ResetBlock(i);
		ResetBetween(b2, 0, b2, n2);
	}
}

void Filter::Reset(Filter &f2)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			ResetBlock(b);
		else if(f2.block_status[b] != FB_EMPTY) // else no change
		{
			if(block_status[b] == FB_FULL) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				blocks[b]->Not(); // always nontrivial
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
		}
	}
}

bool Filter::Get(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	if(block_status[b] == FB_EMPTY)
		return false;
	if(block_status[b] == FB_FULL)
		return true;
	return blocks[b]->Get(n);
}

bool Filter::IsEmpty()
{
	for(int i = 0; i < no_blocks; i++)
		if(block_status[i] != FB_EMPTY)
			return false;
	return true;
}

bool Filter::IsEmpty(int b) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	return (block_status[b] == FB_EMPTY);
}

bool Filter::IsEmptyBetween(_int64 n1, _int64 n2)	// true if there are only 0 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return !Get(n1);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL)
			return false;
		if(block_status[b1] == FB_EMPTY)
			return true;
		return blocks[b1]->IsEmptyBetween(nn1, nn2);
	} else {
		int full_pack_start = (nn1 == 0? b1 : b1 + 1);
		int full_pack_stop  = b2 - 1;
//		if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
		if(nn2 >= BlockObj(b2)-1)
			full_pack_stop	= b2;
		for(int i = full_pack_start; i <= full_pack_stop; i++)
			if(block_status[i] != FB_EMPTY)
				return false;
		if(b1 != full_pack_start) {
			if(block_status[b1] == FB_FULL)
				return false;
//			if( block_status[b1] != FB_EMPTY &&
//				!blocks[b1]->IsEmptyBetween(nn1, 65535))  // note that b1 is never the last block
			if( block_status[b1] != FB_EMPTY &&
				!blocks[b1]->IsEmptyBetween(nn1, BlockObj(b1)-1))  
				return false;
		}
		if(b2 != full_pack_stop) {
			if(block_status[b2] == FB_FULL)
				return false;
			if( block_status[b2] != FB_EMPTY &&
				!blocks[b2]->IsEmptyBetween(0, nn2))
				return false;
		}
	}
	return true;
}

bool Filter::IsFullBetween(_int64 n1, _int64 n2)	// true if there are only 1 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return Get(n1);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL)
			return true;
		if(block_status[b1] == FB_EMPTY)
			return false;
		return blocks[b1]->IsFullBetween(nn1, nn2);
	} else {
		int full_pack_start = (nn1 == 0? b1 : b1 + 1);
		int full_pack_stop  = b2 - 1;
//		if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
		if(nn2 >= BlockObj(b2)-1)
			full_pack_stop	= b2;
		for(int i = full_pack_start; i <= full_pack_stop; i++)
			if(block_status[i] != FB_FULL)
				return false;
		if(b1 != full_pack_start) {
			if(block_status[b1] == FB_EMPTY)
				return false;
//			if( block_status[b1] != FB_FULL &&
//				!blocks[b1]->IsFullBetween(nn1, 65535))  // note that b1 is never the last block
			if( block_status[b1] != FB_FULL &&
				!blocks[b1]->IsFullBetween(nn1, BlockObj(b1)-1))  // note that b1 is never the last block
				return false;
		} 
		if(b2 != full_pack_stop) {
			if(block_status[b2] == FB_EMPTY)
				return false;
			if( block_status[b2] != FB_FULL &&
				!blocks[b2]->IsFullBetween(0, nn2))
				return false;
		}
	}
	return true;
}

bool Filter::IsFull() const
{
	for(int i = 0; i < no_blocks; i++)
		if(block_status[i] != FB_FULL)
			return false;
	return true;
}

bool Filter::IsFull(int b) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	return (block_status[b] == FB_FULL);
}

void Filter::CopyBlock(Filter& f, int block)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(block < no_blocks);
	block_status[block] = f.block_status[block];
	if(f.GetBlock(block)) {
		if(f.shallow)
			blocks[block] = f.blocks[block];
		else {
			if(!blocks[block]) {
				blocks[block] = block_allocator->Alloc();
			}
			new(blocks[block]) Block(*(f.GetBlock(block)), this);
		}
	} else {
		blocks[block] = NULL;
	}
	if(track_changes)
		was_block_changed.set(block);
}


void Filter::CopyChangedBlocks(Filter& f)
{
	assert(no_blocks == f.no_blocks && f.track_changes);

	for(int i = 0; i<no_blocks; ++i)
		if(f.WasBlockChanged(i))
			CopyBlock(f,i);
}

bool Filter::IsEqual(Filter &sec) const
{
	if(no_blocks != sec.no_blocks || no_of_bits_in_last_block != sec.no_of_bits_in_last_block)
		return false;
	for(int b = 0; b < no_blocks; b++) {
		if(block_status[b] != sec.block_status[b])
			return false;
		if(blocks[b] && blocks[b]->IsEqual(*sec.GetBlock(b)) == false)
			return false;
	}
	return true;
}

void Filter::And(Filter &f2)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_EMPTY)
			ResetBlock(b);
		else if(f2.block_status[b] != FB_FULL) // else no change
		{
			if(block_status[b] == FB_FULL) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool empty = blocks[b]->And(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
			if(track_changes)
				was_block_changed.set(b);
		}
	}
}

void Filter::Or(Filter &f2, int pack)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	int b = (pack == -1 ? 0 : pack);
	for(; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			SetBlock(b);
		else if(f2.block_status[b] != FB_EMPTY) // else no change
		{
			if(block_status[b] == FB_EMPTY) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool full = blocks[b]->Or(*(f2.GetBlock(b)));
				if(full)
					SetBlock(b);
			}
			if(track_changes)
				was_block_changed.set(b);
		}
		if(pack != -1)
			break;
	}
}

void Filter::Not()
{
	for(int b = 0; b < no_blocks; b++) {
		if(block_status[b] == FB_FULL)
			block_status[b] = FB_EMPTY;
		else if(block_status[b] == FB_EMPTY)
			block_status[b] = FB_FULL;
		else
			blocks[b]->Not();
	}
	if(track_changes)
		was_block_changed.set();
}

void Filter::AndNot(Filter &f2)			// reset all positions which are set in f2
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			ResetBlock(b);
		else if(f2.block_status[b] != FB_EMPTY && block_status[b] != FB_EMPTY) // else no change
		{
			if(block_status[b] == FB_FULL) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				blocks[b]->Not();
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
			if(track_changes)
				was_block_changed.set(b);
		}
	}
}

void Filter::SwapPack(Filter &f2, int pack)
{
	// WARNING: cannot just swap pointers, as the blocks belong to private pools!
	Block b(this);

	if(block_status[pack] == FB_MIXED) {
		// save block
		b.CopyFrom(*(blocks[pack]), this);
		if(track_changes)
			was_block_changed.set(pack);
	}
	if(f2.block_status[pack] == FB_MIXED) {
		if(!blocks[pack]) {
			blocks[pack] = block_allocator->Alloc();
			new(blocks[pack]) Block(*(f2.blocks[pack]), this);
		} else
			blocks[pack]->CopyFrom(*(f2.blocks[pack]), this);
		if(f2.track_changes)
			f2.was_block_changed.set(pack);
	}
	if(block_status[pack] == FB_MIXED) {
		if(!f2.blocks[pack]) {
			f2.blocks[pack] = block_allocator->Alloc();
			new(f2.blocks[pack]) Block(b, &f2);
		} else
			f2.blocks[pack]->CopyFrom(b, &f2);
	}
	swap(block_status[pack], f2.block_status[pack]);

	// status should be enough - a block could be reused instead of recreation
	// but this seems safer
	if(block_status[pack] != FB_MIXED)
		blocks[pack] = NULL;
	if(f2.block_status[pack] != FB_MIXED)
		f2.blocks[pack] = NULL;
	// temporary block b can get a bit table from the pool - release it
	b.Reset();
}


_int64 Filter::NoOnes() 
{
	if(no_blocks == 0)
		return 0;
	_int64 count = 0;
	for(int b = 0; b < no_blocks - 1; b++) {
		if(block_status[b] == FB_FULL)
//			count += 65536;
			count += BlockObj(b);
		else if(blocks[b])
			count += blocks[b]->NoOnes(); // else empty
	}

	if(block_status[no_blocks - 1] == FB_FULL)
		count += no_of_bits_in_last_block;
	else if(blocks[no_blocks - 1])
		count += blocks[no_blocks - 1]->NoOnes();
	return count;
}

uint Filter::NoOnes(int b)
{
	if(no_blocks == 0)
		return 0;
	if(block_status[b] == FB_FULL) {
//		if(b == no_blocks - 1)
//			return no_of_bits_in_last_block;
//		return 65536;
		return BlockObj(b);
	} else if(blocks[b])
		return blocks[b]->NoOnes();
	return 0;
}

uint Filter::NoOnesUncommited(int b)
{
	int uc = 0;
	if(delayed_block == b) {
		if(block_status[b] == FB_FULL && delayed_stats >= 0)
			uc = -delayed_stats - 1;
	}
	return NoOnes(b) + uc;
}

_int64 Filter::NoOnesBetween(_int64 n1, _int64 n2)	// no of 1 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return (Get(n1) ? 1 : 0);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL)
			return n2 - n1 + 1;
		if(block_status[b1] == FB_EMPTY)
			return 0;
		return blocks[b1]->NoOnesBetween(nn1, nn2);
	}
	_int64 counter = 0;
	int full_pack_start = (nn1 == 0? b1 : b1 + 1);
	int full_pack_stop  = b2 - 1;
//	if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
	if(nn2 >= BlockObj(b2))
		full_pack_stop	= b2;
	for(int i = full_pack_start; i <= full_pack_stop; i++) {
		if(block_status[i] == FB_MIXED)
			counter += blocks[i]->NoOnes();
		else if(block_status[i] == FB_FULL)
//			counter += (i < no_blocks - 1 ? 65536 : no_of_bits_in_last_block);
			counter += BlockObj(i);
	}
	if(b1 != full_pack_start) {
		if(block_status[b1] == FB_FULL)
//			counter += 65535 - nn1 + 1;
			counter += BlockObj(b1) - nn1 ;
		else if(block_status[b1] != FB_EMPTY)
//			counter += blocks[b1]->NoOnesBetween(nn1, 65535);  // note that b1 is never the last block
			counter += blocks[b1]->NoOnesBetween(nn1, BlockObj(b1)-1);  // note that b1 is never the last block
	}
	if(b2 != full_pack_stop) {
		if(block_status[b2] == FB_FULL)
			counter += nn2 + 1;
		else if(block_status[b2] != FB_EMPTY)
			counter += blocks[b2]->NoOnesBetween(0, nn2);
	}
	return counter;
}

int Filter::DensityWeight()			// = 65537 for empty filter or a filter with only one nonempty block.
{									// Otherwise it is an average number of ones in nonempty blocks.
	_int64 count = 0;
	int nonempty = 0;
	if(no_blocks > 0) {
		for(int b = 0; b < no_blocks - 1; b++) {
			if(block_status[b] == FB_FULL) {
//				count += 65536;
				count += BlockObj(b);
				nonempty++;
			} else if(blocks[b]) {
				count += blocks[b]->NoOnes();
				nonempty++;
			}
		}

		if(block_status[no_blocks - 1] == FB_FULL) {
			count += no_of_bits_in_last_block;
			nonempty++;
		} else if(blocks[no_blocks - 1]) {
			count += blocks[no_blocks - 1]->NoOnes();
			nonempty++;
		}
	}
	if(nonempty < 2)
		return 65537;
	return int(count / double(nonempty));
}

_int64 Filter::NoObj() const
{
	_int64 res=_int64(no_blocks)<<16;
	return res-nfobjs;
}

_int64 Filter::NoObjOrig() const
{
	_int64 res = _int64(no_blocks - (no_of_bits_in_last_block ? 1 : 0)) << 16;
	return res + no_of_bits_in_last_block;
}

int Filter::NoBlocks() const
{
	return no_blocks;
}

int Filter::NoAddBits() const
{
	return no_of_bits_in_last_block;
}


void Filter::Grow(_int64 grow_size, bool value)
{
	if(grow_size == 0)
		return;
	int new_bits_in_last = int((no_of_bits_in_last_block + grow_size) & 0xFFFF);
	if(new_bits_in_last == 0)
		new_bits_in_last = 0x10000;
	int bits_added_to_block = grow_size > 0x10000 - no_of_bits_in_last_block ? 0x10000 - no_of_bits_in_last_block
		: (int)grow_size;
	int new_blocks = no_of_bits_in_last_block == 0 ? 
		int((grow_size + 0xFFFF) >> 16) : 
		int((grow_size - (0x10000 - no_of_bits_in_last_block) + 0xFFFF) >> 16);

	if(no_blocks > 0 && track_changes)
		was_block_changed.set(no_blocks - 1);

	//add new blocks
	AddNewBlocks(new_blocks, value);

	//grow the previously last block
	if(no_blocks > 0 && bits_added_to_block > 0) {
		if(!bit_block_pool)
			ConstructPool();
		if(value) {
			if(block_status[no_blocks - 1] == FB_EMPTY) {
				block_status[no_blocks - 1] = FB_MIXED;
				blocks[no_blocks - 1] = block_allocator->Alloc();
				new(blocks[no_blocks - 1]) Block(this, new_blocks == 0 ? new_bits_in_last : 0x10000);
				if(blocks[no_blocks - 1] == NULL)
					throw OutOfMemoryRCException();
				blocks[no_blocks - 1]->Set(no_of_bits_in_last_block, no_of_bits_in_last_block + bits_added_to_block - 1);
			} else if(block_status[no_blocks - 1] == FB_MIXED)
				blocks[no_blocks - 1]->GrowBlock(bits_added_to_block, true);
		} else {
			if(block_status[no_blocks - 1] == FB_FULL) {
				block_status[no_blocks - 1] = FB_MIXED;
				blocks[no_blocks - 1] = block_allocator->Alloc();
				new(blocks[no_blocks - 1]) Block(this, new_blocks == 0 ? new_bits_in_last : 0x10000, true);
				if(blocks[no_blocks - 1] == NULL)
					throw OutOfMemoryRCException();
				blocks[no_blocks - 1]->Reset(no_of_bits_in_last_block, no_of_bits_in_last_block + bits_added_to_block
					- 1);
			} else if(block_status[no_blocks - 1] == FB_MIXED)
				blocks[no_blocks - 1]->GrowBlock(bits_added_to_block, false);
		}
	}
	if(blocks_nf.count(no_blocks-1)==1) 
		blocks_nf.erase(blocks_nf.find(no_blocks-1));
	no_blocks += new_blocks;
	no_of_bits_in_last_block = new_bits_in_last;
	blocks_nf[no_blocks-1]=no_of_bits_in_last_block;
}

void Filter::AddNewBlocks(int new_blocks, bool value)
{
	if(new_blocks > 0) {
		//grow arrays
		if(track_changes)
			was_block_changed.resize(no_blocks + new_blocks, true);
		Block** tmp_blocks = new Block *[no_blocks + new_blocks];
		uchar* tmp_block_status = new uchar[no_blocks + new_blocks];
		memcpy(tmp_blocks, blocks, sizeof(Block*) * no_blocks);
		memcpy(tmp_block_status, block_status, sizeof(uchar) * no_blocks);
		delete[] blocks;
		delete[] block_status;
		blocks = tmp_blocks;
		block_status = tmp_block_status;

		for(int i = no_blocks; i < no_blocks + new_blocks; i++) {
			blocks[i] = NULL;
			if(value)
				block_status[i] = FB_FULL;
			else
				block_status[i] = FB_EMPTY;
		}
	}
}

// block b is copied and appended
// the original one is made empty

void Filter::MoveBitsToEnd(int block, int no_of_bits_to_move)
{
	MoveBlockToEnd(block);
	if(block == no_blocks - 2)
		return;
//	if(no_of_bits_to_move < NoObj() % 65536)
//		ResetBetween(no_blocks - 1, no_of_bits_to_move, no_blocks - 1, int(NoObj() % 65536));
	if(no_of_bits_to_move < no_of_bits_in_last_block)
		ResetBetween(no_blocks - 1, no_of_bits_to_move, no_blocks - 1, no_of_bits_in_last_block);
	no_of_bits_in_last_block = no_of_bits_to_move;
	if(blocks[no_blocks - 1]) {
		blocks[no_blocks - 1]->ShrinkBlock(no_of_bits_to_move);
		if(blocks[no_blocks - 1]->NoOnes() == no_of_bits_in_last_block) {
			blocks[no_blocks - 1] = NULL;
			block_status[no_blocks - 1] = FB_FULL;
		} else if(blocks[no_blocks - 1]->NoOnes() == 0) {
			blocks[no_blocks - 1] = NULL;
			block_status[no_blocks - 1] = FB_EMPTY;
		}
		if(track_changes)
			was_block_changed.set(no_blocks - 1);
	}
}

void Filter::MoveBlockToEnd(int b)
{
	if(b != no_blocks - 1) {
		Grow(0x10000 - no_of_bits_in_last_block, false);
		no_of_bits_in_last_block = 0x10000;
	}

	AddNewBlocks(1, false);
	no_blocks++;
	block_status[no_blocks - 1] = block_status[b];
	blocks[no_blocks - 1] = blocks[b];
	blocks[b] = NULL;
	block_status[b] = FB_EMPTY;
}

char * IBHeapAllocator::malloc(const size_type bytes)
{
	//		std::cerr<< (bytes >> 20) << " for Filter\n";
	//return (char*) Instance()->alloc(bytes, BLOCK_TEMPORARY, the_filter_block_owner);
	IBGuard guard(IBHeapAllocator::mutex);
	return (char*)the_filter_block_owner->alloc(bytes,BLOCK_TEMPORARY);
}

void IBHeapAllocator::free(char * const block)
{
	//Instance()->dealloc(block, the_filter_block_owner);
	IBGuard guard(IBHeapAllocator::mutex);
	the_filter_block_owner->dealloc(block);
}

///////////////////////////////////////////////////////////////////////////////////////////

const int Filter::BlockAllocator::pool_size = 59;
const int Filter::BlockAllocator::pool_stride = 7;

Filter::BlockAllocator::BlockAllocator() : block_object_pool(sizeof (Filter::Block))
{
	free_in_pool = 0;
	pool = new Block*[pool_size];
	next_ndx = 0;
}

Filter::BlockAllocator::~BlockAllocator()
{
	delete [] pool;
}

Filter::Block* Filter::BlockAllocator::Alloc(bool sync)
{
	if(sync)
		block_mut.Lock();
	if(!free_in_pool) {
		for(int i =0; i< pool_size; i++)
			pool[i] = (Filter::Block*)block_object_pool.malloc();
		free_in_pool = pool_size;
		next_ndx = 0;
	}
	free_in_pool--;
	Block* b = pool[next_ndx];
	next_ndx = (next_ndx+pool_stride) % pool_size;
	if(sync)
		block_mut.Unlock();
	return b;
}
