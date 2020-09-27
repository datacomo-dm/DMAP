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

#ifndef _FILTER_H_
#define _FILTER_H_

#include <assert.h>
#ifdef __WIN__
#include <winsock2.h>
#endif
#include <boost/pool/pool.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/static_assert.hpp>
#include <boost/dynamic_bitset.hpp>

#include "common/stdafx.h"
#include "common/CommonDefinitions.h"
#include "compress/BitstreamCompressor.h"
#include "system/RCSystem.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "system/IBFile.h"
#include "util/CircBuf.h"
#include "system/fet.h"
#include <map>
#define MAX_PREFETCH 10
#define MAX_PREFETCHS	MAX_PREFETCH+2
class IBHeapAllocator : public TrackableObject {
public:
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  static char * malloc(const size_type bytes);
  static void free(char * const block);

  static IBMutex mutex;
};

class TheFilterBlockOwner : public TrackableObject
{
	friend class IBHeapAllocator;
	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_FILTER;}
};

extern TheFilterBlockOwner* the_filter_block_owner;

class Filter : public TrackableObject {
public:

	friend class SavableFilter;

	Filter(_int64 no_obj, std::map<int,int> &notfullblocks,bool all_ones = false, bool shallow = false);
	Filter(const Filter& filter);

	//! like Filter(const Filter&), but the Block and block bit table pools are shared
	static Filter* ShallowCopy(Filter& f);

	~Filter();

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_FILTER; };

	// Copying operation
	std::auto_ptr<Filter> Clone() const;

	// Delayed operations on filter
	void ResetDelayed(int b, int pos);
	void ResetDelayed(_uint64 n)
	{ ResetDelayed((int)(n >> 16), (int)(n & 65535)); }

	void Commit();

	// Random and massive access to filter
	void Set();					// all = 1
	void SetBlock(int b);		// all in block = 1
	void Set(_int64 n)			// set position n to 1
	{ Set((int)(n >> 16), (int)(n & 65535));	}
	void Set(int b, int n);	// set position n in block b to 1
	void SetBetween(_int64 n1,_int64 n2);				// set objects n1-n2 to 1
	void SetBetween(int b1, int  n1, int b2, int n2);	// set 1 between...
	void Reset();
	void Reset(_int64 n)
	{ Reset((int)(n >> 16), (int)(n & 65535)); }
	void ResetBlock(int b);
	inline int GetBlockObjs(int b) {
		return (b == no_blocks - 1)?no_of_bits_in_last_block:65536;
	}
	inline void Reset(int b, int n)			// keep it inline - it's considerably faster
	{
		if(block_status[b] == FB_FULL) {
			block_status[b] = FB_MIXED;
/*			if(b == no_blocks - 1) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(this, no_of_bits_in_last_block, true); // set as full, then reset a part of it
			} else {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(this, 65536, true);
			}
			*/
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(this, BlockObj(b), true);
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
	void ResetBetween(_int64 n1,_int64 n2);
	void ResetBetween(int b1, int n1, int b2, int n2);
	void Reset(Filter &f2);			// reset all positions where f2 is 1
	bool Get(int b, int n);
	bool Get(_int64 n)
	{
		if(no_blocks == 0)
			return false;
		return Get((int)(n >> 16), (int)(n & 65535));
	}
	bool IsEmpty();
	bool IsEmpty(int b) const;	// block b
	bool IsEmptyBetween(_int64 n1, _int64 n2);	// true if there are only 0 between n1 and n2, inclusively
	bool IsFull() const;
	bool IsFull(int b) const;		// block b
	bool IsFullBetween(_int64 n1, _int64 n2);	// true if there are only 1 between n1 and n2, inclusively

	//! copy block from f to this
	void CopyBlock(Filter& f, int block);

	//! copy all changed blocks blocks from f to this.
	//! f must have track_changes == true
    void CopyChangedBlocks(Filter& f);


	// Logical operations on filter
	bool IsEqual(Filter &sec) const;
	void And(Filter &f2);
	void Or(Filter &f2, int pack = -1);	// if pack is specified, then only one pack is to be ORed
	void Not();
	void AndNot(Filter &f2);		// reset all positions which are set in f2
	void SwapPack(Filter &f2, int pack);

	// Statistics etc.
	_int64 NoOnes() ;
	uint NoOnes(int b);	// block b
	uint NoOnesUncommited(int b); //with uncommitted Set/Reset
	_int64 NoOnesBetween(_int64 n1, _int64 n2);	// no of 1 between n1 and n2, inclusively
	_int64 NoObj() const;			// number of objects (table size)
	_int64 NoObjOrig() const;
	int NoBlocks() const;
	int BlockObj(int b)  {
		if(cached_b==b) 
			return cached_n;
		cached_b=b;
		if(blocks_nf.count(b)==1) 
			cached_n=blocks_nf.at(b);
		else 
			cached_n=65536;
		return cached_n;
	}
	
	std::map<int,int> &GetNFBlocks() {
		return blocks_nf;
	}
	
	int NoAddBits() const;
	int DensityWeight();			// = 65537 for empty filter or a filter with only one nonempty block.
									// Otherwise it is an average number of ones in nonempty blocks.

	// Maintenance

	//! extend the Filter size by \e grow_size bits
	void Grow(_int64 grow_size, bool value = true);

	void MoveBlockToEnd(int b);
	void MoveBitsToEnd(int block, int no_of_bits_to_move);

	bool WasBlockChanged(int block) const {return was_block_changed[block];	}
	void StartTrackingChanges() {track_changes=true; was_block_changed.resize(no_blocks); was_block_changed.reset();}

	////////////////////////////////////////////////////////////////////////////////
	boost::pool<IBHeapAllocator> *GetBitBlockPool() { return bit_block_pool; };

	friend class FilterOnesIterator;
	friend class FilterOnesIteratorOrdered;

	class Block;
	class BlockAllocator;
	Block* GetBlock(int b) const {assert(b < no_blocks); return blocks[b];};
	uchar	GetBlockStatus(int i){assert(i < no_blocks); return block_status[i];};
	static const uchar FB_FULL = 0;		// block status: full
	static const uchar FB_EMPTY	= 1;		// block status: empty
	static const uchar FB_MIXED	= 2;		// block status: neither full nor empty

protected:
	Filter();
	virtual void AddNewBlocks(int new_blocks, bool value);
	void Construct(bool all_ones = false);

	int no_blocks;							//number of blocks
	uchar *block_status;

	std::map<int,int> blocks_nf;
	int cached_b,cached_n;
	_int64 nfobjs;

	Block** blocks;
	BlockAllocator* block_allocator;
	int no_of_bits_in_last_block;	//number of bits in the last block
	boost::dynamic_bitset<> was_block_changed;
	bool track_changes;
	bool shallow;

    enum { BIT_BLOCK_SIZE=8192,
           COMPRESSED_BIT_BLOCK_SIZE=BIT_BLOCK_SIZE+10 };

private:
	void ConstructPool();

	static const int bitBlockSize;
	static const int compressedBitBlockSize;

	boost::pool<IBHeapAllocator> *bit_block_pool;		//default allocator uses system heap
	int delayed_stats;		// used for delayed Reset: -1 = initial value; -2 = delayed not possible; >=0 = actual set position
	int delayed_block;		// block with uncommitted (delayed) changes, or -1
	IBMutex* bit_mut;

public:

	class Block {
	public:
		Block(Filter* owner, int _no_obj = 65536, bool set_full = false);
		Block(Block const& block, Filter* owner);
		void CopyFrom(Block const& block, Filter* owner);
		void Init(Filter* owner, int _no_obj = 65536, bool set_full = false);
		~Block();
		bool Set(int n);	// the object = 1
		bool Set(int n1, int n2);	// set 1 between..., true => block is full
		inline bool Reset(int n)
		{
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n < no_obj);
			uint  mask = lshift1[n & 31];
			uint& cur_bl = block_table[n >> 5];
			if(cur_bl & mask)	// if this operation change value
			{
				--no_set_bits;
				cur_bl &= ~mask;
			}
			return (no_set_bits == 0);
		}

		bool Reset(int n1, int n2);	// true => block is empty
		void Reset();
		inline bool Get(int n)
		{ return (block_table[n >> 5] & lshift1[n & 31]); }
		bool IsEqual(Block &b2);
		bool And(Block &b2);					// true => block is empty
		bool Or(Block &b2);						// true => block is full
		void Not();
		bool AndNot(Block &b2);					// true => block is empty
		uint NoOnes()
		{ return no_set_bits; }
		bool IsEmptyBetween(int n1, int n2);
		bool IsFullBetween(int n1, int n2);
		int NoOnesBetween(int n1, int n2);

		uint NoObj()							{ return no_obj; }

		void GrowBlock(int grow_size, bool value);
		void ShrinkBlock(int new_size);
		void Store(IBFile &f);		

		friend class FilterOnesIterator;
		friend class FilterOnesIteratorOrdered;

		bool IsNumberOfOnesInHeaderProper(int& tst);

	protected:
		void Clean(bool value);
		void InitBits(int  n1, int n2, bool value);
		void CommonInit(Filter *owner);
		
		uint *block_table;					// objects table
		int no_obj;							// all positions, not just '1'-s
		int no_set_bits;					// number of '1'-s
		int block_size;						// size of the block_table table in uint

		Filter* owner;

		static const uint lshift1[];
		static const int posOf1[];
//		No idea how to create an allocator for the pool below to use BH heap, due to static methods in allocator
	};
	class BlockAllocator {
	public:
		BlockAllocator();
		~BlockAllocator();
		Block* Alloc(bool sync = true);
	private:
		static const int pool_size ;
		static const int pool_stride ;
		int free_in_pool;
		int next_ndx;
		Block** pool;
		IBMutex block_mut;
		boost::pool<IBHeapAllocator> block_object_pool;

	};

};

//////////////////////////////////////////////////////////////////////

class FilterOnesIterator
{
public:
    FilterOnesIterator();
    FilterOnesIterator(Filter *ff);
    void Init(Filter *ff);
    virtual void Rewind();
    void RewindToRow(const _int64 row);
	bool RewindToPack(int pack);
    virtual void NextPack();
	bool NextInsidePack();	// like ++, but rewind to pack beginning after its end. Return false if we just restarted the pack
    bool IsValid()
    {
        return valid;
    }
		void Commit();
    _int64 operator *() const
    {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid);
        return cur_position;
    }
		void ResetDelayed();
    FilterOnesIterator & operator ++();
    _int64 GetPackSizeLeft();
    _int64 GetTotalSize()
    {
        return f->NoOnes();
    }

    bool InsideOnePack()
    {
        return inside_one_pack;
    }

    int GetCurrPack()
    {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid);
        return iterator_b;
    }

    int GetCurrInPack()
    {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid);
        return iterator_n;
    }



	void ResetCurrentPackrow()		
	{ 
		f->ResetBlock(iterator_b); 
		cur_block_full = false;
		cur_block_empty = true;
        curobjs=f->BlockObj(iterator_b);
	}

    //!make copy of the iterator, allowing the copy to traverse at most \e packs_to_go packs
    //! \e packs_to_go
    virtual FilterOnesIterator* Copy(int packs_to_go = MAX_ROW_NUMBER >> 16);

    void SetNoPacksToGo(int n);

	//! get pack number a packs ahead of the current pack
    virtual int Lookahead(int a);

    static const int max_ahead = MAX_PREFETCH*2;

protected:
    bool valid;
    int delayedblock;
    int delayedfrom;
    int delayedto;
    int curobjs;
    Filter *f;
    _int64 cur_position;
    int iterator_b;
    int iterator_n;
	bool cur_block_full;
	bool cur_block_empty;
    uint b;
	int bln;
	int lastn;
	int bitsLeft;
    int ones_left_in_block;
    int prev_block;
    bool inside_one_pack;
    void Reset();
    FixedSizeBuffer<int> buffer;
    int packs_to_go;
    int packs_done;

	bool FindOneInsidePack();		// refactored from operator++
private:
    virtual bool IteratorBpp();
};

//////////////////////////////////////////////////////////////////////

class PackOrderer;

class FilterOnesIteratorOrdered	: public FilterOnesIterator	// iterating through all values 1 in a filter with given order of blocks (datapacks)
{
public:
	FilterOnesIteratorOrdered();		// empty initialization: no filter attached
	FilterOnesIteratorOrdered(Filter *ff, PackOrderer* po);

	void Init(Filter *ff, PackOrderer* po);		// like constructor - reset iterator position
	virtual void Rewind();				// iterate from the beginning (the first 1 in Filter)

	//! move to the next 1 position within an associated Filter
	virtual void NextPack();			// iterate to a first 1 in the next nonempty pack
    virtual FilterOnesIteratorOrdered* Copy();
	//! get pack number a packs ahead of the current pack
    virtual int Lookahead(int a);

private:
	PackOrderer* po;
private:
    virtual bool IteratorBpp();
};

typedef boost::shared_ptr<Filter> FilterPtr;
typedef std::auto_ptr<Filter> FilterAutoPtr;

#endif

