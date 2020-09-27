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

#ifndef JOINERHASH_H_
#define JOINERHASH_H_

#include "Joiner.h"
#include "JoinerHashTable.h"

#include <utility> // make_pair
#include <iostream>
#include <algorithm>
#include <map>
#include <list>
#include <vector>

// 每一个元素是一个行号数组,数组长度为:mind->NoDimensions()
class simple_mem_pool;
struct stru_mem_item;
//typedef std::vector<_int64*> dimensions_pack_rows_array;
typedef std::vector<stru_mem_item*> dimensions_pack_rows_array;
typedef dimensions_pack_rows_array::iterator dimensions_pack_rows_array_iter;
typedef std::map<_int64,dimensions_pack_rows_array> packno_dimensions_pack_rows_array_map;
typedef std::map<_int64,dimensions_pack_rows_array>::iterator packno_dimensions_pack_rows_array_map_iter;

typedef struct wait_commit_pack_info{// fix dma-1202,部分排序包号
    _int64  mit_index;      // 提交匹配使用的包编号,对比时候使用
    int  pack_no;        // 会重复,left join的时候会报错
    bool is_omit;    
    _int64 pack_pos;     // 该包的起始行号
    wait_commit_pack_info(_int64 _mit_index,int _pack,_int64 _pack_pos,bool _omit = false){
        mit_index = _mit_index;
        pack_no = _pack;
        is_omit = _omit;
        pack_pos = _pack_pos;
    }
}wait_commit_pack_info,*wait_commit_pack_info_ptr;
typedef std::list<wait_commit_pack_info>  wait_commit_pack_info_array;
typedef std::list<wait_commit_pack_info>::iterator wait_commit_pack_info_array_iter;


class JoinerHashTableWorker;
class JoinerHash : public TwoDimensionalJoiner
{
    /*
     * There are two sides of join:
     * - "traversed", which is put partially (chunk by chunk) into the hash table,
     * - "matched", which is scanned completely for every chunk gathered in the hash table.
     *
     * Algorithm:
     * 1. determine all traversed and matched dimensions,
     * 2. create hash table,
     * 3. traverse the main "traversed" dimension and put key values into the hash table,
     * 4. put there also information about row numbers of all traversed dimensions
     *    (i.e. the main one and all already joined with it),
     * 5. scan the "matched" dimension, find the key values in the hash table,
     * 6. submit all the joined tuples as the result of join
     *    (take all needed tuple numbers from the hash table and "matched" part of multiindex),
     * 7. if the "traversed" side was not fully scanned, clear hash table and go to 4 with the next chunk.
     *
     */
public:
    JoinerHash(MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table, JoinTips &_tips);
    ~JoinerHash();

    void ExecuteJoinConditions(Condition& cond);
    void ForceSwitchingSides()  { force_switching_sides = true; }

private:
    _int64 TraverseDim(MINewContents &new_mind, MIIterator &mit, _int64 &outer_tuples);     // new_mind is used only for outer joins
    _int64 MatchDim(MINewContents &new_mind, MIIterator &mit);
    void ExecuteJoin();

    //extend for parallel oper:
    _int64 MatchDimMPP(MINewContents &new_mind, MIIterator &mit);
    bool can_match_dim_mpp_log;
    bool CanMatchDimMPP(MINewContents &new_mind);
    int NoValidDimension();
    bool MatchDimInList(packno_dimensions_pack_rows_array_map &dim_item_list_map,
                MINewContents &new_mind,
                simple_mem_pool* mpp_join_mem_pool);

    // 该函数不是线程安全的,只能在MatchDimInPack和MatchDimMPP 内部加锁使用
    bool CommitJoinedTuple(packno_dimensions_pack_rows_array_map &dim_item_list_map,
                wait_commit_pack_info_array &commit_pack_info_list,
                MINewContents &new_mind,
                simple_mem_pool* mpp_join_mem_pool);

    int MatchDimInPack(JoinerHashTableWorker *pworker,
                packno_dimensions_pack_rows_array_map &dim_item_list_map,
                wait_commit_pack_info_array& commit_pack_info_list, 
                MINewContents &new_mind,
                _int64 &joined_tuples,_int64 matching_row);
    
    void SaveJoinedTuple(std::vector<_int64> &new_values, MIInpackIterator &mit,_int64 hash_row);
    
    void InitOuter(Condition& cond);
    _int64 SubmitOuterMatched(MIIterator &mit, MINewContents &new_mind);    // return the number of newly added tuples
    _int64 SubmitOuterTraversed(MINewContents &new_mind);

    //////////////////////////////////////////////////////
    JoinerHashTable jhash;

    /////////////// dimensions description ///////////////
    DimensionVector traversed_dims; // the mask of dimension numbers of traversed dimensions to be put into the join result
    DimensionVector matched_dims;       // the mask of dimension numbers of matched dimensions to be put into the join result
    int no_of_traversed_dims;
    std::vector<int> traversed_hash_column; // the number of hash column containing tuple number for this dimension

    VirtualColumn **vc1;
    VirtualColumn **vc2;
    int cond_hashed;

    bool force_switching_sides;         // set true if the join should be done in different order than optimizer suggests
    bool too_many_conflicts;            // true if the algorithm is in the state of exiting and forcing switching sides
    bool other_cond_exist;              // if true, then check of other conditions is needed on matching
    std::vector<Descriptor> other_cond;

    // Statistics
    _int64 packrows_omitted;            // roughly omitted by by matching
    _int64 packrows_matched;

    _int64 actually_traversed_rows;     // "traversed" side rows, which had a chance to be in the result (for VC distinct values)

    /////////////////////////////////////////////////////
    // Outer join part
    // If one of the following is true, we are in outer join situation:
    bool watch_traversed;       // true if we need to watch which traversed tuples are used
    bool watch_matched;         // true if we need to watch which matched tuples are used
    Filter *outer_filter;       // used to remember which traversed or matched tuples are involved in join
    bool outer_nulls_only;      // true if only null (outer) rows may exists in result

    /////////////////////////////////////////////////////

    void SubmitJoinedTuple(_int64 hash_row, MIIterator &mit, MINewContents &new_mind);   
};



#endif /*JOINERHASH_H_*/
