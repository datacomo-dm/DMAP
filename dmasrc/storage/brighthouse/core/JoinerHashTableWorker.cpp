
#include "JoinerHash.h"
#include "JoinerHashTableWorker.h"
#include "DMThreadGroup.h"
#include "core/tools.h"
#include "TempTable.h"
#include "RCTableImpl.h"
#include "edition/vc/VirtualColumn.h"
#include "common/bhassert.h"
#include "system/ConnectionInfo.h"
#include "core/RoughMultiIndex.h"
#include "system/fet.h"

inline _int64 JoinerHashTable::GetNextRow(unsigned char*    ibuffer,_int64 &_current_row,_int64 &_to_be_returned,_int64 &_current_iterate_step)
{
    //MEASURE_FET("JoinerHashTable::InitCurrentRowToGet()");
    if(_to_be_returned == 0)
        return NULL_VALUE_64;
    _int64 row_to_return = _current_row;
    _to_be_returned--;
    if(_to_be_returned == 0)
        return row_to_return;
    // now prepare the next row to be returned
    _int64 startrow = _current_row;
    _current_row += _current_iterate_step;
    if(_current_row >= no_rows)
        _current_row = _current_row % no_rows;
    do {
        assert( *((int*)(t + _current_row * total_width + mult_offset)) != 0 );
        if( memcmp( t + _current_row * total_width, ibuffer, key_buf_width ) == 0 ) {
            // i.e. identical row found - keep the current_row
            return row_to_return;
        }
        else {  // some other value found - iterate one step forward
            _current_row += _current_iterate_step;
            if(_current_row >= no_rows)
                _current_row = _current_row % no_rows;
        }
    } while( _current_row != startrow );
    return row_to_return;
}

bool _less_than(const _int64 * dia1, const _int64* dia2) {
    return  *dia1< *dia2;
}
bool _more_than(const _int64 * dia1, const _int64* dia2) {
    return  *dia1> *dia2;
}

extern int SHJOIN_MPP_N;
// call this method while CanMatchDimMPP() has got true:
//   so we have make sure:
//          1.!watch_traversed
//          2.!outer_nulls_only
//          3.new_mind.OptimizedDimStay()>=0
//          4.!other_cond_exist
_int64 JoinerHash::MatchDimMPP(MINewContents &new_mind, MIIterator &mit)
{
    MEASURE_FET("JoinerHash::MatchDimMpp(...)");
    _int64 joined_tuples = 0;
    _int64 hash_row;
    _int64 no_of_matching_rows;
    mit.Rewind();
    _int64 matching_row = 0;
    // prepare for parallel join
    int max_thread=min(SHJOIN_MPP_N,8);
    int max_thdata=max_thread+3;
    DMThreadGroup match_pool(max_thread,max_thdata);
    
    IBMutex commit_lock;
    
    simple_mem_pool mpp_join_mem_pool(NoValidDimension()); // fix dma-1203

    packno_dimensions_pack_rows_array_map dim_item_list_map; // ����matched_dims�ڲ��İ���

    wait_commit_pack_info_array commit_pack_info_list; // �ȴ��ύ��pack����
        
    _int64 _mit_index = 0; 
   
    for(int i = 0; i < cond_hashed; i++)
        vc2[i]->InitPrefetching(mit,cond_hashed,max_thdata);
    // ������̲߳���Ԥ��ȡDataPack
    mit.SetPackMttLoad();
    while(mit.IsValid()) {
        if(m_conn.killed()){
            throw KilledRCException();
        }
        
        // Rough and locking part
        bool omit_this_packrow = false;
        bool packrow_uniform = false;               // if the packrow is uniform, process it massively
        //Get and check pack same as none mpp mode:
        if(mit.PackrowStarted()) {
            packrow_uniform = true;
            for(int i = 0; i < cond_hashed; i++) {
                if(jhash.StringEncoder(i)) {
                    if(!vc2[i]->Type().IsLookup()) {    // lookup treated as string, when the dictionaries aren't convertible
                        RCBString local_min = vc2[i]->GetMinString(mit);
                        RCBString local_max = vc2[i]->GetMaxString(mit);
                        if(!local_min.IsNull() && !local_max.IsNull() && jhash.ImpossibleValues(i, local_min, local_max)) {
                            omit_this_packrow = true;
                            break;
                        }
                    }
                    packrow_uniform = false;
                } else {
                    _int64 local_min = vc2[i]->GetMinInt64(mit);
                    _int64 local_max = vc2[i]->GetMaxInt64(mit);
                    if(local_min == NULL_VALUE_64 || local_max == NULL_VALUE_64 ||      // NULL_VALUE_64 only for nulls only
                        jhash.ImpossibleValues(i, local_min, local_max)) {
                        omit_this_packrow = true;
                        break;
                    }
                    if(other_cond_exist || local_min != local_max ||
                            vc2[i]->NullsPossible()) {
                        packrow_uniform = false;
                    }
                }
            }
            packrows_matched++;
            if(packrow_uniform && !omit_this_packrow) {
                for(int i = 0; i < cond_hashed; i++) {
                    _int64 local_min = vc2[i]->GetMinInt64(mit);
                    jhash.PutMatchedValue(i, local_min);
                }
                no_of_matching_rows = jhash.InitCurrentRowToGet() * mit.GetPackSizeLeft();
                if(!tips.count_only)
                    while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64) {
                        MIIterator mit_this_pack(mit);
                        _int64 matching_this_pack = matching_row;
                        do {
                            SubmitJoinedTuple(hash_row, mit_this_pack, new_mind);
                            if(watch_matched)
                                outer_filter->ResetDelayed(matching_this_pack);
                            ++mit_this_pack;
                            matching_this_pack++;
                        } while(mit_this_pack.IsValid() && !mit_this_pack.PackrowStarted());
                    }
                else if(watch_traversed) {
                    while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64)
                        outer_filter->Reset(hash_row);
                }

                joined_tuples += no_of_matching_rows;
                omit_this_packrow = true;
            }
            if(omit_this_packrow) {
                _int64 pack_size_left = mit.GetPackSizeLeft();
                matching_row += pack_size_left;

                // �����Ҫ�ύ�İ���
                commit_lock.Lock();
                wait_commit_pack_info _pack_info(_mit_index,mit.GetCurPackrow(0),mit[0],true);
                commit_pack_info_list.push_back(_pack_info);            
                commit_lock.Unlock();
                
                _mit_index++;     
                mit.NextPackrow();
                packrows_omitted++;
                continue;               // here we are jumping out for impossible or uniform packrow
            }
            if(new_mind.NoMoreTuplesPossible())
                break;                  // stop the join if nothing new may be obtained in some optimized cases
        }
        // next part move to parallel process:
        DMThreadData *ptdata=match_pool.WaitIdleAndLock();
        JoinerHashTableWorker *pworker=new JoinerHashTableWorker(&jhash,&commit_lock);
        //keep current pack and row position in pworker before launch thread
        pworker->SetPackMI(mit);
        pworker->set_mit_index(_mit_index);
        pworker->psimp_mem_pool = &mpp_join_mem_pool; // �ڴ��
        for(int i = 0; i < cond_hashed; i++){
           vc2[i]->LockSourcePacks(mit,ptdata->GetDataID());
        }

        // �����Ҫ�ύ�İ���
        commit_lock.Lock();
        wait_commit_pack_info _pack_info(_mit_index,mit.GetCurPackrow(0),mit[0],false);
        commit_pack_info_list.push_back(_pack_info);           
        commit_lock.Unlock();

        // �����߳�,������ǰ�������������ļ�¼
        _int64 rowsinpack=mit.GetPackSizeLeft();
        ptdata->StartInt(boost::bind(&JoinerHash::MatchDimInPack,
                                        this,
                                        pworker,
                                        boost::ref(dim_item_list_map),
                                        boost::ref(commit_pack_info_list),
                                        boost::ref(new_mind),
                                        boost::ref(joined_tuples),
                                        matching_row));
        
        matching_row+=rowsinpack;
        mit.NextPackrow();
        _mit_index++;
        if(tips.limit != -1 && tips.limit <= joined_tuples) 
            break;
    }

    match_pool.WaitAllEnd();

    MatchDimInList(dim_item_list_map,new_mind,&mpp_join_mem_pool);
    commit_pack_info_list.clear();  

    // ����ڴ�ص�ʹ��ͳ����
    rccontrol.lock(m_conn.GetThreadID()) <<"JoinerHash::MatchDimMPP memory pool size["
                                         <<mpp_join_mem_pool.get_max_mem_pool_size()
                                         <<"],using["
                                         <<mpp_join_mem_pool.get_using_cnt()
                                         <<"],free["
                                         <<mpp_join_mem_pool.get_free_cnt()
                                         <<"],max use num["
                                         <<mpp_join_mem_pool.get_max_use_cnt()
                                         <<"],statistics  use sum["
                                         <<mpp_join_mem_pool.get_statistics_use_sum()
                                         <<"]."
                                         <<unlock;
    
    if(watch_matched)
        outer_filter->Commit();             // commit the delayed resets

    for(int i = 0; i < cond_hashed; i++) {
        vc2[i]->UnlockSourcePacks();
        vc2[i]->StopPrefetching();
    }
    for(int j = 0; j < other_cond.size(); j++)
        other_cond[j].UnlockSourcePacks();

    if(outer_nulls_only)
        joined_tuples = 0;                  // outer tuples added later
    return joined_tuples;
}

inline void JoinerHash::SaveJoinedTuple(std::vector<_int64> &new_values, MIInpackIterator &mit,_int64 hash_row) {
    for(int i = 0; i < mind->NoDimensions(); i++) {
        if(matched_dims[i])
            new_values.push_back(mit[i]);
        else if(traversed_dims[i])
            new_values.push_back(jhash.GetTupleValue(traversed_hash_column[i], hash_row));
    }
    
}

bool JoinerHash::MatchDimInList(packno_dimensions_pack_rows_array_map &dim_item_list_map,
            MINewContents &new_mind,
            simple_mem_pool* mpp_join_mem_pool)
{
    int valid_NoDimensions = NoValidDimension(); //��Ч��ά��

    rccontrol.lock(m_conn.GetThreadID()) <<"JoinerHash::MatchDimMPP -> MatchDimInList deal ["<<dim_item_list_map.size()<<"] packs ."<< unlock;
    packno_dimensions_pack_rows_array_map_iter iter;
    while(!dim_item_list_map.empty()){
        iter = dim_item_list_map.begin();
        
        dimensions_pack_rows_array_iter iter_item = iter->second.begin();  
        for(;iter_item!=iter->second.end();iter_item++){
            
            _int64 *p_dim_item_arry = NULL; // �洢��Ч��ά��
            //p_dim_item_arry = (_int64*)(*iter_item);
            stru_mem_item* pmem_item = (stru_mem_item*)(*iter_item);
            int valid_dim_index = 0;
            for(int j = 0;j < mind->NoDimensions(); j++) {
                if(matched_dims[j]){
                    new_mind.SetNewTableValue(j,p_dim_item_arry[valid_dim_index++]);
                }else if(traversed_dims[j]){
                    new_mind.SetNewTableValue(j,p_dim_item_arry[valid_dim_index++]);
                }   

                assert(valid_dim_index<=valid_NoDimensions);
            }
            new_mind.CommitNewTableValues();

            /*
            if(p_dim_item_arry != NULL){
                delete [] p_dim_item_arry;
                p_dim_item_arry = NULL;
            }
            */
            mpp_join_mem_pool->free_item(pmem_item);
        }   
        
        iter->second.clear();

        dim_item_list_map.erase(iter);
    }

    return true;
}

int JoinerHash::MatchDimInPack(JoinerHashTableWorker *pworker,
                                packno_dimensions_pack_rows_array_map &dim_item_list_map,
                                wait_commit_pack_info_array& commit_pack_info_list,
                                MINewContents &new_mind,_int64 &joined_tuples,
                                _int64 matching_row)
{
    _int64 hash_row;
    _int64 no_of_matching_rows;
    // Exact part - make the key row ready for comparison
    bool null_found = false;
    _int64 _joined_tuples = 0;  
    MIInpackIterator &mit(pworker->GetPackMI());
    int curpack=mit.GetCurPackrow(0);
    int64 cur_pos=matching_row;
    mytimer packtm;
    packtm.Start();
    std::vector<_int64> outer_filter_reset_values;  
    MIDummyIterator combined_mit(mind);     // a combined iterator for checking non-hashed conditions, if any
    do {
        null_found = false; // fix dma-1205
        for(int i = 0; i < cond_hashed; i++) {
            if(vc2[i]->IsNull(mit)) {
                null_found = true;
                break;
            }
            pworker->PutMatchedValue(i, vc2[i], mit);
        }

        if(!null_found ) {  // else go to the next row - equality cannot be fulfilled
            no_of_matching_rows = pworker->InitCurrentRowToGet();

            // Find all matching rows
            // Basic case - just equalities
            if(!tips.count_only){
                while((hash_row = pworker->GetNextRow()) != NULL_VALUE_64){
                   SaveJoinedTuple(pworker->NewValue(),mit,hash_row);  // use the multiindex iterator position

                   if(watch_matched){//try to fix dma-1220
                       // outer_filter->ResetDelayed(cur_pos);
                       outer_filter_reset_values.push_back(cur_pos);
                   }
                }
            }
            
            _joined_tuples += no_of_matching_rows;
        }
        ++mit;
        cur_pos++;
    } while(mit.IsValid() && !mit.PackrowStarted());
    
    if(_joined_tuples>0||cur_pos>matching_row) {
        pworker->Lock();
        
        bool need_storage_to_commit = true;
        joined_tuples+=_joined_tuples;
        if(!tips.count_only) {
            const std::vector<_int64> &new_values(pworker->NewValue());
            
            int valid_NoDimensions = NoValidDimension(); //��Ч��ά��
            int vsize=new_values.size();
            assert(vsize%valid_NoDimensions == 0);
            
            while(!commit_pack_info_list.empty()){// ����Ҫ���л���,ֱ���ύ����,ÿ��ֻ����һ����

                if(commit_pack_info_list.front().is_omit){
                    commit_pack_info_list.pop_front();
                    continue; // ���Եİ�,����Ҫ���еȴ��ύ
                }     
                
                int wait_commit_mit_index = commit_pack_info_list.front().mit_index;
                
                if(vsize == 0){ // �����û���ҵ�ƥ������ݼ�¼,ֱ������,��Ч�ύ
                    if(pworker->get_mit_index() == wait_commit_mit_index){
                        commit_pack_info_list.pop_front();
                    }else{ // ���һ���յĽ���
                        LABEL_ADD_EMPTY_NEW:
                        dimensions_pack_rows_array dim_item_list;
                        dim_item_list_map.insert(std::pair<int,dimensions_pack_rows_array>(
                                pworker->get_mit_index(),dim_item_list));
                    }
                    break; // ���Եİ�,����Ҫ���еȴ��ύ
                }

                
                if(wait_commit_mit_index == pworker->get_mit_index()){ // �ȴ��ύ���뵱ǰ����ͬ����Ҫ���л���,ֱ���ύ����
                    for(int i=0;i<vsize;) {
                        for(int j = 0;j < mind->NoDimensions(); j++){
                            if(matched_dims[j]){
                                new_mind.SetNewTableValue(j,new_values[i++]);
                            }else if(traversed_dims[j]){
                                new_mind.SetNewTableValue(j,new_values[i++]);
                            }   
                        }
                        new_mind.CommitNewTableValues(); 
                    }                      
                    need_storage_to_commit = false;
                    commit_pack_info_list.pop_front();
                    break;
                }else{// ��ͬ�İ�,����ֱ���ύ,��Ҫ����
                    need_storage_to_commit = true;
                    break;
                }
            }

            if(need_storage_to_commit){// ����ֱ���ύ,��Ҫ����
                if(vsize > 0){
                    // ��ȡ����map�İ���
                    packno_dimensions_pack_rows_array_map_iter iter;
                    iter = dim_item_list_map.find(pworker->get_mit_index());
                    assert(iter == dim_item_list_map.end());

                    // ��ȡ����map�İ��Ŷ�Ӧ���к��б�
                    dimensions_pack_rows_array dim_item_list;
                    for(int i=0;i<vsize;) {
                        _int64 *p_dim_item_arry = NULL; // �洢��Ч��ά��
                        // p_dim_item_arry = new _int64[valid_NoDimensions]; 
                        stru_mem_item* p_mem_item = pworker->psimp_mem_pool->alloc_item();
                        if(p_mem_item == NULL){
                            rccontrol.lock(m_conn.GetThreadID()) << "simple_mem_pool alloc_item() return NULL ." << unlock;
                            return 0;
                        }
                        p_dim_item_arry = p_mem_item->new_value;
                        int valid_dim_index = 0;
                        for(int j = 0;j < mind->NoDimensions(); j++) {
                            if(matched_dims[j]){
                                p_dim_item_arry[valid_dim_index++] = new_values[i++];
                            }else if(traversed_dims[j]){
                                p_dim_item_arry[valid_dim_index++] = new_values[i++];
                            }             

                            assert(valid_dim_index <= valid_NoDimensions);
                        }

                        //dim_item_list.push_back(p_dim_item_arry);
                        dim_item_list.push_back(p_mem_item);
                    }

                    // ����map<pack_no,dimensions_pack_rows_array>
                    dim_item_list_map.insert (std::pair<int,dimensions_pack_rows_array>(
                        pworker->get_mit_index(),dim_item_list));
                }else{
                    // ����ǿյļ�¼,�Ѿ��ڱ�ǩ:
                    // LABEL_ADD_EMPTY_NEW 
                    // �������,������д���
                }
           }
        }

        // ���ְ������ύ�ϲ�
        if(!dim_item_list_map.empty() && !commit_pack_info_list.empty()){
            CommitJoinedTuple(dim_item_list_map,commit_pack_info_list,new_mind,pworker->psimp_mem_pool);
        }

        // Reset Outer_filter
        for(int i=0;i<outer_filter_reset_values.size();i++){
            outer_filter->ResetDelayed(outer_filter_reset_values[i]);
        }
        outer_filter_reset_values.clear();

        pworker->Unlock();
    }
    
    delete pworker;
    packtm.Stop();
    
    return cur_pos-matching_row;
}

int JoinerHash::NoValidDimension(){
    int valid_NoDimensions = 0; //��Ч��ά��

    for(int i = 0; i < mind->NoDimensions(); i++) {
        if(matched_dims[i]){
            valid_NoDimensions++;
        }else if(traversed_dims[i]){
            valid_NoDimensions++;
        } 
    }
    assert(valid_NoDimensions<=mind->NoDimensions());  
    return valid_NoDimensions;
}

bool JoinerHash::CanMatchDimMPP(MINewContents &new_mind) {
    for(int i = 0; i < cond_hashed; i++){ // fix dma-1216
        if(vc2[i]->IsExpressionColumn()){ // ���ʽ����,��֧�ֶ��̲߳���join����
            if(rccontrol.isOn()) {
                for(int j=0;j<vc2[i]->GetVarMap().size();j++){
	     			int col_ndx = vc2[i]->GetVarMap()[j].col_ndx;
         	        std::string tabname;
             	    JustATable* _ptab = vc2[i]->GetVarMap()[j].GetTabPtr().get();
					if(_ptab->TableType() == RC_TABLE){
                	    tabname = ((RCTableImpl*)_ptab)->GetPath();
                	}else{
                    	tabname = "TempTable";
                	}
					if(can_match_dim_mpp_log){
						rccontrol.lock(m_conn.GetThreadID()) <<"Table ["<<tabname.c_str()<<"] column ["<<col_ndx<<"] is ExpressionColumn , do not support CanMatchDimMPP."<< unlock;
                        can_match_dim_mpp_log = false;
            		}
				}
			}
            return false;
        }
    }// end fix dma-1216
    return !watch_traversed&&!outer_nulls_only&&!other_cond_exist;
}

// �ú��������̰߳�ȫ��,ֻ����MatchDimInPack��MatchDimMPP �ڲ�����ʹ��
/*
ʵ��˼·:
      dim_item_list_map : 
                    �ڲ��洢ÿһ��Ԫ��Ϊ��,
                    {
                        key----> [ mit_index  <---- pworker->get_mit_index()]
                        value----->[tab_a.row][tab_b.row]...[tab_z.row]
                    }
      commit_pack_info_list:
                    �ڲ��洢�Ѿ������̵߳İ�����Ϣ(���ȴ��ύ�İ�����Ϣ),����[mit_index,pack_no,pack_pos,omit]

��������: 
      2. ��commit_pack_info_list��ͷ��ʼ����,���ݰ��Ž�dim_item_list_map��Ԫ��ȡ�������������ύ��new_mind.
*/
bool JoinerHash::CommitJoinedTuple(
                packno_dimensions_pack_rows_array_map &dim_item_list_map,
                wait_commit_pack_info_array &commit_pack_info_list,
                MINewContents &new_mind,
                simple_mem_pool* mpp_join_mem_pool)
{

    _int64 dim_list_size = dim_item_list_map.size();

    int valid_NoDimensions = NoValidDimension(); //��Ч��ά��
    bool exit_commit = false;
    bool wait_pack_has_process = false;
    while(!commit_pack_info_list.empty()&& !dim_item_list_map.empty()){    

        if(commit_pack_info_list.front().is_omit){
            commit_pack_info_list.pop_front();
            continue; // ���Եİ�,����Ҫ���еȴ��ύ
        }        

        // ���ύ����
        int wait_commit_mit_index = commit_pack_info_list.front().mit_index;
        
        wait_pack_has_process = false; 

        packno_dimensions_pack_rows_array_map_iter dimensions_packno_begin_iter = dim_item_list_map.begin();

        if(wait_commit_mit_index == dimensions_packno_begin_iter->first){ // ��Ӧ�İ���,��Ҫ�ύ     
            dimensions_pack_rows_array_iter iter;

            int debug_delete_num = 0;  // ����

            // dimensions_packno_iter->second Ϊ:dimensions_pack_rows_array
            for(iter = dimensions_packno_begin_iter->second.begin();
                        iter!= dimensions_packno_begin_iter->second.end();iter++){
              
                _int64 *p_dim_item_arry = NULL; // �洢��Ч��ά��
                //p_dim_item_arry = (_int64 *)(*iter);
                stru_mem_item* pmem_item = (stru_mem_item*)(*iter);
                p_dim_item_arry = pmem_item->new_value;

                int valid_dim_index = 0;
                for(int j = 0;j < mind->NoDimensions(); j++) {
                    if(matched_dims[j]){
                        new_mind.SetNewTableValue(j,p_dim_item_arry[valid_dim_index++]);
                    }else if(traversed_dims[j]){
                        new_mind.SetNewTableValue(j,p_dim_item_arry[valid_dim_index++]);
                    }   

                    assert(valid_dim_index<=valid_NoDimensions);
                }
                new_mind.CommitNewTableValues();

                /*
                if(p_dim_item_arry != NULL){
                    delete [] p_dim_item_arry;
                    p_dim_item_arry = NULL;
                }
                */
                mpp_join_mem_pool->free_item(pmem_item);

                debug_delete_num++; 
            }   

            // ���ڵȴ��ύ�����ݰ��Ѿ��ύ�����
            wait_pack_has_process = true;

            // ���dimensions_packno_iter->secondΪ:dimensions_pack_rows_array
            dimensions_packno_begin_iter->second.clear();

            // �����һ���Ѿ������Ԫ��
            dim_item_list_map.erase(dimensions_packno_begin_iter);
        }else{
            exit_commit = true; // ���Ų���,����Ҫ�ύ
            break;
        }
        
        if(wait_pack_has_process){ // ���ڵȴ��ύ�����ݰ��Ѿ��ύ�����,������һ����
            commit_pack_info_list.pop_front();
            continue;
        }        

        if(exit_commit){
            break;
        }
    }

    return true;
}
    
    


