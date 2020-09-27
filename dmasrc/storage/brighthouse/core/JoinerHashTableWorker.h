#ifndef JOINERHASHTABLEWORKER_H_INCLUDED
#define JOINERHASHTABLEWORKER_H_INCLUDED
#include <vector>
#include "JoinerHashTable.h"
#include "MIIterator.h"
#include <stdlib.h>
//for thread safe table using
class simple_mem_pool;
class JoinerHashTableWorker {
	unsigned char*		input_buffer;
	int 				key_buf_width;
	_int64				current_row;	// row value to be returned by the next GetNextRow() function
	_int64				to_be_returned;	// the number of rows left for GetNextRow()
	_int64				current_iterate_step;
	IBMutex  			*commit_lock;
	std::vector<_int64> new_values;
	MIInpackIterator mi_inpack;

protected: 
    //>> begin: fix dma-1204,dma-1205,dma-1215
    _int64 mit_index;
public:
    void set_mit_index(_int64 _mit_index){
        mit_index = _mit_index;
    }
    _int64 get_mit_index(){
        return mit_index;
    }
    simple_mem_pool     *psimp_mem_pool;
    //<< end: fix dma-1204,dma-1205
    
public:
	JoinerHashTable *pjoiner;

	JoinerHashTableWorker(JoinerHashTable *pj,IBMutex *lock):commit_lock(lock),pjoiner(pj) {
		key_buf_width=pj->KeyBufWidth();
		input_buffer=(unsigned char*)malloc(key_buf_width);
		Init();
	}

	void Lock() {
		commit_lock->Lock();
	}
	
	void Unlock() {
		commit_lock->Unlock();
	}

	void Init() {
        mit_index = 0;
		new_values.clear();
		current_row=0;
		to_be_returned=0;
		current_iterate_step=0;
		memset(input_buffer,0,key_buf_width);     
	}

	void SetPackMI(MIIterator &mi) {
		mi_inpack=mi;
	}

	MIInpackIterator &GetPackMI() {
		return mi_inpack;
	}

	void AddNewValue(_int64 v) {
		new_values.push_back(v);
	}

	std::vector<_int64> &NewValue() {
		return new_values;
	}
    
	int NewValueSize() {
		return new_values.size();
	}

	virtual ~JoinerHashTableWorker() {
		free(input_buffer);
	}
	
	void PutMatchedValue(int col, VirtualColumn *vc, MIIterator &mit)			// for all values EXCEPT NULLS
	{
		pjoiner->PutMatchedValue(input_buffer,col,vc,mit);
	}

	void PutMatchedValue(int col, _int64 v)			// for constants
	{
		pjoiner->PutMatchedValue(input_buffer,col,v);
	}

	_int64 InitCurrentRowToGet() {
		return pjoiner->InitCurrentRowToGet(input_buffer,current_row,to_be_returned,current_iterate_step);
	}

	_int64 GetNextRow() {
		return pjoiner->GetNextRow(input_buffer,current_row,to_be_returned,current_iterate_step);
	};
};


//>> begin to fix DMA-1203
typedef struct stru_mem_item{// 单个内存节点
    stru_mem_item* next;
    _int64 *new_value;
    stru_mem_item(){
        next = NULL;
        new_value = NULL;
    }
}stru_mem_item,*stru_mem_item_ptr;

class simple_mem_pool{// 内存池实现
    public:
        simple_mem_pool(const int _valid_no_dimensions,
                        const int _max_mem_items = 1310720/* 65536 * 20  */)
        {
            phead = NULL;    
            ptail = NULL;
            pmem_block = NULL;
            max_mem_items = _max_mem_items;
            valid_no_dimensions = _valid_no_dimensions;
            pmem_block = new stru_mem_item[max_mem_items];
            assert(pmem_block != NULL);

            phead = &pmem_block[0];            
            for(int i=0;i<max_mem_items;i++){
                pmem_block[i].new_value = new _int64[valid_no_dimensions];
                if(i != max_mem_items-1){
                    pmem_block[i].next = &pmem_block[i+1];
                }else{
                    pmem_block[i].next = NULL;
                    ptail = &pmem_block[i];
                }
            }
            free_item_cnt = _max_mem_items;       
            use_item_cnt = 0;
            statistics_use_sum = 0;
            max_use_item_cnt = 0;
            max_mem_items = _max_mem_items; 
        }
        
        virtual ~simple_mem_pool(){
            if(pmem_block != NULL){
                for(int i=0;i<max_mem_items;i++){
                    if(pmem_block[i].new_value != NULL){
                        delete [] pmem_block[i].new_value;
                        pmem_block[i].new_value = NULL;
                    }                    
                }                
                delete [] pmem_block;
                pmem_block = NULL;
            }
            free_item_cnt = 0;
            use_item_cnt = 0;
            max_mem_items = 0;
            statistics_use_sum = 0;
            max_use_item_cnt= 0;
        }

        stru_mem_item* alloc_item(){ // 从链表头部取
            this->mem_pool_lock.Lock();    
            if(this->use_item_cnt == this->max_mem_items){
                this->mem_pool_lock.Unlock();                 
                return NULL;
            }
            stru_mem_item* pnode = this->phead;
            this->phead = this->phead->next;
            this->use_item_cnt++; 
            this->free_item_cnt--; 
            this->statistics_use_sum++;
            if(max_use_item_cnt < use_item_cnt){
                max_use_item_cnt = use_item_cnt;
            }
            this->mem_pool_lock.Unlock(); 
            
            return pnode;
        }
        void free_item(stru_mem_item* pitem){ // 返回链表头部
            mem_pool_lock.Lock(); 
            pitem->next = phead;
            phead = pitem;            
            this->free_item_cnt++;    
            this->use_item_cnt--;  
            mem_pool_lock.Unlock(); 
        }

        inline _int64 get_statistics_use_sum(){ return this->statistics_use_sum;}
        inline int get_max_mem_pool_size(){ return this->max_mem_items;}
        inline int get_using_cnt(){ return this->use_item_cnt;}
        inline int get_free_cnt(){ return this->free_item_cnt;}
        inline int get_max_use_cnt(){ return this->max_use_item_cnt;}
        
    protected:
        IBMutex        mem_pool_lock;
        stru_mem_item* phead;  
        stru_mem_item* ptail; 
        int    max_mem_items;            // 内存池最大节点数
        int    valid_no_dimensions;      // 有效的维度
        _int64 statistics_use_sum;       // 累计分配次数
        int    free_item_cnt;            // 空闲的
        int    use_item_cnt;             // 在使用的  
        int    max_use_item_cnt;         // 内存池最大使用数
        
        stru_mem_item  *pmem_block;      // 整块内存 

};
//<< end to fix DMA-1203

#endif


