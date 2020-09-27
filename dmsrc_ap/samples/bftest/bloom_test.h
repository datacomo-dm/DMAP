#ifndef __BLOOM_TEST_H__
#define __BLOOM_TEST_H__

// create by liujs
#include <stdlib.h>

typedef unsigned int (*hashfunc_t)(const char *,unsigned int len);


typedef struct StruHashFuncItem
{
	hashfunc_t func;
	char *pFuncName;
}StruHashFuncItem;

class bloom_test 
{
protected:
  	int m_comb_num; // 选择组合数
    int pos_fun;  // 移动的位置
    hashfunc_t* m_res_fun;  // hash 函数
    hashfunc_t* m_arr_fun;  // 数组11个函数的
	size_t asize;
	unsigned char *data;
    
    // 组合测试函数，递归实现
    void fun_Combination(hashfunc_t* arr_fun,int arr_size,int start,int pos_fun,
                      const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int comNum);	
public:
  	bloom_test(size_t size);
  	virtual ~bloom_test();
    int write(const char *filename);
  	// 测试
    int CombinationTest(const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int iSelectNum);
};

#endif

