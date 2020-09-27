#include<limits.h>
#include<stdarg.h>
#include <stdio.h>
#include "bloom_test.h"
#include "hash.h"
#include <string.h>
#include "AutoHandle.h"

#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))

#define CONVENT_STR(a)   #a
#define HASH_FUNC_NUM  12          // 必须为g_hashFuncArray的个数
StruHashFuncItem g_hashFuncArray[]={
	{RSHash,CONVENT_STR(RSHash)},
	{JSHash,CONVENT_STR(JSHash)},
	{PJWHash,CONVENT_STR(PJWHash)},
	{ELFHash,CONVENT_STR(ELFHash)},
	{BKDRHash,CONVENT_STR(BKDRHash)},
	{SDBMHash,CONVENT_STR(SDBMHash)},
	{DJBHash,CONVENT_STR(DJBHash)},
	{DEKHash,CONVENT_STR(DEKHash)},
	{BPHash,CONVENT_STR(BPHash)},
	{FNVHash,CONVENT_STR(FNVHash)},
	{APHash,CONVENT_STR(APHash)},
	{JENKINSHash,CONVENT_STR(JENKINSHash)}
};

bloom_test::bloom_test(size_t size)
{
	  m_res_fun = NULL;
	  m_arr_fun = NULL;
	  data = NULL;
	  asize = size;
	  m_comb_num = 0;
	  pos_fun = 0;
}

bloom_test::~bloom_test()
{
  if(m_res_fun != NULL)
  {
  	free(m_res_fun);
  	m_res_fun = NULL;
  }
  
  if(m_arr_fun != NULL)
  {
     free(m_arr_fun);
     m_arr_fun = NULL;
  }
}

int bloom_test::write(const char *filename) 
{
	FILE *fp=fopen(filename,"w+b");
	fwrite(data,(asize+CHAR_BIT-1)/CHAR_BIT,1,fp);
	fclose(fp);
}


// 组合查找
int bloom_test::CombinationTest(const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int iSelectNum)
{
	 // 组合的时候用
	 if(m_res_fun != NULL)
	 {
	 	free(m_res_fun);
	 	m_res_fun = NULL;
	 }	 
	 m_comb_num = iSelectNum;
	 m_res_fun = (hashfunc_t*)malloc(m_comb_num*sizeof(hashfunc_t));
	     
	 // 全局用的
	 if(m_arr_fun != NULL)
	 {
	 	free(m_arr_fun);
	 	m_arr_fun = NULL;
	 }
	 m_arr_fun = (hashfunc_t*)malloc(HASH_FUNC_NUM*sizeof(hashfunc_t));
     for (int i = 0;i<HASH_FUNC_NUM;i++)
     {
	     m_arr_fun[i] = g_hashFuncArray[i].func;
     }        
     
	 pos_fun = 0;
	 int start = 0;
	 // 进行递归测试
	 fun_Combination(m_arr_fun,HASH_FUNC_NUM,start,pos_fun,
	          pSourceData,sourceClen,sourceRows,
	          pFindData,findClen,findRows,
	          m_comb_num);
	 
	 return 0;	
}


void bloom_test::fun_Combination(hashfunc_t* arr_fun,int arr_size,int start,int pos_fun,
                      const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int comNum)
{
	int i,j,n;
	// 组合数是对的情况
	if(pos_fun==comNum)
	{
	    // 日志
	    char *pLogBuff = (char*)malloc(256);
	    memset(pLogBuff,0x0,256);
	    
		// 输出组合数名称
		sprintf(pLogBuff,"--> %d hash : ",comNum);
		for (j = 0;j<HASH_FUNC_NUM;j++)
		{
            for (i=0;i<comNum;i++)
            {
			   if (m_res_fun[i] == g_hashFuncArray[j].func)
			   {
			      sprintf(pLogBuff+strlen(pLogBuff),"%s , ",g_hashFuncArray[j].pFuncName);
			   }
            }
		}
		strcat(pLogBuff,"--->");
		        
       // 清空数据 	
       if(data != NULL)
	   {
	       free(data);
	       data = NULL;
	   }
	   if(!(data=(unsigned char *)calloc((asize+CHAR_BIT-1)/CHAR_BIT, sizeof(char)))) 
	   {
		   throw "alloc memory failed!";
	   }
	   memset(data,0x0,(asize+CHAR_BIT-1)/CHAR_BIT);
	  
      // ADD 
      const char *paddbuff = pSourceData;
      int buffPos = 0;
      for(i=0;i<sourceRows;i++) 
      {
           for(n=0; n<comNum;++n) 
           {
           	    unsigned int hv=m_res_fun[n](paddbuff + buffPos,strlen(paddbuff + buffPos))%asize;
           	    SETBIT(data, hv);
           }
    	   buffPos += sourceClen;
      }                
      
      write("bf_test.data");
        
      // CHECK
      const char *pcheckbuff = pFindData;
      int failct=0;
      int findTimes = 0;  // 找到次数
      
      buffPos = 0;
      for(i=0;i<findRows;i++) 
      {    	
      	  findTimes = 0;
      	  for(n=0; n<comNum; ++n) 
      	  {
               unsigned int hv=m_res_fun[n](pcheckbuff+buffPos,strlen(pcheckbuff+buffPos))%asize;
               if(!(GETBIT(data, hv))) 
               {
               	  // 其中1次没有找到,就不算找到
                  break;
               }
               findTimes++;
          }    	
          
          // 所有组合数都找的了，才算找到
          failct += (findTimes==comNum)?1:0;		
      	  
      	  buffPos+=findClen;
      }
      sprintf(pLogBuff+strlen(pLogBuff)," Failed %d,frate %.4f%%.",failct,(double)failct*100/findRows);
      lgprintf(pLogBuff);
      
      free(pLogBuff);
      pLogBuff = NULL;
      
      return;
    }  
  
    for(i=start;i<=arr_size-comNum+pos_fun;i++)
    {
    	m_res_fun[pos_fun] = arr_fun[i];
    	fun_Combination(arr_fun,arr_size,i+1,pos_fun+1,
    	      pSourceData,sourceClen,sourceRows,
	          pFindData,findClen,findRows,
    	      comNum);
    }
}	
