#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "ThreadList.h"
#include <vector>
#include <string>
#include <iostream>
using namespace std;

int	g_threadExit = 0;  //线程数
pthread_mutex_t         mutex = PTHREAD_MUTEX_INITIALIZER;
int Start(void *ptr);
void* ThreadProc(void* ptr);  // 线程函数

// 查询条件队列
int g_column_type = 0;
std::vector<std::string>  g_test_condition_value_vec;
std::vector<long>  g_test_long_condition_value_vec;

/*
调用方式：
./wdbi_thread_test dsn user pwd dbname,tbname,tsttimes
dsn  ：数据源(data source name)
user : 连接数据库用户名
pwd  : 连接数据库密码
dbname : 数据库名称
tbname : 表名称
tsttimes   ：要进行导入表的数据文件 
*/
typedef struct _InitInfo
{
    char dsn[128];
    char user[128];
    char pwd[128];
    char srcdbname[64];
    char srctbname[64];
    char dbname[64];
    char tbname[64];
    char  colname[64]; // 查询条件列
    int  startitems; // 开始行
    int  items;  // colname中的值数目
    int  threads;
}_InitInfo,*_InitInfoPtr;

typedef struct _ThreadParam{
	_InitInfo _st;
	std::vector<std::string>  _test_condition_value_vec;
	std::vector<long>  _test_long_condition_value_vec;	
}_ThreadParam,*_ThreadParamPtr;

int main(int argc,char *argv[])  
{                   
    if(argc != 12){
        printf("performance_test:调用参数输入错误.\n请参考:./performance_test dsn user pwd srcdbname srctbname dbname tbname colname startitems items threads \n");
        std::cout<<"performance_test:调用参数说明：\n"
        	<<"sdn:		"<<"odbc连接数据源\n"
        	<<"user:	"<<"数据库用户名\n"
        	<<"pwd:		"<<"数据库密码\n"
        	<<"srcdbname:	"<<"查询条件数据库实例名称\n"
        	<<"srctbname:	"<<"查询条件数据库表名称\n"
        	<<"dbname:	"<<"性能查询数据库实例名称\n"
        	<<"tbname:	"<<"性能查询数据库表名称\n"
        	<<"colname:	"<<"查询条件指定列\n"
        	<<"startitems:	"<<"查询条件测试开始检索行\n"
        	<<"items:	"<<"查询条件测试记录条数\n"
        	<<"threads:	"<<"并发线程测试数"<<endl;
        	
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
    
    strcpy(stInitInfo.srcdbname,argv[index++]);
    strcpy(stInitInfo.srctbname,argv[index++]);
    
    strcpy(stInitInfo.dbname,argv[index++]);
    strcpy(stInitInfo.tbname,argv[index++]);
    
    strcpy(stInitInfo.colname,argv[index++]);
    
    stInitInfo.startitems = atoi(argv[index++]);
    stInitInfo.items = atoi(argv[index++]);
    stInitInfo.threads = atoi(argv[index++]);
    int nRetCode = 0;
    WOCIInit("Performance/");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    nRetCode=wociMainEntrance(Start,true,(void*)&stInitInfo,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) 
{ 
	_InitInfoPtr pobj = (_InitInfoPtr)ptr;	


    // 获取查询条件
    AutoHandle dtd;
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));	
    AutoMt mt_condit(dtd,1000000);
    mt_condit.FetchFirst("select distinct %s from %s.%s where %s is not null limit %d,%d",
    	pobj->colname,pobj->srcdbname,pobj->srctbname,pobj->colname,pobj->startitems,pobj->items);
    	
    int cnt = mt_condit.Wait();
    if(cnt<=0){
       lgprintf("select distinct %s from %s.%s where %s is not null limit %d,%d ---> return <=0",
    	pobj->colname,pobj->srcdbname,pobj->srctbname,pobj->colname,pobj->startitems,pobj->items);
	   return 0;
    }

    // 查询条件获取
    g_column_type = mt_condit.getColType(0);
    for(int i=0;i<cnt;i++){
    	switch(g_column_type){
    	  case COLUMN_TYPE_CHAR:
    	  {
          	std::string _value_item = std::string(mt_condit.PtrStr(0,i));
          	g_test_condition_value_vec.push_back(_value_item);
          	break;
          }	
    	  case COLUMN_TYPE_INT:
    	  case COLUMN_TYPE_BIGINT:
    	  {
    	  	long _value_item = *mt_condit.PtrInt(0,i);
    	  	g_test_long_condition_value_vec.push_back(_value_item);
    	  	break;
    	  }
          default: 
          	lgprintf("column type err.\n");	
          	return -1;
    	}
    }
  
	
    pthread_t  *thread_array = new pthread_t[pobj->threads];
    
    // 将数据分散到线程对象中
    _ThreadParam *thread_param = new _ThreadParam[pobj->threads];
    int step_item = cnt/pobj->threads;
    for(int j = 0;j<pobj->threads;j++)
    {
    	memcpy(&thread_param[j]._st,pobj,sizeof(_InitInfo));
	    if (g_column_type == COLUMN_TYPE_CHAR){
	    	for(int i=0;i<step_item;i++){
	    		thread_param[j]._test_condition_value_vec.push_back(g_test_condition_value_vec.back());
	    		g_test_condition_value_vec.pop_back();	    		
	    	}    	
	    }
	    else{
	    	for(int i=0;i<step_item;i++){    	
	    		thread_param[j]._test_long_condition_value_vec.push_back(g_test_long_condition_value_vec.back());
	    		g_test_long_condition_value_vec.pop_back();	    		
	    	}
	    }
	}
    
    
    g_threadExit = 0;
    for (int i=0;i<pobj->threads;i++)
    {
        pthread_create(&thread_array[i],NULL,ThreadProc,&thread_param[i]);
    }
    
    while(g_threadExit!=pobj->threads)
    {
        sleep(1);
        continue;	
    }  
        
    delete [] thread_array;
    thread_array = NULL;

    return 1;
}


// 线程处理函数，进行压缩解压缩测试
void* ThreadProc(void* ptr)
{
	_ThreadParamPtr pobj_init = (_ThreadParamPtr)ptr;
   	_InitInfoPtr pobj = (_InitInfoPtr)(&pobj_init->_st);	
    AutoHandle dtd;
	
    //--1. 连接目标数据库,同一个进程中，多个线程建立连接需要防止并发
    int   rc;
    rc = pthread_mutex_lock(&mutex);
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    rc = pthread_mutex_unlock(&mutex);

	
    //--2. 构建查询语句
    AutoMt mt(dtd,1000000);
    int sleep_cnt = 0;

    while(1)
   {
	   	// 获取查询条件值
	   	sleep_cnt ++;
		if(sleep_cnt % 173 == 0){
			sleep(1);
		}
		
	     std::string str_value;
	     long int_value;
	   	 if(g_column_type == COLUMN_TYPE_CHAR){
			 if(!pobj_init->_test_condition_value_vec.empty()){
		   	 	str_value = pobj_init->_test_condition_value_vec.back();
				 pobj_init->_test_condition_value_vec.pop_back();
				 
			 }
			 else{			 	 
				 break;
			 }
		 }
		 else{
		     if(!pobj_init->_test_long_condition_value_vec.empty()){
		   	 	int_value = pobj_init->_test_long_condition_value_vec.back();
				 pobj_init->_test_long_condition_value_vec.pop_back();				 
			 }
			 else{			 	 
				 break;
			 }
		 	
		}
	
		// 对查询条件进行查询
		mytimer tm;
		tm.Start();
		if(g_column_type == COLUMN_TYPE_CHAR){
	    	mt.FetchFirst("select * from %s.%s where %s='%s' ",pobj->dbname,pobj->tbname,pobj->colname,str_value.c_str());
	    }
	    else{
	    	mt.FetchFirst("select * from %s.%s where %s=%d ",pobj->dbname,pobj->tbname,pobj->colname,int_value);
	    }
	    int rn = mt.Wait();
		tm.Stop();
		
		if(g_column_type == COLUMN_TYPE_CHAR){
	    	lgprintf("======threadid[%lu],[%s='%s'] ---> fetch rows [%d], user time %0.2f sec. \n",
		 	 	pthread_self(),pobj->colname,str_value.c_str(),rn,tm.GetTime());
		}
		else{
			lgprintf("======threadid[%lu],[%s='%d'] ---> fetch rows [%d], user time %0.2f sec. \n",
		 	 	pthread_self(),pobj->colname,int_value,rn,tm.GetTime());
		}
    }
    g_threadExit++;  
    return NULL;
}
