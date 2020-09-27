#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "wdbi_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int	g_threadExit = 0;  //线程数
pthread_mutex_t         mutex = PTHREAD_MUTEX_INITIALIZER;
int Start(void *ptr);
void* ThreadProc(void* ptr);  // 线程函数

void* Start1(void *ptr);
int ThreadProc1(void* ptr);  // 线程函数


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
    char dbname[64];
    char tbname[64];
    int  tsttimes;	
    int  threads;
    char sql[10240];
}_InitInfo,*_InitInfoPtr;


int main(int argc,char *argv[])  
{                   
    if(argc != 8 && argc !=7){
        printf("wdbi_thread_test:调用参数输入错误.\n请参考:./wdbi_thread_test dsn user pwd times threads dbname tbname\n");
	    printf("wdbi_thread_test:调用参数输入错误.\n请参考:./wdbi_thread_test dsn user pwd times threads sql \n");
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
    stInitInfo.tsttimes = atoi(argv[index++]);
    stInitInfo.threads = atoi(argv[index++]);
    stInitInfo.sql[0]=0;
    if(argc == 7){
        strcpy(stInitInfo.sql,argv[index++]);
    }else{
        strcpy(stInitInfo.dbname,argv[index++]);
	    strcpy(stInitInfo.tbname,argv[index++]);
	}
    int nRetCode = 0;
    WOCIInit("wdbi_thread_test");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    
    g_threadExit = 0;
    //pthread_t  *thread_array = new pthread_t[stInitInfo.threads];
    for (int i=0;i<stInitInfo.threads;i++)
    {
        pthread_t xx; 
        pthread_create(&xx,NULL,Start1,(void*)&stInitInfo);
        //pthread_create(&thread_array[i],NULL,Start1,(void*)&stInitInfo);
    }
    
    while(g_threadExit!=stInitInfo.threads)
    {
        sleep(1);
        continue;	
    }  
    
    WOCIQuit(); 
    return nRetCode;
}

void * Start1(void *ptr) 
{ 
    _InitInfoPtr pobj = (_InitInfoPtr)ptr;	

    int nRetCode=wociMainEntranceEx(ThreadProc1,false,(void*)pobj,2,0);
    //int nRetCode=wociMainEntrance(ThreadProc1,true,(void*)pobj,2);

    return NULL;
}

// 线程处理函数，进行压缩解压缩测试
int  ThreadProc1(void* ptr)
{
   	_InitInfoPtr pobj = (_InitInfoPtr)ptr;	
    AutoHandle dtd;
	//--1. 连接目标数据库,同一个进程中，多个线程建立连接需要防止并发
    int   rc;
    rc = pthread_mutex_lock(&mutex);
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    rc = pthread_mutex_unlock(&mutex);
    
    //--2. 构建查询语句
    AutoMt mt(dtd,100000);
    
    for(int i=0;i<pobj->tsttimes;i++)
    {
       if(strlen(pobj->sql) ==0){
           mt.FetchFirst("select * from %s.%s limit 100000",pobj->dbname,pobj->tbname);
       }else{
           mt.FetchFirst(pobj->sql);    
       }
       int rn = mt.Wait();
       lgprintf("threadid[%lu],test times[%d],fetch rows [%d]\n",pthread_self(),i,rn);
    }
    g_threadExit++;  
    return 0;
}


int main1(int argc,char *argv[])  
{                   
    if(argc != 8){
        printf("wdbi_thread_test:调用参数输入错误.\n请参考:./wdbi_thread_test dsn user pwd dbname tbname times threads");
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
    strcpy(stInitInfo.dbname,argv[index++]);
    strcpy(stInitInfo.tbname,argv[index++]);
    stInitInfo.tsttimes = atoi(argv[index++]);
    stInitInfo.threads = atoi(argv[index++]);
    int nRetCode = 0;
    WOCIInit("wdbi_thread_test");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    nRetCode=wociMainEntrance(Start,true,(void*)&stInitInfo,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) 
{ 
	_InitInfoPtr pobj = (_InitInfoPtr)ptr;	
    pthread_t  *thread_array = new pthread_t[pobj->threads];
    g_threadExit = 0;
    for (int i=0;i<pobj->threads;i++)
    {
        pthread_create(&thread_array[i],NULL,ThreadProc,pobj);
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
   	_InitInfoPtr pobj = (_InitInfoPtr)ptr;	
    AutoHandle dtd;
	
	//--1. 连接目标数据库,同一个进程中，多个线程建立连接需要防止并发
    int   rc;
    rc = pthread_mutex_lock(&mutex);
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    rc = pthread_mutex_unlock(&mutex);
    
    //--2. 构建查询语句
    AutoMt mt(dtd,100000);
    
    for(int i=0;i<pobj->tsttimes;i++)
    {
       mt.FetchFirst("select * from %s.%s limit 100000",pobj->dbname,pobj->tbname);
       int rn = mt.Wait();
       lgprintf("threadid[%lu],test times[%d],fetch rows [%d]\n",pthread_self(),i,rn);
    }
    g_threadExit++;  
    return NULL;
}
