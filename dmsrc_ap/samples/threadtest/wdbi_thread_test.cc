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

int	g_threadExit = 0;  //线程数
pthread_mutex_t         mutex = PTHREAD_MUTEX_INITIALIZER;
int Start(void *ptr);
void* ThreadProc(void* ptr);  // 线程函数
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
}_InitInfo,*_InitInfoPtr;


// Just Sleep some seconds;
void TLTest1(ThreadList *pthread) {
	printf("<%d> Sleep %d seconds.\n",pthread->GetID(),pthread->IntegerParam());
	sleep(pthread->IntegerParam());
	printf("<%d> Wakeup! \n",pthread->GetID());
	pthread->SetReturnValue(0);
}

void DeadPipe(ThreadList *pthread) {
	printf("<%d> Write Pipe.\n",pthread->GetID());
	system("rm -f /tmp/threadlist_test.pipe");
	system("mkfifo /tmp/threadlist_test.pipe");
	FILE *ibfile=fopen("/tmp/threadlist_test.pipe","w+b");
	fwrite("adfsa;ljafd;fdas",10,1,ibfile);
	// never run to next line since no read pipe process.
	fclose(ibfile);
}

void ChildTask(ThreadList *pthread) {
	printf("<%d> Sleep %d seconds(ChildThread).\n",pthread->GetID(),pthread->IntegerParam());
	sleep(pthread->IntegerParam());
	printf("<%d> Wakeup! \n",pthread->GetID());
	pthread->SetReturnValue(0);
}

void ParentTask(ThreadList *pthread) {
	
	printf("<%d> Begin %d seconds sleep (ParentThread).\n",pthread->GetID(),pthread->IntegerParam());
	ThreadList childtl(ChildTask,pthread->IntegerParam());
	childtl.Wait();
	printf("<%d> End! \n",pthread->GetID());
	pthread->SetReturnValue(0);
}

void ThrowFunc(ThreadList *pthread) {
	printf("<%d> Begin throw test ...\n",pthread->GetID());
	sleep(2);
	throw "abcde";
	printf("Error : Never here.\n");
}

void ThreadTestWaitOne() {
	printf("Test WaitOne ThreadList...\n");
	printf("----- two elements:\n");
	ThreadList t1;
	//WaitOne模式不允许在构造时开始运行
	ThreadList *pt1=t1.AddThread(new ThreadList());
	//设置SetWaitOne以后,不允许在增加列表成员!
	t1.SetWaitOne();
	t1.Start(5,TLTest1);
	pt1->Start(3,TLTest1);
	while(ThreadList *pthread=t1.WaitOne()) {
		printf("Thread <%d> end.\n",pthread->GetID());
	}
	printf("----- catch exception:\n");
	ThreadList t2;
	ThreadList *pt2=t2.AddThread(new ThreadList());
	//设置SetWaitOne以后,不允许在增加列表成员!
	t2.SetWaitOne();
	t2.Start(5,TLTest1);
	pt2->Start(1,ThrowFunc);
	try {
		while(ThreadList *pthread=t2.WaitOne()) {
			printf("Thread <%d> end.\n",pthread->GetID());
		}
  }
  // catch char * failed by wrong type ,must be const char *
  catch(const char *err) {
  	printf("Got exception:%s in THREAD <%d>.\n",err,t2.GetExceptedThread()->GetID());
  }
  printf("After catch exception throwed in thread. WaitOthersEnd\n");
  t2.WaitThreadAll();
  
	printf("----- terminate thread:\n");
	ThreadList t3;
	ThreadList *pt3=t3.AddThread(new ThreadList());
	//设置SetWaitOne以后,不允许在增加列表成员!
	t3.SetWaitOne();
	t3.Start(5,TLTest1);
	// sleep 3 seconds on pt3
	t3.GetIdleThread()->Start(3,TLTest1);
	//wait pt3 end
	ThreadList *pthread=t3.WaitOne();
	//terminate t3
	t3.Terminate();
	assert(t3.WaitOne()==NULL);
	printf(" thread has killed.\n");
	
}

void ThreadTestNorm()  {
	printf("Test ThreadList...\n");
	printf("----- Normal thread call.\n");
	ThreadList t1(TLTest1,5),t2(TLTest1,10);
	t1.WaitThread();
	t2.WaitThread();
	printf("-----  thread List test.\n");
	ThreadList t3(TLTest1,6);
	t3.AddThread(new ThreadList(TLTest1,12));
	t3.WaitThreadAll();
	printf("----- Test kill thread(pipe).\n");
	ThreadList t4(DeadPipe,1);
	printf("      Wait 5 sec.\n");
	sleep(5);
	t4.Terminate();
	if(t4.Running()) 
		printf("   Terminate failed!!!!\n");
	else
		printf("   Thread has be terminated.\n");
	printf("----- Test thread wait at destroy.\n");
	{
		ThreadList t5(TLTest1,8);
	}
	printf("----- Test reuse list item\n");
	ThreadList t6;
	t6.AddThread(new ThreadList());
	int tid1=t6.Start(8,TLTest1);
	ThreadList *plist=t6.GetIdleThread();
	if(plist==NULL) 
		printf("Error: No idle thread in t6!\n");
	else
		plist->Start(3,TLTest1);
  t6.WaitThreadAll();
	printf("----- Test reuse thread resource\n");
	{
	ThreadList t7(TLTest1,11);
	t7.AddThread(new ThreadList(TLTest1,10));
	t7.AddThread(new ThreadList(TLTest1,6));
	// add idle thread :
	t7.AddThread(new ThreadList());
	t7.AddThread(new ThreadList(TLTest1,12));
	ThreadList *ti=t7.GetIdleThread();
	if(ti==NULL) {
		printf("Error:no idle thread in t7!\n");
	}
	else {
		int id1=ti->Start(3,TLTest1);
		ti->Wait();
		int id2=ti->Start(5);
		if(id2!=id1) 
			printf("Error: different threadid :%d--%d.\n",id1,id2);
		else 
			printf("Running in same thread.\n");
	}
	//Wait while t7 destory...
	}

	printf("----- Test thread in thread\n");
	ThreadList t8(ParentTask,5);	// TODO: more test...
	t8.Wait();
	
	printf("----- Test catch exception throw by thread\n");
	ThreadList t9;
	try {
		t9.Start(1,ThrowFunc);
		t9.Wait();
  }
  // catch char * failed by wrong type ,must be const char *
  catch(const char *err) {
  	printf("Got exception:%s.\n",err);
  }
 	printf("After catch exception throw in thread.\n");
 	//reuse excepted thread;
 	t9.Start(2,TLTest1);
 	t9.Wait();
}

void RunPerformance(ThreadList *pthread){
	int i = 0;
	long value = 500000;//pthread->IntegerParam();
	printf("thread [%d] test [value:%d] begin\n",pthread->GetID(),value);	
	while(i++<value){
		if(i % 7777 == 0){
			printf("thread id = [%d] mod 7777 \n",pthread->GetID());
		}
	}	
	printf("thread [%d] test end\n",pthread->GetID());
}
#define MAX_V  200
void ThreadTestPer(){
	printf("---------------ThreadTestPer begin--------------\n");
   	ThreadList tl;   	
   	int maxthread = 8;	
	for(uint i=1;i<maxthread;i++) {
		tl.AddThread(new ThreadList());
	}
	//tl.SetWaitOne();	
	int array[MAX_V];
	for( uint i=0; i<MAX_V; i++ ) {
		array[i] =1000*(i+1);
		if(tl.GetIdleThread() == NULL){
			while(tl.TryWait()==NULL){
				mSleep(1);
			}
		}
		tl.GetIdleThread()->Start(array[i],RunPerformance);		
	}
	tl.WaitAll();
	//while(tl.WaitOne()!=NULL);	
	printf("---------------ThreadTestPer end--------------\n");
}

void ThreadTestPerAll(){
	printf("---------------ThreadTestPer begin--------------\n");
   	ThreadList tl;   	
   	int maxthread = 8;	
	for(uint i=1;i<maxthread;i++) {
		tl.AddThread(new ThreadList());
	}	
	int array[MAX_V];
	for( uint i=0; i<MAX_V; i++ ) {
		while(NULL == tl.GetIdleThread()){
			mSleep(1);	
		}
		array[0] = 100*(i+1);// 9876543*(i+1);
		tl.GetIdleThread()->Start(array[i],RunPerformance);
	}
	tl.WaitAll();
	printf("---------------ThreadTestPer end--------------\n");
}

int main () {
	// ThreadTestNorm();
	// ThreadTestWaitOne();
	 ThreadTestPer();
	//ThreadTestPerAll();
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
    //nRetCode=wociMainEntrance(Start,true,(void*)&stInitInfo,2);
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
    AutoMt mt(dtd,10000);
    
    for(int i=0;i<pobj->tsttimes;i++)
    {
       mt.FetchFirst("select * from %s.%s limit 10000",pobj->dbname,pobj->tbname);
       int rn = mt.Wait();
       lgprintf("threadid[%lu],test times[%d],fetch rows [%d]\n",pthread_self(),i,rn);
    }
    g_threadExit++;  
    return NULL;
}
