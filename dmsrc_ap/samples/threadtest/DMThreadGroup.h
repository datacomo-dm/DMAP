#ifndef PThreadGroup_HEADER
#define PThreadGroup_HEADER

#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <exception>
#include <stdexcept>
#include <set>
#include <map>
#include <list>
#include <vector>
#ifndef NOBOOST_TLIST
#include <boost/function.hpp>
#endif

#include <time.h>
 #ifndef mSleep
 #define mSleep(msec) \
 { \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=(msec%1000)*1000*1000;\
   nanosleep(&req, &rem);\
 }
 #endif

/*  可重复使用的多线程列表类Thread (for Win32 && Linux)
线程函数体格式
// 定义多线程函数体
// 参数的类型和数量,必须在线程函数和调用者之间协调一致!
//  可以使用下列类型之一
//    *基本类型: int/long/double/char *
//    *自定义结构体
//    *指针数组
//  传入的参数在Thread中会建立一个副本,因此,调用者可以在参数传入Thread后自行释放参数空间
//     注意: 指针数组的成员所指向空间,需要保证线程安全 

void ThreadProcess(DMThread *tlist) {
        // 取得参数结构
        //  指针数组分解
        int i=(int) tlist->GetParams()[0];
        char *msg=(char *) tlist->GetParams()[1];
        // 或者结构体
        assert(sizeof(MyStruct)==tlist->StructParamSize());
        MyStruct *pSt=(MyStruct *)tlist->StructParam();
        // 或者基本类型:Integer/Doulbe/char *
        const char *strParam=tlist->StringParam();
        long lParam=tlist->IntegerParam();
        double fParam=tlist->DoubleParam();
        ......
        //执行任务
        do task....
        //结束
        设置返回值
        
        tlist->SetReturnValue(long v) ;
          或者 
        tlist->SetError(int ecode,const char *err) ;
          或者使用Thread中结构体/指针数组设置返回结果:调用者要使用Thread中的副本检索返回值
}


两种启动方法:
1. 先定义,后启动
        DMThread t1;
        //传入一个整数参数
        t1.Start(ThreadProcess,1);
        t1.WaitThread();
2. 定义时启动
        //传入一个整数参数
        DMThread t1(ThreadProcess,1);
        t1.WaitThread();
指针数组做参数
.....   
     void **params[2];
     params[0]=xxx;
     params[1]=xxx;
     DMThread   tl(ThreadFunc,params,2);
     tl.WaitThread();
......
其它类型的参数
.....   
     MyStruct st1;
     // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
     DMThread   tl(ThreadFunc,(const char *)&st1,sizeof(st1));
     tl.WaitThread();
......
        
     char *str="xxxxxx";
     DMThread   tl(ThreadFunc,str);// 字符串参数,最大1024字节
     tl.WaitThread();
     DMThread   t2(ThreadFunc,3.14);// 浮点参数
     
......
     


多个线程(List)
        DMThread tl(ThreadFun,Params...);
        tl.AddThread(new DMThreadData(ThreadFun,params,...));
        ...
        DMThread *pIdle=t1.GetIdleThread();
        if(pIdle)
           pIdle->Start();
        ...
        int totalThreads=tl.ListSize();
        int busythreads=tl.GetBusyThreads();
        ...
        t1.WaitThreadAll();
结构体中返回值
     MyStruct struct;
     // set value of struct
     // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
     DMThread TL(ThreadProcess,(char *)&struct,sizeof(struct));
     TL.WaitThread();
     MyStruct pstruct=(MyStruct *)TL->StructParam();
     // check value of struct....
*/


/* 互斥机制
线程在初始化或者初次Start时建立,之后使用互斥变量来控制运行
对象销毁时,设置killed标识,线程退出
线程Terminate时,退出运行,下次Start重新创建线程
三个互斥变量 
start_mutex/end_mutex/???restart_mutex

start_mutex: 线程创建后,每次都要收到明确的Start信号才开始运行,确保pEntry不会不受控制的循环执行
       初始化和WaitThread/terminate设置为Lock状态,之后在Start中Unlock,ThreadCommonEntry中Lock

end_mutex: 用于等待pEntry执行完成
            初始化和Wait/Terminate设置为Lock状态,pEntry结束后Unlock

???restart_mutex: 用于pEntry执行完成后,pEntry下次开始等待运行前Wait中Lock start_mutex,确保ThreadCommonEntry中对start的Lock不会发生在Wait的Lock之前
    初始化和pEntry结束后Lock,Wait中UnLock, WaitThread/Terminate复位为Lock
*/

/* Wait 与 WaitThread

Wait : 等待函数执行结束,但不销毁线程,下次Start使用同一个线程
WaitThread: 等待线程执行结束,线程资源释放,下次Start重新创建线程
使用GetID()检查线程ID号,可以看出这两者的区别.
*/


#define ERRMSGLEN 3000
#define TRET_STILLRUN -10000
#define TRET_KILLED -100002
#define TRET_OK 0
#define DEFAULT_STRING_PARALEN 1025

class DMMutex {
        pthread_mutex_t mutexobj;
        long locked,trylocked;
public :
        DMMutex() {
                pthread_mutex_init(&mutexobj,NULL);
                locked=0;trylocked=0;
                //mutexobj=PTHREAD_MUTEX_INITIALIZER;
        }
        pthread_mutex_t *Ref() {return &mutexobj;}
        int Lock() {
            int rt=pthread_mutex_lock(&mutexobj);
            locked++;
            return rt;
        }
        void Unlock()
        {
            pthread_mutex_unlock(&mutexobj);
        }
        ~DMMutex() {
            pthread_mutex_destroy(&mutexobj);
        }
        int TryLock() {
             trylocked++;
             return pthread_mutex_trylock(&mutexobj);
        }
        void Stat() {
            printf(" mutex locked %ld,trylocked %ld. \n",locked,trylocked);
        }
};

class DMCond {
    pthread_cond_t cond;
    long waited,timewaited,signaled,broaded;
    DMMutex mutex;
public:

    DMCond():mutex() {
        pthread_cond_init(&cond, NULL);
        waited=0;signaled=0;broaded=0;timewaited=0;
    }
    int Lock() {
        mutex.Lock();
    }
    
    int TimedWait(long m_sec) {
        struct timespec outtime;
        clock_gettime(CLOCK_REALTIME, &outtime);
        outtime.tv_sec += m_sec/1000l;
        m_sec=m_sec%1000l;
        outtime.tv_sec +=(outtime.tv_nsec+m_sec*1000000l)/1000000000l;
        outtime.tv_nsec = (outtime.tv_nsec+m_sec*1000000l)%1000000000l;
        timewaited++;
        return pthread_cond_timedwait(&cond, mutex.Ref(),&outtime);
    }

    int Wait() {
        waited++;
        return pthread_cond_wait(&cond, mutex.Ref());
    }
    void Stat() {
        mutex.Stat();
        printf("Cond waited %ld,timedwaited %ld,signaled %ld,broadcasted %ld.\n",waited,timewaited,signaled,broaded);
    }
    void Unlock() {
        mutex.Unlock();
    }
    int Signal() {
        signaled++;
        return pthread_cond_signal(&cond);
    }
    int Broadcast() {
        broaded++;
        return pthread_cond_broadcast(&cond);
    }
    
    ~DMCond() {
        pthread_cond_destroy(&cond);
    }
    
};

class DMThreadGroup;
class DMThreadData;
class DMThreadWorker {

    bool killed;
    DMThreadGroup *pThreadGroup;
    
    pthread_t hd_pthread_t;
    long retvalue;
    // force threat param as ThreadItem
    // lockWaitStart at begin & unlockwaitend at end
    static void *ThreadCommonEntry(void *param) ;

public:

    DMThreadWorker(DMThreadGroup *t) {
        killed=false;
        pThreadGroup=t;
        retvalue=0;
        pthread_create(&hd_pthread_t,NULL,DMThreadWorker::ThreadCommonEntry,(void *)this);
    }
    
    //线程已经退出,或者被终止,需要清理ID
    bool ResetID() {
        hd_pthread_t=0;
    }
    
    bool SetKilled(bool v) {killed=v;}
    bool IsKilled() {return killed;}
    long GetID() {
        return hd_pthread_t;
    }
    
    void SetEnd(DMThreadData *t);
    
    bool IsRunning() {
        if(GetID()==0) return false;
        return pthread_kill(hd_pthread_t,0)==0;
    }
    
    //没有设置Killed,线程不会结束
    //如果没有Start,不要Wait,会被死锁
    //WaitThread以后,对象不要在使用!
    void WaitThread() ;
    //Terminating a thread is dangerous.see http://msdn.microsoft.com/en-us/library/ms686717(VS.85).aspx
    //TerminateWithoutWait!
    void Terminate() ;
};

class DMThreadGroup {
    DMCond cond,endcond;
    std::vector<DMThreadWorker*> workers;//guarded by cond  
    std::map<DMThreadData *,DMThreadWorker*> tmap; //guarded by cond
    std::list<DMThreadData *>idledataq; // guraded by endcond
    std::list<DMThreadData *>waitdataq; // guarded by cond
    std::set<DMThreadData *>lockeddataq;// guarded by cond
    void *thread_specific;
    pthread_key_t thread_key;
    int datanum;
    bool bWaitAllEnd;
public:
    void LockAll() {
        endcond.Lock();
        cond.Lock();
    }
    int AddDataID() {return datanum++;}
    void SetThreadSpecific(void *_thread_specific) {
        thread_specific=_thread_specific;
    }

    void SetThreadKey(pthread_key_t key) {
        thread_key=key;
    }

    pthread_key_t GetThreadKey() {
        return thread_key;
    }
    
    void *GetThreadSpecific() {
        return thread_specific;
    }

    void UnlockAll() {
        cond.Unlock();
        endcond.Unlock();
    }   

    // clear job excepted(killed?)
    void ClearJob(DMThreadData *pdata) {
        cond.Lock();
        if(lockeddataq.count(pdata)>0) {
            lockeddataq.erase(pdata);
            cond.Unlock();
            endcond.Lock();
            idledataq.push_back(pdata);
            endcond.Unlock();
        }
        else cond.Unlock();
    }
    
    #ifndef NOBOOST_TLIST
    //放弃还在等待队列中的任务
    void ClearWait(boost::function<bool(DMThreadData *)> checkThread) {
        std::list<DMThreadData *> topushidle;
        cond.Lock();
        std::list<DMThreadData *>::iterator iter=waitdataq.begin();
        while(iter!=waitdataq.end()) {
            if(checkThread(*iter)) {
                lockeddataq.erase(*iter);
                topushidle.push_back(*iter);
                iter=waitdataq.erase(iter);
            }
            else iter++;
        }
        cond.Unlock();
        endcond.Lock();
        idledataq.merge(topushidle);
        endcond.Unlock();
    }
    // check all idle thread for excepted
    void CheckExceptFor(boost::function<bool(DMThreadData *)> checkThread) ;

    //等待指定的任务执行完成
    void WaitFor(boost::function<bool(DMThreadData *)> checkThread) {
        DMThreadData *pWait=NULL;
        do {
         pWait=NULL;
         cond.Lock();
          std::map<DMThreadData *,DMThreadWorker*>::iterator iter=tmap.begin();
          while(iter!=tmap.end()) {
            if(checkThread(iter->first)) {
                pWait=iter->first;
                break;
            }
            iter++;
          }
         cond.Unlock();
         if(pWait) {
            Wait(pWait);
         }
        } while(pWait!=NULL);
    }
    #endif

    // wait for specified DMThreadData
    void Wait(DMThreadData *pData);
        
    void Stat() {
        printf(" ------------\n cond of DMThreadGroup :\n");
        cond.Stat();
        printf(" ------------\n endcond of DMThreadGroup :\n");
        endcond.Stat();
    }
    
    DMThreadGroup(int _threadnum=0,int _dataqnum=0);
    ~DMThreadGroup() ;
    void AddThreadData(DMThreadData *pd);
    void AddThreadWorkers(int num=1) ;
    void AddThreadWorker(DMThreadWorker *p);

    DMThreadData *PullWorkData(DMThreadWorker *pw); 
    void PushThreadData(DMThreadData *pd) ;
    
    int BatchLockDatas(std::list<DMThreadData *> &lst);
    void BatchPushDatas(std::list<DMThreadData *> &lst);
    void GiveBackDatas(std::list<DMThreadData *> &lst);
    void SetEnd(DMThreadData *pd) {     
    
        cond.Lock();
        lockeddataq.erase(pd);
        tmap.erase(pd);
        cond.Unlock();
        
        endcond.Lock();
        idledataq.push_back(pd);
        endcond.Unlock();
        endcond.Broadcast();
    }

    bool IsIdleAll() ;
    bool IsIdleEmpty();

    int GetBusyThreads() ;
    DMThreadData *LockOrCreate(void *_thread_specific=NULL);
    // just check idle ,no wait
    //_rel: unlclock internal clock if no idle before exit
    DMThreadData *GetIdleAndLock(void *_threadspecific=NULL) ;
    // wait for idle thread
    DMThreadData * WaitIdleAndLock(void *_threadspecific=NULL);
    void WaitAllEnd() ;
    void Terminate();
    void clearexcept();
    void checkexcept();
};

class DMThreadCondGuard {
    DMCond *pobj;
public:
    DMThreadCondGuard(DMCond *p) {
        pobj=p;
        pobj->Lock();
    }
    ~DMThreadCondGuard() {
        pobj->Unlock();
    }
};

class DMThreadData {
        DMThreadGroup *pThreadGroup;
        int paramsize;
        bool excepted;
        char errmsg[ERRMSGLEN];
        std::exception_ptr except_ptr;
        long retvalue;
        //params成员所指向的内容,必须是线程安全
        void **params;
        // store structure data
        // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
        char *cParam;
        int cSize;
        long lParam;
        double dParam;
        char strParam[DEFAULT_STRING_PARALEN];
        void init() ;
        bool sync_start;
        void *thread_specific;
        int dataid;
public: 
#ifndef NOBOOST_TLIST
    boost::function<void (DMThreadData *)> pEntry;
    boost::function<int ()> pEntryS;
#else
    void (*pEntry)(DMThreadData *pthreadparam);
#endif      
        int GetDataID() {return dataid;}
        void SetThreadSpecificData(void *_thread_specific) {
            thread_specific=_thread_specific;
        }
        
        void SetThreadSpecific() {
            if(thread_specific!=NULL)
                pthread_setspecific(pThreadGroup->GetThreadKey(),thread_specific);
        }

        void *GetThreadSpecific() {
            return thread_specific;
        }
        

        void SetSyncStart(bool v) {
            sync_start=v;
        }

        void Wait() {
            pThreadGroup->Wait(this);
        }
        
        void ClearJob() {
            pThreadGroup->ClearJob(this);
        }

        void clearException(){
            excepted=false;
            except_ptr = NULL;
        }
        
        void SetException() {
            excepted=true;
            except_ptr = std::current_exception(); // capture
        }
        
        double DoubleParam() {return dParam;}
        long IntegerParam() {return lParam;}
        const char *StringParam() {return strParam;}
        // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
        char *StructParam() {return cParam;}
        int StructParamSize() {return cSize;}

        //params成员所指向的内容,由调用者保证线程安全
        void **GetParams() {
            return params;
        }
        void SetParam(int element,void *p) {
            params[element]=p;
        }
        int GetParamsSize() {
            return paramsize;
        }

        
        //params成员所指向的内容,由调用者保证线程安全
        DMThreadData(void (*pFunc)(DMThreadData *),void **_params,int param_items,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            pEntry=pFunc;
            if(param_items>0) {
                assert(_params);
                paramsize=param_items;
                params=(void **) new void *[param_items];
                memcpy(params,_params,sizeof(void *)*param_items);
            }
            Start();
        }

        // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
        DMThreadData(void (*pFunc)(DMThreadData *),const char *pstruct ,int struct_size,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            pEntry=pFunc;
            cParam=new char[struct_size];
            memcpy(cParam,pstruct,struct_size);
            cSize=struct_size;
            Start();
        }
        
        //launch by call Start
        DMThreadData(DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            pEntry=NULL;
        }

        DMThreadData(void (*pFunc)(DMThreadData *),long value,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            lParam=value;
            pEntry=pFunc;
            Start();
        }

        DMThreadData(void (*pFunc)(DMThreadData *),int value,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            lParam=value;
            pEntry=pFunc;
            Start();
        }
        
        DMThreadData(void (*pFunc)(DMThreadData *),double value,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            dParam=value;
            pEntry=pFunc;
            Start();
        }
        
        DMThreadData(void (*pFunc)(DMThreadData *),const char * value,DMThreadGroup *ptp) {
            pThreadGroup=ptp;
            dataid=ptp->AddDataID();
            init();
            strncpy(strParam,value,DEFAULT_STRING_PARALEN-1);
            strParam[DEFAULT_STRING_PARALEN-1]=0;
            pEntry=pFunc;
            Start();
        }

        
        void SetReturnValue(long v) {retvalue=v;}
        
        long GetReturnValue() {return retvalue;}

        void SetError(long ecode,const char *err) {
            retvalue=ecode;
            strncpy(errmsg,err,ERRMSGLEN);
            errmsg[ERRMSGLEN-1]=0;
        }
        


        DMThreadData *GetExceptedThread() {
            if(excepted) return this;
            return NULL;
        }
        
        bool IsExcepted() {return excepted;}
        //使用指定的结构体参数和线程函数(可选)启动线程
        // Thread仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
        void Start(const char *pstruct,int struct_size,void (*pFunc)(DMThreadData *)=NULL) ;
        
        //使用指定的长整数参数和线程函数(可选)启动线程
        void Start(long value,void (*pFunc)(DMThreadData *)=NULL) {
            lParam=value;
            if(pFunc)
                pEntry=pFunc;
            Start();
        }
        
        //使用指定的整数参数和线程函数(可选)启动线程
        void Start(int value,void (*pFunc)(DMThreadData *)=NULL) {

            lParam=value;
            if(pFunc)
                pEntry=pFunc;
            Start();
        }
        
        //使用指定的浮点数参数和线程函数(可选)启动线程
        void Start(double value,void (*pFunc)(DMThreadData *)=NULL) {

            dParam=value;
            if(pFunc)
                pEntry=pFunc;
            Start();
        }

        //使用指定的字符串参数和线程函数(可选)启动线程
        void Start(const char * value,void (*pFunc)(DMThreadData *)=NULL) {

            pEntry=pFunc;
            strncpy(strParam,value,DEFAULT_STRING_PARALEN-1);
            strParam[DEFAULT_STRING_PARALEN-1]=0;
            if(pFunc)
                pEntry=pFunc;
            Start();
        }
        
        // 使用指定的指针数组和线程函数(可选)启动线程
        //  注意: 类内部仅分配指针数组,复制指针地址,对于数组成员指针所指向的内容,外部调用者必须要保证线程安全
        // allocated params internal
        void Start(void **_params,int param_size,void (*pFunc)(DMThreadData *)=NULL) ;
        
        void ClearParam() {
            if(params) {
                delete [] params;
                params=NULL;
            }
            if(cParam) {
                delete [] cParam;
                cParam=NULL;
            }
        }

        const char *GetErrorMsg() {
            return errmsg;
        }

        
#ifndef NOBOOST_TLIST

        void Start(boost::function<void(DMThreadData *)> pfunc,long workid=0) ;
        
        void StartInt(boost::function<int()> pfunc,long workid=0) ;
#endif
        //使用已设置的函数和参数启动线程
        void Start() ;
        //rethrow exception and clear 
        void checkexcept() {
            if(except_ptr!=NULL) {
                std::exception_ptr local_except_ptr=except_ptr;
                except_ptr=NULL;
                excepted=false;
                pThreadGroup->ClearJob(this);
                pThreadGroup->clearexcept();
                std::rethrow_exception(local_except_ptr);
            }
        }
        void ResetExcept();
        ~DMThreadData();
};


#endif


#ifndef CR_Class_NS_define
#define CR_Class_NS_define
namespace CR_Class_NS {
    std::string u642str(const uint64_t v);
    std::string  i642str(const int64_t v);
};
#endif

