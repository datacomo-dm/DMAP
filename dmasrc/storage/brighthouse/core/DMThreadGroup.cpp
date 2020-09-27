
#include "DMThreadGroup.h"

    DMThreadGroup::DMThreadGroup(int _threadnum,int _dataqnum):cond() {
        thread_key=0;
        thread_specific=NULL;
        datanum=0;
        if(_threadnum==0)
            return;
        for(int i=0;i<_threadnum;i++) {
            workers.push_back(new DMThreadWorker(this));
        }
        if(_dataqnum==0) {
            _dataqnum=_threadnum*1.2+1;
        }
        for(int i=0;i<_dataqnum;i++) {
            idledataq.push_back(new DMThreadData(this));
        }
        bWaitAllEnd = false;
    }

    void DMThreadGroup::Terminate() {
        cond.Lock();
        int tsize=workers.size();
        for(int i=0;i<tsize;i++) {
            workers[i]->Terminate();
            delete workers[i];
        }
        workers.clear();
        cond.Unlock();
    }

    void DMThreadGroup::Wait(DMThreadData *pData)
    {
        DMThreadWorker *pWorker=NULL;
        cond.Lock();
        if(tmap.count(pData)>0) 
            pWorker=tmap[pData];
        cond.Unlock();
        if(pWorker==NULL) return;
        cond.Lock();
        while(tmap.count(pData)>0) {
            cond.TimedWait(100);
        }
        cond.Unlock();
    }
    
    bool DMThreadGroup::IsIdleEmpty() {
        endcond.Lock();
        bool isempty=idledataq.empty();
        endcond.Unlock();
        return isempty;
    }


    DMThreadGroup::~DMThreadGroup() {
        if(!bWaitAllEnd){
        try {
                WaitAllEnd();
        }
        catch(...) {
            //duplicated exception throw,ignored
        }
        }
        cond.Lock();
        int tsize=workers.size();
        for(int i=0;i<tsize;i++) 
            workers[i]->SetKilled(true);
        cond.Unlock();
        cond.Broadcast();
        
        for(int i=0;i<tsize;i++) 
            workers[i]->WaitThread();
        
        cond.Lock();
        for(int i=0;i<tsize;i++) {
            workers[i]->Terminate();
            delete workers[i];
        }
        workers.clear();
        while(!waitdataq.empty()) {
            delete waitdataq.front();
            waitdataq.pop_front();
        }
        for(std::set<DMThreadData *>::iterator it=lockeddataq.begin();it!=  lockeddataq.end();it++) {
            delete *it;
        }
        lockeddataq.clear();
        cond.Unlock();
        
        endcond.Lock();
        while(!idledataq.empty()) {
            delete idledataq.front();
            idledataq.pop_front();
        }
        endcond.Unlock();
    }
    
    void DMThreadGroup::AddThreadWorkers(int num) {
        for(int i=0;i<num;i++) {
            AddThreadWorker(new DMThreadWorker(this));
        }
    }

    void DMThreadGroup::AddThreadWorker(DMThreadWorker *p) {
        endcond.Lock();
        workers.push_back(p);
        idledataq.push_back(new DMThreadData(this));
        endcond.Unlock();
    }
    
    void DMThreadGroup::AddThreadData(DMThreadData *pd) {
        endcond.Lock();
        workers.push_back(new DMThreadWorker(this));
        idledataq.push_back(pd);
        endcond.Unlock();
    }
    // just check idle ,no wait
    DMThreadData *DMThreadGroup::GetIdleAndLock(void *_thread_specific) {
        endcond.Lock();
        DMThreadData *pdata=idledataq.empty()?NULL:idledataq.front();
        if(pdata) {
            idledataq.pop_front();
            endcond.Unlock();
            cond.Lock();
          lockeddataq.insert(pdata);
          cond.Unlock();
        }
        else 
         endcond.Unlock();
        if(pdata) {
         pdata->checkexcept();
         pdata->SetThreadSpecificData(_thread_specific==NULL?thread_specific:_thread_specific);
        }
        return pdata;
    }
    
    int DMThreadGroup::BatchLockDatas(std::list<DMThreadData *> &lst) {
        endcond.Lock();
        if(!idledataq.empty()) {
            lst=idledataq;
            idledataq.clear();
            endcond.Unlock();
            cond.Lock();
            for(std::list<DMThreadData *>::iterator it=lst.begin();it!=lst.end();it++) {
           (*it)->checkexcept();
           lockeddataq.insert(*it);
          }
          cond.Unlock();
        }
        else {
         lst.clear();
         endcond.Unlock();
        }
        return lst.size();
    }
    

    void DMThreadGroup::GiveBackDatas(std::list<DMThreadData *> &lst) {
        if(lst.empty()) return;
        endcond.Lock();
        for(std::list<DMThreadData *>::iterator it=lst.begin();it!=lst.end();it++) {
            idledataq.push_back(*it);
        }
        endcond.Unlock();
    }
    
    DMThreadData *DMThreadGroup::LockOrCreate(void *_thread_specific) {
        endcond.Lock();
        DMThreadData *pdata=idledataq.empty()?NULL:idledataq.front();
        if(pdata) {
            idledataq.pop_front();
            endcond.Unlock();
          cond.Lock();
          lockeddataq.insert(pdata);
          cond.Unlock();
        }
        else {
          endcond.Unlock();
            cond.Lock();
            pdata=new DMThreadData(this);
            workers.push_back(new DMThreadWorker(this));
            lockeddataq.insert(pdata);
            cond.Unlock();
        }
        if(pdata) 
         pdata->checkexcept();
        pdata->SetThreadSpecificData(_thread_specific==NULL?thread_specific:_thread_specific);
        return pdata;
    }
    
    bool DMThreadGroup::IsIdleAll() {
        cond.Lock();
        bool allidle=tmap.size()==0&&waitdataq.size()==0;
        cond.Unlock();
        return allidle;
    }

    int DMThreadGroup::GetBusyThreads() {
        cond.Lock();
        int bnum=tmap.size()+waitdataq.size();
        cond.Unlock();
        return bnum;
    }
    
    DMThreadData * DMThreadGroup::WaitIdleAndLock(void *_thread_specific) {
        DMThreadData *pdata=NULL;
        endcond.Lock();
        pdata=idledataq.empty()?NULL:idledataq.front();
        if(pdata) {
          if(!pdata->IsExcepted())
            idledataq.pop_front();
          endcond.Unlock();
          cond.Lock();
          if(!pdata->IsExcepted())
            lockeddataq.insert(pdata);
          cond.Unlock();
        }
        else {
         while(idledataq.empty()) 
          endcond.Wait();
         pdata=idledataq.front();
         if(!pdata->IsExcepted())
            idledataq.pop_front();
         endcond.Unlock();
         cond.Lock();
         if(!pdata->IsExcepted())
            lockeddataq.insert(pdata);
         cond.Unlock();
        }
        if(!pdata->IsExcepted())
            pdata->checkexcept();
        pdata->SetThreadSpecificData(_thread_specific==NULL?thread_specific:_thread_specific);
        return pdata;
    }
    
    // no force kill any running thread or waiting thread
    // no new task allowed to insert after call WaitAllEnd (same thread or seperate one)
    void DMThreadGroup::WaitAllEnd() {
        //printf("begin waitall.\n");
        cond.Lock();
            bWaitAllEnd = true;
        bool allidle=lockeddataq.size()==0;
        cond.Unlock();
        if(allidle) {
            //printf("first exit of waitall.\n");
            checkexcept();
            return;
        }
        //printf("to loop wait.\n");
        do {
            // lost signal in this time gap?
            // lockeddataq maybe changed between endcond lock and cond unlock
            endcond.Lock();
            endcond.TimedWait(100);
            endcond.Unlock();
            cond.Lock();
            allidle=lockeddataq.size()==0;
            cond.Unlock();
        }while(!allidle);
        //printf("last exit of waitall.\n");
        checkexcept();
    }
    
    void DMThreadGroup::checkexcept() {
        DMThreadCondGuard dgg(&endcond);
        for(std::list<DMThreadData *>::iterator it=idledataq.begin();it!=idledataq.end();it++) 
            (*it)->checkexcept();
    }
    
    void DMThreadGroup::clearexcept(){
        //DMThreadCondGuard dgg(&endcond);
        for(std::list<DMThreadData *>::iterator it=idledataq.begin();it!=idledataq.end();it++){ 
            (*it)->clearException();
        }
    }
    
    // called from outside of child-thread
    void DMThreadGroup::PushThreadData(DMThreadData *pd) {
        cond.Lock();
        waitdataq.push_back(pd);
        bWaitAllEnd = false;
        cond.Unlock();
        cond.Signal();
    }
    
    void DMThreadGroup::BatchPushDatas(std::list<DMThreadData *> &lst) {
        if(lst.empty()) return;
        cond.Lock();
        for(std::list<DMThreadData *>::iterator it=lst.begin();it!=lst.end();it++)
            waitdataq.push_back(*it);
        cond.Unlock();
        if(lst.size()>1)
         cond.Broadcast();
        else
         cond.Signal();
    }

    // called within thread
    DMThreadData *DMThreadGroup::PullWorkData(DMThreadWorker *pw) {
        DMThreadData *pdata=NULL;
        if(pw->IsKilled()) {
             return NULL;
        }
        cond.Lock();
        if(!waitdataq.empty()) {
            if(pw->IsKilled()) {
                cond.Unlock();
                return NULL;
            }
            pdata=waitdataq.front();
            waitdataq.pop_front();
            tmap[pdata]=pw;
            cond.Unlock();
            return pdata;
        }
        while(waitdataq.empty()) {
                        if(pw->IsKilled()) {
                                cond.Unlock();
                                return NULL;
                        }
            cond.Wait();
        }
        pdata=waitdataq.front();
        waitdataq.pop_front();
        tmap[pdata]=pw;
        cond.Unlock();
        return pdata;
    }

void DMThreadWorker::SetEnd(DMThreadData *pdata) {
    pThreadGroup->SetEnd(pdata);
}

void *DMThreadWorker::ThreadCommonEntry(void *param) 
    {
            DMThreadWorker *This=(DMThreadWorker *)param;
            bool hasend=false;
            int _ret_value = 0;
            DMThreadData *pData=NULL;
            while(!This->IsKilled()) {
                //PullWorkData Must Return :
                pData=This->pThreadGroup->PullWorkData(This);               
                if(This->IsKilled() || pData==NULL) {
                    This->SetEnd(pData);
                    hasend=true;
                    break;
                }

                hasend=false;
                pData->SetThreadSpecific();
                try { // can be reused even throw exception
                    
                    if(pData->pEntry!=NULL)
                        pData->pEntry(pData);
        #ifndef NOBOOST_TLIST
                    else if(pData->pEntryS!=NULL){
                        _ret_value=pData->pEntryS();
                        pData->SetReturnValue(_ret_value);
                    }
        #endif
                    else throw "No entry function";
                }
                catch(...) {
                    // terminate by pthreadcancel,can't catch except but must rethrow
                    if(This->IsKilled()) { 
                        This->SetEnd(pData);
                        throw;
                    }
                    pData->SetException();
                }
                This->SetEnd(pData);
                hasend=true;
            }
            //go here for join thread wait
            if(!hasend && pData)
             This->SetEnd(pData);
            return 0;
        }
        // killed and join thread thread
        void DMThreadWorker::WaitThread() {
            if(GetID()==0) return;
            void *retv;
            pthread_join(hd_pthread_t,&retv);
            ResetID();
            if(retvalue==TRET_STILLRUN)
                retvalue=TRET_OK;
        }   

        void DMThreadWorker::Terminate() {
            SetKilled(true);
            if(GetID()!=0) {
                pthread_cancel(hd_pthread_t);
                void *retv;
                pthread_join(hd_pthread_t,&retv);
                //pThreadGroup->SetEnd(NULL);
                ResetID();
            }
            retvalue=TRET_KILLED;
        }
        
        void DMThreadData::init() {
            cParam=NULL;cSize=0;
            errmsg[0]=0;strParam[0]=0;lParam=0;
            params=NULL;retvalue=0;paramsize=0;dParam=0.0;
            excepted=false;
            pEntry=NULL;
            thread_specific=NULL;
            sync_start=true; 
#ifndef NOBOOST_TLIST
            pEntryS=NULL;
#endif
        }

        void DMThreadData::ResetExcept() {
            except_ptr=NULL;
            excepted=false;
        }

        void DMThreadData::Start() {
        #ifndef NOBOOST_TLIST
            assert(pEntry||pEntryS);
        #else
            assert(pEntry);
        #endif
            except_ptr=NULL;
            excepted=false;
            retvalue=TRET_STILLRUN;
            //启动线程执行
            //这一句有可能在线程函数的Lock之前,但不影响流程
            if(sync_start)
                pThreadGroup->PushThreadData(this);
        }

        DMThreadData::~DMThreadData() {

            if(params) {
                delete [] params;
                params=NULL;
            }
            if(cParam) {
                delete [] cParam;
                cParam=NULL;
            }
        }

        void DMThreadData::Start(const char *pstruct,int struct_size,void (*pFunc)(DMThreadData *)) {
            if(cParam) 
                delete [] cParam;
            cParam=new char[struct_size];
            memcpy(cParam,pstruct,struct_size);
            if(pFunc)
                pEntry=pFunc;
            Start();
        }
        
        void DMThreadData::Start(void **_params,int param_size,void (*pFunc)(DMThreadData *)) {
            if(params) {
                delete [] params;
                params=NULL;
            }
            params= new void *[param_size];
            memcpy(params,_params,sizeof(void *)*param_size);
            paramsize=param_size;
            if(pFunc)
                pEntry=pFunc;
            Start();
        }
        
#ifndef NOBOOST_TLIST

        void DMThreadData::Start(boost::function<void(DMThreadData *)> pfunc,long workid) {
            if(params) {
                delete [] params;
                params=NULL;
            }
            if(pfunc)
                pEntry=pfunc;
            lParam=workid;
            Start();
        }
        
        void DMThreadData::StartInt(boost::function<int()> pfunc,long workid) {
            if(params) {
                delete [] params;
                params=NULL;
            }
            if(pfunc) {
                pEntryS=pfunc;
                pEntry=NULL;
            }
            lParam=workid;
            Start();
        }
        
        void DMThreadGroup::CheckExceptFor(boost::function<bool(DMThreadData *)> checkThread) {
        bool excepted=false;
        DMThreadData *pFirst=NULL;
        // clear first
        endcond.Lock();
        std::list<DMThreadData *>::iterator it=idledataq.begin();
        while(it!=idledataq.end()) {
            if((*it)->IsExcepted()) {
                if(!excepted) {
                    excepted=true;
                    pFirst=*it;
                }
                (*it)->ResetExcept();
            }
            ++it;
        }
        endcond.Unlock();
        if(excepted) pFirst->checkexcept();
    }
#endif

#ifndef CR_Class_NS_define
std::string CR_Class_NS::u642str(const uint64_t v){
    std::string ret;
    char _buf[17];
    register uint64_t v_tmp = v;
    register char c_tmp;

    for (int i=15; i>=0; i--) {
        c_tmp = v_tmp & 0xF;
        if (c_tmp > 9) {
            _buf[i] = c_tmp - 10 + 'A';
        } else {
            _buf[i] = c_tmp + '0';
        }
        v_tmp >>= 4;
    }
    _buf[16] = '\0';

    ret = _buf;

    return ret;
}

std::string  CR_Class_NS::i642str(const int64_t v){
    return u642str((uint64_t)v + INT64_MAX + 1);
}


uint64_t CR_Class_NS::str2u64(const std::string &s, const uint64_t defaule_value)
{
    uint64_t ret;
    char *bad_ptr;
    const char *s_ptr = s.c_str();
    ret = strtoull(s_ptr, &bad_ptr, 16);
    if ((ret == 0) && (bad_ptr == s_ptr))
        return defaule_value;
    return ret;
}

int64_t CR_Class_NS::str2i64(const std::string &s, const int64_t defaule_value)
{
    return (((int64_t)str2u64(s, (uint64_t)defaule_value + INT64_MAX + 1)) - INT64_MAX - 1);
}

#endif
