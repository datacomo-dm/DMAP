#include "ThreadList.h"

#ifndef __unix
unsigned __stdcall * ThreadList::ThreadCommonEntry(void *param) 
#else
void * ThreadList::ThreadCommonEntry(void *param) 
#endif
{
	bool hasend=false;
	int _ret_value = 0;
	ThreadList *This=(ThreadList *)param;
	while(!This->IsKilled()) {
		//WaitThread Must Exit From:
		This->start_mutex.Lock();
		if(This->IsKilled()) {
			This->end_mutex.Unlock();
			hasend=true;
			break;
		}
		hasend=false;
		try { // can be reused even throw exception
			if(This->pEntry!=NULL)
				This->pEntry(This);
#ifndef NOBOOST_TLIST
			else if(This->pEntryS!=NULL){
				_ret_value=This->pEntryS();
				This->SetReturnValue(_ret_value);
			}
#endif
			else throw "No entry function";
		}
		catch(...) {
			// terminate by pthreadcancel,can't catch except but must rethrow
			if(This->IsKilled()) { 
				//restore context for next call
				This->end_mutex.Unlock();
				/* do not wait on a terminated thread
				if(This->pStateMutex) 
				{
					//加锁操作共享数据
					// add list and notify parent thread
					This->pStateMutex->Lock();
					This->pEndThread->insert(This);
					This->pWaiton->Unlock();
					This->pStateMutex->Unlock();
				}
				*/
				throw;
			}
			This->excepted=true;
			This->except_ptr = std::current_exception(); // capture
		}
		This->end_mutex.Unlock();
		hasend=true;
		if(This->pStateMutex) 
		{
			//加锁操作共享数据
			// add list and notify parent thread
			This->pStateMutex->Lock();
				This->pEndThread->insert(This);
				This->pWaiton->Unlock();
			This->pStateMutex->Unlock();
		}
	}
	//go here for join thread wait
	if(!hasend)
	 This->end_mutex.Unlock();
	//This->ResetID();
	#ifndef __unix
	_endthreadex( 0 );
return 0;
	#else
	return NULL;
	#endif
}


void ThreadList::init() {
	cParam=NULL;cSize=0;
	errmsg[0]=0;isidle=true;strParam[0]=0;lParam=0;pNext=NULL;
	params=NULL;retvalue=0;pNext=NULL;paramsize=0;dParam=0.0;
	killed=false;excepted=false;locked=false;
	ResetID();
	//初始状态是Lock,等待执行
	start_mutex.Lock();
	end_mutex.Lock();

	pWaiton=NULL;
	pEndThread=NULL;
	pStateMutex=NULL;
	
	pSharedMutex=NULL;
#ifndef NOBOOST_TLIST
	pEntryS=NULL;
#endif
	pSharestateMutex=NULL;
	//restart_mutex.Lock();
	
}

void ThreadList::SetWaitOne(Mutex *pwaitlock,Mutex *pstatelock,std::set<ThreadList *> *pthread) {
	if(pNext) pNext->SetWaitOne(pwaitlock,pstatelock,pthread);
	pWaiton=pwaitlock;
	pStateMutex=pstatelock;
	pEndThread=pthread;
}


//只能设置,不能取消,不能重复设置
//使用WaitOn模式,不能再使用Wait
//在所有线程开始运行以前设置,因此只能使用 ThreadList()/Start()模式
void ThreadList::SetWaitOne() {
	assert(pSharedMutex==NULL);
	pSharedMutex=new Mutex();
	pSharestateMutex=new Mutex();
	SetWaitOne(pSharedMutex,pSharestateMutex,&SharedEndThread);
	pSharedMutex->Lock();
}
//得到最早运行结束的线程
//返回NULL表示列表中所有线程都是空闲
ThreadList *ThreadList::WaitOne() {
	assert(pSharedMutex!=NULL);
	ThreadList *pret=NULL;
	/*
	pSharestateMutex->Lock();
	if(SharedEndThread.size()>0) {
		pret=*SharedEndThread.begin();
		SharedEndThread.erase(pret);
	}
	pSharestateMutex->Unlock();
	if(pret) {
		pret->isidle=true;
		pret->checkexcept();
		return pret;
	}
	*/
	if(IsIdleAll()) return NULL;
	//Wait for one of thread end
	// Lock at init and here ,unlock at thread end
	
	pSharedMutex->Lock();
	pSharestateMutex->Lock();
	assert(SharedEndThread.size()>0);
	pret=*SharedEndThread.begin();
	SharedEndThread.erase(pret);
	if(SharedEndThread.size()>0)
		pSharedMutex->Unlock();
	pret->isidle=true;
	pSharestateMutex->Unlock();
	pret->checkexcept();
	return pret;
}

//线程已经退出,或者被终止,需要清理ID
bool ThreadList::ResetID() {
	#ifndef __unix
	hThread=0;
	#else
	hd_pthread_t=0;
	#endif
}
bool ThreadList::SetKilled(bool v) {killed=v;}
bool ThreadList::IsKilled() {return killed;}
double ThreadList::DoubleParam() {return dParam;}
long ThreadList::IntegerParam() {return lParam;}
const char *ThreadList::StringParam() {return strParam;}
// ThreadList仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
char *ThreadList::StructParam() {return cParam;}
int ThreadList::StructParamSize() {return cSize;}

//params成员所指向的内容,由调用者保证线程安全
void **ThreadList::GetParams() {
	return params;
}
void ThreadList::SetParam(int element,void *p) {
	params[element]=p;
}
int ThreadList::GetParamsSize() {
	return paramsize;
}

ThreadList *ThreadList::GetThread(long id) {
	if(GetID()==id) return this;
	else if(pNext)  return pNext->GetThread(id);
	// not found
	return NULL;
}

long ThreadList::GetID() {
	#ifndef __unix
	return hThread;
	#else
	return hd_pthread_t;
	#endif
}

//params成员所指向的内容,由调用者保证线程安全
ThreadList::ThreadList(void (*pFunc)(ThreadList *),void **_params,int param_items) {
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

// ThreadList仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
ThreadList::ThreadList(void (*pFunc)(ThreadList *),const char *pstruct ,int struct_size) {
	init();
	pEntry=pFunc;
	cParam=new char[struct_size];
	memcpy(cParam,pstruct,struct_size);
	cSize=struct_size;
	Start();
}

//launch by call Start
ThreadList::ThreadList() {
	init();
	pEntry=NULL;
}

ThreadList::ThreadList(void (*pFunc)(ThreadList *),long value) {
	init();
	lParam=value;
	pEntry=pFunc;
	Start();
}

ThreadList::ThreadList(void (*pFunc)(ThreadList *),int value) {
	init();
	lParam=value;
	pEntry=pFunc;
	Start();
}

ThreadList::ThreadList(void (*pFunc)(ThreadList *),double value) {
	init();
	dParam=value;
	pEntry=pFunc;
	Start();
}

ThreadList::ThreadList(void (*pFunc)(ThreadList *),const char * value) {
	init();
	strncpy(strParam,value,DEFAULT_STRING_PARALEN-1);
	strParam[DEFAULT_STRING_PARALEN-1]=0;
	pEntry=pFunc;
	Start();
}


void ThreadList::SetReturnValue(long v) {retvalue=v;}

long ThreadList::GetReturnValue() {return retvalue;}

void ThreadList::SetError(long ecode,const char *err) {
	retvalue=ecode;
	strncpy(errmsg,err,ERRMSGLEN);
	errmsg[ERRMSGLEN-1]=0;
}

bool ThreadList::IsIdleAll() {
	if(!isidle) return false;
	if(pNext) return pNext->IsIdleAll();
	return true;
}

// add new allocated thread
ThreadList *ThreadList::AddThread(ThreadList *pwk) {
	//不允许动态添加到WaitOne模式
	assert(pEndThread==NULL);
	if(pNext) 
		pNext->AddThread(pwk);
	else {
		pNext=pwk;
	}
	return pwk;
}


int ThreadList::GetBusyThreads() {
	return (isidle?0:1)+pNext?0:pNext->GetBusyThreads();
}

int ThreadList::ListSize() {
	return 1+pNext?0:pNext->ListSize();
}			
//避免重复使用idle thread
ThreadList *ThreadList::GetIdleThread() {
	if(LockIfIdle()) return this;
	else if(pNext) return pNext->GetIdleThread();
	return NULL;
}
bool ThreadList::LockIfIdle() {
	checkidle.Lock();
	bool ret=isidle&&!locked;
	if(ret)
		locked=true;
	checkidle.Unlock();
	return ret;
}
ThreadList *ThreadList::GetExceptedThread() {
	if(excepted) return this;
	else if(pNext) return pNext->GetExceptedThread();
	return NULL;
}


//使用指定的结构体参数和线程函数(可选)启动线程
// ThreadList仅对结构体做简单复制,不会调用拷贝构造函数和赋值构造函数!
int ThreadList::Start(const char *pstruct,int struct_size,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	if(cParam) 
		delete [] cParam;
	cParam=new char[struct_size];
	memcpy(cParam,pstruct,struct_size);
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

//使用指定的长整数参数和线程函数(可选)启动线程
int ThreadList::Start(long value,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	lParam=value;
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

//使用指定的整数参数和线程函数(可选)启动线程
int ThreadList::Start(int value,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	lParam=value;
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

//使用指定的浮点数参数和线程函数(可选)启动线程
int ThreadList::Start(double value,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	dParam=value;
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

//使用指定的字符串参数和线程函数(可选)启动线程
int ThreadList::Start(const char * value,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	pEntry=pFunc;
	strncpy(strParam,value,DEFAULT_STRING_PARALEN-1);
	strParam[DEFAULT_STRING_PARALEN-1]=0;
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

// 使用指定的指针数组和线程函数(可选)启动线程
//  注意: 类内部仅分配指针数组,复制指针地址,对于数组成员指针所指向的内容,外部调用者必须要保证线程安全
// allocated params internal
int ThreadList::Start(void **_params,int param_size,void (*pFunc)(ThreadList *)) {
	assert(isidle);
	if(params) {
		delete [] params;
		params=NULL;
	}
	params= new void *[param_size];
	memcpy(params,_params,sizeof(void *)*param_size);
	paramsize=param_size;
	if(pFunc)
		pEntry=pFunc;
	return Start();
}

void ThreadList::StartAll() {
	if(pNext)
		pNext->StartAll();
	Start();
}


int ThreadList::ClearParam() {
	if(params) {
		delete [] params;
		params=NULL;
	}
	if(cParam) {
		delete [] cParam;
		cParam=NULL;
	}
}

const char *ThreadList::GetErrorMsg() {
	return errmsg;
}

void ThreadList::WaitAll() {
	if(pNext) 
		pNext->WaitAll();
	Wait();
}

bool ThreadList::Running() {
	if(GetID()==0) return false;
	#ifndef __unix
	return WaitForSingleObject( hThread, 0)!=WAIT_OBJECT_0;
	#else
	return pthread_kill(hd_pthread_t,0)==0;
	#endif
}
#ifndef NOBOOST_TLIST

int ThreadList::Start(boost::function<void(ThreadList *)> pfunc) {
	assert(isidle);
	if(params) {
		delete [] params;
		params=NULL;
	}
	if(pfunc)
		pEntry=pfunc;
	return Start();
}

int ThreadList::StartInt(boost::function<int()> pfunc) {
	assert(isidle);
	if(params) {
		delete [] params;
		params=NULL;
	}
	if(pfunc) {
		pEntryS=pfunc;
		pEntry=NULL;
	}
	return Start();
}

#endif

//使用已设置的函数和参数启动线程
int ThreadList::Start() {
	assert(isidle);
#ifndef NOBOOST_TLIST
	assert(pEntry||pEntryS);
#else
	assert(pEntry);
#endif
	checkidle.Lock();
	isidle=false;
	locked=false;
	checkidle.Unlock();
	except_ptr=NULL;
	excepted=false;
	retvalue=TRET_STILLRUN;
	//只创建一次线程
	//新创建的线程进入等待执行状态,直至UnlockWaitStart
	if(GetID()==0) {
		#ifndef __unix
		 hThread = (HANDLE)_beginthreadex( NULL, 0, &ThreadItem::ThreadCommonEntry, NULL, 0, &threadID );
		#else
		pthread_create(&hd_pthread_t,NULL,ThreadList::ThreadCommonEntry,(void *)this);
		#endif
	}
	SetKilled(false);
	//启动线程执行
	//这一句有可能在现场函数的Lock之前,但不影响流程
	start_mutex.Unlock();
	return GetID();
}
//rethrow exception and clear 
void ThreadList::checkexcept() {
	if(except_ptr!=NULL) {
		std::exception_ptr local_except_ptr=except_ptr;
		except_ptr=NULL;
		std::rethrow_exception(local_except_ptr);
	}
}

void ThreadList::Wait() {
	//check re_enter
	if(GetID()==0 || isidle) return;
	// Wait threadfunc end
	end_mutex.Lock();
	isidle=true;
	checkexcept();
}

bool ThreadList::TryAndLock() {
	bool ret=false;
	checkidle.Lock();
	if(end_mutex.TryLock()==0 && !locked) {
		isidle=true;
		locked=true;
		ret=true;
	}
	checkidle.Unlock();
	return ret;
}

ThreadList *ThreadList::TryWait() {
	if(TryAndLock()) {
		checkexcept();
		return this;
	}
	else if(pNext)
		return pNext->TryWait();
	else return NULL;
}

//没有设置Killed,线程不会结束
//如果没有Start,不要Wait,会被死锁
// 每一次Start以后,WaitThread和Wait只能使用其中的一个,不能重复使用
void ThreadList::WaitThread() {
	if(GetID()==0) return;
	// wait current process end.
	//rethrow later
	try {
		Wait();
	}
	catch(...) {};
	SetKilled(true);
	//start thread to release it(has set killed).
	start_mutex.Unlock();
	#ifndef __unix			
	WaitForSingleObject( hThread, INFINITE );
	CloseHandle( hThread );
	#else
	void *retv;
	pthread_join(hd_pthread_t,&retv);
	#endif
	ResetID();
	isidle=true;
	//prepared for next start
	// reset mutex lock by thread
	start_mutex.Unlock();
	start_mutex.Lock();
	
	if(retvalue==TRET_STILLRUN)
		retvalue=TRET_OK;
	checkexcept();
}
//Terminating a thread is dangerous.see http://msdn.microsoft.com/en-us/library/ms686717(VS.85).aspx
//TerminateWithoutWait!
void ThreadList::Terminate() {
	SetKilled(true);
	if(!isidle && GetID()!=0) {
		#ifndef __unix
		DWORD ret;
		TerminateThread(hThread,ret);
		CloseHandle( hThread );
		#else
		pthread_cancel(hd_pthread_t);
		#endif
		ResetID();
	}
	// reset all locks:
	end_mutex.Unlock();
	end_mutex.Lock();
	start_mutex.Unlock();
	start_mutex.Lock();
	//restart_mutex.Unlock();
	//restart_mutex.Lock();
	isidle=true;
	retvalue=TRET_KILLED;
}

void ThreadList::WaitThreadAll() {
	if(pNext) 
		pNext->WaitThreadAll();
	WaitThread();
}		


ThreadList::~ThreadList() {
	if(pNext) 
		delete pNext;
	WaitThread();
	if(params) {
		delete [] params;
		params=NULL;
	}
	if(cParam) {
		delete [] cParam;
		cParam=NULL;
	}
	// should only be valid at first item of list 
	if(pSharedMutex)
		delete pSharedMutex;
	if(pSharestateMutex)
		delete pSharestateMutex;
		
}