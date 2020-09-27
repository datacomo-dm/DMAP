#ifndef MT_WORKER_HEADER
#define MT_WORKER_HEADER

#ifdef __unix
#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>
#else
#include <windows.h>
#include <process.h>
#include <conio.h>
#endif
#include <stdio.h>
#include <string.h>

void * LaunchWork(void *ptr) ;

//工作线程封装类，支持内部独立缓存管理
// DM  dpadmin 二次压缩 DMA_MTLOADER (DMA MTQuery?)共用
class worker {
protected :
	char *inbuf;
	int inbuflen;
	char *outbuf;
	int outbuflen;
	int workid;
	bool isidle;
	bool isdone;
	int blockoffset;
	int islocked;
	unsigned long dstlen;
	void **objects;//外部对象
	worker *pNext;
#ifdef __unix
	pthread_t hd_pthread_t;
	pthread_mutex_t status_mutex;
#else
	CRITICAL_SECTION status_mutex;
#endif

public :
	worker() {
		inbuf=outbuf=NULL;
		inbuflen=outbuflen=0;
		isidle=true;dstlen=0;isdone=false;
		pNext=NULL;islocked=0;objects=NULL;
#ifdef __unix
	if(pthread_mutex_init(&status_mutex,NULL)!=0) ;
		//ThrowWith("Create mutex for worker failed.");
#else
	InitializeCriticalSection(&status_mutex);
//(NULL,false,NULL);
//	if(status_mutex==NULL) ThrowWith("Create mutex for worker failed.");
#endif

	}

	virtual int work() =0;
	
	int idlenumber() {
		int num=0;
		if(isidle) num=1;
		if(pNext) return num+pNext->idlenumber();
		return num;
	}

	bool isidleall() {
		if(!isidle) return false;
		if(pNext) return pNext->isidleall();
		return true;
	}
	void AddWorker(worker *pwk) {
		if(pNext) pNext->AddWorker(pwk);
		else pNext=pwk;pwk->pNext=NULL;
	}
	
	int LockStatus() {
	 #ifdef __unix
	 int rt=pthread_mutex_lock(&status_mutex);
	 #else
	 DWORD rt=WAIT_OBJECT_0;
	 EnterCriticalSection(&status_mutex);
	 #endif
	 islocked++;
	 return rt;
	}
	int GetWorkID() { return workid;}
	unsigned long GetOutput(char **pout) {
		*pout=outbuf;
		ReleaseDone();
		return dstlen;
	}
	void ReleaseDone() {
		isdone=false;
		Unlock();
	}
	bool LockIdle() {
		LockStatus();
		if(isdone || !isidle) {
			Unlock();
			return false;
		}
		
		return true;
	}

	bool LockDone() {
		LockStatus();
		if(!isdone) {
			Unlock();
			return false;
		}
		isidle=true;
		return true;
	}

	void Unlock()
	{
		#ifdef __unix
		pthread_mutex_unlock(&status_mutex);
		#else
		LeaveCriticalSection(&status_mutex);
		#endif
		islocked--;
	}

	worker *GetIdleWorker() {
		if(LockIdle()) return this;
		else if(pNext) return pNext->GetIdleWorker();
		return NULL;
	}
	
	worker *GetDoneWorker() {
		if(LockDone()) return this;
		else if(pNext) return pNext->GetDoneWorker();
		return NULL;
	}

	char *getinbuf(int len) {
		if(inbuflen<len) {
			if(inbuf) delete []inbuf;
			inbuf=new char [(int)(len *1.3)];
			inbuflen=(int)(len*1.3);
		}
		return inbuf;
	}
	char *getoutbuf(int len) {
		if(outbuflen<len) {
			if(outbuf) delete []outbuf;
			outbuf=new char [(int)(len *1.3)];
			outbuflen=(int)(len*1.3);
		}
		return inbuf;
	}
	
	void Do(int _workid,const char *in,int inlen,int _blockoff,int estDstlen) {
		blockoffset=_blockoff;
		memcpy(getinbuf(inlen),in,inlen);
		getoutbuf(estDstlen);
		Start(_workid,NULL);
	}
	
	void Start(int _workid,void **_objects) {
		isidle=false;
		isdone=false;
		dstlen=0;
		if(objects) delete [] objects;
		objects=_objects;
		workid=_workid;
		Unlock();
#ifndef __unix
		_beginthread(LaunchWork,81920,(void *)this);
#else
		pthread_create(&hd_pthread_t,NULL,LaunchWork,(void *)this);
		pthread_detach(hd_pthread_t);
#endif
  }		
	virtual ~worker() {
		if(pNext) delete pNext;
		//Wait task idle;
		LockIdle();
		Unlock();
		if(inbuf) delete [] inbuf;
		if(outbuf) delete [] outbuf;
		if(objects) delete [] objects;
		objects=NULL;
		inbuf=outbuf=NULL;
		pNext=NULL;
		inbuflen=outbuflen=0;
#ifdef __unix
		pthread_mutex_unlock(&status_mutex);
		pthread_mutex_destroy(&status_mutex);
#else
		DeleteCriticalSection(&status_mutex);
		//ReleaseMutex(status_mutex);
		//CloseHandle(status_mutex);
#endif
	}
};


#include <time.h>
 #ifndef mSleep
 #define mSleep(msec) \
 { \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=msec%1000*1000*1000;\
   nanosleep(&req, &rem);\
 }
 #endif

#endif

