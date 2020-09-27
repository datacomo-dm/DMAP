#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "woci8intface.h"
#include "AutoHandle.h"
int Start(void *ptr);
 class mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec=ed.tv_sec-st.tv_sec;
	ct.tv_nsec=ed.tv_nsec-st.tv_nsec;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};
 	
int main(int argc,char *argv[]) {
    WOCIInit("cdrstat");
    int nRetCode=wociMainEntrance(Start,true,NULL);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    int rp=0;
    wociSetOutputToConsole(TRUE);
    lgprintf("连接到Oracle&MySQL...");
    AutoHandle dts;
    dts.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
    AutoMt mt(dts,500000);
    mytimer mytimer1;
    mytimer1.Start();
    mt.FetchFirst("select areaid,basfee from cdrusr.tab_gsmvoicdr4 where part_id2='26' and part_id1='860'");
    int rn=mt.Wait();
    AutoMt result(dts,100);
    result.SetGroupParam(mt,"areaid","basfee");
    while (rn>0)
    {
        wociGroup(result,0,rn);
    	mt.FetchNext();
    	rn=mt.Wait();
    	printf("%d rows fetched.\n",rn);
    }
    wociMTPrint(result,0,NULL);   
    wociSetTraceFile("dtadmin");
    AutoHandle dts2;
    dts2.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
    result.CreateAndAppend("cdrstat",dts2);
//    int *ptr=result.PtrInt("basfee",0);
//    int rrn=wociGetMemtableRows(result);
//    for(int i=0;i<rn;i++) {
//    	if(ptr[i]>10000) {
//    	}	
//    	else {
//	}	
//    }
    wociCommit(dts2);
    mytimer1.Stop();
//    if(wociTestTable(dts2,"cdrstat")) {
//    	wociGeneTable(result,"cdrstat",dts2);
//    }
//    wociAppendToDbTable(result,"cdrstat",dts2);
    /**********************/
    lgprintf("%f 正常结束",mytimer1.GetTime()/1000);
    return 0;
}

