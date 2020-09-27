#ifdef WIN32
#include <process.h>
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
#include <math.h>

int Start(void *ptr);

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("samp/desc");
    nRetCode=wociMainEntrance(Start,false,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetTraceFile("samp/desc");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(FALSE);
    AutoHandle dts;
    printf("连接到数据库...\n");
    dts.SetHandle(BuildConn(0));
    char sql[10000];
    bool dispval=GetYesNo("显示结果吗？<Y/N>",false);
    while(true) {
     sql[0]=0;
     getString("DPSQL",NULL,sql);
     if(sql[strlen(sql)-1]==';') sql[strlen(sql)-1]=0;
     if(strcmp(sql,"quit")==0 || strcmp(sql,"exit")==0) return 0;
     {
     	AutoMt mt(dts,20);
     	AutoStmt st(dts);
     try {
     if(dispval) {
    	mt.FetchFirst(sql);
    	int rn=mt.Wait();
    	wociMTCompactPrint(mt,0,NULL);
     }    		
     else {
    	st.Prepare(sql);
    	mt.Build(st);
    	char info[3000];
    	wociGetColumnInfo(mt,info,false);
    	printf("字段结构:\n%s.\n",info);
     }	
     }
     catch(...) {};
	}
    }
    
    return 1;
}
	
