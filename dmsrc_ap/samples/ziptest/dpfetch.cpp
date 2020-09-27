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
#include "dt_svrlib.h"
int Start(void *ptr);

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("samp/fetch/");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("连接到数据库...\n");
    dts.SetHandle(BuildConn(0));
    char sql[10000];
    while(true) {
     sql[0]=0;
     getString("FETCH_SQL",NULL,sql);
     if(sql[strlen(sql)-1]==';') sql[strlen(sql)-1]=0;
     if(strcmp(sql,"quit")==0 || strcmp(sql,"exit")==0) return 0;
     try {
     AutoMt mt(dts,2);
     mt.FetchFirst(sql);
     int rn=mt.Wait();
     int l=wociGetRowLen(mt);
     printf("单行记录长度%d字节.\n",l);
     int meml=getOption("FETCH 缓冲(MB)<20>",20,2,2000);
     mt.SetMaxRows(meml*1024*1024/l);
     int fetchn=getOption("FETCH 次，0表示不限次。<0>",0,0,20000);
     int sleepn=getOption("间隔时间(秒).<0>",0,0,2000);
     mytimer mtm;
     mtm.Clear();
     mtm.Start();
     mt.FetchFirst(sql);
     if(fetchn==0) 
        fetchn=-1;
     LONG64 trn=0;
     rn=mt.Wait();
     trn+=rn;
     while(rn>0 && (fetchn>1|| fetchn==-1) ) {
       mt.FetchNext();
       rn=mt.Wait();
       trn+=rn;
       if(fetchn!=-1) 
		--fetchn;
       printf(".");
       fflush(stdout);
       if(sleepn>0)
          sleep(sleepn);
     }
     mtm.Stop();
     printf("记录数:%lld,时间:%.2fs.\n",trn,mtm.GetTime());
     }
     catch(...) {}
    }     
   return 0;
}

                             
