// odf.cpp : Defines the entry point for the console application.
//

#ifdef WIN32
#include "dtgdi_inc.h"
#endif
#include "odf.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

int pSleep(int msec) 
{ 
   struct timespec req,rem;
   req.tv_sec=msec/1000;
   req.tv_nsec=msec%1000*1000*1000;
   return nanosleep(&req, &rem);
}

int Start(void *ptr);
int main(int argc, char* argv[])
{
    WOCIInit("odf");
	filetrans::InitTransfer();
    int nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
	filetrans::QuitTransfer();
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
#ifdef WIN32
	char un[100],pwd[100],svcn[100],newpwd[100];
	if(!GetDBConnParam(un,pwd,svcn)) return -1;
	if(!GetLogin(un,pwd)) return -1;
	if(!GetNewPassword(un,pwd,newpwd)) return -1;
#endif
	param_db pdb("dtuser","readonly","dtagt");
	dataset_task dtsk(&pdb);
	filetrans ft(&pdb);
	datablockstore dbs(&pdb,&ft);
	int ds_id,ext_st,ds_sn;
#define TEST_EXT
#ifdef TEST_EXT
       while(true) {
       	if(dtsk.GetCompleteTask(ds_id,ds_sn)) {
       		dataset_svr ds(&pdb,&dbs,ds_id);
		ds.DoClear(ds_sn);
       	}
	if(dtsk.GetExtTask(ds_id,ext_st))
	{
		dataset_svr ds(&pdb,&dbs,ds_id);
		ds.DoExtract();
	}
        else {
	      	printf("   暂无数据需要抽取...\r");
	      	fflush(stdout);
	      	if(pSleep(5000)==-1) break;
	}
       }
#else
	int client_id=2003;
	int ds_sn=0;
	while(true) {
	      if(dtsk.GetLoadTask(client_id,ds_id,ds_sn)) {
                destload dl(&pdb,&dbs,client_id,ds_id,ds_sn);
		dl.Load();
	      }
	      else {
	      	printf("   暂无数据需要装入...\r");
	      	fflush(stdout);
	      	if(pSleep(5000)==-1) break;
	      }
	}
#endif
	return 0;
}
