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
#include "dt_scan.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

//int argc;
//char **argv;
int Start(void *ptr);
int main(int _argc,char *_argv[]) {
    int nRetCode = 0;
    WOCIInit("samp/scan_samp");
    nRetCode=wociMainEntrance(Start,true,NULL,0);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) {  
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    dts.SetHandle(wociCreateSession("root","dbplus03","dt",DTDBTYPE_ODBC));
    DT_Scan *ds=CreateDTScan(dts);
    mytimer tm;
    tm.Start();
    ds->OpenTable(33);
    int smtrn;
    double trn=0;
    int dct=0;
    lgprintf("Table scan begin.");
    while(smtrn=ds->GetNextBlockMt())
    {
	trn+=smtrn;
 	dct+=smtrn;
	if(dct>200000) {
		printf(" scan  %.0f  rows\r.",trn);
		fflush(stdout);
		dct=0;
       }
    }
    lgprintf("Total rows :%.0f",trn);
    lgprintf("Test end.");
    tm.Stop();
    lgprintf("Consuming %7.3f second .",tm.GetTime());
    delete ds;
    return 1;
}
	
