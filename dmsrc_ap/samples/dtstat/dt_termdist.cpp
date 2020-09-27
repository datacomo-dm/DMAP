#include "dt_lib.h"
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h> 
#endif
#include <stdlib.h>
#include <stdio.h>
int Start(void *ptr);
char RES_TAB_NAME[30]= "tab_userana_200403";
char TABID[30]="tab_gsmvoicdr3";
//#define TABID 3
#define FIRSTOFF 1
  	
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    if(argc!=2) {
    	printf("Usage: termst [tabname].\nDatabase name use default -'dest'.\n");
    	return -1;
    }
    strcpy(TABID,argv[1]);
    strcpy(RES_TAB_NAME,argv[1]);
    strcat(RES_TAB_NAME,"_termst");
    WOCIInit("termst");
    nRetCode=wociMainEntrance(Start,true,NULL);
    WOCIQuit(); 
    return nRetCode;
}

void DirFullAccessTest(AutoHandle &dts) {
	mytimer tm;
	mytimer tm1,tm2;
	int rn;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	/***************全表直接访问测试 */
	tm.Start();
	TableScan ts(dts);
	ts.OpenTable("dest",TABID);
	int mt;
	int trn=0;
	lgprintf("Table scan begin.");
	tm1.Clear();
	tm2.Clear();
	int oldrm=0;
	AutoMt resmt(0,18000000);
	tm1.Start();
	mt=ts.GetNextBlockMt();
	tm1.Stop();
	if(!mt) return;
	resmt.SetGroupParam(mt,"termphone,msisdn",NULL);
//	stmt.SetGroupParam(mt,"areaid,calltype,term_type","basfee,TOLLFEE,LOCADDFEE");
	while(mt)
	{
		tm2.Start();
		rn=wociGetMemtableRows(mt);
		char *pst,*ptpt,*ptt;
		int stlen,tptlen,ttlen;
		wociGetStrAddrByName(mt,"servicetype",0,&pst,&stlen);
		wociGetStrAddrByName(mt,"termphonetype",0,&ptpt,&tptlen);
		wociGetStrAddrByName(mt,"term_type",0,&ptt,&ttlen);
		for(int i=0;i<rn;i++) {
			if(strcmp(pst+stlen*i,"000")!=0) 
				wociQDeleteRow(mt,i);
			else if(strcmp(ptpt+tptlen*i,"Y")!=0)
				wociQDeleteRow(mt,i);
			else if(strcmp(ptt+ttlen*i,"G")!=0)
				wociQDeleteRow(mt,i);
		}
		//wociCompressBf(mt);
		wociSetGroupSrc(resmt,mt);
		rn=wociGetMemtableRows(mt);
		trn+=rn;
		if(trn%200000<oldrm) {
			//static int mct=0;
			//if(mct++==5) {
			//	printf("%d rows.",wociGetMemtableRows(stmt));
			//	mct=0;
			//}
			printf(".");fflush(stdout);
		}
		oldrm=trn%200000;
		//if(rn>0)
                //  wociGroup(resmt,0,rn);
		tm2.Stop();
		//if(trn>20000000) break;
		tm1.Start();
		mt=ts.GetNextBlockMt();
		tm1.Stop();
	}
	rn=wociGetMemtableRows(resmt);
	lgprintf("Total rows :%d,group to %d rows",trn,rn);
	AutoMt endmt(0,18000000);
	endmt.SetGroupParam(resmt,"termphone",NULL);
	wociReInOrder(resmt);
        wociGroup(endmt,0,rn);
//	
	wociReInOrder(endmt);
	rn=wociGetMemtableRows(endmt);
	lgprintf("End mt rows :%d.",rn);
 	lgprintf("Test end.");
 	if(rn>10)
 	 wociMTPrint(endmt,10,NULL);
 	tm.Stop();
        //AutoHandle dts1;
        //dts1.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
        //AutoStmt insst(dts1);
        //insst.Prepare("insert into 
        //stmt.CreateAndAppend("tab_rival",dts1);
 	//endmt.CreateAndAppend(RES_TAB_NAME,dts);
        //wociCommit(dts);
 	lgprintf("Consuming %7.3f second(rd:%.3f,grp:%.3f) .",tm.GetTime(),tm1.GetTime(),tm2.GetTime());
}

int Start(void *ptr) { 
    AutoHandle dts;
    dts.SetHandle(wociCreateSession("system","gcmanager","dtagt189"));
    DirFullAccessTest(dts);
   return 0;
	mytimer tm;
	mytimer tm1,tm2;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	/***************全表直接访问测试 */
	tm.Start();
    TradeOffMt mt(0,50000);
    AutoStmt stmt(dts);
    stmt.Prepare("select msisdn,termphone from %s where servicetype='000' and termphonetype='Y' and  term_type='G'",TABID);
    mt.Cur()->Build(stmt);
    mt.Next()->Build(stmt);
    mt.FetchFirst();
    int rn=mt.Wait();
    AutoMt result(dts,25000000);
    result.SetGroupParam(*mt.Cur(),"termphone,msisdn",NULL);
    int trn=0;
    while (rn>0)
    {
    	mt.FetchNext();
	wociSetGroupSrc(result,*mt.Cur());
        wociGroup(result,0,rn);
        trn+=rn;
    	rn=mt.Wait();
    	printf("-%d/%d-",trn,wociGetMemtableRows(result));
    	//if(trn>100000) break;
    }
    rn=wociGetMemtableRows(result);
	lgprintf("Total rows :%d,group to %d rows",trn,rn);
	AutoMt endmt(0,18000000);
	endmt.SetGroupParam(result,"termphone",NULL);
	wociReInOrder(result);
        wociGroup(endmt,0,rn);
//	
	wociReInOrder(endmt);
	rn=wociGetMemtableRows(endmt);
	lgprintf("End mt rows :%d.",rn);
 	lgprintf("Test end.");
 	if(rn>10)
 	 wociMTPrint(endmt,10,NULL);
 	tm.Stop();
    	endmt.CreateAndAppend(RES_TAB_NAME,dts);
        wociCommit(dts);
 	lgprintf("运行时间%f秒 正常结束",tm.GetTime());
        //AutoHandle dts1;
        //dts1.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
        //AutoStmt insst(dts1);
        //insst.Prepare("insert into 
        //stmt.CreateAndAppend("tab_rival",dts1);
 	//lgprintf("Consuming %7.3f second(rd:%.3f,grp:%.3f) .",tm.GetTime(),tm1.GetTime(),tm2.GetTime());
    return 1;
}

