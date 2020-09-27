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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int Start(void *ptr);
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("dt_dstldr");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetTraceFile("dptest\fetch_test");
    //wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts,dts1;
//    dts.SetHandle(wociCreateSession("wanggsh","wanggsh","cuyn18",DTDBTYPE_ORACLE));
//    dts1.SetHandle(wociCreateSession("dtuser","readonly","",DTDBTYPE_ORACLE));
    //dts.SetHandle(wociCreateSession("root","cdr0930","dp",DTDBTYPE_ODBC));
    dts.SetHandle(wociCreateSession("dtuser","readonly","//localhost:1521/dtagt",DTDBTYPE_ORACLE));
    //dts1.SetHandle(wociCreateSession("root","cdr0930","localmysql",DTDBTYPE_ODBC));
    
    mytimer mtm;
    mtm.Start();
    AutoMt mt(dts,1000000);
    //AutoMt mt2(dts1,300000);
    // mt.FetchAll("select * from obs.view_cdmavpn_voicdr1 where rownum<100");
    // wociSetFetchSize(mt.GetStmt(),30000);
    //wociSetEcho(FALSE);
    //mt.FetchAll("select svcid,areaid,termphone from dest.TAB_CDMAVOICDR3@dblk_cdrsvr ");
    //mt.FetchFirst("select * from dest.tab_cdmavoicdr2");
    //mt.FetchFirst("select * from dt.dt_path");//@dblk_cdrsvr");
    //mt.FetchFirst("select termphone,begindate,begintime from dest.tab_cdmavoicdr3@dblk_cdrsvr");
    //mt.FetchFirst("select termphone,begindate,begintime from dest.tab_gsmvoicdr3 limit 100000");
    //mt.FetchFirst("select * from dt.dt_taskschedule@dblk_cdrsvr");
    //mt.FetchFirst("select * from dest.tab_cdmavoicdr3@dblk_cdrsvr");
    int totrn=0;
    mt.FetchFirst("select * from cdrusr.tab_cdmavoicdr2@dblk_cdrsvr where rownum<1000001");
//    mt.FetchFirst("select termphone,begindate,begintime from dest.tab_cdmavoicdr3");
    //mt.FetchFirst("select count(*),sum(basfee)  from dest.tab_cdmavoicdr3 where msisdn='13308719031'");
    int rn=mt.Wait();
    totrn+=rn;
//	mt2.Clear();
	//wociCopyColumnDefine(mt2,mt,NULL);
	//mt2.Build();

    //mt.CreateAndAppend("tab_cust_730",dts1);
    //while(rn>0) {
    //	mt.FetchNext();
    //	rn=mt.Wait();
    //	totrn+=rn;
    //    mt.CreateAndAppend("tab_cust_730",dts1);
    //	if(totrn>10000000) break;
    //}
    
    //mt.FetchFirst("select * from dest.tab_gsmvoicdr3");
    //mt.FetchFirst("select * from dest.tab_gsmvoicdr3");
    //rn=mt.Wait();
    //wociMTPrint(mt,10,NULL);
    
    mtm.Stop();
    printf("Fetched %d rows in %.2f sec.\n",totrn,mtm.GetTime());
    mytimer mtm1;
    mtm1.Start();
    mtm.Restart();
    wociSetSortColumn(mt,"part_id1,part_id2,calltype,servicetype,servicecode");
    wociSort(mt);
    mtm1.Stop();
    wociSetSortColumn(mt,"part_id1,subscrbid,prcplnid,msisdn");
    wociSortHeap(mt);
    mtm.Stop();
    printf("Sort time %.2f(%.2f) sec.\n",mtm.GetTime(),mtm1.GetTime());
    /*
    mytimer mtm1;
    int rep=300000;
    mtm1.Start();
    for(int i=0;i<rep;i++) {
    	wociCopyRowsTo(mt,mt2,-1,i,1);
    }
    mtm1.Stop();
    printf("Time for CopyRowsTo:loop %d,tm:%.4f.\n",rep,mtm1.GetTime());
    */
    //int ck=wociTestTable(dts1,"dt.woci_test1");
    //printf("Test Table :%d.\n",ck);
    //if(ck) wociGeneTable(mt,"dt.woci_test1",dts1);
    //wociAppendToDbTable(mt,"dt.woci_test1",dts1,true);
    //wociCommit(dts1);
    //int file=open("    
    return 1;
}
	
