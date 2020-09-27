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
//#include <GetPot>
//日志文件名
#define LOGPATH "bi/group"
int Start(void *ptr);
int argc;
char  **argv;  
int main(int _argc,char *_argv[]) {
    int nRetCode = 0;
    argc=_argc;
    argv=_argv;
    if(argc!=2) {
    	//printf("使用方法:bigroup <control_file>.\n");
    	printf("使用方法:bigroup <month>.\n");
    	return 1;
    }
    WOCIInit(LOGPATH);
    nRetCode=wociMainEntrance(Start,true,NULL/*_argv[1]*/,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ctrlfile) {
    //grpparam gp((const char *)ctrlfile);
    //wociSetTraceFile(gp.logfile.c_str());
    wociSetTraceFile(LOGPATH);
    wociSetOutputToConsole(TRUE);
    wociSetEcho(TRUE);
    AutoHandle dts;
    printf("连接到源数据库...\n");
    
    dts.SetHandle(wociCreateSession("root","cdr0930","dtagt80",DTDBTYPE_ODBC));
    mytimer mt_1;
    mt_1.Start();
    TradeOffMt mt(0,50000);
    AutoStmt stmt(dts);
    //带group的原始SQL:
    /*
    select part_id1,subscrbid,msisdn svcnum,calltype,roamtype,islocal,tolltype,term_type,termphonetype,
(prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)>0.001 flag,
sum(prcpln_basfee/1000) prcpln_basfee,sum(prcpln_LOCADDFEE/1000) prcpln_LOCADDFEE,sum(prcpln_TOLLFEE/1000) prcpln_TOLLFEE,
sum(prcpln_TOLLADDFEE/1000) prcpln_TOLLADDFEE ,sum(DISCNTbasfee/1000) DISCNTbasfee,sum(DISCNTTOLLFEE/1000) DISCNTTOLLFEE,
sum(DISCNTADDFEE/1000) DISCNTADDFEE,sum(prcpln_INFOFEE/1000) prcpln_INFOFEE,sum(DISCNTINFOFEE/1000) DISCNTINFOFEE ,
sum((prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)/1000) sumfee,
sum(ctimes) ctimes,sum(ltimes) ltimes,sum(calltimelen) caltimelen  from dest.tab_cdmavoicdr6
where servicetype in ('000','002') and msisdn='13308829999'
group by part_id1,subscrbid,msisdn ,calltype,roamtype,islocal,tolltype,term_type,termphonetype,
(prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)>0.001
	*/
    //校验SQL:
    //
    /*
    select round(sum(prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE),2) 
    from  tab_cdmavoicdr200701_st where svcnum='13308829999'
    select round(sum(sumfee),2) 
    from  tab_cdmavoicdr200701_st where svcnum='13308829999'
    */
    stmt.Prepare(" select part_id1,subscrbid,msisdn svcnum,calltype,roamtype,islocal,tolltype,term_type,termphonetype, "
" (prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)>0.001 flag, "
" prcpln_basfee/1000 prcpln_basfee,prcpln_LOCADDFEE/1000 prcpln_LOCADDFEE,prcpln_TOLLFEE/1000 prcpln_TOLLFEE, "
" prcpln_TOLLADDFEE/1000 prcpln_TOLLADDFEE ,DISCNTbasfee/1000 DISCNTbasfee,DISCNTTOLLFEE/1000 DISCNTTOLLFEE, "
" DISCNTADDFEE/1000 DISCNTADDFEE,prcpln_INFOFEE/1000 prcpln_INFOFEE,DISCNTINFOFEE/1000 DISCNTINFOFEE , "
" (prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)/1000 sumfee, "
" ctimes,ltimes,calltimelen "
" from dest.tab_gsmvoicdr1 "
" where servicetype in ('000','002') and part_id1='%s' ",argv[1]);
    mt.Cur()->Build(stmt);
    mt.Next()->Build(stmt);
    AutoMt result(dts,10000000);
    result.SetGroupParam(*mt.Cur(),"part_id1,subscrbid,svcnum,calltype,roamtype,islocal,tolltype,term_type,termphonetype,flag","prcpln_basfee,prcpln_LOCADDFEE,prcpln_TOLLFEE,prcpln_TOLLADDFEE,DISCNTbasfee,DISCNTTOLLFEE,DISCNTADDFEE,prcpln_INFOFEE,DISCNTINFOFEE,sumfee,ctimes,ltimes,calltimelen");
    mt.FetchFirst();
    int rn=mt.Wait();
    while (rn>0)
    {
    	mt.FetchNext();
	wociSetGroupSrc(result,*mt.Cur());
        wociGroup(result,0,rn);
    	rn=mt.Wait();
    }
    lgprintf("连接到ods_temp@CUYN30...");
    AutoHandle dts_30;
    dts_30.SetHandle(wociCreateSession("ods_temp","ods_temp","//130.86.12.30:1521/ubisp.ynunicom.com",DTDBTYPE_ORACLE));
    result.CreateAndAppend("tab_gsmvoicdr200701_st",dts_30);        
    wociCommit(dts_30);   
    mt_1.Stop();
    lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
    return 0;
}
	
