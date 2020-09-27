#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "wdbi_inc.h"
#include "dbiAutoHandle.h"
int Start(void *ptr);
int argc;
char **argv; 	
int main(int _argc,char *_argv[]) {
    WDBIInit("stat");
    argc=_argc;
    argv=_argv;
    int nRetCode=wdbiMainEntrance(Start,true,NULL,2);
    WDBIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    int rp=0;
//    char s_areaid[4];
    char s_svcid[3];
    char s_ny[5];
    char s_sql[1024];
    long l_ny = 0;
    
    if (argc != 4)
    {
        printf("命令行参数格式: stat_cdrdt 业务类型 年月 WHERE_SQL\n");
        exit(0);
    }
    
    bzero(s_svcid,sizeof(s_svcid));
    bzero(s_ny,sizeof(s_ny));
    bzero(s_sql,sizeof(s_sql));
    
    strcpy(s_svcid,argv[1]);
    strcpy(s_ny,argv[2]);
    l_ny=atol(s_ny);
    strcpy(s_sql,argv[3]);
    s_sql[strlen(s_sql)]='\0';
    
    wdbiSetOutputToConsole(TRUE);
    lgprintf("连接到DTAGT...");
    //lgprintf("连接到CUYN17...");
    AutoHandle dts1;
    //dts1.SetHandle(wdbiCreateSession("root","cdr0930","localmysql",DTDBTYPE_ODBC));
    dts1.SetHandle(wdbiCreateSession("dtuser","readonly","dtagt",DTDBTYPE_ORACLE));
    AutoHandle dts;
    dts.SetHandle(wdbiCreateSession("dtuser","readonly","dtagt",DTDBTYPE_ORACLE));
    //dts.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
    
    AutoMt s_time(dts,1);
    s_time.FetchAll("select to_char(sysdate,'yyyymmddhh24miss') col_sysdate,to_char(to_number(to_char(sysdate-1,'mm'))) col_month from dual");
    s_time.Wait();
    char start_time[20];
    strcpy(start_time,s_time.PtrStr("col_sysdate",0));
    char s_month[3];
    strcpy(s_month,s_time.PtrStr("col_month",0));
    
    mytimer mt_1;
    mt_1.Start();
    
    TradeOffMt mt(0,10000);
    AutoStmt stmt(dts1); 
        
    if (strcmp(s_svcid,"10")==0)
    {
//    	stmt.Prepare("select %ld month,decode(trim(othcode),'','SUNH',trim(othcode)) othcode,'0' flag,TERMPHONE,calltimelen from dest.tab_gsmvoicdr%s@dblk_cdrsvr",l_ny,s_sql);
    	stmt.Prepare("select othcode,TERMPHONE,calltimelen from dest.tab_gsmvoicdr%s@dblk_cdrsvr",s_sql);
    	//stmt.Prepare("select %ld month,decode(trim(othcode),'','SUNH',trim(othcode)) othcode,'0' flag,TERMPHONE,calltimelen from v_sunh_gsmcdr",l_ny,s_sql);
    }
    else
    {
//    	stmt.Prepare("select %ld month,decode(trim(othcode),'','SUNH',trim(othcode)) othcode,'0' flag,TERMPHONE,calltimelen from dest.tab_cdmavoicdr%s@dblk_cdrsvr",l_ny,s_sql);
    	stmt.Prepare("select othcode,TERMPHONE,calltimelen from dest.tab_cdmavoicdr%s@dblk_cdrsvr",s_sql);
    }
    
    mt.Cur()->Build(stmt);
    mt.Next()->Build(stmt);
    mt.FetchFirst();
    int rn=mt.Wait();
    AutoMt result(dts,30000000);
        
    result.SetGroupParam(*mt.Cur(),"othcode,TERMPHONE","calltimelen");
            
    while (rn>0)
    {
    	mt.FetchNext();
	wdbiSetGroupSrc(result,*mt.Cur());
        wdbiGroup(result,0,rn);
    	rn=mt.Wait();
    }
/*
    lgprintf("连接到CUYN17...");
    AutoHandle dts_17;
    dts_17.SetHandle(wociCreateSession("sunh","sunh123","cuyn17"));
    
    if (strcmp(s_svcid,"10")==0)
    {
    	result.CreateAndAppend("tab_rival_gsm",dts_17);
    }
    else
    {
    	result.CreateAndAppend("tab_rival_cdma",dts_17);
    }
    
    wociCommit(dts_17);
*/    
//    e_time.FetchAll("select to_char(sysdate,'yyyymmddhh24miss'),to_number() from dual");
//    e_time.Wait();
//    char end_time[20];
//    strcpy(end_time,e_time.PtrStr(0,0));
//    
//    AutoStmt stmt(dts2);
//    stmt.Prepare("insert into stat_log values ('%ld','%s','%s','%ld')",1,start_time,end_time,0);
//    stmt.Execute(1);
//    stmt.Wait();
//    wociCommit(dts2);

    mt_1.Stop();
    lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
    return 0;
}

