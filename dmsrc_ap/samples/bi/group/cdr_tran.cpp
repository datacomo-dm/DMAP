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
int Start(void *ptr);
int argc;
char **argv; 	
int main(int _argc,char *_argv[]) {
    WOCIInit("stat");
    argc=_argc;
    argv=_argv;
    int nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    int rp=0;
    char s_tbname[20];
    int  l_month;
    char s_begday[3];
    char s_endday[3];
    char s_flag[2];
    char s_partid1[5];
    char s_partmonth[2];
    
    if (argc != 7)
    {
        printf("命令行参数格式: cdr_tran table_name(tab_cdma/gsmvoicdr) part_month begday endday flag(0:cdr_cnt 1:tran_cnt) areaid\n");
        exit(0);
    }
    
    bzero(s_tbname,sizeof(s_tbname));    
    bzero(s_partmonth,sizeof(s_partmonth));    
    bzero(s_begday,sizeof(s_begday));    
    bzero(s_endday,sizeof(s_endday));    
    bzero(s_flag,sizeof(s_flag));    
    bzero(s_partid1,sizeof(s_partid1));    
    
    strcpy(s_tbname,argv[1]);
    strcpy(s_partmonth,argv[2]);
    strcpy(s_begday,argv[3]);
    strcpy(s_endday,argv[4]);
    strcpy(s_flag,argv[5]);
    strcpy(s_partid1,argv[6]);
    
    s_tbname[strlen(s_tbname)]='\0';
    s_partmonth[strlen(s_partmonth)]='\0';
    s_begday[strlen(s_begday)]='\0';
    s_endday[strlen(s_endday)]='\0';
    s_flag[strlen(s_flag)]='\0';
    s_partid1[strlen(s_partid1)]='\0';
        
    wociSetOutputToConsole(TRUE);
    lgprintf("连接到CUYN30...");
    AutoHandle dts;
    //dts.SetHandle(wociCreateSession("sunh","sunh123","cuyn17",DTDBTYPE_ORACLE));
    //dts.SetHandle(wociCreateSession("sunh","sunh123","cuyn17_9i",DTDBTYPE_ORACLE));
    dts.SetHandle(wociCreateSession("sunhua","sunh123","cuyn30",DTDBTYPE_ORACLE));
    //dts.SetHandle(wociCreateSession("sunh","sunh123","cuyn17_bc",DTDBTYPE_ORACLE));
    //dts.SetHandle(wociCreateSession("system","gcmanager","dtagt80",DTDBTYPE_ORACLE));
        
    mytimer mt_1;
    mt_1.Start();
    
    TradeOffMt mt(0,50000);
    AutoStmt stmt(dts);
    
    if (strcmp(s_flag,"0")==0)
    {
    	stmt.Prepare("select msisdn,BILLMONTH,SUBSCRBID,PART_ID1,PART_ID2 from cdrusr.%s%s@dblnk_to17bc where part_id2>=to_number(%s) and part_id2<=to_number(%s) and part_id1='%s' ",s_tbname,s_partmonth,s_begday,s_endday,s_partid1);    
    }
    else
    {
    	   stmt.Prepare("select msisdn,BILLMONTH,SUBSCRBID,PART_ID1,PART_ID2 from cdrusr.%s%s@dblnk_to17bc where part_id2>=to_number(%s) and part_id2<=to_number(%s) and part_id1='%s' and servicetype='002'",s_tbname,s_partmonth,s_begday,s_endday,s_partid1);    
    }
    
    mt.Cur()->Build(stmt);
    mt.Next()->Build(stmt);
    mt.FetchFirst();
    int rn=mt.Wait();
    AutoMt result(dts,5000000);
        
    result.SetGroupParam(*mt.Cur(),"msisdn,BILLMONTH,SUBSCRBID,PART_ID1,PART_ID2","");
        
    while (rn>0)
    {
    	mt.FetchNext();
	wociSetGroupSrc(result,*mt.Cur());
        wociGroup(result,0,rn);
    	rn=mt.Wait();
    }

    wociSetTraceFile("dtadmin");

    wociSetOutputToConsole(TRUE);
    lgprintf("连接到CUYN30...");
    AutoHandle dts_17;
    dts_17.SetHandle(wociCreateSession("sunhua","sunh123","cuyn30",DTDBTYPE_ORACLE));
    //wociMTPrint(result,0,NULL);
    if ((strcmp(s_flag,"0")==0)&&(strcmp(s_tbname,"tab_cdmavoicdr")==0))
    {
    	result.CreateAndAppend("tab_cdmavoicdr_stati_t1",dts_17);        
    }
    else if ((strcmp(s_flag,"1")==0)&&(strcmp(s_tbname,"tab_cdmavoicdr")==0))
    {
    	result.CreateAndAppend("tab_cdmavoicdr_stati_t2",dts_17);        
    }
    else if ((strcmp(s_flag,"0")==0)&&(strcmp(s_tbname,"tab_gsmvoicdr")==0))
    {
    	result.CreateAndAppend("tab_gsmvoicdr_stati_t1",dts_17);        
    }
    else if ((strcmp(s_flag,"1")==0)&&(strcmp(s_tbname,"tab_gsmvoicdr")==0))
    {
    	result.CreateAndAppend("tab_gsmvoicdr_stati_t2",dts_17);        
    }
    
    wociCommit(dts_17);   

    mt_1.Stop();
    lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
    return 0;
}

