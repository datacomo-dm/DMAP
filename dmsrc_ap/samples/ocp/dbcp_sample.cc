#ifdef WIN32
#include <process.h>
#define getch getchar
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

int Start(void *ptr);
typedef struct stru_dbcp_param
{
    int s_type;
    char s_un[30];
    char s_pwd[80];
    char s_sn[100];      
    
    int d_type;
    char d_un[30];
    char d_pwd[80];
    char d_sn[100];    
    
    char sqlst[1024];  
    char desttb[256];
    int fetchn;    
    
    stru_dbcp_param(){
        s_type = 2;
        d_type = 1;
        fetchn = 40000;
    }
    
}stru_dbcp_param,*stru_dbcp_param_ptr;

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    if(argc != 11){
        printf("input param error\n");
        printf("参数说明：源系统数据表类型(1:oracle,2:其他) 源系统用户名 源系统密码 源系统连接方式 目标系统数据表类型(1:oracle,2:其他) 目标系统用户名 目标系统密码 目标系统连接方式  目标表名  拷贝数据语句 \n");
        printf("DM回写ORACLE:  ./dbcp_sample 2 root dbplus03 dp 1 user pwd  //127.0.0.1:1521/hljdss test_1 \"select * from dp.test1 limit 5\" \n");
        printf("ORACLE拷贝DM:  ./dbcp_sample 1 user pwd  //127.0.0.1:1521/hljdss 2 root dbplus03 dp test1 \"select * from dwainbackup.test_1 where rownum < 5\" \n");
        return -1;   
    }
    stru_dbcp_param _st_obj;
        
    _st_obj.s_type = atoi(argv[1]);
    strcpy(_st_obj.s_un, argv[2]);
    strcpy(_st_obj.s_pwd, argv[3]);
    strcpy(_st_obj.s_sn, argv[4]);
    
    _st_obj.d_type = atoi(argv[5]);
    strcpy(_st_obj.d_un, argv[6]);
    strcpy(_st_obj.d_pwd, argv[7]);
    strcpy(_st_obj.d_sn, argv[8]);
    
    strcpy(_st_obj.desttb, argv[9]);
    strcpy(_st_obj.sqlst, argv[10]);
    
    
    WOCIInit("woci_ocp");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false);
    
    nRetCode=wociMainEntrance(Start,true,(void*)&_st_obj,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    stru_dbcp_param *pobj = (stru_dbcp_param*)ptr;
    
    AutoHandle dts;
    mytimer tm;        
    dts.SetHandle(wociCreateSession(pobj->s_un,pobj->s_pwd,pobj->s_sn,pobj->s_type==1?DTDBTYPE_ORACLE:DTDBTYPE_ODBC));
        
    AutoHandle dtd;
    dtd.SetHandle(wociCreateSession(pobj->d_un,pobj->d_pwd,pobj->d_sn,pobj->d_type==1?DTDBTYPE_ORACLE:DTDBTYPE_ODBC));
    
    AutoStmt srcst(dts);
    srcst.Prepare(pobj->sqlst);
    TradeOffMt mt(0,pobj->fetchn);
    
    mt.Cur()->Build(srcst);
    mt.Next()->Build(srcst);
    
    if(!wociTestTable(dtd,pobj->desttb)) {   	
    	printf("建表(create table)'%s'...\n",pobj->desttb);
	    wociGeneTable(*mt.Cur(),pobj->desttb,dtd);
    }
    
    tm.Start();
    mt.FetchFirst();
    int trn=0;
    printf("开始复制 ... [%s]\n",pobj->sqlst);
    int rrn=mt.Wait();
    while(rrn>0) {
    	mt.FetchNext();
    	wociAppendToDbTable(*mt.Cur(),pobj->desttb,dtd,true);
    	trn+=rrn;
    	rrn=mt.Wait();
    	printf(".");fflush(stdout);
    }
    tm.Stop();
    printf("\n共处理%d行(时间%.2f秒)。\n",trn,tm.GetTime());
    return 1;
}
	
