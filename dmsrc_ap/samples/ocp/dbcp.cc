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
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("woci_ocp");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false);
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    AutoHandle dts;
    mytimer tm;
    printf("选择源数据库...\n");
    dts.SetHandle(BuildConn(0));
    AutoHandle dtd;
    printf("选择目标数据库...\n");
    dtd.SetHandle(BuildConn(0));
    char sqlst[2000];
    memset(sqlst,0,2000);
    //strcpy(sqlst,"select * from zkb.varchar_test");
   // printf("%s\n",sqlst);
    getString("请输入查询语句(中间不要回车)",NULL,sqlst);
    int fetchn=getOption("提交行数",10000,100,50000);
    char desttb[200];
    getString("请输入目标表名称",NULL,desttb);
    //memset(desttb,0,200);
    //strcpy(desttb,"zkb.test02");
    //printf("%s\n",desttb);
    AutoStmt srcst(dts);
    srcst.Prepare(sqlst);
    TradeOffMt mt(0,fetchn);
    mt.Cur()->Build(srcst);
    mt.Next()->Build(srcst);
    if(wociTestTable(dtd,desttb)) {
    	int sel=getOption("目标表已存在，追加(1)/删除重建(2)/清空后添加(3)/退出(4)",2,1,4);
    	if(sel==4) return -1;
    	if(sel==2) {
    		printf("删除表(drop table)'%s'...\n",desttb);
    		AutoStmt st(dtd);
		st.Prepare("drop table %s",desttb);
		st.Execute(1);
		st.Wait();
    		printf("建表(create table)'%s'...\n",desttb);
		wociGeneTable(*mt.Cur(),desttb,dtd);
	}
	else if(sel==3) {
    		printf("清空表(truncate table)'%s'...\n",desttb);
    		AutoStmt st(dtd);
		st.Prepare("truncate table %s",desttb);
		st.Execute(1);
		st.Wait();
	}
    }
    else  {   	
    	printf("建表(create table)'%s'...\n",desttb);
	wociGeneTable(*mt.Cur(),desttb,dtd);
    }
    tm.Start();
    mt.FetchFirst();
    int trn=0;
    printf("开始复制...\n");
    int rrn=mt.Wait();
    while(rrn>0) {
    	mt.FetchNext();
    	wociAppendToDbTable(*mt.Cur(),desttb,dtd,true);
    	trn+=rrn;
    	rrn=mt.Wait();
    	printf(".");fflush(stdout);
    }
    tm.Stop();
    printf("\n共处理%d行(时间%.2f秒)。\n",trn,tm.GetTime());
    return 1;
}
	
