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
#include <string.h>

struct Column_Desc {
	unsigned short type;
	unsigned short dtsize;
	char colname[COLNAME_LEN];
	char dispname[COLNAME_LEN];
	unsigned int dspsize;
	unsigned char prec;
	signed char scale;
	char *pCol;
#ifndef WDBI64BIT
  	char *pBlank;
#endif
  	void ReverseByteOrder() {
  		#ifndef WORDS_BIGENDIAN
  		return ;
  		#endif
  		revShort(&type);
  		revShort(&dtsize);
  		revInt(&dspsize);
  	}
};


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

void ImportAndExportData(AutoMt *Dmt,AutoMt *Smt)
{
	  long bflen = _wdbiGetBufferLen(Smt->handle);
    void *p = malloc(bflen);
    memset(p,0,bflen);
    long len = 0;
    int RowCt=0,maxRows=0;
    int cdlen=0,colnm=0;
    void  *pColDesc = NULL; 
    
    pColDesc = malloc(sizeof(Column_Desc)*4); 
    wociExport(Smt->handle,p,len,maxRows,RowCt);
    wociGetColumnDesc(Smt->handle,&pColDesc,cdlen,colnm);
    wociImport(Dmt->handle,p,len,pColDesc,colnm,cdlen,maxRows,RowCt);
    free(p);

}

void ImportAndExportRowNum(AutoMt *Dmt,AutoMt *Smt)
{
	 char *p = new char[700000];
	 memset(p,0,700000);
	 int rt = 0;
	 for(int i=0;i<1001000;i=i+5000)
	 {  
	 	   rt = 5000;
	 	   if(i+5000>1001000)
	 	   {
	 	      rt = 1001000 - i;
	 	   }
	 	   memset(p,0,700000);
	 		 _wdbiExportSomeRowsWithNF(Smt->handle,p,i,rt);
	     _wdbiAppendRowsWithNF(Dmt->handle,p,rt);
	 }

	// wociAppendRows(Dmt->handle,p,8);
	 delete[] p;
	 
}

 void SetVarStrData(AutoMt *mt)
 {
 	   char *p = new char[1000];
 	   memset(p,0,1000);
 	   int off = 0;
     strcpy(p+off,"1");
     off=off+51;
     strcpy(p+off,"22");
     off=off+51;
 	   strcpy(p+off,"323");
     off=off+51;
     strcpy(p+off,"4234");
     off=off+51;
     strcpy(p+off,"52345");
     off=off+51;
     strcpy(p+off,"623456");
     mt->AddrFresh();
     mt->SetStr(1,8,p,6);
     delete[] p ;
 }
 
void GetData(AutoMt *mt)
{
	char pStr[100];
	const char *pConst = NULL;
	char *p = NULL;
	memset(pStr,0,100);
	for(int i=0;i<10;i++)
	{
		 mt->GetStr(1,i,pStr);
		 printf("%s,",pStr);
		 memset(pStr,0,100);
		 mt->GetStr(2,i,pStr);
		 printf("%s\r\n",pStr);
		 memset(pStr,0,100);
		 pConst = mt->PtrStr(1,i);
		 printf("%s,",pConst);
	   pConst = mt->PtrStr(2,i);
		 printf("%s\r\n",pConst);
	}
   
   	for(int i=100000;i<100010;i++)
	{
		 mt->GetStr(1,i,pStr);
		 printf("%s,",pStr);
		 memset(pStr,0,100);
		 mt->GetStr(2,i,pStr);
		 printf("%s\r\n",pStr);
		 memset(pStr,0,100);
		 pConst = mt->PtrStr(1,i);
		 printf("%s,",pConst);
	   pConst = mt->PtrStr(2,i);
		 printf("%s\r\n",pConst);
	}
}

void CopyData(AutoMt *mt)
{
	  _wdbiCopyVarStrColDataInTable(mt->handle,1,3,5,20000);
}

void CopyDataFromTab(AutoMt *Dmt,AutoMt *Smt)
{
	 _wdbiCopyRowsToNoCut(Smt->handle,Dmt->handle,0,1,20);
}

int Start(void *ptr) { 
    AutoHandle dts;
    mytimer tm;
    printf("选择源数据库...\n");
    
    //dts.SetHandle(BuildConn(0));
      dts.SetHandle(wociCreateSession("root","dbplus03","dma",1004));
   //  dts.SetHandle(wociCreateSession("ics001","ics001","//219.146.12.121:1521/wins",1002));
    AutoHandle dtd;
    printf("选择目标数据库...\n");
   // dtd.SetHandle(BuildConn(0));
    //  dtd.SetHandle(wociCreateSession("root","dbplus03","dma",1004));
     dtd.SetHandle(wociCreateSession("ics001","ics001","//219.146.12.121:1521/wins",1002));
    char sqlst[2000];
    memset(sqlst,0,2000);
    strcpy(sqlst,"select * from zkb.varchar_test limit 200");
   //  strcpy(sqlst,"select * from ics001.policy_push_times ");
   //strcpy(sqlst,"select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct from dp.dp_datapart a where (status=0 or (status in(72,73) and lower(extsql) like 'load %')) and begintime<now() and ifnull(blevel,0)<100  order by blevel,busyct,touchtime,tabid,datapartid limit 2");
    printf("%s\n",sqlst);
   // getString("请输入查询语句(中间不要回车)",NULL,sqlst);
    int fetchn=getOption("提交行数",10000,100,50000);
    fetchn = 100;
    char desttb[200];
    //getString("请输入目标表名称",NULL,desttb);
    memset(desttb,0,200);
    strcpy(desttb,"ics001.test02");
    // strcpy(desttb,"zkb.tb1000");
   // strcpy(desttb,"zkb.test02");
    
    AutoStmt srcst(dts);
    AutoStmt dtcst(dtd);
    srcst.Prepare(sqlst);
   // dtcst.Prepare("insert into zkb.test02  values ( ? ,? ,? ,?, ?, ? )");
    //  dtcst.Prepare("insert into ics001.test02  values ( :1,:2,:3,:4,:5,:6)");
       dtcst.Prepare("insert into ics001.test02  values ( :1,:2,:3)");

   // TradeOffMt mt(0,fetchn);
   // mt.Cur()->Build(srcst);
   // mt.Next()->Build(srcst);
      AutoMt mt(dts,3000);
      mt.Build(srcst);
      mt.FetchAll(sqlst);
      int rrn=mt.Wait();
   // _wdbiCheckData(mt.handle, 1);
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
		wociGeneTable(mt,desttb,dtd);
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
	    wociGeneTable(mt,desttb,dtd);
    }
   
      //   mt.FetchFirst();
          int trn=0;
     //
       //   AutoMt Dmt(0,3000);  
       //   Dmt.Build(srcst);
       //  ImportAndExportData(&Dmt,&mt);
         
        // ImportAndExportRowNum(&Dmt,&mt);
       // SetVarStrData(&Dmt);
          printf("开始复制...\n");
      //    int rrn=mt.Wait();
     //  while(rrn>0) {
      //	mt.FetchNext();
      //    wociCompact(mt);
           //  CopyDataFromTab(&Dmt,&mt); 
           // CopyData(&Dmt);
          // wociAppendToDbTable(mt,desttb,dtd,true);
          _wdbiBindToStatment(mt.handle,dtcst,NULL,1);
          _wdbiExecute(dtcst,100);
          trn = dtcst.Wait();
          _wdbiCommit(dtd);
        //trn+=rrn;
       // rrn=mt.Wait();
    	printf(".");fflush(stdout);
     //	GetData(&Dmt);
    // }
    tm.Stop();
    printf("\n共处理%d行(时间%.2f秒)。\n",trn,tm.GetTime());
    return 1;
}
	
