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
#include "dt_svrlib.h" 
int Start(void *ptr);

     int ci=1;
     char *pidxcolarray[20];

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("dptest\batchquery");
    nRetCode=wociMainEntranceEx(Start,true,NULL,2,WDBI_ENT_INIT|WDBI_ENT_CLEAN);
    WOCIQuit(); 
    return nRetCode;
}

char * BuildQuery(char *sql,const char *tbname,AutoMt &indexmt,int row,bool ora) {
	sprintf(sql,"select * from %s where ",tbname);
	
	//int concol=wociGetColumnNumber(indexmt);
        for(int i=0;i<ci;i++) {
        	//char colname[300];
        	int c=wociGetColumnPosByName(indexmt,(const char*)pidxcolarray[i]);
        	//wociGetColumnName(indexmt,i,colname);
        	char cell[300];
        	wociGetCell(indexmt,row,c,cell,false);
        	switch(wociGetColumnType(indexmt,c)) {
        	case COLUMN_TYPE_CHAR :
        	{
        	  int sl=strlen(cell);
        	  while(cell[--sl]==' ') cell[sl]=0;
        	  sprintf(sql+strlen(sql)," %s='%s' ",pidxcolarray[i],cell);
        	  break;
        	}
        	case COLUMN_TYPE_FLOAT:
        	case COLUMN_TYPE_NUM:
        	case COLUMN_TYPE_INT:
        	case COLUMN_TYPE_BIGINT:
        	  sprintf(sql+strlen(sql)," %s=%s ",pidxcolarray[i],cell);
        	  break;
        	case COLUMN_TYPE_DATE:
        	 if(ora)
  	          sprintf(sql+strlen(sql)," %s=to_date('%s','yyyymmdd hh24:miss') ",pidxcolarray[i],cell);
  	         else
        	  sprintf(sql+strlen(sql)," %s='%s' ",pidxcolarray[i],cell);
        	 break;
        	}
        	if(i+1!=ci) strcat(sql," and ");
        }
        return sql;
}

int Execute(AutoMt &indexmt,const char *tabname,int dts,int irn ,int lrn,bool ora)
{
	char sql[1000];
	AutoMt mt(dts,lrn);
	mt.FetchFirst(BuildQuery(sql,tabname,indexmt,irn,ora));
	int rn=mt.Wait();
	if(rn==lrn) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("超过查询限制记录数%d,不能校验:%s\n",sql);
	}
	else if(rn!=indexmt.GetLong("idx_rownum",irn)) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("错误,查询记录数不符,期望%ld,实际%d :%s\n",indexmt.GetLong("idx_rownum",irn),rn,sql);
	}
	return rn;
}

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("dp_indexq工具用于测试和对比" DBPLUS_STR " 与oracle按索引批量查询大表的效率.\n连接到" DBPLUS_STR "数据库:\n");
    //int cmpdp=GetYesNo("需要直接连" DBPLUS_STR "查询?<Y>",true);
    char und[100],psd[100],svd[100];
    //if(cmpdp) {
    	 getdbcstr(und,psd,svd,"");
         dts.SetHandle(wociCreateSession(und,psd,svd,DTDBTYPE_ODBC));
    //}
    mytimer tm;
    AutoHandle dtsora;
    char uno[100],pso[100],svo[100];
    int cmpora=GetYesNo("需要Oracle对比测试?<N>",false);
    char oratabname[300];
    if(cmpora) {
        printf("连接到Oracle数据库...\n");
        getdbcstr(uno,pso,svo,"");
        dtsora.SetHandle(wociCreateSession(uno,pso,svo,DTDBTYPE_ORACLE));
        getString("输入oracle中的表(视图)名:",NULL,oratabname);
    }
    char sql[10000];
    char dbname[300],tabname[300];
    while(true) {
     AutoMt selmt(dts,500);
     selmt.FetchAll("select databasename \"数据库\", tabname \"表名\",recordnum \"记录数\" from dp.dp_table where recordnum>0 order by databasename,recordnum limit 500");
     selmt.Wait();
	wociSetColumnDisplay(selmt,NULL,0,"数据库",-1,-1);
	wociSetColumnDisplay(selmt,NULL,1,"表名",-1,-1);
	wociSetColumnDisplay(selmt,NULL,2,"记录数",-1,0);

     wociMTCompactPrint(selmt,0,NULL);
     sql[0]=0;
     getString("输入数据库名:",NULL,dbname);
     if(strlen(dbname)==0) break;
     getString("输入表名:",NULL,tabname);
     if(strlen(tabname)==0) break;
     selmt.FetchAll("select tabid,recordnum from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
     if(selmt.Wait()<1) {
     	printf("错误:表%s.%s不存在.\n",dbname,tabname);
     	continue;
     }
     if(selmt.GetDouble("recordnum",0)<1) {
     	printf("错误:表%s.%s中没有记录.\n",dbname,tabname);
     	continue;
     }
     int tabid=selmt.GetInt("tabid",0);
     selmt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
     int indexrn=selmt.Wait();
     if(indexrn<1) {
     	printf("错误:表%s.%s未建立索引.\n",dbname,tabname);
     	continue;
     }
     printf("请选择使用的索引:\n");
     int i;
     for(i=0;i<indexrn;i++)
     	printf("%d: %s.\n",i+1,selmt.PtrStr("columnsname",i));
     int selidx=getOption("索引序号<1>",1,1,indexrn)-1;
     
     //selmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
     //int indexrn=selmt.Wait();
     //if(indexrn<1) {
     //     	printf("错误:表%s.%s未建立索引.\n",dbname,tabname);
     //	continue;
     //}
     //if(strstr(selmt.PtrStr("columnsname",0),"(")) {
     //	printf("错误:表%s.%s使用了部分索引,不能校验查询结果.\n",dbname,tabname);
     //	continue;
     //}
     printf("索引字段:%s.\n",selmt.PtrStr("columnsname",selidx));
     static char icolsnm[1000];
     strcpy(icolsnm,selmt.PtrStr("columnsname",selidx));
     //分解字段,注意部分索引
     char *ptemp=icolsnm;
     while(*ptemp==' ') ++ptemp;
     
     pidxcolarray[ci-1]=ptemp;
     
     while(*ptemp) {
     	if(*ptemp=='(') {
     		*ptemp=0;
     		while(*ptemp!=')') ++ptemp;
     		++ptemp;
     	}
     	if(*ptemp==',') {
     		*ptemp++=0;
     		while(*ptemp==' ') ++ptemp;
     		ci++;
     		pidxcolarray[ci-1]=ptemp;
     	}  
     	if(*ptemp==' ') *ptemp=0;
     	ptemp++;
     }
     char icols[1000];
     strcpy(icols,pidxcolarray[0]);
     for(i=1;i<ci;i++) {
      strcat(icols,",");
      strcat(icols,pidxcolarray[i]);
     }
     
     char idxtbname[200];
     AutoMt indexmt(dts,500);
     if(strlen(selmt.PtrStr("indextabname",selidx))>0) 
     	sprintf(idxtbname,"%s.%s",dbname,selmt.PtrStr("indextabname",selidx));
     else
        sprintf(idxtbname,"%s.%sidx%d",dbname,tabname,selmt.GetInt("indexgid",selidx));
     indexmt.FetchFirst("select * from %s",idxtbname);
     if(indexmt.Wait()<1) {
     	sprintf(idxtbname,"%s.%sidx%d",dbname,tabname,selmt.GetInt("indexgid",selidx));
        indexmt.FetchFirst("select * from %s",idxtbname);
        if(indexmt.Wait()<1) {
     	  printf("错误:找不到索引记录.\n",dbname,tabname);
     	  continue;
     	}
     }
     int il=wociGetRowLen(indexmt)-sizeof(int)*6;
     int concol=wociGetColumnNumber(selmt)-6;
     
/*     int ci=0;
     char icols[1000];
     icols[0]=0;
     for(ci=0;ci<concol;ci++) {
     	char colname[100];
     	wociGetColumnName(indexmt,ci,colname);
     	strcat(icols,colname);
     	if(ci+1!=concol) strcat(icols,",");
     }
*/
     int idxlimitrn=getOption("索引限制记录数<100000>",100000,1000,5000000);
     int limitrn=getOption("限制返回记录数<10000>",10000,100,5000000);
     int connum=getOption("同时连接数<10>",10,1,600);
     int reprn=getOption("每个连接上的查询次数<300>",300,1,5000000);
     int reconn=getOption("连接次数<10>",10,1,1000);
     int userand=GetYesNo("随机顺序?<N>",false);
     indexmt.SetMaxRows(idxlimitrn);
     printf("构造索引数据...\n");
     indexmt.FetchFirst("select %s,sum(idx_rownum) idx_rownum from %s group by %s",icols,idxtbname,icols);
     //indexmt.FetchFirst("select distinct %s from %s ",icols,idxtbname);
     int irn=indexmt.Wait();
     printf("索引数据%d行.\n开始查询...\n",irn);
     indexmt.SetDBC(dts);
     int clp=0;
     sprintf(dbname+strlen(dbname),".%s",tabname);
     int rcc=0;
     int trn=0;
     int seed=(int)time(NULL);
     //if(cmpdp) {
     if(!userand) seed=30022;
     srand(seed);
      for(rcc=0;rcc<reconn;rcc++) {
     	lgprintf("建立%d个数据库连接...",connum);
     	AutoHandle *pdts=new AutoHandle[connum];
     	for(int cc=0;cc<connum;cc++) {
         pdts[cc].SetHandle(wociCreateSession(und,psd,svd,DTDBTYPE_ODBC));
     	}
     	lgprintf("连接建立成功。开始查询...");
        tm.Clear();
        tm.Start();
     	for(int qc=0;qc<reprn*connum;qc++) {
 		int uoff=(int)(((double)rand())/(RAND_MAX)*(irn-1));//i;
 			trn+=Execute(indexmt,dbname,pdts[clp],uoff,limitrn,false);
		clp=(++clp)%connum;
     	}
	delete []pdts;
        tm.Stop();
     	lgprintf("第%d次连接，查询结束",rcc+1);
      }
     //}
     printf( DBPLUS_STR " Query %d times,Rows %d,time :%.2f.\n",reprn*connum*reconn,trn,tm.GetTime());
      tm.Clear();
     if(cmpora) {
      trn=0;
      srand(seed);
      for(rcc=0;rcc<reconn;rcc++) {
     	lgprintf("建立%d个oracle数据库连接...",connum);
     	AutoHandle *pdts=new AutoHandle[connum];
     	for(int cc=0;cc<connum;cc++) {
         pdts[cc].SetHandle(wociCreateSession(uno,pso,svo,DTDBTYPE_ORACLE));
     	}
     	lgprintf("oracle连接建立成功。开始查询...");
        tm.Start();
     	for(int qc=0;qc<reprn*connum;qc++) {
 		int uoff=(int)(((double)rand())/RAND_MAX*(irn-1));//i;
		trn+=Execute(indexmt,oratabname,pdts[clp],uoff,limitrn,true);
		clp=(++clp)%connum;
     	}
	delete []pdts;
        tm.Stop();
     	lgprintf("第%d次连接，oracle查询结束",rcc+1);
     }
     printf("Oracle Query %d times,Rows %d,time :%.2f.\n",reprn*connum*reconn,trn,tm.GetTime());
     }
     
    }
   return 0;
}
