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
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#include "dtio.h"
int Start(void *ptr);

int ci=1;
char *pidxcolarray[20];
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;

cmd_base cp;

int main(int argc,char *argv[]) {
	int nRetCode = 0;
	cmd_base::argc=argc;
        cmd_base::argv=(char **)argv;

	WOCIInit("dputil" PATH_SEP "dprmrows");
	nRetCode=wociMainEntrance(Start,true,NULL,2);
	WOCIQuit(); 
	return nRetCode;
}

char * BuildQuery(char *sql,const char *tbname,AutoMt &indexmt,int row,bool ora) {
	sprintf(sql,"select * from %s where ",tbname);
	for(int i=0;i<ci;i++) {
		//char colname[300];
		int c=i;//wociGetColumnPosByName(indexmt,(const char*)pidxcolarray[i]);
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
	else if(rn!=indexmt.GetInt("idx_rownum",irn)) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("错误,查询记录数不符,期望%d,实际%d :%s\n",indexmt.GetInt("idx_rownum",irn),rn,sql);
	}
	return rn;
}
const char *GetDPLibVersion();
int Start(void *ptr) { 
	wociSetEcho(FALSE);
	wociSetOutputToConsole(TRUE);
	DbplusCert::initInstance();
	DbplusCert::getInstance()->printlogo();
	printf(DBPLUS_STR " 基础库版本 :%s \n",GetDPLibVersion());
    	cp.GetEnv();
	char date[30];
	wociGetCurDateTime(date);
	int y,m,d;
	DbplusCert::getInstance()->GetExpiredDate(y,m,d);
	if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m) 
		ThrowWith("您用的" DBPLUS_STR "版本太老，请更新后使用(Your " DBPLUS_STR " is too old,please update it)!");
	printf("  dprmrows 工具用于按索引键值删除目标表中的数据.\n"
	       " 当前版本只支持唯一独立索引且不含部分索引的字段删除数据.\n"
	       "!!!!!强烈建议您在删除数据前先备份!!!!!!\n");
	if(!GetYesNo("是否继续执行删除过程?<N>",false)) return -1;
	printf("连接到" DBPLUS_STR "数据库:\n");
	AutoHandle dts;
	dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
	SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd);
	lgprintf("取运行参数...");
	sa.Reload();
	char sql[10000];
	char dbname[300],tabname[300];
	char condsql[5000];
	condsql[0]=dbname[0]=tabname[0]=0;
	int selidx=0;
	while(true) {
		AutoMt selmt(dts,500);
		selmt.FetchAll("select databasename \"数据库\", tabname \"表名\",recordnum \"记录数\" from dp.dp_table where recordnum>0 order by databasename,recordnum limit 500");
		selmt.Wait();
		wociSetColumnDisplay(selmt,NULL,0,"数据库",-1,-1);
		wociSetColumnDisplay(selmt,NULL,1,"表名",-1,-1);
		wociSetColumnDisplay(selmt,NULL,2,"记录数",-1,0);
		
		wociMTCompactPrint(selmt,0,NULL);
		sql[0]=0;
		getString("输入数据库名",dbname,dbname);
		//***strcpy(dbname,"test");
		if(strlen(dbname)==0) break;
		getString("输入表名",tabname,tabname);
		//***strcpy(tabname,"tab_subscrb");
		if(strlen(tabname)==0) break;
		selmt.FetchAll("select tabid,recordnum from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
		if(selmt.Wait()<1) {
			lgprintf("错误:表%s.%s不存在.",dbname,tabname);
			continue;
		}
		if(selmt.GetDouble("recordnum",0)<1) {
			lgprintf("错误:表%s.%s中没有记录.",dbname,tabname);
			continue;
		}
		int tabid=selmt.GetInt("tabid",0);
		selmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
		if(selmt.Wait()>1) {
			lgprintf("错误:表%s.%s中独立索引数量超过一个，不支持记录删除.",dbname,tabname);
			continue;
		}
		selmt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
		int indexrn=selmt.Wait();
		if(indexrn<1) {
			lgprintf("错误:表%s.%s未建立索引.",dbname,tabname);
			continue;
		}
		char fulltabname[300];
		sprintf(fulltabname,"%s.%s",dbname,tabname);
		printf("请选择使用的索引:\n");
		int i;
		for(i=0;i<indexrn;i++)
			printf("%d: %s.\n",i+1,selmt.PtrStr("columnsname",i));
		
		selidx=getOption("索引序号",selidx+1,1,indexrn)-1;
		//***int selidx=2;
		
		lgprintf("索引字段:%s.",selmt.PtrStr("columnsname",selidx));
		static char icolsnm[1000];
		strcpy(icolsnm,selmt.PtrStr("columnsname",selidx));
		//分解字段,注意部分索引
		char *ptemp=icolsnm;
		while(*ptemp==' ') ++ptemp;
		
		pidxcolarray[ci-1]=ptemp;
		
		while(*ptemp) {
			if(*ptemp=='(') {
				lgprintf("不支持按部分索引字段删除记录！");
				return -1;
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
				lgprintf("错误:找不到索引记录.");
				continue;
			}
		}
		int il=wociGetRowLen(indexmt)-sizeof(int)*6;
		int concol=wociGetColumnNumber(selmt)-6;
		
		int condlimitrn=getOption("删除条件限制记录数",100000,100,5000000);
		int idxlimitrn=getOption("索引限制返回记录数",100000,100,5000000);
		indexmt.SetMaxRows(idxlimitrn);
		lgprintf("构造删除条件数据.\n选择查询删除条件连接的数据库:");
		AutoHandle dtsc;
		dtsc.SetHandle(BuildConn(0));
		//***dtsc.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
		getString("输入删除条件查询语句，注意返回数据的字段类型、数量和顺序，必须与上面选择的索引字段一致:\n",condsql,condsql);
		//***strcpy(condsql,"select '13388829666' svcnum");
		AutoMt condmt(dtsc,condlimitrn);
		condmt.FetchAll(condsql);
		int condrn=condmt.Wait();
		indexmt.FetchFirst("select %s from %s limit 10",icols,idxtbname );
		indexmt.Wait();
		if(condmt.CompatibleMt(indexmt)) {
			lgprintf("返回字段与索引字段不一致!");
			continue;
		}
		lgprintf("取得删除条件数据%d行.\n",condrn);
		indexmt.FetchFirst("select * from %s limit 10",idxtbname );
		indexmt.Wait();
		AutoMt filemap(dts,2000);
		//只针对唯一独立索引的情况，不需要查询indexgid.
		filemap.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=0",tabid);
		filemap.Wait();
		wociSetIKByName(filemap,"fileid");
		wociOrderByIK(filemap);
		//wociSearchIK
		int lid=0;
		wociReset(indexmt);
		lgprintf("按删除条件取索引数据...\n");
		for(lid=0;lid<condrn;lid++) {
			BuildQuery(sql,idxtbname,condmt,lid,false) ;
			indexmt.FetchAllAt(indexmt.GetRows(),sql);
			indexmt.Wait();
		}
		int irn=indexmt.GetRows();
		int destrn=(int)wociCalculate(indexmt,"idx_rownum",CAL_SUM);
		lgprintf("取得索引数据%d行,对应目标数据%d行。\n",irn,destrn);
		wociSetSortColumn(indexmt,"dtfid,blockstart");
		wociSort(indexmt);
		
		printf("检查上述信息是否正确。\n\n");
		bool needback=GetYesNo("要删除的数据需要备份吗?<Y>",true);
		{
			AutoMt bkmt(dts,50000);
			bkmt.FetchAll("select * from %s.%s limit 10",dbname,tabname);
			bkmt.Wait();
			wociReset(bkmt);
			AutoStmt bkst(dts);
			AutoHandle dtbk;
			char bktabname[300];
			if(needback) {
			 lgprintf("连接到备份数据库:\n");
			 dtbk.SetHandle(BuildConn(0));
			 getString("输入表名",NULL,bktabname);
			 if(wociTestTable(dtbk,bktabname)) {
				int sel=getOption("目标表已存在，追加(1)/删除重建(2)/清空后添加(3)/退出(4)",2,1,4);
				if(sel==4) return -1;
				if(sel==2) {
					lgprintf("删除表(drop table)'%s'...",bktabname);
					AutoStmt st(dtbk);
					st.DirectExecute("drop table %s",bktabname);
					lgprintf("建表(create table)'%s'...",bktabname);
					wociGeneTable(bkmt,bktabname,dtbk);
				}
				else if(sel==3) {
					lgprintf("清空表(truncate table)'%s'...",bktabname);
					AutoStmt st(dtbk);
					st.DirectExecute("truncate table %s",bktabname);
				}
			 }
			 else  { 	
				lgprintf("建表(create table)'%s'...",bktabname);
				wociGeneTable(bkmt,bktabname,dtbk);
			 }
			}			
			int totrows=0;
			for(lid=0;lid<condrn;lid++) {
				BuildQuery(sql,fulltabname,condmt,lid,false) ;
				bkst.Prepare(sql);
				wociReplaceStmt(bkmt,bkst);
				bkmt.FetchAppend();
				while(bkmt.Wait()==50000) // mt full
				{
					if(needback) 
						wociAppendToDbTable(bkmt,bktabname,dtbk,true);
					wociReset(bkmt);
					bkmt.FetchAppend();
					totrows+=50000;
				}
			}
			totrows+=bkmt.GetRows();
			if(needback) 
				wociAppendToDbTable(bkmt,bktabname,dtbk,true);
			//***wociMTPrint(bkmt,0,NULL);
			wociReset(bkmt);
			if(totrows!=destrn) {
				if(totrows<1) {
					errprintf("找不到删除数据,请检查是否重复删除.");
					return -1;
				}
				errprintf("备份数据(%d)与索引数据(%d)中的记录行数不一致.\n如果对应索引值的数据以前做过删除,则可以继续,否则请终止删除操作!!",totrows,destrn);
				if(GetYesNo("继续删除数据吗?<N>",false)) {
					destrn=totrows;
				}
				else return -1;
			}
		}
		int havedel=0;
		for(lid=0;lid<irn;lid++) {
			int p=wociGetRawrnBySort(indexmt,lid);
			int dtfid=indexmt.GetInt("dtfid",p);
			int fpos=wociSearchIK(filemap,dtfid);
			if(fpos<0) {
				errprintf("找不到数据文件,tabid:%d,dtfid:%d.",tabid,dtfid);
				return -1;
			}
			char *fn=NULL;
			fn=filemap.PtrStr("filename",fpos);
			file_mt dfile;
			dfile.Open(fn,0);
			char *tmp=NULL;
			if(dfile.ReadMtOrBlock(indexmt.GetInt("BLOCKSTART",p),indexmt.GetInt("BLOCKSIZE",p),1,&tmp)<0) {
				errprintf("read block from '%s' failed,off:%d,len:%d.",indexmt.GetInt("BLOCKSTART",p),indexmt.GetInt("BLOCKSIZE",p));
				return -1;
			}
			int startrow=indexmt.GetInt("startrow",p);
			int idxrownum=indexmt.GetInt("idx_rownum",p);
			// startrow: 0 based
			int sdel=dfile.deleterows(startrow,idxrownum);
			havedel+=sdel;
			printf("idxr:%d,sdel:%d,fnid:%d,blockstart:%d,startrow:%d\n",
				idxrownum,sdel,indexmt.GetInt("dtfid",p),indexmt.GetInt("BLOCKSTART",p),startrow);
			*filemap.PtrInt("recordnum",fpos)-=sdel;
		}
		if(havedel!=destrn) {
			errprintf("删除记录数不一致! 需要删除%d行,实际删除%d行,请从备份介质恢复数据!",destrn,havedel);
		}
		lgprintf("修改数据字典...");
		double newtotrows=wociCalculate(filemap,"recordnum",CAL_SUM);
		AutoStmt actst(dts);
		actst.DirectExecute("update dp.dp_table set recordnum=%.0f where tabid=%d",newtotrows,tabid);
		actst.DirectExecute("delete from dp.dp_datafilemap where tabid=%d",tabid);
		wociAppendToDbTable(filemap,"dp.dp_datafilemap",dts,true);
		sa.BuildDTP(fulltabname);
		lgprintf("表'%s'数据删除成功,共删除%d行记录.",fulltabname,destrn);
 	}
	return 0;
}
