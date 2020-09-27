#include "dtio_mt.h"
#include "dtio.h"

dtparams_mt::dtparams_mt(const char *dbn,const char *tbn):mttables(20) 
{
	strcpy(dbname,dbn);strcpy(tbname,tbn);dtfilenum=0;pfileid=NULL;
}

uint8B dtparams_mt::GetRecordNum() {
	return (uint8B)GetDouble(GetDPTable(pdtio),"recordnum",0);
}

const char *dtparams_mt::SearchFile(int fid) {
	int mt=GetMt(GetDFMName(pdtio));
	int *pfid;
	wociGetIntAddrByName(mt,"fileid",0,&pfid);
	int rn=wociGetMemtableRows(mt);
        int i;
	for(i=0;i<rn;i++) {
		if(pfid[i]==fid) break;
	}
	if(i==rn) 
		ThrowWith("Data file not found on '%s.%s',id:%d.",dbname,tbname,fid);
	char *faddr;
	int tmpl;
	wociGetStrAddrByName(mt,"filename",i,&faddr,&tmpl);
	return faddr;
}

#define REPLACETABID(mt,tabid) \
{ \
	int strn=wociGetMemtableRows(mt);\
	int lpct;\
	int *sp;\
	wociGetIntAddrByName(mt,"tabid",0,&sp);\
	for(lpct=0;lpct<strn;lpct++) {\
		*sp++=tabid;\
	}\
}


//从dp_seq取得新的tabid,并检查新tabid是否有数据冲突(包括dp参数和存储文件),然后替换备份数据的dp参数在内存的映像中的tabid
void dtparams_mt::restoreCheck(int dts,bool &overwritable,bool &duprecords,char *msg) {
	msg[0]=0;
	duprecords=false;
	int tabid=GetInt(GetDPTable(pdtio),"tabid",0);
	int rn=0;
	if(tabid<1) ThrowWith("在DP参数区域得到了错误的目标表编号：%d.",tabid);
	{	bool ec=wociIsEcho();
		wociSetEcho(FALSE);
		{
		 AutoStmt st(dts);
		 st.DirectExecute("lock tables dp.dp_seq write");
		 st.DirectExecute("update dp.dp_seq set id=id+1");
		}
		AutoMt seq(dts,10);
		seq.FetchAll("select id as fid from dp.dp_seq");
		seq.Wait();
		{
		 AutoStmt st(dts);
		 st.DirectExecute("unlock tables");
		}
		wociSetEcho(ec);
		tabid=seq.GetInt("fid",0);
	}
	AutoMt mt(dts,200);
	mt.FetchFirst("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",
	   GetString("DP_TABLE","databasename",0),GetString("DP_TABLE","tabname",0));
	if(mt.Wait()>0) 
		ThrowWith("表名重复：目标表(编号%d,'%s.%s')已存在，无法做数据恢复操作.",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	mt.FetchFirst("select * from dp.dp_table where tabid=%d",tabid);
	if(mt.Wait()>0)
		ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在，无法做数据恢复操作.",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	mt.FetchFirst("select * from dp.dp_index where tabid=%d",tabid);
	if(mt.Wait()>0)
		ThrowWith("发现不正确的索引参数(表%d)，无法做数据恢复操作.",mt.GetInt("tabid",0));
	mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
	rn=mt.Wait();
	if(rn>0) 
		ThrowWith("发现不正确的数据文件记录(表%d)，无法做数据恢复操作.",mt.GetInt("tabid",0));
	mt.FetchFirst("select * from dp.dp_datapart where tabid=%d",tabid);
	rn=mt.Wait();
	if(rn>0) 
		ThrowWith("发现不正确的数据分组参数(表%d)，无法做数据恢复操作.",mt.GetInt("tabid",0));
	mt.FetchFirst("select * from dp.dp_log where tabid=%d limit 200",tabid);
	if(mt.Wait()>0)
		ThrowWith("发现不正确的日志记录(表%d)，无法做数据恢复操作.",mt.GetInt("tabid",0));
	int tmt=GetMt("DP_TABLE");
	mt.FetchFirst("select * from dp.dp_path where pathid=%d",wociGetIntValByName(tmt,"dstpathid",0));
	if(mt.Wait()<1)
	  sprintf(msg+strlen(msg),"\n**********\n目标表存储路径需要重新配置\n**********\n");
	mt.FetchAll("select * from dp.dp_datasrc where sysid=%d",wociGetIntValByName(tmt,"sysid",0));
	if(mt.Wait()<1)
	  sprintf(msg+strlen(msg),"\n**********\n数据源连接参数需要重新配置\n**********\n");
	REPLACETABID(tmt,tabid);
	tmt=GetMt("DP_INDEX");
	REPLACETABID(tmt,tabid);
	tmt=GetMt("DP_INDEX_ALL");
	REPLACETABID(tmt,tabid);
	tmt=GetMt("DP_DATAFILEMAP");
	REPLACETABID(tmt,tabid);
	tmt=GetMt("DP_LOG");
	REPLACETABID(tmt,tabid);
	tmt=GetMt("DP_DATAPART");
	rn=wociGetMemtableRows(tmt);
	for(int i=0;i<rn;i++) {
	 mt.FetchAll("select * from dp.dp_path where pathid=%d",wociGetIntValByName(tmt,"tmppathid",i));
	 if(mt.Wait()<1)
	  sprintf(msg+strlen(msg),"\n**********\n临时存储路径需要重新配置:数据组%d.\n**********\n",wociGetIntValByName(tmt,"datapartid",i));
	 mt.FetchAll("select * from dp.dp_datasrc where sysid=%d",wociGetIntValByName(tmt,"srcsysid",i));
	 if(mt.Wait()<1)
	  sprintf(msg+strlen(msg),"\n**********\n数据源连接参数需要重新配置:数据组%d.\n**********\n",wociGetIntValByName(tmt,"datapartid",i));
	 char *ptmp;
	 int l;
	 wociGetStrAddrByName(tmt,"extsql",0,&ptmp,&l);
	 if(strlen(ptmp)<6)
	  sprintf(msg+strlen(msg),"\n**********\n数据抽取SQL语句需要重新配置:数据组%d.\n**********\n",wociGetIntValByName(tmt,"datapartid",i));
	}
	REPLACETABID(tmt,tabid);
}
/*
int dtparams_mt::getNextFid(int dts) {
		bool ec=wociIsEcho();
		wociSetEcho(FALSE);
		AutoMt seq(dts,10);
		seq.FetchAll("select dt_fileid.nextval as fid from dual");
		seq.Wait();
		wociSetEcho(ec);
		return seq.GetInt("fid",0);
}
*/
void dtparams_mt::adjparam(const char *newdir) {
	int rn=GetRowNum(GetDFMName(pdtio));
	int i;
	char fn[PATH_LEN];
	char basec[PATH_LEN];
	printf("修改数据存储路径为:%s.\n",newdir);
	for(i=0;i<rn;i++) {
/*
	 if(fiddup) {
	 	fid=getNextFid(dts);
	 	SetInt("DT_DATAFILEMAP","fileid",fid,i);
	 	SetInt("DT_INDEXFILEMAP","fileid",fid,i);
	 }
*/
	 if(newdir) {
	 	  strcpy(basec,GetString(GetDFMName(pdtio),"filename",i));
	 	  sprintf(fn,"%s%s",newdir,basename(basec));
	 	  SetString(GetDFMName(pdtio),"filename",fn,i);
	 	  pdtio->GetContentMt()->MoveUnit(DTIO_UNITTYPE_DATFILE,dirname(basec),basename(basec),newdir);
	 	  
	 	  strcpy(basec,GetString(GetIFMName(pdtio),"idxfname",i));
	 	  sprintf(fn,"%s%s",newdir,basename(basec));
	 	  SetString(GetIFMName(pdtio),"idxfname",fn,i);
	 	  pdtio->GetContentMt()->MoveUnit(DTIO_UNITTYPE_IDXFILE,dirname(basec),basename(basec),newdir);
	 } 	
	}	
/*
	rn=GetRowNum("DT_TASKSCHEDULE");

	if(fiddup) {
	 for(i=0;i<rn;i++ {
	  fid=getNextFid(dts);
	  SetInt("DT_TASKSCHEDULE","taskid",fid,i);
	  SetInt("DT_TASKSCHEDULE","taskstatus",30,i); //文件编号
	 }
	}
	*/
}


//实际删除/插入记录以前,先修改内存表中的参数,以适合于恢复而不引起DT系统已有数据的冲突.
// 由于文件ID号的修改涉及索引数据文件和索引表的根新,暂不考虑.
void dtparams_mt::restore(int dts,const char *newdir) 
{
	int tabid=GetInt(GetDPTable(pdtio),"tabid",0);
	AutoMt mt(dts);
	mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	int rn=mt.Wait();
	if(rn>0 && int(wociGetDoubleValByName(mt,"recordnum",0))!=0) 
	  ThrowWith("目标表(编号%d,'%s.%s')非空，无法做数据恢复操作.",tabid,mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	  
	bool fiddup=false;
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",tabid,
	  GetInt(GetDFMName(pdtio),"fileid",0));
	if(mt.Wait()>0) 
		ThrowWith("在另一个目标表中发现重复的文件编号(tabid:%d,fileid:%d,文件名:'%s'),无法做数据恢复操作.\n",mt.GetInt("tabid",0),mt.GetInt("fileid",0),mt.PtrStr("filename",0));
	//if(newdir!=NULL || fiddup) adjparam(dts,newdir,fiddup);
	//
	//缺少CloseTable操作(recordnum set 0, flush table ...);
	printf("恢复参数表...\n");
	AutoStmt st(dts);
	st.Prepare("delete from dp.dp_table where tabid=%d",tabid);st.Execute(1);st.Wait();
	st.Prepare("delete from dp.dp_index where tabid=%d",tabid);st.Execute(1);st.Wait();
	st.Prepare("delete from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);st.Execute(1);st.Wait();
	st.Prepare("delete from dp.dp_log where tabid=%d ",tabid);st.Execute(1);st.Wait();
	//st.Prepare("delete from dp.dp_indexfilemap where tabid=%d and fileflag!=2",tabid);st.Execute(1);st.Wait();
	st.Prepare("delete from dp.dp_datapart where tabid=%d",tabid);st.Execute(1);st.Wait();
	wociAppendToDbTableWithColName(GetMt("DP_TABLE"),"dp.dp_table",dts,true);
	wociAppendToDbTableWithColName(GetMt("DP_INDEX_ALL"),"dp.dp_index",dts,true);
	wociAppendToDbTableWithColName(GetMt("DP_DATAFILEMAP"),"dp.dp_datafilemap",dts,true);
	//wociAppendToDbTable(GetMt("DP_INDEXFILEMAP"),"dp.dp_indexfilemap",dts,true);
	wociAppendToDbTableWithColName(GetMt("DP_DATAPART"),"dp.dp_datapart",dts,true);
	wociAppendToDbTableWithColName(GetMt("DP_LOG"),"dp.dp_log",dts,true);
	wociCommit(dts);
}

void dtparams_mt::PrintDetail(bool withlog) {
	printf("基本参数:\n");
	wociMTCompactPrint(GetMt("DP_TABLE"),0,NULL);
	printf("索引:\n");
	wociMTCompactPrint(GetMt("DP_INDEX_ALL"),0,NULL);
	printf("文件:\n");
	wociMTCompactPrint(GetMt("DP_DATAFILEMAP"),0,NULL);
	printf("任务:\n");
	wociMTCompactPrint(GetMt("DP_DATAPART"),0,NULL);
	if(withlog) {
	 printf("日志.\n");
	 wociMTCompactPrint(GetMt("DP_LOG"),0,NULL);
	}
}

void dtparams_mt::GetDataFileMap(int *fnid,char *fns,int fnlen)
{
	int fn=GetDataFileNum();
	for(int i=0;i<fn;i++) {
		fnid[i]=GetInt("DP_DATAFILEMAP","fileid",i);
		strncpy(fns+fnlen*i,GetString("DP_DATAFILEMAP","filename",i),fnlen);
	}
}

void dtparams_mt::GetIndexMap(int *ptaboff,int *psubidxid,int *pidxfieldnum)
{
		int irn=GetTotIndexNum();
		int *idxid=new int[irn];
		int soledct=0;
		int i;
		for(i=0;i<irn;i++) {
			pidxfieldnum[i]=GetInt("DP_INDEX_ALL","idxfieldnum",i);
			psubidxid[i]=GetInt("DP_INDEX_ALL","seqinidxtab",i);
			idxid[i]=GetInt("DP_INDEX_ALL","indexgid",i);
			if(GetInt("DP_INDEX_ALL","issoledindex",i)>=1) 
				ptaboff[i]=soledct++;
			else ptaboff[i]=-1;
		}
		for(i=0;i<irn;i++) {
			int issoled=GetInt("DP_INDEX_ALL","issoledindex",i);
			if(issoled==0) {
			  int id=GetInt("DP_INDEX_ALL","indexgid",i);
			  for(int j=0;j<irn;j++) {
				if(id==idxid[j] && ptaboff[j]>=0) {
				  ptaboff[i]=ptaboff[j];
				  break;
				}
			  }
			}
		}
		delete []idxid;
}
	
void dtparams_mt::FetchParam(int dts,bool psoleindexmode,dtioStream *_pdtio) {
	AutoMt mt(dts,MAX_DST_DATAFILENUM);
	pdtio=_pdtio;
	mt.FetchAll("select * from dp.dp_table where databasename='%s' and tabname='%s'",
		dbname,tbname);
	if(mt.Wait()<1) ThrowWith("Could not found '%s.%s' in dp.dp_table",dbname,tbname);
	int tabid=mt.GetInt("tabid",0);
	int indexgid=0;
	AppendMt(mt,"DP_TABLE");
	double drn=mt.GetDouble("recordnum",0);
	if(pdtio->GetStreamType()==FULL_BACKUP) {
	 mt.FetchAll("select count(*) rn from %s.%s ",dbname,tbname);
	 mt.Wait();
	 if(mt.GetDouble("rn",0)!=drn) 
		ThrowWith("记录数不一致,参数表为%.0f,实际为%.0f.\n",drn,mt.GetDouble("rn",0));
	}
	mt.FetchAll("select ifnull(sum(recordnum),0) rn from dp.dp_datafilemap where tabid=%d and fileflag=0 and isfirstindex=1",tabid);
        int rnx=mt.Wait();
	double rrn=mt.GetDouble("rn",0);
	if(rnx<1) rrn=0;
	if(rrn!=drn ) 
		ThrowWith("记录数不一致,参数表为%.0f,数据文件为%.0f.\n",drn,mt.GetDouble("rn",0));
	if(psoleindexmode) {
		mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex=2",tabid);
		if(mt.Wait()<1) {
		  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by seqindattab",
			     tabid);
		  if(mt.Wait()<1) ThrowWith("找不到表'%s.%s'的独立索引",dbname,tbname);
		  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 and indexgid=%d order by seqindattab",
			     tabid,mt.GetInt("indexgid",0));
		  mt.Wait();
		}
	        indexgid=mt.GetInt("indexgid",0);
	}  
	else {
		  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by seqindattab",
			     tabid);
		  if(mt.Wait()<1) ThrowWith("找不到表'%s.%s'的独立索引",dbname,tbname);
	}	
	int rn=mt.GetRows();
  	// Replace index tabname with autogeneration
  	int i;
  	//for(i=0;i<rn;i++) 
  	//  if(strlen(mt.PtrStr("indextabname",i))==0)
  	//    sprintf(mt.PtrStr("indextabname",i),"%sidx%d",tbname,mt.GetInt("indexgid",i));
	AppendMt(mt,"DP_INDEX");
	if(psoleindexmode) 
	 mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d order by seqindattab",tabid,indexgid);
	else mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab",tabid);
	if(mt.Wait()<1) ThrowWith("Could not found index on '%s.%s'(tabid:%d) in dt_index",dbname,tbname,tabid);
        if(psoleindexmode) {
        	rn=mt.GetRows();
        	for(i=0;i<rn;i++)
			*mt.PtrInt("seqindattab",i)=mt.GetInt("seqinidxtab",i);
	}
	AppendMt(mt,"DP_INDEX_ALL");
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (procstatus!=0 or fileflag=1) order by datapartid,fileid",tabid);
	if(mt.Wait()>0) 
	 ThrowWith("表'%s.%s' 的一些数据文件还未处理完成，不能备份.",dbname,tbname);
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 %s order by datapartid,fileid",tabid,
	   psoleindexmode?" and isfirstindex=1":"");
	mt.Wait();
	AppendMt(mt,"DP_DATAFILEMAP");//Do append even a empty mt.
	if(pdtio->GetStreamType()==FULL_BACKUP) {
	 mt.FetchAll("select * from dp.dp_log where tabid=%d order by evt_tm desc limit %d",tabid,MAX_DST_DATAFILENUM-1);
	 mt.Wait();
	 AppendMt(mt,"DP_LOG");
	}
	//mt.FetchAll("select * from dp.dp_indexfilemap where tabid=%d and fileflag!=2 order by datapartid,fileid",tabid);
	//mt.Wait();
	//AppendMt(mt,"DP_INDEXFILEMAP");//Do append even a empty mt.
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=5 and begintime<now() order by datapartid",
		tabid);
	if(mt.Wait()>0) 
		ThrowWith("表'%s.%s' 还未处理完成，任务非空闲，不能备份.",dbname,tbname);
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and begintime<now() order by datapartid",
		tabid);
	mt.Wait();
	AppendMt(mt,"DP_DATAPART");//Do append even a empty mt.
}

void dtparams_mt::CheckCompact() {
	if(pdtio->GetVersion()==DTIO_OLDVERSION) {
	  AutoMt mt(0,1);
	  //CREATE TABLE dp_table (	       CREATE TABLE dt_table (          	
	  //tabid integer , 			tabid integer ,                  
	  //tabdesc varchar(80) NOT NULL,        tabdesc varchar(40) NOT NULL,     
	  //tabname varchar(60) NOT NULL,        tabname varchar(30) NOT NULL,     
	  //cdfileid integer NOT NULL,           cdfileid integer NOT NULL,        
	  //soledindexnum integer NOT NULL,      soledindexnum integer NOT NULL,   
	  //totindexnum integer NOT NULL,        totindexnum integer NOT NULL,     
	  //recordlen integer NOT NULL,          recordlen integer NOT NULL,       
	  //recordnum double NOT NULL,           recordnum double NOT NULL,        
	  //maxrecinblock integer NOT NULL,      datasetid integer NOT NULL,       
	  //firstdatafileid integer NOT NULL,    maxrecinblock integer NOT NULL,   
	  //databasename varchar(40) ,           firstdatafileid integer NOT NULL, 
	  //datafilenum integer NOT NULL,        databasename varchar(20) ,        
	  //totalbytes double NOT NULL,          fullaccessthreadnum integer ,     
	  //sysid integer ,                      datafilenum integer NOT NULL,     
	  //blocktype integer NOT NULL,          totalbytes double NOT NULL,       
	  //srcowner varchar(120) ,              sysid integer ,                   
	  //srctabname varchar(120) ,            srctabid integer ) ;           
	  //dstpathid integer ,                                                    
	  //lstfid integer ) ;                    
	  wociAddColumn(mt,"tabid","tabid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"tabdesc","tabdesc",COLUMN_TYPE_CHAR,80,0);
	  wociAddColumn(mt,"tabname","tabname",COLUMN_TYPE_CHAR,60,0);
	  wociAddColumn(mt,"cdfileid","cdfileid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"soledindexnum","soledindexnum",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"totindexnum","totindexnum",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"recordlen","recordlen",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"recordnum","recordnum",COLUMN_TYPE_FLOAT,16,0);
	  wociAddColumn(mt,"maxrecinblock","maxrecinblock",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"firstdatafileid","firstdatafileid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"databasename","databasename",COLUMN_TYPE_CHAR,40,0);
	  wociAddColumn(mt,"datafilenum","datafilenum",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"totalbytes","totalbytes",COLUMN_TYPE_FLOAT,16,0);
	  wociAddColumn(mt,"sysid","sysid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"blocktype","blocktype",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"srcowner","srcowner",COLUMN_TYPE_CHAR,120,0);
	  wociAddColumn(mt,"srctabname","srctabname",COLUMN_TYPE_CHAR,120,0);
	  wociAddColumn(mt,"dstpathid","dstpathid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"lstfid","lstfid",COLUMN_TYPE_INT,9,0);
	  AutoMt mt1(0,1);
	  mt1.SetHandle(GetMt("DT_TABLE"),true);
	  int rn=mt1.GetRows();
	  wociBuild(mt,rn);
	  mt.AddrFresh();
	  //wociSetXXXXValues调用可以调整内存表的记录数。
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"tabid"),0,rn,mt1.PtrInt("tabid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"cdfileid"),0,rn,mt1.PtrInt("cdfileid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"soledindexnum"),0,rn,mt1.PtrInt("soledindexnum",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"totindexnum"),0,rn,mt1.PtrInt("totindexnum",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"recordlen"),0,rn,mt1.PtrInt("recordlen",0));
	  wociSetDoubleValues(mt,wociGetColumnPosByName(mt,"recordnum"),0,rn,mt1.PtrDouble("recordnum",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"maxrecinblock"),0,rn,mt1.PtrInt("maxrecinblock",0));
  	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"firstdatafileid"),0,rn,mt1.PtrInt("firstdatafileid",0));
  	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"datafilenum"),0,rn,mt1.PtrInt("datafilenum",0));
  	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"sysid"),0,rn,mt1.PtrInt("sysid",0));
  	  int i;
  	  for(i=0;i<rn;i++) {
  //	  	strcpy(mt.PtrStr("tabdesc",i),mt1.PtrStr("tabdesc",i));
  //	  	strcpy(mt.PtrStr("tabname",i),mt1.PtrStr("tabname",i));
  //	  	strcpy(mt.PtrStr("databasename",i),mt1.PtrStr("databasename",i));
    	    mt.SetStr("tabdesc",i,(char *)mt1.PtrStr("tabdesc",i));
  	  	  mt.SetStr("tabname",i,(char *)mt1.PtrStr("tabname",i));
  	  	  mt.SetStr("databasename",i,(char *)mt1.PtrStr("databasename",i));
  	  }
  	  DeleteMt("DT_TABLE");
  	  AppendMt(mt,"DP_TABLE");
  	  //CREATE TABLE dp_index (			CREATE TABLE dt_index (           
	  // indexgid integer ,                           indexid integer ,                 
	  // tabid integer NOT NULL,                      tabid integer NOT NULL,           
	  // indextabname varchar(60) NOT NULL,           indextabname varchar(30) NOT NULL,
	  // seqindattab integer NOT NULL,                indexidindattab integer NOT NULL, 
	  // seqinidxtab integer NOT NULL,                indexidinidxtab integer NOT NULL, 
	  // issoledindex integer ,                       reuseindexid integer ,            
	  // columnsname varchar(200) ,                   issoledindex integer ,            
	  // reusecols varchar(200) ,                     columnsname varchar(200) ,        
	  // idxfieldnum integer ) ;                      cdfileid integer NOT NULL,        
	  //                                              issoledpindex integer NOT NULL,   
          //                                              reusecols varchar(200) ,          
          //                                              idxfieldnum integer ) ;
          wociClear(mt);
	  wociAddColumn(mt,"indexgid","indexgid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"tabid","tabid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"indextabname","indextabname",COLUMN_TYPE_CHAR,60,0);
	  wociAddColumn(mt,"seqindattab","seqindattab",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"seqinidxtab","seqinidxtab",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"issoledindex","issoledindex",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"columnsname","columnsname",COLUMN_TYPE_CHAR,200,0);
	  wociAddColumn(mt,"reusecols","reusecols",COLUMN_TYPE_CHAR,200,0);
	  wociAddColumn(mt,"idxfieldnum","idxfieldnum",COLUMN_TYPE_INT,9,0);
	  mt1.SetHandle(GetMt("DT_INDEX"),true);
	  rn=mt1.GetRows();
	  wociBuild(mt,rn);
	  mt.AddrFresh();
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"indexgid"),0,rn,mt1.PtrInt("indexid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"tabid"),0,rn,mt1.PtrInt("tabid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"seqindattab"),0,rn,mt1.PtrInt("indexidindattab",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"seqinidxtab"),0,rn,mt1.PtrInt("indexidinidxtab",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"issoledindex"),0,rn,mt1.PtrInt("issoledindex",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"idxfieldnum"),0,rn,mt1.PtrInt("idxfieldnum",0));
  	  for(i=0;i<rn;i++) {
  	//  	strcpy(mt.PtrStr("indextabname",i),mt1.PtrStr("indextabname",i));
  	//  	strcpy(mt.PtrStr("columnsname",i),mt1.PtrStr("columnsname",i));
  	//  	strcpy(mt.PtrStr("reusecols",i),mt1.PtrStr("reusecols",i));
  			  mt.SetStr("indextabname",i,(char*)mt1.PtrStr("indextabname",i));
	       	mt.SetStr("columnsname",i,(char*)mt1.PtrStr("columnsname",i));
		      mt.SetStr("reusecols",i,(char*)mt1.PtrStr("reusecols",i));
  	  	  if(mt1.GetInt("issoledpindex",i)==1) *mt.PtrInt("issoledindex",i)=2;
  	  }
  	  DeleteMt("DT_INTDEX");
  	  AppendMt(mt,"DP_INDEX");
	  wociReset(mt);
	  mt1.SetHandle(GetMt("DT_INDEX_ALL"),true);
	  rn=mt1.GetRows();
	  wociReset(mt);
	  wociBuild(mt,rn);
	  mt.AddrFresh();
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"indexgid"),0,rn,mt1.PtrInt("indexid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"tabid"),0,rn,mt1.PtrInt("tabid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"seqindattab"),0,rn,mt1.PtrInt("indexidindattab",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"seqinidxtab"),0,rn,mt1.PtrInt("indexidinidxtab",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"issoledindex"),0,rn,mt1.PtrInt("issoledindex",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"idxfieldnum"),0,rn,mt1.PtrInt("idxfieldnum",0));
  	  for(i=0;i<rn;i++) {
  	  //	strcpy(mt.PtrStr("indextabname",i),mt1.PtrStr("indextabname",i));
  	  //	strcpy(mt.PtrStr("columnsname",i),mt1.PtrStr("columnsname",i));
  	  //	strcpy(mt.PtrStr("reusecols",i),mt1.PtrStr("reusecols",i));
  	      mt.SetStr("indextabname",i,(char *)mt1.PtrStr("indextabname",i));
		      mt.SetStr("columnsname",i,(char *)mt1.PtrStr("columnsname",i));
		      mt.SetStr("reusecols",i,(char *)mt1.PtrStr("reusecols",i));
  	  	if(mt1.GetInt("issoledpindex",i)==1) *mt.PtrInt("issoledindex",i)=2;
  	  	if(mt1.GetInt("reuseindexid",i)>0) *mt.PtrInt("indexgid",i)=mt1.GetInt("reuseindexid",i);
  	  }
  	  DeleteMt("DT_INTDEX_ALL");
  	  AppendMt(mt,"DP_INDEX_ALL");
          
          //CREATE TABLE dp_datafilemap (       CREATE TABLE dt_datafilemap (
          //fileid integer ,                    fileid integer , 
          //filename varchar(254) NOT NULL,     filename varchar(255) NOT NULL, 
          //datapartid integer ,                subdatasetid integer , 
          //pathid integer NOT NULL,            pathid integer NOT NULL, 
          //tabid integer ,                     tabid integer , 
          //indexgid integer ,                  indexid integer , 
          //isfirstindex integer ,              isfirstindex integer , 
          //filesize integer ,                  filesize integer , 
          // recordnum integer ,                recordnum integer , 
          // procstatus integer ,               datasetid integer , 
          // compflag integer ,                 procstatus integer , 
          // fileflag integer ,                 compflag integer , 
          // idxfname varchar(254) NOT NULL,    fileflag integer ) ;
               
          // idxfsize integer not null,      
          // idxrn	  integer not null);

          wociClear(mt);
	  wociAddColumn(mt,"fileid","fileid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"filename","filename",COLUMN_TYPE_CHAR,254,0);
	  wociAddColumn(mt,"datapartid","datapartid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"pathid","pathid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"tabid","tabid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"indexgid","indexgid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"isfirstindex","isfirstindex",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"filesize","filesize",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"recordnum","recordnum",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"procstatus","procstatus",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"compflag","compflag",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"fileflag","fileflag",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"idxfname","idxfname",COLUMN_TYPE_CHAR,254,0);
	  wociAddColumn(mt,"idxfsize","idxfsize",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"idxrn","idxrn",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"blevel","blevel",COLUMN_TYPE_INT,6,0);
	  mt1.SetHandle(GetMt("DT_DATAFILEMAP"),true);
	  rn=mt1.GetRows();
	  wociBuild(mt,rn);
	  mt.AddrFresh();
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"fileid"),0,rn,mt1.PtrInt("fileid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"tabid"),0,rn,mt1.PtrInt("tabid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"datapartid"),0,rn,mt1.PtrInt("subdatasetid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"pathid"),0,rn,mt1.PtrInt("pathid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"indexgid"),0,rn,mt1.PtrInt("indexid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"isfirstindex"),0,rn,mt1.PtrInt("isfirstindex",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"filesize"),0,rn,mt1.PtrInt("filesize",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"recordnum"),0,rn,mt1.PtrInt("recordnum",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"procstatus"),0,rn,mt1.PtrInt("procstatus",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"compflag"),0,rn,mt1.PtrInt("compflag",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"fileflag"),0,rn,mt1.PtrInt("fileflag",0));
  //	  for(i=0;i<rn;i++) {
  //	  	strcpy(mt.PtrStr("filename",i),mt1.PtrStr("filename",i));
  //	  }
  
    int nCol = mt1.GetColPosByName("filename");
    int nStrLen = mt1.GetColLen("filename");
	  char *pStr = new char[rn * nStrLen];
	  memset(pStr, 0,rn * nStrLen);
	  mt1.GetStr(nCol,0,pStr,rn);
	  mt.SetStr("filename", 0,pStr, rn);
	  delete[] pStr;
  
	  // CREATE TABLE dt_indexfilemap (   
	  // fileid integer ,                 
	  // filename varchar(255) NOT NULL,  
	  // subdatasetid integer ,           
	  // pathid integer NOT NULL,         
	  // tabid integer ,                  
	  // indexid integer ,                
	  // isfirstindex integer ,           
	  // filesize integer ,               
	  // recordnum integer ,              
	  // datasetid integer ,              
	  // compflag integer ,               
          // fileflag integer ) ;             
	  //mt.FetchFirst("select * from dp.dp_indexfilemap where tabid=-1");
	  //mt.Wait();
	  //wociReset(mt);
	  mt1.SetHandle(GetMt("DT_INDEXFILEMAP"),true);
	  if(mt1.GetRows()!=rn)
	    ThrowWith("原有版本的备份文件中索引数据和目标数据文件数不一致  '%s.%s'",dbname,tbname);
  	  for(i=0;i<rn;i++) {
  	  	if(mt.GetInt("fileid",i)!=mt1.GetInt("fileid",i)) 
	          ThrowWith("原有版本的备份文件中索引数据和目标数据文件标识号不一致  '%s.%s'",dbname,tbname);
  	  	//strcpy(mt.PtrStr("idxfname",i),mt1.PtrStr("filename",i));
  	  }
  	  
  	nCol = mt1.GetColPosByName("filename");
  	nStrLen = mt1.GetColLen("filename");
	  pStr = new char[rn * nStrLen ];
	  memset(pStr,0,rn * nStrLen );
	  mt1.GetStr(nCol,0,pStr,rn);
	  mt.SetStr("idxfname",0,pStr,rn);
	  delete[] pStr;
	  
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"idxfsize"),0,rn,mt1.PtrInt("filesize",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"idxrn"),0,rn,mt1.PtrInt("recordnum",0));
  	  DeleteMt("DT_DATAFILEMAP");
  	  DeleteMt("DT_INDEXFILEMAP");
  	  AppendMt(mt,"DP_DATAFILEMAP");
  	   	  
  	  //由于老版本的备份文件中缺少源表和分区相关信息,无法恢复数据抽取sql.
  	  //CREATE TABLE dt_taskschedule (               CREATE TABLE dp_datapart (   
  	  //taskid integer ,                             datapartid integer NOT NULL, 
  	  //datasetid integer NOT NULL,                  /*已删除*/launchtime time ,            
  	  //srctabid integer NOT NULL,                   begintime time ,             
  	  //partid integer ,                             /*已删除*/endtime time ,               
  	  //onlypartid integer NOT NULL,                 istimelimit integer ,        
  	  //launchtime datetime ,                        /*已删除*/lastlaunchtime time ,        
  	  //begintime datetime ,                         status integer ,             
  	  //endtime datetime ,                           partdesc varchar(200) ,      
  	  //datecyclid integer ,                         tabid integer ,              
  	  //morecondition varchar(201) ,                 compflag integer ,           
  	  //istimelimit integer ,                        oldstatus integer ,          
  	  //strategyforpileup integer ,                  srcsysid integer NOT NULL,   
  	  //lastlaunchtime datetime ,                    extsql varchar(4000) NOT NULL,
  	  //taskstatus integer ,                         tmppathid integer NOT NULL) ;
  	  //datasetdetail varchar(31) , 
  	  //pretaskid integer , 
  	  //tabid integer , 
  	  //compflag integer , 
  	  //oldstatus integer ) ;
  	  
    wociClear(mt);
	  wociAddColumn(mt,"datapartid","datapartid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"begintime","begintime",COLUMN_TYPE_DATE,9,0);
	  wociAddColumn(mt,"istimelimit","istimelimit",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"status","status",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"partdesc","partdesc",COLUMN_TYPE_CHAR,200,0);
	  wociAddColumn(mt,"tabid","tabid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"compflag","compflag",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"oldstatus","oldstatus",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"srcsysid","srcsysid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"extsql","extsql",COLUMN_TYPE_CHAR,4000,0);
	  wociAddColumn(mt,"tmppathid","tmppathid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"blevel","blevel",COLUMN_TYPE_INT,6,0);
	  wociReset(mt);
	  mt1.SetHandle(GetMt("DT_TASKSCHEDULE"),true);
	  rn=mt1.GetRows();
	  wociBuild(mt,rn);
	  mt.AddrFresh();
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"datapartid"),0,rn,mt1.PtrInt("partid",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"tabid"),0,rn,mt1.PtrInt("tabid",0));
//	  wociSetDateValues(mt,wociGetColumnPosByName(mt,"launchtime"),0,rn,mt1.PtrDate("launchtime",0));
	  wociSetDateValues(mt,wociGetColumnPosByName(mt,"begintime"),0,rn,mt1.PtrDate("begintime",0));
//	  wociSetDateValues(mt,wociGetColumnPosByName(mt,"endtime"),0,rn,mt1.PtrDate("endtime",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"istimelimit"),0,rn,mt1.PtrInt("istimelimit",0));
//	  wociSetDateValues(mt,wociGetColumnPosByName(mt,"lastlaunchtime"),0,rn,mt1.PtrDate("lastlaunchtime",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"status"),0,rn,mt1.PtrInt("taskstatus",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"compflag"),0,rn,mt1.PtrInt("compflag",0));
	  wociSetIntValues(mt,wociGetColumnPosByName(mt,"oldstatus"),0,rn,mt1.PtrInt("oldstatus",0));
  	  for(i=0;i<rn;i++) {
  	  	//strcpy(mt.PtrStr("partdesc",i),mt1.PtrStr("datasetdetail",i));
  	  	mt.SetStr("partdesc",i,(char *)mt1.PtrStr("datasetdetail",i),1);
  	  }
  	  DeleteMt("DT_TASKSCHEDULE");
  	  AppendMt(mt,"DP_DATAPART");

	  //CREATE TABLE dp_log (
	  // tabid integer , 
	  // datapartid integer , 
	  // evt_tm datetime , 
	  // evt_tp integer , 
	  // event varchar(1000) ) ;

          wociClear(mt);
	  wociAddColumn(mt,"tabid","tabid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"datapartid","datapartid",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"evt_tm","evt_tm",COLUMN_TYPE_DATE,9,0);
	  wociAddColumn(mt,"evt_tp","evt_tp",COLUMN_TYPE_INT,9,0);
	  wociAddColumn(mt,"event","event",COLUMN_TYPE_CHAR,1000,0);
	  wociBuild(mt,1);
	  mt.AddrFresh();
  	AppendMt(mt,"DP_LOG");
        }
}
