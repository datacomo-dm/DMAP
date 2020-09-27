#ifdef WIN32
#include <direct.h>
#include <io.h>
#else
#include <unistd.h>
#include <ctype.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include "dt_lib.h"
#include "zlib.h"
#define COMPRESSLEVEL 5
#ifdef WIN32
#define mCopyFile(a,b) CopyFile(a,b,FALSE)
#else
#define mCopyFile(a,b) uCopyFile(a,b)
#endif

#ifdef __unix
int uCopyFile(const char * src,const char *dest) {
 FILE *fsrc=fopen(src,"rb");
 if(fsrc==NULL) 
   ThrowWith("Open source file error while copy file '%s' to '%s'",
 	  	   src,dest);
 FILE *fdest=fopen(dest,"w+b");
 if(fdest==NULL) {
 	ThrowWith("Create dest file error while copy file '%s' to '%s'",
 	  	   src,dest);
 	fclose(fsrc);
 	return -2;
 }
 char buf[1024];
 for(;;) {
 	int l=fread(buf,1,1024,fsrc);
 	if(l>0) {
 	  l=fwrite(buf,1,l,fdest);
 	  if(l<1) {
 	  	fclose(fsrc);
 	  	fclose(fdest);
 	  	ThrowWith("Wirte file error while copy file '%s' to '%s'",
 	  	   src,dest);
 	  }
 	}
 	else break;
 	  
 }
 fclose(fsrc);
 fclose(fdest);
 return 1;		
}
#endif

int CopyMySQLTable(const char *path,const char *sdn,const char *stn,
				   const char *ddn,const char *dtn)
{
	char srcf[300],dstf[300];
	// check destination directory
	sprintf(dstf,"%s%s",path,ddn);
	xmkdir(dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
	mCopyFile(srcf,dstf);
	return 1;
}


int CreateMtFromFile(int maxrows,char *filename)
{
	FILE *fp=fopen(filename,"rb");
	if(!fp)
		ThrowWith("CreateMt from file '%s' which could not open.",filename);
	int cdlen=0,cdnum=0;
	fread(&cdlen,sizeof(int),1,fp);
	fread(&cdnum,sizeof(int),1,fp);
	if(cdlen==0 || cdnum==0)
		ThrowWith("Could not read columns info from file 's' !",filename);
	char *pbf=new char[cdlen];
	if(fread(pbf,1,cdlen,fp)!=cdlen) {
		delete [] pbf;
		ThrowWith("Could not read columns info from file 's' !",filename);
	}
	int mt=wociCreateMemTable();
	wociImport(mt,NULL,0,pbf,cdnum,cdlen,maxrows,0);
	delete []pbf;
	return mt;
}

	MySQLConn::MySQLConn() {
		memset(host,0,sizeof(host));
		memset(username,0,sizeof(username));
		memset(password,0,sizeof(password));
		memset(dbname,0,sizeof(dbname));
		myData=NULL;
		result=NULL;
	}

	 void MySQLConn::FlushTables()
	{
		DoQuery("flush tables");
#ifdef WIN32
			Sleep(2000);
#else
			sleep(2);
#endif

	}

         void MySQLConn::SelectDB(char *db) {
		if(!myData) ThrowWith("Connection has not build while select db:%s\n",db);
		if ( mysql_select_db( myData, db ) < 0 ) {
			mysql_close( myData ) ;
			ThrowWith( "Can't select the %s database !\n", dbname ) ;
		}
	}

	void MySQLConn::Connect(char *host,char *username,char *password,char *dbname,unsigned int portnum) {
		if(host) strcpy(this->host,host);
		else this->host[0]=0;
		if(username) strcpy(this->username,username);
		else strcpy(this->username,"root");
		if(password) strcpy(this->password,password);
		else strcpy(this->password,"root");
		if(dbname) strcpy(this->dbname,dbname);
		else strcpy(this->dbname,"mysql");
		if(portnum!=0) this->portnum=portnum;
		else portnum=3306;
		if ( (myData = mysql_init((MYSQL*) 0)) && 
         mysql_real_connect( myData, host,username,password,NULL,portnum,
			   NULL, 0 ) )
		{
			if ( mysql_select_db( myData, this->dbname ) < 0 ) {
				mysql_close( myData ) ;
				myData=NULL;
				ThrowWith( "Can't select the '%s' database !\n", this->dbname ) ;
			}
		}
		else {
			myData=NULL;
			ThrowWith( "Can't connect to the mysql server on port %d !\n",
					portnum ) ;
		}
	}

	int MySQLConn::DoQuery(const char *szSQL)
	{
		if(!myData) ThrowWith("Connection has not build while query:%s\n",szSQL);
		if(result) {
			mysql_free_result(result);
			result=NULL;
		}
		//char dstsql[3000];
		//mysql_real_escape_string(myData,dstsql,szSQL,strlen(szSQL));
		if ( mysql_query( myData, szSQL)) {//dstsql,strlen(dstsql) ) ) 
			//  ./include/mysqld_error.h :
			//   #define ER_CANT_DROP_FIELD_OR_KEY 1091
			//   #define ER_NO_SUCH_TABLE 1146
			if(mysql_errno(myData)==1091 || mysql_errno(myData)==1146) return 0;
  		 ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", szSQL ,
			 mysql_errno(myData),mysql_error(myData)) ;
		}
		result=mysql_store_result(myData);
		if(!result) {
			//ERROR 1091: Can't DROP 'tab_gsmvoicdr2_2'. Check that column/key exists
			//if(mysql_errno(myData)==1091) return ;
			if (mysql_errno(myData))
			{
				ThrowWith("Error: %s\n", mysql_error(myData));
			}
			else if (mysql_field_count(myData) == 0)
			{
				// query does not return data
				// (it was not a SELECT)
				num_rows = mysql_affected_rows(myData);
				//const char *info=mysql_info(myData) ;
				//if(info) lgprintf(info);
			}
		}
		return num_rows;
	}

	bool MySQLConn::TouchTable(const char *tabname) {
		if(!myData) ThrowWith("Connection has not build while touchtable:%s\n",tabname);
		char sql[300];
		sprintf(sql,"desc %s",tabname);
		if(result) {
			mysql_free_result(result);
			result=NULL;
		}
		if ( mysql_query( myData, sql ) ) {
		 if(mysql_errno(myData)==1146) return false;
  		 ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", sql ,
			 mysql_errno(myData),mysql_error(myData)) ;
		}
		result=mysql_store_result(myData);
		return true;
	}


	
	void SysAdmin::CreateDT(int tabid) 
	{
	    MySQLConn conn;
	    wociSetTraceFile("CreateDT");
	    conn.Connect(NULL,NULL,NULL,"dt");
		int tabp=wociSearchIK(dt_table,tabid);
		int srctabid=dt_table.GetInt("srctabid",tabp);
		int srctabp=wociSearchIK(dt_srctable,srctabid);

		int srcid=dt_srctable.GetInt("srcsysid",srctabp);
		int srcidp=wociSearchIK(dt_srcsys,srcid);
		//int tabid=dt_table.GetInt("tabid",tabp);
		
		//Reset recordnum before create data table and index
		char sqlbf[200];
		sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
		lgprintf("记录数清零...");
		conn.DoQuery(sqlbf);

		lgprintf("Flush MySQL...");
		conn.FlushTables();
		//建立到数据源的连接
		AutoHandle srcsys;
		char tabname[300];
		const char *pathval=GetPathName(0,"cdes");
		srcsys.SetHandle(BuildSrcDBC(srcidp));
	    //构造源数据的内存表结构
		AutoMt srcmt(srcsys,0);
		srcmt.SetHandle(GetSrcTableStructMt(srctabp,tabp,srcsys));

		//创建目标表
		char *dbsn=dt_table.PtrStr("databasename",tabp);
		conn.SelectDB(dbsn);
		strcpy(tabname,dt_table.PtrStr("tabname",tabp));
		bool newdttab=CreateTableOnMysql(pathval,conn,srcmt,tabname,tabid,0,0,true);
		
		//建立索引表和目标表、索引表的索引
		AutoMt idxtarget(dts,100);
		AutoMt idxmt(dts,100);
		//每一个独立索引都需要建立索引表
		idxmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",
			tabid);
		int rn=idxmt.Wait();
		if(rn<1) 
			ThrowWith("no soled index defined in table '%s'",
			  dt_srctable.PtrStr("srctabname",srctabp));
		for(int i=0;i<rn;i++) {
			CreateIndexTable(conn,srcmt,tabid,idxmt.GetInt("indexid",i),
				idxmt.PtrStr("indextabname",i),idxmt.PtrStr("columnsname",i),pathval,true);
			CreateIndex(tabname,idxmt.PtrStr("indextabname",i),0,conn,tabid,
				idxmt.GetInt("indexid",i),true);
		}
	}


DataDump::DataDump(int dtdbc,int maxmem,int maxrows):fnmt(dtdbc,300)
{
	this->dtdbc=dtdbc;
	//Create fnmt and build column structrues.
	fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
	fnmt.Wait();
	fnmt.Reset();
	taskid=srctabid=indexid=0;
	memlimit=maxmem;
	idxmaxrows=maxrows;
	maxblockrn=0;
}

int DataDump::BuildMiddleFilePath(int _fid) {
	int fid=_fid;
	sprintf(tmpfn,"%smddt_%d.dat",dp.tmppath[0]
		,fid);
	sprintf(tmpidxfn,"%smdidx_%d.dat",dp.tmppath[0],
		fid);
	while(true) {
		int freem=GetFreeM(tmpfn);
		if(freem<1024) {
			lgprintf("Available space on hard disk less then 1G : %dM,waitting 5 minutes for free...",freem);
#ifdef WIN32
			Sleep(300000);
#else
			sleep(300);
#endif
		}
		else break;
	}
	return fid;
}

void DataDump::ProcBlock(int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid)
{
	freeinfo1("Start ProcBlock");
	int fid=BuildMiddleFilePath(_fid);
	blockmt.Reset();
	int cur=*pCur;
	char *idxcolsname=dp.idxp[idx].idxcolsname;
	int *ikptr=NULL;
	int strow=-1;
	int subrn=0;
	int blockrn=0;
	
	AutoMt idxdt(0,idxmaxrows);
	wociCopyColumnDefine(idxdt,cur,idxcolsname);
	wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_INT,0,0);
//	wociAddColumn(idxdt,"idx_storesize","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);
	idxdt.Build();
	freeinfo1("After Build indxdt mt");
	
	void *idxptr[20];
	int pidxc1[10];
	bool pkmode=false;
	sorttm.Start();
	int cn1=wociConvertColStrToInt(cur,idxcolsname,pidxc1);
	if(cn1==1 && wociGetColumnType(cur,pidxc1[0])==COLUMN_TYPE_INT)
		pkmode=true;
	if(!pkmode) {
		wociSetSortColumn(cur,idxcolsname);
		wociSortHeap(cur);
	}
	else {
		wociSetIKByName(cur,idxcolsname);
		wociOrderByIK(cur);
		wociGetIntAddrByName(cur,idxcolsname,0,&ikptr);
	}
	sorttm.Stop();
	int idx_blockoffset=0,idx_store_size=0,idx_startrow=0,idx_rownum=0;
	int idxc=cn1;
	idxptr[idxc++]=&idx_blockoffset;
//	idxptr[idxc++]=&idx_store_size;
	idxptr[idxc++]=&idx_startrow;
	idxptr[idxc++]=&idx_rownum;
	idxptr[idxc++]=&fid;
	idxptr[idxc]=NULL;
	dt_file df;
	df.Open(tmpfn,1,fid);
	idx_blockoffset=df.WriteHeader(cur);
	idxdt.Reset();
	int rn=wociGetMemtableRows(cur);
	adjtm.Start();
	for(int i=0;i<rn;i++) {
		int thisrow=pkmode?wociGetRawrnByIK(cur,i):wociGetRawrnBySort(cur,i);
		//int thisrow=wociGetRawrnByIK(cur,i);
		if(strow==-1) {
		 strow=thisrow;
		 idx_startrow=blockrn;
		}
		//子块分割
		else 
		  if(pkmode?(ikptr[strow]!=ikptr[thisrow]):
		    (wociCompareSortRow(cur,strow,thisrow)!=0) ){
		  //if(ikptr[strow]!=ikptr[thisrow]) {
			for(int c=0;c<cn1;c++) {
				idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
			}
			idx_rownum=blockrn-idx_startrow;
			wociInsertRows(idxdt,idxptr,NULL,1);
			strow=thisrow;
			idx_startrow=blockrn;
		}
		//blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
		wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
		blockrn++;//=wociGetMemtableRows(blockmt);
		//块和子块的分割
		if(blockrn>=maxblockrn) {
			adjtm.Stop();
			fiotm.Start();
			for(int c=0;c<cn1;c++) {
				idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
			}
			idx_rownum=blockrn-idx_startrow;
			wociInsertRows(idxdt,idxptr,NULL,1);
			idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL);
			idx_startrow=0;
			strow=-1;blockrn=0;
			blockmt.Reset();
			fiotm.Stop();
			adjtm.Start();
		}
	}
	adjtm.Stop();
	//保存最后的块数据
	if(blockrn) {
		for(int c=0;c<cn1;c++) {
			idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
		}
		idx_rownum=blockrn-idx_startrow;
		wociInsertRows(idxdt,idxptr,NULL,1);
		idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL);
		idx_startrow=0;
		strow=-1;blockrn=0;
		blockmt.Reset();
	}
	//保存索引数据
	{
		dt_file di;
		di.Open(tmpidxfn,1);
		di.WriteHeader(idxdt,wociGetMemtableRows(idxdt));
		di.WriteMt(idxdt,COMPRESSLEVEL);
	}
	//保存文件索引
	{
		void *ptr[20];
		int subdatasetid=((partid%100)*100+(idx%100))*10000+(datasetid%10000);
		ptr[0]=&fid;ptr[1]=&taskid;ptr[2]=&partid;
		ptr[3]=&subdatasetid;ptr[4]=&srctabid;ptr[5]=&dp.tabid;
		int rn=df.GetRowNum();
		int fl=df.GetLastOffset();
		ptr[6]=&rn;ptr[7]=&fl;
		char now[10];
		wociGetCurDateTime(now);
		ptr[8]=tmpfn;ptr[9]=tmpidxfn;ptr[10]=now;
		int state=1;
		ptr[11]=&state;
		char nuldt[10];
		memset(nuldt,0,10);
		ptr[12]=now;//nuldt;
		ptr[13]=&dp.idxp[idx].idxid;
		ptr[14]=dp.idxp[idx].idxcolsname;
		ptr[15]=dp.idxp[idx].idxreusecols;
		ptr[16]=&datasetid;
		ptr[17]=NULL;
		wociInsertRows(fnmt,ptr,NULL,1);
	}
	freeinfo1("End ProcBlock");
}

int DataDump::DoDump(SysParam &sp) {
	wociSetTraceFile("dtdump");
	taskid=sp.GetFirstTaskID(NEWTASK,srctabid,datasetid);
	if(taskid<1) return 0;
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledInexParam(srctabid,&dp);
	if(xmkdir(dp.tmppath[0])) 
		ThrowWith("临时主路径无法建立,源表:%d,任务号:%d,路径:%s.",
		       srctabid,taskid,dp.tmppath[0]);
	if(xmkdir(dp.tmppath[1]))
		ThrowWith("临时备用路径无法建立,源表:%d,任务号:%d,路径:%s.",
		       srctabid,taskid,dp.tmppath[1]);
	int partoff=0;
	int realrn=memlimit/dp.rowlen;
	if(realrn>dp.maxrecnum) realrn=dp.maxrecnum;
	{
	AutoStmt st(dtdbc);
	st.Prepare("delete from dt_middledatafile where datasetid=%d and taskid=%d",datasetid,taskid);
	st.Execute(1);
	st.Wait();
	}
	//realrn=50000;
	//indexparam *ip=&dp.idxp[dp.psoledindex];
	maxblockrn=sp.GetMaxBlockRnFromSrcTab(srctabid);
	AutoMt blockmt(0,maxblockrn);
	fnmt.Reset();
	int partid=0;
	AutoHandle srcdbc;
	fnorder=0;
	srcdbc.SetHandle(sp.BuildSrcDBCFromSrcTableID(srctabid));
	sp.UpdateTaskStatus(DUMPING,taskid);
	/*
	try {
	*/
	wociSetEcho(TRUE);
	
	while(partid=sp.GetDumpSQL(taskid,partoff++,dumpsql),partid!=-1) {
		//idxdt.Reset();
		
		TradeOffMt dtmt(0,realrn);
		AutoStmt stmt(srcdbc);
		stmt.Prepare(dumpsql);
		dtmt.Cur()->Build(stmt);
		dtmt.Next()->Build(stmt);
		blockmt.Clear();
		blockmt.Build(stmt);
		//准备数据索引表插入变量数组
		dtmt.FetchFirst();
		
		int rn=dtmt.Wait();
		while(rn>0) {
			dtmt.FetchNext();
			lgprintf("开始数据处理");
			freeinfo1("before call prcblk");
			for(int i=0;i<dp.soledindexnum;i++) {
				ProcBlock(partid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextFileID());
			}
			lgprintf("数据处理结束");
			freeinfo1("after call prcblk");
			rn=dtmt.Wait();
		}
		//delete dtmt;
		//delete stmt;
	}
	wociSetEcho(FALSE);
	wociAppendToDbTable(fnmt,"dt_middledatafile",dtdbc);
	sp.UpdateTaskStatus(DUMPED,taskid);
	/*
	}
	catch(...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("数据导出异常终止，任务号:%d,分区号:%d,中间文件数:%d.",taskid,partid,frn);
		int i;
		errprintf("删除中间文件...");
		for(i=0;i<frn;i++) {
			errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
			   fnmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("datafilename",i));
			unlink(fnmt.PtrStr("indexfilename",i));
		}
		errprintf("恢复任务状态.");
		sp.UpdateTaskStatus(NEWTASK,taskid);
		errprintf("删除中间文件记录...");
		AutoStmt st(dtdbc);
		st.Prepare("delete from dt_middledatafile where datasetid=%d and taskid=%d",datasetid,taskid);
		st.Execute(1);
		st.Wait();
		throw;
	}
	*/
	lgprintf("数据抽取结束,任务状态1-->2,taskid:%d",taskid);
	lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
	lgprintf("结束");
	//lgprintf("按任意键继续...");
	//getchar();
	//MYSQL中的MY_ISAM不支持事务处理，对MYSQL表的修改不需要提交.
	return 0;
}

MiddleDataLoader::MiddleDataLoader(SysAdmin *_sp):
	   indexmt(0,0),mdindexmt(0,0),blockmt(0,0),mdf(_sp->GetDTS(),300)
	  {
		  sp=_sp;
		  tmpfilenum=0;
		  pdtf=NULL;
		  pdtfid=NULL;
		  dtfidlen=0;
	  }

int MiddleDataLoader::Load(MySQLConn &conn,int MAXINDEXRN) {
	//Check deserved temporary(middle) fileset
	//检查状态为1的任务
	wociSetTraceFile("MLoad");

	mdf.FetchAll("select * from dt_middledatafile where procstate<=1  order by subdatasetid");
	int rn=mdf.Wait();
	if(rn>0) {
		mdf.FetchAll("select * from dt_middledatafile where subdatasetid=%d",
			mdf.GetInt("subdatasetid",0));
		rn=mdf.Wait();
		if(rn<1) 
			ThrowWith("MiddleDataLoader::Load : 确定数据子集后找不到中间数据记录。");
	}
	else return 0;
	//取基本参数
	int subdatasetid=mdf.GetInt("subdatasetid",0);
	int srctabid=mdf.GetInt("srctabid",0);
	int indexid=mdf.GetInt("indexid",0);
	int datasetid=mdf.GetInt("datasetid",0);
	int tabid=mdf.GetInt("tabid",0);
	int taskid=mdf.GetInt("taskid",0);
	//从dt_datafilemap(存blockmt文件表)和dt_indexfilemap(存indexmt文件表)
	//建立内存表结构
	char fn[300];
	AutoMt fnmt(sp->GetDTS(),300);
	fnmt.FetchAll("select * from dt_datafilemap where rownum<1");
	fnmt.Wait();
	AutoMt fnidxmt(sp->GetDTS(),300);
	fnidxmt.FetchAll("select * from dt_indexfilemap where rownum<1");
	fnidxmt.Wait();
#ifdef __unix
	long dispct=0,lstdispct=0;
#else
	__int64 dispct=0,lstdispct=0;
#endif
	
	char sqlbf[200];
	try {	
	//为防止功能重入,中间文件状态修改.
	lgprintf("修改中间文件的处理状态(subdatasetid:%d,%d个文件)：1-->2",subdatasetid,rn);
	sprintf(sqlbf,"update dt.dt_middledatafile set procstate=2 where subdatasetid=%d and procstate<=1 and taskid=%d",subdatasetid,taskid);
	if(conn.DoQuery(sqlbf)!=rn) 
		ThrowWith("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
			"   taskid:%d,tabid:%d,datasetid:%d,subdatasetid:%d,srctabid:%d,indexid:%d.\n",
			taskid,tabid,datasetid,subdatasetid,srctabid,indexid);

	tmpfilenum=rn;
	//索引数据文件遍历，累加索引总行数
	int idxrn=0;
	int i;
	for( i=0;i<rn;i++) {
		dt_file df;
		df.Open(mdf.PtrStr("indexfilename",i),0);
		idxrn+=df.GetRowNum();
	}
	lgprintf("索引记录数:%d",idxrn);
	//建立中间索引(中间文件数据块索引)内存表mdindexmt和目标数据块内存表blockmt
	int maxblockrn=sp->GetMaxBlockRnFromSrcTab(srctabid);
	{
	dt_file idf;
	idf.Open(mdf.PtrStr("indexfilename",0),0);
	mdindexmt.SetHandle(idf.CreateMt(idxrn));
	//wociAddColumn(idxmt,"dtfileid",NULL,COLUMN_TYPE_INT,4,0);
	//idxmt.SetMaxRows(idxrn);
	mdindexmt.Build();
	idf.Open(mdf.PtrStr("datafilename",0),0);
	blockmt.SetHandle(idf.CreateMt(maxblockrn));
	//mdblockmt.SetHandle(idf.CreateMt(maxblockrn));
	}
	int crn=0;
//	wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
	// pdtfid为一个字符数组，偏移为x的值表示中间索引内存表第x行的文件序号(Base0);
	if(dtfidlen<idxrn) 
	{
		if(pdtfid)
			delete [] pdtfid;
		pdtfid=new unsigned char [idxrn];
		dtfidlen=idxrn;
	}
	//pdtf为file_mt对象的数组。存放数据文件对象。
	if(pdtf) delete [] pdtf;
	pdtf=new file_mt[rn];
	//mdindexmt.SetMaxRows(idxrn);
	//读入全部索引数据到mdindexmt(中间索引内存表),并打开全部数据文件
	//pdtfid指向对应的文件序号。
	lgprintf("读索引数据...");
	for(i=0;i<rn;i++) {
		dt_file df;
		df.Open(mdf.PtrStr("indexfilename",i),0);
		int brn=df.ReadMt(0,0,mdindexmt,false);
		for(int j=crn;j<crn+brn;j++)
			pdtfid[j]=(unsigned char )i;
		crn+=brn;
		//pdtf[i].SetParalMode(true);
		pdtf[i].Open(mdf.PtrStr("datafilename",i),0);
	}
	lgprintf("索引数据:%d.",crn);
	//对mdindexmt(中间索引内存表)做排序。
	//由于排序不涉及内存表的数据排列，而是新建记录顺序表，因此，
	// pdtfid作为内存表外的等效内存表字段，不需做处理。
	lgprintf("排序...");
	{
		char sort[300];
		sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
		wociSetSortColumn(mdindexmt,sort);
		wociSortHeap(mdindexmt);
	}
	lgprintf("排序完成.");
	//取得全部独立索引结构
	sp->GetSoledInexParam(srctabid,&dp);
	//检查需要处理的中间数据是否使用主独立索引，如果是，isfirstidx=1.
	int isfirstidx=0;
	indexparam *ip;
	{
	int idxp=dp.GetOffset(indexid);
	ip=&dp.idxp[idxp];
	if(idxp==dp.psoledindex) isfirstidx=1;
	}
	//从结构描述文件建立indexmt,indexmt是目标索引内存表。是建立目标索引表的数据源。
	indexmt.SetHandle(CreateMtFromFile(MAXINDEXRN,ip->cdsfilename));

	//取独立索引和复用索引在blockmt(目标数据块内存表)结构中的位置，
	// 检查结构描述文件建立的索引是否和系统参数表中指定的字段数相同。
	void *indexptr[40];
	int pblockc1[20];
	int pblockc2[20];
	bool pkmode=false;
	int bcn1=wociConvertColStrToInt(blockmt,ip->idxcolsname,pblockc1);
	if(bcn1==1 && wociGetColumnType(blockmt,pblockc1[0])==COLUMN_TYPE_INT)
		pkmode=true;
	int bcn2=wociConvertColStrToInt(blockmt,ip->idxreusecols,pblockc2);
	if(wociGetColumnNumber(indexmt)!=bcn1+bcn2+6) {
		ThrowWith("Column number error,subdatasetid:%d,colnum:%d,deserved:%d",
			subdatasetid,wociGetColumnNumber(indexmt),bcn1+bcn2+6);
	}
	//设置dt_index中的idxfieldnum
	sprintf(sqlbf,"update dt.dt_index set idxfieldnum=%d where indexid=%d or reuseindexid=%d",
		bcn1+bcn2,indexid,indexid);
	lgprintf("设置dt_index中的索引字段数(idxfieldnum).");
	conn.DoQuery(sqlbf);

	//取独立索引在mdindexmt(中间索引存表)结构中的位置。
	//设置对indexmt插入记录需要的结构和变量。
	int pidxc1[20];
	int cn1=wociConvertColStrToInt(mdindexmt,ip->idxcolsname,pidxc1);
	int dtfid,blockstart,blocksize,blockrn=0,startrow,rownum;
	int stcn=bcn1+bcn2;
	indexptr[stcn++]=&dtfid;
	indexptr[stcn++]=&blockstart;
	indexptr[stcn++]=&blocksize;
	indexptr[stcn++]=&blockrn;
	indexptr[stcn++]=&startrow;
	indexptr[stcn++]=&rownum;
	indexptr[stcn]=NULL;
	//indexmt中的blocksize,blockstart?,blockrownum需要滞后写入，
	//因而需要取出这些字段的首地址。
	int *pblocksize;
	int *pblockstart;
	int *pblockrn;
	wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
	wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
	wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
	//mdindexmt中下列字段是读中间数据文件的关键项。
	int *poffset,*pstartrow,*prownum;
	wociGetIntAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
	wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
	wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);

	//indexmt 记录行数计数复位
	int indexmtrn=0;
	
	//建立目标数据文件和目标索引文件对象(dt_file).
	// 目标数据文件和目标索引文件一一对应，目标数据文件中按子块方式存储内存表
	//  目标索引文件中为一个单一的内存表，文件头结构在写入内存表时建立
	dtfid=sp->NextFileID();
	dp.usingpathid=0;
	sprintf(fn,"%s%d_%d_%d.dat",dp.dstpath[0],srctabid,subdatasetid,dtfid);
	dt_file dstfile;
	dstfile.Open(fn,1);
	blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
	char idxfn[300];
	sprintf(idxfn,"%s%d_%d_%d.idx",dp.dstpath[0],srctabid,subdatasetid,dtfid);
	dt_file idxf;
	idxf.Open(idxfn,1);

	startrow=0;
	rownum=0;
	blockrn=0;
	int subtotrn=0;
	lgprintf("开始数据处理(MiddleDataLoading)....");

	/*******按照Sort顺序遍历mdindexmt(中间索引内存表)***********/
	//
	//
	lgprintf("创建文件,编号:%d...",dtfid);
	int firstrn=wociGetRawrnBySort(mdindexmt,0);
	//mytimer instm,
	for(i=0;i<idxrn;i++) {
		int thisrn=wociGetRawrnBySort(mdindexmt,i);
		int rfid=pdtfid[thisrn];
		int sbrn=prownum[thisrn];
		int sbstart=pstartrow[thisrn];
		int sameval=mdindexmt.CompareRows(firstrn,thisrn,pidxc1,cn1);
		
		if(sameval!=0) {
			//要处理的数据块和上次的数据块不是一个关键字，
			// 所以，先保存上次的关键字索引，关键字的值从blockmt中提取。
			// startrow变量始终保存第一个未存储关键字索引的数据块开始行号。
			int c;
			for(c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
				wociInsertRows(indexmt,indexptr,NULL,1);
				dispct++;
				if(dispct-lstdispct>5000) {
					lstdispct=dispct;
					lgprintf("已处理%d组数据",dispct);
				}
			}
			firstrn=thisrn;
			//这里，出现startrow暂时指向无效行的情况(数据未填充).
			startrow=blockrn;
			rownum=0;
		}
		//从数据文件中读入数据块
		int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
		//数据块拆分 
		//检查数据块是否需要拆分
		if(blockrn+sbrn>maxblockrn) {
			//每个数据块至少需要达到最大值的80%。
			if(blockrn<maxblockrn*.8) {
				//如果不足80%，把当前处理的数据块拆分
				int rmrn=maxblockrn-blockrn;
				wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
				rownum+=rmrn;
				sbrn-=rmrn;
				sbstart+=rmrn;
				blockrn+=rmrn;
			}
			//保存子快索引
			if(startrow<blockrn) {
			int c;
			for(c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
				wociInsertRows(indexmt,indexptr,NULL,1);
				dispct++;
			}
			}

			//保存块数据
			blocksize=dstfile.WriteMt(blockmt,COMPRESSLEVEL)-blockstart;
			int irn=wociGetMemtableRows(indexmt);
			while(--irn>=0) {
				if(pblockstart[irn]==blockstart) {
					pblocksize[irn]=blocksize;
					pblockrn[irn]=blockrn;
				}
				else break;
			}
			blockstart+=blocksize;
			subtotrn+=blockrn;
			if(blockstart>2000000000) {
			//增加文件对照表记录(dt_datafilemap)
				{
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=fn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&blockstart;
					fnmtptr[8]=&subtotrn;
					fnmtptr[9]=&datasetid;
					int procstatus=0;
					fnmtptr[10]=&procstatus;
					int compflag=COMPRESSLEVEL;
					fnmtptr[11]=&compflag;
					wociInsertRows(fnmt,fnmtptr,NULL,1);
				}
			//
			//增加索引数据对照表记录(dt_indexfilemap)
				{
					int irn1=wociGetMemtableRows(indexmt);
					idxf.WriteHeader(indexmt,dtfid);
					int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL);
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=idxfn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&idxfsize;
					fnmtptr[8]=&irn1;
					fnmtptr[9]=&datasetid;
					int compflag=COMPRESSLEVEL;
					fnmtptr[10]=&compflag;
					wociInsertRows(fnidxmt,fnmtptr,NULL,1);
				}
			//
				dtfid=sp->NextFileID();
				sprintf(fn,"%s%d_%d_%d.dat",dp.dstpath[0],srctabid,subdatasetid,dtfid);
				dstfile.SetFileHeader(subtotrn,fn);
				dstfile.Open(fn,1);
				blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
				lgprintf("创建文件,编号:%d...",dtfid);
				sprintf(idxfn,"%s%d_%d_%d.idx",dp.dstpath[0],srctabid,subdatasetid,dtfid);
				idxf.SetFileHeader(wociGetMemtableRows(indexmt),idxfn);
				idxf.Open(idxfn,1);
				idxf.WriteHeader(indexmt,0,dtfid);
				indexmt.Reset();
				subtotrn=0;
				blockrn=0;
				lgprintf("创建文件,编号:%d...",dtfid);
				
			} // end of IF blockstart>2000000000)
			blockmt.Reset();
			blockrn=0;
			firstrn=thisrn;
			startrow=blockrn;
			rownum=0;
		} //end of blockrn+sbrn>maxblockrn
		wociCopyRowsTo(mt,blockmt,-1,sbstart,sbrn);
		rownum+=sbrn;
		blockrn+=sbrn;
	} // end of for(...)

	if(blockrn>0) {
			
			//保存子快索引
			int c;
			for( c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
			 wociInsertRows(indexmt,indexptr,NULL,1);
			 dispct++;
			}
			//保存块数据
			//保存块数据
			blocksize=dstfile.WriteMt(blockmt,COMPRESSLEVEL)-blockstart;
			int irn=wociGetMemtableRows(indexmt);
			while(--irn>=0) {
				if(pblockstart[irn]==blockstart) {
					pblocksize[irn]=blocksize;
					pblockrn[irn]=blockrn;
				}
				else break;
			}
			blockstart+=blocksize;
			subtotrn+=blockrn;
			//增加文件对照表记录(dt_datafilemap)
				{
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=fn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&blockstart;
					fnmtptr[8]=&subtotrn;
					fnmtptr[9]=&datasetid;
					int procstatus=0;
					fnmtptr[10]=&procstatus;
					int compflag=COMPRESSLEVEL;
					fnmtptr[11]=&compflag;
					wociInsertRows(fnmt,fnmtptr,NULL,1);
				}
			//增加索引数据对照表记录(dt_indexfilemap)
				{
					int irn1=wociGetMemtableRows(indexmt);
					idxf.WriteHeader(indexmt,dtfid);
					int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL);
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=idxfn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&idxfsize;
					fnmtptr[8]=&irn1;
					fnmtptr[9]=&datasetid;
					int compflag=COMPRESSLEVEL;
					fnmtptr[10]=&compflag;
					wociInsertRows(fnidxmt,fnmtptr,NULL,1);
				}
			//
				
			//
			dstfile.SetFileHeader(subtotrn,NULL);
			idxf.SetFileHeader(wociGetMemtableRows(indexmt),NULL);
			indexmt.Reset();
			blockmt.Reset();
			blockrn=0;
			startrow=blockrn;
			rownum=0;
	}

	lgprintf("删除数据子集编号为%d的数据和索引记录...",subdatasetid);
	{
	AutoMt dfnmt(sp->GetDTS(),100);
	dfnmt.FetchAll("select * from dt_datafilemap where subdatasetid=%d",subdatasetid);
	int dfn=dfnmt.Wait();
	if(dfn>0) {
	  AutoStmt st(sp->GetDTS());
	  for(int di=0;di<dfn;di++)
	  {
		  lgprintf("删除'%s'和附加的depcp,dep5文件",dfnmt.PtrStr("filename",di));
		  unlink(dfnmt.PtrStr("filename",di));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
	  }
	  st.Prepare(" delete from dt_datafilemap where subdatasetid=%d",subdatasetid);
	  st.Execute(1);
	  st.Wait();
	}
	
	dfnmt.FetchAll("select * from dt_indexfilemap where subdatasetid=%d",subdatasetid);
	dfn=dfnmt.Wait();
	if(dfn>0) {
	  AutoStmt st(sp->GetDTS());
	  for(int di=0;di<dfn;di++)
	  {
		  lgprintf("删除'%s'和附加的depcp,dep5文件",dfnmt.PtrStr("filename",di));
		  unlink(dfnmt.PtrStr("filename",di));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
	  }
	  st.Prepare(" delete from dt_indexfilemap where subdatasetid=%d",subdatasetid);
	  st.Execute(1);
	  st.Wait();
	}
	}
	//目标表数据索引文件检查,数据子集编号相同的记录删除,但数据文件保留(???),需要手工删除.
	while(true){
	 AutoMt ckmt(sp->GetDTS(),300);
	 ckmt.FetchAll("select * from dt_table where tabid=%d",tabid);
	 if(ckmt.Wait()<1) 
	 	ThrowWith("dt_table中没有tabid=%d的记录！",tabid);
	 if(ckmt.GetInt("recordnum",0)>0 ) {
	 	errprintf("编号为%d的表:%s不是空表，继续数据处理以前需要清空！",
	 	    tabid,ckmt.PtrStr("tabname",0));
	 	printf("\n现在就清空表?删除(Y)/放弃(A)/继续(其他):");
		char ans[100];
		gets(ans);
		if(tolower(ans[0])=='y') {
			DestLoader dl(sp);
			dl.RemoveTable(ckmt.PtrStr("databasename",0),
			               ckmt.PtrStr("tabname",0),conn,false);
		}
		else if(tolower(ans[0])=='a')
			ThrowWith("数据整理时用户选择终止操作：目标表%d非空,任务%d.",tabid,taskid);
	 }
	 else break;
	 //sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	 //lgprintf("记录数清零...");
	 //conn.DoQuery(sqlbf);
	 //lgprintf("源表锁定...");
	 //lgprintf("Flush MySQL...");
	 //conn.FlushTables();//.DoQuery("flush tables");
	}

	wociAppendToDbTable(fnmt,"dt_datafilemap",sp->GetDTS());
	wociAppendToDbTable(fnidxmt,"dt_indexfilemap",sp->GetDTS());
	lgprintf("修改中间文件的处理状态(subdatasetid:%d,%d个文件)：2-->3",subdatasetid,rn);
	sprintf(sqlbf,"update dt.dt_middledatafile set procstate=3 where subdatasetid=%d",subdatasetid);
	conn.DoQuery(sqlbf);
	}	
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		int fidxrn=wociGetMemtableRows(fnidxmt);
		errprintf("数据整理出现异常，subdatasetid:%d,taskid:%d.",subdatasetid ,taskid);
		errprintf("恢复中间文件的处理状态(subdatasetid:%d,%d个文件)：2-->1",subdatasetid,rn);
		sprintf(sqlbf,"update dt.dt_middledatafile set procstate=1 where subdatasetid=%d ",subdatasetid);
		conn.DoQuery(sqlbf);
		errprintf("删除已整理的数据和索引文件.");
		int i;
		errprintf("删除数据文件...");
		for(i=0;i<frn;i++) {
			errprintf("\t %s \t %s",fnmt.PtrStr("filename",i),
			   fnmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("filename",i));
		}
		errprintf("删除索引文件...");
		for(i=0;i<fidxrn;i++) {
			errprintf("\t %s \t %s",fnidxmt.PtrStr("filename",i),
			   fnidxmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<fidxrn;i++) {
			unlink(fnidxmt.PtrStr("filename",i));
		}
		errprintf("删除已处理数据文件和索引文件记录...");
		AutoStmt st(sp->GetDTS());
		st.Prepare("delete from dt_datafilemap where subdatasetid=%d",subdatasetid);
		st.Execute(1);
		st.Wait();
		st.Prepare("delete from dt_indexfilemap where subdatasetid=%d ",subdatasetid);
		st.Execute(1);
		st.Wait();
		throw;
	}

	lgprintf("数据处理(MiddleDataLoading)结束,共处理数据包%d个.",dispct);
	lgprintf("生成%d个数据文件,已插入dt_datafilemap表.",wociGetMemtableRows(fnmt));
	//wociMTPrint(fnmt,0,NULL);
	lgprintf("生成%d个索引文件,已插入dt_indexfilemap表.",wociGetMemtableRows(fnidxmt));
	//wociMTPrint(fnidxmt,0,NULL);
	//检查是否最后一批数据
	try
	{
	//mdf.FetchAll("select * from dt_taskschedule where taskid=%d and taskstatus>=2",
	//	taskid);
	//int rn=mdf.Wait();
	//if(rn>=1) {
		//针对多分区处理的任务，检查其他分区的数据（其他数据子集）是否整理完毕。
		//  如果是单分区处理任务，则以下查询总是返回空记录集。
		mdf.FetchAll("select * from dt_middledatafile where procstate!=3 and taskid=%d",
				taskid);
		int rn=mdf.Wait();
		if(rn==0) {
			lgprintf("最后一批数据已处理完,任务状态2-->3,taskid:%d",taskid);
			//如果是单分区处理任务，必须是所有相同数据集的任务状态为3，才能启动下一步的操作（数据装入）。
			sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=3 where taskid=%d",
				taskid);
			conn.DoQuery(sqlbf);
		}
		//重新创建表结构。
		sp->CreateDT(tabid);
	//}
	lgprintf("删除中间临时文件...");
	mdf.FetchAll("select * from dt_middledatafile where subdatasetid=%d and taskid=%d",subdatasetid,taskid);
	{
	  int dfn=mdf.Wait();
	  for(int di=0;di<dfn;di++) {
		lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
		unlink(mdf.PtrStr("datafilename",di));
		lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
		unlink(mdf.PtrStr("indexfilename",di));
	 } 
	 lgprintf("删除记录...");
  	 AutoStmt st(sp->GetDTS());
	 st.Prepare("delete from dt_middledatafile where subdatasetid=%d and taskid=%d",subdatasetid,taskid);
	 st.Execute(1);
	 st.Wait();
	}
	
	}
	catch(...) {
		errprintf("数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\ntaskid:%d,subdatasetid:%d。",
		taskid,subdatasetid);
		throw;
	}
	return 1;
	//Load index data into memory table (indexmt)
  }

bool SysAdmin::EmptyIndex(const char *databasename,int tabid,MySQLConn &conn)
{
	AutoMt indexmt(dts,100);
	indexmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
	int rn=indexmt.Wait();
	if(rn<1) return false;
	char sqlbf[300];
	for(int i=0;i<rn;i++) {
		lgprintf("清空索引表%s.%s...",databasename,indexmt.PtrStr("indextabname",i));
		sprintf(sqlbf,"truncate table %s.%s",databasename,indexmt.PtrStr("indextabname",i));
		conn.DoQuery(sqlbf);
	}
	return true;
}

int SysAdmin::GetSrcTableStructMt(int srctabp, int tabp, int srcsys)
{
		AutoStmt srcst(srcsys);
		srcst.Prepare("select * from %s.%s",dt_srctable.PtrStr("srcschedulename",srctabp),
			dt_srctable.PtrStr("srctabname",srctabp));
		int mt=wociCreateMemTable();
		wociBuildStmt(mt,srcst,10);
		return mt;
}

bool SysAdmin::CreateTableOnMysql(const char *cdpathval,MySQLConn &conn, int srcmt, const char *tabname, int tabid, int indexid,int targettype,bool forcecreate)
{
		//如果目标表已存在，先删除
	    char sqlbf[3000];
		bool exist=conn.TouchTable(tabname);
		if(exist && !forcecreate) return false;
		if(exist) {
			printf("table %s has exist,dropped.\n",tabname);
			sprintf(sqlbf,"drop table %s",tabname);
			conn.DoQuery(sqlbf);
		}
		//else 
		{
			//建立目标标及其表结构的描述文件
			wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
			conn.DoQuery(sqlbf);
			int fid=NextFileID();
			char fn[300];
			sprintf(fn,"%sdt_%s_%d_%d.cdsc",cdpathval,tabname,tabid,fid);
			FILE *fp=fopen(fn,"w+b");
			if(fp==NULL) ThrowWith("Create file '%s' failed\n",fn);
			void *pcd;
			int cdlen,cdnum;
			wociGetColumnDesc(srcmt,&pcd,cdlen,cdnum);
			fwrite(&cdlen,1,sizeof(int),fp);
			fwrite(&cdnum,1,sizeof(int),fp);
			fwrite(pcd,1,cdlen,fp);
			fclose(fp);
			//修改表结构描述文件的链接
			if(targettype==0)
			sprintf(sqlbf,"update dt.dt_table set cdfileid=%d,recordlen=%d where tabid=%d",
				fid,wociGetRowLen(srcmt),tabid);
			else 
			sprintf(sqlbf,"update dt.dt_index set cdfileid=%d where indexid=%d",
				fid,indexid);
			conn.DoQuery(sqlbf);
			char tmp[300];
			mysql_escape_string(tmp,fn,strlen(fn));
			AutoMt oldval(dts,100);
			oldval.FetchAll("select * from dt_cdfilemap where tabid=%d and targettype=%d and indexid=%d",
				tabid,targettype,indexid);
			int rn=oldval.Wait();
			if(rn>0) {
				AutoMt chkmt(dts,100);
				for(int i=0;i<rn;i++) {
					int oldfid=oldval.GetInt("fileid",i);
					int ckrn=0;
					if(targettype==0) {
					    chkmt.FetchAll("select * from dt_table where cdfileid=%d",
							oldfid);
						ckrn=chkmt.Wait();
					}
					else
					{
					    chkmt.FetchAll("select * from dt_index where cdfileid=%d",
							oldfid);
						ckrn=chkmt.Wait();
					}
					if(ckrn==0) {
						lgprintf("删除原有字段格式记录和文件'%s'",oldval.PtrStr("filename",i));
						unlink(oldval.PtrStr("filename",i));
					 AutoStmt st(dts);
					 st.Prepare("delete from dt_cdfilemap where fileid=%d",oldfid);
					 st.Execute(1);
					 st.Wait();
					}
				}
			}
			sprintf(sqlbf,"insert into dt.dt_cdfilemap values (%d,'%s',0,%d,%d,%d,%d,%d)",
				fid,tmp,targettype,tabid,indexid,sizeof(int)*2+cdlen,cdnum);
			conn.DoQuery(sqlbf);
		}
		return true;
}

//建立表并更新复用字段值(dt_index.reusecols)
bool SysAdmin::CreateIndexTable(MySQLConn &conn,int srcmt,int tabid,int indexid, const char *tabname, const char *solecolsname,const char *pathval, bool forcecreate)
{
		//建立索引表和目标表、索引表的索引
		AutoMt idxtarget(dts,100);
		//每一个独立索引都需要建立索引表
		wociClear(idxtarget);
		wociCopyColumnDefine(idxtarget,srcmt,solecolsname);
		//查找该独立索引附带的非独立索引，并以此为依据建立索引表
		AutoMt idxsubmt(dts,100);
		idxsubmt.FetchAll("select * from dt_index where reuseindexid=%d order by indexidinidxtab",
					indexid);
		int srn=idxsubmt.Wait();
		char colsname[300];
		for(int j=0;j<srn;j++) {
			strcpy(colsname,idxsubmt.PtrStr("columnsname",j));
			//重复的字段自动剔除
			wociCopyColumnDefine(idxtarget,srcmt,colsname);
		}
		//重构索引复用段
		char reusedcols[300];
		int pcol[20];
		int cn1;
		cn1=wociConvertColStrToInt(srcmt,solecolsname,pcol);
		reusedcols[0]=0;
		int tcn=wociGetMtColumnsNum(idxtarget);
		if(tcn>cn1) {
			for(int i=cn1;i<tcn;i++) {
				if(i!=cn1) strcat(reusedcols,",");
				wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
			}
		}
		{
			AutoStmt st(dts);
			if(strlen(reusedcols)>0)
			 st.Prepare("update dt_index set reusecols='%s' where indexid=%d",
				reusedcols,indexid);
			else
			 st.Prepare("update dt_index set reusecols=null where indexid=%d",
				indexid);
			st.Execute(1);
			st.Wait();
		}
		//索引表公共字段
		wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"rownum",NULL,COLUMN_TYPE_INT,10,0);
		//如果索引表已存在，先删除
		char sqlbf[3000];
		bool exist=conn.TouchTable(tabname);
		if(exist && !forcecreate) return false;
		if( exist ) {
			printf("Index table '%s' has exist,dropped!\n",tabname);
			sprintf(sqlbf,"drop table %s",tabname);
			conn.DoQuery(sqlbf);
		}
		CreateTableOnMysql(pathval,conn,idxtarget,tabname,tabid,indexid,1,true);
		{
				char emptytabname[300];
				sprintf(emptytabname,"template.pri_%s",tabname);
				if(conn.TouchTable(emptytabname)) {
					printf("Empty Temp table '%s' has exist,skip create.\n",emptytabname);
					sprintf(sqlbf,"drop table %s",emptytabname);
					conn.DoQuery(sqlbf);
				}
				//else
				{
					wociGetCreateTableSQL(idxtarget,sqlbf,emptytabname,true);
					conn.DoQuery(sqlbf);
					printf(sqlbf);
					printf("\n");
				}
		}
		return true;
}

//createtype: 0--- datatable and indextable 1--datatable 2--indextable
bool SysAdmin::CreateIndex(char *dttabname, char *idxtabname, int createtype, MySQLConn &conn, int tabid, int indexid, bool forcecreate)
{
			AutoMt mt(dts,10);
			mt.FetchAll("select * from dt_index where indexid=%d and issoledindex=1",indexid);
			int rn=mt.Wait();
			if(rn<1)
				ThrowWith("Indexid is invalid or not a soled :%d",indexid);

			char colsname[1000];
			//char idxname[300];
			strcpy(colsname,mt.PtrStr("columnsname",0));
			if(createtype==0 || createtype==2) {
			  CreateIndex(idxtabname,conn,mt.GetInt("indexidinidxtab",0),colsname,forcecreate);
			}
			if(createtype==0 || createtype==1) {
				CreateIndex(dttabname,conn,mt.GetInt("indexidindattab",0),colsname,forcecreate);
			}
			
			//查找该独立索引附带的非独立索引，并以此为依据建立索引表
			AutoMt idxsubmt(dts,100);
			idxsubmt.FetchAll("select * from dt_index where reuseindexid=%d order by indexidinidxtab",
						indexid);
			int srn=idxsubmt.Wait();
			for(int j=0;j<srn;j++) {
				strcpy(colsname,idxsubmt.PtrStr("columnsname",j));
				CreateIndex(idxtabname,conn,idxsubmt.GetInt("indexidinidxtab",j),colsname,forcecreate);
				CreateIndex(dttabname,conn,idxsubmt.GetInt("indexidindattab",j),colsname,forcecreate);
			}
			return true;
}

void SysAdmin::CreateIndex(char *dtname, MySQLConn &conn, int id, char *colsname, bool forcecreate)
{
	char sqlbf[3000];	
	if(forcecreate) {
		sprintf(sqlbf,"drop index %s_%d on %s",dtname,
			id,dtname);
		conn.DoQuery(sqlbf);
	}
	sprintf(sqlbf,"create index %s_%d on %s(%s)",
		dtname,id,
		dtname,colsname);
	conn.DoQuery(sqlbf);
}

#define revint(v) ((v>>24)|(v<<24)|(v>>8&0xff00)|(v<<8&0xff0000))

int DestLoader::Load(MySQLConn &conn) {
	//Check deserved temporary(middle) fileset
	wociSetTraceFile("DLoad");

	AutoMt mdf(psa->GetDTS(),10);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus=3 and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("没有发现处理完成等待装入的数据(任务状态=3).\n");
		return 0;
	}
	char sqlbf[1000];
	srctabid=mdf.GetInt("srctabid",0);
	taskid=mdf.GetInt("taskid",0);
	datasetid=mdf.GetInt("datasetid",0);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus!=3 and datasetid=%d and rownum<2",datasetid);
	rn=mdf.Wait();
	if(rn>0) {
		printf("数据装入时，数据集%d包含的任务中，一些任务状态不是'已整理'(3),例如任务号%d(状态%d).\n",datasetid,
		  mdf.GetInt("taskid",0),mdf.GetInt("taskstatus",0));
		return 0;
	}
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus=0 and rownum<2",datasetid);
	if(mdf.Wait()<1) 
		ThrowWith("致命错误，任务%d指示已完成数据整理，但找不到对应数据集%d的数据记录。\n可能是数据文件记录不存在或状态非空闲(0).\n",taskid,datasetid);
	indexid=mdf.GetInt("indexid",0);
	//mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and indexid=%d ",datasetid,indexid);
	subdatasetid=mdf.GetInt("subdatasetid",0);
	tabid=mdf.GetInt("tabid",0);
	//partid=mdf.GetInt("partid",0);
	dumpparam dpsrc;
	psa->GetSoledInexParam(srctabid,&dpsrc);
	psa->GetSoledInexParam(srctabid,&dp,tabid);
	
	AutoMt idxmt(psa->GetDTS(),10);
	idxmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
	int idxrn=idxmt.Wait();
	for(int i=0;i<idxrn;i++) {
		indexid=idxmt.GetInt("indexid",i);
		mdf.FetchAll("select * from dt_indexfilemap where datasetid=%d and indexid=%d order by subdatasetid,fileid",
		 datasetid,indexid);
		rn=mdf.Wait();
		AutoMt datmt(psa->GetDTS(),100);
		datmt.FetchAll("select * from dt_datafilemap where datasetid=%d and tabid=%d and indexid=%d and procstatus=0 order by subdatasetid,fileid",datasetid,tabid,
			indexid);
		int datrn=datmt.Wait();
		//防止功能重入或并行运行时的重复执行
		if(datrn<1) continue;
		int off=dp.GetOffset(indexid);
		char fn[300];
		const char *pathval=psa->GetPathName(0,"msys");		
		try {
			//为防止功能重入,数据文件状态修改.
		  lgprintf("修改数据文件的处理状态(datasetid:%d,indexid:%d,%d个文件)：0-->1",datasetid,indexid,datrn);
		  sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=1 where datasetid=%d and procstatus=0 and indexid=%d",
			        datasetid,indexid);
		  if(conn.DoQuery(sqlbf)!=rn) 
				ThrowWith("修改数据文件的处理状态异常，可能是与其它进程冲突。\n"
					"   taskid:%d,tabid:%d,datasetid:%d,srctabid:%d,indexid:%d.\n",
					taskid,tabid,datasetid,srctabid,indexid);
		  for(int k=0;k<datrn;k++) {
			//Build table data file link information.
			if(k+1==datrn) {
				dt_file df;
				df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
				df.SetFileHeader(0,NULL);
			}
			else {
				dt_file df;
				df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
				df.SetFileHeader(0,datmt.PtrStr("filename",k+1));
			}
		  }
	    	  lgprintf("开始数据装入(DestLoading),文件数:%d,tabid:%d,datasetid:%d,taskid:%d ,indexid:%d ...",
		  	rn,tabid,datasetid,taskid,indexid);
#ifdef WIN32
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
#else
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
#endif		
		  //struct _finddata_t ft;
		  FILE *fp =NULL;
		  fp=fopen(fn,"a+b");
		  if(fp==NULL) 
			ThrowWith("Open file %s for writing failed!",fn);
		  int totidxrn=0;
		  //lgprintf("索引文件：%s",mdf.PtrStr("filename",0));
		  for(int i=0;i<rn;i++) {
			//Build index data file link information.
			if(i+1==rn) {
				dt_file df;
				df.Open(mdf.PtrStr("filename",i),2,mdf.GetInt("fileid",i));
				df.SetFileHeader(0,NULL);
				//lgprintf(
			}
			else {
				dt_file df;
				df.Open(mdf.PtrStr("filename",i),2,mdf.GetInt("fileid",i));
				df.SetFileHeader(0,mdf.PtrStr("filename",i+1));
			}
			file_mt idxf;
			idxf.Open(mdf.PtrStr("filename",i),0);
			int mt=idxf.ReadBlock(0,0);
			while(mt) {
				wociCopyToMySQL(mt,0,0,fp);
				totidxrn+=wociGetMemtableRows(mt);
				mt=idxf.ReadBlock(-1,0);
			}
		  }
		  fclose(fp);
		  fp=fopen(fn,"rb");
		  fseek(fp,0,SEEK_END);
		  long fsz=ftell(fp);//_filelength(_fileno(fp));
		  fclose(fp);
		  char srcfn[300];
#ifdef WIN32
		  sprintf(srcfn,"%stemplate\\pri_%s.frm",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(!CopyFile(srcfn,fn,FALSE))
#else
		  char cmdl[300];
		  sprintf(srcfn,"%stemplate/pri_%s.frm",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx/pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(uCopyFile(srcfn,fn)!=1)
#endif
		  ThrowWith("Copy file '%s' to '%s' failed!\n",srcfn,fn);

#ifdef WIN32
		  sprintf(srcfn,"%stemplate\\pri_%s.MYI",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(!CopyFile(srcfn,fn,FALSE))
#else
		  sprintf(srcfn,"%stemplate/pri_%s.MYI",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(uCopyFile(srcfn,fn)!=1)
#endif
			 ThrowWith("Copy file '%s' to '%s' failed!\n",srcfn,fn);
		  totidxrn=revint(totidxrn);
		  fsz=revint(fsz);
		  int bk=0;
		  fp=fopen(fn,"r+b");
		  fseek(fp,28,SEEK_SET);
		  fwrite(&bk,1,4,fp);
		  fwrite(&totidxrn,1,4,fp);
		  fseek(fp,68,SEEK_SET);
		  fwrite(&bk,1,4,fp);
		  fwrite(&fsz,1,4,fp);
		  fseek(fp,0,SEEK_END);
		  fclose(fp); 
		}
		catch(...) {
		  lgprintf("恢复数据文件的处理状态(datasetid:%d,indexid:%d,%d个文件)：1-->0",datasetid,indexid,datrn);
		  sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=0 where datasetid=%d and indexid=%d",
			        datasetid,indexid);
		  conn.DoQuery(sqlbf);
#ifdef WIN32
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
#else
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx/pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
#endif
                  throw;
		}
	}
	lgprintf("数据装入(DestLoading)结束 ...");
	AutoStmt updst(psa->GetDTS());
	updst.Prepare("update dt_taskschedule set taskstatus=4 where datasetid=%d",
		datasetid);
	updst.Execute(1);
	updst.Wait();
	lgprintf("任务状态更新,3(MLoaded)--->4(DLoaded),任务号:%d,数据集编号:%d",taskid,datasetid);
	return 1;
}


int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname,MySQLConn &conn)
{
	lgprintf("Move table '%s -> '%s'.",srctabname,dsttabname);
	AutoMt mt(psa->GetDTS(),100);
	mt.FetchAll("select pathval from dt_path where pathtype='msys'");
	int rn=mt.Wait();
	if(rn<1) 
		ThrowWith("找不到MySQL数据目录(dt_path.pathtype='msys'),数据转移异常中止.");
	char dtpath[300];
	strcpy(dtpath,mt.PtrStr(0,0));

	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
	int ntabid=-1;
	rn=mt.Wait();
	if(rn>0) {
		//if(mt.GetInt("srctabid",0)>0) {
		//	lgprintf("表名称已被占用，并且不能被覆盖（迁移目标表，srctabid>0）。");
		//	return 0;
		//}
		printf("表'%s'已存在(记录数:%d)，删除(Y/N)?",dsttabname,mt.GetInt("recordnum",0));
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("取消数据转移。 ");
			return 0;
		}
		RemoveTable(dstdbn,dsttabname,conn,false);
		if(mt.GetInt("srctabid",0)>0) 
			ntabid=mt.GetInt("tabid",0);
	}
	int dsttabid=ntabid;
	if(ntabid==-1) {
		mt.FetchAll("select max(tabid) from dt_table");
		mt.Wait();
		dsttabid=*mt.PtrInt(0,0)+1;
	}
	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	if(rn<1) {
		lgprintf("源表'%s'不存在.",srctabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	datasetid=mt.GetInt("datasetid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	//if(recordnum<1) {
	//	lgprintf("源表'%s'数据为空，数据转移失败。",srctabname);
	//	return 0;
	//}
	lgprintf("源表'%s.%s' id:%d,datasetid:%d,recordnum:%d,first data file id :%d",
		srcdbn,srctabname,tabid,datasetid,recordnum,firstdatafileid);
	int idxstart=0;
	//新表不存在，则在dt_table中新建一条记录
	if(ntabid==-1) {
		*mt.PtrInt(0,0)=dsttabid;
		strcat(mt.PtrStr("tabdesc",0),"_r");
		strcpy(mt.PtrStr("tabname",0),dsttabname);
		strcpy(mt.PtrStr("databasename",0),dstdbn);
		if(mt.GetInt("srctabid",0)>0) 
			*mt.PtrInt("srctabid",0)=-*mt.PtrInt("srctabid",0);
		wociAppendToDbTable(mt,"dt_table",psa->GetDTS());
		mt.FetchAll("select max(indexid) from dt_index");
		mt.Wait();
		idxstart=*mt.PtrInt(0,0)+1;
		CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
	}
	//暂时关闭源表和目标表的数据访问，记录数已存在本地变量recordnum。
	char sqlbuf[1000];
	lgprintf("关闭源表和目标表，记录数：%d",recordnum);
	sprintf(sqlbuf,"update dt.dt_table set recordnum=0 where tabid in (%d,%d)",
		tabid,dsttabid);
	conn.DoQuery(sqlbuf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables ");
	//如果是目标表覆盖模式，目标表必须和源表的结构（包括数据和索引）必须一致，否则
	//  后果将不可预料。
	mt.FetchAll("select * from dt_index where tabid=%d order by indexid ",tabid);
	rn=mt.Wait();
	AutoMt idxdmt(psa->GetDTS(),100);
	if(ntabid!=-1) {
		idxdmt.FetchAll("select * from dt_index where tabid=%d order by indexid",ntabid);
		if(idxdmt.Wait()!=rn) 
			ThrowWith("目标表的索引数与源表不同，无法执行数据转移操作!");
	}
	//创建索引记录和索引表，修改索引文件和数据文件的tabid,indexid 指向
	for(int i=0;i<rn;i++) {
		int oldidxid=*mt.PtrInt(0,i);
		if(ntabid==-1) {
			*mt.PtrInt(0,i)=idxstart;// indexid
			*mt.PtrInt(1,i)=dsttabid; // tabid
			lgprintf("创建索引，编号 :%d",idxstart);
		}
		else
			idxstart=*idxdmt.PtrInt(0,i);
		if(mt.GetInt("issoledindex",i)==1) {
			char tmpfn[300];
			strcpy(tmpfn,mt.PtrStr("indextabname",i));
			const char *pdfn;
			if(ntabid==-1) {
			 sprintf(mt.PtrStr("indextabname",i),"%sidx%d",dsttabname,idxstart);
			 pdfn=mt.PtrStr("indextabname",i);
			}
			else pdfn=idxdmt.PtrStr("indextabname",i);
			lgprintf("索引表 '%s.%s'-->'%s.%s...'",srcdbn,tmpfn,dstdbn,pdfn);
			CopyMySQLTable(dtpath,srcdbn,tmpfn,dstdbn,pdfn);
			sprintf(sqlbuf,"update dt.dt_datafilemap set tabid=%d,indexid=%d where tabid=%d and indexid=%d and datasetid=%d",
				dsttabid,idxstart,tabid,oldidxid,datasetid);
			int un=conn.DoQuery(sqlbuf);
			if(un<1 && recordnum>0) 
				ThrowWith("找不到数据文件记录，表编号%d,索引号%d,数据集号:%d,数据移动异常终止！",
					tabid,oldidxid,datasetid);
			sprintf(sqlbuf,"update dt.dt_indexfilemap set tabid=%d,indexid=%d where tabid=%d and indexid=%d and datasetid=%d",
				dsttabid,idxstart,tabid,oldidxid,datasetid);
			if(un<1 && recordnum>0) 
				ThrowWith("找不到索引文件记录，表编号%d,索引号%d,数据集号:%d,数据移动异常终止！",
					tabid,oldidxid,datasetid);
			un=conn.DoQuery(sqlbuf);
			if(ntabid==-1) {
				for(int j=0;j<rn;j++) {
					if(mt.GetInt("reuseindexid",j)==oldidxid) {
						*mt.PtrInt("reuseindexid",j)=idxstart;
						strcpy(mt.PtrStr("indextabname",j),mt.PtrStr("indextabname",i));
						lgprintf("索引编号%d复用编号%d.",mt.GetInt("indexid",j),idxstart);
					}
				}
			}
		}
		idxstart++;
	}
	if(ntabid==-1) 
		wociAppendToDbTable(mt,"dt_index",psa->GetDTS());// waitting not needed.
		
	sprintf(sqlbuf,"update dt.dt_table set datasetid=%d,recordnum=%d,firstdatafileid=%d,totalbytes=%15.0f,datafilenum=%d where tabid=%d",
		datasetid,recordnum,firstdatafileid,totalbytes,datafilenum,dsttabid);
	conn.DoQuery(sqlbuf);
	// reset firstdatafileid of srctable?
	//sprintf(sqlbuf,"update dt.dt_table set datasetid=0,firstdatafileid=0 where tabid=%d",
	//	tabid);
	//conn.DoQuery(sqlbuf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables ");
	lgprintf("删除源表..");
	RemoveTable(srcdbn,srctabname,conn,false);
	lgprintf("数据已从表'%s'转移到'%s'。",srctabname,dsttabname);
	return 1;
}

int DestLoader::ReLoad(MySQLConn &conn) {
	wociSetTraceFile("ReLoad");

	AutoMt mdf(psa->GetDTS(),10);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus in (7,10) and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("没有发现重新压缩完成等待装入的数据(Recompressed,taskstatus in(7,10).\n");
		return 0;
	}
	bool dpcp=mdf.GetInt("taskstatus",0)==7;
	datasetid=mdf.GetInt("datasetid",0);
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d order by indexid",datasetid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("装入二次压缩数据时找不到数据文件记录。");
		return 1;
	}
	tabid=mdf.GetInt("tabid",0);
	AutoMt idxmt(psa->GetDTS(),300);
	mdf.FetchAll("select * from dt_indexfilemap where datasetid=%d and tabid=%d order by subdatasetid,fileid",
	  datasetid,tabid);
	rn=mdf.Wait();
	AutoMt datmt(psa->GetDTS(),300);
	datmt.FetchAll("select * from dt_datafilemap where datasetid=%d and tabid=%d order by subdatasetid,fileid",datasetid,tabid);
	int datrn=datmt.Wait();
	AutoStmt updst(psa->GetDTS());
	char tmpfn[300];
	int k;
	unsigned long dtflen[1000];
	unsigned long idxflen[1000];
	//先检查
	for(k=0;k<datrn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		dtflen[k]=df.GetFileSize();
		if(dtflen[k]<1) 
			ThrowWith("file '%s' empty!",tmpfn);
	}
	for(k=0;k<rn;k++) {
		sprintf(tmpfn,"%s.%s",mdf.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		idxflen[k]=df.GetFileSize();
		if(idxflen[k]<1) 
			ThrowWith("file '%s' empty!",tmpfn);
	}
	char sqlbf[200];
	sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=%d where datasetid=%d", dpcp?8:11,datasetid);
	if(conn.DoQuery(sqlbf)<1) 
		ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突。\n"
			"  tabid:%d,datasetid:%d.\n",
			tabid,datasetid);
		
	//防止功能重入，修改任务状态
	//操作数据前先关闭表
	sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("数据已关闭.");
	//用新的数据文件替换原来的文件：先删除原文件，新文件名称更改为原文件并修改文件记录中的文件大小字段。
	lgprintf("开始数据和索引文件替换...");
	for(k=0;k<datrn;k++) {
		updst.Prepare("update dt_datafilemap set filesize=%d,compflag=%d where fileid=%d",
			dtflen[k],dpcp?10:5,datmt.GetInt("fileid",k));
		updst.Execute(1);
		updst.Wait();
		const char *filename=datmt.PtrStr("filename",k);
		unlink(filename);
		//sprintf(tmpfn,"%s.%s",filename,dpcp?"dep5":"depcp");
		//rename(filename,tmpfn);
		//lgprintf("rename file '%s' as '%s'",filename,tmpfn);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	for(int i=0;i<rn;i++) {
		updst.Prepare("update dt_indexfilemap set filesize=%d ,compflag=%d where fileid=%d",
			idxflen[i],dpcp?10:5,mdf.GetInt("fileid",i));
		updst.Execute(1);
		updst.Wait();
		const char *filename=mdf.PtrStr("filename",i);
		unlink(filename);
		//sprintf(tmpfn,"%s.%s",filename,dpcp?"dep5":"depcp");
		//rename(filename,tmpfn);
		//lgprintf("rename file '%s' as '%s'",filename,tmpfn);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	lgprintf("数据和索引文件已成功替换...");
	sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=3 where datasetid=%d", datasetid);
	conn.DoQuery(sqlbf);
	sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=0 where datasetid=%d",datasetid);
	lgprintf("任务状态修改为数据整理结束(3),数据文件处理状态改为未处理(0).");
	Load(conn);
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa,MySQLConn &conn) 
{
	AutoMt mdf(psa->GetDTS(),200);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus in(4,8,11) and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	taskid=mdf.GetInt("taskid",0);
	srctabid=mdf.GetInt("srctabid",0);
	datasetid=mdf.GetInt("datasetid",0);
	//如果目标表经过转移操作，则从任务号到目标表编号的对应只能从数据集映射。
	mdf.FetchAll("select distinct tabid from dt_table where datasetid=%d",datasetid);
	rn=mdf.Wait();
	if(rn<1) {
		mdf.FetchAll("select distinct tabid from dt_srctable where srctabid=%d",srctabid);
		rn=mdf.Wait();
		if(rn<1) {
			lgprintf("找不到任务号:%d对应的Tabid",taskid);
			return 0;
		}
	}
	tabid=mdf.GetInt("tabid",0);
	
	//check intergrity.
	int newdsid=datasetid;
	mdf.FetchAll("select * from dt_index where tabid=%d and issoledpindex=1",tabid);
	rn=mdf.Wait();
	if(rn<1) 
		ThrowWith("目标表%d缺少主索引记录.",tabid);
	int pindexid=mdf.GetInt("indexid",0);
	mdf.FetchAll("select sum(filesize) tsize,count(*) fnum from dt_datafilemap where datasetid=%d and indexid=%d",
		datasetid,pindexid);
	mdf.Wait();
	double totalbytes=mdf.GetDouble("tsize",0);
	int datafilenum=mdf.GetInt("fnum",0);
	
	mdf.FetchAll("select count(*) rn from dt_index where tabid=%d and issoledindex=1",tabid);
	rn=mdf.Wait();
	mdf.FetchAll("select distinct indexid from dt_datafilemap where datasetid=%d",datasetid);
	if(mdf.Wait()!=rn) 
	{
		lgprintf("出现错误: 装入独立索引数(%d)和索引参数表中的值(%d)不符.",
			rn,mdf.GetInt("rn",0));
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	
	// Get new & old datasetid
	mdf.FetchAll("select datasetid,databasename from dt_table where tabid=%d",tabid);
	mdf.Wait();
	char dbname[100];
	strcpy(dbname,mdf.PtrStr("databasename",0));
	conn.SelectDB(dbname);
	int olddsid=mdf.GetInt("datasetid",0);
	char sqlbf[3000];
	mdf.FetchAll("select * from dt_index where tabid=%d order by indexidindattab",tabid);
	rn=mdf.Wait();
	//int psoledid=-1;
	//先关闭目标表
	sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("数据已关闭.");
	
	lgprintf("开始索引重建,tabid:%d,独立索引数 :%d,datasetid=%d",
		tabid,rn,datasetid);
	int i;
	for(i=0;i<rn;i++) {
		int soled=mdf.GetInt("issoledindex",i);
		if(soled) {
		// 目标表备份--〉源表转移到目标表。
			int indexidinidxtab=mdf.GetInt("indexidinidxtab",i);
			char *tbname=mdf.PtrStr("indextabname",i);
			
			char *colsname=mdf.PtrStr("columnsname",i);
			indexid=mdf.GetInt("indexid",i);
			
			//if(mdf.GetInt("issoledpindex",i)==1) psoledid=indexid;
			
			//sprintf(sqlbf,"bakidx.%s_%d",tbname,olddsid);
			
			//bool bakex=conn.TouchTable(sqlbf);
			sprintf(sqlbf,"tmpidx.pri_%s_%d",tbname,datasetid);
			if(!conn.TouchTable(sqlbf)) 
				ThrowWith("没有找到数据装入时生成的索引表:%s,需要手工对数据装入（调整任务状态为3整理完毕).",
				 sqlbf);
			
			sprintf(sqlbf,"drop table %s.%s",dbname,tbname);
			conn.DoQuery(sqlbf);
			
			//bool dstex=conn.TouchTable(sqlbf);
			
			
			//if(bakex && dstex &&srcex) {
			//	sprintf(sqlbf,"drop table bakidx.%s_%d",tbname,olddsid);
			//	conn.DoQuery(sqlbf);
			//	lgprintf("备份索引表已删除.");
			//}
			//if(dstex && srcex) {
			//	sprintf(sqlbf,"alter table %s.%s rename bakidx.%s_%d",
			//		dbname,tbname,tbname,olddsid);
			//	conn.DoQuery(sqlbf);
			//	lgprintf("备份索引表已建立.");
			//}
			//源表不存在，有可能是上次非正常运行，已有部分索引表成功建立。
			//if(srcex) {
				sprintf(sqlbf,"alter table tmpidx.pri_%s_%d rename %s.%s",
					tbname,datasetid,dbname,tbname);
				conn.DoQuery(sqlbf);
				lgprintf("在线索引表已建立.");
			//}
			//if(dstex) {
				_Psa->CreateIndex(tbname,conn,indexidinidxtab,colsname,true);
				lgprintf("创建索引:%s.%s",tbname,colsname);
				for(int j=0;j<rn;j++) {
					if(mdf.GetInt("reuseindexid",j)==indexid) {
						_Psa->CreateIndex(tbname,conn,mdf.GetInt("indexidinidxtab",j),
							mdf.PtrStr("columnsname",j),true);
						lgprintf("创建索引:%s.%s",tbname,mdf.PtrStr("columnsname",j));
					}
	                                //else {					
					//	printf("reuseindexid:%d!=%d",mdf.GetInt("reuseindexid",j),indexid) ;
					//}

				}
			//}
		}
	}
	//if(psoledid==-1) 
	//	ThrowWith("No primary soled index define in dt_index on tableid: %d",tabid);
	lgprintf("索引建立完成.");
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and isfirstindex=1 order by subdatasetid,fileid",
		datasetid);
	rn=mdf.Wait();
		
	int sumrn=0;
	for(i=0;i<rn;i++) sumrn+=mdf.GetInt("recordnum",i);
	lgprintf("数据源准备完毕,记录数:%d.",sumrn);
	
	sprintf(sqlbf,"update dt.dt_table set datasetid=%d,recordnum=%d,firstdatafileid=%d ,totalbytes=%15.0f ,datafilenum=%d where tabid=%d",
		datasetid,sumrn,mdf.GetInt("fileid",0),totalbytes,datafilenum,tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("数据已成功上线.");
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_taskschedule set taskstatus=5 where datasetid=%d",
		datasetid);
	st.Execute(1);
	st.Wait();
	st.Prepare("update dt_datafilemap set procstatus=0 where datasetid=%d",
		datasetid);
	st.Execute(1);
	st.Wait();
	
	lgprintf("删除中间临时文件...");
	mdf.FetchAll("select * from dt_middledatafile where datasetid=%d",datasetid);
	{
		int dfn=mdf.Wait();
		for(int di=0;di<dfn;di++) {
			lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
			unlink(mdf.PtrStr("datafilename",di));
			lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
			unlink(mdf.PtrStr("indexfilename",di));
		}
		lgprintf("删除记录...");
		st.Prepare("delete from dt_middledatafile where datasetid=%d",datasetid);
		st.Execute(1);
		st.Wait();
	} 
	
	lgprintf("任务状态4(DestLoaded)-->5(Complete),taskid:%d.",taskid);
	return 1;
}

thread_rt LaunchWork(void *ptr) 
{
	((worker *) ptr)->work();
	thread_end;
}


int DestLoader::ReCompress(int threadnum)
{
	wociSetTraceFile("DeepCompress");
	AutoMt mdt(psa->GetDTS(),200);
	AutoMt mdf(psa->GetDTS(),200);
	mdt.FetchAll("select * from dt_taskschedule where (taskstatus=6 or taskstatus=9) and rownum<2");
	int rn1=mdt.Wait();
	int rn;
	int i=0;
	bool deepcmp;
	if(rn1<1) {
		return 0;
	}
	for(i=0;i<rn1;i++) {
		taskid=mdt.GetInt("taskid",i);
		srctabid=mdt.GetInt("srctabid",i);
		datasetid=mdt.GetInt("datasetid",i);
		deepcmp=mdt.GetInt("taskstatus",i)==6;
		mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus =0 order by subdatasetid,fileid",datasetid);
		rn=mdf.Wait();
		if(rn<1) {
	        	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus <>2 order by subdatasetid,fileid",datasetid);
	        	rn=mdf.Wait();
	        	if(rn<1) {
				AutoStmt st1(psa->GetDTS());
				st1.Prepare("update dt_taskschedule set taskstatus=%d where datasetid=%d",
					deepcmp?7:10,datasetid);
				st1.Execute(1);
				st1.Wait();
				st1.Prepare("update dt_datafilemap set procstatus =0 where datasetid=%d",datasetid);
				st1.Execute(1);
				st1.Wait();
				lgprintf("二次压缩任务已完成，任务状态已修改为%d,数据文件处理状态修改为空闲(0)",deepcmp?7:10);
			}
			else lgprintf("二次压缩任务未完成,但已没有等待压缩的数据");
		}
		else break;
	}
	if(i==rn1) return 0;
	//防止重入，修改数据文件状态。
	int fid=mdf.GetInt("fileid",0);
	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
	tabid=mdf.GetInt("tabid",0);

	//dstf.SetFileHeader(0,srcf.GetNextFileName());
	//mdf.SetMaxRows(10);
	mdf.FetchAll("select filename from dt_indexfilemap where fileid=%d",
		fid);
	mdf.Wait();
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("filename",0),deepcmp?"depcp":"dep5");
	double dstfilelen=0;
	try {
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_datafilemap set procstatus=1 where fileid=%d and procstatus=0",
		fid);
	st.Execute(1);
	st.Wait();
	if(wociGetFetchedRows(st)!=1) {
		lgprintf("处理文件压缩时状态异常,taskid:%d,fid:%d,可能与其它进程冲突！"
				,taskid,fid);
		return 1;
	}
	file_mt idxf;
	lgprintf("数据处理，数据文件:'%s',索引文件:'%s'.",srcfn,mdf.PtrStr("filename",0));
	idxf.Open(mdf.PtrStr("filename",0),0);
	if(idxf.ReadBlock(-1,0)<0)
		ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));

	dt_file srcf;
	srcf.Open(srcfn,0,fid);
	dt_file dstf;
	dstf.Open(dstfn,1,fid);
	mdf.SetHandle(srcf.CreateMt());
	int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());

	AutoMt *pidxmt=(AutoMt *)idxf;
	lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
	int *pblockstart=pidxmt->PtrInt("blockstart",0);
	int *pblocksize=pidxmt->PtrInt("blocksize",0);
	dt_file idxdstf;
	idxdstf.Open(idxdstfn,1,fid);
	mdf.SetHandle(idxf.CreateMt());
	idxdstf.WriteHeader(mdf,wociGetMemtableRows(*pidxmt),fid,idxf.GetNextFileName());
	//idxdstf.SetFileHeader(wociGetMemtableRows(*pidxmt),idxf.GetNextFileName());
	blockcompress bc(deepcmp?10:5);
	for(i=1;i<threadnum;i++) {
		bc.AddWorker(new blockcompress(deepcmp?10:5));
	}
	char *srcbf=new char[10000000];//每一次处理的最大数据块（解压缩后）。
	char *dstbf=new char[10000000];//可累积的最多数据(压缩后).
	int dstseplen=1000000;
	bool isfilled[10];
	int filledlen[10];
	int filledworkid[10];
	char *outcache[10];
	for(i=0;i<10;i++) {
		isfilled[i]=false;
		filledworkid[i]=0;
		outcache[i]=dstbf+i*1000000;
		filledlen[i]=0;
	}
	int workid=0;
	int nextid=0;
	int oldblockstart=pblockstart[0];
	int lastrow=0;
	bool iseof=false;
	bool isalldone=false;
	int lastdsp=0;
	while(!isalldone) {//文件处理完退出
		if(srcf.ReadMt(-1,0,mdf,1,1,srcbf)<0) {
			iseof=true;
		}
		block_hdr *pbh=(block_hdr *)srcbf;
		while(true) { //任务交付后退出
			worker *pbc=NULL;
			if(!iseof) {
				pbc=bc.GetIdleWorker();
				if(pbc) {
					pbc->Do(workid++,srcbf,pbh->origlen+sizeof(block_hdr),
						pbh->origlen/2); //Unlock internal
			 		break;
				}
			}
			pbc=bc.GetDoneWorker();
			if(pbc) {
				char *pout;
				int dstlen=pbc->GetOutput(&pout);//Unlock internal;
				int doneid=pbc->GetWorkID();
				if(dstlen>dstseplen) 
					ThrowWith("Compress destionation buffer exceed %d",1000000);
				//get empty buf:
				for(i=0;i<10;i++) if(!isfilled[i]) break;
				if(i==10) ThrowWith("Write cache buffer fulled!.");
				memcpy(outcache[i],pout,dstlen);
				filledworkid[i]=doneid;
				filledlen[i]=dstlen;
				isfilled[i]=true;
				//lgprintf("Fill to cache %d,doneid:%d,len:%d",i,doneid,dstlen);
			}
			
			for(i=0;i<10;i++) {
				if(isfilled[i] && filledworkid[i]==nextid) 
					break;
			}
			if(i<10) {
				for(;pblockstart[lastrow]==oldblockstart;) {
					pblockstart[lastrow]=lastoffset;
					pblocksize[lastrow++]=filledlen[i];
				}
				lastoffset=dstf.WriteBlock(outcache[i],filledlen[i],0,true);
				oldblockstart=pblockstart[lastrow];
				dstfilelen+=filledlen[i];
				filledworkid[i]=0;
				filledlen[i]=0;
				isfilled[i]=false;
				nextid++;
				if(nextid-lastdsp>=200) {
				 printf("write from cache %d,nextid %d\n",i,nextid);
				 lastdsp=nextid;
				}
			}
			else if(iseof) {
				if(bc.isidleall()) {
					isalldone=true;
					break;
				}
			}
			if(!pbc) {
#ifdef WIN32
				Sleep(200);
#else
				usleep(200);
#endif
			}
		}
	}
	idxdstf.WriteMt(*pidxmt,COMPRESSLEVEL);
	dstf.Close();
	idxdstf.Close();
	st.Prepare("update dt_datafilemap set procstatus=2 where fileid=%d and procstatus=1",
		fid);
	st.Execute(1);
	st.Wait();
	}
	catch(...) {
	errprintf("数据二次压缩出现异常，文件处理状态恢复...");
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_datafilemap set procstatus=0 where fileid=%d and procstatus=1",
		fid);
	st.Execute(1);
	st.Wait();
	errprintf("删除数据文件和索引文件");
	unlink(dstfn);
	unlink(idxdstfn);
	throw;
	}
	
	lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%f.",dstfn,dstfilelen);
	return 1;
}

int DestLoader::RemoveTable(const char *dbn, const char *tabname, MySQLConn &conn,bool prompt)
{
	lgprintf("remove table '%s ...",tabname);
	AutoMt mt(psa->GetDTS(),100);
	AutoStmt st(psa->GetDTS());
	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		printf("表'%s'不存在!",tabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	datasetid=mt.GetInt("datasetid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	int srctabid=mt.GetInt("srctabid",0);
	if(recordnum<1 && prompt) {
		lgprintf("源表'%s'数据为空，继续？(Y/N)",tabname);
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("取消删除。 ");
			return 0;
		}
	}
	if(prompt)
	{
		printf("表 '%s.%s': %s将被删除，记录数:%d,继续?(Y/N)?",
			dbn,tabname,mt.PtrStr("tabdesc",0),recordnum);
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("取消删除。 ");
			return 0;
		}
	}
	char sqlbuf[1000];
	lgprintf("关闭源表，记录数：%d",recordnum);
	sprintf(sqlbuf,"update dt.dt_table set recordnum=0,firstdatafileid=0,datasetid=0,totalbytes=0 where tabid=%d",
		tabid);
	conn.DoQuery(sqlbuf);
	lgprintf("MySQL刷新...");
	conn.FlushTables();//.DoQuery("flush tables ");
	mt.FetchAll("select * from dt_datafilemap where tabid=%d and datasetid=%d",
		tabid,datasetid);
	rn=mt.Wait();
	int i=0;
	for(i=0;i<rn;i++) {
		  lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
		  unlink(mt.PtrStr("filename",i));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		  unlink(tmp);
	  }
	st.Prepare(" delete from dt_datafilemap where tabid=%d and datasetid=%d",tabid,datasetid);
	st.Execute(1);
	st.Wait();
	
	mt.FetchAll("select * from dt_indexfilemap where tabid=%d and datasetid=%d",
		tabid,datasetid);
	rn=mt.Wait();
	for(i=0;i<rn;i++) {
		  lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
		  unlink(mt.PtrStr("filename",i));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		  unlink(tmp);
	}
	st.Prepare(" delete from dt_indexfilemap where tabid=%d and datasetid=%d",tabid,datasetid);
	st.Execute(1);
	st.Wait();
	
	if(srctabid<0) {
		st.Prepare(" delete from dt_table where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
		lgprintf(sqlbuf);
		conn.DoQuery(sqlbuf);
		mt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
		rn=mt.Wait();
		for(i=0;i<rn;i++) {
			sprintf(sqlbuf,"drop table %s.%s",dbn,mt.PtrStr("indextabname",i));
			lgprintf(sqlbuf);
			conn.DoQuery(sqlbuf);
		}
		st.Prepare(" delete from dt_index where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}
	else psa->EmptyIndex(dbn,tabid,conn);
		
	lgprintf("表'%s.%s'已删除.",dbn,tabname);
	return 1;
}
