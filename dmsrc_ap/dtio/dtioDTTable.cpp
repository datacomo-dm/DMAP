#include "dtio.h"
#include "dt_svrlib.h"
dtioDTTable::dtioDTTable(const char *dbn,const char *tbn,dtioStream *_dtio,bool _psoleindexmode):dtmts(dbn,tbn)
{
	strcpy(dbname,dbn);strcpy(tablename,tbn);dtio=_dtio;
	psoleindexmode=_psoleindexmode;
}

//预留直接访问数据功能。
uint8B dtioDTTable::ReadBlockData(int fid,int offset,int len ,char *bf) {
	ThrowWith("此功能暂不支持!");	
	return 0;
}

void dtioDTTable::FetchParam(int dts)
{
	dtmts.FetchParam(dts,psoleindexmode,dtio);
}

uint8B dtioDTTable::SerializeParam(dtioStream *alterDTIO,const char *newtab) 
{
	dtioStream *ldtio=alterDTIO==NULL?dtio:alterDTIO;
	printf("备份'%s.%s'--参数...\n",dbname,tablename);
	uint8B st=ldtio->GetOffset();
	uint8B len=dtmts.Serialize(ldtio,0,true)-st;
        ldtio->GetContentMt()->SetUnit(DTIO_UNITTYPE_DTPARAM,dbname,newtab!=NULL?newtab:tablename,(double)st,(double)len);
	return ldtio->GetOffset();
}

uint8B dtioDTTable::DeserializeParam()
{
	//以下信息会输出到mysqld日志，屏蔽。
	//printf("恢复'%s.%s'--参数...\n",dbname,tablename);
	double st=0,len=0;
	dtio->GetContentMt()->GetUnit(DTIO_UNITTYPE_DTPARAM,dbname,tablename,st,len);
	dtio->SeekAt((uint8B)st);
	dtmts.Deserialize(dtio);
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeDestTab()
{
	printf("备份'%s.%s'--目标表...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_DESTDBFILE);
	myio.SetTable(dbname,tablename);
	myio.Serialize();
	return dtio->GetOffset();
}

uint8B dtioDTTable::DeserializeDestTab(const char *newtbn)
{
	printf("恢复'%s.%s'--目标表...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_DESTDBFILE);
	myio.SetTable(dbname,tablename);
	myio.Deserialize(newtbn==NULL?tablename:newtbn);
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeIndex()
{
	printf("备份'%s.%s'--索引表...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_IDXDBFILE);
	int idxnum=dtmts.GetSoledIndexNum();
	int datapartnum=dtmts.GetTotDataPartNum();
	for(int i=0;i<idxnum;i++)
	{
		char dbn[200],tbn[200];
		dtmts.GetIndexTable(i,dbn,tbn);
		myio.SetTable(dbn,tbn);
		myio.Serialize();
		if(dtmts.IsPartIndex(dtio->GetOutDir())) {
			for(int datapart=0;datapart<datapartnum;datapart++) {
				myio.SetTable(dbn,dtmts.PartIndexTbn(i,datapart));
				myio.Serialize();
			}
		}
	}
	return dtio->GetOffset();
}

//这个操作中如果newtbn!=null,会修改dt_table/dt_index内存表中的tabname字段
//  这种情况需要确保该调用是除了DTP参数生成外的最后一次内存表引用

//newtbn!=null,这是更名恢复表的操作。
uint8B dtioDTTable::DeserializeIndex(const char *newtbn)
{
	printf("恢复'%s.%s'--索引表...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_IDXDBFILE);
	int idxnum=dtmts.GetSoledIndexNum();
	char dbn[PATH_LEN],tbn[PATH_LEN];
	dtmts.GetIndexTable(0,dbn,tbn);
	int datapartnum=dtmts.GetTotDataPartNum();
	for(int i=0;i<idxnum;i++)
	{
		dtmts.GetIndexTable(i,dbn,tbn);
		myio.SetTable(dbn,tbn);
		if(newtbn!=NULL) {
		 sprintf(tbn,"%sidx%d",newtbn,dtmts.GetInt("DP_INDEX","indexgid",i));//i+1);
		 //empty index tabname field.
		 dtmts.SetString(GetDPIndex(dtio),"indextabname","",i);
		}
		myio.Deserialize(tbn);
		char fn[PATH_LEN];
		//检查分区索引表是否存在(.frm).
		sprintf(fn,"%s.frm",dtmts.PartIndexTbn(i,0));
		if(dtio->GetContentMt()->CheckUnit(DTIO_UNITTYPE_MFRMFILE,dbn,fn))
		{
			sprintf(fn,"%s%s/%s.MRG",dtio->GetInBaseDir(),dbn,tbn);
			FILE *fp=fopen(fn,"w+t");
			for(int datapart=0;datapart<datapartnum;datapart++) {
			 myio.SetTable(dbn,dtmts.PartIndexTbn(i,datapart));
			 myio.Deserialize(dtmts.PartIndexTbn(i,datapart,newtbn));
			 fprintf(fp,"%s\n",dtmts.PartIndexTbn(i,datapart,newtbn));
			}
			fprintf(fp,"#INSERT_METHOD=LAST\n");
		}
	}
	//目标表已恢复，为DTP参数文件生成作准备
	if(newtbn!=NULL) {
	  dtmts.SetString(GetDPTable(dtio),"tabname",newtbn,0);
	}
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeFile()
{
	printf("备份'%s.%s'--数据文件...\n",dbname,tablename);
	int filenum=dtmts.GetDataFileNum();
	dtiofile outf(dtio,true);
	char fn[200];
	for(int i=0;i<filenum;i++) {
		outf.Serialize((dtmts.GetDataFile(i,fn),fn),DTIO_UNITTYPE_DATFILE);
		outf.Serialize((dtmts.GetIndexFile(i,fn),fn),DTIO_UNITTYPE_IDXFILE);
	}
	return dtio->GetOffset();
}

void dtioDTTable::GetFile(const char *fn,int type,const char *newdir)
{
		  dtiofile inf(dtio,true);
		  char opath[200],obasename[200];
		  char dirc[200],basec[200];
		  strcpy(dirc ,fn);
		  strcpy(basec ,fn);
		  strcpy(opath, dirname(dirc));
		  strcpy(obasename,basename(basec));
		  inf.DeserializeInit(opath,obasename,type);
		  if(newdir!=NULL) strcpy(opath,newdir);
		  if(opath[strlen(opath)-1]!=PATH_SEPCHAR) {
			  strcat(opath,PATH_SEP);
		  }
		  strcat(opath,obasename);
		  inf.Deserialize(opath);
}

uint8B dtioDTTable::DeserializeFile(const char *newdir)
{
		printf("恢复'%s.%s'--数据文件...\n",dbname,tablename);
		  int filenum=dtmts.GetDataFileNum();
		  char fn[200];
		  for(int i=0;i<filenum;i++) {
			  dtmts.GetDataFile(i,fn);
			  GetFile(fn,DTIO_UNITTYPE_DATFILE,newdir);
			  dtmts.GetIndexFile(i,fn);
			  GetFile(fn,DTIO_UNITTYPE_IDXFILE,newdir);
		  }
	return dtio->GetOffset();
}

uint8B dtioDTTable::Serialize()
{
	printf("备份'%s.%s'...\n",dbname,tablename);
	SerializeParam();
	SerializeDestTab();
	SerializeIndex();
	SerializeFile();
	return dtio->GetOffset();
}

void dtioDTTable::RelinkFile() {
	printf("重建文件链接.\n");
	int rn=dtmts.GetDataFileNum();
	if(dtmts.GetInt(GetDFMName(dtio),"fileid",0)!=
	  dtmts.GetInt(GetDPTable(dtio),"firstdatafileid",0)) {
	  printf("错误：文件映射表与目标表记录的首文件号不一致，无法作文件链接，需要重新装入数据.");
	  return;
	}
	
	for(int k=0;k<rn;k++) {
		//Build index & data file link information.
		if(k+1==rn) {
		  dt_file df;
		  df.Open(dtmts.GetString(GetDFMName(dtio),"filename",k),2,dtmts.GetInt(GetDFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,NULL);
		  //必须关闭文件,否则下次打开时如果fid和openmode相同时可能被错误忽略.
		  df.Close();
		  df.Open(dtmts.GetString(GetIFMName(dtio),"idxfname",k),2,dtmts.GetInt(GetIFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,NULL);
		  df.Close();
		  //lgprintf(
		}
		else {
		  dt_file df;
		  df.Open(dtmts.GetString(GetDFMName(dtio),"filename",k),2,dtmts.GetInt(GetDFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,dtmts.GetString(GetDFMName(dtio),"filename",k+1));
		  //必须关闭文件,否则下次打开时如果fid和openmode相同时可能被错误忽略.
		  df.Close();
		  df.Open(dtmts.GetString(GetIFMName(dtio),"idxfname",k),2,dtmts.GetInt(GetIFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,dtmts.GetString(GetIFMName(dtio),"idxfname",k+1));
		  df.Close();
		}
	}
}
			  
uint8B dtioDTTable::Deserialize(int dts,const char *newdir)
{
	printf("恢复'%s.%s'...\n",dbname,tablename);
	//DeserializeParam();
	char fn[500];
	dtmts.GetDataFile(0,fn);
#ifdef WIN32
	if(fn[1]!=':' && (newdir==NULL || newdir[1]!=':')) 
		ThrowWith("备份文件来自Unix平台(%s)，必须指定Windows平台的数据文件恢复绝对路径(-datapath选项)",fn);
#else
	if(fn[1]==':' && (newdir==NULL || newdir[1]==':')) 
		ThrowWith("备份文件来自Windows平台(%s)，必须指定Unix平台的数据文件恢复绝对路径(-datapath选项)",fn);
#endif
	DeserializeDestTab();
	DeserializeIndex();
	DeserializeFile(newdir);
	dtmts.adjparam(newdir);
	dtmts.restore(dts,newdir);
	RelinkFile();
	return dtio->GetOffset();
}


