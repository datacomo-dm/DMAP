/* Copyright (C) 2005 Guosheng Wang
这是DT项目中的数据备份/恢复程序的一部分。
DT备份/恢复程序对DT系统中的对象做打包/解包的处理.处理的数据对象包括:MyTable系统表,
DT系统表,DT目标表(目标表参数,数据文件,目标表/索引表),其他有可能出现的对象还有
Oracle(hs,dblink,synomyms)/MySQL/ODBC配置参数等.
设计的目标不仅限于备份/恢复,其他有可能的使用目的还有:
1). DT系统参数到mysqld服务程序的传递：可以在MySQL中每个表的三个文件(.frm,.MYI,.MYD)之外
增加一个文件，例如.dts文件，存储该表的DT系统参数，mysqld读取DT参数不再直接连接数据库作
sql操作，可以使DT表刷新更可靠，简化mysqld对DT系统参数的查询，隔离管理程序（例如dtadmin)更改
参数表时对mysqld的影响．
2). 备份后的数据，或者称为打包后的数据，可以不经解包/或者少量数据解包，直接供mysqld使用．
3). 通过FTP方式直接把数据打包到远程主机，供热备份系统或光盘备份使用．
4). 通过备份系统（例如veritas．．．)...

  考虑到磁带/光盘等媒介等操作特点，在数据打包时设计了'预处理'功能，即先生成汇总数据、目录表等，便于写到文件头，
  在读写媒介时可以最早/最方便地得到这些信息。
*/

#ifndef DTIO_H
#define DTIO_H
#include "dtio_common.h" 
#include "AutoHandle.h"
#include "dtio_mt.h"
#include "dtio_mtchooser.h"
#include "dtiomap.h"
#include "dt_common.h"
#ifndef MYSQL_SERVER
#include "mysqlconn.h"
#endif
#include <stdio.h>
#include <stdlib.h>
#ifdef WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>
#include <stdio.h>
#include <stdlib.h>
#endif
enum DTIO_STREAM_TYPE {
	FULL_BACKUP,DTP_BIND,DTP_DETACH
};
struct dtioHeader {
	uint4 flag;
	uint4 hdsize; // size of this header structrue,include extended mt data.
	uint8B imagesize; // size of whole dumped image ,include this header.
	uint4 version;
	//备份可以任选以下组合 
	//bool sysparmaexist;
	//bool dtparamexist;
	//bool dtdataexist;
	//uint8B dbsparamoffset,dbsparamlength; // database server(MySQL) system parameter table storeage area.
	//uint8B dtparamoffset,dtparamlength; // dt system parameter table storeage area.
	//uint8B dtdataoffset,dtdatalength;// dt destinateion table's data files and index files storeage area.
	enum DTIO_STREAM_TYPE stype;
	char exptime[8]; //  date and time op exporting.char array type for indep of platform.
	uint8B exportid; // identifier of export operation.
	uint8B dependon_exportid; //依赖的其他备份数据集.(为增量备份,特别是精确增量备份作准备).
	uint4 internalfileid; // sequence of internal files.
	char detaildesc[160]; // detail description of backup objects.
	char basedir[PATH_LEN];
	uint4 dtiocontct;//续接文件数量
	char basefilename[PATH_LEN];//不含路径,后缀的文件基本名,用于生成和查找续接文件.
	uint8B filelimit;// maximum length of a single file,data will be split to seperate file(s) while exceeds limits.
	uint8B filelength[MAXCONTFILE]; // size of all files,include this one.预留文件大小任意拆分的功能,便于灵活利用存储资源.
	char attachto[PATH_LEN]; //以哪个文件为附件.
	char blank[200];
	void ReverseByteOrder() {
		int i=0;
		rev8B(&exportid);
		rev8B(&dependon_exportid);
		rev8B(&filelimit);
		for(i=0;i<MAXCONTFILE;i++)
			rev8B(filelength+i);
		rev8B(&imagesize);
		revInt(&flag);revInt(&hdsize);revInt(&version);revInt(&stype);
		revInt(&internalfileid);revInt(&dtiocontct);
	}
};

#define CONTFILEFLAG 0xa533
struct dtioHeaderCont {
	uint4 flag; // 与首文件的标识不同
	uint8B hdsize;
	uint8B filelength; // size of this file,include this header.
	uint4 version;
	uint8B exportid; // 一组导出数据文件使用相同的标识码。
	uint4 internalfileid; // sequence of internal files.
	char blank[200];
	void ReverseByteOrder() {
	 rev8B(&hdsize);rev8B(&filelength);rev8B(&exportid);revInt(&flag);revInt(&version);revInt(&internalfileid);	
	}
};

// dtioStream类的特点:
//   1. 数据遍历:不实际作目标文件的写操作,只计算数据占用空间,通过write=false,可以和实际写数据时同样的流程来调用.
//   2. 灵活重定向:可以通过FTP等通道,把目标文件的存储物理位置转移到其它平台或备份设备(系统).

class DTIOExport dtioStream {
protected :
	//uint8B length;//总长度
	uint8B curoffset;//在当前文件中的偏移。
	uint8B offset; //总偏移.
	bool write;
	int cursplitid;
	char streamopt[PATH_LEN];//参数: file/ftp/tape/cdrom/backupsys
	char streamname[PATH_LEN];//基本名称(文件名)
	char streamlocation[PATH_LEN]; //目录名或目标系统地址
	char obasedir[PATH_LEN];
	uint8B splitlen;//文件拆分点
	uint4 splitedct;//数据拆分次数
	dtioHeader dtiohdr;
	dtioUnitMap mtcontent;
	dtioHeaderCont *hdrcont; // size of all files,include this one.预留文件大小任意拆分的功能,便于灵活利用存储资源.
	void SplitLenChanged() ;
	uint8B getStartOffset(int fid) ;
	uLongCRC crcv;
public :
	uint4 GetVersion() { return dtiohdr.version;}
	void SetOutDir(const char *_odir) {strcpy(dtiohdr.basedir,_odir);}
	const char *GetOutDir() {return dtiohdr.basedir;}
	const char *GetInBaseDir() {return obasedir;}
	dtioStream(const char *_basedir) ;
	DTIO_STREAM_TYPE GetStreamType() {return dtiohdr.stype;}
	const char *GetAttachFileName() {return dtiohdr.attachto;}
	int SetStream(const char *bname,const char *bopt,const char *path) ;
	virtual uint8B StreamWriteInit(DTIO_STREAM_TYPE _stype=FULL_BACKUP,const char *_attachto=NULL) ;
	virtual uint8B StreamReadInit() ;
	void initcrc() {crcv=0;}
	uLongCRC getcrc() {return crcv;}
	dtioUnitMap *GetContentMt() {return &mtcontent;}
	uint4 GetSplitCt() {return splitedct;}
	uint8B GetSPlitLen() {return splitlen;}
	void SetSplitLen(uint8B val) {splitlen=val;SplitLenChanged();}
	bool IsWrite() {return write;}
	void SetWrite(bool val) {write=val;}
	uint8B GetLength() {return dtiohdr.imagesize;}
	uint8B GetCurOffset() {return curoffset;}
	uint8B GetOffset() { return offset;}
	virtual uint8B Get(void *bf,uint8B len)=0;
	uint8B GetInt(void *bf) {
		int t=0;
		uint8B r=Get(&t,sizeof(int));
		revInt(&t);
		*(int *)bf=t;
		return r;
	}
	uint8B Get8B(void *bf) {
		uint8B t=0;
		uint8B r=Get(&t,sizeof(uint8B));
		rev8B(&t);
		*(uint8B *)bf=t;
		return r;
	}
	uint8B GetDouble(void *bf) {
		double t=0;
		uint8B r=Get(&t,sizeof(double));
		revDouble(&t);
		*(double *)bf=t;
		return r;
	}
	virtual uint8B Put(void *bf,uint8B len,bool splitable=true)=0;
	uint8B PutInt(void *bf) {
		int t=*(int *)bf;
		revInt(&t);
		return Put(&t,sizeof(int));
	}
	uint8B PutDouble(void *bf) {
		double t=*(double *)bf;
		revInt(&t);
		return Put(&t,sizeof(double));
	}
	uint8B Put8B(void *bf)
	{
		uint8B t=*(uint8B *)bf;
		rev8B(&t);
		return Put(&t,sizeof(uint8B));
	}
	
	virtual uint8B SeekAt(uint8B offset)=0;
	virtual uint8B SeekFrom(uint8B from,uint8B _offset)=0;
	virtual int SetStreamName(const char *fname)=0;
	virtual ~dtioStream() {delete []hdrcont;}
};


class DTIOExport dtioStreamFile :public dtioStream {
	FILE *fp;
protected :
	void OpenFile(int id,bool forwrite) ;
public :
	dtioStreamFile(const char *_basedir);
	virtual ~dtioStreamFile() {
		if(fp) fclose(fp);
		char fn[400];
		for(unsigned int id=0;id<=splitedct;id++)
		 {
		 	if(id!=0)
			 sprintf(fn,"%s%s.%04d",streamlocation,streamname,id);
			else sprintf(fn,"%s%s",streamlocation,streamname);
			//文件设置为只读模式
#ifdef WIN32
			_chmod(fn,_S_IREAD );
#else
			chmod(fn,S_IRUSR|S_IRGRP|S_IROTH);
#endif
		}
	}
	int SetStreamName(const char *fname) ;
	virtual uint8B Put(void *bf,uint8B len,bool splitable=true) ;
	virtual uint8B SeekAt(uint8B offset);
	virtual uint8B SeekFrom(uint8B from,uint8B _offset);
	virtual uint8B Get(void *bf,uint8B len) ;
	virtual uint8B StreamWriteInit(DTIO_STREAM_TYPE _stype=FULL_BACKUP,const char *_attachto=NULL) ;
	virtual uint8B StreamReadInit() ;
};

#define DTIOFILEFLAG 	  0x3167
#define DTIOFILEBLOCKFLAG 0x3169    //压缩文件按块存储,结构为FLAG(4bytes)+ORIGLEN(4bytes)+STORELEN(4bytes)+compressedblock.

struct dtioDataBlockHeader {
	uint4 flag;
	uint8B hdsize;
	uint8B blocksize; // size of whole dumped image size,include this header.
	char blank[20];
	void ReverseByteOrder() {
		rev8B(&hdsize);rev8B(&blocksize);revInt(&flag);
	}
};
//文件总的存储空间为每所有数据块的和(包括块的上述描述字节),压缩类型等为全局变量,只存储一次.
class DTIOExport  dtiofile {
	uint4 compressflag;
	uint8B filelen;
	uint8B storelen;//对于被压缩的目标数据,存储长度和原始长度不一致.
	uint8B startoffset;
	char path[PATH_LEN];
	char filename[PATH_LEN];
	char *porigbuf;
	char *pzipbuf;
	int filetype;
	bool enablesplit;//是否有必要限制一些文件在目标中不允许拆分?
	uint4 blocklen;
	dtioStream *dtio;
	uint8B seekat;
	void GetFile(const char *fn,int type,const char *newdir=NULL);
public :
     void OpenDtDataFile(const char *fn) ;
     uint8B GetFileLen() { return filelen;}
     void SeekAt(unsigned long off) ;
     size_t ReadBuf(char *bf,size_t len) ;
	dtiofile(dtioStream *_dtio,bool _enablesplit,uint4 _blocklen=4096000) ;
	~dtiofile() ;
	uint8B Serialize(const char *fn,int type,const char *prefix=NULL,int _compressflag=0);
	uint8B DeserializeInit(const char *prefix,const char *fbasename,int type);
	uint4 GetCompresFlag() {return compressflag;}
	uint8B GetStartPos() {return startoffset;}
	int GetFileName(char *fn,int len) ;
	int GetPath(char *_path,int len) ;
	// offset 表示文件内的相对偏移
	uint8B GetData(char *bf,uint8B offset,uint8B len);
	bool CheckCRC();
	uint8B Deserialize(const char *fn);//return scaned offset.
};

class DTIOExport dtioMyTable {
	char dbname[PATH_LEN];
	char tablename[PATH_LEN];
	dtioStream *dtio;
	int tbtype;
public :
	void SetTableByID(int id);
	int GetTableNum();
	void SetType(int type);
	dtioMyTable(dtioStream *_dtio);
	void GetDbName(char *dbn) ;
	void GetTableName(char *tn);
	void SetTable(const char *dbn,const char *tbn) ;
	uint8B Serialize(int compressflag=0);
	uint8B  Deserialize(const char *newtbn=NULL);
};

#ifndef MYSQL_SERVER
class DTIOExport dtioMyTableGroup {
	char desc[PATH_LEN];
	int type;
	int tabnum;
	dtioMyTable myio;
	dtioStream *dtio;
public :
	dtioMyTableGroup(dtioStream *_dtio,int _type,const char *_desc=NULL);
	void AddTable(const char *dbn,const char *tbn,int compressflag=0) ;
	int GetTableNum() ;
	int SearchTable(int rid,char *dbn,char *tbn);
	virtual void AfterEachRestore(MySQLConn &conn,const char *dbn,char *tbn) {
		char sql[200];
		sprintf(sql,"flush table %s.%s",dbn,tbn);
		conn.DoQuery(sql);
	}
	virtual void AfterRestoreAll(MySQLConn &conn) {
	}
	int Restore(MySQLConn &conn,int rid);
	int RestoreAll(MySQLConn &conn);
};

class DTIOExport dtioMySys :public dtioMyTableGroup {
public :
	dtioMySys(dtioStream *pdtio):dtioMyTableGroup(pdtio,DTIO_UNITTYPE_SYSDBFILE,"MySQL系统表") {};
	void AddTables() {
		AddTable("mysql","columns_priv");
		AddTable("mysql","db");
		AddTable("mysql","func");
		AddTable("mysql","tables_priv");
		AddTable("mysql","user");
		AddTable("mysql","host");
	}
	void AfterRestoreAll(MySQLConn &conn) {
		conn.DoQuery("flush privileges");
	}
};


class DTIOExport dtioDTSys :public dtioMyTableGroup {
public :
	dtioDTSys(dtioStream *pdtio):dtioMyTableGroup(pdtio,DTIO_UNITTYPE_DTDBFILE,"DT系统表") {
	}
	void AddTables() {
/*
		AddTable("dt","dt_cdfilemap");
		//AddTable("dt","dt_datachangelog");
		AddTable("dt","dt_datafilemap");
		//AddTable("dt","dt_filelink");
		AddTable("dt","dt_index");
		AddTable("dt","dt_indexfilemap");
		AddTable("dt","dt_middledatafile");
		AddTable("dt","dt_path");
		AddTable("dt","dt_srcpartinfo");
		AddTable("dt","dt_srcsys");
		AddTable("dt","dt_srctable");
		AddTable("dt","dt_table");
		AddTable("dt","dt_taskschedule");
*/
		AddTable("dp","dp_code");
		AddTable("dp","dp_codetype");
		AddTable("dp","dp_datafilemap");
		AddTable("dp","dp_datapart");
		AddTable("dp","dp_datasrc");
		AddTable("dp","dp_index");
		//AddTable("dp","dp_indexfilemap");
		AddTable("dp","dp_middledatafile");
		AddTable("dp","dp_path");
		AddTable("dp","dp_seq");
		AddTable("dp","dp_table");
		AddTable("dp","dp_user");
		AddTable("dp","dp_log");
	}
};
#endif

class DTIOExport dtioDTTable {
	char dbname[PATH_LEN];
	char tablename[PATH_LEN];
	dtioStream *dtio;
	dtparams_mt dtmts;
	bool psoleindexmode;
	void GetFile(const char *fn,int type,const char *newdir);
public:
	dtioDTTable(const char *dbn,const char *tbn,dtioStream *_dtio,bool _psoleindexmode=true);
	dtparams_mt *GetParamMt() {return &dtmts;}
	void RelinkFile();
	//预留直接访问数据功能。
	uint8B ReadBlockData(int fid,int offset,int len ,char *bf);
	void FetchParam(int dts) ;//Fill mtparam from MySQL Server(dt_table,dt_index,dt_filemap,....)
	uint8B SerializeParam(dtioStream *alterDTIO=NULL,const char *newtab=NULL); //Write mtparam and register content of dtio. register dt table itself!
	uint8B DeserializeParam();//Read content,then fill mtparam,return 0 for no item found,others for offset.
	uint8B SerializeDestTab();//Write dest table (columns define ,no real data);
	uint8B DeserializeDestTab(const char *newtbn=NULL);//Read Dest table and restore to basedir relative.
	uint8B SerializeIndex();//Write index tables and register content of dtio.using data compression default.
	uint8B DeserializeIndex(const char *newtbn=NULL);//Read index tables and restore to basedir relative.
	uint8B SerializeFile();//Write data files and index files and register content of dtio.
	uint8B DeserializeFile(const char *newdir=NULL);//Read data files and index files and register content of dtio.
	// if newdir != NULL, need update parammt;
	//以下的成员是对上面的序列/反序列子过程的一个集成。
	uint8B Serialize();
	uint8B Deserialize(int dts,const char *newdir=NULL);
};

class DTIOExport dtioDTTableGroup {
	int type;
	char desc[PATH_LEN];//Group name ?
	int itemnum;
	dtioStream *dtio;
	bool psoledidxmode;
	dtioDTTable *pdtiodttab;
	int dts;
public :
	dtioDTTableGroup(int _dts,int _type,const char *_desc,dtioStream *_dtio,bool psoledidxonly) ;
	~dtioDTTableGroup( ) ;
	int AddTable(const char *dbn,const char *tbn,bool paramonly=false);
	int GetTableNum() ;
	dtioDTTable *GetDTTable() {return pdtiodttab;}
	int SearchTable(int rid,char *dbn,char *tbn);
	int SearchIDByName(const char *dbn,const char *tbn);
	int Restore(int rid,bool fullmode=true); //fullmode=false for parammt and index table,exclude data and index files.
	int RestoreAll(bool fullmode=true);
};

#ifndef MYSQL_SERVER
class dtioMain {
	bool myparam; //是否含MySQL参数表
	bool dtparam; //是否含DT参数表
	bool dtdata; //是否含DT数据
	bool configfiles;//是否含配置文件:$ORACLE_HOME/network/admin/tnsnames.ora,
	//               $ORACLE_HOME/hs/admin/initXXXX.ora
	//               /etc/odbc.ini, /etc/odbcinst.ini
	//               $MYSQLBASEDIR/var/my.cnf
	bool psoledidxmode;//主独立索引标志.仅备份主索引及数据.
	char basedir[PATH_LEN]; //MySQL base directory
	char streamPath[PATH_LEN]; //destination stream path and stream name.
	char destdir[PATH_LEN];// 恢复操作时数据文件可以指定路径,path for dt data(idx) file restore.如果Stream中有多个DT表,有可能每个表的路径不同.
	dtioStream *pdtio;
	AutoHandle dts; // 代理数据库服务器连接符。代理数据库服务器中有DT参数表的同义词。
	MTChooser dtmt;//目标DT表选单
	MTChooser parammt; //MySQL系统参数表，和DT系统参数表做恢复操作时必须慎重，特别对于DT_PATH等标的操作会严重影响DT系统。
	MySQLConn conn;

protected :
	//int getOption(const char *prompt,int defaultval,const int lmin,const int lmax) ;
	//int getString(const char *prompt,const char *defaultval,char *val) ;
	//int getdbcstr(char *un,char *pwd,char *sn,const char *prompt) ;
	//bool GetYesNo(const char *prompt,bool defval) ;
	int CreateDataTableAndIndex(const char *dbname,const char *tbname,dtparams_mt *pdt);
	int SetSequence();
public :
	dtioMain() ;
	~dtioMain() ;
	void BackupPrepare() ;
	void Backup(const char *dbname=NULL,const char *tabname=NULL) ;
	void RunByParam(void *cp);
	void DoBackup(const char *dbname=NULL,const char *tabname=NULL) ;
	void BindPrepare();
	void PreviousBind();
	void CreateBind(const char *dbn,const char *tbn);
	void DoBind(const char *dbn,const char *tbn);
	void RestorePrepare();
	void List(const char *filename=NULL,bool withlog=false,const char *dbn=NULL,const char *tbn=NULL);
	void PreviousRestore(const char *dbn=NULL,const char *tbn=NULL,const char *ndtpath=NULL);
	void DoRestore();
   	void DetachPrepare();
   	void PreviousDetach(const char *dbn=NULL,const char *tbn=NULL,const char *newtbn=NULL);
};
#endif

#endif
