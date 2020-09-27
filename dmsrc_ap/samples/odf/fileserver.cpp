#include "odf.h"  
  
#include <ncftp.h>				/* Library header. */

void *filetrans::envhdr=NULL;
void *filetrans::connhdr=NULL;

#define connInfo ((FTPConnectionInfo *)envhdr)
#define libInfo	 ((FTPLibraryInfo *)connhdr)
#define lconnInfo envhdr
#define llibInfo connhdr
  void filetrans::clear()
  {
	  if(connected) {
		    FTPCloseHost(connInfo);
	  }
      client_id=0;fstype=0;filehdr=NULL;isopen=false;
      connected=false;
  }

  filetrans::filetrans(param_db *_pdb)
  {
    pdb=_pdb;isopen=false;
    connected=false;filehdr=NULL;client_id=0;fstype=0;
  }
  
  void filetrans::SetContext(int _ds_id,int _client_id)
  {
   client_id=_client_id;
   ds_id=_ds_id;
   AutoMt mt(*pdb,10);
   mt.FetchAll("select * from tabt_filesys where sysid=(select ext_filesysid from tabt_dataset where ds_id=%d)",ds_id);
   if(mt.Wait()<1) 
      	  ThrowWith("数据集-%d 无效的文件系统标识!",ds_id);
   fstype=mt.GetInt("systype",0);
   mt.GetStr("hostaddr",0,host);
   mt.GetStr("username",0,user);
   mt.GetStr("authcode",0,authcode);
   if(mt.GetInt("systype",0)==2) ConnectFTP();
  }

  void filetrans::InitTransfer() {
	InitWinsock();	/* Calls WSAStartup() on Windows; No-Op on UNIX */
	llibInfo=(void *)new FTPLibraryInfo;
	lconnInfo=(void *)new FTPConnectionInfo;
	int result = FTPInitLibrary(libInfo);
	if (result < 0) 
		ThrowWith("FTP初始化:%s.",FTPStrError(result));
	result = FTPInitConnectionInfo(libInfo, connInfo, kDefaultFTPBufSize);
	if (result < 0) 
		ThrowWith("FTP初始化:%s.",FTPStrError(result));
	//connInfo->debugLog = stdout;	/* Print the whole FTP conversation. */
  }
  
  void filetrans::QuitTransfer() {
	FTPCloseHost(connInfo);
	DisposeWinsock();	/* Calls WSACleanup() on Windows; No-Op on UNIX */
	delete libInfo;
	delete connInfo;
  }
  
  bool filetrans::ConnectFTP() {

  	//ThrowWith("数据集-%d FTP暂未实现!",ds_id);
	char pswd[100];
	int result;
	strcpy(pswd,authcode);
	decode(pswd);
	strncpy(connInfo->host, host, sizeof(connInfo->host) - 1);
	strncpy(connInfo->user, user, sizeof(connInfo->user) - 1);
	strncpy(connInfo->pass, pswd, sizeof(connInfo->pass) - 1);
	//connInfo.debugLog = stdout;	/* Print the whole FTP conversation. */

	if ((result = FTPOpenHost(connInfo)) < 0) 
		ThrowWith("FTP 连接:%s.",FTPStrError(result));
	connected=true;
	return true;
  }
  
  
  //void Open(fileid);
  void filetrans::Transfer(int fileid) {
	  int result;
  	AutoMt mt(*pdb,10);
  	mt.FetchAll("select * from tabt_extrec where fileid=%d",fileid);
  	if(mt.Wait()<1)
  	  ThrowWith("抽取文件-%d 未找到!",fileid);
  	//客户端使用的文件编号和服务端抽取时相同，因此，需要检查之前是否有传输操作，并作相应的删除处理
  	AutoMt mt1(*pdb,10);
  	mt1.FetchAll("select * from tabt_transrec where fileid=%d and client_id=%d",fileid,client_id);
  	if(mt1.Wait()>0)
  	  {
  	  	//原位文件和已删除文件不处理
  	  	if(mt1.GetInt("local_status",0)==0)
  	  	 unlink(mt1.PtrStr("local_filename",0));
  		AutoStmt st(*pdb);
  	  	st.DirectExecute("delete from tabt_transrec where fileid=%d",fileid);
  	  }
  	char client_path[MAX_PATH];
  	pdb->GetClientPath(mt.GetInt("ds_id",0),client_id,client_path);
  	sprintf(client_path+strlen(client_path),"/%d_%d_%d_%d.tabt",mt.GetInt("ds_id",0),
  	     mt.GetInt("ext_sn",0),mt.GetInt("blocktype",0),fileid);
  	AutoStmt stmt(*pdb);
  	switch(fstype) {
  	case 1:
  	  stmt.Prepare("insert into tabt_transrec values (%d,%d,2,sysdate,'%s',2,%d)", //原位文件，已下载
  	       fileid,client_id,mt.PtrStr("filename",0),mt.GetInt("ds_id",0));
  	  break;
  	case 2:
		lgprintf("FTP 传输 '%s'->'%s'.",mt.PtrStr("filename",0),client_path);
  		result = FTPGetOneFile3(
				connInfo,
				mt.PtrStr("filename",0),client_path,
				kTypeBinary,
				-1,
				kResumeNo,
				kAppendNo,
				kDeleteNo,
				kNoFTPConfirmResumeDownloadProc,
				0
			);
		if (result < 0) 
			ThrowWith("FTP 传输 '%s'->'%s':%s.",mt.PtrStr("filename",0),client_path,FTPStrError(result));
  	    stmt.Prepare("insert into tabt_transrec values (%d,%d,2,sysdate,'%s',0,%d)", //FTP文件，已下载
  	       fileid,client_id,client_path,mt.GetInt("ds_id",0));
  	  break;
  	case 3: //本机文件拷贝
  	   lgprintf("复制文件'%s'->'%s'.",mt.PtrStr("filename",0),client_path);
  	   if(mCopyFile(mt.PtrStr("filename",0),client_path)<1)  //defined at dt_common(version modified 2005/04/19)
  	    ThrowWith("复制文件'%s'->'%s'失败",mt.PtrStr("filename",0),client_path);
  	   stmt.Prepare("insert into tabt_transrec values (%d,%d,2,sysdate,'%s',0,%d)", //已下载,本机文件
  	       fileid,client_id,client_path,mt.GetInt("ds_id",0));
  	   break;
  	}
  	stmt.Execute(1);
	stmt.Wait();
  	wociCommit(*pdb);
  }
  
  filetrans::~filetrans()
  {
  	clear();
  	//Close envir hdr;
  }
