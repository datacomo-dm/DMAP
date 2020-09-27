
#include "dmncftp.h"
#include "dt_common.h"
#include <glob.h>
#include <ncftp.h>				/* Library header. */
#include <zlib.h>
       #include <sys/types.h>
       #include <sys/stat.h>
       #include <unistd.h>
void *FTPClient::envhdr=NULL;
int FTPClient::refct=0;

#define connInfo ((FTPConnectionInfo *)connhdr)
#define libInfo	 ((FTPLibraryInfo *)envhdr)
#define lconnInfo connhdr
#define llibInfo envhdr

long localfilesize(const char *srcname) {
	struct stat _stat;
	if(stat(srcname, &_stat)!=0) {
		ThrowWith("文件'%s'打开(fstat)失败.",srcname);
	}
	return _stat.st_size;
}	
#define GZ_BUFLEN 1024*64
long localcopyfile(const char *srcname,const char *dstname) {

		FILE *sfp=fopen(srcname,"rb");
		if(sfp==NULL) {
			ThrowWith("copyfile:%s打开失败.",srcname);
			return -1;
		}
		FILE *fp=NULL;

    fp=fopen(dstname,"wb");
    if(fp==NULL) {
    	fclose(sfp);
    	ThrowWith("copyfile:%s打开失败.",dstname);
    }
    char buf[GZ_BUFLEN];
    long wrted=0;
    int errcode=0;
    int readed=fread(buf,1,GZ_BUFLEN,sfp);
    while(readed>0) {
    	int w=fwrite(buf,readed,1,fp);
    	if(w<1)
    		ThrowWith("copyfile:%s->%s写入数据错误.",srcname,dstname);
    	wrted+=w;
    	readed=fread(buf,1,GZ_BUFLEN,sfp);
    }
    fclose(sfp);
    fclose(fp);
    
    return wrted;
}


	// dstname may be null
	long gzipfile(const char *srcname,const char *dstname) {
		FILE *sfp=fopen(srcname,"rb");
		if(sfp==NULL) {
			ThrowWith("gzip:%s打开失败.",srcname);
			return -1;
		}
		gzFile fp=NULL;
		char gzfile[300];
		if(dstname==NULL) {
			strcpy(gzfile,srcname);
			strcat(gzfile,".gz");
		}
		else strcpy(gzfile,dstname);
    fp=gzopen(gzfile,"wb");
    if(fp==NULL) {
    	fclose(sfp);
    	ThrowWith("gzip:%s打开失败.",gzfile);
    }
    char buf[GZ_BUFLEN];
    long wrted=0;
    int errcode=0;
    int readed=fread(buf,1,GZ_BUFLEN,sfp);
    while(readed>0) {
    	int w=gzwrite(fp,buf,readed);
    	if(w<1)
    		ThrowWith("gzip:%s写入数据错误,%s.",srcname,gzerror(fp,&errcode));
    	wrted+=w;
    	readed=fread(buf,1,GZ_BUFLEN,sfp);
    }
    fclose(sfp);
    gzclose(fp);
    if(dstname==NULL)
    	unlink(srcname);
    return wrted;
  }
  
	// dstname may be null
	long gunzipfile(const char *srcname,const char *dstname) {
		gzFile sfp=gzopen(srcname,"rb");
		if(sfp==NULL) {
			ThrowWith("gzip:%s打开失败.",srcname);
			return -1;
		}
		FILE *fp=NULL;
		char file[300];
		if(dstname==NULL) {
			strcpy(file,srcname);
			// remove .gz
			file[strlen(srcname)-3]=0;
		}
		else strcpy(file,dstname);
    fp=fopen(file,"wb");
    if(fp==NULL) {
    	gzclose(sfp);
    	ThrowWith("gunzip:%s打开失败.",file);
    }
    char buf[GZ_BUFLEN];
    long wrted=0;
    int errcode=0;
    int readed=gzread(sfp,buf,GZ_BUFLEN);
    if(readed<0)
    		ThrowWith("gzip:%s读数据错误,%s.",file,gzerror(sfp,&errcode));
    while(readed>0) {
    	int w=fwrite(buf,1,readed,fp);
    	if(w<1)
    		ThrowWith("gzip:%s写入数据错误,%s.",file,gzerror(sfp,&errcode));
    	wrted+=w;
    	
    	readed=gzread(sfp,buf,GZ_BUFLEN);
    	if(readed<0)
    		ThrowWith("gzip:%s读数据错误,%s.",file,gzerror(sfp,&errcode));
    }
    fclose(fp);
    gzclose(sfp);
    if(dstname==NULL)
    	unlink(srcname);
    return wrted;
  }

	int listfile(const char *parttern,const char *lstfile) {
		glob_t globbuf;
		memset(&globbuf,0,sizeof(globbuf));
		globbuf.gl_offs = 0;
		FILE *fp=fopen(lstfile,"wt");
		if(fp==NULL) {
			ThrowWith("结果文件%s打开失败.",lstfile);
		}
		// GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
		// GLOB_BRACE : style brace expressions of the form {a,b}.
		if(!glob(parttern, GLOB_DOOFFS|GLOB_BRACE, NULL, &globbuf)) {
			for(int i=0;i<globbuf.gl_pathc;i++) {
				fprintf(fp,"%s\n",globbuf.gl_pathv[i]);
			}
		}
		fclose(fp);
		globfree(&globbuf);
	}

  void FTPClient::Close()
  {
	  if(connected) {
		    FTPCloseHost(connInfo);
	  }
      fstype=0;filehdr=NULL;
      connected=false;
  }

  
  FTPClient::FTPClient()
  {
    connected=false;filehdr=NULL;fstype=0;
	if(envhdr==NULL) {
		InitTransfer();
	}
	lconnInfo=(void *)new FTPConnectionInfo;
	delete connInfo;
	lconnInfo=(void *)new FTPConnectionInfo;
	int result = FTPInitConnectionInfo(libInfo, connInfo, kDefaultFTPBufSize);
	if (result < 0) 
		ThrowWith("FTP初始化:%s.",FTPStrError(result));
        delete connInfo;
        lconnInfo=(void *)new FTPConnectionInfo;
        FTPInitConnectionInfo(libInfo, connInfo, kDefaultFTPBufSize);
	refct++;
  }
  
  void FTPClient::SetContext(const char *_host,const char *_user,const char *_password)
  {
  	if(connected) 
		Close();
  	strcpy(host,_host);
	strcpy(user,_user);
	strcpy(authcode,_password);
	ConnectFTP();
  }

  void FTPClient::InitTransfer() {
	InitWinsock();	/* Calls WSAStartup() on Windows; No-Op on UNIX */
	llibInfo=(void *)new FTPLibraryInfo;
	int result = FTPInitLibrary(libInfo);
	if (result < 0) 
		ThrowWith("FTP初始化:%s.",FTPStrError(result));
  }
  
  void FTPClient::QuitTransfer() {
	DisposeWinsock();	/* Calls WSACleanup() on Windows; No-Op on UNIX */
	delete libInfo;
	llibInfo=NULL;
  }



  bool FTPClient::ConnectFTP() {
	int result;
	//for test
	FTPConnectionInfo *ptr1=connInfo;	
	strcpy(connInfo->host, host);
	strcpy(connInfo->user, user);
	strcpy(connInfo->pass, authcode);
	//connInfo.debugLog = stdout;	/* Print the whole FTP conversation. */

	if ((result = FTPOpenHost(connInfo)) < 0) 
		ThrowWith("FTP 连接:%s.",FTPStrError(result));
	connected=true;
	return true;
  }
  
  void FTPClient::PutFile(const char *srcpath,const char *destpath) {
	int result;
	if(!connected)
		ConnectFTP();
   	result = FTPPutOneFile3(
				connInfo,
				srcpath,destpath,
				kTypeBinary,
				-1,kAppendNo,
				NULL, NULL, kResumeNo, kDeleteNo, kNoFTPConfirmResumeUploadProc, 0
			);
	if (result < 0) 
			ThrowWith("FTP 上传 '%s'->'%s':%s.",srcpath,destpath,FTPStrError(result));
 }

  //按二进制方式从远端获取文件
  void FTPClient::GetFile(const char *srcpath,const char *destpath) {
	int result;
	int retry_times= 0;
retry_begin:
	if(!connected){
		ConnectFTP();	
	}
  	result = FTPGetOneFile3(
				connInfo,
				srcpath,destpath,
				kTypeBinary,
				-1,
				kResumeNo,
				kAppendNo,
				kDeleteNo,
				kNoFTPConfirmResumeDownloadProc,
				0
			);
	if (result < 0) {
		if(retry_times++<5){
			Close();			
			goto retry_begin;
		}
		ThrowWith("FTP 下载 '%s'->'%s':%s.",srcpath,destpath,FTPStrError(result));
	}
  }
  
  // delete a single file
  void FTPClient::Delete(const char *remotefile) {
	if(!connected)
		ConnectFTP();	int result = FTPDelete(connInfo, remotefile, kRecursiveNo, kGlobNo);
	if (result < 0) 
			ThrowWith("FTP 删除'%s':%s.",remotefile,FTPStrError(result));
  }

  //return files name
  int FTPClient::ListFile(const char *fileparttern,const char *lstfile){
	FTPLineList fileList;
	if(!connected)
		ConnectFTP();
	FILE *fp=fopen(lstfile,"wt");
	if(fp==NULL) 
		ThrowWith("FTP list '%s'>>'%s':'%s'文件不能写入.",fileparttern,lstfile,lstfile); 
	InitLineList(&fileList);
	int result=FTPRemoteGlob(connInfo, &fileList, fileparttern, kGlobYes);
 	if (result < 0 && result != kErrGlobNoMatch) {
		fclose(fp);
		ThrowWith("FTP list '%s'>>'%s':%s.",fileparttern,lstfile,FTPStrError(result)); 
 	}
 	if(result != kErrGlobNoMatch) {
	 FTPLinePtr lp=fileList.first;
	 while(lp!=NULL) {
		fputs(lp->line,fp);
		fputs("\n",fp);
		lp = lp->next;
	 }
  }
	DisposeLineListContents(&fileList);
	fclose(fp);
  }

  long FTPClient::FileSizeAndMTime(const char *remotepath,time_t &mdtm)
  {
		if(!connected)
			ConnectFTP();	  	
		longest_int fsize=0;
		int result=FTPFileSizeAndModificationTime(connInfo,remotepath,&fsize,kTypeBinary,&mdtm);
		if (result < 0) {
			ThrowWith("FTP FileSize '%s':%s.",remotepath,FTPStrError(result)); 
 		}
		return fsize;
  }
  
  long FTPClient::FileSize(const char *remotepath) {
	    int retry_times= 0;
retry_begin:
  		if(!connected)
		ConnectFTP();
  		longest_int fsize=0;
		int result=FTPFileSize(connInfo,remotepath,&fsize,kTypeBinary);
		if (result < 0) {
			if(retry_times++<5){
				Close();
				goto retry_begin;
			}
			ThrowWith("FTP FileSize '%s':%s.",remotepath,FTPStrError(result)); 
 		}
		return fsize;
  }
  
  void FTPClient::GetMTime(const char *const file, time_t &mdtm) {
	if(!connected)
		ConnectFTP();
	int result=FTPFileModificationTime(connInfo,file,&mdtm);
	if (result < 0) {
		ThrowWith("FTP GetMTime '%s':%s.",file,FTPStrError(result)); 
 	}
  }
  
  FTPClient::~FTPClient()
  {
  	Close();
        if(lconnInfo)
	  delete connInfo;
	lconnInfo=NULL;
	//Close envir hdr;
	refct--;
	if(refct==0) {
		QuitTransfer();
	}
  }
  
