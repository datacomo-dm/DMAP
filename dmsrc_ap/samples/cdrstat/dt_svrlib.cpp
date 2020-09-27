//#include <cstring>
//#include <queue>
#include "dt_svrlib.h"
#include "zlib.h"
#include <lzo1x.h>
//#include <lzo1z.h>
//#include <lzo1c.h>
//#include <lzo1.h>
//#include <zzlib.h>
#include <bzlib.h>
#ifndef __unix
#include <io.h> 
#include <process.h>
#include <direct.h>
#define STRICMP _stricmp
#else
#define getch getchar
#include <stdio.h>
#include <pthread.h>
#include <strings.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/mode.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <unistd.h>
#define _chdir chdir
#define _mkdir mkdir
#define STRICMP strcasecmp
#endif


#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif

//using namespace std;
SvrAdmin *SvrAdmin::psa=NULL;
AutoHandle *SvrAdmin::pdts=NULL;
int SvrAdmin::svrstarted=0;
int SvrAdmin::shutdown=0;
#ifdef __unix
pthread_t SvrAdmin::hd_pthread_t;
#endif
void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[1000];
	vsprintf(msg,format,vlist);
	//errprintf(msg);
	va_end(vlist);
	throw msg;
}

int GetFreeM(const char *path) {
#ifdef WIN32
   char drive[_MAX_DRIVE];
   char dir[_MAX_DIR];
   char fname[_MAX_FNAME];
   char ext[_MAX_EXT];
   _diskfree_t dskinfo;
   _splitpath( path, drive, dir, fname, ext );
   _getdiskfree(drive[0]-'a'+1,&dskinfo);
   double freebytes=(double)dskinfo.avail_clusters*dskinfo.bytes_per_sector*dskinfo.sectors_per_cluster;
   return (int )(freebytes/(1024*1024));
#else
   struct statfs freefs;
   if(statfs("/dds/dtdata",&freefs)==0)
     return freefs.f_fsize*freefs.f_bavail/(1024*1024);
   return 20480;
#endif
}

int xmkdir(const char *dir) {
	int len=strlen(dir);
	char tdir[300];
	int rt=0;
	const char *pt=dir;
	int off=0;
	while(*pt && pt-dir<len) {
		if(*pt=='\\' || *pt=='/' || !pt[1]) {
			if(pt-dir>0 && pt[-1]!=':') 
			{
				strcpy(tdir,dir);
				tdir[pt-dir+1]=0;
				rt=_chdir(tdir);
				if(rt) {
#ifdef WIN32
				 rt=_mkdir(tdir);
#else
				 rt=mkdir(tdir,S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
#endif
				}
				if(rt) break;
			}
		}
		pt++;
	}
	return rt;
}


	int SplitStr(const char *src,char **dst,char **pbf) {
		char *bf=*pbf;
		int sn=0;
		while(*src) {
			const char *se=src;
			while(*se && *se!=',') se++;
			strncpy(bf,src,se-src);
			bf[se-src]=0;
			dst[sn++]=bf;
			bf+=se-src+1;
			src=se;
			if(*src==',') src++;
		}
		*pbf=bf;
		return sn;
	}

	void BuildWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf)
	{
		strcpy(sqlbf," where ");
		char bf[10000];
		char *pbf=bf;
		char *cars[60],*bars[60],*ears[60];
		int cn=SplitStr(cols,cars,&pbf);
		int bn=SplitStr(bvals,bars,&pbf);
		int en=SplitStr(evals,ears,&pbf);
		if(cn<1) ThrowWith("Encounter a null partiotion column(s) name");
		if(cn!=bn || bn!=cn) ThrowWith("Mismatch column(s) and value(s) of partiton info");
		for(int i=0;i<cn;i++) 
			sprintf(sqlbf+strlen(sqlbf),"%s %s>=%s and %s<%s",
				i==0?"":" and ",cars[i],bars[i],cars[i],ears[i]);
	}

thread_rt Handle_Read(void *ptr) {
	dt_file *p=(dt_file *)ptr;
	while(1)
	{
		if(WAIT_TIMEOUT==p->WaitStart(50)) {
			if(p->Terminated()) {
				p->End();
				break;
			}
		}
		else {
			if(p->Terminated()) {
				p->End();
				break;
			}
			p->ReadMtThread();
			p->End();
		}
	}
	p->ResetTerminated();
	thread_end; 
}

	dt_file::dt_file(BOOL _paral) {
		fp=NULL;
		openmode=0;
		fnid=0;
		filename[0]=0;
		lastoffset=0;
		blockbf=NULL;
		cmprsbf=NULL;
		paral=_paral;
		bflen=0;filesize=0;pwrkmem=NULL;
		cmprslen=0;oldoffset=0;terminate=false;
		if (lzo_init() != LZO_E_OK)
		{
		   ThrowWith("lzo_init() failed !!!\n");
		}

		if(paral) {
#ifdef __unix
	if(pthread_mutex_init(&hd_start,NULL)!=0)
		ThrowWith("create mutex faid");
	if(pthread_mutex_init(&hd_end,NULL)!=0)
		ThrowWith("create mutex faid");
	WaitStart(); // LockStart
	
	pthread_create(&hd_pthread_t,NULL,Handle_Read,(void *)this);
	pthread_detach(hd_pthread_t);
#else
	static int evtct=0;
	char evtname[100];
	sprintf(evtname,"dt_file_%d",evtct++);
	hd_start=CreateEvent(NULL,false,false,evtname);
	sprintf(evtname,"dt_file_%d",evtct++);
	hd_end=CreateEvent(NULL,false,true,evtname);
	_beginthread(Handle_Read,81920,(void *)this);
#endif
	End();// Unlock End.
	}
	}

void dt_file::SetParalMode(bool val) {
	if(val) {
		if(paral) return;
		paral=val;
#ifdef __unix
	if(pthread_mutex_init(&hd_start,NULL)!=0)
		ThrowWith("create mutex faid");
	if(pthread_mutex_init(&hd_end,NULL)!=0)
		ThrowWith("create mutex faid");
	WaitStart(); // LockStart
	
	pthread_create(&hd_pthread_t,NULL,Handle_Read,(void *)this);
	pthread_detach(hd_pthread_t);
#else
	static int evtct=0;
	char evtname[100];
	sprintf(evtname,"dt_file_%d",evtct++);
	hd_start=CreateEvent(NULL,false,false,evtname);
	sprintf(evtname,"dt_file_%d",evtct++);
	hd_end=CreateEvent(NULL,false,true,evtname);
	_beginthread(Handle_Read,81920,(void *)this);
#endif
	}
	else {
		if(paral) {
			paral=false;
#ifdef __unix
		//WaitEnd();
		Start();
		while(terminate==true);
		//WaitEnd();
#else
		ResetEvent(hd_end);
		Start();
		WaitEnd();
#endif
		//Lock(0,INFINITE);
		//Lock(1,INFINITE);
#ifdef __unix
		pthread_mutex_unlock(&hd_start);
		pthread_mutex_destroy(&hd_end);
#else
		CloseHandle(hd_start);
		CloseHandle(hd_end);
#endif
		}
		paral=false;
	}
}


	void dt_file::SetFileHeader(int rn,const char *nextfile) {
		if(!fp) ThrowWith("Write on open failed file at SetNextFile,filename:%s",filename);
		//file_hdr fh;
		//fread(&fh,sizeof(file_hdr),1,fp);
		if(nextfile==NULL) {
			fh.nextfile[0]=0;
			fh.islastfile=1;
		}
		else {
			strcpy(fh.nextfile,nextfile);
			fh.islastfile=0;
		}
		if(rn>0)
		 fh.rownum=rn;
		
		fseek(fp,0,SEEK_SET);
		fwrite(&fh,sizeof(file_hdr),1,fp);
	}
	//openmode 0:rb read 1:w+b 2:wb
	void dt_file::Open(const char *filename,int openmode,int fnid) {
		if(fnid==this->fnid && fp && this->openmode==openmode) return;
		if(fp) fclose(fp);
		fp=NULL;
		bk_lastrn=0;
		if(openmode==0) 
			fp=fopen(filename,"rb");
		else if(openmode==1)
			fp=fopen(filename,"w+b");
		else if(openmode==2) 
			fp=fopen(filename,"r+b");
		if(!fp) ThrowWith("Open file error! filename:%s,openmode:%d",
			filename,openmode);
		if(fnid==-1) this->fnid=0;
		else this->fnid=fnid;
		strcpy(this->filename,filename);
		this->openmode=openmode;
		lastoffset=0;
		//if(openmode==1) WriteHeader(fnid);
		//else 
		if(openmode!=1) {
			fseek(fp,0,SEEK_END);
			filesize=ftell(fp);//_filelength(_fileno(fp));
			fseek(fp,0,SEEK_SET);
			ReadHeader();
		}
		touchedblock=false;
	}
    
	int dt_file::WriteHeader(int mt,int rn,int fid,const char *nextfilename) {
		if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
		fseek(fp,0,SEEK_SET);
		fh.fileflag=FILEFLAG;
		fh.fnid=fnid=fid;
		fh.islastfile=1;
		fh.rownum=rn;
	    void *pcd=NULL;
		if(!mt) {
			fh.cdlen=fh.cdnum=fh.rowlen=0;
		}
		else {
			wociGetColumnDesc(mt,&pcd,fh.cdlen,fh.cdnum);
			fh.rowlen=wociGetRowLen(mt);
		}
		if(nextfilename) {
			fh.islastfile=false;
			strcpy(fh.nextfile,nextfilename);
		}
		else
		{
			fh.islastfile=true;
			fh.nextfile[0]=0;
		}
		if(fwrite(&fh,sizeof(file_hdr),1,fp)!=1 ||
			fwrite(pcd,1,fh.cdlen,fp)!=fh.cdlen) 
			ThrowWith("Write file header failed! filename:%s,fnid:%d",
			   filename,fnid);
		lastoffset=sizeof(file_hdr)+fh.cdlen;
		return lastoffset;
	}

	int dt_file::WriteBlock(char *bf,unsigned int len,int compress,bool packed) {
		if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
		if(packed) {
			if(fwrite(bf,1,len,fp)!=len)
				ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%d",
					filename,len,lastoffset);
			lastoffset+=len;
			if(fh.rowlen>0) fh.rownum+=(len-sizeof(block_hdr))/fh.rowlen;
			return lastoffset;
		}
		
		block_hdr bh;
		bh.blockflag=BLOCKFLAG;
		bh.compressflag=compress;
		bh.origlen=len;
		bh.storelen=len;
		char *dstbf=bf;
		if(compress>0) {
			unsigned int len2=len/2;
			if(cmprslen<len2) {
				if(cmprsbf) delete [] cmprsbf;
				cmprsbf= new char[len2];
				if(!cmprsbf) ThrowWith("MemAllocFailed on WriteBlock len:%d,len/3:%d",
					len,len2);
				cmprslen=len2;
			}
			int rt=0;
			uLong dstlen=len2;
//			mytimer tmr;
//			tmr.Start();
			/*******ZZLIB Compress ****
			if(compress==11) {
				dstlen = ZzCompressBlock((unsigned char *)bf, len, 0, 0 );
				dstbf=bf;
				if(dstlen<0) rt=dstlen;
			}
			else 
			/************************/
			if(compress==10) {
				unsigned int dstlen2=len2;
				rt=BZ2_bzBuffToBuffCompress(cmprsbf,&dstlen2,bf,len,1,0,0);
				dstlen=dstlen2;
				dstbf=cmprsbf;
				//tmr.Start();
				//for(int k=0;k<10;k++) 
				// BZ2_bzBuffToBuffDecompress(bf,&len,cmprsbf,dstlen2,0,0);
				//dstlen = ZzCompressBlock((unsigned char *)bf, len, 0, 0 );
				//dstbf=bf;
				
				//if(dstlen<0) rt=dstlen;
				//tmr.Start();
				//int dstlen2=0;
				//memcpy(cmprsbf,bf,dstlen);
				//for(int k=0;k<10;k++) {
					//memcpy(bf,cmprsbf,dstlen);
					//dstlen2=ZzUncompressBlock((unsigned char *)bf);
				//}
				//if(dstlen>=0) {
				//	lgprintf("compress data failed,skip compress");
				//	bh.compressflag=0;
				//}
				//dstlen = ZzCompressBlock((unsigned char *)bf, len, 1, 0 );
			}			
			/*****/

			/******* lzo compress ****/
			else if(compress==5) {
			//unsigned int dstlen2=len2;
				if(!pwrkmem)  {
					pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
					new char[LZO1X_MEM_COMPRESS+204800];
                                        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+204800);
                                }
				//long to int converting mayby error on BIGENDIEN diferrent host.
				//rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,(unsigned int *)&dstlen,pwrkmem);
				//for(int k=0;k<100;k++)
				  rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,(unsigned int *)&dstlen,pwrkmem);
				dstbf=cmprsbf;
				//tmr.Start();
				//for(int k=0;k<100;k++)
				//	r=lzo1x_decompress((unsigned char *)cmprsbf,dstlen2,(unsigned char*)bf,&len,pwrkmem);
				//dstlen=dstlen2;
				//if (r != LZO_E_OK) {
				//	lgprintf("compress data failed,skip compress");
				//	bh.compressflag=0;
				//}
			}
			/*****/
		    /*** zlib compress ***/
			//int zrt=0;
			else if(compress==1) {
			 rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
			 dstbf=cmprsbf;
			 //tmr.Start();
			 //for(int k=0;k<100;k++) {
			 //	uLong dstlen2=len;//pbh->origlen;
			 //	zrt=uncompress((Bytef *)bf,&dstlen2,(Bytef *)(cmprsbf),dstlen);
			 //}
			}
			else 
				ThrowWith("Invalid compress flag %d",compress);
		    if(rt!=Z_OK) {
				ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
			}
			/****/
			else {
				//dstbf=cmprsbf;
				bh.storelen=dstlen;
			}
			//tmr.Stop();
			//double tct=tmr.GetTime();
			//tct=0;
		}
		if(fwrite(&bh,sizeof(block_hdr),1,fp)!=1 ||
			fwrite(dstbf,1,bh.storelen,fp)!=bh.storelen)
			ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%d",
			   filename,len,lastoffset);
		lastoffset+=sizeof(block_hdr)+bh.storelen;
		if(fh.rowlen>0) fh.rownum+=len/fh.rowlen;
		return lastoffset;
	}

	int dt_file::WriteMt(int mt,int compress) {
		int bfl1=wociGetRowLen(mt)*wociGetMemtableRows(mt);
		if(bfl1>bflen || !blockbf) {
			bflen=bfl1;
			if(blockbf) delete[]blockbf;
			blockbf=new char[bfl1];
			if(!blockbf) ThrowWith("Memory allocation faild in WriteMt of dt_file,size:%d",bfl1);
		}
		wociExportSomeRows(mt,blockbf,0,wociGetMemtableRows(mt));
		return WriteBlock(blockbf,bfl1,compress);
	}

int dt_file::CreateMt(int maxrows)
{
	if(lastoffset!=sizeof(file_hdr) )
		ReadHeader();
	if(fh.cdlen<1) 
		ThrowWith("CreateMt on file '%s' which does not include column desc info",filename);
	char *pbf=new char[fh.cdlen];
	if(fread(pbf,1,fh.cdlen,fp)!=fh.cdlen) {
		delete [] pbf;
		ThrowWith("CreateMt read column desc info failed on file '%s'",
		   filename);
	}
	int mt=wociCreateMemTable();
	lastoffset=sizeof(file_hdr)+fh.cdlen;
	wociImport(mt,NULL,0,pbf,fh.cdnum,fh.cdlen,maxrows,0);
	delete []pbf;
	return mt;
}

int dt_file::ReadHeader()
{
	fseek(fp,0,SEEK_SET);
	if(fread(&fh,sizeof(file_hdr),1,fp)!=1) 
		ThrowWith("Read file header error on '%s'",filename);
	lastoffset=sizeof(file_hdr);
	oldoffset=lastoffset;
	touchedblock=false;
	fnid=fh.fnid;
	return sizeof(file_hdr);
}

 
int dt_file::ReadMt(int offset, int storesize,AutoMt &mt,int clearfirst,int _singlefile,char *poutbf,BOOL forceparal)
{
	bool mth=((offset==-1 || forceparal) && paral);
	singlefile=_singlefile;
	if(storesize<0) storesize=0;
	if(offset==0 || offset<-1) offset=sizeof(file_hdr)+fh.cdlen;
	if(offset==-1) offset=lastoffset;
	if(oldoffset==offset && touchedblock) return lastoffset;
	// Wait thread end and start another thread
	if(offset>=filesize) {
		int breakhere=0;
	}
	while(oldoffset!=offset || !touchedblock) {
		if(mth) {
			WaitEnd();
		}
		//Lock(1,INFINITE);
		//Unlock(1);
		
		bk_offset=offset;
		int rn=bk_lastrn;//pbh->origlen/fh.rowlen;
		if(rn<0) {
			return -1;
		}
		
		if(rn>0) {
			if(poutbf) {
				block_hdr *pbh=(block_hdr *)cmprsbf;
				memcpy(poutbf,pbh,sizeof(block_hdr));
				memcpy(poutbf+sizeof(block_hdr),bk_psrc,pbh->origlen);
			}
			else {
				if(clearfirst) {
					if(mt.GetMaxRows()<rn) {
						mt.SetMaxRows(rn*1.3);
						mt.Build();
					}
					wociReset(mt);
				}
				wociAppendRows(mt,bk_psrc,rn);
			}
			lastoffset=bk_offset+bk_storesize;
			oldoffset=bk_offset;
			touchedblock=true;
			lastrn=rn;
			bk_offset=lastoffset;
			bk_lastrn=0;
			if(!mth) continue;
		}
		
		// read first block
		bool exit=false;
		if(oldoffset==offset && touchedblock) exit=true;
		bk_lastoffset=lastoffset;
		bk_lastrn=0;
		bk_storesize=storesize;
		//连续文件检查
		if(bk_offset>=filesize) {
		 if(fh.islastfile==0 && singlefile!=1) {
			int oldfilesize=filesize;
			Open(fh.nextfile,0); 
			//file_mt的Open成员已含有cdes数据解析
			if(lastoffset==sizeof(file_hdr)) {
				/***********************/
				char *pbf=new char[fh.cdlen];
				if(fread(pbf,1,fh.cdlen,fp)!=fh.cdlen) {
					delete [] pbf;
					ThrowWith("CreateMt read column desc info failed on file '%s'",
						filename);
				}
				delete []pbf;
			}
			/*********************/
			bk_lastoffset=sizeof(file_hdr)+fh.cdlen;
			bk_offset=bk_lastoffset;
			if(offset==oldfilesize)
			 offset=lastoffset;
		} 
		else {
			if(exit) {
				if(mth) 
					End();
				return lastrn;
			}
			bk_lastrn=-1;
			return -1; // reach end of data.
		}
	}

		if(mth) {
			Start();
		}
		else {
			ReadMtThread();
		}
		if(exit) return lastrn;
	}
/*
	//continue next block;
	bk_lastoffset=lastoffset;
	bk_lastrn=0;
	bk_offset=offset;
	bk_storesize=storesize;
#ifdef __unix
	pthread_create(&hd_pthread_t,NULL,Handle_Read,(void *)this);
#else
	_beginthread(Handle_Read,0,(void *)this);
#endif
*/	
//	if(ReadMtThread()<0) return -1;
	return lastrn;
}

// System parameter container
	int SysParam::BuildSrcDBCFromSrcTableID(int srctabid) {
		int srctabp=wociSearchIK(dt_srctable,srctabid);
		if(srctabp<0) ThrowWith("BuildSrcDBCFromSrcTableID has a invalid srctabid:%d",
			srctabid);
		int srcid=dt_srctable.GetInt("srcsysid",srctabp);
		int srcidp=wociSearchIK(dt_srcsys,srcid);
		if(srcidp<0) ThrowWith("BuildSrcDBCFromSrcTableID has a invalid srcsysid in dt_srcsys table:srcid=%d"
			,srcid);
		//int tabid=dt_srctable.GetInt("tabid",srctabp);
		return BuildSrcDBC(srcidp);
	};

	int SysParam::GetSoledInexParam(int srctabid,dumpparam *dp,int tabid) {
		int srctabp=wociSearchIK(dt_srctable,srctabid);
		if(tabid==-1)
                   tabid=dt_srctable.GetInt("tabid",srctabp);
		int tabp=wociSearchIK(dt_table,tabid);
		AutoMt idxmt(dts);
		idxmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1 order by indexid",
			tabid);
		idxmt.Wait();
		int rn=idxmt.Wait();
		if(rn<1) ThrowWith("Cound not found primary index of tabid:%d",tabid);
		dp->rowlen=dt_table.GetInt("recordlen",tabp);
		AutoMt cdf(dts);
		for(int i=0;i<rn;i++) {
			cdf.Clear();
			cdf.FetchAll("select * from dt_cdfilemap where fileid=%d",
			 idxmt.GetInt("cdfileid",i));
			int rn1=cdf.Wait();
		        if(rn1<1) ThrowWith("Index column desc file not found in dt_cdfilemap,cdfileid:%d",idxmt.GetInt("cdfileid",i));
			strcpy(dp->idxp[i].cdsfilename,cdf.PtrStr("filename",0));
			dp->idxp[i].cdsfs=cdf.GetInt("filesize",0);
			dp->idxp[i].colnum=cdf.GetInt("columnnum",0);
			strcpy(dp->idxp[i].idxcolsname,idxmt.PtrStr("columnsname",i));
			strcpy(dp->idxp[i].idxreusecols,idxmt.PtrStr("reusecols",i));
			dp->idxp[i].idxid=idxmt.GetInt("indexid",i);
			strcpy(dp->idxp[i].idxtbname,idxmt.PtrStr("indextabname",i));
			dp->idxp[i].idinidx=idxmt.GetInt("indexidinidxtab",i);
			dp->idxp[i].idindat=idxmt.GetInt("indexidindattab",i);
			if(idxmt.GetInt("issoledpindex",i)==1) 
				dp->psoledindex=i;
		}
		dp->tmppathid[0]=dt_srctable.GetInt("mddatafilepathid1",srctabp);
		dp->tmppathid[1]=dt_srctable.GetInt("mddatafilepathid2",srctabp);
		strcpy(dp->tmppath[0],GetPathName(dp->tmppathid[0]));
		strcpy(dp->tmppath[1],GetPathName(dp->tmppathid[1]));
		dp->dstpathid[0]=dt_srctable.GetInt("dstdatafilepathid1",srctabp);
		dp->dstpathid[1]=dt_srctable.GetInt("dstdatafilepathid2",srctabp);
		strcpy(dp->dstpath[0],GetPathName(dp->dstpathid[0]));
		strcpy(dp->dstpath[1],GetPathName(dp->dstpathid[1]));
		dp->soledindexnum=rn;
		dp->srctabid=srctabid;
		dp->maxrecnum=dt_srctable.GetInt("mdfilemaxrecnum",srctabp);
		dp->tabid=tabid;//dt_srctable.GetInt("tabid",srctabp);
		return rn;
	}


	int SysParam::GetFirstTaskID(TASKSTATUS ts,int &srctabid,int &datasetid) {
		AutoMt tasksch(dts);
		tasksch.FetchAll("select * from dt_taskschedule where begintime<sysdate and taskstatus=%d order by begintime",ts);
		int rn=tasksch.Wait();
		if(rn<1) return 0;
		srctabid=tasksch.GetInt("srctabid",0);
		datasetid=tasksch.GetInt("datasetid",0);
		return tasksch.GetInt("taskid",0);
	};

	int SysParam::UpdateTaskStatus(TASKSTATUS ts,int taskid) {
		AutoStmt tasksch(dts);
		tasksch.Prepare("update dt_taskschedule set taskstatus=%d ,lastlaunchtime=launchtime,launchtime=:l1 where taskid=%d and taskstatus<>%d",
			ts,taskid,ts);
		char dt[10];
		wociGetCurDateTime(dt);
		tasksch.BindDate(1,dt);
		tasksch.Execute(1);
		tasksch.Wait();
		int rn=wociGetFetchedRows(tasksch);
		if(rn<1) 
			ThrowWith("任务状态修改为%d时失败，可能是与其它进程冲突。\n"
				" 任务标号：%d.",ts,taskid);
		wociCommit(dts);
		return rn;
	};

	int SysParam::GetDumpSQL(int taskid,int partoffset,char *sqlresult) {
		AutoMt tasksch(dts);
		tasksch.FetchAll("select * from dt_taskschedule where taskid=%d",taskid);
		int rn=tasksch.Wait();
		if(rn<1) ThrowWith("Invalid taskid in GetDumpSQL ,taskid:%d",taskid);
		int partonly=tasksch.GetInt("onlypartid",0);
		int partid=tasksch.GetInt("partid",0);
		AutoMt srcpart(dts);
		int partn=0;
		if(partonly) {
		 srcpart.FetchAll("select * from dt_srcpartinfo where partid=%d",
			partid);
		 partn=srcpart.Wait();
		}
		else {
		 srcpart.FetchAll("select * from dt_srcpartinfo where srctabid=%d order by partid",
			tasksch.GetInt("srctabid",0)); 
		 partn=srcpart.Wait();
		}
		if(partn<1 && partonly) 
			ThrowWith("Partition information of taskid:%d could not found.",taskid);
		if(partoffset>=partn && partoffset) //if partonly and read once more , return -1
			return -1; //no more partitoin .
		int srctabp=wociSearchIK(dt_srctable,tasksch.GetInt("srctabid",0));
		if(srctabp<0) ThrowWith("BuildSrcDBCFromTaskID has a invalid srctabid where taskid=%d",
			tasksch.GetInt("taskid",0));
		sprintf(sqlresult,"select * from %s.%s ",
			dt_srctable.PtrStr("srcschedulename",srctabp),
			dt_srctable.PtrStr("srctabname",srctabp));
		if(partn) {
			char *cols=srcpart.PtrStr("columnsname",partoffset);
			char *bvals=srcpart.PtrStr("beginvalues",partoffset);
			char *evals=srcpart.PtrStr("endvalues",partoffset);
			BuildWhere(cols,bvals,evals,sqlresult+strlen(sqlresult));
			partid=srcpart.GetInt("partid",partoffset);
		}
		char *pmc=tasksch.PtrStr("morecondition",0);
		if(strlen(pmc)) 
			sprintf(sqlresult+strlen(sqlresult)," %s %s",partn?" and ":" where ",pmc);
		return partid;
	}

	const char *SysParam::GetPathName(int pathid,char *pathtype)
	{
		const char *rt=internalGetPathName(pathid,pathtype);
		if(!rt) ThrowWith("Could not found path value of column describe in dt_path table");
		if(xmkdir(rt)) ThrowWith("Path '%s' could create or change to\n",rt);
		return rt;
	}
	
	const char * SysParam::internalGetPathName(int pathid, char *pathtype)
	{
		if(pathtype==NULL) {
			int p=wociSearchIK(dt_path,pathid);
			if(p<0) return NULL;
			return dt_path.PtrStr("pathval",p);
		}
		int rn=wociGetMemtableRows(dt_path);
		int p=dt_path.GetPos("pathtype",COLUMN_TYPE_CHAR);
		for(int i=0;i<rn;i++) {
			if(STRICMP(dt_path.PtrStr(p,i),pathtype)==0) {
				return dt_path.PtrStr("pathval",i);
			}
		}
		return NULL;
	}

	int SysParam::NextFileID() {
		wociSetEcho(FALSE);
		AutoMt seq(dts,10);
		seq.FetchAll("select dt_fileid.nextval as fid from dual");
		seq.Wait();
		wociSetEcho(TRUE);
		return seq.GetInt("fid",0);
	}
	
	int SysParam::NextDatasetID() {
		AutoMt seq(dts,10);
		seq.FetchAll("select dt_datasetid.nextval as dsid from dual");
		seq.Wait();
		return seq.GetInt("dsid",0);
	}

	void SysParam::Reload()
	{
	 dt_path.Clear();
	 dt_path.FetchAll("select * from dt_path");
	 dt_path.Wait();
	 wociSetIKByName(dt_path,"pathid");
	 wociOrderByIK(dt_path);
/*****************************/

	 dt_srctable.Clear();
	 dt_srctable.FetchAll("select * from dt_srctable");
	 dt_srctable.Wait();
	 wociSetIKByName(dt_srctable,"srctabid");
	 wociOrderByIK(dt_srctable);
	 wociSetSortColumn(dt_srctable,"tabid");
	 wociSort(dt_srctable);
     
/*****************************/
 	 dt_srcsys.Clear();
	 dt_srcsys.FetchAll("select * from dt_srcsys");
	 dt_srcsys.Wait();
/*****************************/
	 wociSetIKByName(dt_srcsys,"sysid");
	 wociOrderByIK(dt_srcsys);

	 dt_table.Clear();
	 dt_table.FetchAll("select * from dt_table order by tabid");
	 dt_table.Wait();
	 wociSetIKByName(dt_table,"tabid");
	 wociOrderByIK(dt_table);
	 wociSetSortColumn(dt_table,"tabname");
	 wociSort(dt_table);

	 dt_index.Clear();
	 dt_index.FetchAll("select * from dt_index order by indexid");
	 dt_index.Wait();
	 wociSetIKByName(dt_index,"indexid");
	 wociOrderByIK(dt_index);
	 wociSetSortColumn(dt_index,"tabid");
	 wociSort(dt_index);
/*****************************/
	}
	
	int SysParam::GetSrcTabid(int tabid,int &p) {
		void *ptr[2];
		ptr[0]=&tabid;
		ptr[1]=NULL;
		p=wociSearch(dt_srctable,ptr);
		if(p<0) ThrowWith("get srctabid from srctable with tabid error,tabid:%d",tabid);
		return dt_srctable.GetInt("srctabid",p);
	}
	bool SysParam::GetBlankTable(int &tabid)
	{
		int rows=wociGetMemtableRows(dt_table);
		int cdpos=dt_table.GetPos("cdfileid",COLUMN_TYPE_INT);
		for(int i=0;i<rows;i++) {
			if(*dt_table.PtrInt(cdpos,i)==0) {
				tabid=dt_table.GetInt("tabid",i);
				return true;
			}
		}
		return false;
	}


	void SysParam::GetSrcSys(int sysid,char *sysname,char *svcname,char *username,char *pwd)
	{
		int res=wociSearchIK(dt_srcsys,sysid);
		if(res<0) ThrowWith("srcsys get error,sysid:%d",sysid);
		dt_srcsys.GetStr("sysname",res,sysname);
		dt_srcsys.GetStr("svcname",res,svcname);
		dt_srcsys.GetStr("username",res,username);
		dt_srcsys.GetStr("pswd",res,pwd);
	}


int SysParam::BuildSrcDBC(int srcidp)
{
	char *pwd=dt_srcsys.PtrStr("pswd",srcidp);
	char pswd[200];
	if(!*pwd) {
		printf("SYSID:%d\nSVCNAME:%s\nUSERNAME:%s\n Input password:",
			dt_srcsys.GetInt("sysid",srcidp),dt_srcsys.PtrStr("svcname",srcidp),
			dt_srcsys.PtrStr("username",srcidp));
		scanf("%s",pswd);
		pwd=pswd;
	}
	return wociCreateSession(dt_srcsys.PtrStr("username",srcidp),
			 pwd,dt_srcsys.PtrStr("svcname",srcidp));
}

int SysParam::GetMiddleFileSet(int procstate)
{
	AutoMt mdfile(dts);
	mdfile.FetchAll("select * from dt_middledatafile where procstate=%d and rownum<2 ",procstate);
	int rn=mdfile.Wait();
	if(rn<1) return 0;
	int mt;
	AutoStmt st(dts);
	st.Prepare("select * from dt_middledatefile where subdataset=%d order by mdfid",mdfile.GetInt("subdatasetid",0));
	mt=wociCreateMemTable();
	wociBuildStmt(mt,st,500);
	wociFetchAll(mt);
	wociWaitLastReturn(mt);
	return mt;
}


int SysParam::GetMaxBlockRn(int tabid)
{
		int p=wociSearchIK(dt_table,tabid);
		if(p<0) ThrowWith("get maxrecinblock from table with tabid error,tabid:%d",tabid);
		return dt_table.GetInt("maxrecinblock",p);
}

int SysParam::GetMaxBlockRnFromSrcTab(int srctabid)
{
		int p=wociSearchIK(dt_srctable,srctabid);
		if(p<0) ThrowWith("get maxrecinblock from srctable with srctabid error,tabid:%d",srctabid);
		return GetMaxBlockRn(dt_srctable.GetInt("tabid",p));
}

thread_rt DT_CreateInstance(void *p) {
	SvrAdmin::CreateInstance();
	thread_end;
}

void SvrAdmin::CreateInstance()
{
	if(!pdts) return ;
	SvrAdmin *_psa=new SvrAdmin(*pdts);
	_psa->Reload();
	psa=_psa;
	SetSvrStarted();
}

///* Database server extendted interface
	SvrAdmin *SvrAdmin::GetInstance() {
		if(psa==NULL && shutdown==0) {
			//if(pdts==NULL || !svrstarted) return NULL;
			void *pval=NULL;
			psa=(SvrAdmin *)0x0001;
#ifdef __unix
			pthread_create(&hd_pthread_t,NULL,DT_CreateInstance,(void *)pval);
			pthread_detach(hd_pthread_t);
#else
			_beginthread(DT_CreateInstance,81920,(void *)pval);
#endif
			//psa=new SvrAdmin(*pdts);
			//psa->Reload();
		}
		
		return svrstarted?(psa==(SvrAdmin *)0x1?NULL:psa):NULL;
	}
	void SvrAdmin::ReleaseInstance() {
		if(psa!=NULL && psa!=(void *)0x1) {
			delete psa;
			psa=NULL;
		}
	}
	int SvrAdmin::CreateDTS(const char *svcname,const char *usrnm,const char *pswd) {
		if(pdts==NULL) {
			pdts=new AutoHandle;
			pdts->SetHandle(wociCreateSession(usrnm,pswd,svcname));
		}
		return *pdts; 
	}
	
	void SvrAdmin::ReleaseDTS() {
		if(pdts) 
			delete pdts;
		pdts=NULL;
	}
	
	int SvrAdmin::GetIndexStruct(int p) {
		int mt=wociCreateMemTable();
		wociAddColumn(mt,"idxidintab",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"idxidinidx",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"soledflag",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"tableoff",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"indexid",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"tabname",NULL,COLUMN_TYPE_CHAR,300,0);
		wociAddColumn(mt,"reuseindexid",NULL,COLUMN_TYPE_INT,300,0);
		wociAddColumn(mt,"idxfieldnum",NULL,COLUMN_TYPE_INT,0,0);
		wociBuild(mt,100);
		int tabid=dsttable.GetInt("tabid",p);
		int *ptab=dt_index.PtrInt("tabid",0);
		int *pidxtb=dt_index.PtrInt("indexidindattab",0);
		int	*pidxidx=dt_index.PtrInt("indexidinidxtab",0);
		int *pissoled=dt_index.PtrInt("issoledindex",0);
		int *pindexid=dt_index.PtrInt("indexid",0);
		int idxtabnamep=dt_index.GetPos("indextabname",COLUMN_TYPE_CHAR);
		int rn=wociGetMemtableRows(dt_index);
		int solect=0;
		int i;
		for( i=0;i<rn;i++ ) {
			if(ptab[i]!=tabid) continue;
			else {
				void *ptr[20];
				ptr[0]=pidxtb+i;
				ptr[1]=pidxidx+i;
				ptr[2]=pissoled+i;
				int soledflag=-1;
				if(pissoled[i]==1) soledflag=solect++;
				ptr[3]=&soledflag;
				ptr[4]=pindexid+i;
				ptr[5]=dt_index.PtrStr(idxtabnamep,i);
				ptr[6]=dt_index.PtrInt("reuseindexid",i);
				ptr[7]=dt_index.PtrInt("idxfieldnum",i);
				ptr[8]=NULL;
				wociInsertRows(mt,ptr,NULL,1);
			}
		}
		int totidxnum=GetTotIndexNum(p);
		//int *pissole=NULL;
		int *ptableoff=NULL;
		int *pidxid=NULL;
		int *preuseid;
		//wociGetIntAddrByName(mt,"soledflag",0,&pissole);
		wociGetIntAddrByName(mt,"tableoff",0,&ptableoff);
		wociGetIntAddrByName(mt,"reuseindexid",0,&preuseid);
		wociGetIntAddrByName(mt,"indexid",0,&pidxid);
		for(i=0;i<totidxnum;i++) {
			if(preuseid[i]>0) {
				for(int j=0;j<totidxnum;j++) {
					if(pidxid[j]==preuseid[i]) {
						ptableoff[i]=ptableoff[j];
						break;
					}
				}
			}
		}
		return mt;
	}

	int SvrAdmin::Search(const char *pathval) {
		void *ptr[2];
		ptr[0]=(void *)pathval;
		ptr[1]=NULL;
		return wociSearch(dsttable,ptr);
	}
 
	const char *SvrAdmin::GetFileName(int fileid) {
		int i=wociSearchIK(filemap,fileid);
		if(i<0) {
			lgprintf("Invalid file id :%d",fileid);
			return NULL;
		}
		return filemap.PtrStr(filenamep,i);
	}

	void SvrAdmin::Reload() {
		SysParam::Reload();
		filemap.FetchAll("select * from dt_datafilemap");
		filemap.Wait();
		wociSetIKByName(filemap,"fileid");
		wociOrderByIK(filemap);
		filenamep=filemap.GetPos("filename",COLUMN_TYPE_CHAR);

		wociClear(dsttable);
		wociAddColumn(dsttable,"tabid",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"soledindexnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"totindexnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"recordnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"pathval",NULL,COLUMN_TYPE_CHAR,300,0);
		wociAddColumn(dsttable,"firstfile",NULL,COLUMN_TYPE_CHAR,300,0);
		wociAddColumn(dsttable,"databasename",NULL,COLUMN_TYPE_CHAR,300,0);
		//wociAddColumn(dsttable,"colnum",NULL,COLUMN_TYPE_INT,0,0);
		int rn=wociGetMemtableRows(dt_table);
		dsttable.SetMaxRows(rn);
		dsttable.Build();
		for(int i=0;i<rn;i++) {
			int recnum=dt_table.GetInt("recordnum",i);
			if(recnum>0) {
				void *ptr[20];
				char pathval[300];
				ptr[0]=dt_table.PtrInt("tabid",i);
				ptr[1]=dt_table.PtrInt("soledindexnum",i);
				ptr[2]=dt_table.PtrInt("totindexnum",i);
				sprintf(pathval,"./%s/%s",dt_table.PtrStr("databasename",i),
					dt_table.PtrStr("tabname",i));
				ptr[3]=&recnum;
				ptr[4]=pathval;
				ptr[5]=(void *)GetFileName(dt_table.GetInt("firstdatafileid",i));
				ptr[6]=dt_table.PtrStr("databasename",i);
				if(ptr[5]==NULL) ptr[5]="";//continue;
				ptr[7]=NULL;
				wociInsertRows(dsttable,ptr,NULL,1);
			}
		}
		wociSetSortColumn(dsttable,"pathval");
		wociSort(dsttable);
	}

const char * SvrAdmin::GetDbName(int p)
{
	return dsttable.PtrStr(6,p);
}

int dt_file::WaitStart(int tm)
{
	#ifdef __unix
	int rt=pthread_mutex_lock(&hd_start);
	#else
	DWORD rt=WAIT_OBJECT_0;
	rt=WaitForSingleObject(hd_start,tm);
	#endif
	if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
		#ifndef __unix
		rt=GetLastError();
		#endif
		ThrowWith("Waitstart failed ,code:%d",rt);
	}
	return rt;
}


int dt_file::ReadMtThread()
{
	//Lock(1);// Ready

	bk_lastoffset=lastoffset;
	if(bk_lastoffset!=bk_offset) {
		bk_lastoffset=bk_offset;
		fseek(fp,bk_offset,SEEK_SET);
	}
	/*
	if(bk_offset>=filesize) {
		if(fh.islastfile==0 && singlefile!=1) {
			Open(fh.nextfile,0); 
			//file_mt的Open成员已含有cdes数据解析
			if(lastoffset==sizeof(file_hdr)) {
				/***********************
				char *pbf=new char[fh.cdlen];
				if(fread(pbf,1,fh.cdlen,fp)!=fh.cdlen) {
					delete [] pbf;
					ThrowWith("CreateMt read column desc info failed on file '%s'",
						filename);
				}
				delete []pbf;
			}
			/*********************
			bk_lastoffset=sizeof(file_hdr)+fh.cdlen;
			bk_offset=bk_lastoffset;
			if(bk_offset>filesize) {
				int stophere=1;
			}

		}
		else {
			if(dbgct<1200000) {
				int stophere=1;
			}
			bk_lastrn=-1;
			return -1; // reach end of data.
		}
	}
	*/
	if(bk_storesize==0) {
		block_hdr bh;
		if(fread(&bh,sizeof(block_hdr),1,fp)!=1)
		  ThrowWith("File read failed on '%s',offset:%d,size:%d",
			filename,bk_offset,bk_storesize);
		if(bh.blockflag!=BLOCKFLAG) 
		 ThrowWith("Invalid block flag on '%s' ,offset :%d",
			filename,bk_offset);
		bk_storesize=bh.storelen+sizeof(block_hdr);
		if(cmprslen<bk_storesize) {
			if(cmprsbf) delete []cmprsbf;
			cmprsbf=new char[int(bk_storesize*1.3)];
			cmprslen=int(bk_storesize*1.3);
		}
		memcpy(cmprsbf,&bh,sizeof(block_hdr));
		if(fread(cmprsbf+sizeof(block_hdr),1,bh.storelen,fp)!=bh.storelen)
		  ThrowWith("File read failed on '%s',offset:%d,size:%d",
			filename,bk_offset,bk_storesize);
	}
	else {
		if(cmprslen<bk_storesize) {
			if(cmprsbf) delete []cmprsbf;
			cmprsbf=new char[int(bk_storesize*1.3)];
			cmprslen=int(bk_storesize*1.3);
		}
		if(fread(cmprsbf,1,bk_storesize,fp)!=bk_storesize) {
			ThrowWith("File read failed on '%s',offset:%d,size:%d",
				filename,bk_offset,bk_storesize);
		}
	}
	block_hdr *pbh=(block_hdr *)cmprsbf;
	if(pbh->blockflag!=BLOCKFLAG) 
		ThrowWith("Invalid block flag on '%s' ,offset :%d",
			filename,bk_offset);
	bk_psrc=cmprsbf+sizeof(block_hdr);

	if(pbh->compressflag!=0) {
			int rt=0;
			if(bflen<pbh->origlen) {
				if(blockbf) delete []blockbf;
				blockbf=new char[int(pbh->origlen*1.2)];
				bflen=int(pbh->origlen*1.2);
			}
			uLong dstlen=bflen;//pbh->origlen;
			//zzlib
		    /***************************
			if(pbh->compressflag==11) {
				memcpy(blockbf,bk_psrc,pbh->storelen);
				dstlen = ZzUncompressBlock((unsigned char *)bk_psrc);
				if(dstlen<0) rt=dstlen;
			}
			else 
			/****************************/
			//bzlib2
			if(pbh->compressflag==10) {
				unsigned int dstlen2=dstlen;
				rt=BZ2_bzBuffToBuffDecompress(blockbf,&dstlen2,bk_psrc,pbh->storelen,0,0);
				dstlen=dstlen2;
				bk_psrc=blockbf;
			}			
			/*****/

			/******* lzo compress ****/
			else if(pbh->compressflag==5) {
				unsigned int dstlen2=dstlen;
				rt=lzo1x_decompress((unsigned char*)bk_psrc,pbh->storelen,(unsigned char *)blockbf,&dstlen2,NULL);
				dstlen=dstlen2;
				bk_psrc=blockbf;
			}
			/*****/
		    /*** zlib compress ***/
			else if(pbh->compressflag==1) {
			 rt=uncompress((Bytef *)blockbf,&dstlen,(Bytef *)bk_psrc,pbh->storelen);
			 bk_psrc=blockbf;
			}
			else 
				ThrowWith("Invalid uncompress flag %d",pbh->compressflag);
		    if(rt!=Z_OK) {
				ThrowWith("Decompress failed,datalen:%d,compress flag%d,errcode:%d",
					pbh->storelen,pbh->compressflag,rt);
			}
	}
	int rn=pbh->origlen/fh.rowlen;
	bk_lastrn=rn;
	return rn;
}

int dt_file::WaitEnd(int tm)
{
	#ifdef __unix
	int rt=pthread_mutex_lock(&hd_end);
	#else
	DWORD rt=WAIT_OBJECT_0;
	rt=WaitForSingleObject(hd_end,tm);
	#endif
	if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
		#ifndef __unix
		rt=GetLastError();
		#endif
		ThrowWith("Waitend failed ,code:%d",rt);
	}
	return rt;
}

void dt_file::Start()
{
	if(paral) {
#ifdef __unix
	 pthread_mutex_unlock(&hd_start);
#else
	SetEvent(hd_start);
#endif
	}
}

void dt_file::End()
{
	if(paral) {
#ifdef __unix
	 pthread_mutex_unlock(&hd_end);
#else
	SetEvent(hd_end);
#endif
	}
}
