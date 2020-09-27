#include "dtio.h"
#ifndef WIN32
#include "zlib/zlib.h"
#endif
extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);
DTIOExport int8B DEFAULT_SPLITLEN=((int8B)4500*1024*1024);
void dtioStreamFile::OpenFile(int id,bool forwrite) {
	if(fp!=NULL) fclose(fp);
	fp=NULL;
	char fn[400];
	if(streamlocation[strlen(streamlocation)-1]!=PATH_SEPCHAR) 
		strcat(streamlocation,PATH_SEP);
	if(id!=0)
	sprintf(fn,"%s%s.%04d",streamlocation,streamname,id);
	else sprintf(fn,"%s%s",streamlocation,streamname);
	if(forwrite) {
		//如果文件为只读模式,先删除,再建立
		fp=fopen(fn,"rb");
		if(fp) {
			fseek(fp,0,SEEK_END);
			long l=ftell(fp);
			fclose(fp);
			fp=NULL;
			if(GetStreamType()==FULL_BACKUP && l!=0)
			 ThrowWith("文件'%s'已存在,不能继续！",fn);
#ifdef WIN32
			_chmod(fn,_S_IREAD | _S_IWRITE );
			_unlink(fn);
#else
			unlink(fn);
			chmod(fn,S_IRUSR|S_IRGRP|S_IROTH);
#endif
		}
		fp=fopen(fn,"w+b");
	}
	else fp=fopen(fn,"rb");
	if(fp==NULL) 
	{ 
		#ifndef MYSQL_SERVER
		char prompt[300];
		if(!forwrite) {
		  sprintf(prompt,"打开文件 '%s' 错误，检查路径和文件名是否正确!\n重试(Y/N)<yes>:",fn);
			while(GetYesNo(prompt,true)) {
			 getString("目标路径及文件名:",fn,fn);
			 fp=fopen(fn,"rb");
			 if(fp!=NULL) break;
			 sprintf(prompt,"打开文件 '%s' 错误，检查路径和文件名是否正确!\n重试(Y/N)<yes>:",fn);
			}
		}
		#endif	
		if(fp==NULL)	
		ThrowWith("%s '%s' 错误，检查路径和文件名是否正确!",forwrite?"创建文件":"打开文件 ",fn);
	}
	if(!forwrite) {
      		if(id==0) { 
			//fread(&dtiohdr,1,sizeof(dtioHeader),fp);
			//curoffset=sizeof(dtioHeader);
			//if(dtiohdr.flag!=DTIO_STREAMFLAG) 
			//	ThrowWith("Not a valid file :'%s'!",fn);
			//if(dtiohdr.version!=DTIO_VERSION)
			//	ThrowWith("File '%s' has a unrecognized verion:%0x!",fn,dtiohdr.version);
			offset=0;curoffset=0;//初始化工作在WriteInit中完成
		}
		else {
			curoffset=0;
			Get(hdrcont+id,sizeof(dtioHeaderCont));
			hdrcont[id].ReverseByteOrder();
			//fread(hdrcont+id,1,sizeof(dtioHeaderCont),fp);
			curoffset=sizeof(dtioHeaderCont);
			offset+=sizeof(dtioHeaderCont);
			if(hdrcont[id].flag!=CONTFILEFLAG) 
				ThrowWith("文件格式错误 :'%s'!",fn);
			if(hdrcont[id].version!=GetVersion())
				ThrowWith("文件 '%s' 版本不可识别 :%0x--%0x!",fn,hdrcont[id].version,dtiohdr.version);
			if(hdrcont[id].exportid!=dtiohdr.exportid)
				ThrowWith("文件 '%s' 不是同一组!",fn,dtiohdr.version);
		}
	}
 	else {
 		if(id!=0) {
 			curoffset=0;
 			if(write) hdrcont[id].ReverseByteOrder();
 			Put(hdrcont+id,sizeof(dtioHeaderCont),true);
 			if(write) hdrcont[id].ReverseByteOrder();
 		}
 		else {curoffset=0;offset=0;}//WriteInit will initialize.
 	}
}

dtioStreamFile::dtioStreamFile(const char *_basedir):dtioStream(_basedir) {
	fp=NULL;
}

int dtioStreamFile::SetStreamName(const char *fname) {
		  char realname[PATH_LEN];
		  realname[0]=0;
		  bool created=false;
  		  FILE *fptmp=fopen(fname,"rb");
		  if(fptmp==NULL) {
  		    fptmp=fopen(fname,"w+b");
		    if(fptmp==NULL)
				ThrowWith("路径错误或权限不足,不能建立 '%s' .",fname);
 		    fclose(fptmp);
 		    //AIX操作系统要求存在文件，才能调用realpath. 因此不能删除
    		    //unlink(fname);
    		    created=true;
		  }
		  else fclose(fptmp);
		  realpath(fname,realname);
		  if(realname[0]==0)
			ThrowWith("Stream name '%s' is invalid on SetStreamName.",fname);
		  if(created) unlink(fname);
#ifdef WIN32
		  char *dirc = _strdup(realname);
		  char *basec = _strdup(realname);
#else
		  char *dirc = strdup(realname);
		  char *basec = strdup(realname);
#endif
		  char *dname,*bname;
		  dname = dirname(dirc);
		  bname = basename(basec);
/*
		  //Cut tailer '.000' as basefilename
		  int l=strlen(bname);
		  if(l>5) {
		  	if(strcmp(bname+l-4,".000")==0)
		  	  bname[l-4]=0;
		  }
*/		  dtioStream::SetStream(bname,"file",dname);
		  strcpy(dtiohdr.basefilename,bname);
		  free(dirc);
		  free(basec); 
		  return 0;
}

uint8B dtioStreamFile::Put(void *bfv,uint8B len,bool splitable) {
		char *bf=(char *)bfv;
		if(curoffset+len>splitlen) {
			  if(!splitable) ThrowWith("Write a unsplitable block exceeds maximun length,stream len :%ld,"
				  " block len :%ld,splite count:%d.",curoffset,len,splitedct);
			  uint4 filllen=splitlen-curoffset;
			  if(write && dp_fwrite(bf,1,filllen,fp)!=filllen) 
				  ThrowWith("Write a block error,basename:%d,location:%s,contid:%d,curoffset :%ld,length:%d."
				  ,streamname,streamlocation,cursplitid,curoffset,len);
	  		  if(write) crcv=crc32(crcv,(const Bytef *)bf,(uInt)filllen);
			  bf+=filllen;
			  len-=filllen;
			  hdrcont[splitedct].filelength=splitlen;
			  dtiohdr.filelength[splitedct]=splitlen;
			  
			  dtioHeaderCont *hdr=hdrcont+ ++splitedct;
			  hdr->internalfileid=splitedct;
			  dtiohdr.dtiocontct=splitedct;
			  hdr->exportid=dtiohdr.exportid;
			  cursplitid=splitedct;
			  
			  dtiohdr.imagesize+=filllen;
			  curoffset+=filllen;
		  	  offset+=filllen;
	  		  
	  		  if(fp) fclose(fp);
			  fp=NULL;
		  }
		  if(!fp) {
			  OpenFile(splitedct,true);
			  /*
			  if(write) {
				if(splitedct>0)
					  dp_fwrite(&hdrcont[splitedct],1,sizeof(dtioHeaderCont),fp);
				else	  dp_fwrite(&dtiohdr,1,sizeof(dtioHeader),fp);
			  }
			  if(splitedct)
				  curoffset=sizeof(dtioHeaderCont);
			  else
				  curoffset=sizeof(dtioHeader);
				  */
		  }
		  if(curoffset+len>splitlen) 
		     return Put(bf,len,splitable);
		  if(write && dp_fwrite(bf,1,len,fp)!=len) 
			  ThrowWith("Write a block error,basename:%d,location:%s,contid:%d,curoffset :%ld,length:%d.",
			  streamname,streamlocation,cursplitid,curoffset,len);
	  	  if(write) crcv=crc32(crcv,(const Bytef *)bf,(uInt)len);
		  dtiohdr.imagesize+=len;
		  curoffset+=len;
		  offset+=len;
		  hdrcont[splitedct].filelength=curoffset;
		  dtiohdr.filelength[splitedct]=curoffset;
		  if(curoffset>splitlen)
		  	ThrowWith("Invalid  current offset of stream:%lld,should limit to %lld",curoffset,splitlen);
		  return offset;
}

uint8B dtioStreamFile::SeekAt(uint8B _offset)
{
	int i=0;
	uint8B startat=0;
	if(_offset>0) {
		if(dtiohdr.imagesize<_offset) 
			ThrowWith("seek on invalid dtio stream ,name '%s',stream len %ld,seet at:%ld."
				,streamname,dtiohdr.imagesize,_offset);
		for(i=0;i<=dtiohdr.dtiocontct;i++) {
      			if(_offset<startat+dtiohdr.filelength[i] || dtiohdr.filelength[i]==0) break;
				startat+=dtiohdr.filelength[i];
		}
		if(i>dtiohdr.dtiocontct ) 
			ThrowWith("seek on invalid dtio stream :invalid file head ,name '%s',stream len %ld,seek at:%ld.",
			streamname,dtiohdr.imagesize,_offset);
		if(cursplitid!=i) {
			cursplitid=i;
			if(fp) fclose(fp);
			fp=NULL;
		}
	}
	if(!fp) {
		OpenFile(cursplitid,false);
	}
	offset=_offset;
	_offset-=startat;
	if(curoffset!=_offset) {
		fseeko(fp,_offset,SEEK_SET);
#ifdef WIN32
		fflush(fp);
#endif

	}
	curoffset=_offset;
	return offset;
}

uint8B dtioStreamFile::SeekFrom(uint8B from,uint8B _offset)
{
	int i=0;
	uint8B startat=0;
	uint8B adj=0;
	if(dtiohdr.filelimit<from+_offset) {
		for(i=0;i<=dtiohdr.dtiocontct;i++) {
      			if(from<startat+dtiohdr.filelength[i] || dtiohdr.filelength[i]==0) break;
				startat+=dtiohdr.filelength[i];
		}
		for(;i<=dtiohdr.dtiocontct;i++) {
      			if(from+_offset<startat+dtiohdr.filelength[i] || dtiohdr.filelength[i]==0) break;
      			adj+=sizeof(dtioHeaderCont);
      			startat+=dtiohdr.filelength[i];
		}
	}	
	return SeekAt(from+_offset+adj);
}

uint8B dtioStreamFile::Get(void *bfv,uint8B len) {
	char *bf=(char *)bfv;
	if(fp==NULL) OpenFile(cursplitid,false);
	uint8B clen=dtiohdr.filelimit;
	if(curoffset+len>clen && clen>0) {
      		uint8B rdd=clen-curoffset;
			if(fread(bf,1,rdd,fp)!=rdd)
				ThrowWith("read file error,name '%s',splitid %d,offset %ld,want length:%ld."
				,streamname,cursplitid,curoffset,rdd);
			crcv=crc32(crcv,(const Bytef *)bf,(uInt)rdd);
			len-=rdd;
			bf+=rdd;
			curoffset+=rdd;
			offset+=rdd;
			SeekAt(	getStartOffset(cursplitid+1)+sizeof(dtioHeaderCont));
			//备份时,跨文件分拆的数据块的长度实际含续接文件头?
			//len-=sizeof(dtioHeaderCont);
	}
	if(fp==NULL) OpenFile(cursplitid,false);
   	if(fread(bf,1,len,fp)!=len)
		ThrowWith("read file error,name '%s',splitid %d,offset %lld,want length:%lld."
		,streamname,cursplitid,curoffset,len);
	crcv=crc32(crcv,(const Bytef *)bf,(uInt)len);
	curoffset+=len;
	offset+=len;
	return offset;
}

uint8B dtioStreamFile::StreamWriteInit(DTIO_STREAM_TYPE _stype,const char *_attachto) {
	if(fp!=NULL) fclose(fp);
	fp=NULL;
	if(_attachto!=NULL) {
		char realname[PATH_LEN];
		//2005/09/28 修改w+b为rb ,源文件使用w+b会被截断（等同于删除)
		FILE *fptmp=fopen(_attachto,"rb");
		if(fptmp==NULL) 
		 ThrowWith("路径错误或权限不足,不能建立 '%s' .",_attachto);
		fclose(fptmp);
		realname[0]=0;
		realpath(_attachto,realname);
		if(realname[0]==0)
	  		ThrowWith("Stream name '%s' is invalid.",realname);
		return dtioStream::StreamWriteInit(_stype,realname);
	}
	return dtioStream::StreamWriteInit(_stype,_attachto);
}

uint8B dtioStreamFile::StreamReadInit() {
	if(fp!=NULL) fclose(fp);
	fp=NULL;
	return dtioStream::StreamReadInit();
}
