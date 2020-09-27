#include <stdio.h>
#include "dtio.h"

     dtiofile::dtiofile(dtioStream *_dtio,bool _enablesplit,uint4 _blocklen) {
     	dtio=_dtio;
     	compressflag=0;blocklen=_blocklen;
     	enablesplit=_enablesplit;
     	filelen=0;porigbuf=new char[blocklen];pzipbuf=new char[blocklen+200];
     	startoffset=0;
     };
     
	 dtiofile::~dtiofile() {
		 if(porigbuf) delete []porigbuf;
		 if(pzipbuf) delete[]pzipbuf;
	 }
     
	 uint8B dtiofile::Serialize(const char *fn,int type,const char *prefix,int _compressflag)
	 {
		  if(dtio->IsWrite()) printf("备份文件:'%s'...\n",fn);
		  char *dirc = strdup(fn);
		  char *basec = strdup(fn);
		  strcpy(path, dirname(dirc));
		  strcpy(filename,basename(basec));
		  if(prefix!=NULL) strcpy(path,prefix);
		  filetype=type;
		  compressflag=_compressflag;
		  uint8B off=dtio->GetLength();
		  startoffset=off;
		  FILE *fp=fopen(fn,"rb");
		  if(fp==NULL) 
			  ThrowWith("文件'%s'不能打开(读)",fn);
		  uint8B flen=0;
		  fseeko(fp,0,SEEK_END);
		  uint8B shouldlen=ftello(fp);
		  fseeko(fp,0,SEEK_SET);
		  int flag=DTIOFILEFLAG;
		  dtio->initcrc();
		  //dtio->Put(&flag,sizeof(int),enablesplit);
		  //dtio->Put(&compressflag,sizeof(int),enablesplit);
		  dtio->PutInt(&flag);
		  dtio->PutInt(&compressflag);
		  if(dtio->IsWrite()) {
		    uint8B rdd=fread(porigbuf,1,blocklen,fp);
		    int dct=0;
		    while(rdd>0) {
			  flen+=rdd;
			  dtio->Put(porigbuf,rdd,enablesplit);
			  if(shouldlen!=0l && dct++>10) {
			   dct=0;
			   printf("    %10lld/%10lld 已处理%d%%\r",flen,shouldlen,(int)(flen*100/shouldlen));
			   fflush(stdout);
			  }
			  if(rdd!=blocklen) break;
			  rdd=fread(porigbuf,1,blocklen,fp);
		    }
	            printf("                                                                 \r");
		  }
		  else {
		  	uint8B rm=shouldlen;
		  	while (rm>0) {
		  		uint8B rdd=rm>blocklen?blocklen:rm;
		  		dtio->Put(porigbuf,rdd,enablesplit);
		  		rm-=rdd;
		  	}
		  }
		  fclose(fp);
		  uLongCRC crcfoo=dtio->getcrc();
		  storelen=flen;
		  //dtio->Put(&crcfoo,sizeof(int),enablesplit);
		  dtio->PutInt(&crcfoo);
		  //dtio->GetContentMt()->SetUnit(filetype,path,filename,(double)off,(double)(dtio->GetLength()-off));
		  dtio->GetContentMt()->SetUnit(filetype,path,filename,(double)off,(double)(shouldlen+sizeof(int)*3));
		  free(dirc);free(basec);
		  return dtio->GetOffset();
	 }

     uint8B dtiofile::DeserializeInit(const char *prefix,const char *fbasename,int type)
	 {
		double off,len;
		if(!dtio->GetContentMt()->GetUnit(type,prefix,fbasename,off,len))
		  ThrowWith("找不到项目: 类型 %d,'%s.%s'.",type,prefix);
		startoffset=(uint8B) off;
		storelen=(uint8B)len;
		strcpy(path,prefix);
		strcpy(filename,fbasename);
		filetype=type;
		dtio->SeekAt(startoffset);
		dtio->initcrc();
		int flag=0;
		dtio->GetInt(&flag);
		if(flag!=DTIOFILEFLAG) 
			ThrowWith("文件格式错误，offset:%lld,%x应该是%x.",dtio->GetOffset(),flag,DTIOFILEFLAG);
		dtio->GetInt(&compressflag);
		startoffset+=sizeof(int)*2;
		storelen-=sizeof(int)*3;
		filelen=storelen;
		return dtio->GetOffset();
	 }

     int dtiofile::GetFileName(char *fn,int len) {
     	if(strlen(filename)>len) ThrowWith("dtiofile::GetFileName could not fill destination buffer,filename:%s.",filename);
     	strcpy(fn,filename);
     	return strlen(filename);
     }

     int dtiofile::GetPath(char *_path,int len) {
     	if(strlen(path)>len) ThrowWith("dtiofile::GetFileName could not fill destination buffer,filename:%s.",filename);
     	strcpy(_path,path);
     	return strlen(path);
     }
     
     void dtiofile::OpenDtDataFile(const char *fn) {
	  char dirc[200],basec[200];
	  strcpy(dirc ,fn);
	  strcpy(basec ,fn);
	  DeserializeInit(dirname(dirc),basename(basec),DTIO_UNITTYPE_DATFILE);
	  seekat=0;
     }
     
     void dtiofile::SeekAt(unsigned long off) {
     	seekat=off;
	dtio->SeekFrom( startoffset,seekat);
     }
     
     size_t dtiofile::ReadBuf(char *bf,size_t len) {
		dtio->SeekFrom(startoffset,seekat);
		dtio->Get(bf,len);
		seekat+=len;
		return len;
     }     	
     
     uint8B dtiofile::GetData(char *bf,uint8B offset,uint8B len)
	 {
		if(compressflag!=0)
			ThrowWith("无法读取压缩后的打包文件片断:'%s.%s'.",path,filename);
		dtio->SeekAt(startoffset+offset);
		return dtio->Get(bf,len);
	 }

     
     bool dtiofile::CheckCRC()
	 {
		 return true;
	 }

     uint8B dtiofile::Deserialize(const char *fn)
	 {
		if(startoffset==0) 
			ThrowWith("反序列过程之前应该先调用初始化.");
		printf("恢复文件:'%s %s'->'%s'...\n",path,filename,fn);
		dtio->SeekAt(startoffset);
		FILE *fp=fopen(fn,"w+b");
		if(fp==NULL) {
			int l=strlen(fn);
			char path[300];
			strcpy(path,fn);
			while(--l && path[l]!=PATH_SEPCHAR);
			path[l]=0;
			xmkdir(path);
			fp=fopen(fn,"w+b");
			if(fp==NULL) 
			ThrowWith("无法打开文件'%s'(写).",fn);
		}
		uint8B rm=storelen; //First int: file flag,last int :crc.
	        int dct=0;
		while(rm>0) {
			uint8B gtd=rm>blocklen?blocklen:rm;
			dtio->Get(porigbuf,gtd);
			dp_fwrite(porigbuf,1,(uint4)gtd,fp);
			rm-=gtd;
			if(storelen!=0l && dct++>10) {
			   dct=0;
			   printf("    %10lld/%10lld 已处理%d%%\r",storelen-rm,storelen,(int)((storelen-rm)*100/storelen));
			   fflush(stdout);
			}
		}
                printf("                                                                 \r");
		uLongCRC crcfoo,crcc;
		crcc=dtio->getcrc();
		dtio->GetInt(&crcfoo);
		//dtio->Get(&crcfoo,sizeof(int));
		fclose(fp);
		if(crcfoo!=crcc) 
			ThrowWith("文件'%s %s'CRC校验错!",path,filename);
		dtio->initcrc();
		return dtio->GetOffset();
	 }

