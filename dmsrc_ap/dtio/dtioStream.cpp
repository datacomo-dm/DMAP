#include "dtio.h"


void dtioStream::SplitLenChanged() {
	dtiohdr.filelimit=splitlen;
}

uint8B dtioStream::getStartOffset(int fid) {
	uint8B cof=0;
	for(int i=0;i<fid;i++) cof+=dtiohdr.filelength[i];
	return cof;
}

dtioStream::dtioStream(const char *_basedir) {
	strcpy(obasedir,_basedir);
	memset(&dtiohdr,0,sizeof(dtioHeader));
	dtiohdr.flag=DTIO_STREAMFLAG;
	dtiohdr.hdsize=sizeof(dtioHeader);
	dtiohdr.version=DTIO_VERSION;
	wociGetCurDateTime(dtiohdr.exptime);
	dtiohdr.exportid=*(( uint8B *)dtiohdr.exptime);
	dtiohdr.internalfileid=0;
	hdrcont=(dtioHeaderCont *)new dtioHeaderCont[MAXCONTFILE];
	memset(hdrcont,0,sizeof(dtioHeaderCont)*MAXCONTFILE);
	for(int i=0;i< MAXCONTFILE;i++) {
		hdrcont[i].flag=CONTFILEFLAG;
      	 	hdrcont[i].hdsize=sizeof(dtioHeaderCont);
		hdrcont[i].version=DTIO_VERSION;
		hdrcont[i].exportid=dtiohdr.exportid;
	}
	streamname[0]=0;
	streamopt[0]=0;
	streamlocation[0]=0;
	curoffset=0;offset=0;cursplitid=0;write=true;
	dtiohdr.filelimit=splitlen=DEFAULT_SPLITLEN;
	splitedct=0;
}

int dtioStream::SetStream(const char *bname,const char *bopt,const char *path) {
	strcpy(streamname,bname);
	strcpy(streamopt,bopt);
	strcpy(streamlocation,path);
	return 0;
}


uint8B dtioStream::StreamWriteInit(DTIO_STREAM_TYPE _stype,const char *_attachto) {
	dtiohdr.stype=_stype;
	if(_attachto!=NULL) strncpy(dtiohdr.attachto,_attachto,sizeof(dtiohdr.attachto));
	offset=curoffset=0;
	cursplitid=0;
	mttables mts(1);
	mts.AppendMt(mtcontent.GetMt(),"DTIOCONTENT",false);
	splitedct=0;
	if(write) dtiohdr.ReverseByteOrder();
	Put(&dtiohdr,sizeof(dtiohdr));
	if(write) dtiohdr.ReverseByteOrder();
	dtiohdr.dtiocontct=0;
	mts.Serialize(this,0);//compress FALSE, compact FALSE
	mtcontent.Reset();
	crcv=0;
	return offset;
}

uint8B dtioStream::StreamReadInit() {
	offset=curoffset=0;
	Get(&dtiohdr,sizeof(dtiohdr));
	dtiohdr.ReverseByteOrder();
	if(dtiohdr.flag!=DTIO_STREAMFLAG) 
		ThrowWith("Not a valid file :'%s'!",streamname);
	if(dtiohdr.version>DTIO_VERSION)
		ThrowWith("File '%s' has a unrecognized verion:%0x!",streamname,dtiohdr.version);
	splitlen=dtiohdr.filelimit;
	splitedct=dtiohdr.dtiocontct;
	SeekAt(dtiohdr.hdsize);
	mttables mts(1);
	mts.Deserialize(this);
	mtcontent.CopyMt(mts.GetMt("DTIOCONTENT"));
	crcv=0;
	//mt=exCopyMt(mts.GetMt("DTIOCONTENT"));
	return curoffset;
}
