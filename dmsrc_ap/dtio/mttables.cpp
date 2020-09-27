#include "dtio_mt.h"
#include "dtio.h"

char *GetBuf(char *blockbf,int &bf_len,int nlen) {
			if(nlen>bf_len || !blockbf) {
				bf_len=int(nlen*1.3);
				if(blockbf) delete[]blockbf;
				blockbf=new char[bf_len];
				if(!blockbf) ThrowWith("Memory allocation faild ,size:%d",bf_len);
                memset(blockbf,0x0,bf_len);
			}
			return blockbf;
}

mttables::mttables(int _maxtabnum,int _splitrows)
{ 
	maxtabnum=_maxtabnum;
	pmt=new int[maxtabnum];tabnum=0;
	pdestroy=new bool[maxtabnum];
	for(int i=0;i<maxtabnum;i++) pdestroy[i]=false;
	mtnames=new char[maxtabnum*MTNAMELEN];
	splitrows=_splitrows;
	pdtio=NULL;
}  

uint4 mttables::AppendMt(int mt,const char *mtname,bool copymt) 
{
	if(tabnum+1>maxtabnum) 
		ThrowWith("超过最大允许长度内存表数量(%d):'%s',mttables::AppendMt.",tabnum,mtname);
	if(strlen(mtname)>MTNAMELEN) 
		ThrowWith("内存表的名称超过最大允许长度:'%s',mttables::AppendMt.",mtname);
	int _mt=mt;
	if(copymt) {
		_mt=wociCreateMemTable();
	 	wociCopyColumnDefine(_mt,mt,NULL);
	 	wociBuild(_mt,wociGetMemtableRows(mt));
		wociCopyRowsTo(mt,_mt,0,0,wociGetMemtableRows(mt));
		pdestroy[tabnum]=true;
	}
	else pdestroy[tabnum]=false;
	pmt[tabnum]=_mt;
	strcpy(mtnames+tabnum*MTNAMELEN,mtname);
	tabnum++;
	return tabnum;
}

int mttables::GetMt(const char *mtname) {
	for(int i=0;i<tabnum;i++) {
		if(strcmp(mtnames+i*MTNAMELEN,mtname)==0) return pmt[i];
	}
	ThrowWith("内存表'%s'找不到.",mtname);
	return 0;
}

bool mttables::DeleteMt(const char *mtname)
{
	for(int i=0;i<tabnum;i++) {
		if(strcmp(mtname+i*MTNAMELEN,mtname)==0) {
			if(pdestroy[i]) wocidestroy(pmt[i]);
			memmove(pdestroy+i,pdestroy+i+1,(tabnum-i-1)*sizeof(bool));
			memmove(pmt+i,pmt+i+1,(tabnum-i-1)*sizeof(int));
			memmove(mtnames+i*MTNAMELEN,mtnames+(i+1)*MTNAMELEN,(tabnum-i-1)*MTNAMELEN);
			tabnum--;
			return true;
		}
	}
	return false;
}

const char * mttables::GetString(const char *mtname,const char *colname,int rid)
{
	char *pval;
	int clen;
	wociGetStrAddrByName(GetMt(mtname),(char *)colname,rid,&pval,&clen);
	return pval;
}

const char * mttables::GetDate(const char *mtname,const char *colname,int rid)
{
	char *pval;
	wociGetDateAddrByName(GetMt(mtname),(char *)colname,rid,&pval);
	return pval;
}

void mttables::SetString(const char *mtname,const char *colname,const char *value,int rid)//rid=-1 indicate to set all rows
{
	int mt=GetMt(mtname);
	int col=wociGetColumnPosByName(mt,(char *)colname);
	wociSetStrValues(mt,col,rid,1,(char *)value);
}

void mttables::SetDate(const char *mtname,const char *colname,const char *value,const int rid)//rid=-1 indicate to set all rows
{
	int mt=GetMt(mtname);
	int col=wociGetColumnPosByName(mt,(char *)colname);
	wociSetDateValues(mt,col,rid,1,(char *)value);
}

void mttables::SetInt(const char *mtname,const char *colname,int value,const int rid)//rid=-1 indicate to set all rows
{
	int mt=GetMt(mtname);
	int col=wociGetColumnPosByName(mt,(char *)colname);
	wociSetIntValues(mt,col,rid,1,&value);
}

int mttables::GetInt(const char *mtname,const char *colname,const int rid)
{
	return wociGetIntValByName(GetMt(mtname),(char *)colname,rid);
}

int mttables::GetRowNum(const char *mtname)
{
	return wociGetMemtableRows(GetMt(mtname));
}

double mttables::GetDouble(const char *mtname,const char *colname,const int rid)
{
	return wociGetDoubleValByName(GetMt(mtname),(char *)colname,rid);
}

uint8B mttables::Serialize(dtioStream *dtio,int compressflag,bool compact)
{
	int flag=MTBLOCKFLAG;
	pdtio=dtio;
	char *blockbf=NULL;
	int bf_len=0;
	int v=(int)compact;
	dtio->initcrc();
	dtio->PutInt(&flag);
	dtio->PutInt(&tabnum);
	dtio->PutInt(&compressflag);
	dtio->PutInt(&v);
	dtio->PutInt(&splitrows);
	dtio->Put(mtnames,tabnum*MTNAMELEN);
	for(int i=0;i<tabnum;i++) {
		int mt=pmt[i];
		char *pdesc;
		int cdlen,cdnum;
		int rn=wociGetMemtableRows(mt);
		int maxrn=wociGetMaxRows(mt);
		int rl=wociGetRowLen(mt);
		wociReverseCD(mt);
		wociGetColumnDesc(mt,(void **)&pdesc,cdlen,cdnum);
		flag=MTBLOCKSTART;
		dtio->PutInt(&flag);
		dtio->PutInt(&cdlen);
		dtio->PutInt(&cdnum);
		dtio->PutInt(&rn);
		dtio->PutInt(&maxrn);
		dtio->PutInt(&rl);
		dtio->Put(pdesc,cdlen);
		wociReverseCD(mt);
		int bfl1=rl*rn;
		blockbf=GetBuf(blockbf,bf_len,bfl1);
		wociExportSomeRows(mt,blockbf,0,rn);
		dtio->Put(blockbf,bfl1);
		if(!compact && rn<maxrn) {
			bfl1=(maxrn-rn)*rl;
			blockbf=GetBuf(blockbf,bf_len,bfl1);
			memset(blockbf,0,bfl1);
			dtio->Put(blockbf,bfl1);
		}
	}
	uLongCRC crc=dtio->getcrc();
	dtio->PutInt(&crc);
//	dtio->Put(&crc,sizeof(uLong));
	if(blockbf!=NULL) delete []blockbf;
	return dtio->GetLength();
}

uint8B  mttables::Deserialize(dtioStream *dtio)
{
	pdtio=dtio;
	char header[300];
	int flag=MTBLOCKFLAG;
	int *phd=(int *)&header;
	char *blockbf=NULL;
	int bf_len=0;
	dtio->initcrc();
	dtio->Get(header,5*sizeof(int));
	revInt(phd);
	revInt(phd+1);
	revInt(phd+2);
	revInt(phd+3);
	revInt(phd+4);
	if(phd[0]!=flag)
		ThrowWith("文件格式错误，offset:%ld,%x应该是%x.",dtio->GetOffset(),phd[0],flag);
	tabnum=phd[1];
	if(tabnum>maxtabnum) 
		ThrowWith("不能装入%d个内存表，已被限制为%d.",tabnum,maxtabnum);
	int compressflag=phd[2];
	bool compact=phd[3];
	splitrows=phd[4];
	dtio->Get(mtnames,tabnum*MTNAMELEN);
	for(int i=0;i<tabnum;i++) {
		int mt=wociCreateMemTable();
		char *pdesc=NULL;
		int cdlen,cdnum;
		int rn;
		int maxrn;
		int rl=0;
		dtio->GetInt(&flag);
		if(flag!=MTBLOCKSTART)
		 ThrowWith("文件格式错误，offset:%ld,%x应该是%x.",dtio->GetOffset(),flag,MTBLOCKSTART);
		dtio->GetInt(&cdlen);
		dtio->GetInt(&cdnum);
		dtio->GetInt(&rn);
		dtio->GetInt(&maxrn);
		dtio->GetInt(&rl);
		int tlen=0;
		pdesc=GetBuf(pdesc,tlen,cdlen);
		dtio->Get(pdesc,cdlen);
		int bfl1=rl*rn;
		blockbf=GetBuf(blockbf,bf_len,bfl1);
		dtio->Get(blockbf,bfl1);
		wociImport(mt,blockbf,bfl1,pdesc,cdnum,cdlen,maxrn,rn);
		delete []pdesc;
		if(!compact && rn<maxrn) {
			bfl1=(maxrn-rn)*rl;
			blockbf=GetBuf(blockbf,bf_len,bfl1);
			dtio->Get(blockbf,bfl1);

			//dtio->SeekAt(dtio->GetOffset()+bfl1);
		}
		pmt[i]=mt;
		pdestroy[i]=true;
	}
	uLongCRC crcfoo,crc;
	crc=dtio->getcrc();
	//dtio->Get(&crcfoo,sizeof(uLong));
	dtio->GetInt(&crcfoo);
	if(crcfoo!=crc) 
		ThrowWith("读取表数据时CRC校验错误,位移:%lld.\n",dtio->GetLength());
	dtio->initcrc();
	if(blockbf!=NULL) delete []blockbf;
	CheckCompact();
	return dtio->GetLength();
}

mttables::~mttables() {
	for(int i=0;i<tabnum;i++) {
		if(pdestroy[i]) {
			wocidestroy(pmt[i]);
		}
	}
	delete[] pmt;
	delete[] pdestroy;
	delete[] mtnames;
}


void mttables::Clear()
{
	for(int i=0;i<tabnum;i++) {
		if(pdestroy[i]) {
			wocidestroy(pmt[i]);
		}
	}
	memset(pmt,0,sizeof(int)*maxtabnum);
	memset(mtnames,0,maxtabnum*MTNAMELEN);
}
