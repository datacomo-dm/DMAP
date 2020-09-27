//#include <process.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
//#include <windows.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "AutoHandle.h"



int Start(void *ptr);
mytimer tmt,tmf,tmb;
void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[1000];
	vsprintf(msg,format,vlist);
	errprintf(msg);
	va_end(vlist);
	throw msg;
}

// -l local file mode
// -p path value
bool remotemode=true;
char path[300];
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("imei");
    path[0]=0;
    if(argc>1) {
    	if(strcmp(argv[1],"-l")==0) remotemode=false;
    	else  strcpy(path,argv[1]);
    	if(argc>2) {
    		if(strcmp(argv[2],"-l")==0) remotemode=false;
    		else strcpy(path,argv[2]);
    	}
    }
    if(path[0]==0) {
    	printf("命令帮助: cdrp [本地模式] [路径]\n"
    	       "   本地模式 : -l ,可选,缺省为ftp模式,连接到计费主机.\n"
    	       "   路径: 本地或计费主机的绝对路径.\n");
    	return 0;
    }
    nRetCode=wociMainEntrance(Start,false,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

#ifndef byte
typedef unsigned char byte;
#endif

//#define GETLEN(ptr) 
//#define GETTAG(ptr) 

struct cdr_data {
	int cdr_type;
	int calldur;
	int startdate;
	int starttime;
	int callingtype;
	char msisdn[30];
	char termphone[30];
	char other[30];
	char imei[16];
	char filename[300];
	int blockoffset;
	void output() {
		printf("off:%x,cdr_type:%2x,start:%d%d,msisdn:%s,termphone:%s,imei:%s.\n",blockoffset,
			    cdr_type,startdate,starttime,msisdn,termphone,imei);
	}
};
//#define DEBUG

//  HW for eric, ERIC for hw
#define MSC_HW 1
//#define MSC_ERIC 1

#ifdef MSC_HW
#define MSC_FMT "HW."
#define MAX_BLOCK_LEN 252
#define IMEI_TABLENAME "tab_imei_hw"
#define MAX_RECORDLEN 252
#define MAX_IMEI_ROWS 5000000
#define MAX_SUBSCRB_ROWS 5000000
#define MAX_IMEI_FILES 1500000
#define LISTFILE "listhw.txt"
#else
// constant for eric switch
#define MSC_FMT "ER."
#define IMEI_TABLENAME "tab_imei"
#define MAX_BLOCK_LEN 2048
#define MAX_RECORDLEN 1024
#define LISTFILE "list.txt"


#define MAX_IMEI_ROWS 11500000
#define MAX_SUBSCRB_ROWS 5000000
#define MAX_IMEI_FILES 1500000
#endif
// macros for eric switch
#define GETLEN(ptr) ( *ptr>0x81?(ptr+=3,((ptr[-2]<<8)+ptr[-1])):(*ptr<0x80?(ptr++,ptr[-1]):(ptr+=2,ptr[-1])))
#define GETTAG(ptr) ( (*ptr&0x1f)==31?(ptr+=2,((ptr[-2]<<8)+ptr[-1])):(ptr++,ptr[-1]))

#ifdef DEBUG
#define DBG_SQL " where rownum<10"
#else
#define DBG_SQL " "
#endif

// create table tab_imei (areaid char(6),subscrbid number(9),msisdn varchar2(11),imei varchar2(15),firstfid number(8),firstpos number(8),firsttime date,lastfid number(8),lastpos number(8),lasttime date,calltimes number(6));
// create table tab_imei_file (fileid number(9),filename varchar2(300));

class imeitable {
AutoHandle sess;
AutoMt mt;
AutoMt mtfile;
AutoMt mtsubscrb;
int imei_invalid_rows,msisdn_invalid_rows;
public:
	imeitable():mt(0,10),mtfile(0,10),mtsubscrb(0,10)
	{
		lgprintf("开始数据库读取.");imei_invalid_rows=msisdn_invalid_rows=0;
		#ifndef DEBUG
		AutoHandle ts;
		ts.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.12.18:1522/obs9i",DTDBTYPE_ORACLE));
		mtsubscrb.SetDBC(ts);
		mtsubscrb.SetMaxRows(MAX_SUBSCRB_ROWS);
		mtsubscrb.FetchAll("select svcnum,areaid,subscrbid,opendate from obs.tab_subscrb where svcid='10' and svcid<>'9'");
		mtsubscrb.Wait();
		lgprintf("排序...");
		wociSetSortColumn(mtsubscrb,"svcnum");
		wociSort(mtsubscrb);
		#endif
		lgprintf("连接12.80...");
		sess.SetHandle(wociCreateSession("dtuser","readonly","//130.86.12.80/dtagt",DTDBTYPE_ORACLE));
		mt.SetDBC(sess);
		mt.SetMaxRows(MAX_IMEI_ROWS);
		mt.FetchAll("select * from %s " DBG_SQL,IMEI_TABLENAME);
		mt.Wait();
		lgprintf("排序...");
		wociSetSortColumn(mt,"msisdn,subscrbid,imei");
		wociSort(mt);
		mtfile.SetDBC(sess);
		mtfile.SetMaxRows(MAX_IMEI_FILES);
		mtfile.FetchAll("select * from tab_imei_file " DBG_SQL);
		mtfile.Wait();
		lgprintf("排序...");
		wociSetSortColumn(mtfile,"filename");
		wociSort(mtfile);
		lgprintf("完成数据库读取.");
	}

	void commit(bool truncate=true)
	{
		
		wociMTPrint(mt,10,NULL);
		#ifdef DEBUG
		return;
		#endif
		if(truncate) {
			AutoStmt st(sess);
			st.DirectExecute("truncate table %s",IMEI_TABLENAME);
			st.DirectExecute("truncate table tab_imei_file",IMEI_TABLENAME);
		}
		mt.CreateAndAppend(IMEI_TABLENAME);
		mtfile.CreateAndAppend("tab_imei_file");
	}

	bool haveProcessed(const char *fn)
	{
		char sfn[300];
		strcpy(sfn,fn);
		if(strcmp(sfn+strlen(sfn)-3,".gz")==0) 
			sfn[strlen(sfn)-3]=0;// cut last .gz 
		void *ptr[10];
		ptr[0]=sfn;
		ptr[1]=NULL;
		int fileid=0;
		int off=wociSearch(mtfile,ptr);
		return off>=0;
	}
	const char *getStat() {
		static char str[200];
		sprintf(str,"号码无效话单:%d, 串号无效话单:%d.",msisdn_invalid_rows,imei_invalid_rows);
		return str;
	}

	void check(cdr_data &cdr)
	{
		void *ptr[16];
		char msisdn[30];
		//检查非法串号：
		// 数据依据：http://www.numberingplans.com/?page=plans&sub=imeinr
		/*
		if(strlen(cdr.imei)!=15 || strcmp(cdr.imei,"000000000000000")==0 ||
		  !(strncmp(cdr.imei,"33",2)==0 || strncmp(cdr.imei,"35",2)==0 || strncmp(cdr.imei,"44",2)==0 ||
		  strncmp(cdr.imei,"49",2)==0 || strncmp(cdr.imei,"50",2)==0 || strncmp(cdr.imei,"51",2)==0 ||
		  strncmp(cdr.imei,"52",2)==0 || strncmp(cdr.imei,"10",2)==0 || strncmp(cdr.imei,"30",2)==0 ||
		  strncmp(cdr.imei,"45",2)==0 || strncmp(cdr.imei,"01",2)==0 || strncmp(cdr.imei,"54",2)==0 ||
		  strncmp(cdr.imei,"53",2)==0)
		  ) {
		}*/
		if(strlen(cdr.imei)!=15 || strcmp(cdr.imei,"000000000000000")==0 || cdr.imei[0]==' ') {
			imei_invalid_rows++;
			#ifdef DEBUG
			lgprintf("Invalid imei:%s.",cdr.imei);
			cdr.output();
			#endif
		  	return;
		}
		strcpy(msisdn,cdr.msisdn);
		if(strncmp(msisdn,"86",2)==0)
			strcpy(msisdn,msisdn+2);
		if(strlen(msisdn)!=11 || !(strncmp(msisdn,"13",2)==0 || strncmp(msisdn,"15",2)==0)) {
			msisdn_invalid_rows++;
			#ifdef DEBUG
			lgprintf("Invalid msisdn number:%s.",msisdn);
			cdr.output();
			#endif
			return;
		}
		ptr[0]=cdr.filename;
		ptr[1]=NULL;
		int fileid=0;
		int off=wociSearch(mtfile,ptr);
		if(off<0) {
			fileid=mtfile.GetRows()+1;
			ptr[0]=&fileid;
			ptr[1]=cdr.filename;
			ptr[2]=NULL;
			wociInsertRows(mtfile,ptr,NULL,1);
		}
		else fileid=mtfile.GetInt("fileid",off);
		char dt[20];
		wociSetDateTime(dt,cdr.startdate/10000,(cdr.startdate%10000)/100,
				 cdr.startdate%100,cdr.starttime/10000,(cdr.starttime%10000)/100,
				 cdr.starttime%100);
		char areaid[10];
		int subscrbid=0;
		ptr[0]=msisdn;
		ptr[1]=NULL;
		off=wociSearch(mtsubscrb,ptr);
		if(off>=0) {
			if(memcmp(mtsubscrb.PtrDate("opendate",off),dt,7)<=0) {
				strcpy(areaid,mtsubscrb.PtrStr("areaid",off));
				subscrbid=mtsubscrb.GetInt("subscrbid",off);
			}
			else {
				strcpy(areaid,"closed");
				subscrbid=0;
			}
		}
		else {
			memset(areaid,0,sizeof(areaid));
			subscrbid=0;
		}
		
		ptr[0]=msisdn;
		ptr[1]=&subscrbid;
		ptr[2]=cdr.imei;
		ptr[3]=NULL;

		off=wociSearch(mt,ptr);
		int ct=1;
		if(off<0) {
			ptr[0]=areaid;
			ptr[1]=&subscrbid;
			ptr[2]=msisdn;
			ptr[3]=cdr.imei;
			ptr[4]=&fileid;
			ptr[5]=&cdr.blockoffset;
			ptr[6]=dt;
			ptr[7]=&fileid;
			ptr[8]=&cdr.blockoffset;
			ptr[9]=dt;
			ptr[10]=&ct;
			ptr[11]=NULL;
			wociInsertRows(mt,ptr,NULL,1);
		}
		else {
			if(memcmp(dt,mt.PtrDate(6,off),7)<0) {
				memcpy(mt.PtrDate(6,off),dt,7);
				*mt.PtrInt(4,off)=fileid;
				*mt.PtrInt(5,off)=cdr.blockoffset;
			}
			if(memcmp(dt,mt.PtrDate(9,off),7)>0) {
				memcpy(mt.PtrDate(9,off),dt,7);
				*mt.PtrInt(7,off)=fileid;
				*mt.PtrInt(8,off)=cdr.blockoffset;
			}
			*mt.PtrInt(10,off)+=1;
		}
	}
};



class cdrfile {
protected :
FILE *fp;
int blockid,recordid;
char filename[300];
double totrecords;
int fieldtag;
int fieldlen;
byte *block;
byte *record;
int block_len;
int cdr_type;
int record_len;
cdr_data cdr;	
imeitable *pimeimt;
	void getcallingtype()
	{
	 int fieldlen=GETLEN(record);
	 if(fieldlen!=1) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
	 cdr.callingtype=record[0];
	 record+=fieldlen;
	}

	virtual int getdate() {
	 int fieldlen=GETLEN(record);
	 if(fieldlen!=3) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
	 int rt=(record[0]+2000)*10000+record[1]*100+record[2];
	 record+=fieldlen;
	 return rt;
	}

	virtual int gettime() {
	 int fieldlen=GETLEN(record);
	 if(fieldlen!=3) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
	 int rt=record[0]*10000+record[1]*100+record[2];
	 record+=fieldlen;
	 return rt;
	}

	int getoffset(){
		if(record==NULL) return blockid*block_len;
		return blockid*block_len+(record-block);
	}

	virtual void getbcdstring(char *dst,int len,bool skipfirst)
	{
	 int fieldlen=GETLEN(record);
	 if(fieldlen*2>len+1) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
	 unsigned char *field_end=record+fieldlen;
	 if(skipfirst) {
		//*dst++=(*record&0x0f)+'0';
		record++;
	 }
	 int bytes=skipfirst?1:0;
	 while(bytes++<fieldlen) {
		 if((*record&0x0f)!=0xf) *dst++=(*record&0x0f)+'0';
		 else break;
		 if((*record>>4)!=0xf) *dst++=(*record>>4)+'0';
		 else break;
		 record++;
	 }
	 record=field_end;
	 //if(bytes<=fieldlen) record++;
	 *dst=0;
	}

	void getcalldur() {
		fieldlen=GETLEN(record);
		if(fieldlen!=3) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
		cdr.calldur=record[0]*3600+record[1]*60+record[2];
		record+=fieldlen;
	}

	void skipfield() 
	{
		int len=GETLEN(record);
		record+=len;
	}

	virtual void decodefield()
	{
		fieldtag=GETTAG(record);
		fieldtag|=(cdr_type<<16);
		switch(fieldtag) {
		case 0xa1008c:
		case 0xa4008d: //call duration
			getcalldur();break;
		case 0xa70085:
		case 0xa50086:
		case 0xa10086:
		case 0xa40087: //Called Subscriber IMEI
			getbcdstring(cdr.imei,16,false);break;
		case 0xa50087:
		case 0xa10089:
		case 0xa4008a: //Date For Start of Charge
		case 0xa70086:
			cdr.startdate=getdate();break;
		case 0xa50088:
		case 0xa1008a:
		case 0xa4008b: //Time For Start of Charge
		case 0xa70087:
			cdr.starttime=gettime();break;
		case 0xa50083:
		case 0xa40083: //Type of Calling Subscriber
		case 0xa10083:
			getcallingtype();break;
		case 0xa40085:
		case 0xa70083:   // this party
		case 0xa50084:
		case 0xa10084: //Calling Party Number
			getbcdstring(cdr.msisdn,30,true);break;
		case 0xa40084:
		case 0xa10087:   //other party
		 //Called Party Number
			getbcdstring(cdr.termphone,30,true);break;
		case 0xa40080:
		default:
			skipfield();
		}
	}
public:
	void setfilename(const char *fn)
	{
		strcpy(filename,fn);
		//remove path
		int i=strlen(filename);;
		for(;i>0;i--) 
		  if(filename[i]=='/' || filename[i]=='\\') break;
		if(i>0) strcpy(filename,filename+i+1);
		strcpy(cdr.filename,filename);
	}
	cdrfile(const char *fn,imeitable *pi)
	{
		if(fn!=NULL) {
			fp=fopen(fn,"rb");
		    if(fp==NULL) ThrowWith("Open file '%s' for read failed.",fn);
			setfilename(fn);
		}
		else fp=NULL;
		block=new byte[MAX_BLOCK_LEN];
		record=NULL;block_len=record_len=0;
		blockid=recordid=0;pimeimt=pi;totrecords=0;
	}
	
	double gettotalrecords() {return totrecords;}
	~cdrfile() {
		if(fp) fclose(fp);
		delete []block;
	}
	
	void setfile(const char *fn)
	{
		if(fp) fclose(fp);
		fp=fopen(fn,"rb");
		if(fp==NULL) ThrowWith("Open file '%s' for read failed.",fn);
		setfilename(fn);
		blockid=recordid=0;
	}
	
	bool getblock(int len) {
	 int l=fread(block,1,len,fp);
	 if(l==0) return false; //means reach eof
	 if(l!=len) ThrowWith("a invalid block in file '%s'",filename);
	 block_len=len;
	 return true;
	}
	
	virtual void procfile(int blen)
	{
		tmb.Start();
		while(getblock(blen)) 
			parseblock();
		fclose(fp);fp=NULL;
		tmb.Stop();
	}

	virtual void parserecord()
	{
		byte *record_end=record+record_len;
		do {
		
		cdr_type=*record++;
		memset(&cdr,0,sizeof(cdr));
		strcpy(cdr.filename,filename);
		cdr.blockoffset=record-block-1+(blockid-1)*block_len;
		cdr.cdr_type=cdr_type;
		switch(cdr_type) {
		case 0xa1: //MS Originating
		case 0xa4: //MS terminating 被叫话单
		{
			//single record len
		  	int s_record_len=GETLEN(record);
			//if((blockid-1)*0x800+(record-block)>=0x8d800)
			//{
			//	int stopheare=0;
			//}
			totrecords++;
			byte *srecord_end=record+s_record_len;
			recordid++;
			while(record<srecord_end) 
				decodefield();

			break;
		}
		case 0xa5: //MS Originating SMS in MSC
		case 0xa7: //MS Terminating SMS in MSC
		case 0xa0: //Transit 
		case 0xa2: //Roaming Call Forwarding
		case 0xa3: //Call Forwarding
		case 0xa6: //MS Originating SMS in SMS-IWMSC
		case 0xa8: //MS Terminating SMS in SMS-GMSC
		case 0xa9: //SS Subscriber Procedure
		case 0xad: //Transit IN Outgoing Call
		case 0xae: //IN Incoming Call
		case 0xaf: //IN Outgoing Call
		case 0xb1: //ISDN Originating Call
		case 0xb2: //ISDN Call Forwarding Call
		case 0xb3: //ISDN Supplementary Service Procedure
		default: 
			//printf("Unknow cdr type:%2x,file:;%s',offset:%d.\n",cdr_type,filename,getoffset());
			record=record_end;
			return;
		}
		}
		while(record<record_end) ;
		pimeimt->check(cdr); 
		//cdr.output();
	}

	virtual void parseblock() // for eric
	{
		byte * pblock=block;
		blockid++;
		recordid=0;
		while(pblock<block+block_len) {
		 int btag=*pblock++;
		 if(btag==0) return;//pad 0x0 on end of block for 2048 fixed length.
		if((blockid-1)*block_len+pblock-block>=0x289d0)
		{
			int dbgheer=0;
		}
		 if(btag==0xa1) {
			 pblock++;
			 continue;
		 };
		 if(btag!=0xa0) {
			record_len=GETLEN(pblock);
			lgprintf("not a charging block(%x),skipped(%d bytes),offset:%x.",btag,record_len,
				(blockid-1)*block_len+(pblock-block));
			pblock+=record_len;
			continue;
		 }
		 record_len=GETLEN(pblock);
		 if(record_len>MAX_RECORDLEN) return;/* 块尾无效记录 ?*/
		 record=pblock;
		 parserecord();
		 pblock+=record_len;
		 if(record>pblock) 
			 ThrowWith("Oh,a magic thing--a record is larger than himself!");
		}
	}
};

class cdr_parser_hw :public cdrfile {
protected :
	virtual int getdate() {
	 int rt=(record[1]+2000)*10000+record[2]*100+record[3];
	 record+=4;
	 return rt;
	}

	virtual int gettime() {
	 int rt=record[0]*10000+record[1]*100+record[2];
	 record+=3;
	 return rt;
	}
	
	virtual void getbcdstringHW(char *dst,int len,int skipbytes)
	{
	 //int fieldlen=GETLEN(record);
	 //if(fieldlen*2>len+1) ThrowWith("Get a valid field, cdr:%2x,field:%2x",cdr_type,fieldtag);
	 fieldlen=len/2+skipbytes;
	 unsigned char *field_end=record+fieldlen;
	 int bytes=skipbytes;
	 record+=skipbytes;
	 while(bytes++<fieldlen) {
		 if((*record>>4)!=0xf) *dst++=(*record>>4)+'0';
		 else break;
		 if((*record&0x0f)!=0xf) *dst++=(*record&0x0f)+'0';
		 else break;
		 record++;
	 }
	 record=field_end;
	 //if(bytes<=fieldlen) record++;
	 *dst=0;
	}
	virtual void decodefield()
	{
		byte *pstart=record;
		cdr.callingtype=record[9];
		record=pstart+10;
		cdr.startdate=getdate();
		cdr.starttime=gettime();
		record=pstart+31;
		getbcdstringHW(cdr.termphone,24,2);
		record=pstart+67;
		getbcdstringHW(cdr.msisdn,24,0);
		record=pstart+83; //imei
		getbcdstringHW(cdr.imei,16,0);
		record=pstart+95; //call duration
		cdr.calldur=(record[0]<<24)+(record[1]<<16)+(record[2]<<8)+record[3];
		record=pstart+252;
	}

public:

	cdr_parser_hw(const char *fn,imeitable *pi):cdrfile(fn,pi){};
	virtual void parserecord()
	{
		byte *record_end=record+record_len;
		do {
		cdr_type=record[9];
		memset(&cdr,0,sizeof(cdr));
		strcpy(cdr.filename,filename);
		cdr.blockoffset=record-block-1+(blockid-1)*block_len;
		cdr.cdr_type=cdr_type;
		switch(cdr_type) {
		case 0: //MS Originating
		case 1: //MS terminating 被叫话单
		{
			//single record len
		  	int s_record_len=252;
			//if((blockid-1)*0x800+(record-block)>=0x8d800)
			//{
			//	int stopheare=0;
			//}
			totrecords++;
			byte *srecord_end=record+s_record_len;
			recordid++;
			while(record<srecord_end) 
				decodefield();
			break;
		}
		case 2: //MS Roaming cdr
		case 6: ////MS Terminating SMS in MSC
		case 7: //MS Originating SMS in MSC
		default: 
			//printf("Unknow cdr type:%2x,file:;%s',offset:%d.\n",cdr_type,filename,getoffset());
			record=record_end;
			return;
		}
		}
		while(record<record_end) ;
		pimeimt->check(cdr); 
		//cdr.output();
	}

	virtual void parseblock() // for eric
	{
		byte * pblock=block;
		blockid++;
		recordid=0;
		while(pblock<block+block_len) {
		 record_len=252;//GETLEN(pblock);
		 //if(record_len>MAX_RECORDLEN) return;/* 块尾无效记录 ?*/
		 record=pblock;
		 parserecord();
		 pblock+=record_len;
		 if(record>pblock) 
			 ThrowWith("Oh,a magic thing--a record is larger than himself!");
		}
	}
};


class file_man
{
	FILE *flist;
	bool ftpmode;
	//duplicated process file
	bool duplicateMode;
	char lines[300];
	bool firstpart;
	char *ptr;
	char host[100],username[100],passwd[100],path[300];
public:
	file_man(bool _dupMode=false)
	{
		flist=NULL;memset(lines,0,sizeof(lines));
		firstpart=true;ptr=NULL;
		duplicateMode=_dupMode;ftpmode=true;
	}
	~file_man() {
		if(flist) fclose(flist);
	}

	void list(const char *_host,const char *_username,const char *_passwd,const char *_path)
	{
		char cmd[300];
		strcpy(host,_host);strcpy(username,_username);strcpy(passwd,_passwd);
		if(_path[0]=='/') 
			sprintf(path,"%%2F%s",_path+1);
		else strcpy(path,_path);
		if(flist) fclose(flist);
		flist=NULL;
		unlink(LISTFILE);
		sprintf(cmd,"ncftpls -u %s -p %s ftp://%s/%s >" LISTFILE,username,passwd,host,
			path);
		lgprintf("获取文件列表...");
		system(cmd);
		flist=fopen(LISTFILE,"rt");
		if(flist==NULL) ThrowWith("文件列表失败.");
		memset(lines,0,sizeof(lines));
		fclose(flist);flist=NULL;
		firstpart=true;ptr=NULL;
		ftpmode=true;
	}
	
	void listlocal(const char *_path)
	{
		char cmd[300];
		if(flist) fclose(flist);
		flist=NULL;
		strcpy(path,_path);
		unlink(LISTFILE);
		sprintf(cmd,"ls -1 %s>" LISTFILE,_path);
		printf("cmd: %s\n",cmd);
		system(cmd);
		flist=fopen(LISTFILE,"rt");
		if(flist==NULL) ThrowWith("tar文件列表失败.");
		memset(lines,0,sizeof(lines));
		ftpmode=false;
	}

	bool getfile(char *fn,imeitable *it)
	{
		char cmd[300];
		tmf.Start();
		char *end=fn+strlen(fn)-1;
		while(*end<' ') *end--=0;
		if(!duplicateMode && it->haveProcessed(fn))
		 return false;
		char *str=strstr(fn+6,".");
		if(str && str[-2]=='G' || str[-2]=='T') return false;
		if(ftpmode) {
		 sprintf(cmd,"ncftpget -u %s -p %s ftp://%s/%s/%s ",
			username,passwd,host,path,fn);
		 system(cmd);
		}
		else {
		 sprintf(cmd,"cp -p %s/%s %s ",
			path,fn,fn);
		 system(cmd);
		}
		struct stat st;
		if(stat(fn,&st))
			ThrowWith("文件传输失败:%s.",fn);
		if(strcmp(fn+strlen(fn)-3,".gz")==0) {
			sprintf(cmd,"gzip -d %s",fn);
			fn[strlen(fn)-3]=0;
			unlink(fn);
			system(cmd);
			if(stat(fn,&st))
			 ThrowWith("文件解压缩失败:%s.",fn);
		}
		tmf.Stop();
		return true;
	}

	bool getnextfile(char *fn,imeitable *it,const char *prefix,const char sepchar,bool noskip)
	{
		if(!flist) flist=fopen(LISTFILE,"rt");
		if(flist==NULL) ThrowWith("文件列表失败.");
		if(!firstpart && ptr!=NULL){
		  if(strncmp(ptr,prefix,strlen(prefix))==0) {
			firstpart=true;strcpy(fn,ptr);
			if(noskip && getfile(fn,it)) return true;
		  }
		}
		firstpart=true;
		while(fgets(lines,300,flist)!=NULL) {
			ptr=lines;
			char *end=lines+strlen(lines);
			while(*ptr!=sepchar && ptr<end) ptr++;
			if(ptr<end) *ptr++=0;
			while(*ptr==sepchar && ptr<end) ptr++;
			if(ptr>=end)  ptr=NULL;
			if(strncmp(lines,prefix,strlen(prefix))==0) {
				strcpy(fn,lines);
				firstpart=false;
				if(noskip && getfile(fn,it)) return true;
			}
			else if(ptr && strncmp(ptr,prefix,strlen(prefix))==0) {
				strcpy(fn,ptr);
				firstpart=true;			
				if(noskip && getfile(fn,it)) return true;
			}
		}
		return false; // reach eof
	}
};

//'ER.MSCKM1.525320060731155'
//#define filename "ER.MSCKM1.525120060731153"
// ncftpget -u ynoci -p ynoci123 ftp://130.86.12.11/%2Fbackupdata/gsm/pickbackdata/200608/ER.MSCBS1.001320060814267.gz
int Start(void *ptr) { 
	int rp=0;
	char fn[300];
	wociSetEcho(false);
        wociSetOutputToConsole(TRUE);
        tmt.Start();tmf.Start();tmb.Start();
	file_man fm;
	if(remotemode)
	//"/backupdata/gsm/pickbackdata/200608"
	 fm.list("130.86.12.11","ynoci","ynoci123",path);
	 //"/dbsturbo/cdrfile/backupdata/backup_gsm/tar4"
	else fm.listlocal(path);
	imeitable it;
	int skipfiles=(ptr==NULL?-1:atoi((char *)ptr));
	if(skipfiles>0) 
		printf("跳过前%d个文件.\n",skipfiles);
#ifdef MSC_HW
	cdr_parser_hw cdf(NULL,&it);
#else
	cdrfile cdf(NULL,&it);
#endif 
	int ct=0;
	
	while(fm.getnextfile(fn,&it,MSC_FMT,' ',ct>=skipfiles) ) {
	 //cdf.setfile("ER.MSCBS1.000620060814260");
	 #ifdef DEBUG
	 printf("file:%s.\n",fn);
	 #endif
	 if(ct>=skipfiles) {
	  cdf.setfile(fn);
	  cdf.procfile(MAX_BLOCK_LEN);
	  unlink(fn);
	 }
	 ct++;
	 if(ct%5000==0) it.commit();
	 if(ct%20==0) {
	 	printf("已处理文件数:%d.\n",ct);
	 	//for test
	 	//break;
	 }
	}
	if(cdf.gettotalrecords()>.5) it.commit();
	tmt.Stop();
	printf("文件数 :%d ,有效话单数:%.0f. \n%s. \n时间: 文件处理: %.1f 块处理%.1f 总时间%.1f.\n",ct,cdf.gettotalrecords(),it.getStat(),tmf.GetTime(),tmb.GetTime(),tmt.GetTime());
    return 1;
}

