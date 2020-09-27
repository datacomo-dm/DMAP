#ifndef __WOCI8ERR_H
#define __WOCI8ERR_H
#ifndef sb4
#define sb4 int
#endif
#ifndef sword
#define sword int
#endif

//#ifndef BOOL
//#define FALSE 0
//#define TRUE 1
//#define BOOL int
//#endif
#ifndef _MAX_DIR 
#define _MAX_DIR 600
#endif
int errprintf(const char*, ...);
class WOCIError {
protected:
	//##ModelId=426CAE9000BE
 char errbuf[14096];
	//##ModelId=426CAE9000BF
 static char filen[];
	//##ModelId=426CAE9000CB
 static int lineid;
	//##ModelId=426CAE9000CC
 static char afilen[];
	//##ModelId=426CAE9000DA
 static int alineid;
	//##ModelId=426CAE9000DC
 sb4 errcode ;
	//##ModelId=426CAE9000E0
 bool autoThrow;
	//##ModelId=426CAE9000EA
 static bool echo;
public :
	//##ModelId=426CAE9000EB
	static void SetEcho(bool val);
	//##ModelId=426CAE9000FA
	static bool IsEcho() {return echo;};
	//##ModelId=426CAE9000FC
	void SetErrCode(int ec);
	//##ModelId=426CAE9000FF
 sword retval;
	//##ModelId=426CAE900109
 void SetAutoThrow(bool val) {autoThrow=val;}
	//##ModelId=426CAE90010B
 WOCIError() {errcode=retval=0;autoThrow=false;}
	//##ModelId=426CAE90010C
 static void logwhere(int l,const char *fn) {
	alineid=l;
	strcpy(afilen,fn);
 }
	//##ModelId=426CAE900110
 void SetErrPos(int l,const char *fn) {
	 lineid=l;
	 strcpy(filen,fn);
 }
	//##ModelId=426CAE90011B
 void GetErrPos(int &l,char **fn) {l=lineid,*fn=(char *)filen;}
	//##ModelId=426CAE90011E
 void GetAErrPos(int &l,char **fn) {l=alineid,*fn=(char *)afilen;}
	//##ModelId=426CAE90012A
 void checkerr(sword status);
	//##ModelId=426CAE90012C
 void GetLastError(sb4 &erc,char **buf) {erc=errcode,*buf=(char *)errbuf;}
	//##ModelId=426CAE900139
 void SetErrMsg(char *str) {strcpy(errbuf,str);};
	//##ModelId=426CAE90013B
 void SetCancel() {
	 strcpy(filen," ");
	 lineid=0;
	 retval=errcode=-1;
	 strcpy(errbuf,"User cancelled operation!");
 }
	//##ModelId=426CAE90013C
 void Throw() {
	  if(alineid!=-1)
	   errprintf("Error occurs at file %s line %d.\n",afilen,alineid);
	  if(lineid!=-1)
	   errprintf("File %s line %d.\n",filen,lineid);
	  errprintf (" ErrorCode: %d.  %s\n",errcode,errbuf);
	  throw(*((WOCIError *)this));
 }
	//##ModelId=426CAE90013D
 virtual sb4 GetErrCode() {return errcode;}
};

#endif

