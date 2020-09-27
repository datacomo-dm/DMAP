#ifndef __WDBIERR_H
#define __WDBIERR_H

#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#ifndef BOOL
#define BOOL int
#endif

#ifndef _MAX_DIR 
#define _MAX_DIR 600
#endif
#ifndef DllExport
#ifdef WIN32
#ifdef  WDBI_EXPORTS
#define DllExport   __declspec( dllexport ) 
#else
#define DllExport   __declspec( dllimport ) 
#endif
#else
#define DllExport
#endif
#endif
#define MAX_ERROR_LEN 4096*10
DllExport int errprintf(const
			  char*, ...);
class WDBIError {
protected:
 char errbuf[MAX_ERROR_LEN];
 static char filen[];
 static int lineid;
 static char afilen[];
 static int alineid;
 int errcode ;
 bool autoThrow;
 static bool echo;
 static bool trace;
 virtual void checkerr(int status) {errcode=status;}
 
public :
	static void SetTrace(bool val);
	static void SetEcho(bool val);
	static bool IsEcho() {return echo;};
	void SetErrCode(int ec);
 int retval;
 void SetAutoThrow(bool val) {autoThrow=val;}
 WDBIError() {errcode=retval=0;autoThrow=false;}
 static void logwhere(int l,const char *fn) {
	alineid=l;
	strcpy(afilen,fn);
 }
 void SetErrPos(int l,const char *fn) {
	 lineid=l;
	 strcpy(filen,fn);
 }
 void GetErrPos(int &l,char **fn) {l=lineid,*fn=(char *)filen;}
 void GetAErrPos(int &l,char **fn) {l=alineid,*fn=(char *)afilen;}
 void GetLastError(int &erc,char **buf) {erc=errcode,*buf=(char *)errbuf;}
 void SetErrMsg(char *str) {strcpy(errbuf,str);};
 void SetCancel() {
	 strcpy(filen," ");
	 lineid=0;
	 retval=errcode=-1;
	 strcpy(errbuf,"User cancelled operation!");
 }
 void Throw() {
	  if(trace && alineid!=-1)
	   errprintf("Error occurs at file %s line %d.\n",afilen,alineid);
	  if(trace && lineid!=-1)
	   errprintf("File %s line %d.\n",filen,lineid);
	  errprintf (" ErrorCode: %d.  %s\n",errcode,errbuf);
	  throw(*((WDBIError *)this));
 }
 virtual int GetErrCode() {return errcode;}
};

#endif

