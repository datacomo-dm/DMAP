#ifndef __unix
#include "StdAfx.h"
#include <windows.h>
#endif 
#include <stdio.h>
#include "woci8.h"
#include <string.h>
#include "AVLIntTree.hpp"
#ifndef __unix
#include <conio.h>
#define STRICMP stricmp
#else
#define getch getchar
#include <strings.h>
#include <pthread.h>
static struct timespec interval = {0, 20000};
#define STRICMP strcasecmp
#endif
#define FETCH_ROWS 50000
#define EXECUTE_TIMES 50000
#ifndef __unix
#define DllExport   __declspec( dllexport ) 
#else
#define DllExport
#endif
 
static char *DT_ERR_MSG[]= {
	"  ",
	"The statment handle is null",
	"  ",
	"Columns is empty,add some columns before use DataTable",
	"The statment is not a SELECT,couldn't bind to DataTable",
	"Column type is unrecognized.",
	"Failed to allocate memory for DataTable",
	"Out of rows",
	"Out of rows or columns",
	"Out of columns",
	"Clear DataTable before add columns to it",
	"Column name or display name is out of length",
	"Column length is invalid for corresponding type",
	"Can't find deserved column name",
	"Can't find deserved column index",
	"Can't find deserved column type",
	"Specified for primary key column is not a numeric type",
	"Primary key index point to a invalid column id ",
	"Primary key index has not defined  ",
	"Find null pointer in Reference all column",
	"Order on Null primary key ",
	"No column be assigned to order",
	"Column name is duplicate",
	"Invalid column name",
	"Destination data column type is invalid",
	"Destination data column is empty",
	"Mismatch sorted column on type or sequence",
	"No any sorted column founded ",
	"Assigned source key columns is not match sorted column on destination",
	"Key column is empty",
	"Assigned source data columns is not match columns setted on destination",
	"Referenced key column is empty",
	"Need integer type to match key column",
	"Memory table is full,exit by user",
	"Can not open text file for read",
	"Out of maximum column width",
	" ",
	"Reference DT can't set to null",
	"Source DT can't set to null",
	"No columns in reference DT",
	"Invalid source DT pointer",
	"Invalid reference DT pointer",
	"Invalid start row number",
	"Invalid row numbers in parameter of Group()",
	"Out of source DT rows",
	"Must set primary key on reference DT",
	"Set source DT before reference",
	"Out of maximum rows allowed in data group",
	"Out of source DT columns",
	"Out of reference DT columns",
	"Column of source DT pointer to reference DT is not a integer type",
	"Out of maximun columns allowed in data group",
	"Only numeric type could use for sumary",
	"Set parameter on a builded data group",
	"Clear data group before set parameter",
	"Not found sorted column",
	"Some sorted column not be used",
	"Column type do not match the sorted",
	"   ",
	"   ",
	"   ",
};


static char *DTL_ERR_MSG[]= {
	"  ",
	"Datatable can't be found. ",
	"Not defined any datatable.  ",
	"A datatable is exist using same name.",
	"    ",
	"    ",
};
 
bool WOCIError::echo=true;
OCIEnv *envhp;
OCIError *errhp;

//FILE *errfp=NULL;
//FILE *lgfp=NULL;
#ifdef __unix
#define MAX_PATH 1024
#endif

char errfile[MAX_PATH]="\x0";
char lgfile[MAX_PATH]="\x0";
bool __output_to_console=false;

#ifdef __unix
     void _strtime(char *nowstr)
       {
           time_t nowbin;
           const struct tm *nowstruct;
           #ifdef WIN32
           (void)setlocale(LC_ALL, "");
           #endif
           if (time(&nowbin) == (time_t) - 1)
               return;
           nowstruct = localtime(&nowbin);
           strftime(nowstr, 9, "%H:%M:%S", nowstruct) ;
       } 

     void _strdate(char *nowstr)
       {
           time_t nowbin;
           const struct tm *nowstruct;
           #ifdef WIN32
           (void)setlocale(LC_ALL, "");
           #endif
           if (time(&nowbin) == (time_t) - 1)
               return;
           nowstruct = localtime(&nowbin);
           strftime(nowstr, 11, "%Y:%m:%d", nowstruct) ;
       } 
#endif

DllExport void wociSetOutputToConsole(bool val)
{
	__output_to_console=val;
}

DllExport void wociSetErrorFile(const char *fn){
	strcpy(errfile,fn);
}

DllExport void wociSetLogFile(const char *fn)
{
	strcpy(lgfile,fn);
}

DllExport int errprintf(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	FILE *errfp=NULL;
	if(errfile[0]!=0) 
		errfp=fopen(errfile,"a+t");
	int rt=0;
	char dtstr[15],tmstr[15];
	_strtime(tmstr);
	_strdate(dtstr);
	if(strlen(format)>2) {
	if(__output_to_console) 
	 printf("[%s %s] ",dtstr,tmstr);
	if(errfp) 
	 fprintf(errfp,"[%s %s] ",dtstr,tmstr);
	}
	
	if(__output_to_console) 
	 rt=vprintf(format,vlist);
	if(errfp) 
	 rt=vfprintf(errfp,format,vlist);
	
	if(strlen(format)>2 && format[strlen(format)-1]!='\n') {
	 if(__output_to_console) 
	  printf("\n");
	 if(errfp) 
	  fprintf(errfp,"\n");
	}
	fclose(errfp);
	va_end(vlist);
	return rt;
}

DllExport int lgprintf(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	FILE *lgfp=NULL;
	if(lgfile[0]!=0) 
		lgfp=fopen(lgfile,"a+t");
	
	int rt=0;
	char dtstr[15],tmstr[15];
	_strtime(tmstr);
	_strdate(dtstr);
	if(strlen(format)>2) {
	if(__output_to_console) 
	 printf("[%s %s] ",dtstr,tmstr);
	if(lgfp) 
	 fprintf(lgfp,"[%s %s] ",dtstr,tmstr);
	}
	
	if(__output_to_console) 
	 rt=vprintf(format,vlist);
	if(lgfp) 
	 rt=vfprintf(lgfp,format,vlist);
	
	if(strlen(format)>2 && format[strlen(format)-1]!='\n') {
	 if(__output_to_console) 
	  printf("\n");
	 if(lgfp) 
	  fprintf(lgfp,"\n");
	}
	va_end(vlist);
	fclose(lgfp);
	return rt;
}

DllExport int myprintf(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	FILE *lgfp=NULL;
	if(lgfile[0]!=0) 
		lgfp=fopen(errfile,"a+t");
	int rt=0;
	if(__output_to_console) 
	 rt=vprintf(format,vlist);
	if(lgfp) 
	 rt=vfprintf(lgfp,format,vlist);
	va_end(vlist);
	fclose(lgfp);
	return rt;
}

void WOCIStartup () {
#ifndef NO_DBMS
  (void) OCIInitialize((ub4) OCI_THREADED, (dvoid *)0,
                       (dvoid * (*)(dvoid *, size_t)) 0,
                       (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                       (void (*)(dvoid *, dvoid *)) 0 );
  (void) OCIEnvInit( (OCIEnv **) &envhp, OCI_DEFAULT, (size_t) 0,
                     (dvoid **) 0 );
  (void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR,
                   (size_t) 0, (dvoid **) 0);
#endif
}

void WOCIShutdown() {
#ifndef NO_DBMS
	if (envhp)
    (void) OCIHandleFree((dvoid *) envhp, OCI_HTYPE_ENV);
#endif
}

bool WOCISession::SetNonBlockMode()  {
#ifndef NO_DBMS
	if(retval=OCIAttrSet ((dvoid *) svrhp, (ub4) OCI_HTYPE_SERVER, 
                           (dvoid *) 0, (ub4) 0, 
                           (ub4) OCI_ATTR_NONBLOCKING_MODE, errhp)) {
				checkerr(retval);
				return false;
			}
#endif
	return true;
}

bool WOCISession::Connect (const char *username,const char *password,const char *svcname)
	{
#ifndef NO_DBMS
	try {
			authp = (OCISession *) 0;
  			/* server contexts */
			(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svrhp, OCI_HTYPE_SERVER,
                   (size_t) 0, (dvoid **) 0);

			(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX,
                   (size_t) 0, (dvoid **) 0);

			if(retval=OCIServerAttach( svrhp, errhp, (text *)svcname, strlen((const char *)svcname), 0)) throw retval;

			/* set attribute server context in the service context */
			if(retval=OCIAttrSet( (dvoid *) svchp, OCI_HTYPE_SVCCTX, (dvoid *)svrhp,
                     (ub4) 0, OCI_ATTR_SERVER, (OCIError *) errhp)) throw retval;
			(void) OCIHandleAlloc((dvoid *) envhp, (dvoid **)&authp,
                        (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);

			if(retval=OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
                 (dvoid *) username, (ub4) strlen((char *)username),
                 (ub4) OCI_ATTR_USERNAME, errhp)) throw retval;

			if(retval=OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
                 (dvoid *) password, (ub4) strlen((char *)password),
                 (ub4) OCI_ATTR_PASSWORD, errhp)) throw retval;
			if(retval=OCIAttrSet((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX,
                   (dvoid *) authp, (ub4) 0,
                   (ub4) OCI_ATTR_SESSION, errhp)) throw retval;
			if(retval=OCISessionBegin ( svchp,  errhp, authp, OCI_CRED_RDBMS,
                          (ub4) OCI_DEFAULT)) throw retval;

		}
		catch (int e) {
			checkerr(e);
			return false;
		}
//#ifdef WIN32
//    if(!SetNonBlockMode()) return false;
//#endif
#endif
		strcpy(UserName,username);
		strcpy(Password,password);
		strcpy(SvcName,svcname);
		return true;
	}


void WOCIError::checkerr(sword status)
{
  errcode=status;
  switch (status)
  {
  case OCI_SUCCESS:
	errcode=0;
    break;
  case OCI_SUCCESS_WITH_INFO:
    strcpy(errbuf,"Error - OCI_SUCCESS_WITH_INFO");
    break;
  case OCI_NEED_DATA:
    strcpy(errbuf,"Error - OCI_NEED_DATA");
	if(autoThrow) {
		strcpy(filen," ");
		lineid=-1;
		throw(*((WOCIError *)this));
	}
    break;
  case OCI_NO_DATA:
    strcpy(errbuf,"Error - OCI_NODATA");
    break;
  case OCI_ERROR:
#ifndef NO_DBMS
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
	if(autoThrow && errcode!=1405 && errcode!=1403) {
		strcpy(filen," ");
		lineid=-1;
		throw(*((WOCIError *)this));
	}
#endif
    //(void) printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    strcpy(errbuf,"Error - OCI_INVALID_HANDLE");
	if(autoThrow) {
		strcpy(filen," ");
		lineid=-1;
		throw(*((WOCIError *)this));
	}
	break;
  case OCI_STILL_EXECUTING:
    strcpy(errbuf,"Error - OCI_STILL_EXECUTE");
    break;
  case OCI_CONTINUE:
    strcpy(errbuf,"Error - OCI_CONTINUE");
    break;
  default:
    break;
  }
}


bool WOCIStatment::BindByPos(int pos,char *result,int size) {
#ifndef NO_DBMS
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, size,SQLT_STR,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_CHAR;
	bindedlen[bindednum]=size;
	bindednum++;
	}
#endif
	return true;
}
		
bool WOCIStatment::BindByPos(int pos,int *result) {
#ifndef NO_DBMS
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, sizeof(int),SQLT_INT,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_INT;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatment::BindByPos(int pos,double *result) {
#ifndef NO_DBMS
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, sizeof(double),SQLT_FLT,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_FLOAT;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatment::BindByPos(int pos,unsigned char *result) {
#ifndef NO_DBMS
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, 21,SQLT_NUM,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_NUM;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatment::BindByPos(int pos,char *result) {
#ifndef NO_DBMS
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, 7,SQLT_DAT,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_DATE;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatment::DefineByPos(int pos,char *result,int size) {
#ifndef NO_DBMS
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, size,SQLT_STR,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatment::DefineByPos(int pos,int *result) {
#ifndef NO_DBMS
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, sizeof(int),SQLT_INT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}
bool WOCIStatment::DefineByPos(int pos,double *result) {
#ifndef NO_DBMS
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, sizeof(double),SQLT_FLT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatment::DefineByPos(int pos,unsigned char *result) {
#ifndef NO_DBMS
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, 21,SQLT_NUM,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatment::DefineByPos(int pos,char *result) {
#ifndef NO_DBMS
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, 7,SQLT_DAT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatment::Prepare(char *_sqlstmt)
{
#ifndef NO_DBMS
	eof=false;
	if(NewStmt()) return false;

	if(strlen(_sqlstmt)>2000) return false;

	strcpy(sqlstmt,_sqlstmt);
	if(retval=OCIStmtPrepare(sthp, errhp, (text *)sqlstmt,
                                (ub4) strlen((char *) sqlstmt),
                                (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT/*OCI_NO_SHARING*/))
	{
		checkerr(retval);
		return false;
	}
	if(!DescribeStmt()) return false;
	bindednum=0;
	executed=false;
	executing=false;
#endif
	return true;
}

int WOCIStatment::Fetch(int rows)
{
#ifndef NO_DBMS
	if(eof) return 0;
	while((retval=OCIStmtFetch(sthp,errhp,rows,OCI_FETCH_NEXT,OCI_DEFAULT))
		==OCI_STILL_EXECUTING && !GetSession()->IsTerminate()) {
#ifdef __unix
			pthread_delay_np (&interval);
#else
			Sleep(10);
#endif
	}
	if(GetSession()->IsTerminate()) {
		SetCancel();
		BreakAndReset();
		return retval; //been set to -1;
	}
	else if(retval!=OCI_STILL_EXECUTING) {
		checkerr(retval);
		if(retval==OCI_NO_DATA) eof=true;
		if(GetErrCode()==1405 || GetErrCode()==1403) //fetched column value is null
			retval=OCI_SUCCESS_WITH_INFO;
		OCIAttrGet((dvoid *) sthp, (ub4) OCI_HTYPE_STMT,
                 (dvoid *) &rowct, NULL,
                  OCI_ATTR_ROW_COUNT, errhp);
	}
	if(echo) lgprintf(".");
#ifdef __unix
	if(__output_to_console)
	fflush(stdout);
#endif
#endif
	return retval;
}

sword WOCIStatment::Execute(int times,int offset) {
#ifndef NO_DBMS
	if(times==0) eof=false;
	if(echo) lgprintf("Excuting :\n %s... ",sqlstmt);
	int processed=0;
	int process=times-processed;
	executing=true;
	if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
	while(!GetSession()->IsTerminate()){
		//retval=OCIStmtExecute(psess->svchp,sthp,errhp,process,offset,
		//(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL,OCI_BATCH_ERRORS );// OCI_DEFAULT);
		retval=OCIStmtExecute(psess->svchp,sthp,errhp,process,offset,
		(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
		if(retval==OCI_STILL_EXECUTING)
#ifdef __unix
			pthread_delay_np (&interval);
#else
			Sleep(10);
#endif
		else if(retval==OCI_ERROR) { 
			/*
		 sb4 ec;
		 (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &ec,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
		 if(ec==1010) Sleep(1000);
		 else */
			 break;
		}
		else {
			processed+=process;
			if(processed>=times)
			 break;
			 int i;
			for( i=0;i<bindednum;i++) {
				switch(bindedtype[i]) {
				case COLUMN_TYPE_FLOAT:
					BindByPos(i+1,((double *)bindedptr[i])+processed);
					break;
				case COLUMN_TYPE_INT:
					BindByPos(i+1,((int *)bindedptr[i])+processed);
					break;
				case COLUMN_TYPE_CHAR:
					BindByPos(i+1,((char *)bindedptr[i])+processed*bindedlen[i],bindedlen[i]);
					break;
				case COLUMN_TYPE_DATE:
					BindByPos(i+1,((char *)bindedptr[i])+processed*7);
					break;
				default :
					errprintf("\nWOCIStatment::Execute 中遇到不可识别的字段类型！\n");
					throw -2;
				}
			}
			process=times-processed;
			if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
		}
	}
	if(GetSession()->IsTerminate()) {
		SetCancel();
		BreakAndReset();
		return retval; //been set to -1;
	}
	else if(retval!=OCI_STILL_EXECUTING) {
		checkerr(retval);
		if(retval!=OCI_NO_DATA)
		OCIAttrGet((dvoid *) sthp, (ub4) OCI_HTYPE_STMT,
                 (dvoid *) &rowct, NULL,
                  OCI_ATTR_ROW_COUNT, errhp);
	}
//	int num_errs;
//	OCIAttrGet (sthp, OCI_HTYPE_STMT, &num_errs, 0,
//            OCI_ATTR_NUM_DML_ERRORS, errhp);
//    if (num_errs) checkerr(OCI_ERROR);

	if(times!=0) {
		if(echo) lgprintf(" %d rows affected.\n",times);
	}
	else if(echo) lgprintf("Done.\n");
	executing=false;
	executed=true;
#endif
	return retval;
}

sword WOCIStatment::BreakAndReset() {
#ifndef NO_DBMS
	try {
	if(retval=OCIBreak(psess->svchp,errhp))
		throw retval;
#ifdef SQLT_TIME
#ifndef __unix
	if(retval=OCIReset(psess->svchp,errhp))
		throw retval;
#endif
#endif
	}
	catch (int &e) {
		checkerr(e);
	}
	if(autoThrow) 
		throw(*((WOCIError *)this));
	
#endif
	return retval;
}

bool WOCIStatment::DescribeStmt()
{
#ifndef NO_DBMS
	try {
	OCIParam *colhd;     /* column handle */
	
	//(void) OCIHandleAlloc((dvoid *) envhp, (dvoid **)&colhd,
    //                    (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);
	if(retval=OCIAttrGet(sthp,OCI_HTYPE_STMT,&stmttype,
		0,OCI_ATTR_STMT_TYPE,errhp)) throw retval;
	if(stmttype!=OCI_STMT_SELECT) throw 0;
	while((retval=OCIStmtExecute(psess->svchp,sthp,errhp,1,0,
		(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DESCRIBE_ONLY))==OCI_STILL_EXECUTING && !GetSession()->IsTerminate()){
#ifdef __unix
			pthread_delay_np (&interval);
#else
		Sleep(10);
#endif
	}
	
	if(retval)
		throw retval;
/* Get the number of columns in the query */
    if(retval=OCIAttrGet(sthp, OCI_HTYPE_STMT, &colct, 
                      0, OCI_ATTR_PARAM_COUNT, errhp))
					  throw retval;

	for (ub4 i = 1; i <= colct; i++)
	{
		/* get parameter for column i */
		if(retval=OCIParamGet(sthp, OCI_HTYPE_STMT, errhp, (dvoid **)&colhd, i)) 
			throw retval;
		/* get data-type of column i */
		if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].type, 0, OCI_ATTR_DATA_TYPE, errhp))
					throw retval;
		if(coldsc[i-1].type==SQLT_NUM) {
			OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].prec, 0, OCI_ATTR_PRECISION, errhp);
			OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].scale, 0, OCI_ATTR_SCALE, errhp);
			if(coldsc[i-1].prec==0 && coldsc[i-1].scale==0) {
				coldsc[i-1].prec=DEFAULT_NUMBER_PREC;
			    coldsc[i-1].scale=DEFAULT_NUMBER_SCALE;
			}
		}

		char *str;
		ub4 nml;
		if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &str, &nml, OCI_ATTR_NAME, errhp))
					throw retval;
		memcpy(coldsc[i-1].colname,str,nml);
		coldsc[i-1].colname[nml]=0;
		if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].dtsize, 0, OCI_ATTR_DATA_SIZE, errhp))
					throw retval;
		//if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
        //            &coldsc[i-1].dspsize, 0, OCI_ATTR_DISP_SIZE, errhp))
		//			throw retval;
		coldsc[i-1].dspsize=nml+1;
		coldsc[i-1].dispname[0]=0;
	}
	
	}
	catch (int &e) {
		checkerr(e);
		if(e!=0)
			return false;
	}
#endif
	return true;
}

bool DataTable::BuildStmt(WOCIStatment *pst,ub4 rows,bool noadj) {
	//if(pbf)
	//	Clear();
	if(pst) {
#ifndef NO_DBMS	
	unsigned int colinstmt;
	colinstmt=pst->GetColumns();
	if(pst->GetStmtType()!=OCI_STMT_SELECT) ReturnErr(DT_ERR_NOTSELECT);
	if(colinstmt<1) ReturnErr(DT_ERR_COLUMNEMPTY);
	pstmt=pst;
	memmove(&cd[colinstmt],&cd[0],colct*sizeof(cd[0]));
	colct+=colinstmt;
	pst->CopyColumnDesc(&cd[0]);
	for(unsigned int i=colinstmt;i<colct;i++) {
		for (unsigned j=0;j<colinstmt;j++) {
			if(STRICMP(cd[i].colname,cd[j].colname)==0) ReturnErr(DT_ERR_COLNAMEDUPLICATE);
		}
	}
#endif
	} else pstmt=NULL;
	maxrows=rows;
	rowlen=0;
	
	for(ub4 i=0;i<colct;i++) {
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			cd[i].type=SQLT_CHR;
			if(!noadj)
			cd[i].dtsize+=1;
			cd[i].prec=cd[i].dtsize;
			cd[i].scale=0;
			rowlen+=cd[i].dtsize;
			cd[i].dspsize=max(cd[i].dspsize,cd[i].dtsize+1u);
			break;
		case SQLT_NUM:
			if(cd[i].scale==0 && cd[i].prec<12) {
				cd[i].type=SQLT_INT;
				cd[i].dspsize=max(cd[i].dspsize,11);
				cd[i].dtsize=sizeof(int);
				rowlen+=sizeof(int);
				break;
			}
			cd[i].dspsize=max(cd[i].dspsize,cd[i].prec+1u);
			cd[i].dtsize =sizeof(double);
			rowlen+=sizeof(double);
			break;
		case SQLT_FLT:
			cd[i].dspsize=max(cd[i].dspsize,11);
			cd[i].dtsize =sizeof(double);
			rowlen+=sizeof(double);
			break;
		case SQLT_INT:
			cd[i].dspsize=max(cd[i].dspsize,11);
			cd[i].dtsize=sizeof(int);
			rowlen+=sizeof(int);
			break;
		case SQLT_DAT:
			cd[i].dspsize=max(cd[i].dspsize,20);
			cd[i].dtsize=7;
			rowlen+=7;
			break;
		default:
			//Not impletemented.
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
		}
		if(strlen(cd[i].dispname)==0) strcpy(cd[i].dispname,cd[i].colname);
	}

	if(pbf) delete [] pbf;	
	bflen=rowlen*(maxrows+10); //预留空间做排序
	pbf=new char[bflen];
	if(!pbf) ReturnErr(DT_ERR_OUTOFMEMORY);
	if(pQDelKey) {
		delete [] pQDelKey;
		pQDelKey=new int [maxrows];
		if(!pQDelKey) ReturnErr(DT_ERR_OUTOFMEMORY);
		memset(pQDelKey,0,maxrows*sizeof(int));
	}
	if(pSortedPos) {
		delete [] pSortedPos;
		pSortedPos=new unsigned int [maxrows];
		if(!pSortedPos) ReturnErr(DT_ERR_OUTOFMEMORY);
	}
	if(pPKSortedPos) {
		delete [] pPKSortedPos;
		pPKSortedPos=new unsigned int [maxrows];
		if(!pPKSortedPos) ReturnErr(DT_ERR_OUTOFMEMORY);
	}
	//qdelmax=3000;
	if(pstmt) FetchPrepare(0);
	else memset(pbf,0,rowlen*maxrows);
	rowct=0;
	//int mu=GetMemUsed();
	//if(mu>1000000) 
	//	lgprintf("Build MT Used :%d bytes",mu);
	return true;
}

unsigned int DataTable::FetchAll(int st) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT);
	//if(st==0) 
		rowct=st;
	FetchPrepare(st);
	bool rtval=true;
	while(true) {
	unsigned int rows=pstmt->GetRows();
	unsigned int fetchn=min((maxrows-rows-rowct),FETCH_ROWS);
	if(fetchn<1) {break;}
	if(pstmt->Fetch(fetchn)==-1) ReturnErr(DT_ERR_OCIERR);
	if(pstmt->GetRows()<rows+fetchn) {
		break;}
	FetchPrepare(pstmt->GetRows()+rowct);
	}
	rowct+=pstmt->GetRows();
	if(echo) lgprintf("%d rows fetched.\n",rowct);
	if(rowct==maxrows) {
		errprintf("***Warning : Memory table is full!\n");
		if(echo) {
		errprintf("Press q to exit or any other key to continue ...");
		fflush(stdout);
		int ch=getch();
		if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		printf("\n");
		}
	}
	RestorePtr();
	return rtval==false?0:rowct;
}

unsigned int DataTable::FetchFirst(ub4 rn) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT);
	bool rtval=true;
	rowct=0;
	int oldrn=rn;
	rn=min((ub4)rn,maxrows);
	FetchPrepare(0);
	while(true) {
		unsigned int rows=pstmt->GetRows();
		unsigned int fetchn=min((rn-rows),FETCH_ROWS);
		if(fetchn<1) {break;}
		if(pstmt->Fetch(fetchn)==-1) ReturnErr(DT_ERR_OCIERR);
		if(pstmt->GetRows()<rows+fetchn) {break;}
		FetchPrepare(pstmt->GetRows());
	}
	rowct=pstmt->GetRows();
	if(rowct!=rn && echo)
	 lgprintf("%d rows fetched.\n",rowct);
	if(rowct==maxrows && oldrn!=maxrows) {
		errprintf("***Warning : Memory table is full!\n");
		if(echo) {
		printf("Press q to exit or any other key to continue ...");
		fflush(stdout);
		int ch=getch();
		if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		printf("\n");
		}
	}
	RestorePtr();
	return rtval==false?0:rowct;
}

unsigned int DataTable::FetchNext(ub4 rn) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT);
	rowct=0;
	bool retv=true;
	int oldrn=rn;
	rn=min(rowct+rn,maxrows)-rowct;
	FetchPrepare(rowct);
	int oldrowct=pstmt->GetRows();
	while(true) {
		unsigned int rows=pstmt->GetRows();
		unsigned int fetchn=min((rn+oldrowct-rows),FETCH_ROWS);
		if(fetchn<1) {break;}
		int rt=pstmt->Fetch(fetchn);
		if(rt==-1) ReturnErr(DT_ERR_OCIERR);
		if(rt==OCI_NO_DATA) {break;}
		if(pstmt->GetRows()<rows+fetchn) {break;}
		FetchPrepare(rowct+pstmt->GetRows()-oldrowct);
	}
	unsigned int rct=pstmt->GetRows()-oldrowct;
	if(rct>0) rowct+=rct;
	if(rct!=rn && echo)  lgprintf("%d rows fetched.\n",pstmt->GetRows());
	RestorePtr();
	if(rowct==maxrows && oldrn!=maxrows) {
		errprintf("***Warning : Memory table is full!\n");
		if(echo) {
		printf("Press q to exit or any other key to continue ...");
		fflush(stdout);
		int ch=getch();
		if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		printf("\n");
		}
	}
	return retv==false?0:rct;
}

bool DataTable::GetLine(char *str,int rownum,bool rawpos,char *colsnm) {
	int colrf[MAX_COLUMN],i;
	if(colsnm!=NULL) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;

	char fmt[300];
	str[0]=0;
	if(!rawpos) {
	if(pSortedPos) {
		rownum=pSortedPos[rownum];
	}
	else if(pPKSortedPos) {
		rownum=pPKSortedPos[rownum];
	}
	}
	if((ub4 )rownum>rowct-1) ReturnErr(DT_ERR_OUTOFROW) ;
	for(ub4 j=0;j<colnum;j++) {
		i=colrf[j];
		char fmttmp[200];
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			//sprintf(fmttmp,"%%%ds ",cd[i].dspsize-1);
			sprintf(fmttmp,"%%%ds",cd[i].dspsize);
			sprintf(fmt,fmttmp,cd[i].pCol+rownum*cd[i].dtsize);
			//sprintf(fmt,"%s",cd[i].pCol+rownum*cd[i].dtsize);
			strcat(str,fmt);
			
			break;
		case SQLT_NUM:
			sprintf(fmttmp,"%%%d.%df ",cd[i].prec,cd[i].scale);
			sprintf(fmt,fmttmp,*(double *)(cd[i].pCol+rownum*sizeof(double)));
			//sprintf(fmt,"%4.2f",*(double *)(cd[i].pCol+rownum*sizeof(double)));
			strcat(str,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmttmp,"%%%df ",10);
			sprintf(fmt,fmttmp,*(double *)(cd[i].pCol+rownum*sizeof(double)));
			//sprintf(fmt,"%4.2f",*(double *)(cd[i].pCol+rownum*sizeof(double)));
			strcat(str,fmt);
			break;
		case SQLT_INT:
			sprintf(fmttmp,"%%%dd ",10);
			sprintf(fmt,fmttmp,*(int *)(cd[i].pCol+rownum*sizeof(int)));
			//sprintf(fmt,"%d",*(int *)(cd[i].pCol+rownum*sizeof(int)));
			strcat(str,fmt);
			break;
		case SQLT_DAT:
			{
				WOCIDate dt(cd[i].pCol+rownum*7);
				dt.GetString(fmt);
				strcat(str,fmt);
			}
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
		//strcat(str,"|");
	}
	return true;
}

void DataTable::GetTitle(char *str,int l,char *colsnm) {
	char fmt[300];
	str[0]=0;
	int len=0;
	int colrf[MAX_COLUMN],i;
	if(colsnm!=NULL) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	
	for(ub4 j=0;j<colnum;j++) {
		char fmttmp[20];
		int i=colrf[j];
		sprintf(fmttmp,"%%%ds",cd[i].dspsize);
		sprintf(fmt,fmttmp,cd[i].dispname);
		len+=strlen(fmt);
		if(len<l)
		 strcat(str,fmt);
		else return;
	}
}

bool DataTable::FetchPrepare(ub4 rn)
{
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT);
	if(rn==0)
		memset(pbf,0,rowlen*maxrows);
	rn=min(rn,maxrows-1);
	unsigned int colst=pstmt->GetColumns();
	RestorePtr();
//	for(ub4 i=0;i<colst;i++)
//		cd[i].pCol+=rn*cd[i].dtsize;
	int i;
	for( i=0;i<colst;i++) {
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			pstmt->DefineByPos(i+1,cd[i].pCol+rn*cd[i].dtsize,cd[i].dtsize);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			pstmt->DefineByPos(i+1,(double *)(cd[i].pCol+rn*cd[i].dtsize));
			break;
		case SQLT_INT:
			pstmt->DefineByPos(i+1,(int *)(cd[i].pCol+rn*cd[i].dtsize));
			break;
		case SQLT_DAT:
			pstmt->DefineByPos(i+1,cd[i].pCol+rn*cd[i].dtsize);
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
	}
	return true;
}

void DataTable::RestorePtr()
{
	cd[0].pCol=pbf;
	for(ub4 i=1;i<colct;i++) 
		cd[i].pCol=cd[i-1].pCol+maxrows*cd[i-1].dtsize;
}

char * DataTable::LocateCell(ub4 colid, ub4 rowst)
{
	if(maxrows<=rowst) ReturnNullErr(DT_ERR_OUTOFROW);
	if(colct<=colid) ReturnNullErr(DT_ERR_OUTOFCOLUMN);
	return cd[colid].pCol+rowst*cd[colid].dtsize;
}

bool DataTable::AddColumn(char *name,char *dspname,int ctype,int length,int scale) {
	// Clear before addcolumn
	if(pbf) ReturnErr(DT_ERR_CLEARBEFOREADD);
	if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN);

	if(strlen(name)>=COLNAME_LEN-1 || (dspname && strlen(dspname)>=COLNAME_LEN-1) )
		ReturnErr(DT_ERR_OUTOFCOLNAME);
	for(unsigned int i=0;i<colct;i++) {
		if(STRICMP(cd[i].colname,name)==0) ReturnErr(DT_ERR_COLNAMEDUPLICATE);
	}
	
	switch (ctype) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
		if(length==-1) ReturnErr(DT_ERR_INVALIDCOLLEN);
		cd[colct].type=SQLT_CHR;
		cd[colct].dtsize=length;
//		cd[colct].dtsize=length-1; //Increase at buid,so minus 1 here.
//		cd[colct].dspsize=strlen(name);
		strcpy(cd[colct].colname,name);
		cd[colct].prec=0;
		cd[colct].scale=0;
		break;
	case SQLT_FLT :
		cd[colct].type=SQLT_FLT;
		cd[colct].dtsize=sizeof(double);
//		cd[colct].dspsize=strlen(name);
		break;
	case SQLT_NUM  :
		if(length==-1) ReturnErr(DT_ERR_INVALIDCOLLEN);
		if(scale==0 && length<11) {
			cd[colct].type=SQLT_INT;
			cd[colct].dtsize=sizeof(int);
//			cd[colct].dspsize=strlen(name);
			cd[colct].prec=length;
			cd[colct].scale=scale;
			break;
		}
		cd[colct].type=SQLT_NUM;
		cd[colct].dtsize=sizeof(double);
//		cd[colct].dspsize=cd[colct].dspsize=strlen(name);
		cd[colct].prec=length;
		cd[colct].scale=scale;
		break;
	case SQLT_INT	:
		cd[colct].type=SQLT_INT;
		cd[colct].dtsize=sizeof(int);
		cd[colct].prec=0;
		cd[colct].scale=0;
//		cd[colct].dspsize=strlen(name);
		break;
	case SQLT_DAT :
		cd[colct].type=SQLT_DAT;
		cd[colct].dtsize=7;
		cd[colct].prec=0;
		cd[colct].scale=0;
		break;
	default : //Invalid column type
		ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
	}
	
	strcpy(cd[colct].colname,name);
	if(dspname) 
		strcpy(cd[colct].dispname,dspname);
	else
		strcpy(cd[colct].dispname,name);
	cd[colct].dspsize=strlen(cd[colct].dispname)+1;
	rowlen+=cd[colct].dtsize;
	colct++;
	return true;
}

bool DataTable::Build(ub4 rows,bool noadj) {
	if(!BuildStmt(NULL,rows,noadj)) return false;
	RestorePtr();
	memset(pbf,0,rowlen*maxrows);
	return true;
}

void DataTable::SetColDspName(char *colnm,char *nm) {
	int ind=GetColumnIndex(colnm);
	if(strlen(nm)<COLNAME_LEN-1) {
		strcpy(cd[ind].dispname,nm);
		if(strlen(nm)>=cd[ind].dspsize)
			cd[ind].dspsize=strlen(nm)+1;
	}
}
	
int DataTable::GetValues(char *col,ub4 rowst,ub4 rownm,int *bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(char *col,ub4 rowst,ub4 rownm,double *bf) //NUM FLOAT type
{	
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(char *col,ub4 rowst,ub4 rownm,char *bf) //Date type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(char *col,ub4 rowst,ub4 rownm,char *bf,int &cellLen)//char varchar type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf,cellLen);
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,int *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	if( cd[colid].type==SQLT_INT) 
		memcpy(bf,tp,sizeof(int)*rownm);
	else if(cd[colid].type==SQLT_NUM) {
		for(int i=0;i<rownm;i++) 
			bf[i]=(int)((double *)tp)[i];
	}
	else  ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,double *bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	memcpy(bf,tp,sizeof(double)*rownm);
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	memcpy(bf,tp,7*rownm);
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		 cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	memcpy(bf,tp,cd[colid].dtsize*rownm);
	return rownm;
}

int DataTable::GetColumnIndex(char *name) {
	unsigned int i;
	for( i=0;i<colct;i++) 
		if(STRICMP(cd[i].colname,name)==0) break;
	if(i==colct) ReturnIntErr(DT_ERR_MISMATCHCOLNAME);
	return i;
}

int DataTable::GetColumnName(ub4 colid,char *nmbf) {
	if(colid<colct) 
	 strcpy(nmbf,cd[colid].dispname);
	else ReturnIntErr(DT_ERR_MISMATCHCOLID);
	return 0;
}

int DataTable::GetColumnLen(ub4 colid) {
	if(colid<colct) 
		return (cd[colid].type==SQLT_NUM || cd[colid].type==SQLT_INT) ?cd[colid].prec:cd[colid].dtsize;
	return 0;
}


int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,int *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst+rownm>=maxrows ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_INT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,sizeof(int)*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,double *bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst+rownm>=maxrows ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);

	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,sizeof(double)*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst+rownm>=maxrows ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,7*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst+rownm>=maxrows ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		 cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,cd[colid].dtsize*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

void DataTable::ErrorCheck(char *fn, int ln, int ercd)
{
	SetErrPos(ln,fn);
	if(ercd==DT_ERR_OCIERR) {
		char *msg;
		GetLastError(errcode,&msg);
		strcpy(errbuf,msg);
		retval=ercd;
	}
	else
	{
		retval=errcode=ercd;
		sprintf(errbuf,"Exception : %s on DataTable '%s'",DT_ERR_MSG[ercd],dtname);
		//strcpy(errbuf,DT_ERR_MSG[ercd]);
	}
	if(autoThrow) throw(*((WOCIError *)this));
}

void WOCISession::Commit()
{
#ifndef NO_DBMS
	OCITransCommit(svchp, errhp, (ub4) 0);
#endif
}

void WOCISession::Rollback()
{
#ifndef NO_DBMS
	OCITransRollback(svchp, errhp, (ub4) 0);
#endif
}

void WOCIStatment::checkerr(sword status)
{
	if(status!=OCI_SUCCESS) { //==OCI_ERROR) {
#ifndef NO_DBMS
	ub4 row_off=0,vsize=4;
	ub2 st_off=0;
	OCIError *errhndl;
	int num_errs;
	errbuf[0]=0;
	
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
	OCIAttrGet (sthp,OCI_HTYPE_STMT,&st_off,0,
				  OCI_ATTR_PARSE_ERROR_OFFSET,errhp);
	if(st_off) {
			//sprintf(errbuf+strlen(errbuf),"\n错误出现位置: %s.",sqlstmt);
			strcat(errbuf,"\n");
			strcat(errbuf,sqlstmt);
			errbuf[strlen(errbuf)-(strlen(sqlstmt)-st_off)]=0;
			strcat(errbuf,"^");
			strcat(errbuf,sqlstmt+st_off);
			strcat(errbuf,"\n");
	}
	/*
	OCIAttrGet (sthp, OCI_HTYPE_STMT, &num_errs, 0,
            OCI_ATTR_NUM_DML_ERRORS, errhp);
	if(num_errs>0) {
	(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhndl, OCI_HTYPE_ERROR,
                  (size_t) 0, (dvoid **) 0);
		strcat(errbuf,"\n以下语句绑定数据错误:\n");
		strcat(errbuf,sqlstmt);
		strcat(errbuf,"\n");
	int procne=(num_errs>20?20:num_errs);
	for(int i=0;i<procne;i++) {
		OCIParamGet(errhp, OCI_HTYPE_ERROR,errhp, (void **)&errhndl, i);
		row_off=-1;
		OCIAttrGet (errhndl, OCI_HTYPE_ERROR, &row_off, &vsize, 
                  OCI_ATTR_DML_ROW_OFFSET, errhp);
		if(row_off>=0) {
			sprintf(errbuf+strlen(errbuf),"\n错误出现在第%d行数据.",row_off+1);
	
	    (void) OCIErrorGet((dvoid *)errhndl, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf+strlen(errbuf), (ub4) sizeof(errbuf)-strlen(errbuf), OCI_HTYPE_ERROR);
		char *pf=errbuf+strlen(errbuf);
		strcpy(pf,"Binded parameter(s):\n");
		for(int j=0;j<bindednum;j++) {
			switch(bindedtype[j]) {
				case COLUMN_TYPE_FLOAT:
					sprintf(pf+strlen(pf),"%f,",((double *)bindedptr[j])[row_off]);
					break;
				case COLUMN_TYPE_INT:
					sprintf(pf+strlen(pf),"%d,",((int *)bindedptr[j])[row_off]);
					break;
				case COLUMN_TYPE_CHAR:
					sprintf(pf+strlen(pf),"'%s',",((char *)bindedptr[j])+row_off*bindedlen[j]);
					break;
				case COLUMN_TYPE_DATE:
					{
						char str[30];
						WOCIDate dt((char *)bindedptr[j]+row_off*7);
						dt.GetString(str);
						strcat(pf,str);
					}
					break;
				default :
					errprintf("\nWOCIStatment::Execute 中遇到不可识别的字段类型！\n");
					throw -2;
				}
		}
		}
	}
	if(num_errs>20) {
		sprintf(errbuf+strlen(errbuf),"\n.......\n错误行数%d.\n",num_errs);
	}
	OCIHandleFree(errhndl,OCI_HTYPE_ERROR);
	}
	*/
	else if(st_off==0) {
		WOCIError::checkerr(status);
		return;
	}
	if(autoThrow && errcode!=1405 && errcode!=1403) {
		strcpy(filen," ");
		lineid=-1;
		throw(*((WOCIError *)this));
	}

/*
OCI_ATTR_PARSE_ERROR_OFFSET
Mode 
READ 

Description 
Returns the parse error offset for a statement. 

Attribute Datatype 
ub2 * 



OCI_ATTR_DML_ROW_OFFSET

Mode 
READ 

Description 
Returns the offset (into the DML array) at which the error occurred. 
Attribute Datatype 
ub4 * 

*/
#endif
	}
	else WOCIError::checkerr(status);
    //(void) printf("Error - %.*s\n", 512, errbuf);
}

int WOCIStatment::NewStmt()
{
#ifndef NO_DBMS
	if(sthp) OCIHandleFree(sthp,OCI_HTYPE_STMT);
	if(retval=OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &sthp,
			OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0)) {
		 checkerr(retval);
         sthp=NULL;
	}
#endif
	return retval;
}


	DataTableLink::DataTableLink(bool athr) {
		pHome=NULL;
		autoThrow=athr;
	}
	
	DataTableLink::~DataTableLink() {
		DataTable *pT=pHome;
		while(pT) {
			DataTable *pT2=pT->GetNextTable();
			delete pT;
			pT=pT2;
		}
	}
	
	DataTable *DataTableLink::FirstTab() {return pHome;}
	
	DataTable *DataTableLink::LastTab() {
		DataTable *pT=pHome;
		if(!pT) ReturnNullErr(DTL_ERR_EMPTYLINK);
		while(pT->GetNextTable()) pT=pT->GetNextTable();
		return pT;
	}
	
	DataTable *DataTableLink::AddTable(char *name,bool atthr) {
		DataTable *pdt;
		if(ExistTable(name)) ReturnNullErr(DTL_ERR_ALREADYEXIST);
		pdt=new DataTable(atthr);
		pdt->SetName(name);
		DataTable *pT=pHome==NULL?NULL:LastTab();
		if(!pT) 
			pHome=pdt;
		else 
			pT->SetNextTable(pdt);
		return pdt;
	}
	
	DataTable *DataTableLink::FindTable(char *name) {
		DataTable *pdt=pHome;
		while(pdt) {
			if(pdt->MatchName(name)) return pdt;
			pdt=pdt->GetNextTable();
		}
		ReturnNullErr(DTL_ERR_DTNOTFOUND);
	}
	bool DataTableLink::DeleteTable(char *name) {
		if(!pHome) 
			ReturnErr(DTL_ERR_EMPTYLINK);
		if(pHome->MatchName(name)) {
			DataTable *pt=pHome->GetNextTable();
			delete pHome;
			pHome=pt;
			return true;
		}
		DataTable *pdt=pHome;
		while(pdt->GetNextTable()) {
			if(pdt->GetNextTable()->MatchName(name)) {
				pdt->SetNextTable(pdt->GetNextTable()->GetNextTable());
				delete pdt->GetNextTable();
				return true;
			}
			pdt=pdt->GetNextTable();
		}
		ReturnErr(DTL_ERR_DTNOTFOUND);
	}

void DataTableLink::ErrorCheck(char *fn, int ln, int ercd)
{
	SetErrPos(ln,fn);
		retval=errcode=ercd;
		sprintf(errbuf,"Datatable Linker Exception : %s .",DTL_ERR_MSG[ercd]);
		//strcpy(errbuf,DT_ERR_MSG[ercd]);
	if(autoThrow) throw(*((WOCIError *)this));
}

bool DataTableLink::ExistTable(char *nm)
{
		DataTable *pdt=pHome;
		while(pdt) {
			if(pdt->MatchName(nm)) return true;
			pdt=pdt->GetNextTable();
		}
		return false;
}


int DataTable::GetAddr(char *col,ub4 rowst,ub4 rownm,int **bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(char *col,ub4 rowst,ub4 rownm,double **bf) //NUM FLOAT type
{	
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(char *col,ub4 rowst,ub4 rownm,char **bf) //Date type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(char *col,ub4 rowst,ub4 rownm,char **bf,int &cellLen)//char varchar type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf,cellLen);
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,int **bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_INT ) ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=(int *)tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,double **bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=(double *)tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW);
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		 cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=tp;
	return rownm;
}

bool DataTable::SetPKID(int id)
{
	if(id<0 || (unsigned int)id>=colct) ReturnErr(DT_ERR_PKOUTOFCOL);
	if(cd[id].type!=SQLT_INT)
		ReturnErr(DT_ERR_NOTPKCOL);
	pkid=id;
	return true;
}

unsigned int DataTable::SearchPK(int key,int schopt)
{
	if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED);
	if(rowct==0) return schopt==2?0:-1;
	int *bf=(int *)cd[pkid].pCol;
	unsigned int head=0;
	unsigned int tail=rowct-1;
	unsigned int mid;
    if(!pPKSortedPos) {
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[mid]>key) tail=mid;
		else if(bf[mid]<key) head=mid;
		else return mid;
		if(tail==0) break;
	}
	/*
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[mid]<key) tail=mid;
		else if(bf[mid]>key) head=mid;
		else break;
	}*/
	if(bf[tail]==key) return tail;
	if(bf[head]==key) return head;
	if(schopt==2) {
		if(bf[tail]<key)
			return tail+1;
		if(bf[mid]<key) 
			return mid+1;
		if(bf[head]<key) 
			return head+1;
		return head;
	}
	//if(bf[mid]==key) return mid;
	}
	else {
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[pPKSortedPos[mid]]>key) tail=mid;
		else if(bf[pPKSortedPos[mid]]<key) head=mid;
		else return (schopt!=0)?mid:pPKSortedPos[mid];
		if(tail==0) break;
	}
	/*
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[mid]<key) tail=mid;
		else if(bf[mid]>key) head=mid;
		else break;
	}*/
	if(bf[pPKSortedPos[tail]]==key) 
		return (schopt!=0)?tail:pPKSortedPos[tail];
	if(bf[pPKSortedPos[head]]==key) 
		return (schopt!=0)?head:pPKSortedPos[head];
	if(schopt==2) {
		if(bf[pPKSortedPos[tail]]<key)
			return tail+1;
		if(bf[pPKSortedPos[mid]]<key) 
			return mid+1;
		if(bf[pPKSortedPos[head]]<key) 
			return head+1;
		return head;
	}
	//if(bf[pPKSortedPos[mid]]==key) return pPKSortedPos[mid];
	}
	return -1;
}

bool DataTable::SetPKID(char *nm)
{
	int i=GetColumnIndex(nm);
	if(i<0) return false;
	return SetPKID(i);
}

int DataTable::GetColumnNum()
{	
	return colct;
}

unsigned short DataTable::GetColumnType(int id)
{
	if((unsigned int)id>=colct) ReturnIntErr(DT_ERR_OUTOFCOLUMN);
	return cd[id].type;
}


bool MemTable::SetGroupSrc(DataTable *src) {
	if(dtSrc==NULL)
	 keycolsrcct=0;
	if(src==NULL)
		ReturnErr(DTG_ERR_PARMSRCISNULL)
	dtSrc=src;
	return true;
}

bool MemTable::SetGroupRef(DataTable *ref,unsigned int skey) {
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF);
	if(ref==NULL)
		ReturnErr(DTG_ERR_PARMREFISNULL)
	if(ref->GetColumnNum()<1) 
		ReturnErr(DTG_ERR_NOCOLUMNSINREF);
	if(!ref->IsPKSet()) ReturnErr(DTG_ERR_MUSTSETREFPK);
	if(dtSrc->GetColumnType(skey)!=SQLT_INT) 
		ReturnErr(DTG_ERR_PKMUSTINT_INSRCDT);
	dtRef=ref;
	srckey=skey;
	return true;
}

bool MemTable::SetGroupColSrc(int *colarray)//-1 means end
{
	int i=0;
	if(pbf) 
		ReturnErr(DTG_ERR_SETONBUILDEDDTP);
	if(colct>0 || rowct>0)
		ReturnErr(DTG_ERR_CLEARBEFOREUSING);
	while(*colarray!=-1) {
		if(*colarray>dtSrc->GetColumnNum()) return false;
		grpbyColSrc[i++]=*colarray;
		char cn[COLNAME_LEN];
		dtSrc->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,dtSrc->GetColumnType(*colarray),dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL);
	}
	return true;
}

bool MemTable::SetGroupColRef(int *colarray)//-1 means end
{
	int i=0;
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF);
	if(!dtRef) ReturnErr(DTG_ERR_INVALIDREFDT);
	while(*colarray!=-1) {
		if(*colarray>dtRef->GetColumnNum()) ReturnErr(DTG_ERR_OUTOFREFCOL);
		grpbyColRef[i++]=*colarray;
		char cn[COLNAME_LEN];
		dtRef->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,dtRef->GetColumnType(*colarray),dtRef->GetColumnLen(*colarray),
			dtRef->GetColumnScale(*colarray));
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL);
	}
	return true;
}


bool MemTable::SetCalCol(int *colarray)//only apply to src,-1 means end
{
	int i=0;
	char cn[COLNAME_LEN];
	if(!dtSrc) ReturnErr(DTG_ERR_INVALIDSRCDT);
	while(*colarray!=-1) {
		if(*colarray>dtSrc->GetColumnNum()) ReturnErr(DTG_ERR_OUTOFSRCCOL);
		calCol[i++]=*colarray;
		unsigned short tp=dtSrc->GetColumnType(*colarray);
		if(tp!=SQLT_NUM && tp!=SQLT_FLT && tp!=SQLT_INT)
			ReturnErr(DTG_ERR_NEEDNUMERICCOLTYPE);
		dtSrc->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		/*
		char cn2[COLNAME_LEN];
		strcpy(cn2,cn);
		strcat(cn,"_min");
		AddColumn(cn2,cn2,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		strcpy(cn2,cn);
		strcat(cn,"_max");
		AddColumn(cn2,cn2,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		*/
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL);
	}
	strcpy(cn,"rows_count");
	AddColumn(cn,cn,SQLT_INT);
	return true;
}

bool MemTable::Group(int rowstart,int rownum) {
	if(!dtSrc) ReturnErr(DTG_ERR_INVALIDSRCDT);
	char *bfsrc[MAX_COLUMN];//Full reference;
	char *bfref[MAX_COLUMN];// Full reference;
	if(rownum==0) rownum=dtSrc->GetRows();
	unsigned short srctp[MAX_COLUMN],reftp[MAX_COLUMN]; //Column type of both src and ref group column
	int sct=0,rct=0; // Column number of both src and ref group ones.
	char *gpsrc[MAX_COLUMN];//Group reference;
	char *gpref[MAX_COLUMN];//Group reference;
	// Set column reference point ,group number,and group coumn type.
	if(dtRef) {
		dtRef->RefAllCol(rowstart,bfref);
		//if(!dtRef->IsPKSet()) ReturnErr(DTG_ERR_MUSTSETREFPK);
	}
	else if(grpbyColRef[0]!=-1)
		ReturnErr(DTG_ERR_INVALIDREFDT);
	if(rowstart<0) ReturnErr(DTG_ERR_INVALIDSTARTROW);
	if(rownum<0) ReturnErr(DTG_ERR_INVALIDROWNUM);
	if(rowstart+rownum>dtSrc->GetRows())
		ReturnErr(DTG_ERR_OUTOFSRCROWS);
	dtSrc->RefAllCol(rowstart,bfsrc);
	while(grpbyColSrc[sct]!=-1) {
		srctp[sct]=dtSrc->GetColumnType(grpbyColSrc[sct]);
		gpsrc[sct]=bfsrc[grpbyColSrc[sct]];
		sct++;
	}
	while(grpbyColRef[rct]!=-1) {
		reftp[rct]=dtRef->GetColumnType(grpbyColRef[rct]);
		gpref[rct]=bfref[grpbyColRef[rct]];
		rct++;
	}
	// Begin group for every rows at source datatable
	void *ptr[MAX_COLUMN];
	int src;
	for(int strid=0;strid<rownum;strid++) {
		int rid=dtSrc->GetRawrnBySort(strid);
		unsigned int rffd=0; // Search result from Reference datatable, row number base 0;
		if(dtRef) {
			if(srckey!=-1)
			 rffd=dtRef->SearchPK(*((int *)(bfsrc[srckey]+sizeof(int)*rid)));
			else {
				for(src=0;src<keycolsrcct;src++) {
					ptr[src]=bfsrc[keycolsrc[src]]+rid*cd[keycolsrc[src]].dtsize;
				}
				ptr[src]=NULL;
				rffd=dtRef->Search(ptr);
			}
			if((int)rffd==-1)
				continue;
		}
		int i;
		for( i=0;i<sct;i++) 
			ptr[i]=gpsrc[i]+cd[i].dtsize*rid;
		for(;i<sct+rct;i++)
			ptr[i]=gpref[i-sct]+cd[i].dtsize*rffd;
		ptr[i]=NULL;
		//Search exists rows.
		// this is sorted version
		
		i=Search(ptr);
		if(i==-1) i=rowct;
		
		// Compare exists rows in this group datatable.
		//Replaced by Sorted version
		/*
		for( i=0;i<rowct;i++) {
			for(int j=0;j<sct;j++) { // Compare at source 
				int moff=i*cd[j].dtsize; // Offset of this group datatable
				int soff=rid*cd[j].dtsize; // Offset at source datatable
				if(srctp[j]==SQLT_INT) {
					if(*((int *)(cd[j].pCol+moff))!=*((int *)(gpsrc[j]+soff))) break;
				}
				else {
					if(stricmp(cd[j].pCol+moff,gpsrc[j]+soff)!=0) break;
				}
			}
			if(j!=sct) continue; // Not match at this group row .
			for(int k=0;k<rct;k++) { //Compare every group column at reference
				int moff=i*cd[j+k].dtsize;// Offset of this group datatable
				int soff=rffd*cd[j+k].dtsize;// Offset of referenced key row ;
				if(reftp[k]==SQLT_INT) {
					if(*((int *)(cd[j+k].pCol+moff))!=*((int *)(gpref[k]+soff))) break;
				}
				else {
					if(stricmp(cd[j+k].pCol+moff,gpref[k]+soff)!=0) break;
				}
			}
			if((j+k)==(sct+rct)) break; //when ==sct , match at this i row.
		}
		*/
		if(i==rowct) //not found matched item,add one.
		{
			if(rowct>=maxrows) ReturnErr(DTG_ERR_OUTOFDTGMAXROWS);
			// sum columns group by (both src and ref);
			int rof=sct+rct;
			int k;
			for( k=0;calCol[k]!=-1;k++) {
			 int r=rof+k;//*3;
			 ptr[r]=bfsrc[calCol[k]]+rid*cd[rof+k].dtsize;
			 ptr[r+1]=ptr[r];
			 ptr[r+2]=ptr[r];
			}
			int tmp=1;
			ptr[rof+k/**3*/]=&tmp;
			InsertRows(ptr,NULL,1);
			/*Replace by sorted version
			for(int j=0;j<sct;j++) { // Add source dt group column
				int dtsize=cd[j].dtsize;
				int moff=i*dtsize;
				int soff=rid*dtsize;
				memcpy(cd[j].pCol+moff,gpsrc[j]+soff,dtsize);
			}
 			for(int l=0;l<rct;l++) { //add reference dt group column
				int dtsize=cd[j+l].dtsize;
				int moff=i*dtsize;// Offset of this group datatable
				int soff=rffd*dtsize;// Offset of referenced key row ;
				memcpy(cd[j+l].pCol+moff,gpref[l]+soff,dtsize);
			}
			j=sct+rct;
			for(int k=0;calCol[k]!=-1;k++) { // Add calculate column.
				int r=j+k;
				int moff=i*cd[r].dtsize;
				int soff=rid*cd[r].dtsize;
				if(cd[r].type==SQLT_INT) {
					*((int *)(cd[r].pCol+moff))=*((int *)(bfsrc[calCol[k]]+soff));
				}
				else {
					*((double *)(cd[r].pCol+moff))=*((double *)(bfsrc[calCol[k]]+soff));
				}
			}
			
			int moff=i*cd[j+k].dtsize; // Counter column;
			*((int *)(cd[j+k].pCol+moff))=1; // Counter=1;
			rowct++;
			*/
			
		}
		else 
		{
			int j=sct+rct;
			int k;
			for( k=0;calCol[k]!=-1;k++) {
				int r=j+k;//*3;
				int moff=i*cd[r].dtsize;
				int soff=rid*cd[r].dtsize;
				if(cd[r].type==SQLT_INT) {
					int v=*((int *)(bfsrc[calCol[k]]+soff));
					*((int *)(cd[r].pCol+moff))+=v;
					/*
					if(v<*((int *)(cd[r+1].pCol+moff)) )
						*((int *)(cd[r+1].pCol+moff))=v;
					if(v>*((int *)(cd[r+2].pCol+moff)) )
						*((int *)(cd[r+2].pCol+moff))=v;
						*/
				}
				else {
					//*((double *)(cd[r].pCol+moff))+=*((double *)(bfsrc[calCol[k]]+soff));
					double v=*((double *)(bfsrc[calCol[k]]+soff));
					*((double *)(cd[r].pCol+moff))+=v;
					/*
					if(v<*((double *)(cd[r+1].pCol+moff)) )
						*((double *)(cd[r+1].pCol+moff))=v;
					if(v>*((double *)(cd[r+2].pCol+moff)) )
						*((double *)(cd[r+2].pCol+moff))=v;
						*/
				}
			}
			int moff=i*cd[j+k].dtsize; // Counter column;
			(*((int *)(cd[j+k].pCol+moff)))++; // Counter +1;
		}
	}
	return true;			
}

bool DataTable::RefAllCol(ub4 rowst,char **bf)
{
	for(unsigned int i=0;i<colct;i++) {
		if( rowst>=rowct ) ReturnErr(DT_ERR_OUTOFROW);
		char *tp=LocateCell(i,rowst);
		if(!tp) ReturnErr(DT_ERR_INVALIDPTR_ATREFALL);
		bf[i]=tp;
	}
	return true;
}

bool MemTable::Build(ub4 mxr)
{
	if(!DataTable::Build(mxr,true)) return false;
	
	if(dtSrc==NULL) return true;
	int sct=0,rct=0; // Column number of both src and ref group ones.
	while(grpbyColSrc[sct]!=-1) {
		sct++;
	}
	while(grpbyColRef[rct]!=-1) {
		rct++;
	}
	int ptr[MAX_COLUMN];
	int i;
	for( i=0;i<sct+rct;i++) 
		ptr[i]=i;
	ptr[i]=-1;
	//if(
		SetSortColumn(ptr) ;//) return false;
	return Sort();
	
	//return true;
}

bool DataTable::IsPKSet()
{
	return pkid!=-1;
}

bool DataTable::OrderByPK()
{
	if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK);
	if(!pPKSortedPos) {
		pPKSortedPos=new unsigned int [maxrows];
//		if(maxrows>500000) {
//			lgprintf("PK used :%d bytes",maxrows*sizeof(int));
//		}
	}
	if(rowct<1) return true;
	unsigned int n=rowct-1;
	unsigned int m=rowct/2-1;
	unsigned int i;
	for( i=0;i<rowct;i++) 
		pPKSortedPos[i]=i;
	int *pk;
	GetAddr(pkid,0,1,&pk);
	// Create heap;
	for(i=m;(int)i>=0;i--) 
		RebuildHeapPK(i,n,pk);
	// Order heap
	for(i=n;i>0;i--)
	{
		//int x=pk[i]; //exchange first one and last one.
		//pk[i]=pk[0];pk[0]=x;
		unsigned int x=pPKSortedPos[i]; //exchange first one and last one.
		pPKSortedPos[i]=pPKSortedPos[0];pPKSortedPos[0]=x;
		RebuildHeapPK(0,i-1,pk);
	}
	return true;
}

void DataTable::RebuildHeapPK(unsigned int m, unsigned int n, int *pk)
{
	unsigned int i=m,j=2*i+1;
	int x=pk[pPKSortedPos[i]];
	unsigned int xi=pPKSortedPos[i];
	while(j<=n) {
		if(j<n && pk[pPKSortedPos[j]]<pk[pPKSortedPos[j+1]]) j+=1; //取左右子树中的较大根结点
		if(x<pk[pPKSortedPos[j]]) {
			//pk[i]=pk[j];i=j;j=i*2; //继续调整
			pPKSortedPos[i]=pPKSortedPos[j];i=j;j=i*2+1; //继续调整
		}
		else break;
	}
	//pk[i]=x;
	pPKSortedPos[i]=xi;
}

bool DataTable::SetSortColumn(int *col)
{
	for(nSortColumn=0;*col!=-1;nSortColumn++,col++) 
		if(nSortColumn<colct)
		sortcolumn[nSortColumn]=*col;
		else ReturnErr(DT_ERR_OUTOFCOLUMN);
	if(!pSortedPos) {
		pSortedPos=new unsigned int [maxrows];
	}
	//nSortColumn++;
	for(unsigned int i=0;i<rowct;i++) 
		pSortedPos[i]=i;
	return true;
}

bool DataTable::Sort()
{
	if(nSortColumn<1) ReturnErr(DT_ERR_NOCOLUMNTOSORT);
	//if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK);
	if(!pSortedPos) {
		pSortedPos=new unsigned int [maxrows];
	}
	if(ptree) delete ptree;
	ptree=new AVLtree(this);

	for( int i=0;i<rowct;i++) 
	ptree->add_item(i);
	ptree->inorder((int *)pSortedPos);
//	pSortedPos[i]=i;
	// Create heap;
/*
	if(rowct<1) return true;
	int n=rowct-1;
	int m=rowct/2-1;

	  for(i=m;(int)i>=0;i--) 
		RebuildHeap(i,n);
	// Order heap
	for(i=n;i>0;i--)
	{
		//int x=pk[i]; //exchange first one and last one.
		//pk[i]=pk[0];pk[0]=x;
		unsigned int x=pSortedPos[i]; //exchange first one and last one.
		pSortedPos[i]=pSortedPos[0];pSortedPos[0]=x;
		RebuildHeap(0,i-1);
	}
*/
	return true;
}

void DataTable::RebuildHeap(unsigned int m, unsigned int n)
{
	unsigned int i=m,j=2*i+1;
	
	//int x=pk[pSortedPos[i]];
	unsigned int xp=pSortedPos[i];
	//CopySortRow(i,maxrows);
	unsigned int xi=pSortedPos[i];
	while(j<=n) {
		//if(j<n && pk[pSortedPos[j]]<pk[pSortedPos[j+1]]) j+=1; //取左右子树中的较大根结点
		if(j<n && CompareSortRow(pSortedPos[j],pSortedPos[j+1])<0) j+=1; //取左右子树中的较大根结点
		//if(x<pk[pSortedPos[j]]) {
		if(CompareSortRow(xp,pSortedPos[j])<0) {
			//pk[i]=pk[j];i=j;j=i*2; //继续调整
			pSortedPos[i]=pSortedPos[j];i=j;j=i*2+1; //继续调整
		}
		else break;
	}
	//pk[i]=x;
	pSortedPos[i]=xi;
}

bool DataTable::CompactBuf()
{
	if(rowct<1 || maxrows<1 || colct<1) 
		ReturnErr(DT_ERR_OUTOFRANGE);
	int off=cd[0].dtsize*rowct;
	for(ub4 i=1;i<colct;i++) {
		memcpy(pbf+off,
			cd[i].pCol,cd[i].dtsize*rowct);
		cd[i].pCol=pbf+off;
		off+=cd[i].dtsize*rowct;
	}
	maxrows=rowct;
	return true;
}

bool DataTable::CopyRowsTo(DataTable *tbTo,int toStart,int start,int rowsnum)
{
	if(toStart+rowsnum > tbTo->maxrows || start+rowsnum>rowct)
		ReturnErr(DT_ERR_OUTOFMEMORY);
	if(toStart==-1) toStart=tbTo->rowct;
	for(ub4 i=0;i<colct;i++) {
		memcpy(tbTo->cd[i].pCol+cd[i].dtsize*toStart,
			cd[i].pCol+cd[i].dtsize*start,cd[i].dtsize*rowsnum);
	}
	if(toStart==tbTo->rowct) 
	  tbTo->BuildAppendIndex(rowsnum);
	else if(toStart+rowsnum>tbTo->rowct)
	  tbTo->rowct=toStart+rowsnum;
	return true;
}

bool DataTable::GetColumnDesc(void **pColDesc,int &cdlen,int &_colnm) {
	if(colct<1) 
		ReturnErr(DT_ERR_COLUMNEMPTY);
	*pColDesc=(void *)(&cd);
	cdlen=sizeof(Column_Desc)*colct;
	_colnm=colct;
	return true;
}

bool DataTable::AppendRows(char *pData,int rnnum) 
{
	//pData=(void *)pbf;
	if(rowct+rnnum>maxrows) 
		ReturnErr(DT_ERR_OUTOFMEMORY);
	//_len=rowlen*rnnum;
	int off=0;
	for(ub4 i=0;i<colct;i++) {
		memcpy(cd[i].pCol+cd[i].dtsize*rowct,pData+off,cd[i].dtsize*rnnum);
		off+=cd[i].dtsize*rnnum;
	}
	rowct+=rnnum;
	return true;
}

	bool DataTable::ExportSomeRows(char *pData,int startrn,int rnnum) 
{
	//pData=(void *)pbf;
	if(startrn+rnnum>rowct) 
		ReturnErr(DT_ERR_OUTOFRANGE);
	//_len=rowlen*rnnum;
	int off=0;
	for(ub4 i=0;i<colct;i++) {
		memcpy(pData+off,
			cd[i].pCol+cd[i].dtsize*startrn,cd[i].dtsize*rnnum);
		off+=cd[i].dtsize*rnnum;
	}
	return true;
}

bool DataTable::Export(void *pData,int &_len,int &_maxrows,int &_rowct) 
{
	pData=(void *)pbf;
	_len=rowlen*maxrows;
	_maxrows=maxrows;
	_rowct=rowct;
	return true;
}

// Add _cdlen paramter for platform compatibility.
bool DataTable::Import(void *pData,int _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct)
{
	Clear();

	int cdl=_cdlen/_colnm;
	// Last member 'char *pbf' of Column_Desc count not cross platform.
	for(int i=0;i<_colnm;i++) {
		memcpy(cd+i,(char *)pColDesc+i*cdl,sizeof(Column_Desc)-sizeof(char *));
		cd[i].pCol=NULL;
	}
	memcpy(&cd,pColDesc,sizeof(Column_Desc)*_colnm);
	colct=_colnm;
	Build(_maxrows<=0?1:_maxrows,true);
	int off=0;
	if(_maxrows>0) {
		for(ub4 i=0;i<colct;i++) {
			memcpy(cd[i].pCol,(char *)pData+off,cd[i].dtsize*_rowct);
			off+=cd[i].dtsize*_rowct;
		}
		rowct=_rowct;
	}

	return true;
}

void DataTable::CopySortRow(unsigned int from, unsigned int to)
{
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			strcpy(cd[col].pCol+cd[col].dtsize*to,cd[col].pCol+cd[col].dtsize*from);
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			*((double *)(cd[col].pCol+cd[col].dtsize*to))=
				*((double *)(cd[col].pCol+cd[col].dtsize*from));
			break;
		case SQLT_INT:
			*((int *)(cd[col].pCol+cd[col].dtsize*to))=
				*((int *)(cd[col].pCol+cd[col].dtsize*from));
			break;
		case SQLT_DAT:
			memcpy(cd[col].pCol+cd[col].dtsize*to,cd[col].pCol+cd[col].dtsize*from,7);
			break;
		default:
			//Not impletemented.
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE);
		}
	}
}

// -1 :row1<row2 ; 0 : row1=row2 ; 1 : row1>row2
int DataTable::CompareSortRow(unsigned int row1, unsigned int row2)
{
	int cmp;
	double dres;
	int ires;
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			cmp=strcmp(cd[col].pCol+cd[col].dtsize*row1,cd[col].pCol+cd[col].dtsize*row2);
			if(cmp!=0) return cmp;
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			dres=*((double *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((double *)(cd[col].pCol+cd[col].dtsize*row2));
			if(dres<0) return -1;
			else if(dres>0) return 1;
			break;
		case SQLT_INT:
			ires=*((int *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((int *)(cd[col].pCol+cd[col].dtsize*row2));
			if(ires<0) return -1;
			else if(ires>0) return 1;
			break;
		case SQLT_DAT:
			cmp=memcmp(cd[col].pCol+cd[col].dtsize*row1,cd[col].pCol+cd[col].dtsize*row2,7);
			if(cmp!=0) return cmp;
			break;
		default:
			//Not impletemented.
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
		}
	}
	return 0;
}

void DataTable::GetColumnTitle(int col, char *str, int len)
{
	char fmt[300];
	str[0]=0;
	int l=0;
	char fmttmp[20];
	sprintf(fmttmp,"%%%ds",cd[col].dspsize);
	sprintf(fmt,fmttmp,cd[col].dispname);
	len=strlen(fmt);
	if(l<len)
	 strcat(str,fmt);
}

bool DataTable::GetCell(unsigned int row, int col, char *str,bool rawpos)
{
	char fmt[300];
	str[0]=0;
	if(!rawpos) {
	if(pSortedPos) {
		row=pSortedPos[row];
	}
	else if(pPKSortedPos) {
		row=pPKSortedPos[row];
	}
	}
	if((ub4 )row>rowct-1) ReturnErr(DT_ERR_OUTOFROW) ;
		char fmttmp[200];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			sprintf(fmttmp,"%%%ds ",cd[col].dspsize-1);
			sprintf(fmt,fmttmp,cd[col].pCol+row*cd[col].dtsize);
			strcat(str,fmt);
			break;
		case SQLT_NUM:
			sprintf(fmttmp,"%%%d.%df ",cd[col].prec,cd[col].scale);
			sprintf(fmt,fmttmp,*(double *)(cd[col].pCol+row*sizeof(double)));
			strcat(str,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmttmp,"%%%df ",10);
			sprintf(fmt,fmttmp,*(double *)(cd[col].pCol+row*sizeof(double)));
			strcat(str,fmt);
			break;
		case SQLT_INT:
			sprintf(fmttmp,"%%%dd ",10);
			sprintf(fmt,fmttmp,*(int *)(cd[col].pCol+row*sizeof(int)));
			strcat(str,fmt);
			break;
		case SQLT_DAT:
			{
				WOCIDate dt(cd[col].pCol+row*7);
				dt.GetString(fmt);
				strcat(str,fmt);
			}
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
	return true;
}

int DataTable::GetColumnDispWidth(int col)
{
	if((unsigned )col<colct) 
	 return int(cd[col].dspsize);
	else ReturnIntErr(DT_ERR_MISMATCHCOLID);
	return 0;
}

bool DataTable::GeneTable(char *tabname,WOCISession *psess)
{
	char crttab[2000];
	char fmt[200];
	sprintf(crttab,"create table %s (",tabname);
	for(unsigned int col=0;col<colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			sprintf(fmt,"%s varchar2(%d) ",cd[col].colname,cd[col].dtsize-1);
			strcat(crttab,fmt);
			break;
		case SQLT_NUM:
			sprintf(fmt,"%s number(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			strcat(crttab,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmt,"%s float ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_INT:
			if(cd[col].prec>0) 
				sprintf(fmt,"%s number(%d) ",cd[col].colname,cd[col].prec);
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_DAT:
			sprintf(fmt,"%s date ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
		if(col<colct-1) strcat(crttab,",");
	}
	strcat(crttab,")");
	
	WOCISession *ps=psess;
	if(psess==NULL)
		ps=pstmt->GetSession();
	WOCIStatment st(ps);
	 st.Prepare(crttab);
	 st.Execute(1);
	return true;
}


bool DataTable::CopyToTab(char *tabname, WOCISession *ps)
{
	char isttab[2000];
	char tmp[200];
	sprintf(isttab,"insert into %s values (",tabname);
	unsigned int col;
	for(col=0;col<colct;col++) {
		sprintf(tmp,":id%d ",col+1);
		strcat(isttab,tmp);
		if(col<colct-1) strcat(isttab,",");
	}
	strcat(isttab,")");
	WOCISession *pss;
	if(ps) pss=ps;
	else pss=pstmt->GetSession();
	WOCIStatment st(pss);
	st.SetEcho(false);
	st.Prepare(isttab);
	int i;
	for( i=StartRows;i<rowct;i+=50000) {
	 for(col=0;col<colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			st.BindByPos(col+1,(char *)(cd[col].pCol+i*cd[col].dtsize),cd[col].dtsize);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			st.BindByPos(col+1,(double *)(cd[col].pCol+i*cd[col].dtsize));
			break;
		case SQLT_INT:
			st.BindByPos(col+1,(int *)(cd[col].pCol+i*cd[col].dtsize));
			break;
		case SQLT_DAT:
			st.BindByPos(col+1,(char *)(cd[col].pCol+i*cd[col].dtsize));
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
	}
	 int rn=(rowct-i)>50000?50000:(rowct-i);
	 st.Execute(rn);
	 lgprintf(".");
#ifdef __unix
 	if(__output_to_console)
	 fflush(stdout);
#endif
 	}
	lgprintf("%d rows inserted.\n",rowct);
	return true;
}

bool MemTable::SetCalCol(char *colsnm)
{
	int gpr[100];
	if(!dtSrc->ConvertColStrToInt(colsnm,gpr)) return false;
	return SetCalCol(gpr);
}

bool MemTable::SetGroupColRef(char *colsnm)
{
	int gpr[100];
	if(!dtRef->ConvertColStrToInt(colsnm,gpr)) return false;
	return SetGroupColRef(gpr);
}

bool MemTable::SetGroupColSrc(char *colsnm)
{
	int gpr[100];
	if(!dtSrc->ConvertColStrToInt(colsnm,gpr)) return false;
	return SetGroupColSrc(gpr);
}

int DataTable::ConvertColStrToInt(const char *colsnm, int *pbf)
{
	char cn[COLNAME_LEN+1];
	if(colsnm==NULL) {
		for(int i=0;i<colct;i++) pbf[i]=i;
		return colct;
	}
	int len=strlen(colsnm);
	int np=0;
	int ip=0;
	int i;
	for( i=0;i<len;i++) {
		if(colsnm[i]==',') {
			if(np>0) {
				cn[np]=0;
				pbf[ip++]=GetColumnId(cn);
				np=0;
			}
		}
		else if(colsnm[i]!=' ') {
				cn[np++]=colsnm[i];
				if(np>COLNAME_LEN-1) 
					ReturnErr(DT_ERR_OUTOFCOLNAME);
		}
	}
	if(np>0) { // last column
		cn[np]=0;
		pbf[ip++]=GetColumnId(cn);
	}
	pbf[ip]=-1;
	return ip;
}

int DataTable::GetColumnId(char *cnm)
{
	unsigned int i;
	for( i=0;i<colct;i++) 
		if(STRICMP(cd[i].colname,cnm)==0) break;
	if(i==colct) ReturnIntErr(DT_ERR_INVALIDCOLNAME);
	return i;
}

bool DataTable::SetSortColumn(char *colsnm)
{
	int gpr[100];
	if(!ConvertColStrToInt(colsnm,gpr)) return false;
	return SetSortColumn(gpr);
}

void DataTable::SetStartRows(unsigned int st)
{
	StartRows=st;
}

WOCIStatment::WOCIStatment(WOCISession *s,bool atthr) {
		psess=s;
		rowct=0;
		sthp=NULL;		
		autoThrow=atthr;
		sqlstmt=new char[2000];
		eof=false;
		for(int i=0;i<MAX_COLUMN;i++) {
			dfp[i]=NULL;
			pind[i]=plen[i]=pret[i]=NULL;
		}
	}

void WOCIError::SetErrCode(int ec)
{
	errcode=ec;
}

//复制字段类型，追加到当前表的定义中
bool DataTable::CopyColumnDefine(const char *_colsname, DataTable *tbFrom)
{
	if(pbf) ReturnErr(DT_ERR_CLEARBEFOREADD);
	if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN);
	int grp[100];
	if(!tbFrom->ConvertColStrToInt(_colsname,grp)) return false;
	Column_Desc *pcd;
	int cdlen,colnm;
	tbFrom->GetColumnDesc((void **)&pcd,cdlen,colnm);
	for(int i=0;grp[i]!=-1;i++) 
	{
		int j;
		for(j=0;j<colct;j++) {
			if(STRICMP(cd[j].colname,pcd[grp[i]].colname)==0) break;
		}
		if(j!=colct) continue;
		memcpy(cd+colct,pcd+grp[i],sizeof(Column_Desc));
		colct++;
		if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN);
	}
	return true;
}


//destKey : true 关键字段在目标内存表中指定
//			flase 关键字段在源内存表中指定
// 使用分号(;)分隔关键字段和数值字段
//  各字段间用逗号(,)分隔
// 使用关键字段的内存表要事先做关键字段设定或按顺序做排序操作
bool DataTable::ValuesSetTo(char *_colto, char *_colfrom, DataTable *tbFrom,bool destKey,int op)
{
	bool usePK=false;
	char colto[1000],colfrom[1000];
	strcpy(colto,_colto);strcpy(colfrom,_colfrom);
	int ckto[MAX_COLUMN],cdto[MAX_COLUMN]; //column dest(to) key and value(data)
	int p;
	
	//分解目标字段
	char *strp=colto;
	while(*strp) {if(*strp==';' && *strp) break;strp++;}
	p=strp-colto;
	if(p>=strlen(colto)) p=0;
	//取数值字段
	if(!ConvertColStrToInt(colto+p+(p==0?0:1),cdto)) return false;
	int dstdct=0; // source key column count
	while(cdto[dstdct]!=-1) dstdct++;
	if(dstdct<1) ReturnErr(DT_ERR_DATACOLNULL);
	//截断字符串
	colto[p]=0;
	for(p=0;p<dstdct;p++) 
		if(cd[cdto[p]].type!=SQLT_NUM && 
			cd[cdto[p]].type!=SQLT_FLT && 
			cd[cdto[p]].type!=SQLT_INT)
		ReturnErr(DT_ERR_INVDATACOLTYPE);
	//取关键字段
	if(!ConvertColStrToInt(colto,ckto)) return false;
	//得到关键字的字段数
	int dstkct=0; // source key column count
	while(ckto[dstkct]!=-1) dstkct++;
	//if(dstkct<1) ReturnErr(DT_ERR_KEYCOLNULL);
	//是否使用PK,检查是否已排序
	bool ttt=IsPKSet();
	if(destKey && IsPKSet()) 
		usePK=true;
	else if(destKey) {
		//if(dstkct!=nSortedColumn) ReturnErr(DT_ERR_SOMESORTEDCOLNOTUSED);
		if(!pSortedPos) ReturnErr(DT_ERR_COLUNSORTED);
		//for(p=0;p<dstkct;p++) 
		//	if(sortcolumn[p]!=ckto[p]) ReturnErr(DT_ERR_MISMATCHSORTED);
	}
	//分解源表字段
	int cdfrom[MAX_COLUMN],ckfrom[MAX_COLUMN];
	strp=colfrom;
	while(*strp) {if(*strp && *strp==';') break;strp++;}
	p=strp-colfrom;
	if(p>=strlen(colfrom)) p=0;
	//取数值字段
	if(!tbFrom->ConvertColStrToInt(colfrom+p+(p==0?0:1),cdfrom)) return false;
	int srcdct=0; // source key column count
	while(cdfrom[srcdct]!=-1) srcdct++;
	if(srcdct!=dstdct) ReturnErr(DT_ERR_DATACOLMISMATCH);
	//截断字符串
	colfrom[p]=0;
	for(p=0;p<srcdct;p++) 
		if(tbFrom->cd[cdfrom[p]].type!=SQLT_NUM && 
			tbFrom->cd[cdfrom[p]].type!=SQLT_FLT && 
			tbFrom->cd[cdfrom[p]].type!=SQLT_INT)
		ReturnErr(DT_ERR_INVDATACOLTYPE);
	//取关键字段
	if(!tbFrom->ConvertColStrToInt(colfrom,ckfrom)) return false;
	//得到关键字的字段数
	int srckct=0; // source key column count
	while(ckfrom[srckct]!=-1) srckct++;
	if(destKey && srckct==0) ReturnErr(DT_ERR_KEYCOLUMNEMPTY);
	if(!destKey && dstkct==0) ReturnErr(DT_ERR_KEYCOLUMNEMPTY);
	//if(srckct!=dstkct) ReturnErr(DT_ERR_KEYCOLMISMATCH);
	//是否使用PK,检查是否已排序
	if(!destKey && dstkct==1 && tbFrom->IsPKSet() &&
		GetColumnType(ckto[0])==SQLT_INT) {
		usePK=true;
	}
	else if(destKey && srckct==1 && IsPKSet() &&
		tbFrom->GetColumnType(ckfrom[0])==SQLT_INT) {
		usePK=true;
	}
	else {
		if(destKey) {//使用目标内存表做关键索引
			if(pSortedPos) ReturnErr(DT_ERR_COLUNSORTED);
			for(p=0;p<srckct;p++) 
				if(tbFrom->GetColumnType(ckfrom[p]) !=
					GetColumnType(sortcolumn[p]))
					  ReturnErr(DT_ERR_MISMATCHSORTED);
		}
		else {//使用源内存表做关键索引
			if(!tbFrom->pSortedPos) ReturnErr(DT_ERR_COLUNSORTED);
			for(p=0;p<dstkct;p++) 
				if(GetColumnType(ckto[p]) !=
					tbFrom->GetColumnType(tbFrom->sortcolumn[p]))
					  ReturnErr(DT_ERR_MISMATCHSORTED);
		}
	}

	void *dptr[MAX_COLUMN];
	void *kptr[MAX_COLUMN];
	int klen[MAX_COLUMN];
	for(p=0;p<srcdct;p++) 
		tbFrom->GetAddr(cdfrom[p],&dptr[p],0);
	int rn=0;
	if(destKey) {
		rn=tbFrom->GetRows();
		for(p=0;p<srckct;p++) {
			tbFrom->GetAddr(ckfrom[p],&kptr[p],0);
			klen[p]=tbFrom->cd[ckfrom[p]].dtsize;
		}
		for(p=0;p<rn;p++) {
			int schd=-1;
			if(usePK) 
			 schd=(int )SearchPK(((int*)kptr[0])[p]);
			else {
				void *rckptr[MAX_COLUMN];
				for(int i=0;i<srckct;i++) 
					rckptr[i]=(char *)kptr[i]+p*klen[i];
				schd=Search(rckptr);
			}
			if(schd!=-1) { 
				//数据设置
				for(int i=0;i<srcdct;i++) {
					if(cd[cdto[i]].type!=SQLT_INT) {
						if(op==0) {
						((double *)cd[cdto[i]].pCol)[schd]=
						((double *)dptr[i])[p];
						}
						else if(op==1) {
						((double *)cd[cdto[i]].pCol)[schd]+=
						((double *)dptr[i])[p];
						}
						else if(op==2)
							((double *)cd[cdto[i]].pCol)[schd]-=
						((double *)dptr[i])[p];

					}
					else {
						if(op==0) {
						((int *)cd[cdto[i]].pCol)[schd]=
						((int *)dptr[i])[p];
						}
						else if(op==1) {
						((int *)cd[cdto[i]].pCol)[schd]+=
						((int *)dptr[i])[p];
						}
						else if(op==2)
							((int *)cd[cdto[i]].pCol)[schd]-=
						((int *)dptr[i])[p];
					}
				}
			}
		}
	} 
	else	{
		rn=GetRows();
		for(p=0;p<dstkct;p++) {
			GetAddr(ckto[p],&kptr[p],0);
			klen[p]=cd[ckto[p]].dtsize;
		}
		for(p=0;p<rn;p++) {
			int schd=-1;
			if(usePK) 
			 schd=(int )tbFrom->SearchPK(((int*)kptr[0])[p]);
			else {
				void *rckptr[MAX_COLUMN];
				for(int i=0;i<srckct;i++) 
					rckptr[i]=(char *)kptr[i]+p*klen[i];
				schd=tbFrom->Search(rckptr);
			}
			if(schd!=-1) { 
				//数据设置
				for(int i=0;i<srcdct;i++) {
					if(cd[cdto[i]].type!=SQLT_INT) {
						if(op==0) {
						((double *)cd[cdto[i]].pCol)[schd]=
						((double *)dptr[i])[p];
						}
						else if(op==1) {
						((double *)cd[cdto[i]].pCol)[schd]+=
						((double *)dptr[i])[p];
						}
						else if(op==2)
							((double *)cd[cdto[i]].pCol)[schd]-=
						((double *)dptr[i])[p];

					}
					else {
						if(op==0) {
						((int *)cd[cdto[i]].pCol)[schd]=
						((int *)dptr[i])[p];
						}
						else if(op==1) {
						((int *)cd[cdto[i]].pCol)[schd]+=
						((int *)dptr[i])[p];
						}
						else if(op==2)
							((int *)cd[cdto[i]].pCol)[schd]-=
						((int *)dptr[i])[p];
					}
				}
			}
		}
	}
	return true;
}

bool DataTable::GetAddr(int colid, void **buf,int off)
{
	if(colid>=colct)
		ReturnErr(DT_ERR_OUTOFCOLUMN);
	if( off>=rowct ) ReturnErr(DT_ERR_OUTOFROW);
	char *tp=LocateCell(colid,0);
	tp+=off*cd[colid].dtsize;
	*buf=tp;
	return true;
}

int DataTable::Search(void **ptr,int schopt)
{
	if(schopt==0 && ptree) {
		int *pos=ptree->search(ptr);
		if(pos==NULL) return -1;
		return *pos;
		//Search(ptr,
	}
	int head=0;
	int tail=rowct-1;
	int mid=0;
    if(rowct==0) {
		if(schopt==2) return 0;
		return -1;
	}
	if(pSortedPos==NULL) ReturnIntErr(DT_ERR_COLUNSORTED);
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		int r=CompareSortedColumn(pSortedPos[mid],ptr);
		if(r>0) tail=mid;
		else if(r<0) head=mid;
		else return (schopt!=0)?mid:pSortedPos[mid];
	}
	if(CompareSortedColumn(pSortedPos[tail],ptr)==0) 
		return (schopt!=0)?tail:pSortedPos[tail];
	if(CompareSortedColumn(pSortedPos[head],ptr)==0) 
		return (schopt!=0)?head:pSortedPos[head];
	if(schopt==2) {
		if(CompareSortedColumn(pSortedPos[tail],ptr)<0)
			return tail+1;
		if(CompareSortedColumn(pSortedPos[mid],ptr)<0) 
			return mid+1;
		if(CompareSortedColumn(pSortedPos[head],ptr)<0) 
			return head+1;
		return head;
	}
	return -1;
}

int DataTable::CompareSortedColumn(unsigned int row1, void **ptr)
{
	int cmp;
	double dres;
	int ires;
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			cmp=STRICMP(cd[col].pCol+cd[col].dtsize*row1,(char *)ptr[i]);
			if(cmp!=0) return cmp;
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			dres=*((double *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((double *)(ptr[i]));
			if(dres<0) return -1;
			else if(dres>0) return 1;
			break;
		case SQLT_INT:
			ires=*((int *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((int *)(ptr[i]));
			if(ires<0) return -1;
			else if(ires>0) return 1;
			break;
		case SQLT_DAT:
			cmp=memcmp(cd[col].pCol+cd[col].dtsize*row1,ptr[i],7);
			if(cmp!=0) return cmp;
			break;
		default:
			//Not impletemented.
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
		}
	}
	return 0;
}

//21.112.1.52
bool MemTable::SetSortedGroupRef(DataTable *ref, char *colssrc)
{
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF);
	if(ref==NULL)
		ReturnErr(DTG_ERR_PARMREFISNULL)
	if(ref->GetColumnNum()<1) 
		ReturnErr(DTG_ERR_NOCOLUMNSINREF);
	if(!dtSrc->ConvertColStrToInt(colssrc,keycolsrc)) return false;
	while(keycolsrc[keycolsrcct]!=-1) {
		if(ref->sortcolumn[keycolsrcct]==-1) ReturnErr(DTG_ERR_SORTEDCOLNOTFOUND);
		if(ref->GetColumnType(ref->sortcolumn[keycolsrcct])!=
				dtSrc->GetColumnType(keycolsrc[keycolsrcct]))
				ReturnErr(DTG_ERR_NOTMATCHSORTEDCOLTYPE);

		keycolsrcct++;
	}
	if(keycolsrcct!=ref->nSortColumn) ReturnErr(DTG_ERR_SOMESORTEDCOLNOTUSED);
	dtRef=ref;
	return true;
}

int DataTable::GetMemUsed()
{
	int used=bflen;
	if(pSortedPos) 
		used+=sizeof(int)*maxrows;
	if(pPKSortedPos)
		used+=sizeof(int)*maxrows;
	if(pQDelKey) 
	  used+=sizeof(int)*maxrows;
	return used;
}

bool DataTable::DeleteRow(int rown)
{
	if(rown>=rowct || rown<0)
		ReturnErr(DT_ERR_OUTOFROW);
	int i;
	if(pPKSortedPos) {
		int key=((int *)(cd[pkid].pCol))[rown];
		int pos=SearchPK(key,1);
		if(pos<0) ReturnErr(DT_ERR_OUTOFROW);
		memmove(pPKSortedPos+pos,pPKSortedPos+pos+1,
			(rowct-pos-1)*sizeof(int));
		for(i=0;i<rowct-1;i++)
			if(pPKSortedPos[i]>=rown)
				pPKSortedPos[i]--;

	}
	if(pSortedPos) {
		//非PK类排序，删除记录仅调整pSortedPos的值。
		char *ptr[MAX_COLUMN];
		for(i=0;i<nSortColumn;i++) 
			ptr[i]=cd[sortcolumn[i]].pCol+rown*cd[sortcolumn[i]].dtsize;
		int pos=Search((void **)ptr,1);
		if(pos<0) ReturnErr(DT_ERR_OUTOFROW);
		memmove(pSortedPos+pos,pSortedPos+pos+1,
			(rowct-pos-1)*sizeof(int));
		for(i=0;i<rowct-1;i++)
			if(pPKSortedPos[i]>=rown)
				pPKSortedPos[i]--;
	}
	for( i=0;i<colct;i++) 
		memmove(cd[i].pCol+rown*cd[i].dtsize,
			cd[i].pCol+(rown+1)*cd[i].dtsize,
			(rowct-rown-1)*cd[i].dtsize);
	rowct--;
	return true;
}

bool DataTable::InsertRows(void **ptr, char *colsname,int num)
{
	int colrf[MAX_COLUMN],i;
	if(rowct+num>maxrows) ReturnErr(DT_ERR_OUTOFMEMORY);
	if(colsname!=NULL) 
		ConvertColStrToInt(colsname,colrf);
	else {
		for(i=0;i<colct;i++) 
		 if(ptr[i]!=NULL)
			colrf[i]=i;
		 else break;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	for(i=0;i<colnum;i++) 
		memcpy(cd[colrf[i]].pCol+rowct*cd[colrf[i]].dtsize,
			ptr[i],cd[colrf[i]].dtsize*num);
	BuildAppendIndex(num);
	return true;
}

void DataTable::BuildAppendIndex(int num) {	
	int i;
	for( i=0;i<num;i++) {
		if(pPKSortedPos) {
			int key=((int *)(cd[pkid].pCol))[rowct];
			int pos=SearchPK(key,2);
			memmove(pPKSortedPos+pos+1,pPKSortedPos+pos,
				(rowct-pos)*sizeof(int));
			pPKSortedPos[pos]=rowct;
		}
		if(pSortedPos) {
			if(ptree) ptree->add_item(rowct);
			else {
				char *ptr[MAX_COLUMN];
				for(int j=0;j<nSortColumn;j++) 
				ptr[j]=cd[sortcolumn[j]].pCol+rowct*cd[sortcolumn[j]].dtsize;
			
				int pos=Search((void **)ptr,2);
			
			//static int ct=0,tot=0;
			//tot+=(rowct-pos)*sizeof(int);
			//if(++ct==99900) 
			//	printf("Total moved %d\n",tot);
			//pos=rowct;
			
				memmove(pSortedPos+pos+1,pSortedPos+pos,
					(rowct-pos)*sizeof(int));
					pSortedPos[pos]=rowct;
			}
		}
		rowct++;
	}
}

bool DataTable::BindToStatment(WOCIStatment *_pstmt, char *colsname, int rowst)
{
	if(!_pstmt) ReturnErr(DT_ERR_EMPTYSTMT);
	if(rowst>=rowct || rowst<0) ReturnErr(DT_ERR_OUTOFROW);
	int colrf[MAX_COLUMN],i;
	if(colsname!=NULL) 
		ConvertColStrToInt(colsname,colrf);
	else 
		for(i=0;i<colct;i++) colrf[i]=i;
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	for(i=0;i<colnum;i++) {
		switch(cd[colrf[i]].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			_pstmt->BindByPos(i+1,cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst
				,cd[colrf[i]].dtsize);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			_pstmt->BindByPos(i+1,(double *)(cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst));
			break;
		case SQLT_INT:
			_pstmt->BindByPos(i+1,(int *)(cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst));
			break;
		case SQLT_DAT:
			_pstmt->BindByPos(i+1,cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst);
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
	}
	return true;
}

int WOCIStatment::TestTable(char *tbname)
{
#ifndef NO_DBMS
  sword     retval;
  OCIDescribe  *dschp = (OCIDescribe *)0;

  OCIParam *parmp; 
//  ub4       parmcnt;
  ub2       numcols;

  if((retval=OCIHandleAlloc((dvoid *) envhp, (dvoid **) &dschp,
                           (ub4) OCI_HTYPE_DESCRIBE,
                           (size_t) 0, (dvoid **) 0)))
						   return -1;

  if ((retval = OCIDescribeAny(psess->svchp, errhp, (dvoid *)tbname,
                               (ub4) strlen((char *) tbname),
                               OCI_OTYPE_NAME, (ub1)1,
                               OCI_PTYPE_TABLE, dschp)) != OCI_SUCCESS)
  {
	retval=0;
  }
  else
  {
                                           /* get the parameter descriptor */
    retval=OCIAttrGet((dvoid *)dschp, (ub4)OCI_HTYPE_DESCRIBE,
                         (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
                         (OCIError *)errhp);
	checkerr(retval);
                                        /* Get the attributes of the table */
                                               /* column list of the table */
                                                      /* number of columns */
    retval=OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &numcols, (ub4 *) 0,
                         (ub4) OCI_ATTR_NUM_COLS, (OCIError *)errhp);
                                               /* now describe each column */
	checkerr(retval);
    retval=numcols;
  }
  OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);
#endif
  return retval;	
}

bool DataTable::QDeleteRow(int rownm)
{
	//if(pkid==-1) ReturnErr(DT_ERR_PKNOTDEFINED);
	if(!pQDelKey) {
		pQDelKey=new int [maxrows];
		if(!pQDelKey) ReturnErr(DT_ERR_OUTOFMEMORY);
		memset(pQDelKey,0,maxrows*sizeof(int));
	}
	if(pQDelKey[rownm]==1) return false;
	pQDelKey[rownm]=1;
	qdelct++;
	/*
	if(IsQDelete(rownm)) return false;
	int key=rownm;//((int *)cd[pkid].pCol)[rownm];
	int ins=SearchQDel(key,2);
	memmove(pQDelKey+ins+1,pQDelKey+ins,(qdelct-ins)*sizeof(int));
	pQDelKey[ins]=key;
	qdelct++;
	if(qdelct>qdelmax-20) {
		int *ptr=new int[qdelmax+1000];
		memcpy(ptr,pQDelKey,qdelct*sizeof(int));
		delete pQDelKey;
		pQDelKey=ptr;
		qdelmax+=1000;
	}
	*/
	return true;
}

int DataTable::QDeletedRows()
{
	return qdelct;
}

bool DataTable::IsQDelete(int rowrn)
{
	//int key=((int *)cd[pkid].pCol)[rowrn];
	return pQDelKey==NULL?false:pQDelKey[rowrn]==1;//SearchQDel(rowrn,0)!=-1;
}

int DataTable::SearchQDel(int key, int schopt)
{
	
	if(qdelct==0 || pQDelKey==NULL) return schopt==2?0:-1;
	//if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED);
	int head=0;
	int tail=qdelct-1;
	int mid=0;
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(pQDelKey[mid]>key) tail=mid;
		else if(pQDelKey[mid]<key) head=mid;
		else return mid;
	}
	if(pQDelKey[tail]==key) 
		return tail;
	if(pQDelKey[head]==key) 
		return head;
	if(schopt==2) {
		if(pQDelKey[tail]<key)
			return tail+1;
		if(pQDelKey[mid]<key) 
			return mid+1;
		if(pQDelKey[head]<key) 
			return head+1;
		return head;
	}
	return -1;
}

bool DataTable::CompressBf()
{
	if(qdelct<=0 || pQDelKey==NULL) return true;
	int mt=0,mts=0,mtct=0;//MoveToPosition,MovoToStart(From),MoveToCount
	//int *kbf=(int *)cd[pkid].pCol;
	int i;
	for( i=0;i<rowct;i++) {
		if(pQDelKey[i]==1) { //SearchQDel(i,0)!=-1) {
			//Found a deleted row
			if(mtct>0) { 
				//Previous row(s) not be deleted.Do move...
				for(int j=0;j<colct;j++) {
					memmove(cd[j].pCol+mt*cd[j].dtsize,
						cd[j].pCol+mts*cd[j].dtsize,mtct*cd[j].dtsize);
				}
				//i-=mtct;
				mt+=mtct;
				mts=i+1;mtct=0;
			}
			else
				//Previous row(s) be deleted too.
				mts++;
		}
		else 
			//Found a undeleted row
			mtct++;
	}
	if(mtct>0) {
				//The lastest rows will be moved.
				for(int j=0;j<colct;j++) {
					memmove(cd[j].pCol+mt*cd[j].dtsize,
						cd[j].pCol+mts*cd[j].dtsize,mtct*cd[j].dtsize);
				}
	}
	rowct-=qdelct;
	qdelct=0;
	memset(pQDelKey,0,maxrows*sizeof(int));
	if(pkid!=-1)
	OrderByPK();
	if(pSortedPos)
		Sort();
	return true;
}

int DataTable::GetRawrnByPK(int ind)
{
	if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED);
	if(ind<0 || ind >rowct) ReturnIntErr(DT_ERR_OUTOFROW);
	if(pPKSortedPos) return pPKSortedPos[ind];
	return ind;
}

int DataTable::GetRawrnBySort(int ind)
{
	if(!pSortedPos) return ind;//ReturnIntErr(DT_ERR_COLUNSORTED);
	if(ind<0 || ind >rowct) ReturnIntErr(DT_ERR_OUTOFROW);
	return pSortedPos[ind];
}

bool DataTable::FreshRowNum()
{
	rowct=pstmt->GetRows();
	if(rowct==maxrows) {
		errprintf("***Warning : Memory table is full on BatchSelect!\n");
		if(echo) {
		printf("Press q to exit or any other key to continue ...");
		fflush(stdout);
		int ch=getch();
		if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		printf("\n");
		}
	}
	return true;
}

int DataTable::GetMaxRows()
{
	return maxrows;
}


int WOCISession::TestTable(char *tabname)
{
	WOCIStatment st(this);
	return st.TestTable(tabname);
}

void WOCIError::SetEcho(bool val)
{
 echo=val;
}

bool MemTable::SetGroupRef(DataTable *ref, char *colnm)
{
	int gpr[100];
	if(!dtSrc->ConvertColStrToInt(colnm,gpr)) return false;
	return SetGroupRef(ref,gpr[0]);
}

void DataTable::GetMTName(char *bf)
{
 strcpy(bf,dtname);
}

void wociSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) ;

int DataTable::ReadFromTextFile(char *fn, int rowst, int rownm)
{
	FILE *fp=fopen(fn,"rt");
	if(fp==NULL) ReturnErr(DT_ERR_CANNOTOPENTEXTFILE);
	//char skip[1000];
	int linect=1;
	if(rownm>(maxrows-rowct) || rownm==0) rownm=maxrows-rowct;
	while(linect++<rowst)
	 while(fgetc(fp)!='\n');
	char fmt[MAX_COLUMN][200];
	char *ptr[MAX_COLUMN];
	int i;
	for( i=0;i<colct;i++) {
		//if(i!=0) {
		//	strcat(fmt,"%*[ ,\t]");
		ptr[i]=(char *)cd[i].pCol+cd[i].dtsize*rowct;
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			sprintf(fmt[i],"%%%dc",cd[i].dtsize-1);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			strcpy(fmt[i],"%f");
			break;
		case SQLT_INT:
			strcpy(fmt[i],"%d");
			break;
		case SQLT_DAT:
			strcpy(fmt[i],"%19c");
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
	}
	int rdrowct=0;
	try {
	 while(linect++<rowst+rownm) {
	  for(i=0;i<colct;i++) {
		ptr[i]+=cd[i].dtsize;
		if(cd[i].type==SQLT_DAT) {
				int y,m,d,h,mi,s;
			    fscanf(fp,"%4d/%2d/%2d %2d:%2d:%2d",&y,&m,&d,&h,&mi,&s);
				wociSetDateTime(ptr[i],y,m,d,h,mi,s);
		}
		else
			fscanf(fp,fmt[i],ptr[i]);
		fscanf(fp,"%*[ ,\t]");
	  }
	  while(fgetc(fp)!='\n');
	  rdrowct++;
	 }	 
	}
	catch (WOCIError &e) {
		fclose(fp);
		throw e;
	}
	catch (...) {
		errprintf("Read from text file failed,%d row readed.\n",rdrowct);
	}
	fclose(fp);
	rowct+=rdrowct;
	return rdrowct;
}

int DataTable::ReplaceStmt(WOCIStatment *_pstmt)
{
	pstmt=_pstmt;
	return true;
}



int DataTable::LoadSort(FILE * fp)
{
	int len=0;
	fread(&len,sizeof(int),1,fp);
	if(len<0 || len!=rowct || len>maxrows) return -1;
	if(!pSortedPos)
		 pSortedPos=new unsigned int [maxrows];
	if(fread(pSortedPos,sizeof(int),rowct,fp)!=rowct) return -2;
	return 1;
}

int DataTable::SaveSort(FILE * fp)
{
	if(!pSortedPos) return -1;
	fwrite(&rowct,sizeof(int),1,fp);
	if(fwrite(pSortedPos,sizeof(int),rowct,fp)!=rowct) return -2;
	return 1;
}

void DataTable::ReInOrder()
{
	if(!pSortedPos || !ptree) return;
	ptree->inorder((int *)pSortedPos);
}

void DataTable::destroyptree()
{
		if(ptree) {
			delete ptree;
			ptree=NULL;
		}
}

void DataTable::AddrFresh(char **colval, int *collen,int *coltp)
{
	for(int i=0;i<colct;i++) {
		colval[i]=cd[i].pCol;
		collen[i]=cd[i].dtsize;//GetColumnLen(i);
		coltp[i]=cd[i].type;
	}
}

int DataTable::GetCreateTableSQL(char *crttab,const char *tabname,bool ismysql)
{
	char fmt[200];
	sprintf(crttab,"create table %s (",tabname);
	for(unsigned int col=0;col<colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			if(ismysql) {
			  if(cd[col].dtsize>256)
				ReturnIntErr(DT_ERR_OUTOFCOLUMNWIDE);
			  if(cd[col].dtsize-1<4)
				sprintf(fmt,"%s char(%d) ",cd[col].colname,cd[col].dtsize-1);
			  else
				sprintf(fmt,"%s varchar(%d) ",cd[col].colname,cd[col].dtsize-1);
			}
			else sprintf(fmt,"%s varchar2(%d) ",cd[col].colname,cd[col].dtsize-1);
			strcat(crttab,fmt);
			break;
		case SQLT_NUM:
			if(ismysql) 
				sprintf(fmt,"%s decimal(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			else
				sprintf(fmt,"%s number(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			strcat(crttab,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmt,"%s float ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_INT:
			if(cd[col].prec>0) {
				if(ismysql) 
					sprintf(fmt,"%s integer(%d) ",cd[col].colname,cd[col].prec);
				else
					sprintf(fmt,"%s number(%d) ",cd[col].colname,cd[col].prec);
			}
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_DAT:
			if(ismysql)
				sprintf(fmt,"%s datetime ",cd[col].colname);
			else
				sprintf(fmt,"%s date ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		default:
			ReturnIntErr(DT_ERR_INVALIDCOLUMNTYPE);
			break;
		}
		if(col<colct-1) strcat(crttab,",");
	}
	strcat(crttab,")");
	return strlen(crttab);
}

#define SetBit(bf,colid) bf[colid/8]|=(0x1<<(colid%8))
#define ResetBit(bf,colid) bf[colid/8]&=~(0x1<<(colid%8))

void DataTable::CopyToMySQL(unsigned int startrow, unsigned int rownum, FILE *fp)
{
	char bhdr[60];
	char varflag[40];
	char nullflag[40];
	char fmttmp[100];
	char fmt[100];
	char recbuf[3000];
	if(rownum==0) 
		rownum=rowct-startrow;
	if(startrow+rownum>rowct) ReturnVoidErr(DT_ERR_OUTOFROW);
	int i;
	for( i=startrow;i<startrow+rownum;i++) {
		int pos=i;
		if(pkid!=-1) pos=GetRawrnByPK(i);
		else if(pSortedPos) pos=GetRawrnBySort(i);
		int dtlen=0;
		char *src;
		int slen=0;
		if(i>1000) {
			int stoph=1;
		}
		if(i>64402) {
			int stoph=1;
		}
		if(i>100000) {
			int stoph=1;
		}

		memset(varflag,0,40);
		memset(nullflag,0xff,40);
		int varcol=0;
		for(unsigned int col=0;col<colct;col++,varcol++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			src=cd[col].pCol+cd[col].dtsize*pos;
			slen=strlen(src);
			if(cd[col].dtsize-1<4) {
				strcpy(recbuf+dtlen,src);
				memset(recbuf+dtlen+slen,' ',cd[col].dtsize-slen-1);
				dtlen+=cd[col].dtsize-1;
			    //ResetBit(varflag,varcol);
				ResetBit(nullflag,col);
				varcol--;
			}
			else {
				if(slen==0) {
					SetBit(varflag,varcol);
					SetBit(nullflag,col);
					recbuf[dtlen++]=0;
				}
				else if(cd[col].dtsize-slen<3) {
				 strcpy(recbuf+dtlen,src);
				 //_strnset(recbuf,' ',cd[col].dtsize-1-slen);
				 memset(recbuf,' ',cd[col].dtsize-1-slen);
				 ResetBit(varflag,varcol);
				 ResetBit(nullflag,col);
				 dtlen+=cd[col].dtsize-1;
				}
				else {
				 strcpy(recbuf+dtlen+1,src);
				 recbuf[dtlen]=slen;
				 SetBit(varflag,varcol);
				 ResetBit(nullflag,col);
				 dtlen+=slen+1;
				}
			}
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			sprintf(fmttmp,"%%1.%df ",cd[col].scale);
			sprintf(fmt,fmttmp,*(double *)(cd[col].pCol+pos*sizeof(double)));
			slen=strlen(fmt);
			if(slen>cd[col].prec) ReturnVoidErr(DT_ERR_OUTOFCOLUMNWIDE);
			if(slen==cd[col].prec) {
				recbuf[dtlen]=' ';
				strcpy(recbuf+dtlen+1,fmt);
				ResetBit(varflag,varcol);
				ResetBit(nullflag,col);
				dtlen+=slen+1;
			}
			else {
				strcpy(recbuf+dtlen+1,fmt);
				recbuf[dtlen]=slen;
				dtlen+=slen+1;
				SetBit(varflag,varcol);
				ResetBit(nullflag,col);
			}
			break;
		case SQLT_INT:
			//if(cd[col].type==SQLT_INT) {
				//sprintf(fmttmp,"%%%dd ",10);
				//if(cd[col].prec==0) {
					*(int *)(recbuf+dtlen)=((int *)cd[col].pCol)[pos];//*sizeof(int));
					slen=sizeof(int);
					dtlen+=slen;
					ResetBit(varflag,varcol);
					ResetBit(nullflag,col);
					break;
				//}
				//sprintf(fmt,"%d",*(int *)(cd[col].pCol+pos*sizeof(int)));
			//}
		case SQLT_DAT:
			src=cd[col].pCol+cd[col].dtsize*pos;
			if(*src==0) {
				SetBit(varflag,varcol);
				SetBit(nullflag,col);
			}
			else {
				
				#define LL(A)		((__int64) A)
				typedef __int64 longlong;
				
				longlong mdt=0;
				mdt=LL(10000000000)*((src[0]-100)*100+src[1]-100);
			    mdt+=LL(100000000)*src[2];
				mdt+=LL(1000000)*src[3];
				mdt+=LL(10000)*(src[4]-1);
				mdt+=100*(src[5]-1);
				mdt+=src[6]-1;
				memcpy(recbuf+dtlen,&mdt,sizeof(longlong));
				dtlen+=sizeof(longlong);
				ResetBit(varflag,varcol);
				ResetBit(nullflag,col);
			}
			break;
			//memcpy(cd[col].pCol+cd[col].dtsize*pos,cd[col].pCol+cd[col].dtsize*pos,7);
			//break;
		default:
			//Not impletemented.
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE);
		}
		}
		int colbytes=(colct+7)/8;
		dtlen+=colbytes*2;
		int dtlen1=dtlen+4;
		//dtlen+=colbytes*2;
		int blockflag=3;
		int extrabytes=0;
		if(dtlen1<20) {
			extrabytes=20-dtlen1;
			blockflag=3;
		}
		else {
		 extrabytes=4-dtlen1%4;
		 if(extrabytes==3) // type 1 flag
		 {
			extrabytes=0;
			blockflag=1;
		 }
		 else
		 {
			if(extrabytes==4) extrabytes=0;
			blockflag=3;
		 }
		}
		int hdl=0;
		bhdr[hdl++]=blockflag;
		bhdr[hdl++]=(dtlen)>>8;
		bhdr[hdl++]=(dtlen)&0xff;
		if(blockflag==3)
			bhdr[hdl++]=extrabytes;
		memcpy(bhdr+hdl,&varflag,colbytes);hdl+=colbytes;
		memcpy(bhdr+hdl,&nullflag,colbytes);hdl+=colbytes;
		fwrite(bhdr,1,hdl,fp);
		fwrite(recbuf,1,dtlen-colbytes*2,fp);
		if(extrabytes>0) {
			memset(recbuf,0,extrabytes);
			fwrite(recbuf,1,extrabytes,fp);
		}
	}
}

void WOCIStatment::PrepareDefine(int pos)
{
	/*
	if(!pind[pos]) pind[pos]=new sb2[FETCH_ROWS];
	if(!plen[pos]) plen[pos]=new sb2[FETCH_ROWS];
	if(!pret[pos]) pret[pos]=new sb2[FETCH_ROWS];
	memset(pind[pos],0,sizeof(sb2)*FETCH_ROWS);
	memset(plen[pos],0,sizeof(sb2)*FETCH_ROWS);
	memset(pret[pos],0,sizeof(sb2)*FETCH_ROWS);
	*/
}

bool DataTable::SortHeap()
{
	if(nSortColumn<1) ReturnErr(DT_ERR_NOCOLUMNTOSORT);
	//if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK);
	if(!pSortedPos) {
		pSortedPos=new unsigned int [maxrows];
	}
	if(ptree) delete ptree;
	ptree=NULL;
	int i;
	for( i=0;i<rowct;i++) 
		pSortedPos[i]=i;
	// Create heap;

	if(rowct<1) return true;
	int n=rowct-1;
	int m=rowct/2-1;
    for(i=m;(int)i>=0;i--) 
		RebuildHeap(i,n);
	// Order heap
	for(i=n;i>0;i--)
	{
		//int x=pk[i]; //exchange first one and last one.
		//pk[i]=pk[0];pk[0]=x;
		unsigned int x=pSortedPos[i]; //exchange first one and last one.
		pSortedPos[i]=pSortedPos[0];pSortedPos[0]=x;
		RebuildHeap(0,i-1);
	}
	return true;
}
