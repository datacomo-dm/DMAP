#ifndef _INTERFACE_H
#define _INTERFACE_H
#define INT_INVALID_HANDLE	-1
#define INT_ALLOCATE_FAIL   -2
#define COLUMN_TYPE_CHAR	1 //SQLT_CHR
#define COLUMN_TYPE_FLOAT	4 //SQLT_FLT
#define COLUMN_TYPE_NUM		2 //SQLT_NUM
#define COLUMN_TYPE_INT		3 //SQLT_INT
#define COLUMN_TYPE_LONG	8 //SQLT_LNG
#define COLUMN_TYPE_DATE	12 //SQLT_DAT
#define VALUESET_SET		0 
#define VALUESET_ADD		1
#define VALUESET_SUB		2
#ifdef __unix
#define getch getchar
#endif
//////////////////////////////////////////////////////////////////
// Common function (5). 
//
//
//////////////////////////////////////////////////////////////////
#ifndef BOOL
#define FALSE 0
#define TRUE 1
#define BOOL int
#endif
#include <stdio.h>
void wocilogwhere(const char *fn,int ln);
#ifdef WOCI_DEBUG 
#define LOGWHERE wocilogwhere(__FILE__,__LINE__)
#define mtCommit(sess) {LOGWHERE;wociCommit(sess);}
#else
#define mtCommit(sess) wociCommit(sess)
#endif
//Catch level : 0 -- no catch 1--catch WOCIError 2--catch 'WOCIError' and 'int' and 'char *' exceptions
#ifdef __cplusplus
 int wociMainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel=2) ;
#else
 int wociMainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel) ;
#endif
 int wocidestroy(int handle) ;
 int wociDestroyAll();
 void wociBreakAll();
//////////////////////////////////////////////////////////////////
// Session function(6) . 
//
//
//////////////////////////////////////////////////////////////////
 int wociCreateSession(const char *username,const char *password,const char *svcname) ;
 int wociCommit(int sess) ;
 int wociRollback(int sess) ;
 int wociSetTerminate(int sess,bool val) ;
 bool wociIsTerminate(int sess) ;
 bool wociSetNonBlockMode(int sess) ;

//////////////////////////////////////////////////////////////////
// Statment function (18). 
//
//
//////////////////////////////////////////////////////////////////
 int wociCreateStatment(int sess) ;
 bool wociBindStrByPos(int stmt,int pos,char *ptr,int len) ;
 bool wociBindDoubleByPos(int stmt,int pos,double *ptr) ;
 bool wociBindIntByPos(int stmt,int pos,int *ptr) ;
 bool wociBindDateByPos(int stmt,int pos,char *ptr) ;
 bool wociDefineStrByPos(int stmt,int pos,char *ptr,int len) ;
 bool wociDefineDoubleByPos(int stmt,int pos,double *ptr) ;
 bool wociDefineIntByPos(int stmt,int pos,int *ptr) ;
 bool wociDefineDateByPos(int stmt,int pos,char *ptr);
 int wociBreakAndReset(int stmt) ;
 int wociExecute(int stmt,int times) ;
 int wociExecuteAt(int stmt,int times,int offset) ;
 int wociFetch(int stmt,int rows) ;
 int wociGetStmtColumnsNum(int stmt) ;
 int wociGetMtColumnsNum(int mt) ;
 //int wociGetColumnsNum(int stmt) ;
 int wociGetFetchedRows(int stmt) ;
 int wociGetSession(int stmt) ;
 unsigned short wociGetStmtType(int stmt) ;
 bool wociPrepareStmt(int stmt,char *sqlstmt) ;
 int wociGetCreateTableSQL(int memtab,char *buf,const char *tabname,bool ismysql=false) ;
//////////////////////////////////////////////////////////////////
// MemTable function (43). 
//
//
//////////////////////////////////////////////////////////////////
 int wociCreateMemTable() ;
 bool wociAddColumn(int memtab,char *name,char *dspname,int ctype,int length,int scale) ;
 bool wociBuildStmt(int memtab,int stmt,unsigned int rows);
 BOOL wociBuild(int memtab,unsigned int rows);
 void wociClear(int memtab) ;
 BOOL wociAppendToDbTable(int memtab,char *tablename,int sess) ;
 unsigned int wociFetchAll(int memtab) ;
 unsigned int wociFetchFirst(int memtab,unsigned int rows) ;
 unsigned int wociFetchNext(int memtab,unsigned int rows) ;
 bool wociGeneTable(int memtab,char *tablename,int sess) ;
 
 int wociGetStrAddrByName(int memtab,char *col,unsigned int rowst,char **pstr,int *celllen) ;
 int wociGetDateAddrByName(int memtab,char *col,unsigned int rowst,char **pstr) ;
 int wociGetDoubleAddrByName(int memtab,char *col,unsigned int rowst,double **pstr);
 int wociGetIntAddrByName(int memtab,char *col,unsigned int rowst,int **pstr) ;
 
 int wociGetStrValByName(int memtab,char *col,unsigned int rowst,char *pstr) ;
 int wociGetDateValByName(int memtab,char *col,unsigned int rowst,char *pstr) ;
 double wociGetDoubleValByName(int memtab,char *col,unsigned int rowst);
 int wociGetIntValByName(int memtab,char *col,unsigned int rowst) ;

 int wociGetStrAddrByPos(int memtab,int col,unsigned int rowst,char **pstr,int *celllen) ;
 int wociGetDateAddrByPos(int memtab,int col,unsigned int rowst,char **pstr) ;
 int wociGetDoubleAddrByPos(int memtab,int col,unsigned int rowst,double **pstr);
 int wociGetIntAddrByPos(int memtab,int col,unsigned int rowst,int **pstr) ;
 
 int wociGetBufferLen(int memtab) ;
 bool wociGetCell(int memtab,unsigned int row,int col,char *str,bool rawpos) ;
 int wociGetColumnDisplayWidth(int memtab,int col) ;
 int wociGetColumnPosByName(int memtab,char *colname);
 int wociGetColumnName(int memtab,int id,char *colname);
 int wociGetColumnDataLenByPos(int memtab,int colid);
 int wociGetColumnNumber(int memtab);
 int wociGetColumnScale(int memtab,int colid);
 void wociGetColumnTitle(int memtab,int colid,char *str,int len);
 unsigned short wociGetColumnType(int memtab,int colid);
 bool wociGetLine(int memtab,unsigned int row,char *str,bool rawpos,char *colsnm);
 int wociGetMemtableRows(int memtab);
 void wociGetTitle(int memtab,char *str,int len,char *colsnm);
 bool wociIsIKSet(int memtab) ;
 bool wociOrderByIK(int memtab);
 unsigned int wociSearchIK(int memtab,int key);
 void wociSetColumnDisplayName(int memtab,char *colnm,char *str);
 bool wociSetIKByName(int memtab,char *str) ;
 bool wociOrderByIK(int memtab,int colid) ;
 bool wociSetSortColumn(int memtab,char *colsnm) ;
 void wociReInOrder(int memtab);
 int wociSetStrValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) ;
 int wociSetDateValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) ;
 int wociSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,double *bf);
 int wociSetIntValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,int *bf) ;
 bool wociSort(int memtab) ;
/*  Group Functions (6)***************************************/
 bool wociSetGroupSrc(int memtab,int src) ;
 bool wociSetIKGroupRef(int memtab,int ref,char *colnm) ;
 bool wociSetGroupSrcCol(int memtab,char *colsnm) ;
 bool wociSetGroupRefCol(int memtab,char *colsnm) ;
 bool wociSetSrcSumCol(int memtab,char *colsnm) ;
 bool wociGroup(int memtab,int rowstart,int rownum);
 bool wociCopyRowsTo(int memtab,int memtabto,int toStart,int start,int rowsnum);
 bool wociGetColumnDesc(int memtab,void **pColDesc,int &cdlen,int &_colnm) ;
 bool wociCopyColumnDefine(int memtab,int memtabfrom,const char *colsname);
 bool wociExportSomeRows(int memtab,char *pData,int startrn,int rnnum);
 bool wociExport(int memtab,void *pData,long  &_len,int &_maxrows,int &_rowct) ;
 BOOL wociImport(int memtab,void *pData,long _len,void *pColDesc,int _colnm,int _maxrows,int _rowct);
 BOOL wociAppendRows(int memtab,char *pData,int rnnum); 
 	//保留表结构，清除索引结构，数据行数置零
 void wociReset(int memtab) ;
 void wociClearIK(int memtab) ;
 void wociClearSort(int memtab) ;
int wociGetMaxRows(int memtab); 
 bool wociCompact(int memtab);
 int wociGetRowLen(int memta);
//////////////////////////////////////////////////////////////////
// ExcelEnv function(7) . 
//
//
//////////////////////////////////////////////////////////////////
 int wociCreateExcelEnv() ;
 void wociSetDir(int excel,char *strTemplate,char *strReport);
 void wociSetMemTable(int excel,int memtab);
 bool wociLoadTemplate(int excel,char *tempname);
 bool wociSelectSheet(int excel,char *sheetname) ;
 bool wociFillData(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol, unsigned int fromrow, unsigned int colnum, unsigned int rownum,bool rawpos) ;
 bool wociFillTitle(int excel,unsigned int tocol,unsigned int torow,unsigned int fromcol,unsigned int colnum);
 bool wociSaveAs(int excel,char *filename) ;

 int wociGetMemUsed(int memtab);
 bool wociSetSortedGroupRef(int memtab,int mtref, char *colssrc);
 int  wociSearch(int memtab,void **ptr);
 bool wociValuesSet(int memtab,int mtsrc,char *colto,char *colfrom,bool usedestKey,int op);
 bool wociDeleteRow(int memtab,int rown) ;
 bool wociInsertRows(int memtab,void **ptr,char *colsname,int num);
 bool wociBindToStatment(int memtab,int stmt,char *colsname,int rowst);
 int wociTestTable(int sess,char *tablename);
 int wociGetRawrnBySort(int memtab,int ind);
 int wociGetRawrnByIK(int memtab,int ind);
 bool wociCompressBf(int memtab);
 int wociSearchQDel(int memtab,int key,int schopt);
 bool wociIsQDelete(int memtab,int rownm);
 int wociQDeletedRows(int memtab);
 bool wociQDeleteRow(int memtab,int rownm);
 void wociWaitStmt(int stmt);
 void wociWaitMemtable(int memtab);
 int wociWaitLastReturn(int handle);
 int wociWaitTime(int handle,int time);
 bool wociFreshRowNum(int memtab);
 int wociBatchSelect(int result,int param,char *colsnm);
 void wociMTPrint(int memtab,int rownm,char *colsnm) ;

void wociGetCurDateTime(char *date) ;
//Parametes: year(four digits),month(1-12),day(1-31),hour(0-23),minute(0-59),second(0-59).
// return a new datatime handle
void wociSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) ;
//Parameters:year,month,day ; hour,minute,second will be set to zero.
// return a new datetime handle
void wociSetDate(char *date,int year,int mon,int day);
//Format : yyyy/mm/dd hh24:mi:ss
void wociDateTimeToStr(char *date,char *str) ;
int wociGetYear(char *date) ;
int wociGetMonth(char *date);
int wociGetDay(char *date) ;
int wociGetHour(char *date) ;
int wociGetMin(char *date) ;
int wociGetSec(char *date) ;
void wociSetEcho(bool val);
void wociMTToTextFile(int memtab,char *fn,int rownm,char *colsnm) ;
void wociGetMTName(int memtab,char *bf);
int wociReadFromTextFile(int memtab,char *fn,int rowst,int rownm);
unsigned int wociFetchAllAt(int memtab,int st) ;
int wociDateDiff(char *d1,char *d2);
void wociSetOutputToConsole(bool val);
void wociSetErrorFile(const char *fn);
void wociSetLogFile(const char *fn);
int errprintf(const char *format,...) ;
int lgprintf(const char *format,...) ;
void WDBIInit(char *appname);
void WDBIQuit();
bool wociReplaceStmt(int memtab,int stmt);
int wociConvertColStrToInt(int memtab,const char *colsn,int *pcol);
int wociSaveSort(int memtab,FILE *fp);
int wociLoadSort(int memtab,FILE *fp);
void wociCopyToMySQL(int memtab,unsigned int startrow,
			unsigned int rownum,FILE *fp);
int wociCompareSortRow(int memtab,unsigned int row1, unsigned int row2);
void wociList(char *pout);
int wociGetLastError(int handle);
void wociAddrFresh(int memtab,char **colval,int *collen,int *tp);
// End of file woci8intface.h
//Use Var Char Add Function
 int _wdbiGetVarCol(int memtab,int *pCol,int *pTypeLen);
int  _wdbiCopyVarStrColDataInTable(int memtab,int nCol,long nToRow,long nFromRow,long nCopyRowNum);
const char*  _wdbiGetVarStrAddrByPos(int memtab,int col,unsigned int rowst,int *celllen) ;
 const char *_wdbiGetVarLenStrAddrByName(int memtab,const char *col,unsigned int rowst,int *celllen);
int _wdbiGetStrValByPos(int memtab,int col,unsigned int rowst,char *pstr,int rowNm=1);
BOOL _wdbiCheckData(int memtab, int nCol);
#endif

