#ifndef  WDBI_H
#define WDBI_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __unix
#include <strings.h>
#include <unistd.h>
#include <strings.h>
#include <pthread.h>
#define getch getchar
#define STRICMP strcasecmp
#define MAX_PATH 1024
#else
#include "StdAfx.h"
#include <windows.h>
#include <conio.h>
#ifndef NO_EXCEL
#include <comdef.h>
#endif
#include <process.h>
#define STRICMP stricmp
#endif
#define FETCH_ROWS 50000
#define EXECUTE_TIMES 50000
#define TWO_G_SIZE  (2*1000*1024*1024)  //2*1024*1024*1024 整数溢出
#define TEN_M_SIZE  (10*1024*1024)
#define TEN_K_SIZE  (10*1024)
//sql语句G最大长度
#include "dt_global.h"
#include "wdbi_common.h"

#include <time.h>
#ifndef mSleep
#ifdef WIN32
#define mSleep(msec) Sleep(msec)
#else
#define mSleep(msec) \
{ \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=msec%1000*1000*1000;\
   nanosleep(&req, &rem);\
}
#endif
#endif

extern "C" {
#include <oci.h>
}

//using namespace std;
#include <time.h>
#include "wdbi_common.h"
#include "wdbierr.h"


#ifndef PATH_SEP
#ifdef WIN32
#define PATH_SEP    "\\"
#define PATH_SEPCHAR   '\\'
#ifndef MAX_PATH
#define MAX_PATH _MAX_PATH
#endif
#else
#define PATH_SEP   "/"
#define PATH_SEPCHAR   '/'
#endif
#endif

#define DT_ERR_EMPTYSTMT            1
#define DT_ERR_OCIERR               2
#define DT_ERR_COLUMNEMPTY          3
#define DT_ERR_NOTSELECT            4
#define DT_ERR_INVALIDCOLUMNTYPE    5
#define DT_ERR_OUTOFMEMORY          6
#define DT_ERR_OUTOFROW             7
#define DT_ERR_OUTOFRANGE           8
#define DT_ERR_OUTOFCOLUMN          9
#define DT_ERR_CLEARBEFOREADD       10
#define DT_ERR_OUTOFCOLNAME         11 //exceed maximum column name storage size;
#define DT_ERR_INVALIDCOLLEN        12
#define DT_ERR_MISMATCHCOLNAME      13
#define DT_ERR_MISMATCHCOLID        14
#define DT_ERR_MISMATCHCOLTYPE      15
#define DT_ERR_NOTPKCOL             16
#define DT_ERR_PKOUTOFCOL           17
#define DT_ERR_PKNOTDEFINED         18
#define DT_ERR_INVALIDPTR_ATREFALL  19
#define DT_ERR_ORDERONNULLPK        20
#define DT_ERR_NOCOLUMNTOSORT       21
#define DT_ERR_COLNAMEDUPLICATE     22
#define DT_ERR_INVALIDCOLNAME       23
#define DT_ERR_INVDATACOLTYPE       24
#define DT_ERR_DATACOLNULL          25
#define DT_ERR_MISMATCHSORTED       26
#define DT_ERR_COLUNSORTED          27
#define DT_ERR_KEYCOLMISMATCH       28
#define DT_ERR_KEYCOLNULL           29
#define DT_ERR_DATACOLMISMATCH      30
#define DT_ERR_KEYCOLUMNEMPTY       31
#define DT_ERR_NEEDINTTYPE          32
#define DT_ERR_MEMFULL              33
#define DT_ERR_CANNOTOPENTEXTFILE   34
#define DT_ERR_OUTOFCOLUMNWIDE      35
#define DT_ERR_NOTSUPPORT       36
#define DT_ERR_VALUELOSS        37

#define DTL_ERR_DTNOTFOUND          1
#define DTL_ERR_EMPTYLINK           2
#define DTL_ERR_ALREADYEXIST        3
#define DTG_ERR_BASE                38
#define DTG_ERR_PARMREFISNULL           DTG_ERR_BASE+1
#define DTG_ERR_PARMSRCISNULL           DTG_ERR_BASE+2
#define DTG_ERR_NOCOLUMNSINREF          DTG_ERR_BASE+3
#define DTG_ERR_INVALIDSRCDT            DTG_ERR_BASE+4
#define DTG_ERR_INVALIDREFDT            DTG_ERR_BASE+5
#define DTG_ERR_INVALIDSTARTROW         DTG_ERR_BASE+6
#define DTG_ERR_INVALIDROWNUM           DTG_ERR_BASE+7
#define DTG_ERR_OUTOFSRCROWS            DTG_ERR_BASE+8
#define DTG_ERR_MUSTSETREFPK            DTG_ERR_BASE+9
#define DTG_ERR_SETSRCBEFOREREF         DTG_ERR_BASE+10
#define DTG_ERR_OUTOFDTGMAXROWS         DTG_ERR_BASE+11
#define DTG_ERR_OUTOFSRCCOL         DTG_ERR_BASE+12
#define DTG_ERR_OUTOFREFCOL         DTG_ERR_BASE+13
#define DTG_ERR_PKMUSTINT_INSRCDT       DTG_ERR_BASE+14
#define DTG_ERR_OUTOFMAXDTGCOL          DTG_ERR_BASE+15
#define DTG_ERR_NEEDNUMERICCOLTYPE      DTG_ERR_BASE+16
#define DTG_ERR_SETONBUILDEDDTP         DTG_ERR_BASE+17
#define DTG_ERR_CLEARBEFOREUSING        DTG_ERR_BASE+18
#define DTG_ERR_SORTEDCOLNOTFOUND       DTG_ERR_BASE+19
#define DTG_ERR_SOMESORTEDCOLNOTUSED    DTG_ERR_BASE+20
#define DTG_ERR_NOTMATCHSORTEDCOLTYPE   DTG_ERR_BASE+21
#define DTG_ERR_OUTOFMEMTEXTOUT       DTG_ERR_BASE+22
#define ReturnErr(code,pi,ps) {ErrorCheck(__FILE__,__LINE__,code,pi,ps);return false;}
#define ReturnNullErr(code,pi,ps) {ErrorCheck(__FILE__,__LINE__,code,pi,ps);return NULL;}
#define ReturnIntErr(code,pi,ps) {ErrorCheck(__FILE__,__LINE__,code,pi,ps);return -1;}
#define ReturnVoidErr(code,pi,ps) {ErrorCheck(__FILE__,__LINE__,code,pi,ps);return ;}
#define ReturnStmtErr(code,pi,ps) {ErrorCheck(__FILE__,__LINE__,code,pi,ps);}

#define BUFF_SEC_NUM  32

extern OCIEnv *envhp;
extern OCIError *errhp;
#ifndef max
#define max(a,b)            (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min(a,b)            (((a) < (b)) ? (a) : (b))
#endif

DllExport void _wdbiSetOutputToConsole(bool val);
DllExport int myprintf(const char *format,...);
void WDBIShutdown() ;
void WDBIStartup () ;
DllExport int lgprintf(const char *format,...);
DllExport int errprintf(const char *format,...);
DllExport void wdbiSetErrorFile(const char *fn);
DllExport void wdbiSetLogFile(const char *fn);

//修改代码，使用bit表示BOOL by chxl
void SetBitValue(mbyte *pData, long n ,bool val );
bool CheckBitValue(mbyte *pData,long n);
//
// Column_Desc
struct Column_Desc {
    unsigned short type;
    unsigned short dtsize;
    char colname[COLNAME_LEN];
    char dispname[COLNAME_LEN];
    ub4 dspsize;
    ub1 prec;
    sb1 scale;
    char *pCol;
#ifndef WDBI64BIT
    char *pBlank;
#endif
    void ReverseByteOrder()
    {
#ifndef WORDS_BIGENDIAN
        return ;
#endif
        revShort(&type);
        revShort(&dtsize);
        revInt(&dspsize);
    }
};

class WDBIStatement ;
class DataTable;
class WDBISession :public WDBIError
{
protected :
    bool terminate;
    char UserName[100],Password[100],SvcName[100];
public:
    virtual int TestTable(const char *tabname)
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"TestTable");
    }
    void ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params);
    virtual void Rollback()
    {
        ReturnVoidErr(DT_ERR_NOTSUPPORT,0,"Rollback");
    }
    virtual void Commit()
    {
        ReturnVoidErr(DT_ERR_NOTSUPPORT,0,"Commit");
    }
    WDBISession(bool atthr=true)
    {
        terminate=false;
        autoThrow=atthr;
        UserName[0]=0;
        Password[0]=0;
        SvcName[0]=0;
    }
    virtual bool SetNonBlockMode()
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"SetNonBlockMode");
    }
    void SetTerminate(bool val)
    {
        terminate=val;
    }
    bool IsTerminate()
    {
        return terminate;
    }
    virtual int GetColumnInfo(Column_Desc *cd,int _colct,char *crttab)
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"GetColumnInfo(WDBISession))");
    }
    virtual void FillParam(char *bf,int colid)
    {
        ReturnVoidErr(DT_ERR_NOTSUPPORT,0,"FillParam");
    }
    virtual bool Connect (const char *username,const char *password,const char *svcname)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"Connect");
    }
    virtual ~WDBISession() {};
    virtual WDBIStatement CreateStatement() ;
    virtual WDBIStatement *NewStatement() ;
};

class WOCISession : public WDBISession
{
public :
    OCIServer *svrhp;
    OCISvcCtx *svchp;
    OCISession *authp ;
public :
    virtual int TestTable(const char *tabname);
    virtual void Rollback();
    virtual void Commit();
    WOCISession (bool atthr=true):WDBISession(atthr) {}
    virtual bool SetNonBlockMode() ;
    virtual void FillParam(char *bf,int colid)
    {
        sprintf(bf,":%d",colid);
    }
    virtual bool Connect (const char *username,const char *password,const char *svcname);
    virtual int GetColumnInfo(Column_Desc *cd,int _colct,char *crttab);
    virtual ~WOCISession()
    {
#ifndef NO_DBMS
        Rollback();
        OCISessionEnd(svchp,  errhp, authp,
                      (ub4) OCI_DEFAULT);
        OCIHandleFree(authp,OCI_HTYPE_SESSION);
        OCIServerDetach(svrhp,errhp,OCI_DEFAULT );
        OCIHandleFree(svchp, OCI_HTYPE_SVCCTX);
        OCIHandleFree(svrhp, OCI_HTYPE_SERVER);
#endif
    }
    virtual void checkerr(sword status);
    virtual WDBIStatement CreateStatement();
    virtual WDBIStatement *NewStatement();
};


/*
#ifndef NO_MYSQL
#include "mysql/mysql.h"

class WMYSQLSession : public WDBISession {
MYSQL       * myData ;
char dbname[100];
int portnum;
char szSQL[3000];
MYSQL_RES *result;
int num_rows;
public :
    virtual int TestTable(char *tabname);
    virtual void Rollback();
    virtual void Commit() ;
    WMYSQLSession (bool atthr=true):WDBISession(atthr) {myData=NULL;result=NULL;szSQL[0]=0;}
    virtual bool Connect (const char *username,const char *password,const char *svcname);
    virtual ~WMYSQLSession() {
#ifndef NO_MYSQL
        if(result) mysql_free_result(result);
        if(myData) mysql_close( myData ) ;
#endif
    }
    virtual void checkerr(sword status);
    virtual WDBIStatement CreateStatement();
    virtual WDBIStatement *NewStatement();
};
#endif
*/

void WDBIStartup();
void WDBIShutdown();

class WDBIStatement :public WDBIError
{
protected :
    WDBISession *psess;
    Column_Desc coldsc[MAX_COLUMN];
    sb2 *pind[MAX_COLUMN];
    sb2 *plen[MAX_COLUMN];
    sb2 *pret[MAX_COLUMN];
    unsigned short stmttype;
    ub4 colct;
    ub4 rowct;
    ub4 fetchsize;
    bool eof;
    char *sqlstmt;
    void *bindedptr[MAX_COLUMN];
    int  bindedtype[MAX_COLUMN];
    /*** 2005/06/17 加*/
    int  bindedpos[MAX_COLUMN];
    //char bindedio[MAX_COLUMN]; // In or Out Indicator.
    int bindednum;
    int bindedlen[MAX_COLUMN];
    int *bindedind[MAX_COLUMN];
    bool executed;
    bool executing;
    static int DEFAULT_NUMBER_PREC ;
    static int DEFAULT_NUMBER_SCALE ;

    DataTable *m_pDataTab;
    long       m_nTabSartRow;
    long       m_nStartSecNum;
    long       m_nFetchWriteRow;
    int        m_nWriteCol[MAX_COLUMN];
    int        m_nNumWriteCol;
    int        m_nBindID[MAX_COLUMN];

public:
    static void SetDefaultPrec(int _prec,int _scale)
    {
        DEFAULT_NUMBER_PREC=_prec;
        DEFAULT_NUMBER_SCALE=_scale;
    }

    const char *GetSqlText()
    {
        return sqlstmt;
    }
    bool iseof()
    {
        return eof;
    }
    virtual void PrepareDefine(int pos)
    {
        ReturnVoidErr(DT_ERR_NOTSUPPORT,0,"PrepareDefine");
    }
    virtual void ConvertFetchedLong(LONG64 *ptr,int rowrn)
    {
        ReturnVoidErr(DT_ERR_NOTSUPPORT,0,"PrepareDefine");
    }
    virtual int TestTable(const char *tbname)
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"TestTable(WDBIStatement))");
    }
    virtual int Fetch(int rows,bool first)
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"Fetch(WDBIStatement))");
    }
    unsigned int GetRows()
    {
        return rowct;
    }
    int GetColumns()
    {
        return colct;
    }
    unsigned short GetStmtType()
    {
        return stmttype;
    }
    void CopyColumnDesc(Column_Desc *cd)
    {
        memcpy(cd,&coldsc,sizeof(coldsc[0])*colct);
    }
    void ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params);
    WDBIStatement(WDBISession *s,bool atthr=true)
    {
        psess=s;
        m_pDataTab = NULL;
        m_nFetchWriteRow = 0;
        m_nTabSartRow = 0;
        m_nStartSecNum=0;
        rowct=0;
        autoThrow=atthr;
        sqlstmt=new char[MAX_STMT_LEN];
        eof=false;
        fetchsize=FETCH_ROWS;
        bindednum=0;
        executed=executing=false;
        memset(coldsc,0,sizeof(coldsc));
        memset(bindedind,0,sizeof(int *)*MAX_COLUMN);
        for(int i=0; i<MAX_COLUMN; i++)
            pind[i]=plen[i]=pret[i]=NULL;

    }
    void ClearNullBind()
    {
        for(int i=0; i<MAX_COLUMN; i++) {
            if(bindedind[i]) delete []bindedind[i];
            bindedind[i]=NULL;
        }
    }
    void SetDataTable(DataTable *pDataTab,int nStartRow)
    {
        m_pDataTab = pDataTab;
        m_nFetchWriteRow = 0;
        m_nTabSartRow = nStartRow;
        m_nNumWriteCol = 0;
        m_nStartSecNum=0;
    }

    BOOL IsVarCharCol(int nCol)                          //判断是否是变长存储
    {
        if(m_pDataTab == NULL) {
            return FALSE;
        }
        for(int i=0; i<m_nNumWriteCol; i++) {
            if(nCol==m_nWriteCol[i]) {
                return TRUE;
            }
        }
        return FALSE;
    }

    void SetVarDataWriteCol(int nCol)
    {
        m_nBindID[m_nNumWriteCol] =  bindednum;
        m_nWriteCol[m_nNumWriteCol] = nCol;
        m_nNumWriteCol++;
    }

    void ReleaseDataTale()
    {
        m_pDataTab = NULL;
        m_nFetchWriteRow = 0;
        m_nNumWriteCol = 0;
        m_nTabSartRow = 0;
        m_nStartSecNum=0;
    }

    void SetFetchSize(unsigned int fsize)
    {
        fetchsize=fsize;
    }
    unsigned int GetFetchSize()
    {
        return fetchsize;
    }
    virtual ~WDBIStatement()
    {
        if(sqlstmt)
            delete [] sqlstmt;
        sqlstmt=NULL;
        for(int i=0; i<MAX_COLUMN; i++) {
            if(pind[i]) delete []pind[i];
            if(plen[i]) delete []plen[i];
            if(pret[i]) delete []pret[i];
            if(bindedind[i]) delete []bindedind[i];
        }
    }
    WDBISession *GetSession()
    {
        return psess;
    }

    virtual int  GetColumnNum()
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"GetColumnNum())");
    }
    virtual bool GetColumnName(int col,char* colname)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"GetColumnName(int,string))");
    }
    virtual int  GetColumnType(int col)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"GetColumnType(int))");
    }
    virtual int  GetColumnSize(int col)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"GetColumnSize(int))");
    };
    virtual bool SetColumnSize(int col,int size)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"SetColumnSize(int,int))");
    };

    virtual bool Prepare(const char *_sqlstmt)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(int))");
    }
    virtual bool BindByPos(int pos,char *result,int size)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(string))");
    }
    virtual bool BindByPos(int pos,int *result)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(int))");
    }
    virtual bool BindByPos(int pos,LONG64 *result)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(LONG64))");
    }
    virtual bool BindByPos(int pos,double *result)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(double))");
    }
    virtual bool SetBindNullFlag(int pos,mbyte *flag,unsigned int nCheckbit ,unsigned int rowsnum)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"SetBindNullFlag");
    }
    // for NUMBER ,len=21
    virtual bool BindByPos(int pos,unsigned char *result)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(NUMBER,len=21))");
    }
    // For Date type ,len=7;
    virtual bool BindByPos(int pos,char *result)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"BindByPos(date)");
    }
    virtual bool DefineByPos(int pos,char *param,int size)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(string)");
    }
    virtual bool DefineByPos(int pos,int  *param)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(int)");
    }
    virtual bool DefineByPos(int pos,LONG64  *param)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(LONG64)");
    }
    virtual bool DefineByPos(int pos,double *param)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(double)");
    }
    // for NUMBER ,len=21
    virtual bool DefineByPos(int pos,unsigned char *param)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(NUMBER len==21)");
    }
    // For Date type ,len=7
    virtual bool DefineByPos(int pos,char *param)
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DefineByPos(date type)");
    }
    virtual bool DescribeStmt()
    {
        ReturnErr(DT_ERR_NOTSUPPORT,0,"DescribeStmt");
    }
    virtual sword BreakAndReset()
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"BreakAndReset");
    }
    virtual sword Execute(int times=1,int offset=0)
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"Execute");
    }
    sb4 GetErrCode()
    {
        if(errcode!=0) return errcode;
        if(psess!=NULL) return psess->GetErrCode();
        return 0;
    }

    virtual void GetNullFlag(mbyte *flags,unsigned int rowsnum,unsigned int startrow,unsigned int maxrows) {};
private:
    virtual int NewStmt()
    {
        ReturnIntErr(DT_ERR_NOTSUPPORT,0,"NewStmt");
    }
    virtual void SetNullDateBind(int rnum) {};
};


class WOCIStatement :public WDBIStatement
{
    void checkstmterr(sword status);
    void SetNullDateBind(int rnum);

protected :
    OCIStmt   *sthp;
    OCIDefine *dfp[MAX_COLUMN];
    OCIBind *bdp[MAX_COLUMN];

public:
    //bool SetBindNullFlag(int pos,int *flag,unsigned int rowsnum)
    bool SetBindNullFlag(int pos,mbyte *flag,unsigned int nCheckbit ,unsigned int rowsnum)
    {
        if(bindedind[pos]) delete []bindedind[pos];
        bindedind[pos]=(int *)new sb2[rowsnum];
        sb2 *pt=(sb2 *)(bindedind[pos]);
        for(int i=0; i<rowsnum; i++) {
            if (CheckBitValue(flag,nCheckbit+i)) {
                pt[i] = DB_NULL_DATA;
            } else {
                pt[i] = 0;
            }
            //pt[i]=flag[i]==DB_NULL_DATA?DB_NULL_DATA:0;
        }
        return true;
    }
    void GetNullFlag(mbyte *flags,unsigned int rowsnum,unsigned int startrow,unsigned int maxrows)
    {
        for (int i = 0; i < colct; i++) {
            //int *pt=flags+i*maxrows+startrow;
            long n = i*maxrows+startrow;
            sb2 *pi=(sb2 *)pind[i+1];
            if(!pi) continue;
            for(int j=0; j<rowsnum; j++) {
                //*pt++=*pi++==DB_NULL_DATA?DB_NULL_DATA:0;
                if(*pi++==DB_NULL_DATA) {
                    SetBitValue(flags,n + j,true);
                } else {
                    SetBitValue(flags,n + j,false);
                }
            }
        }
    }
    virtual void PrepareDefine(int pos);
    virtual int TestTable(const char *tbname);
    virtual int Fetch(int rows,bool first);
    unsigned short GetStmtType()
    {
        return stmttype;
    }
    WOCIStatement(WDBISession *s,bool atthr=true) ;
    virtual void checkerr(sword status);
    virtual void ConvertFetchedLong(LONG64 *ptr,int rowrn)
    {
        return ;
        for(int i=0; i<rowrn; i++) {
            *ptr=(LONG64)(*(double *)ptr);
            ptr++;
        }
    }
    virtual ~WOCIStatement()
    {
#ifndef NO_DBMS
        if(sthp) {
            OCIHandleFree(sthp,OCI_HTYPE_STMT);
            sthp=NULL;
        }
#endif
    }
    virtual bool Prepare(const char *_sqlstmt);
    virtual bool BindByPos(int pos,char *result,int size);
    virtual bool BindByPos(int pos,int *result);
    virtual bool BindByPos(int pos,LONG64 *result);
    virtual bool BindByPos(int pos,double *result);
    virtual bool BindByPos(int pos,unsigned char *result); // for NUMBER ,len=21
    virtual bool BindByPos(int pos,char *result); // For Date type ,len=7;
    virtual bool DefineByPos(int pos,char *param,int size);
    virtual bool DefineByPos(int pos,int  *param);
    virtual bool DefineByPos(int pos,double *param);
    virtual bool DefineByPos(int pos,LONG64 *param);
    virtual bool DefineByPos(int pos,unsigned char *param);// for NUMBER ,len=21
    virtual bool DefineByPos(int pos,char *param);// For Date type ,len=7;

    virtual bool DescribeStmt();
    virtual sword BreakAndReset();
    virtual sword Execute(int times=1,int offset=0);

    virtual int  GetColumnNum();
    virtual bool GetColumnName(int col,char* colname);
    virtual int  GetColumnType(int col);
    virtual int  GetColumnSize(int col);
    virtual bool SetColumnSize(int col,int size);


private:
    int NewStmt();
};


#ifndef NO_ODBC
#include <sql.h>
#include <sqlext.h>

#define CHECK_OPT_NULL  100
#define CHECK_OPT_SUCCWITHINFO  101
#define CHECK_OPT_NOTABLE   102
//#define CHECK_OPT_SUCCWITHINFO  100
#define DBSTYPE_UNKNOWN 0
#define DBSTYPE_MYSQL   1
#define DBSTYPE_DB2     2
#define DBSTYPE_SYBASE  3
#define DBSTYPE_ACCESS  4
#define DBSTYPE_FIREBIRD 5
#define DBSTYPE_ORACLE 6
#define GENE_STR_LEN 200
class WODBCSession : public WDBISession
{
    static SQLHENV henv;
    char dbs_name[GENE_STR_LEN];
    char dbs_version[GENE_STR_LEN];
    void ocheckerr(SQLRETURN status,int handletype,SQLHANDLE handle,int chkopt);
public :
    SQLHDBC hdbc;
    int GetDBSType()
    {
        if(strncmp(dbs_name,"DB2",3)==0) return DBSTYPE_DB2;
        if(strncmp(dbs_name,"MySQL",5)==0)
            return DBSTYPE_MYSQL;
        if(strncmp(dbs_name,"SQL Server",10)==0) return DBSTYPE_SYBASE;
        if(strncmp(dbs_name,"ACCESS",6)==0) return DBSTYPE_ACCESS;
        if(strncmp(dbs_name,"Firebird",8)==0) return DBSTYPE_FIREBIRD;
        if(strncmp(dbs_name,"Oracle",6)==0) return DBSTYPE_ORACLE;
        return DBSTYPE_UNKNOWN;
    }
    virtual bool SetNonBlockMode() ;
    void chkenv(SQLRETURN status)
    {
        ocheckerr(status,1,henv,CHECK_OPT_NULL);
    }
    void chkdbc(SQLRETURN status)
    {
        ocheckerr(status,2,hdbc,CHECK_OPT_NULL);
    }
    virtual int TestTable(const char *tabname);
    virtual void Rollback();
    virtual void Commit() ;
    virtual void FillParam(char *bf,int colid)
    {
        strcpy(bf,"? ");
    }
    virtual int GetColumnInfo(Column_Desc *cd,int _colct,char *crttab);
    WODBCSession (bool atthr=true):WDBISession(atthr)
    {
        henv=NULL;
        hdbc=NULL;
    }
    virtual bool Connect (const char *username,const char *password,const char *svcname);
    virtual ~WODBCSession()
    {
        SQLRETURN rc;
        rc = SQLDisconnect(hdbc);
        //AIX can not process embeded throw
        //chkdbc(rc);
        rc = SQLFreeConnect(hdbc);
        //AIX can not process embeded throw
        chkdbc(rc);
        //rc = SQLFreeEnv(*henv);
        //myenv(*henv,rc);
    }
    //virtual void checkerr(sword status);
    virtual WDBIStatement CreateStatement();
    virtual WDBIStatement *NewStatement();
};
#define NULL_INDTYPE SQLLEN
class WODBCStatement :public WDBIStatement
{
//  void checkstmterr(SQLRETURN status);
    void ocheckerr(SQLRETURN status,int handletype,SQLHANDLE handle,int chkopt);
    void chkstmt(SQLRETURN status)
    {
        if(status==SQL_SUCCESS || status==SQL_SUCCESS_WITH_INFO ) return;
        ocheckerr(status,3,sthp,CHECK_OPT_NULL);
    }
    void chktesttab(SQLRETURN status)
    {
        if(status==SQL_SUCCESS || status==SQL_SUCCESS_WITH_INFO ) return;
        ocheckerr(status,3,sthp,CHECK_OPT_NOTABLE);
    }
    void ReleaseBind();
    void ReleaseDef();
    void RealBind(int process,int processed=0);
    SQLSMALLINT coltype[MAX_COLUMN];
    TIMESTAMP_STRUCT *datebind[MAX_COLUMN];
    SQLINTEGER *datelen[MAX_COLUMN];
    TIMESTAMP_STRUCT *datedef[MAX_COLUMN];
    char *resultdate[MAX_COLUMN];
    void SetNullDateBind(int rnum);
    int bind_start,bind_num;//这两个变量是用来控制不能批量执行的odbc driver，不会在date类型上重复做批量类型转换
protected :
    SQLHSTMT sthp;
    /*
        OCIStmt   *sthp;
        OCIDefine *dfp[MAX_COLUMN];
        OCIBind *bdp[MAX_COLUMN];
    */
public:
    //bool SetBindNullFlag(int pos,int *flag,unsigned int rowsnum)
    bool SetBindNullFlag(int pos,mbyte *flag,unsigned int nCheckbit ,unsigned int rowsnum)
    {
        if(bindedind[pos]) delete []bindedind[pos];
        bindedind[pos]=(int *)new NULL_INDTYPE[rowsnum];
        NULL_INDTYPE *pt=(NULL_INDTYPE *)(bindedind[pos]);
        for(int i=0; i<rowsnum; i++) {
            if (CheckBitValue(flag,nCheckbit+i)) {
                pt[i] = DB_NULL_DATA;
            } else {
                pt[i] = SQL_NTS;
            }
            //pt[i]=flag[i]==DB_NULL_DATA?DB_NULL_DATA:SQL_NTS;
        }
        return true;
    }
    void GetNullFlag(mbyte *flags,unsigned int rowsnum,unsigned int startrow,unsigned int maxrows)
    {
        for (int i = 0; i < colct; i++) {
            //int *pt=flags+i*maxrows+startrow;
            long n = i*maxrows+startrow;
            NULL_INDTYPE *pi=(NULL_INDTYPE *)pind[i+1];
            if(!pi) continue;
            for(int j=0; j<rowsnum; j++) {
                //  *pt++=*pi++==DB_NULL_DATA?DB_NULL_DATA:0;
                if(*pi++==DB_NULL_DATA) {
                    SetBitValue(flags,n + j,true);
                } else {
                    SetBitValue(flags,n + j,false);
                }
            }
        }
    }

    virtual void PrepareDefine(int pos);
    virtual int TestTable(const char *tbname);
    virtual int Fetch(int rows,bool first);
    unsigned short GetStmtType()
    {
        return stmttype;
    }
    virtual void ConvertFetchedLong(LONG64 *ptr,int rowrn)
    {
    }
    WODBCStatement(WDBISession *s,bool atthr=true) ;
    //virtual void checkerr(sword status);
    virtual ~WODBCStatement()
    {
        SQLRETURN rc;
        if(sthp) {
            rc = SQLFreeStmt(sthp, SQL_DROP);
            chkstmt(rc);
        }
        sthp=NULL;
        ReleaseBind();
        ReleaseDef();
    }
    virtual bool Prepare(const char *_sqlstmt);
    virtual bool BindByPos(int pos,char *result,int size);
    virtual bool BindByPos(int pos,int *result);
    virtual bool BindByPos(int pos,LONG64 *result);
    virtual bool BindByPos(int pos,double *result);
    virtual bool BindByPos(int pos,unsigned char *result); // for NUMBER ,len=21
    virtual bool BindByPos(int pos,char *result); // For Date type ,len=7;
    virtual bool DefineByPos(int pos,char *param,int size);
    virtual bool DefineByPos(int pos,int  *param);
    virtual bool DefineByPos(int pos,LONG64  *param);
    virtual bool DefineByPos(int pos,double *param);
//  virtual bool DefineByPos(int pos,unsigned char *param);// for NUMBER ,len=21
    virtual bool DefineByPos(int pos,char *param);// For Date type ,len=7;

    virtual bool DescribeStmt();
    virtual sword BreakAndReset();
    virtual sword Execute(int times=1,int offset=0);

    virtual int  GetColumnNum();
    virtual bool GetColumnName(int col,char* colname);
    virtual int  GetColumnType(int col);
    virtual int  GetColumnSize(int col);
    virtual bool SetColumnSize(int col,int size);

private:
    int NewStmt();
};
#endif

/*
typedef struct tag_WDBI_MYBIND {
    MYSQL_BIND bind;
    char isdt;
    char *pBuf;
    unsigned long resLen;
    MYSQL_TIME mtm;
    my_bool isnull;
} WDBI_MYBIND;

class WMYSQLStatement :public WDBIStatement {
    MYSQL_STMT *pstmt;
    MYSQL_RES *pRes;
    int paramct;
    WDBI_MYBIND *pInBind;
    WDBI_MYBIND *pOutBind;
    void checkstmterr(sword status);
protected :
//  sb2 *pind[MAX_COLUMN];
//  sb2 *plen[MAX_COLUMN];
//  sb2 *pret[MAX_COLUMN];
//  OCIStmt   *sthp;
//  OCIDefine *dfp[MAX_COLUMN];
//  OCIBind *bdp[MAX_COLUMN];

public:
    virtual void PrepareDefine(int pos);
    virtual int TestTable(char *tbname);
    virtual int Fetch(int rows,bool first);
    unsigned short GetStmtType() {return stmttype;}
    WMYSQLStatement(WDBISession *s,bool atthr=true) ;
    virtual void checkerr(sword status);
    virtual ~WMYSQLStatement() {
        if(pInBind) delete []pInBind;
        if(pOutBind) delete []pOutBind;
        if(pRes) mysql_free_result(pRes);
        if(pstmt) {
            mysql_stmt_close(pstmt);
        }
    }
    virtual bool Prepare(char *_sqlstmt);
    virtual bool BindByPos(int pos,char *result,int size);
    virtual bool BindByPos(int pos,int *result);
    virtual bool BindByPos(int pos,double *result);
    //virtual bool BindByPos(int pos,unsigned char *result); // for NUMBER ,len=21
    virtual bool BindByPos(int pos,char *result); // For Date type ,len=7;
    virtual bool DefineByPos(int pos,char *param,int size);
    virtual bool DefineByPos(int pos,int  *param);
    virtual bool DefineByPos(int pos,double *param);
    //virtual bool DefineByPos(int pos,unsigned char *param);// for NUMBER ,len=21
    virtual bool DefineByPos(int pos,char *param);// For Date type ,len=7;

    virtual bool DescribeStmt();
    //virtual sword BreakAndReset();
        virtual sword Execute(int times=1,int offset=0);

private:
    int NewStmt();
  };
*/

class AVLtree;
class DataTable :public WDBIError
{
protected :
    void RebuildHeapPK(unsigned int m,unsigned int n,int *pk);
    Column_Desc cd[MAX_COLUMN];
    ub4 colct,rowct;
    int pkid;
    //char  *pbf;

    char  *m_pColBuff[MAX_COLUMN][BUFF_SEC_NUM];      //每一个Col分配一个内存，32表示分配数据段数，每个数据段为2G控件空间
    ub4   *m_pColPerDataLen[MAX_COLUMN][BUFF_SEC_NUM]; // 记录每一条变长值的长度。
    ub4   m_nszRowNumPerSec[MAX_COLUMN][BUFF_SEC_NUM];  //每一个Section记录了多少条记录，Section装满之后值不变
    ub4   m_nUseBuffSectionNum[MAX_COLUMN];                       //当前使用第几个Section
    //ub4   m_nSecRowCt[MAX_COLUMN];                    //pColBuff内每一个Col当前Section中当前记录的条数。
    long  m_nVarSecBfMaxSize[MAX_COLUMN][BUFF_SEC_NUM];         //      每一个Sec的真实空间长度，
    ub4   m_nIDVarCol[MAX_COLUMN];                              //记录变长Col的ID号
    ub4   m_nNumVarCol;
    ub4   m_nFirstMixRowN;

    short m_nVarLenLimit ;

    char *m_pColExtend[MAX_COLUMN];

    unsigned int *pPKSortedPos;//主键排序结果
    unsigned int *pSortedPos; //普通排序结果
    long bflen;
    ub4 maxrows;
    WDBIStatement *pstmt;
    //Quick delete support :
    mbyte *pQDelKey;
    //int *pnullind;
    mbyte *pnullind;
    AVLtree *ptree;
    int qdelct;
    bool isinorder;
    //int qdelmax;
    //
    char dtname[DATATABLE_NAMELEN]; //Data table name
    ub4 rowlen;
    DataTable *pNext;
    void ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params);
    void BuildAppendIndex(int num) ;
    void SetNullValue(unsigned int rows,unsigned int st);
public :
    double Calculate(const char *colname,int op);
    bool IsFixedMySQLBlock();
    void ReverseCD()
    {
        int i=0;
        for(i=0; i<colct; i++) {
            cd[i].ReverseByteOrder();
        }
    }


    int SetVarLenLimit(int nVarLen);  //设置变长门限
    void ConvertFetchedLong(int st);
    int ReplaceStmt(WDBIStatement *psmt);
    int GetColumnInfo(char *crttab,bool ismysql);
    bool SortLastRows(int rn);
    int ReadFromTextFile(const char *fn,int rowst,int rownm);
    void GetMTName(char *bf);
    int GetMaxRows();
    bool FreshRowNum();
    int GetRawrnBySort(int ind);
    int GetRawrnByPK(int ind);
    bool CompressBf();
    int SearchQDel(int key,int schopt);
    bool IsQDelete(int rownm);
    int QDeletedRows();
    bool QDeleteRow(int rownm);
    void SetQDeleteFlagFalse(int Row);
    bool BindToStatment(WDBIStatement *pstmt,const char *colsname,int rowst);
    bool InsertRows(void **ptr,const char *colsname,int num);
    bool DeleteRow(int rown);
    long GetMemUsed();
    int sortcolumn[MAX_COLUMN];//排序列
    int sortcolumnpartlen[MAX_COLUMN];//部分索引排序的支持
    unsigned int nSortColumn; //排序列数
    //int CompareSortedColumn(unsigned int row1,void **ptr);
    int CompareSortedColumn(unsigned int _row1, void **ptr,int nRellSortColumn = -1);
    int SearchSortedRows(void **ptr,int &nStartSortRowNum,int &nEndSortRowNum,int nReallSortCol = -1);
    int Search(void **ptr,int schopt=0,int nReallSortCol=-1);
    bool GetAddr(int colid,void **buf,int off);
    void SetStartRows(unsigned int st);
    unsigned int StartRows;
    bool ValuesSetTo(const char *colto,const char *colfrom,DataTable *tbFrom,bool destKey,int op);
    bool CompactBuf();
    bool CopyRowsTo(DataTable *tbTo,int toStart,int start,int rowsnum=1,bool cut=true);
    bool GetColumnDesc(void **pColDesc,int &cdlen,int &_colnm) ;
    bool CopyColumnDefine(const char *_colsname, DataTable *tbFrom);
    bool ExportSomeRowsWithNF(char *pData,int startrn,int rnnum);
    long ExportSomeRows(char *pData,int startrn,int rnnum);
    bool Export(void *pData,long &_len,int &_maxrows,int &_rowct) ;
    bool Import(void *pData,long _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct);
    long AppendRows(char *pData,int rnnum);
    bool AppendRowsWithNF(char *pData,int rnnum);
    int ConvertColStrToInt(const char *colsnm,int *pbf,int *partlen=NULL);
    bool SetSortColumn(const char *colsnm);
    int GetColumnId(const char *cnm);
    bool CopyToTab(const char *tabname,WDBISession *ps=NULL,bool autocommit=true,bool withcolname=false);
    bool GeneTable(const char *tabname,WDBISession *psess=NULL);
    int GetColumnDispWidth(int col);
    bool GetCell(unsigned int row,int col,char *str,bool rawpos=false);
    void GetColumnTitle(int col,char *str,int len);
    int CompareSortRow(unsigned int row1, unsigned int row2);
    void CopySortRow(unsigned int from,unsigned int to);
    void RebuildHeap(unsigned int m,unsigned int n);
    bool Sort();

    int  BackupSortAVL(char *pBuff,int nLen);
    int  GetSortAVLLen();
    bool SetAVLSort(char *pBuff,int nNodeNum,int nRootPos);

    void adjustColumnType(bool initadj,bool importmode);
    bool SetSortColumn(int *col);
    bool OrderByPK();
    bool IsPKSet();
    bool RefAllCol(ub4 rowst,char **bf);
    short GetColumnType(int id);
    unsigned int GetColumnScale(int id)
    {
        return cd[id].scale;
    }
    int GetColumnNum();
    bool SetPKID(const char *nm);
    int SearchPK(int key,int schopt=0);
    bool SetPKID(int id);
    bool FetchPrepare(ub4 rn);
    DataTable *GetNextTable()
    {
        return pNext;
    }
    void SetNextTable(DataTable *pTb)
    {
        pNext=pTb;
    }
    DataTable(bool atthr=true)
    {
        colct=rowct=0,
        //pbf=NULL,
        pstmt=NULL,dtname[0]=0;
        pNext=NULL;
        pkid=-1;
        pSortedPos=NULL;
        nSortColumn=0;
        maxrows=bflen=0;
        autoThrow=atthr;
        pPKSortedPos=NULL;
        pnullind=NULL;
        pQDelKey=NULL;
        qdelct=0;
        rowlen=0;
        StartRows=0;
        ptree=NULL;
        isinorder=false;
        SetNullColBuff();
        m_nVarLenLimit = 8; //大于8字节的设置变长
    }
    void SetColumnDisplay(const char *cols,int colid,const char *dspname,int prec,int scale)
    {
        int ind=-1;
        if(cols!=NULL) GetColumnIndex(cols);
        else ind=colid;
        if(ind<0 || ind>=colct) return;
        if(scale>=0) cd[ind].scale=scale;
        if(prec>=0) cd[ind].prec=prec;
        if(strlen(dspname)<COLNAME_LEN-1) {
            strcpy(cd[ind].dispname,dspname);
            if(strlen(dspname)>=cd[ind].dspsize)
                cd[ind].dspsize=strlen(dspname)+1;
        }
        adjustColumnType(false,false);
    }
    bool IsNull(int col,unsigned int rowrn);
    void SetNull(int col,unsigned int rowrn,bool isnull=true);
    void ExportNullFlag(void *pData,int &_len);
    void ImportNullFlag(void *pData);

    int  ExportQDeleteFlag(void *pData,int &nDelete);
    void ImportQDeleteFlag(void *pData,int nDelete);

    bool DeleteToTabWithPK(const char *tabname, WDBISession *ps,bool autocommit=true);
    bool SetBindNullFlagForOutMem(WDBIStatement *_pstmt,int pos,int colid,int rowst);
    int CompareColumn(int *pnCol,unsigned int _row,void **ptr);

    long GetVarBuffPos(int nCol,long rown ,int nCurSec=-1);
    long GetVarOnlyRown(int nCol,ub4 nRown,int nCurSec)
    {
        long Row = 0;
        for (int i=0; i<nCurSec; i++) {
            Row = Row + m_nszRowNumPerSec[nCol][i];
        }
        Row = Row + nRown;
        return Row;
    }

    long GetVarDataNum(int nCol)
    {
        int nUseCurSec = m_nUseBuffSectionNum[nCol];
        long nNum = 0;
        for(int i=0; i<= nUseCurSec; i++) {
            nNum = nNum + m_nszRowNumPerSec[nCol][i];
        }
        return nNum;
    }

    const char * GetVarAddress(int nCol,long nRow)
    {
        long nPos = GetVarBuffPos(nCol,nRow);
        char *p = m_pColBuff[nCol][0] + nPos;
        return (const char *)p;
    }

    long GetVarBuffSize(int nCol)
    {
        long nSize = 0;
        for (int i=0; i<=m_nUseBuffSectionNum[nCol]; i++) {
            nSize = nSize + m_nVarSecBfMaxSize[nCol][i];
        }
        return nSize;
    }

    long GetVarDataSize(int nCol)
    {
        long nVarRowNum = GetVarDataNum(nCol);
        if(rowct==0 ||nVarRowNum==0 ) {
            return 0;
        }
        long nSize = 0;
        for (int i=0; i<m_nUseBuffSectionNum[nCol]; i++) {
            nSize = nSize + m_nVarSecBfMaxSize[nCol][i];
        }
        nSize = nSize + m_pColPerDataLen[nCol][0][nVarRowNum -1];
        return nSize;
    }

    int GetVarColAndVarLen(int *pCol,int *pTypeLen)
    {
        for(int i=0; i<colct; i++) {
            if(FindVarColID(i)>-1) {
                pCol[i] = 1;
            } else {
                pCol[i] = 0;
            }
            if(pTypeLen !=NULL)
                pTypeLen[i] = cd[i].dtsize;
        }
        return colct;
    }

    int FindVarColID(int nCol)
    {
        if (m_nNumVarCol==0) {
            return -1;
        }
        for (int i=0; i<m_nNumVarCol; i++) {
            if(m_nIDVarCol[i] == nCol) {
                return i;
            }
        }
        return -1;
    }
    void DelExtendVarData()
    {
        for (int i=0; i<colct; i++) {
            if (m_pColExtend[i]!=NULL) {
                delete[] m_pColExtend[i];
                m_pColExtend[i] = NULL;
            }
        }
        RestorePtr();
    }

    long GetReNewVarBuffSize(int nCol,long nNeedSize=0);
    int GetVarMaxFetchRow(int nMaxRow);
    int InsertVarRow(int nCol,long nStRow,const char *pStr,int nNum);
    int DeleteVarRow(int nCol,long nFromRow,long nDelRowNum);
    int ExtendColVarData(int nCol,long StRow,long nNumRow, char **pBindStr);       //必须调用
    int GetVarPerDataLen(int nCol,ub4 *pDataLen, long stRow,long RowNum,int nUseCurSec = -1);
    long GetVarCharVal(int nCol,long nStRow,char *pStr = NULL,long nRowNum=1);
    int VarCharBuffModify(ub4 nCurRow);
    int SetBuffWithoutChar();
    int ResetVarCharBuff(int nColVarID,long nCurRow);
    int CopyVarCharFromOtherTab(int nCol,long nToBeginRow,long nScBeginRow,long nCopyRowNum,DataTable *pTab);
    int CopyVarDataInTab(int nCol,long nToRow,long nFromRow,long nCopyRowNum);
    int FindVarSecRow(int nCol,long nSrcRow,ub4 &nCurRow)
    {
        long nTempCurRow = nSrcRow;
        int  nSec = -1;
        for (int i=0; i<m_nUseBuffSectionNum[nCol]; i++) {
            if (nTempCurRow >= m_nszRowNumPerSec[nCol][i]) {
                nTempCurRow = nTempCurRow - m_nszRowNumPerSec[nCol][i];
                continue;
            } else {
                nSec = i;
                break;
            }
        }
        if (nSec == -1) {
            nSec = m_nUseBuffSectionNum[nCol];
        }
        nCurRow = nTempCurRow;
        return nSec;
    }

    void SetNullColBuff()
    {
        for (int i=0; i< MAX_COLUMN ; i++) {
            for (int j=0; j<BUFF_SEC_NUM; j++) {
                m_pColBuff[i][j] = NULL;
                m_pColPerDataLen[i][j] = NULL;
            }
            m_pColExtend[i] = NULL;
        }
        memset(m_nIDVarCol,0,sizeof(ub4)*MAX_COLUMN);
        memset(m_nszRowNumPerSec,0,sizeof(ub4)*BUFF_SEC_NUM*MAX_COLUMN);
        memset(m_nUseBuffSectionNum,0,sizeof(ub4)*MAX_COLUMN);
        m_nNumVarCol = 0;
        m_nFirstMixRowN = 0;
    }

    void ReSetColBuff()
    {

        for(int nCol = 0; nCol<colct; nCol++) {
            if(FindVarColID(nCol)>-1) {
                int nCurSecNum = m_nUseBuffSectionNum[nCol];
                long nBuffSize = m_nVarSecBfMaxSize[nCol][0];
                if(nCurSecNum > 0) {
                    char *pTem = NULL;
                    pTem= (char *)realloc(m_pColBuff[nCol][0],nBuffSize);
                    if(pTem !=NULL) {
                        m_pColBuff[nCol][0] = pTem;
                    }
                }
                memset(m_pColBuff[nCol][0],0,sizeof(char)*nBuffSize);
                memset(m_pColPerDataLen[nCol][0],0,sizeof(ub4)*maxrows);
                memset(m_nszRowNumPerSec[nCol],0,BUFF_SEC_NUM*sizeof(ub4));
                memset(m_nVarSecBfMaxSize[nCol],0,BUFF_SEC_NUM*sizeof(long));
                m_nVarSecBfMaxSize[nCol][0] = nBuffSize;
                m_nUseBuffSectionNum[nCol]= 0;
            } else {
                memset(m_pColBuff[nCol][0], 0, sizeof(char)*cd[nCol].dtsize*maxrows);
            }
        }
        RestorePtr();
    }

    void ReleaseColBuff()
    {
        for (int i=0; i< MAX_COLUMN ; i++) {
            if (m_pColBuff[i][0] != NULL) {
                free(m_pColBuff[i][0]); //delete[] m_pColBuff[i][0];
                m_pColBuff[i][0] = NULL;
            }
            if (m_pColPerDataLen[i][0] != NULL) {
                delete[] m_pColPerDataLen[i][0];
                m_pColPerDataLen[i][0] = NULL;
            }

            if (m_pColExtend[i] != NULL) {
                delete[] m_pColExtend[i];
                m_pColExtend[i] = NULL;
            }
            for(int j=0; j<BUFF_SEC_NUM; j++) {
                m_nVarSecBfMaxSize[i][j]=0;
            }
        }
        memset(m_nIDVarCol,0,sizeof(ub4)*MAX_COLUMN);
        memset(m_nszRowNumPerSec,0,sizeof(ub4)*BUFF_SEC_NUM*MAX_COLUMN);
        memset(m_nUseBuffSectionNum,0,sizeof(ub4)*MAX_COLUMN);
        m_nNumVarCol = 0;
        m_nFirstMixRowN = 0;
    }

    void CutRowData(long nRowst)
    {
        for(int i=0; i<colct; i++) {
            if(FindVarColID(i)>-1) {
                long nRowCount = GetVarDataNum(i);
                if(nRowst >= nRowCount) {
                    continue;
                }
                ub4 nCurSecRow = 0;
                int nSec = FindVarSecRow(i,nRowst,nCurSecRow);
                long nAllBuffSize = GetVarBuffSize(i);
                long nStartPos = GetVarBuffPos(i,nRowst);
                long nSizeBuff = 0;
                for(int s = nSec + 1; s<=m_nUseBuffSectionNum[i]; s++) {
                    nSizeBuff = nSizeBuff + m_nVarSecBfMaxSize[i][s];
                    m_nszRowNumPerSec[i][s]=0;
                }

                m_nVarSecBfMaxSize[i][nSec]= m_nVarSecBfMaxSize[i][nSec]+nSizeBuff;
                m_nUseBuffSectionNum[i] = nSec;
                m_nszRowNumPerSec[i][nSec] = nCurSecRow;
                memset(m_pColBuff[i][0]+nStartPos,0,(nAllBuffSize-nStartPos)*sizeof(char));
                memset(m_pColPerDataLen[i][0]+nRowst,0,(nRowCount-nRowst)*sizeof(ub4));

            } else {
                if(nRowst >= rowct) {
                    continue;
                }
                memset(m_pColBuff[i][0]+nRowst*cd[i].dtsize,0,(rowct - nRowst)*cd[i].dtsize);
            }
        }
    }



    virtual ~DataTable()
    {
        ReleaseColBuff();
        if(pSortedPos) {
            delete [] pSortedPos;
            pSortedPos=NULL;
        }
        if(pPKSortedPos) {
            delete [] pPKSortedPos;
            pPKSortedPos=NULL;
        }
        if(pQDelKey) {
            delete [] pQDelKey;
            pQDelKey=NULL;
        }
        if(pnullind) {
            delete [] pnullind;
            pnullind=NULL;
        }
        destroyptree();
    }
    void SetName(const char *nm)
    {
        strcpy(dtname,nm);
    }
    bool MatchName(const char *nm)
    {
#ifndef __unix
        return _stricmp(dtname,nm)==0;
#else
        return strcasecmp(dtname,nm)==0;
#endif
    }

    void Clear()
    {
        //if(pbf) {
        //  delete [] pbf;
        //  pbf=NULL;
        //}
        ReleaseColBuff();
        if(pSortedPos) {
            delete [] pSortedPos;
            pSortedPos=NULL;
        }
        if(pPKSortedPos) {
            delete [] pPKSortedPos;
            pPKSortedPos=NULL;
        }
        if(pQDelKey) {
            delete [] pQDelKey;
            pQDelKey=NULL;
        }
        if(pnullind) {
            delete [] pnullind;
            pnullind=NULL;
        }
        qdelct=0;//qdelmax=0;
        maxrows=bflen=0;
        pstmt=NULL;
        nSortColumn=0;
        rowlen=0;
        pkid=-1;
        colct=rowct=0;
        sortcolumn[0]=-1;
        memset(cd,0,sizeof(cd));
        nSortColumn=0;
        StartRows=0;
    }
    //保留表结构，清除索引结构，数据行数置零
    void Reset()
    {
        if(pSortedPos) {
            delete [] pSortedPos;
            pSortedPos=NULL;
        }
        if(pPKSortedPos) {
            delete [] pPKSortedPos;
            pPKSortedPos=NULL;
        }
        destroyptree();
        qdelct=0;//qdelmax=0;
        nSortColumn=0;
        pkid=-1;
        rowct=0;
        sortcolumn[0]=-1;
        StartRows=0;
        //memset(pbf,0,rowlen*maxrows);
        ReSetColBuff();
        if(pnullind) {
            //memset(pnullind,0,sizeof(int)*colct*maxrows);
            //long nLen = maxrows%8==0?maxrows/8:maxrows/8+1;
            memset(pnullind,0,sizeof(mbyte)*(colct*maxrows/8 + 1));
        }
    }
    void SetRows(int rn)
    {
        rowct=rn;
    }
    void clearSort()
    {
        if(pSortedPos) {
            delete [] pSortedPos;
            pSortedPos=NULL;
        }
        destroyptree();
        sortcolumn[0]=-1;
        nSortColumn=0;
    }
    void clearIK()
    {
        if(pPKSortedPos) {
            delete [] pPKSortedPos;
            pPKSortedPos=NULL;
        }
        pkid=-1;
    }
    bool AddColumn(const char *name,const char *dspname,int ctype,int length=-1,int scale=0);
    virtual bool Build(ub4 rows,bool noadj=false,bool importmode=false);
    void SetColDspName(const char *colnm,const char *nm);

    int GetValues(const char *col,ub4 rowst,ub4 rownm,int *bf); //integer type
    int GetValues(const char *col,ub4 rowst,ub4 rownm,LONG64 *bf); //big integer type
    int GetValues(const char *col,ub4 rowst,ub4 rownm,double *bf); //NUM FLOAT type
    int GetValues(const char *col,ub4 rowst,ub4 rownm,char *bf); //Date type
    int GetValues(const char *col,ub4 rowst,ub4 rownm,char *bf,int &cellLen);//char varchar type

    int GetValues(ub4 colid,ub4 rowst,ub4 rownm,int *bf); //integer type
    int GetValues(ub4 colid,ub4 rowst,ub4 rownm,LONG64 *bf); //big integer type
    int GetValues(ub4 colid,ub4 rowst,ub4 rownm,double *bf); //NUM FLOAT type
    int GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf); //Date type
    int GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf,int &cellLen);//char varchar type

    int GetAddr(const char *col,ub4 rowst,ub4 rownm,int **bf); //integer type
    int GetAddr(const char *col,ub4 rowst,ub4 rownm,LONG64 **bf); //bit integer type
    int GetAddr(const char *col,ub4 rowst,ub4 rownm,double **bf); //NUM FLOAT type
    int GetAddr(const char *col,ub4 rowst,ub4 rownm,char **bf); //Date type
    int GetAddr(const char *col,ub4 rowst,ub4 rownm,char **bf,int &cellLen);//char varchar type

    int GetAddr(ub4 colid,ub4 rowst,ub4 rownm,int **bf); //integer type
    int GetAddr(ub4 colid,ub4 rowst,ub4 rownm,LONG64 **bf); //big integer type
    int GetAddr(ub4 colid,ub4 rowst,ub4 rownm,double **bf); //NUM FLOAT type
    int GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf); //Date type
    int GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf,int &cellLen);//char varchar type <8
    const char * GetAddr(ub4 colid,ub4 rowst,int &cellLen) ;  //char varchar > 8;

    int SetValues(ub4 colid,ub4 rowst,ub4 rownm,const int *bf); //integer type
    int SetValues(ub4 colid,ub4 rowst,ub4 rownm,const LONG64 *bf); //big integer type
    int SetValues(ub4 colid,ub4 rowst,ub4 rownm,const double *bf); //NUM FLOAT type
    int SetValues(ub4 colid,ub4 rowst,ub4 rownm,const char *bf); //Date type
    int SetValues(ub4 colid,ub4 rowst,ub4 rownm,const char *bf,int &cellLen);//char varchar type

    int GetColumnIndex(const char *name);
    int GetColumnName(ub4 colid,char *nmbf);
    int SetColumnName(ub4 colid,char *nmbf);


    int GetColumnLen(ub4 colid);

    bool BuildStmt(WDBIStatement *pst,ub4 rows,bool noadj=false,bool importmode=false);
    unsigned int FetchAll(int st);
    unsigned int FetchFirst(ub4 rn,ub4 st=0);
    unsigned int FetchNext(ub4 rn,ub4 st=0);
    bool GetCompactLen(int rowstart,int rownum,const char *colsnm,int *clen);
    bool GetLine(char *str,int rownm,bool rawpos=false,const char *colsnm=NULL,int *clen=NULL) ;
    bool GetLineStr(char *str,int _rownm,bool rawpos=false,const char *colsnm=NULL,int  *clen=NULL,char escapecha=0);
    void GetTitle(char *str,int l,const char *colsnm,int *clen=NULL);
    int GetRows()
    {
        return min(maxrows,rowct-StartRows);
    }
    long GetBfLen()
    {
        long nBuffLen = 0;
        for(int i=0; i<colct; i++) {
            if(FindVarColID(i) >-1) {
                nBuffLen = nBuffLen + GetVarBuffSize(i);
            } else {
                nBuffLen = nBuffLen + cd[i].dtsize*maxrows;
            }
        }
        return nBuffLen;
    }
    ub4 GetRowLen()
    {
        return rowlen;
    }

    BOOL  CheckData(int nCol)      //调试使用函数
    {

        if(FindVarColID(nCol)>-1) {
            char *pBf = m_pColBuff[nCol][0];
            int n = 0;
            long off = 0;
            int Rown = GetVarDataNum(nCol);
            int i = 0;
            while(i<Rown) {
                if(*(pBf+off)==0) {
                    if((off+1) == GetVarBuffPos(nCol,i+1)) {
                        i++;
                    } else {
                        return FALSE;
                    }
                }
                off++;
            }
            return TRUE;
        }
        return TRUE;
    }
    //if WORDS_BIGENDIAN
    void ReverseByteOrder(int rowst=0);
private:
    void destroyptree();
    char * LocateCell(ub4 colid,ub4 rowst);
    void RestorePtr();
public:
    bool SortHeap();
    int DecodeIBBRow(int row,char *recbuf,char *nullflag);
    void CopyToIBBinary(unsigned int startrow, unsigned int rownum, FILE *fp);
    int CopyToIBBuffer(unsigned int startrow,char *buf,int maxbuflen,int *actual_len);
    void CopyToMySQL(unsigned int startrow,unsigned int rownum,FILE *fp);
    int GetCreateTableSQL(char *buf,const char *tabname,bool ismysql=false);

    //>> begin: get create column comment sql, JIRA DMA-460 20130116
    int GetCreateTableColumnCommentSQL(char *crttab,const char *tabname,const void* stColumComment,const int columnsNum,bool ismysql = true);
    int GetColumnCommentInfo(char *crttab,const void* columnCommentInfo,const int columnsNum,bool ismysql = true);
    int strcasecmp(const char* s1,const char* s2);
    //>> end: get create column comment sql, JIRA DMA-460 20130116

    void AddrFresh(char **colval,int *collen,int *coltp);
    void ReInOrder();
    int LoadSort(FILE * fp);
    int SaveSort(FILE * fp);
    virtual sb4 GetErrCode()
    {
        if(errcode!=0) return errcode;
        if(pstmt!=NULL) return pstmt->GetErrCode();
        return 0;
    }
    bool IsValueCol(int col)
    {
        return cd[col].type==SQLT_NUM || cd[col].type==SQLT_FLT || cd[col].type==SQLT_INT || cd[col].type==SQLT_LNG;
    }
    int TransToInt(void *ptr,int off,int ct);
    double TransToFlt(void *ptr,int off,int ct);
    LONG64 TransToLng(void *ptr,int off,int ct);
    void * TransType(void *buf,void *ptr,int off,int ct1,int ct2);
    void TransSet(void *buf,void *ptr,int off,int ct1,int ct2,int op,int collen);
};

class MemTable :public DataTable
{
    DataTable *dtSrc,*dtRef; //DataTable source(dynamic data),DataTable be referenced(option)
    int grpbyColSrc[MAX_GROUPCOL],grpbyColRef[MAX_GROUPCOL]; // -1 represent end.
    int calCol[MAX_GROUPCOL]; // Only apply to source datatable,-1 represent end;
//  double sum[MAX_GROPUCOL];
//  int counter; //rows been grouped.
    int srckey; //Source key column to map to reference datatable
    int keycolsrc[MAX_COLUMN];
    int keycolsrcct; // source key column count

public:
    bool SetGroupRef(DataTable *ref,const char *colnm);
    bool SetSortedGroupRef(DataTable *ref,const char *colssrc);
    bool SetGroupColSrc(const char *colsnm);
    bool SetGroupColRef(const char *colsnm);
    bool SetCalCol(const char *colsnm);
    virtual bool Build(ub4 rows,bool noadj=false,bool importmode=false);
    MemTable(bool atthr=true):DataTable(atthr)
    {
        dtSrc=dtRef=NULL;
//      counter=0;
        srckey=-1;
        keycolsrcct=0;
        for(int i=0; i<MAX_GROUPCOL; i++) {
            calCol[i]=grpbyColSrc[i]=grpbyColRef[i]=-1;
//          sum[i]=0;
        }
    };
    bool SetGroupSrc(DataTable *src) ;
    bool SetGroupRef(DataTable *ref,unsigned int skey) ;
    bool SetGroupColSrc(int *colarray);//-1 means end
    bool SetGroupColRef(int *colarray);//-1 means end
    bool SetCalCol(int *colarray);//only apply to src,-1 means end
    bool Group(int rowstart,int rownum);
};


class DataTableLink:public WDBIError
{
    DataTable *pHome;
public :
    bool ExistTable(const char *nm);
    DataTableLink(bool athr=true) ;
    ~DataTableLink() ;
    DataTable *FirstTab() ;
    DataTable *LastTab() ;
    DataTable *AddTable(const char *name,bool atthr=true) ;
    DataTable *FindTable(const char *name) ;
    bool DeleteTable(const char *name) ;
private:
    void ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params);
};

class WOCIDate
{
    unsigned char date[7];
    struct tm strtm;
    time_t tmtm;

public:
    WOCIDate() {};
    WOCIDate(const char *dt)
    {
        SetDate(dt);
    }
    void SetDate(const char *dt)
    {
        memcpy(date,dt,7);
    }
    void SetToNow()
    {
        time( &tmtm);
        strtm=*localtime(&tmtm);
        date[0]=(strtm.tm_year+1900)/100+100;
        date[1]=(strtm.tm_year+1900)%100+100;
        date[2]=strtm.tm_mon+1;
        date[3]=strtm.tm_mday;
        date[4]=strtm.tm_hour+1;
        date[5]=strtm.tm_min+1;
        date[6]=strtm.tm_sec+1;
    };
    void GetString(char *str)
    {
        if(date[4]!=0)
            //sprintf(str,"%04d/%02d/%02d %02d:%02d:%02d   ", // len =20
            sprintf(str,"%04d/%02d/%02d %02d:%02d:%02d ", // len =20
                    (date[0]-100)*100+date[1]%100,date[2],date[3],date[4]-1,date[5]-1,date[6]-1);
        else {
            memset(str,' ',20);
            str[20]=0;
            //str[0]=0;
        }
    }
    void GetString_csv(char *str)
    {
        if(date[4]!=0)
            //sprintf(str,"%04d/%02d/%02d %02d:%02d:%02d   ", // len =20
            sprintf(str,"\"%04d/%02d/%02d %02d:%02d:%02d\"", // len =20
                    (date[0]-100)*100+date[1]%100,date[2],date[3],date[4]-1,date[5]-1,date[6]-1);
        else {
            memset(str,' ',20);
            str[20]=0;
            //str[0]=0;
        }
    }
    int GetYear()
    {
        return (date[0]-100)*100+date[1]-100;
    }
    int GetMonth()
    {
        return date[2];
    }
    int GetDay()
    {
        return date[3];
    }
    int GetHour()
    {
        return date[4]-1;
    }
    int GetMin()
    {
        return date[5]-1;
    }
    int GetSec()
    {
        return date[6]-1;
    }

    time_t GetTime_T()
    {
        convtm();
        return tmtm;
    }
    void GetStruct_Tm(struct tm * stm)
    {
        convtm();
        memcpy(stm,&strtm,sizeof(strtm));
    }
private:
    void convtm()
    {
        if(date[4]==0) {
            tmtm=0;
            strtm=*localtime(&tmtm);
        } else {
            strtm.tm_year=GetYear()-1900;
            strtm.tm_mon=GetMonth()-1;
            strtm.tm_mday=GetDay();
            strtm.tm_hour=GetHour();
            strtm.tm_min=GetMin();
            strtm.tm_sec=GetSec();
            tmtm=mktime(&strtm);
            strtm=*localtime(&tmtm);
        }
    }
};

void frprt(int val,char *str);
extern FILE *errfp;
extern FILE *lgfp;
extern bool __output_to_console;

#define WDBIThrow(a) {a.SetErrPos(__LINE__,__FILE__);a.Throw();}
#endif
