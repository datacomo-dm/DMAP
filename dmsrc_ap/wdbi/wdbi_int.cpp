#include "wdbi.h"
#include "wdbi_inc.h"
#include "wdbierr.h"
#if !defined(__unix) && !defined(NO_EXCEL)
#include "WOCIExcelEnv1.h"
#endif
#define INT_INVALID_HANDLE      -1
#define INT_ALLOCATE_FAIL       -2
#define INT_NOHANDLERES         -3
#define INT_CREATEOBJ_FAIL      -4
#define INT_CREATEMTMUTEX_FAIL  -5
#define INT_CREATESTMUTEX_FAIL  -6
#define INT_WAITMTMUTEX_FAIL    -7
#define INT_WAITSTMUTEX_FAIL    -8
#define INT_STMT_BUSY       -9
#define INT_INVALID_DBSTYPE -10
#define INT_NOSUPPORT_DBSTYPE   -11
#define INT_USER_CANCEL     -12
#define MAX_SESSION 820
#define MAX_STATMENT 1620
#define MAX_MEMTABLE 1620
#define MAX_EXCELFILE 30

#define HANDLE_SESS         0x2000
#define HANDLE_STMT         0x4000
#define HANDLE_MEMTAB       0x6000
#define HANDLE_EXCELF       0x8000
#ifndef WIN32
#define INFINITE -1
#define WAIT_OBJECT_0 0
#define WAIT_TIMEOUT -1
#endif
#define ThrowInvalidHandle(code) {ErrorCheck(__FILE__,__LINE__,code);}

static WDBISession * hd_sess[MAX_SESSION];
static WDBIStatement * hd_stmt[MAX_STATMENT];
static MemTable * hd_memtab[MAX_MEMTABLE];
static int id_stmtused[MAX_MEMTABLE];

#ifdef __unix
static pthread_t hd_pthread_t[MAX_MEMTABLE];
static pthread_mutex_t hd_mutex_stmt[MAX_MEMTABLE];
static pthread_mutex_t hd_mutex_getfree;//PTHREAD_MUTEX_INITIALIZER;
#define thread_rt void *
#define thread_end return NULL
#else
static HANDLE hd_mutex_stmt[MAX_MEMTABLE];
static HANDLE hd_mutex_getfree=NULL;
static HANDLE inprocess[MAX_STATMENT];
#define thread_rt void
#define thread_end return
#ifndef NO_EXCEL
static WOCIExcelEnv * hd_excelf[MAX_EXCELFILE];
#endif
#endif

static int retval_stmt[MAX_STATMENT];
static bool mutex_stmt_busy[MAX_STATMENT];

//static HANDLE hd_mutex_memtab[MAX_MEMTABLE];
static int retval_memtab[MAX_MEMTABLE];
static bool throwed=false;
//static bool mutex_memtab_busy[MAX_STATMENT];
void ErrorCheck(const char *fn, int ln,int errcode);
DllExport void _wdbiSetTraceFile(const char *fn); 

bool asyncMode=false;
int LockStmt(int stmt,int tm);
int LockMemtable(int memtab,int tm);
void UnlockStmt(int stmt);
void UnlockMemtable(int memtab);
DllExport int _wdbiWaitStmt(int stmt,int tm);
DllExport int _wdbiWaitLastReturn(int handle) ;
DllExport void _wdbiMTToTextFile(int memtab,const char *fn,int rownm,const char *colsnm) ;
//////////////////////////////////////////////////////////////////
// Common function . 
//
//
//////////////////////////////////////////////////////////////////
static bool wdbiinitialized=false;
DllExport void _WDBIInit(const char *appname) {
    if(wdbiinitialized) return;
    
    if(appname) {
        _wdbiSetTraceFile(appname);
    }
    else {
        _wdbiSetTraceFile("wdbi8");
    }
    WDBIStartup();
    int i;
    for(i=0;i<MAX_SESSION;i++)
        hd_sess[i]=NULL;
    for(i=0;i<MAX_STATMENT;i++)
        hd_stmt[i]=NULL;
    for(i=0;i<MAX_MEMTABLE;i++)
        hd_memtab[i]=NULL;
#if !(defined(__unix) || defined(NO_EXCEL))
    for(i=0;i<MAX_EXCELFILE;i++)
        hd_excelf[i]=NULL;
#endif
#ifdef __unix
    //if(hd_mutex_getfree==NULL)
    if(pthread_mutex_init(&hd_mutex_getfree,NULL)!=0)
        ThrowInvalidHandle(INT_CREATESTMUTEX_FAIL);
#else
    if(hd_mutex_getfree==NULL)
    hd_mutex_getfree=CreateMutex(NULL,false,NULL);
    if(hd_mutex_getfree==NULL) ThrowInvalidHandle(INT_CREATESTMUTEX_FAIL);
#endif
    wdbiinitialized=true;
}

void BreakAll() {
    for(int i=0;i<MAX_SESSION;i++) {
        if(hd_sess[i]) {
            hd_sess[i]->SetTerminate(true);
        }
    }
}

DllExport void _wdbiBreakAll() {
    BreakAll();
}
    void CatchProcess() {
    bool skip=true;
    int i;
    for(i=0;i<MAX_MEMTABLE;i++) {
        if(hd_memtab[i]) {
            if(hd_memtab[i]->GetRows()>0) {
                skip=false;break;
            }
        }
    }
    if(!skip) {
        if(WDBIError::IsEcho()) {
        printf("Before exit,you have the last chance to save non-empty memory table.\n");
        printf("Save them to a text file (y/n)?");//是否需要保存内存表的内容到文本文件<Y/N>?");
        fflush(stdout);
        int ch=getch();
        if(ch=='Y' || ch=='y') {
            printf("\nThe contents of memory table(s) will be appended to the file or creating on new file.");//内存表内容被追加到文件尾，新文件自动创建。");
            printf("\nInput file name :");//输入文件名:");
            fflush(stdout);
            char fn[300];
            scanf("%s",fn);
            for(i=0;i<MAX_MEMTABLE;i++) {
                if(hd_memtab[i]) {
                    if(hd_memtab[i]->GetRows()>0) {
                        _wdbiMTToTextFile(HANDLE_MEMTAB+i,fn,0,NULL);
                    }
                }
            }
        }
        }
    }
}

DllExport int _wdbiDestroyAll() 
{
    int i;
    for(i=0;i<MAX_STATMENT;i++) {
        if(hd_stmt[i]) {
            if(hd_stmt[i]==(WDBIStatement *)-1) {
                hd_stmt[i]=NULL;
                continue;
            }
            if(mutex_stmt_busy[i]) {
                ThrowInvalidHandle(INT_STMT_BUSY);
                _wdbiWaitStmt(i+HANDLE_STMT,INFINITE);
            }
            delete hd_stmt[i];
            hd_stmt[i]=NULL;
#ifdef __unix
            pthread_mutex_unlock(&hd_mutex_stmt[i]);
            pthread_mutex_destroy(&hd_mutex_stmt[i]);
#else
            ReleaseMutex(hd_mutex_stmt[i]);
            CloseHandle(hd_mutex_stmt[i]);
#endif
        }
    }
    for(i=0;i<MAX_SESSION;i++) {
        if(hd_sess[i]) {
            if(hd_sess[i]==(WDBISession *)-1) {
                hd_sess[i]=NULL;
                continue;
            }
            delete hd_sess[i];
            hd_sess[i]=NULL;
        }
    }

    for(i=0;i<MAX_MEMTABLE;i++) {
        if(hd_memtab[i]) {
            if(hd_memtab[i]==(MemTable *)-1) {
                hd_memtab[i]=NULL;
                continue;
            }
            delete hd_memtab[i];
            hd_memtab[i]=NULL;
//          ReleaseMutex(hd_mutex_memtab[i]);
//          CloseHandle(hd_mutex_memtab[i]);
        }
    }
#if !(defined(__unix) || defined(NO_EXCEL))
    for(i=0;i<MAX_EXCELFILE;i++){
        if(hd_excelf[i]) {
            if(hd_excelf[i]==(WDBIExcelEnv *)-1) {
                hd_excelf[i]=NULL;
                continue;
            }
            delete hd_excelf[i];
            hd_excelf[i]=NULL;
        }
    }
#endif
#ifdef __unix
            pthread_mutex_unlock(&hd_mutex_getfree);
            pthread_mutex_destroy(&hd_mutex_getfree);
#else
    if(hd_mutex_getfree) {
            ReleaseMutex(hd_mutex_getfree);
            CloseHandle(hd_mutex_getfree);
    hd_mutex_getfree=NULL;
    }
#endif
    return 0;
    }
 
DllExport void _WDBIQuit() {
    if(!wdbiinitialized) return;
    _wdbiDestroyAll();
    WDBIShutdown();
    wdbiinitialized=false;
} 

int delstmtct=0;
DllExport int _wdbidestroy(int handle) {
    if(handle<HANDLE_SESS)  ThrowInvalidHandle(INT_INVALID_HANDLE);//ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(handle<HANDLE_STMT) {
        handle-=HANDLE_SESS;
        if(handle>MAX_SESSION) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_sess[handle]) {
            if(hd_sess[handle]!=(void *)-1)
            try {
                delete hd_sess[handle];
            }
            catch(...) {
                hd_sess[handle]=NULL;
                if(!throwed) {
                    throwed=true;
                    throw;
                }
            }
            hd_sess[handle]=NULL;
        }
        //else 
        //  ThrowInvalidHandle(INT_INVALID_HANDLE);
    } else if(handle<HANDLE_MEMTAB) {
        handle-=HANDLE_STMT;
        if(handle>MAX_STATMENT) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(mutex_stmt_busy[handle]) {
            ThrowInvalidHandle(INT_STMT_BUSY);
            _wdbiWaitStmt(handle+HANDLE_STMT,INFINITE);
        }
        delstmtct++;
        if(hd_stmt[handle]) {
            if(hd_stmt[handle]!=(void *)-1) {
#ifdef __unix
            pthread_mutex_unlock(&hd_mutex_stmt[handle]);
            pthread_mutex_destroy(&hd_mutex_stmt[handle]);
#else
            ReleaseMutex(hd_mutex_stmt[handle]);
            CloseHandle(hd_mutex_stmt[handle]);
#endif
            try {
                delete hd_stmt[handle];
            }
            catch (...)
            {
                hd_stmt[handle]=NULL;
                if(!throwed) {
                    throwed=true;
                    throw;
                }
            }
            }
            hd_stmt[handle]=NULL;
            mutex_stmt_busy[handle]=false;
        }
        //else 
        //  ThrowInvalidHandle(INT_INVALID_HANDLE);
    }   
#if defined(__unix) || defined(NO_EXCEL)
    else {
#else
    else if(handle<HANDLE_EXCELF) {
#endif
        handle-=HANDLE_MEMTAB;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_memtab[handle]) {
            if(hd_memtab[handle]!=(void *)-1)
            try {
            delete hd_memtab[handle];
            }
            catch(...) {
                hd_memtab[handle]=NULL;
                if(!throwed) {
                    throwed=true;
                    throw;
                }
            }
            hd_memtab[handle]=NULL;
            //ReleaseMutex(hd_mutex_memtab[handle]);
            //CloseHandle(hd_mutex_memtab[handle]);
        }
        //else ThrowInvalidHandle(INT_INVALID_HANDLE);
    } 
#if !defined(__unix) &&  !defined(NO_EXCEL)
    else {
        handle-=HANDLE_EXCELF;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_excelf[handle]) {
            if(hd_excelf[handle]!=(void *)-1)
            delete hd_excelf[handle];
            hd_excelf[handle]=NULL;
        }
        //else ThrowInvalidHandle(INT_INVALID_HANDLE);
    }
#endif
    return 0;
}

int GetFreeHandle (void **ptr,int len) {
#ifdef __unix
    pthread_mutex_lock(&hd_mutex_getfree);
#else
    WaitForSingleObject(hd_mutex_getfree,INFINITE);
#endif
    int rt=-1;
    int i;
    for(i=0;i<len;i++) 
        if(ptr[i]==NULL) {rt=i;break;}
    if(rt!=-1) ptr[rt]=(void *)-1;
#ifdef __unix
    pthread_mutex_unlock(&hd_mutex_getfree);
#else
    ReleaseMutex(hd_mutex_getfree);
#endif
    if(rt!=-1) return rt;
    ThrowInvalidHandle(INT_NOHANDLERES);
    return -1;
}

inline int CheckSessHd(int p,int len) {
    if(p<0 || p>=len || hd_sess[p]==NULL) 
        ThrowInvalidHandle(INT_INVALID_HANDLE);
    return p;
}

inline int CheckStmtHd(int p,int len) {
    if(p<0 || p>=len || hd_stmt[p]==NULL) 
        ThrowInvalidHandle(INT_INVALID_HANDLE);
    return p;
}

inline int CheckMtHd(int p,int len) {
    if(p<0 || p>=len || hd_memtab[p]==NULL) 
        ThrowInvalidHandle(INT_INVALID_HANDLE);
    return p;
}

#if (defined(WIN32) && !defined(NO_EXCEL))
inline int CheckExcelHd(int p,int len) {
    if(p<0 || p>=len || hd_excelf[p]==NULL) 
        ThrowInvalidHandle(INT_INVALID_HANDLE);
    return p;
}
#endif
//////////////////////////////////////////////////////////////////
// Session function . 
//
//
//////////////////////////////////////////////////////////////////


DllExport int _wdbiCreateSession(const char *username,const char *password,const char *svcname,int dbs_type) {
    int f=GetFreeHandle((void **)hd_sess,MAX_SESSION);
    if(f==-1) ThrowInvalidHandle(INT_NOHANDLERES);
    WDBISession *p=NULL;
    if(dbs_type==DTDBTYPE_ORACLE)
    {
    #ifndef NO_OCI
      p= new WOCISession();
    #else 
      ThrowInvalidHandle(INT_NOSUPPORT_DBSTYPE);
    #endif
    }
    else if(dbs_type==DTDBTYPE_ODBC)
    {
    #ifndef NO_ODBC
      p= new WODBCSession();
    #else 
      ThrowInvalidHandle(INT_NOSUPPORT_DBSTYPE);
    #endif
    }
    else ThrowInvalidHandle(INT_INVALID_DBSTYPE);
    if(!p) ThrowInvalidHandle(INT_ALLOCATE_FAIL);
    hd_sess[f]=p;
    bool rt=false;
    try {
     rt=p->Connect(username,password,svcname);
    }
    catch(...) {
        delete p;
        hd_sess[f]=NULL;
        throwed=true;
        throw;
    }
    if(!rt) {
        delete p;
        hd_sess[f]=NULL;
        ThrowInvalidHandle(INT_CREATEOBJ_FAIL);
    }
    return f+HANDLE_SESS;
}

DllExport int _wdbiCommit(int sess) {
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_sess[f]->Commit();
    return 0;
}

DllExport int _wdbiRollback(int sess) {
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_sess[f]->Rollback();
    return 0;
}

DllExport int _wdbiSetTerminate(int sess,bool val) {
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_sess[f]->SetTerminate(val);
    return 0;
}

DllExport bool _wdbiIsTerminate(int sess) {
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) return false;
    return hd_sess[f]->IsTerminate();
}

DllExport bool _wdbiSetNonBlockMode(int sess) {
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) return false;
    return hd_sess[f]->SetNonBlockMode();
}

//////////////////////////////////////////////////////////////////
// Statment function . 
//
//
//////////////////////////////////////////////////////////////////
int stmtcrtct=0;
DllExport int _wdbiCreateStatment(int sess) {
    int t=GetFreeHandle((void **)hd_stmt,MAX_STATMENT);
    if(t==-1) ThrowInvalidHandle(INT_NOHANDLERES);
    int f=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    WDBIStatement *p=hd_sess[f]->NewStatement();
    if(!p) ThrowInvalidHandle(INT_ALLOCATE_FAIL);
    if(hd_stmt[t]!=(WDBIStatement *)-1) {
        throwed=true;
        throw;
    }
    stmtcrtct++;
    hd_stmt[t]=p;
    mutex_stmt_busy[t]=false;
#ifdef __unix
    if(pthread_mutex_init(&hd_mutex_stmt[t],NULL)!=0)
        ThrowInvalidHandle(INT_CREATESTMUTEX_FAIL);
#else
    hd_mutex_stmt[t]=CreateMutex(NULL,false,NULL);
    if(hd_mutex_stmt[t]==NULL) ThrowInvalidHandle(INT_CREATESTMUTEX_FAIL);
    inprocess[t]=CreateEvent(NULL,false,false,NULL);
#endif
    return t+HANDLE_STMT;
}

DllExport bool _wdbiBindStrByPos(int stmt,int pos,char *ptr,int len) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->BindByPos(pos,ptr,len);
}

DllExport unsigned int _wdbiGetFetchSize(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->GetFetchSize();
}

DllExport void _wdbiSetFetchSize(int stmt,unsigned int fsize) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return ;
    hd_stmt[f]->SetFetchSize(fsize);
}

DllExport bool _wdbiBindDoubleByPos(int stmt,int pos,double *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool _wdbiBindIntByPos(int stmt,int pos,int *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool _wdbiBindLongByPos(int stmt,int pos,LONG64 *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool _wdbiBindDateByPos(int stmt,int pos,char *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool _wdbiDefineStrByPos(int stmt,int pos,char *ptr,int len) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->DefineByPos(pos,ptr,len);
}

DllExport bool _wdbiDefineDoubleByPos(int stmt,int pos,double *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport bool _wdbiDefineIntByPos(int stmt,int pos,int *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport bool _wdbiDefineLongByPos(int stmt,int pos,LONG64 *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport bool _wdbiDefineDateByPos(int stmt,int pos,char *ptr) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return false;
    return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport int _wdbiBreakAndReset(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->BreakAndReset();
}

thread_rt Execute(void *ptr) {
    int f=((int *)ptr)[0];
    int times=((int *)ptr)[1];
    delete [](int *)ptr;
    if(asyncMode) {
#ifndef __unix
    LockStmt(f,INFINITE);
    SetEvent(inprocess[f]);
//#else
//  UnlockStmt(f);
#endif
    }
    try {
        retval_stmt[f]=(int)hd_stmt[f]->Execute(times);
        //if(retval_stmt[f]==0)
        retval_stmt[f]=(int)hd_stmt[f]->GetRows();
    }
    catch(WDBIError &e) {
      retval_stmt[f]=-1;
    }
    //catch(...) {
        //printf("A Error detected,program terminated!\n");
    //  retval_stmt[f]=-2;
    //}
    if(asyncMode) 
        UnlockStmt(f);
    thread_end;
}

DllExport int _wdbiExecute(int stmt,int times) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(mutex_stmt_busy[f]) ThrowInvalidHandle(INT_STMT_BUSY);
    int *val=new int[20];
    val[0]=f;val[1]=times;
    if(asyncMode) {
#ifndef __unix
    // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)
    _beginthread(Execute,0,(void *)val);
    WaitForSingleObject(inprocess[f],INFINITE);
#else
    LockStmt(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,Execute,(void *)val);
    pthread_detach(hd_pthread_t[f]);    
    //LockStmt(f,INFINITE);
#endif
    }
    else {
        Execute(val);
    }
    
    if(!asyncMode) {
        return _wdbiWaitLastReturn(stmt);
    }
    
    return retval_stmt[f];

}

DllExport int _wdbiExecuteAt(int stmt,int times,int offset) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(mutex_stmt_busy[f]) ThrowInvalidHandle(INT_STMT_BUSY);
    return hd_stmt[f]->Execute(times,offset);
}

DllExport int _wdbiFetch(int stmt,int rows) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(mutex_stmt_busy[f]) ThrowInvalidHandle(INT_STMT_BUSY);
    return hd_stmt[f]->Fetch(rows,true);
}

DllExport int _wdbiGetStmtColumnsNum(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetColumns();
}

DllExport int _wdbiGetMtColumnsNum(int mt) {
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnNum();
}

DllExport int _wdbiGetFetchedRows(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetRows();
}

DllExport int _wdbiGetSession(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    WDBISession *ps=hd_stmt[f]->GetSession();
    if(!ps) ThrowInvalidHandle(INT_INVALID_HANDLE);
    for(int i=0;i<MAX_SESSION;i++) {
        if(hd_sess[i]==ps) return i+HANDLE_SESS;
    }
    ThrowInvalidHandle(INT_INVALID_HANDLE);
    return -1;
}

DllExport unsigned short _wdbiGetStmtType(int stmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetStmtType();
}

DllExport bool _wdbiPrepareStmt(int stmt,const char *sqlstmt) {
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(mutex_stmt_busy[f]) ThrowInvalidHandle(INT_STMT_BUSY);
    if(f==-1) return false;
    return hd_stmt[f]->Prepare(sqlstmt);
}

//>> Begin:add by liujs
DllExport  int _wdbiGetStmtColumnNum(int stmt)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetColumnNum();
}
DllExport  bool _wdbiGetStmtColumnName(int stmt,int col,char *colname)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetColumnName(col,colname);
}
DllExport  int _wdbiGetStmtColumnType(int stmt,int col)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetColumnType(col);
}
DllExport  int _wdbiGetStmtColumnSize(int stmt,int col)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->GetColumnSize(col);
}
DllExport  bool _wdbiSetStmtColumnSize(int stmt,int col,int size)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_stmt[f]->SetColumnSize(col,size);
}
//>> End:add by liujs


//////////////////////////////////////////////////////////////////
// MemTable function . 
//
//
//////////////////////////////////////////////////////////////////

DllExport int _wdbiCreateMemTable() {
    int t=GetFreeHandle((void **)hd_memtab,MAX_MEMTABLE);
    if(t==-1) ThrowInvalidHandle(INT_NOHANDLERES);
    MemTable *p=new MemTable();
    if(!p) ThrowInvalidHandle(INT_ALLOCATE_FAIL);
    hd_memtab[t]=p;
    //hd_mutex_memtab[t]=CreateMutex(NULL,false,NULL);
    //if(hd_mutex_memtab[t]==NULL) ThrowInvalidHandle(INT_CREATEMTMUTEX_FAIL);
    //mutex_memtab_busy[t]=false;
    id_stmtused[t]=-1;
    return t+HANDLE_MEMTAB;
}

DllExport bool _wdbiAddColumn(int memtab,const char *name,const char *dspname,int ctype,int length,int scale) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->AddColumn(name,dspname,ctype,length,scale);
}

DllExport bool _wdbiBuildStmt(int memtab,int stmt,unsigned int rows)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(t==-1) return false;
    if(mutex_stmt_busy[t]) ThrowInvalidHandle(INT_STMT_BUSY);
    id_stmtused[f]=t;
    return hd_memtab[f]->BuildStmt(hd_stmt[t],rows);
}

DllExport bool _wdbiBuild(int memtab,unsigned int rows)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    id_stmtused[f]=-1;
    return hd_memtab[f]->Build(rows);
}

DllExport void _wdbiClear(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->Clear();
    return ;
}

DllExport void _wdbiAddrFresh(int memtab,char **colval,int *collen,int *coltp) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->AddrFresh(colval,collen,coltp);
    return ;
}

/*
//锁住语句对象
void CopyToDbTable(void *memtab) {
    void **ptr=(void **)memtab;
    int f=*((int *)ptr[0]);
    int t=*((int *)ptr[1]);
    char tablename[200];
    strcpy(tablename,(char *)ptr[2]);
    LockStmt(t);
    SetEvent(inprocess[t]);
    retval_memtab[f]=0;
    try{
        retval_memtab[f]=hd_memtab[f]->CopyToTab(tablename,hd_sess[t]); 
    }
    catch(WDBIError &e) {
      sb4 cd;
      char *str;
      char *fn;
      int l;
      e.GetErrPos(l,&fn);
      if(l!=-1)
       printf("Error occurs at file %s line %d.\n",fn,l);
      e.GetLastError(cd,&str);
      printf (" ErrorCode: %d.  %s\n",cd,str);
      retval_memtab[f]=-1;
    }
    catch(...) {
        //printf("A Error detected,program terminated!\n");
        retval_memtab[f]=-2;
    }
    UnlockStmt(t);
}
*/

DllExport bool _wdbiAppendToDbTableWithColName(int memtab,const char *tablename,int sess,bool autocommit) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(t==-1) return false;
    return hd_memtab[f]->CopyToTab(tablename,hd_sess[t],autocommit,true);
}

DllExport bool _wdbiAppendToDbTable(int memtab,const char *tablename,int sess,bool autocommit) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(t==-1) return false;
    return hd_memtab[f]->CopyToTab(tablename,hd_sess[t],autocommit,false);
    /*
    void *ptr[]={&f,&t,tablename};
    _beginthread(CopyToDbTable,0,(void *)ptr);
    WaitForSingleObject(inprocess[t],INFINITE);
    if(!asyncMode) {
        _wdbiWaitMemtable(memtab);
        return retval_memtab[f];
    }
    return true;
    */
}

thread_rt FetchAll(void *memtab) {
    int *ptr=(int *)memtab;
    int f=ptr[0];
    int st=ptr[1];
    delete [](int *)ptr;
    if(asyncMode) {
#ifndef __unix
    LockMemtable(f,INFINITE);
    SetEvent(inprocess[id_stmtused[f]]);
//#else
//  UnlockMemtable(f);
#endif
    }
    try{
        hd_stmt[id_stmtused[f]]->Execute(0,0);
        retval_memtab[f]=(int)hd_memtab[f]->FetchAll(st);
    }
    catch(WDBIError &e) {
      retval_memtab[f]=-1;
    }
    //catch(...) {
        //printf("A Error detected,program terminated!\n");
    //  retval_memtab[f]=-2;
    //}
    if(asyncMode) {
        UnlockMemtable(f);
    }
    thread_end;
}

DllExport unsigned int _wdbiFetchAll(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    int *ptr=new int[2];
    ptr[0]=f;
    ptr[1]=0;
    if(mutex_stmt_busy[id_stmtused[f]]) ThrowInvalidHandle(INT_STMT_BUSY);

    if(asyncMode) {
#ifdef __unix
    LockMemtable(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,FetchAll,(void *)ptr);
    pthread_detach(hd_pthread_t[f]);
    //LockMemtable(f,INFINITE);
#else
            // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)

    _beginthread(FetchAll,0,(void *)ptr);
    WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
    }
    else {
        FetchAll(ptr);
    }
    
    if(!asyncMode) {
        return _wdbiWaitLastReturn(memtab);
    }
    
    return 0;
}

DllExport unsigned int _wdbiFetchAllAt(int memtab,int st) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    int *ptr=new int[2];
    ptr[0]=f;
    ptr[1]=st;
    if(f==-1) return 0;
    if(mutex_stmt_busy[id_stmtused[f]]) ThrowInvalidHandle(INT_STMT_BUSY);
    if(asyncMode) {
#ifdef __unix
    LockMemtable(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,FetchAll,(void *)ptr);
    pthread_detach(hd_pthread_t[f]);
    //LockMemtable(f,INFINITE);
#else
            // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)

    _beginthread(FetchAll,0,(void *)ptr);
    WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
    }
    else {
        FetchAll(ptr);
        return retval_memtab[f];
    }
    /*
    if(!asyncMode) {
        _wdbiWaitMemtable(memtab,INFINITE);
        return retval_memtab[f];
    }
    */
    return 0;
}


thread_rt FetchFirst(void *ptr) {
    int f=((int *)ptr)[0];
    int rows=((int *)ptr)[1];
    delete [] (int *)ptr;
    if(asyncMode) {
#ifndef __unix
    LockMemtable(f,INFINITE);
    SetEvent(inprocess[id_stmtused[f]]);
//#else
//  UnlockMemtable(f);
#endif
    }
    try {
        hd_stmt[id_stmtused[f]]->Execute(0,0);
        retval_memtab[f]=(int)hd_memtab[f]->FetchFirst(rows);
        }
    catch(WDBIError &e) {
      retval_memtab[f]=-1;
    }
    //catch(...) {
        //printf("A Error detected,program terminated!\n");
    //  retval_memtab[f]=-2;
    //}
    if(asyncMode) 
        UnlockMemtable(f);
    thread_end;
}

DllExport unsigned int _wdbiFetchFirst(int memtab,unsigned int rows) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    int *val=new int[2];
    val[0]=f;val[1]=rows;
    if(mutex_stmt_busy[id_stmtused[f]]) ThrowInvalidHandle(INT_STMT_BUSY);
    if(asyncMode) {
#ifdef __unix
    LockMemtable(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,FetchFirst,(void *)val);
    pthread_detach(hd_pthread_t[f]);
//  LockMemtable(f,INFINITE);
#else
            // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)

    _beginthread(FetchFirst,0,(void *)val);
    WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
    }
    else {
        FetchFirst(val);
    }
    
    if(!asyncMode) {
        return _wdbiWaitLastReturn(memtab);
    }
    return retval_memtab[f];
}

thread_rt FetchNext(void *ptr) {
    int f=((int *)ptr)[0];
    int rows=((int *)ptr)[1];
    int st=((int *)ptr)[2];
    delete [] (int *)ptr;
    if(asyncMode) {
#ifndef __unix
    LockMemtable(f,INFINITE);
    SetEvent(inprocess[id_stmtused[f]]);
//#else
//    UnlockMemtable(f);
#endif
    }
    try {
    retval_memtab[f]=(int)hd_memtab[f]->FetchNext(rows,st);
        }
    catch(WDBIError &e) {
      retval_memtab[f]=-1;
    }
    //catch(...) {
    //  retval_memtab[f]=-2;
    //}
    if(asyncMode) 
        UnlockMemtable(f);
    thread_end;
}

DllExport unsigned int _wdbiFetchNext(int memtab,unsigned int rows) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    int *val=new int[3];
    val[0]=f;val[1]=rows;val[2]=0;
    if(mutex_stmt_busy[id_stmtused[f]]) ThrowInvalidHandle(INT_STMT_BUSY);
    if(asyncMode) {
#ifdef __unix
    LockMemtable(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,FetchNext,(void *)val);
    pthread_detach(hd_pthread_t[f]);
//  LockMemtable(f,INFINITE);
#else
            // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)

    _beginthread(FetchNext,0,(void *)val);
    WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
    }
    else {
        FetchNext(val);
    }
    
    if(!asyncMode) {
        return _wdbiWaitLastReturn(memtab);
    }
    
    return retval_memtab[f];
}

DllExport unsigned int _wdbiFetchAt(int memtab,unsigned int rows,int st) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    int *val=new int[3];
    val[0]=f;val[1]=rows;val[2]=st;
    if(mutex_stmt_busy[id_stmtused[f]]) ThrowInvalidHandle(INT_STMT_BUSY);
/*
    if(asyncMode) {
#ifdef __unix
    LockMemtable(f,INFINITE);
    pthread_create(&hd_pthread_t[f],NULL,FetchNext,(void *)val);
    pthread_detach(hd_pthread_t[f]);
//  LockMemtable(f,INFINITE);
#else
    _beginthread(FetchNext,81920,(void *)val);
    WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
    }
    else {
*/      FetchNext(val);
/*  }
    
    if(!asyncMode) {
*/
        return _wdbiWaitLastReturn(memtab);
/*  }
    
    return retval_memtab[f];
*/
}

DllExport bool _wdbiGeneTable(int memtab,const char *tablename,int sess) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(t==-1) return false;
    return hd_memtab[f]->GeneTable(tablename,hd_sess[t]);
}


DllExport int _wdbiCopyVarStrColDataInTable(int memtab,int nCol,long nToRow,long nFromRow,long nCopyRowNum){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->CopyVarDataInTab(nCol,nToRow,nFromRow,nCopyRowNum);
}

DllExport int _wdbiGetStrAddrByName(int memtab,const char *col,unsigned int rowst,char **pstr,int *celllen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr,*celllen);
}


DllExport  const char *_wdbiGetVarLenStrAddrByName(int memtab,const char *col,unsigned int rowst,int *celllen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int ncol = hd_memtab[f]->GetColumnIndex(col);
    return hd_memtab[f]->GetAddr(ncol,rowst,*celllen);
}


DllExport int _wdbiGetDateAddrByName(int memtab,const char *col,unsigned int rowst,char **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetDoubleAddrByName(int memtab,const char *col,unsigned int rowst,double **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetIntAddrByName(int memtab,const char *col,unsigned int rowst,int **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetLongAddrByName(int memtab,const char *col,unsigned int rowst,LONG64 **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetStrValByName(int memtab,const char *col,unsigned int rowst,char *pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int len;
    return hd_memtab[f]->GetValues(col,rowst,1,pstr,len);
}

DllExport int _wdbiGetStrValByPos(int memtab,int col,unsigned int rowst,char *pstr,int rowNm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int len;
    return hd_memtab[f]->GetValues(col,rowst,rowNm,pstr,len);
}


DllExport int _wdbiGetDateValByName(int memtab,const char *col,unsigned int rowst,char *pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetValues(col,rowst,1,pstr);
}

DllExport double _wdbiGetDoubleValByName(int memtab,const char *col,unsigned int rowst){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    double val;
    hd_memtab[f]->GetValues(col,rowst,1,&val);
    return val;
}

DllExport int _wdbiGetIntValByName(int memtab,const char *col,unsigned int rowst) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int val;
    hd_memtab[f]->GetValues(col,rowst,1,&val);
    return val;
}

DllExport LONG64 _wdbiGetLongValByName(int memtab,const char *col,unsigned int rowst) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    LONG64 val;
    hd_memtab[f]->GetValues(col,rowst,1,&val);
    return val;
}

DllExport int _wdbiGetStrAddrByPos(int memtab,int col,unsigned int rowst,char **pstr,int *celllen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr,*celllen);
}

DllExport  const char*  _wdbiGetVarStrAddrByPos(int memtab,int col,unsigned int rowst,int *celllen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr((ub4)col,(ub4)rowst,*celllen);
}


DllExport int _wdbiGetDateAddrByPos(int memtab,int col,unsigned int rowst,char **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetDoubleAddrByPos(int memtab,int col,unsigned int rowst,double **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetIntAddrByPos(int memtab,int col,unsigned int rowst,int **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetLongAddrByPos(int memtab,int col,unsigned int rowst,LONG64 **pstr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int _wdbiGetBufferLen(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return (int)hd_memtab[f]->GetBfLen();
}

DllExport bool _wdbiGetCell(int memtab,unsigned int row,int col,char *str,bool rawpos) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->GetCell(row,col,str,rawpos);
}

DllExport int _wdbiSetColumnName(int memtab,int id,char *colname)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SetColumnName(id,colname);
}

DllExport int _wdbiGetColumnDisplayWidth(int memtab,int col) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnDispWidth(col);
}

DllExport int _wdbiGetColumnPosByName(int memtab,const char *colname)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnId(colname);
}

DllExport int _wdbiGetColumnName(int memtab,int id,char *colname)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnName(id,colname);
}

DllExport int _wdbiGetColumnDataLenByPos(int memtab,int colid)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnLen(colid);
}

DllExport int _wdbiGetColumnNumber(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnNum();
}

DllExport int _wdbiGetColumnScale(int memtab,int colid)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnScale(colid);
}

DllExport void _wdbiGetColumnTitle(int memtab,int colid,char *str,int len)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->GetColumnTitle(colid,str,len);
}

DllExport short _wdbiGetColumnType(int memtab,int colid)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnType(colid);
}

DllExport bool _wdbiGetLine(int memtab,unsigned int row,char *str,bool rawpos,const char *colnm,int *clen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->GetLine(str,row,rawpos,colnm,clen);
}

// 获取整行数据字符串，add by liujs
DllExport bool _wdbiGetLineStr(int memtab,unsigned int row,char *str,bool rawpos,const char *colnm,int *clen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE); 
    if(f==-1) return false; 

    char escape_cha = '\"';
    return (int)hd_memtab[f]->GetLineStr(str,row,rawpos,colnm,clen,escape_cha);
}


DllExport int _wdbiGetMemtableRows(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetRows();
}

DllExport void _wdbiReInOrder(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_memtab[f]->ReInOrder();
}

DllExport void _wdbiGetTitle(int memtab,char *str,int len,const char *colsnm,int *clen)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->GetTitle(str,len,colsnm,clen);
}

DllExport bool _wdbiIsIKSet(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->IsPKSet();
}

DllExport bool _wdbiOrderByIK(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->OrderByPK();
}

DllExport int _wdbiSearchIK(int memtab,int key)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int rn=hd_memtab[f]->SearchPK(key);
    if(rn>=0 && !hd_memtab[f]->IsQDelete(rn)) return rn;
    return -1;
}

DllExport int _wdbiSearchIKLE(int memtab,int key)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int rn=hd_memtab[f]->SearchPK(key,2);
    if(rn>=0) 
    {
        rn=hd_memtab[f]->GetRawrnByPK(rn);
        if(!hd_memtab[f]->IsQDelete(rn)) return rn;
    }
    return -1;
}

DllExport void _wdbiSetColumnDisplayName(int memtab,const char *colnm,char *str)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->SetColDspName(colnm,str);
    return ;
}

DllExport bool _wdbiSetIKByName(int memtab,const char *str) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SetPKID(str);
}

//DllExport bool _wdbiOrderByIK(int memtab,int colid) {
//  int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
//  if(f==-1) return false;
//  return (int)hd_memtab[f]->SetPKID(colid);
//}

DllExport bool _wdbiSetSortColumn(int memtab,const char *colsnm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SetSortColumn(colsnm);
}

DllExport int _wdbiSetStrValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const char *bf) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    int len=0;
    return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf,len);
}

DllExport int _wdbiSetDateValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const char *bf) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport int _wdbiSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const double *bf) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport int _wdbiSetIntValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const int *bf) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport int _wdbiSetLongValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const LONG64 *bf) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport bool _wdbiSort(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->Sort();
}

DllExport bool _wdbiSortHeap(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SortHeap();
}

/*  Group Functions ***************************************/

DllExport bool _wdbiSetGroupSrc(int memtab,int src) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(src-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return (int)hd_memtab[f]->SetGroupSrc(hd_memtab[t]);
}

DllExport bool _wdbiSetIKGroupRef(int memtab,int ref,const char *colnm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(ref-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return (int)hd_memtab[f]->SetGroupRef(hd_memtab[t],colnm);
}

DllExport bool _wdbiSetGroupSrcCol(int memtab,const char *colsnm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SetGroupColSrc(colsnm);
}


DllExport bool _wdbiSetGroupRefCol(int memtab,const char *colsnm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SetGroupColRef(colsnm);
}

DllExport bool _wdbiSetSrcSumCol(int memtab,const char *colsnm) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->SetCalCol(colsnm);
}

DllExport bool _wdbiGroup(int memtab,int rowstart,int rownum) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->Group(rowstart,rownum);
}

//////////////////////////////////////////////////////////////////
// ExcelEnv function . 
//
//
//////////////////////////////////////////////////////////////////
#if !(defined(__unix) || defined(NO_EXCEL))
DllExport int _wdbiCreateExcelEnv() {
    int t=GetFreeHandle((void **)hd_excelf,MAX_EXCELFILE);
    if(t==-1) ThrowInvalidHandle(INT_NOHANDLERES);
    WOCIExcelEnv *p=new WOCIExcelEnv();
    if(!p) ThrowInvalidHandle(INT_CREATEOBJ_FAIL);
    hd_excelf[t]=p;
    return t+HANDLE_EXCELF;
}
DllExport void _wdbiSetDir(int excel,const char *strTemplate,const char *strReport)
{
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return ;
    hd_excelf[f]->SetDir(strTemplate,strReport);
}

DllExport void _wdbiSetMemTable(int excel,int memtab)
{
    int t=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return ;
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return ;
    hd_excelf[f]->SetDataTable(hd_memtab[t]);
}

DllExport bool _wdbiLoadTemplate(int excel,const char *tempname) {
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return false;
    return (int)hd_excelf[f]->LoadTemp(tempname);
}

DllExport bool _wdbiSelectSheet(int excel,const char *sheetname) {
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return false;
    return (int)hd_excelf[f]->SelectSheet(sheetname);
}

DllExport bool _wdbiFillData(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol, unsigned int fromrow, unsigned int colnum, unsigned int rownum,bool rawpos) {
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return false;
    return (int)hd_excelf[f]->Fill(tocol,torow,fromcol,fromrow,colnum,rownum,rawpos);
}

DllExport bool _wdbiFillTitle(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol,unsigned int colnum) {
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return false;
    return (int)hd_excelf[f]->FillTitle(tocol,torow,fromcol,colnum);
}

DllExport bool _wdbiSaveAs(int excel,const char *filename) {
    int f=CheckExcelHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
    if(f==-1) return false;
    return (int)hd_excelf[f]->SaveAs(filename);
}
#endif
 
void ErrorCheck(const char *fn, int ln,int errcode)
{
    WDBIError err;
    err.SetErrPos(ln,fn);
    //err.retval=err.errcode=ercd;
    char buf[1000];
    switch(errcode) {
    case INT_INVALID_DBSTYPE:
        sprintf(buf,"Exception : Invalid database server type.");
        break;
    case INT_NOSUPPORT_DBSTYPE:
        sprintf(buf,"Exception : Database server type is not supported in this library ,try to use another lib..");
        break;
    case INT_INVALID_HANDLE :
        sprintf(buf,"Exception : Invalid handle.");
        break;
    case INT_ALLOCATE_FAIL   :
        sprintf(buf,"Exception : Allocate object fail.");
        break;
    case INT_NOHANDLERES        :
        sprintf(buf,"Exception : Handle resource is exhausted");
        break;
    case INT_CREATEOBJ_FAIL :
        sprintf(buf,"Exception : Create object fail.");
        break;
    case INT_CREATEMTMUTEX_FAIL :
        sprintf(buf,"Exception : Create memory table's mutex object fail.");
        break;
    case INT_CREATESTMUTEX_FAIL :
        sprintf(buf,"Exception : Create statment's mutex ojbect fail.");
        break;
    case INT_WAITMTMUTEX_FAIL   :
        sprintf(buf,"Exception : Wait for memory table mutex ojbect fail.");
        break;
    case INT_WAITSTMUTEX_FAIL   :
        sprintf(buf,"Exception : Wait for statment mutex ojbect fail.");
        break;
    case INT_STMT_BUSY  :
        sprintf(buf,"Exception : Execute(Query) or Delete on a busy statement.");
        break;
    case INT_USER_CANCEL:
        sprintf(buf,"Exception : User break detected.");
        break;
    default :
        sprintf(buf,"Exception : Errorcode no defined.");
        break;
    }
    err.SetErrCode(errcode);
    err.SetErrMsg(buf);
    if(!throwed) {
     throwed=true;
     err.Throw();
    }
}

DllExport int _wdbiGetMemUsed(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->GetMemUsed();
}

DllExport bool _wdbiSetSortedGroupRef(int memtab,int mtref, const char *colssrc)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(mtref-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return hd_memtab[f]->SetSortedGroupRef(hd_memtab[t],colssrc);
}

DllExport int _wdbiSearch(int memtab,void **ptr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    int rn=hd_memtab[f]->Search(ptr);
    if(rn>=0 && !hd_memtab[f]->IsQDelete(rn)) return rn;
    return -1; 
}

DllExport int _wdbiSearchNoQDel(int memtab,void **ptr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    int rn=hd_memtab[f]->Search(ptr);
    if(rn>=0) return rn;
    return -1; 
}



DllExport void _wdbiSetQDeleteFlagFalse(int memtab, int nRow)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return;
    hd_memtab[f]->SetQDeleteFlagFalse(nRow); 
}

DllExport int _wdbiSetVarLenLimit(int memtab,int nVarLen)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    int rn=hd_memtab[f]->SetVarLenLimit(nVarLen);
    return rn;
}

DllExport int _wdbiSearchLE(int memtab,void **ptr) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    int rn=hd_memtab[f]->Search(ptr,2);
    if(rn>=0) 
    {
        rn=hd_memtab[f]->GetRawrnBySort(rn);
        if(!hd_memtab[f]->IsQDelete(rn)) return rn;
    }
    return -1; 
}

DllExport bool _wdbiValuesSet(int memtab,int mtsrc,const char *colto,const char *colfrom,bool usedestKey,int op)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(mtsrc-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return hd_memtab[f]->ValuesSetTo(colto,colfrom,hd_memtab[t],usedestKey,op);
}

DllExport bool _wdbiDeleteRow(int memtab,int rown) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->DeleteRow(rown);
}

DllExport int _wdbiGetCreateTableSQL(int memtab,char *buf,const char *tabname,bool ismysql) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->GetCreateTableSQL(buf,tabname,ismysql);
}


DllExport int _wdbiGetCreateTableColumnCommentSQL(int memtab,char *crttab,const char *tabname,const void* stColumComment,const int columnsNum,bool ismysql) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->GetCreateTableColumnCommentSQL(crttab,tabname,stColumComment,columnsNum,ismysql);
}

DllExport bool _wdbiInsertRows(int memtab,void **ptr,const char *colsname,int num)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->InsertRows(ptr,colsname,num);
}

DllExport bool _wdbiBindToStatment(int memtab,int stmt,const char *colsname,int rowst)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(t==-1) return false;
    return hd_memtab[f]->BindToStatment(hd_stmt[t],colsname,rowst);
}


DllExport int _wdbiTestTable(int sess,const char *tablename) {
    int t=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(t==-1) return false;
    return hd_sess[t]->TestTable(tablename);
}

DllExport int _wdbiGetRawrnBySort(int memtab,int ind) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->GetRawrnBySort(ind);
}

DllExport int _wdbiGetRawrnByIK(int memtab,int ind) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->GetRawrnByPK(ind);
}

DllExport bool _wdbiCompressBf(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->CompressBf();
}

DllExport bool _wdbiCopyRowsTo(int memtab,int memtabto,int toStart,int start,int rowsnum) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(memtabto-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return hd_memtab[f]->CopyRowsTo(hd_memtab[t],toStart,start,rowsnum);
}

DllExport bool _wdbiCopyRowsToNoCut(int memtab,int memtabto,int toStart,int start,int rowsnum) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(memtabto-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return hd_memtab[f]->CopyRowsTo(hd_memtab[t],toStart,start,rowsnum,false);
}

DllExport bool _wdbiCopyColumnDefine(int memtab,int memtabfrom,const char *colsname) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckMtHd(memtabfrom-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(t==-1) return false;
    return hd_memtab[f]->CopyColumnDefine(colsname,hd_memtab[t]);
}

DllExport bool _wdbiGetColumnDesc(int memtab,void **pColDesc,int &cdlen,int &_colnm) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->GetColumnDesc(pColDesc,cdlen,_colnm);
}

DllExport long _wdbiExportSomeRows(int memtab,char *pData,int startrn,int rnnum)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->ExportSomeRows(pData,startrn,rnnum);
}

DllExport int _wdbiExportQDeleteFlag(int memtab,char *pData ,int &nDeleteRowct)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->ExportQDeleteFlag(pData,nDeleteRowct);
}

DllExport void _wdbiImportQDeleteFlag(int memtab,char *pData , int nDeleteRowct)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return;
    return hd_memtab[f]->ImportQDeleteFlag(pData,nDeleteRowct);
}


DllExport bool _wdbiExport(int memtab,void *pData,long &_len,int &_maxrows,int &_rowct) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->Export(pData,_len,_maxrows,_rowct);
}

DllExport bool _wdbiImport(int memtab,void *pData,long _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->Import(pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct);
}

DllExport long _wdbiAppendRows(int memtab,char *pData,int rnnum)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->AppendRows(pData,rnnum);
}
    //保留表结构，清除索引结构，数据行数置零
DllExport void _wdbiReset(int memtab) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->Reset();
}

DllExport void _wdbiClearIK(int memtab) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->clearIK();
}

DllExport void _wdbiClearSort(int memtab) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->clearSort();
}

DllExport bool _wdbiCompact(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->CompactBuf();
}

DllExport void _wdbiSetMTName(int memtab,const char *mtname)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
        hd_memtab[f]->SetName(mtname);
}

DllExport void _wdbiSetMTRows(int memtab,int rows)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    hd_memtab[f]->SetRows(rows);
}
DllExport bool _wdbiIsNull(int mt,int col,unsigned rown){
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    return hd_memtab[f]->IsNull(col,rown);
}

DllExport void _wdbiSetNull(int mt,int col,unsigned rown,bool setnull) {
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    hd_memtab[f]->SetNull(col,rown,setnull);
}


DllExport bool _wdbiExportSomeRowsWithNF(int mt,char *pData,int startrn,int rnnum){
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    return hd_memtab[f]->ExportSomeRowsWithNF(pData,startrn,rnnum);
}

DllExport bool _wdbiAppendRowsWithNF(int mt,char *pData,int rnnum){
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    return hd_memtab[f]->AppendRowsWithNF(pData,rnnum);
}

DllExport bool _wdbiIsQDelete(int memtab,int rowrn) 
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->IsQDelete(rowrn);
}

DllExport int _wdbiQDeletedRows(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->QDeletedRows();
}

DllExport bool _wdbiQDeleteRow(int memtab,int rownm)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->QDeleteRow(rownm);
}

DllExport int _wdbiGetRowLen(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->GetRowLen();
}

int LockStmt(int stmt,int tm)
{
    int f=CheckStmtHd(stmt,MAX_STATMENT);
    if(f==-1) return -1;
#ifdef __unix
    int rt=pthread_mutex_lock(&hd_mutex_stmt[f]);
#else
    //if(mutex_stmt_busy[f]) ThrowInvalidHandle(INT_STMT_BUSY);

    DWORD rt=WaitForSingleObject(hd_mutex_stmt[f],tm);
#endif
        if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) 
            ThrowInvalidHandle(INT_WAITSTMUTEX_FAIL);
    //}
    if(rt==WAIT_OBJECT_0)
    mutex_stmt_busy[f]=true;
    return rt;
}

int LockMemtable(int memtab,int tm)
{
    int f=memtab;//CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
#ifdef __unix
    int rt=pthread_mutex_lock(&hd_mutex_stmt[id_stmtused[f]]);
#else
    //if(mutex_stmt_busy[id_stmtused[f]]) {
    DWORD rt=WaitForSingleObject(hd_mutex_stmt[id_stmtused[f]],tm);
#endif
        if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
#ifndef __unix
            rt=GetLastError();
#endif
            ThrowInvalidHandle(INT_WAITMTMUTEX_FAIL);
        }
    //}
    if(rt==WAIT_OBJECT_0)
    mutex_stmt_busy[id_stmtused[f]]=true;
    return rt;
}

void UnlockStmt(int stmt)
{
    int f=stmt;//CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return ;
#ifdef __unix
    pthread_mutex_unlock(&hd_mutex_stmt[f]);
#else
    ReleaseMutex(hd_mutex_stmt[f]);
#endif
    mutex_stmt_busy[f]=false;
}

void UnlockMemtable(int memtab)
{
    int f=memtab;//CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
#ifdef __unix
    pthread_mutex_unlock(&hd_mutex_stmt[id_stmtused[f]]);
#else
    ReleaseMutex(hd_mutex_stmt[id_stmtused[f]]);
#endif
    mutex_stmt_busy[id_stmtused[f]]=false;
}

DllExport int _wdbiWaitStmt(int stmt,int tm)
{
    int f=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return -1;
    int rt;
    if( (rt=LockStmt(f,tm))==WAIT_OBJECT_0)
     UnlockStmt(f);
    return rt;
    //while(mutex_stmt_busy[f]) Sleep(500);
}

DllExport int _wdbiWaitMemtable(int memtab,int tm)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    int rt;
    if ( (rt=LockMemtable(f,tm))==WAIT_OBJECT_0)
    UnlockMemtable(f);
    return rt;
    //while(mutex_stmt_busy[id_stmtused[f]]) Sleep(500);
}

extern bool wdbi_kill_in_progress;

DllExport int _wdbiWaitLastReturn(int handle) {
    int f;
    if(wdbi_kill_in_progress) 
        ThrowInvalidHandle(INT_USER_CANCEL);
    if(handle>=HANDLE_MEMTAB) {
        f=CheckMtHd(handle-HANDLE_MEMTAB,MAX_MEMTABLE);
        if(f==-1) return -1;
        _wdbiWaitMemtable(handle,INFINITE);
        if(retval_memtab[f]<0) {
//          #ifdef WIN32
//              AfxDumpStack();
//              #endif
            throwed=true;
            throw *(WDBIError *)hd_memtab[f];
        }
        return retval_memtab[f];
    }
    f=CheckStmtHd(handle-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return -1;
    _wdbiWaitStmt(handle,INFINITE);
    if(retval_stmt[f]<0) {
//        #ifdef WIN32
//        AfxDumpStack();
//        #endif
          throwed=true;
          throw *(WDBIError *)hd_stmt[f];
    }
    return retval_stmt[f];
}

DllExport int _wdbiWaitTime(int handle,int time) {
    int f;
    if(handle>=HANDLE_MEMTAB) {
        f=CheckMtHd(handle-HANDLE_MEMTAB,MAX_MEMTABLE);
        if(f==-1) return -1;
        int ret=_wdbiWaitMemtable(handle,time);
        return ret;
    }
    f=CheckStmtHd(handle-HANDLE_STMT,MAX_STATMENT);
    if(f==-1) return -1;
    int ret=_wdbiWaitStmt(handle,time);
    return ret;
}

DllExport bool _wdbiFreshRowNum(int memtab) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->FreshRowNum();
}

thread_rt BatchSelect(void *memtab) {
    int *ptr=(int *)memtab;
    int p=ptr[0];
    int r=ptr[1];
    int t=id_stmtused[r];
    char colsnm[500];
    strcpy(colsnm,(char *)(ptr+2));
    delete [] (int *)ptr;
    if(asyncMode) {
#ifndef __unix
    LockMemtable(r,INFINITE);
    LockMemtable(p,INFINITE);
    SetEvent(inprocess[id_stmtused[r]]);
//#else
//  UnlockMemtable(r);
#endif
    }
    retval_memtab[r]=0;
    try{
    int off=0;
    int maxr=hd_memtab[r]->GetMaxRows();
    hd_stmt[t]->SetEcho(false);
    int i;
    for(i=0;i<hd_memtab[p]->GetRows();i++){
        hd_memtab[p]->BindToStatment(hd_stmt[t],colsnm,i);
        hd_stmt[t]->Execute(maxr,off);
        //hd_stmt[t]->Execute(0,0);//off);
        //hd_memtab[r]->FetchAll(off);
        off=hd_stmt[t]->GetRows();
    }
    hd_memtab[r]->FreshRowNum();
    retval_memtab[r]=hd_memtab[r]->GetRows();
    hd_stmt[t]->SetEcho(true);
    lgprintf("Execute %d times .\n%d rows fetched.\n",
        i,
        hd_memtab[r]->GetRows());
    }
    catch(WDBIError &e) {
      retval_memtab[r]=-1;
    }
    //catch(...) {
    //  retval_memtab[r]=-2;
    //}
    if(asyncMode) {
        UnlockMemtable(p);
        UnlockMemtable(r);
    }
    thread_end;
}

DllExport int _wdbiBatchSelect(int result,int param,const char *colsnm)
{
    int p=CheckMtHd(param-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(p==-1) return false;
    int r=CheckMtHd(result-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(r==-1) return false;
    int t=id_stmtused[r];
    if(t==-1) return false;
    int *ptr=new int[2+strlen(colsnm)*sizeof(char)/sizeof(int)+1];
    ptr[0]=p;ptr[1]=r;strcpy((char *)(ptr+2),colsnm);
    //void *ptr[]={&p,&r,colsnm,NULL};
    if(mutex_stmt_busy[id_stmtused[r]]) ThrowInvalidHandle(INT_STMT_BUSY);
    if(asyncMode) {
#ifdef __unix
    LockMemtable(r,INFINITE);
    LockMemtable(p,INFINITE);
    pthread_create(&hd_pthread_t[r],NULL,BatchSelect,(void *)ptr);
    pthread_detach(hd_pthread_t[r]);
//  LockMemtable(r,INFINITE);
#else
            // 2008/04/06 , stack_size 81920 may be cause stack overflow ,so change to 0 to use default as 
        //  main thread (default is 1M as vs2005msdn says)

    _beginthread(BatchSelect,0,(void *)ptr);
    WaitForSingleObject(inprocess[id_stmtused[r]],INFINITE);
#endif
    }
    else {
        BatchSelect(ptr);
    }
    
    if(!asyncMode) {
        return _wdbiWaitLastReturn(result);
    }
    
    return retval_memtab[r];
}   

DllExport bool _wdbiBatchNonSelect(int stmt,int param,const char *colsnm)
{
    int f=CheckMtHd(param-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(t==-1) return false;
    int i;
    if(mutex_stmt_busy[t]) ThrowInvalidHandle(INT_STMT_BUSY);
    for(i=0;i<hd_memtab[f]->GetRows();i++){
        hd_memtab[f]->BindToStatment(hd_stmt[t],colsnm,i);
        hd_stmt[t]->Execute(1,0);
    }
    return true;
}   

DllExport void _wdbiGetMTName(int memtab,char *bf)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->GetMTName(bf);
}

DllExport int _wdbiGetMaxRows(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    return hd_memtab[f]->GetMaxRows();
}


DllExport int _wdbiGetVarCol(int memtab,int *pCol,int *pTypeLen)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    return hd_memtab[f]->GetVarColAndVarLen(pCol,pTypeLen);

}

DllExport int _wdbiReadFromTextFile(int memtab,const char *fn,int rowst,int rownm)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->ReadFromTextFile(fn,rowst,rownm);
}

#define MAXTEXTLEN 50000
DllExport void _wdbiMTToTextFile(int memtab,const char *fn,int rownm,const char *colsnm) {
    int rows=_wdbiGetMemtableRows(memtab);
    char mess[MAXTEXTLEN];    
    //取列标题
    if(rownm==0 || rownm>rows) rownm=rows;
    FILE *fp=fopen(fn,"a+t");
    if(fp==NULL) {
        throwed=true;
        throw;
    }
    _wdbiGetMTName(memtab,mess);
    fprintf(fp,"\n\n%s\n",mess);
    _wdbiGetTitle(memtab,mess,MAXTEXTLEN,colsnm,NULL);
    fprintf(fp,mess);fprintf(fp,"\n");
    int i;
    for(i=0;i<rownm;i++) {
      //取单行数据的文本
      _wdbiGetLine(memtab,i,mess,false,colsnm,NULL);
      fprintf(fp,mess);
      fprintf(fp,"\n");
    }
    fclose(fp);
}

//  导出文本文件*.cvs,add by liujsDllExport 
void _wdbiMTToTextFileStr(int memtab,const char *fn,int rownm,const char *colsnm,const bool title){ 
    int rows=_wdbiGetMemtableRows(memtab);  
    char mess[MAXTEXTLEN];  //取列标题  
    if(rownm==0 || rownm>rows) 
        rownm=rows; 

    FILE *fp=fopen(fn,"a+t");   
    if(fp==NULL) {      
        throwed=true;       
        printf("open file %s error \n",fn);// add by liujs      
        throw;  
    }

    if(title){
        memset(mess,0,MAXTEXTLEN*sizeof(char));
        char cn[100];
        memset(cn,0,100);
        int colnum = wociGetColumnNumber(memtab);
        for(int c=0;c<colnum;c++){
            wociGetColumnName(memtab,c,cn);
            sprintf(mess+strlen(mess),"%s",cn);
            if(c != colnum-1){
                strcat(mess,",");
            }
        }
        fprintf(fp,mess);fprintf(fp,"\n");
    }

    for(int i=0;i<rownm;i++) {       
        //取单行数据的文本字符串   
        mess[0] =0;     
        _wdbiGetLineStr(memtab,i,mess,false,colsnm,NULL);     
        fprintf(fp,mess);       
        fprintf(fp,"\n");   
    }   
    fclose(fp);
    fp = NULL;
}


int myprintf(const char *format,...);
DllExport void _wdbiMTPrint(int memtab,int rownm,const char *colsnm,bool compact) {
    //取总行数
    int clen[200];
    int rows=_wdbiGetMemtableRows(memtab);
    char mess[MAXTEXTLEN];
    //取列标题
    if(rownm==0 || rownm>rows) rownm=rows;
    if(compact) _wdbiGetCompactLen(memtab,0,rows,colsnm,clen);
    _wdbiGetTitle(memtab,mess,MAXTEXTLEN,colsnm,compact?clen:NULL);
    myprintf(mess);myprintf("\n");
    int l=strlen(mess);
    memset(mess,'_',l);
    mess[l]=0;
    myprintf(mess);myprintf("\n");
    int i;
  
    for(i=0;i<rownm;i++) {
      //取单行数据的文本
      _wdbiGetLine(memtab,i,mess,false,colsnm,compact?clen:NULL);
      myprintf(mess);
      myprintf("\n");
    }
}

DllExport bool _wdbiGetCompactLen(int memtab,unsigned int rowstart,unsigned int rownum,const char *colnm,int *clen) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return (int)hd_memtab[f]->GetCompactLen(rowstart,rownum,colnm,clen);
}

DllExport void _wdbiSetColumnDisplay(int memtab,const char *cols,int colid,const char *dspname,int prec,int scale) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return ;
    hd_memtab[f]->SetColumnDisplay(cols,colid,dspname,prec,scale);
}

DllExport void _wdbiSetEcho(bool val) {
    WDBIError::SetEcho(val);
/*
    if(handle<HANDLE_SESS)  ThrowInvalidHandle(INT_INVALID_HANDLE);//ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(handle<HANDLE_STMT) {
        handle-=HANDLE_SESS;
        if(handle>MAX_SESSION || !hd_sess[handle]) ThrowInvalidHandle(INT_INVALID_HANDLE);
        hd_sess[handle]->SetEcho(val);
    } else if(handle<HANDLE_MEMTAB) {
        handle-=HANDLE_STMT;
        if(handle>MAX_STATMENT) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(!hd_stmt[handle]) ThrowInvalidHandle(INT_INVALID_HANDLE);
        hd_stmt[handle]->SetEcho(val);
    }   
#if defined(__unix) || defined(NO_EXCEL)
    else {
#else
    else if(handle<HANDLE_EXCELF) {
#endif
        handle-=HANDLE_MEMTAB;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(!hd_memtab[handle]) ThrowInvalidHandle(INT_INVALID_HANDLE);
        hd_memtab[handle]->SetEcho(val);
    } 
#if !(defined(__unix) || defined(NO_EXCEL))
    else {
        handle-=HANDLE_EXCELF;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_excelf[handle]) ThrowInvalidHandle(INT_INVALID_HANDLE);
        hd_excelf[handle]->SetEcho(val);
    }
#endif
*/
}

////////////////////////////////////////
//Backup SortAVL Data
//////////////////////////////////////
DllExport int _wdbiGetSortAVLLen(int memtab)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    return hd_memtab[f]->GetSortAVLLen();
}

//返回的是root的位置
DllExport int _wdbiBackupSortAVL(int memtab,char *pBuff,int nLen)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    return hd_memtab[f]->BackupSortAVL(pBuff,nLen);
}

DllExport bool _wdbiSetAVLSort(int memtab,char *pBuff,int nNodeNum,int nRootPos)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return 0;
    return hd_memtab[f]->SetAVLSort(pBuff,nNodeNum,nRootPos);
}

//////////////////////////////////////


DllExport int _wdbiDateDiff(const char *d1,const char *d2) {
    WOCIDate dt1(d1),dt2(d2);
    return int(difftime(dt1.GetTime_T(),dt2.GetTime_T()));
}

DllExport double _wdbiCalculate(int memtab,const char *colnm,int op) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->Calculate(colnm,op);
}

DllExport bool _wdbiReplaceStmt(int memtab,int stmt)
{
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(t==-1) return false;
    id_stmtused[f]=t;
    return hd_memtab[f]->ReplaceStmt(hd_stmt[t])!=0;
}

DllExport int _wdbiConvertColStrToInt(int memtab,const char *colsn,int *pcol) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->ConvertColStrToInt(colsn,pcol);
}

DllExport int _wdbiCompareSortRow(int memtab,unsigned int row1, unsigned int row2) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return hd_memtab[f]->CompareSortRow(row1,row2);
}

DllExport int _wdbiSaveSort(int memtab,FILE *fp){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->SaveSort(fp);
}

DllExport bool _wdbiIsFixedMySQLBlock(int memtab){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->IsFixedMySQLBlock();
}

DllExport void _wdbiCopyToMySQL(int memtab,unsigned int startrow,
            unsigned int rownum,FILE *fp){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_memtab[f]->CopyToMySQL(startrow,rownum,fp);
}

DllExport void _wdbiCopyToIBBinary(int memtab,unsigned int startrow,
            unsigned int rownum,FILE *fp){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_memtab[f]->CopyToIBBinary(startrow,rownum,fp);
}


DllExport int _wdbiCopyToIBBuffer(int memtab,unsigned int startrow,
            char *buf,int maxbuflen,int *actual_len){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->CopyToIBBuffer(startrow,buf,maxbuflen,actual_len);
}

DllExport void _wdbiReverseByteOrder(int mt)
{
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_memtab[f]->ReverseByteOrder();
}

DllExport void _wdbiReverseCD(int mt)
{
    int f=CheckMtHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    hd_memtab[f]->ReverseCD();
}

DllExport int _wdbiLoadSort(int memtab,FILE *fp){
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->LoadSort(fp);
}

DllExport int _wdbiGetColumnInfo(int memtab,char *crttab,bool ismysql) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
    return hd_memtab[f]->GetColumnInfo(crttab,ismysql);
}


DllExport BOOL _wdbiCheckData(int memtab, int nCol) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    return  hd_memtab[f]->CheckData(nCol);
}


DllExport int _wdbiGetLastError(int handle){
    if(handle<HANDLE_SESS)  ThrowInvalidHandle(INT_INVALID_HANDLE);//ThrowInvalidHandle(INT_INVALID_HANDLE);
    if(handle<HANDLE_STMT) {
        handle-=HANDLE_SESS;
        if(handle>MAX_SESSION) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_sess[handle]) {
            if(hd_sess[handle]!=(void *)-1)
                return hd_sess[handle]->GetErrCode();
        }
        else 
            ThrowInvalidHandle(INT_INVALID_HANDLE);
    } else if(handle<HANDLE_MEMTAB) {
        handle-=HANDLE_STMT;
        if(handle>MAX_STATMENT) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_stmt[handle]) {
            if(hd_stmt[handle]!=(void *)-1) 
                return hd_stmt[handle]->GetErrCode();
        }
        else 
            ThrowInvalidHandle(INT_INVALID_HANDLE);
    }   
#if defined(__unix) || defined(NO_EXCEL)
    else {
#else
    else if(handle<HANDLE_EXCELF) {
#endif
        handle-=HANDLE_MEMTAB;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_memtab[handle]) {
            if(hd_memtab[handle]!=(void *)-1)
                return hd_memtab[handle]->GetErrCode();
        }
        else ThrowInvalidHandle(INT_INVALID_HANDLE);
    } 
#if !defined(__unix) &&  !defined(NO_EXCEL)
    else {
        handle-=HANDLE_EXCELF;
        if(handle>MAX_MEMTABLE) ThrowInvalidHandle(INT_INVALID_HANDLE);
        if(hd_excelf[handle]) {
            if(hd_excelf[handle]!=(void *)-1)
                return hd_excelf[handle]->GetErrCode();
        }
        else ThrowInvalidHandle(INT_INVALID_HANDLE);
    }
#endif
    return 0;
}

DllExport void _wdbiList(char *pout){
    int usect=0,newct=0,busyct=0;
    int i;
    for(i=0;i<MAX_STATMENT;i++) {
        if(hd_stmt[i]) {
            if(hd_stmt[i]==(WDBIStatement *)-1) {
                newct++;
            }
            usect++;
            if(mutex_stmt_busy[i]) busyct++;
        }
    }
    sprintf(pout,"语句-------- 最大值:%d,当前使用:%d,正在执行:%d,新语句:%d.\n",MAX_STATMENT,usect,busyct,newct);
    usect=0;newct=0;
    for(i=0;i<MAX_SESSION;i++) {
        if(hd_sess[i]) {
            if(hd_sess[i]==(WDBISession *)-1) {
                newct++;
            }
            usect++;
        }
    }
    sprintf(pout,"%s数据库连接-- 最大值:%d,当前使用:%d,新连接:%d.\n",pout,MAX_SESSION,usect,newct);
    newct=0;usect=0;
    for(i=0;i<MAX_MEMTABLE;i++) {
        if(hd_memtab[i]) {
            if(hd_memtab[i]==(MemTable *)-1) {
                newct++;
            }
            //_wdbiGetColumnInfo(i+HANDLE_MEMTAB,pout+strlen(pout),false);
            //strcat(pout,"\n");
            usect++;
        }
    }
    sprintf(pout,"%s内存表------ 最大值:%d,当前使用:%d,新表:%d.\n",pout,MAX_MEMTABLE,usect,newct);
#if !(defined(__unix) || defined(NO_EXCEL))
    usect=0;newct=0;
    for(i=0;i<MAX_EXCELFILE;i++){
        if(hd_excelf[i]) {
            if(hd_excelf[i]==(WOCIExcelEnv *)-1) {
                newct++;
            }
            usect++;
        }
    }
    sprintf(pout,"%sExcel------- 最大值:%d,当前使用:%d,新表:%d.\n",pout,MAX_EXCELFILE,usect,newct);
#endif

}


DllExport bool _wdbiDeleteToDbTableWithPK(int memtab,const char *tablename,int sess,bool autocommit) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckSessHd(sess-HANDLE_SESS,MAX_SESSION);
    if(t==-1) return false;
    return  hd_memtab[f]->DeleteToTabWithPK(tablename,hd_sess[t],autocommit);
}

DllExport bool _wdbiSetBindNullFlagForOutMem(int memtab,const int stmt,int pos,int colid,int rowst) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return false;
    int t=CheckStmtHd(stmt-HANDLE_STMT,MAX_STATMENT);
    if(t==-1) return false;
    return  hd_memtab[f]->SetBindNullFlagForOutMem(hd_stmt[t],pos,colid,rowst);
}

DllExport int _wdbiSearchSortedRows(int memtab,void **ptr,int &nStartSortRowNum,int &nEndSortRowNum ,int nReallSortCol) {
    int f=CheckMtHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
    if(f==-1) return -1;
    return hd_memtab[f]->SearchSortedRows(ptr,nStartSortRowNum,nEndSortRowNum,nReallSortCol);
}

extern char errfile[];
extern char lgfile[];
DllExport void _wdbilogwhere(const char *fn,int ln) {
    if(!throwed) 
     WDBIError::logwhere(ln,fn);
}

#ifndef WDBI_VERSION
#define WDBI_VERSION "dma 1.6.0"
#endif
DllExport void _wdbiPrintfVersion(){
    lgprintf("WDBI-[%s]",WDBI_VERSION); 
}


DllExport int _wdbiGetMaxSession()
{
    return MAX_SESSION;
}
DllExport int _wdbiGetMaxStatment()
{
    return MAX_STATMENT;
}
DllExport int _wdbiGetMaxMemTable()
{
    return MAX_MEMTABLE;
}

DllExport void _wdbiSetDefaultPrec(int prec,int scale) {
    WDBIStatement::SetDefaultPrec(prec,scale);
}
