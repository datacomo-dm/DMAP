#ifndef __unix
#include "stdafx.h"
#endif
#include "interface.h"
//#include "woci8intface.h"
#ifndef __unix
#include <comdef.h>
#include <conio.h>
//#if !defined(__unix) //&& !defined(NO_EXCEL)
#include "WOCIExcelEnv1.h"
//#endif
#include <process.h>
#else
#define getch getchar
#include <stdio.h>
#include <pthread.h>
#include <strings.h>
#include "woci8.h"
#endif
#define INT_INVALID_HANDLE		-1
#define INT_ALLOCATE_FAIL		-2
#define INT_NOHANDLERES			-3
#define INT_CREATEOBJ_FAIL		-4
#define INT_CREATEMTMUTEX_FAIL	-5
#define INT_CREATESTMUTEX_FAIL	-6
#define INT_WAITMTMUTEX_FAIL	-7
#define INT_WAITSTMUTEX_FAIL	-8

#define MAX_SESSION 220
#define MAX_STATMENT 520
#define MAX_MEMTABLE 520
#define MAX_EXCELFILE 30

#define HANDLE_SESS			0x2000
#define HANDLE_STMT			0x4000
#define HANDLE_MEMTAB		0x6000
#define HANDLE_EXCELF		0x8000

static WOCISession * hd_sess[MAX_SESSION];
static WOCIStatment * hd_stmt[MAX_STATMENT];
static MemTable * hd_memtab[MAX_MEMTABLE];
static int id_stmtused[MAX_MEMTABLE];
#ifdef __unix
#define INFINITE -1
#define WAIT_OBJECT_0 0
#define WAIT_TIMEOUT -1
static pthread_t hd_pthread_t[MAX_MEMTABLE];
static pthread_mutex_t hd_mutex_stmt[MAX_MEMTABLE];
static pthread_mutex_t hd_mutex_getfree;//PTHREAD_MUTEX_INITIALIZER;
#else
static HANDLE hd_mutex_stmt[MAX_MEMTABLE];
static HANDLE hd_mutex_getfree=NULL;
#if !(defined(__unix) || defined(NO_EXCEL))
static WOCIExcelEnv * hd_excelf[MAX_EXCELFILE];
#endif
#endif
static int retval_stmt[MAX_STATMENT];
static bool mutex_stmt_busy[MAX_STATMENT];
#ifndef __unix
static HANDLE inprocess[MAX_STATMENT];
#endif
//static HANDLE hd_mutex_memtab[MAX_MEMTABLE];
static int retval_memtab[MAX_MEMTABLE];
//static bool mutex_memtab_busy[MAX_STATMENT];
void ErrorCheck(char *fn, int ln,int errcode);
#define ThrowInvalidHandle(code) {ErrorCheck(__FILE__,__LINE__,code);}

bool asyncMode=false;
int LockStmt(int stmt,int tm);
int LockMemtable(int memtab,int tm);
void UnlockStmt(int stmt);
void UnlockMemtable(int memtab);
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif
//////////////////////////////////////////////////////////////////
// Common function . 
//
//
//////////////////////////////////////////////////////////////////
static bool wociinitialized=false;
DllExport void WOCIInit(char *appname) {
	if(wociinitialized) return;
	
	if(appname) {
		char fn[200];
		strcpy(fn,appname);
		strcat(fn,".log");
		wociSetLogFile(fn);
		strcpy(fn,appname);
		strcat(fn,".err");
		wociSetErrorFile(fn);
	}
	else {
		wociSetLogFile("woci8err.log");
		wociSetErrorFile("woci8info.log");
	}
	WOCIStartup();
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
	wociinitialized=true;
}

void BreakAll() {
	for(int i=0;i<MAX_SESSION;i++) {
		if(hd_sess[i]) {
			hd_sess[i]->SetTerminate(true);
		}
	}
}

DllExport void wociBreakAll() {
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
		if(WOCIError::IsEcho()) {
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
						wociMTToTextFile(HANDLE_MEMTAB+i,fn,0,NULL);
					}
				}
			}
		}
		}
	}
}

DllExport void WOCIQuit() {
	if(!wociinitialized) return;
	wociDestroyAll();
	WOCIShutdown();
	wociinitialized=false;
}

DllExport int wociDestroyAll() 
	{
		int i;
		for(i=0;i<MAX_STATMENT;i++) {
		if(hd_stmt[i]) {
			if(hd_stmt[i]==(WOCIStatment *)-1) {
				hd_stmt[i]=NULL;
				continue;
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
			if(hd_sess[i]==(WOCISession *)-1) {
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
//			ReleaseMutex(hd_mutex_memtab[i]);
//			CloseHandle(hd_mutex_memtab[i]);
		}
	}
#if !(defined(__unix) || defined(NO_EXCEL))
	for(i=0;i<MAX_EXCELFILE;i++){
		if(hd_excelf[i]) {
			if(hd_excelf[i]==(WOCIExcelEnv *)-1) {
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

int delstmtct=0;
DllExport int wocidestroy(int handle) {
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
				throw;
			}
			hd_sess[handle]=NULL;
		}
		//else 
		//	ThrowInvalidHandle(INT_INVALID_HANDLE);
	} else if(handle<HANDLE_MEMTAB) {
		delstmtct++;
		handle-=HANDLE_STMT;
		if(handle>MAX_STATMENT) ThrowInvalidHandle(INT_INVALID_HANDLE);
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
				throw;
			}
			}
			hd_stmt[handle]=NULL;
		}
		//else 
		//	ThrowInvalidHandle(INT_INVALID_HANDLE);
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
				throw;
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

inline int CheckHd(int p,int len) {
	if(p<0 || p>=len) 
		ThrowInvalidHandle(INT_INVALID_HANDLE);
	return p;
}

//////////////////////////////////////////////////////////////////
// Session function . 
//
//
//////////////////////////////////////////////////////////////////


DllExport int wociCreateSession(const char *username,const char *password,const char *svcname) {
	int f=GetFreeHandle((void **)hd_sess,MAX_SESSION);
	if(f==-1) ThrowInvalidHandle(INT_NOHANDLERES);
	WOCISession *p= new WOCISession();
	if(!p) ThrowInvalidHandle(INT_ALLOCATE_FAIL);
	hd_sess[f]=p;
	bool rt=false;
	try {
	 rt=p->Connect(username,password,svcname);
	}
	catch(...) {
		delete p;
		hd_sess[f]=NULL;
		throw;
	}
	if(!rt) {
		delete p;
		hd_sess[f]=NULL;
		ThrowInvalidHandle(INT_CREATEOBJ_FAIL);
	}
	return f+HANDLE_SESS;
}

DllExport int wociCommit(int sess) {
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	hd_sess[f]->Commit();
	return 0;
}

DllExport int wociRollback(int sess) {
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	hd_sess[f]->Rollback();
	return 0;
}

DllExport int wociSetTerminate(int sess,bool val) {
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	hd_sess[f]->SetTerminate(val);
	return 0;
}

DllExport bool wociIsTerminate(int sess) {
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) return false;
	return hd_sess[f]->IsTerminate();
}

DllExport bool wociSetNonBlockMode(int sess) {
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) return false;
	return hd_sess[f]->SetNonBlockMode();
}

//////////////////////////////////////////////////////////////////
// Statment function . 
//
//
//////////////////////////////////////////////////////////////////
int stmtcrtct=0;
DllExport int wociCreateStatment(int sess) {
	int t=GetFreeHandle((void **)hd_stmt,MAX_STATMENT);
	if(t==-1) ThrowInvalidHandle(INT_NOHANDLERES);
	int f=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	WOCIStatment *p=new WOCIStatment(hd_sess[f]);
	if(!p) ThrowInvalidHandle(INT_ALLOCATE_FAIL);
	if(hd_stmt[t]!=(WOCIStatment *)-1) 
		throw;
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

DllExport bool wociBindStrByPos(int stmt,int pos,char *ptr,int len) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->BindByPos(pos,ptr,len);
}

DllExport bool wociBindDoubleByPos(int stmt,int pos,double *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool wociBindIntByPos(int stmt,int pos,int *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool wociBindDateByPos(int stmt,int pos,char *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->BindByPos(pos,ptr);
}

DllExport bool wociDefineStrByPos(int stmt,int pos,char *ptr,int len) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->DefineByPos(pos,ptr,len);
}

DllExport bool wociDefineDoubleByPos(int stmt,int pos,double *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport bool wociDefineIntByPos(int stmt,int pos,int *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport bool wociDefineDateByPos(int stmt,int pos,char *ptr) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->DefineByPos(pos,ptr);
}

DllExport int wociBreakAndReset(int stmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->BreakAndReset();
}

thread_rt Execute(void *ptr) {
	int f=((int *)ptr)[0];
	int times=((int *)ptr)[1];
	delete []ptr;
	if(asyncMode) {
#ifndef __unix
	LockStmt(f,INFINITE);
	SetEvent(inprocess[f]);
#else
	UnlockStmt(f);
#endif
	}
	try {
		retval_stmt[f]=(int)hd_stmt[f]->Execute(times);
	}
	catch(WOCIError &e) {
		
	  sb4 cd;
	  char *str;
	  char *fn;
	  int l;
 	  e.GetErrPos(l,&fn);
	  if(l!=-1)
	   errprintf("Error occurs at file %s line %d.\n",fn,l);
	  e.GetLastError(cd,&str);
	  errprintf (" ErrorCode: %d.  %s\n",cd,str);
	  
	  retval_stmt[f]=-1;
	}
	catch(...) {
		//printf("A Error detected,program terminated!\n");
		retval_stmt[f]=-2;
	}
	if(asyncMode) 
		UnlockStmt(f);
	thread_end;
}

DllExport int wociExecute(int stmt,int times) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	int *val=new int[2];
	val[0]=f;val[1]=times;
	if(asyncMode) {
#ifndef __unix
	_beginthread(Execute,81920,(void *)val);
	WaitForSingleObject(inprocess[f],INFINITE);
#else
	LockStmt(f,INFINITE);
	pthread_create(&hd_pthread_t[f],NULL,Execute,(void *)val);	
	LockStmt(f,INFINITE);
#endif
	}
	else {
		Execute(val);
	}
	
	if(!asyncMode) {
		return wociWaitLastReturn(stmt);
	}
	
	return retval_stmt[f];

}

DllExport int wociExecuteAt(int stmt,int times,int offset) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->Execute(times,offset);
}

DllExport int wociFetch(int stmt,int rows) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->Fetch(rows);
}

DllExport int wociGetStmtColumnsNum(int stmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->GetColumns();
}

DllExport int wociGetMtColumnsNum(int mt) {
	int f=CheckHd(mt-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnNum();
}

DllExport int wociGetFetchedRows(int stmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->GetRows();
}

DllExport int wociGetSession(int stmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	WOCISession *ps=hd_stmt[f]->GetSession();
	if(!ps) ThrowInvalidHandle(INT_INVALID_HANDLE);
	for(int i=0;i<MAX_SESSION;i++) {
		if(hd_sess[i]==ps) return i+HANDLE_SESS;
	}
	ThrowInvalidHandle(INT_INVALID_HANDLE);
	return -1;
}

DllExport unsigned short wociGetStmtType(int stmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_stmt[f]->GetStmtType();
}

DllExport bool wociPrepareStmt(int stmt,char *sqlstmt) {
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return false;
	return hd_stmt[f]->Prepare(sqlstmt);
}

//////////////////////////////////////////////////////////////////
// MemTable function . 
//
//
//////////////////////////////////////////////////////////////////

DllExport int wociCreateMemTable() {
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

DllExport bool wociAddColumn(int memtab,char *name,char *dspname,int ctype,int length,int scale) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->AddColumn(name,dspname,ctype,length,scale);
}

DllExport bool wociBuildStmt(int memtab,int stmt,unsigned int rows)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(t==-1) return false;
	id_stmtused[f]=t;
	return hd_memtab[f]->BuildStmt(hd_stmt[t],rows);
}

DllExport BOOL wociBuild(int memtab,unsigned int rows)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	id_stmtused[f]=-1;
	return hd_memtab[f]->Build(rows);
}

DllExport void wociClear(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->Clear();
	return ;
}

DllExport void wociAddrFresh(int memtab,char **colval,int *collen,int *coltp) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
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
	catch(WOCIError &e) {
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
DllExport BOOL wociAppendToDbTable(int memtab,char *tablename,int sess) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(t==-1) return false;
	return hd_memtab[f]->CopyToTab(tablename,hd_sess[t]);
	/*
	void *ptr[]={&f,&t,tablename};
	_beginthread(CopyToDbTable,0,(void *)ptr);
	WaitForSingleObject(inprocess[t],INFINITE);
	if(!asyncMode) {
		wociWaitMemtable(memtab);
		return retval_memtab[f];
	}
	return true;
	*/
}

thread_rt FetchAll(void *memtab) {
	int *ptr=(int *)memtab;
	int f=ptr[0];
	int st=ptr[1];
	delete []ptr;
	if(asyncMode) {
#ifndef __unix
	LockMemtable(f,INFINITE);
	SetEvent(inprocess[id_stmtused[f]]);
#else
	UnlockMemtable(f);
#endif
	}
	try{
		hd_stmt[id_stmtused[f]]->Execute(0,0);
		retval_memtab[f]=(int)hd_memtab[f]->FetchAll(st);
	}
	catch(WOCIError &e) {
	  sb4 cd;
	  char *str;
	  char *fn;
	  int l;
 	  e.GetErrPos(l,&fn);
	  if(l!=-1)
	   errprintf("Error occurs at file %s line %d.\n",fn,l);
	  e.GetLastError(cd,&str);
	  errprintf (" ErrorCode: %d.  %s\n",cd,str);
	  retval_memtab[f]=-1;
	}
	catch(...) {
		//printf("A Error detected,program terminated!\n");
		retval_memtab[f]=-2;
	}
	if(asyncMode) {
		UnlockMemtable(f);
	}
	thread_end;
}

DllExport unsigned int wociFetchAll(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return 0;
	int *ptr=new int[2];
	ptr[0]=f;
	ptr[1]=0;
	if(asyncMode) {
#ifdef __unix
	LockMemtable(f,INFINITE);
	pthread_create(&hd_pthread_t[f],NULL,FetchAll,(void *)ptr);
	LockMemtable(f,INFINITE);
#else
	_beginthread(FetchAll,81920,(void *)ptr);
	WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
	}
	else {
		FetchAll(ptr);
	}
	
	if(!asyncMode) {
		return wociWaitLastReturn(memtab);
	}
	
	return 0;
}

DllExport unsigned int wociFetchAllAt(int memtab,int st) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	int *ptr=new int[2];
	ptr[0]=f;
	ptr[1]=st;
	if(f==-1) return 0;
	if(asyncMode) {
#ifdef __unix
	LockMemtable(f,INFINITE);
	pthread_create(&hd_pthread_t[f],NULL,FetchAll,(void *)ptr);
	LockMemtable(f,INFINITE);
#else
	_beginthread(FetchAll,81920,(void *)ptr);
	WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
	}
	else {
		FetchAll(ptr);
		return retval_memtab[f];
	}
	/*
	if(!asyncMode) {
		wociWaitMemtable(memtab,INFINITE);
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
#else
	UnlockMemtable(f);
#endif
	}
	try {
		hd_stmt[id_stmtused[f]]->Execute(0,0);
		retval_memtab[f]=(int)hd_memtab[f]->FetchFirst(rows);
		}
	catch(WOCIError &e) {
		
	  sb4 cd;
	  char *str;
	  char *fn;
	  int l;
 	  e.GetErrPos(l,&fn);
	  if(l!=-1)
	   errprintf("Error occurs at file %s line %d.\n",fn,l);
	  e.GetLastError(cd,&str);
	  errprintf (" ErrorCode: %d.  %s\n",cd,str);
	  retval_memtab[f]=-1;
	}
	catch(...) {
		//printf("A Error detected,program terminated!\n");
		retval_memtab[f]=-2;
	}
	if(asyncMode) 
		UnlockMemtable(f);
	thread_end;
}

DllExport unsigned int wociFetchFirst(int memtab,unsigned int rows) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return 0;
	int *val=new int[2];
	val[0]=f;val[1]=rows;
	if(asyncMode) {
#ifdef __unix
	LockMemtable(f,INFINITE);
	pthread_create(&hd_pthread_t[f],NULL,FetchFirst,(void *)val);
	LockMemtable(f,INFINITE);
#else
	_beginthread(FetchFirst,81920,(void *)val);
	WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
	}
	else {
		FetchFirst(val);
	}
	
	if(!asyncMode) {
		return wociWaitLastReturn(memtab);
	}
	return retval_memtab[f];
}

thread_rt FetchNext(void *ptr) {
	int f=((int *)ptr)[0];
	int rows=((int *)ptr)[1];
	delete [] (int *)ptr;
	if(asyncMode) {
#ifndef __unix
	LockMemtable(f,INFINITE);
	SetEvent(inprocess[id_stmtused[f]]);
#else
    UnlockMemtable(f);
#endif
	}
	try {
	retval_memtab[f]=(int)hd_memtab[f]->FetchNext(rows);
		}
	catch(WOCIError &e) {
	  sb4 cd;
	  char *str;
	  char *fn;
	  int l;
 	  e.GetErrPos(l,&fn);
	  if(l!=-1)
	   errprintf("Error occurs at file %s line %d.\n",fn,l);
	  e.GetLastError(cd,&str);
	  errprintf (" ErrorCode: %d.  %s\n",cd,str);
	  retval_memtab[f]=-1;
	}
	catch(...) {
		//printf("A Error detected,program terminated!\n");
		retval_memtab[f]=-2;
	}
	if(asyncMode) 
		UnlockMemtable(f);
	thread_end;
}

DllExport unsigned int wociFetchNext(int memtab,unsigned int rows) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return 0;
	int *val=new int[2];
	val[0]=f;val[1]=rows;
	if(asyncMode) {
#ifdef __unix
	LockMemtable(f,INFINITE);
	pthread_create(&hd_pthread_t[f],NULL,FetchNext,(void *)val);
	LockMemtable(f,INFINITE);
#else
	_beginthread(FetchNext,81920,(void *)val);
	WaitForSingleObject(inprocess[id_stmtused[f]],INFINITE);
#endif
	}
	else {
		FetchNext(val);
	}
	
	if(!asyncMode) {
		return wociWaitLastReturn(memtab);
	}
	
	return retval_memtab[f];
}

DllExport bool wociGeneTable(int memtab,char *tablename,int sess) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(t==-1) return false;
	return hd_memtab[f]->GeneTable(tablename,hd_sess[t]);
}

DllExport int wociGetStrAddrByName(int memtab,char *col,unsigned int rowst,char **pstr,int *celllen) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr,*celllen);
}

DllExport int wociGetDateAddrByName(int memtab,char *col,unsigned int rowst,char **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetDoubleAddrByName(int memtab,char *col,unsigned int rowst,double **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetIntAddrByName(int memtab,char *col,unsigned int rowst,int **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetStrValByName(int memtab,char *col,unsigned int rowst,char *pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	int len;
	return hd_memtab[f]->GetValues(col,rowst,1,pstr,len);
}

DllExport int wociGetDateValByName(int memtab,char *col,unsigned int rowst,char *pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetValues(col,rowst,1,pstr);
}

DllExport double wociGetDoubleValByName(int memtab,char *col,unsigned int rowst){
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	double val;
	hd_memtab[f]->GetValues(col,rowst,1,&val);
	return val;
}

DllExport int wociGetIntValByName(int memtab,char *col,unsigned int rowst) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	int val;
	hd_memtab[f]->GetValues(col,rowst,1,&val);
	return val;
}

DllExport int wociGetStrAddrByPos(int memtab,int col,unsigned int rowst,char **pstr,int *celllen) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr,*celllen);
}

DllExport int wociGetDateAddrByPos(int memtab,int col,unsigned int rowst,char **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetDoubleAddrByPos(int memtab,int col,unsigned int rowst,double **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetIntAddrByPos(int memtab,int col,unsigned int rowst,int **pstr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetAddr(col,rowst,1,pstr);
}

DllExport int wociGetBufferLen(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return (int)hd_memtab[f]->GetBfLen();
}

DllExport bool wociGetCell(int memtab,unsigned int row,int col,char *str,bool rawpos) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->GetCell(row,col,str,rawpos);
}

DllExport int wociGetColumnDisplayWidth(int memtab,int col) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnDispWidth(col);
}

DllExport int wociGetColumnPosByName(int memtab,char *colname)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnId(colname);
}

DllExport int wociGetColumnName(int memtab,int id,char *colname)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnName(id,colname);
}

DllExport int wociGetColumnDataLenByPos(int memtab,int colid)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnLen(colid);
}

DllExport int wociGetColumnNumber(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnNum();
}

DllExport int wociGetColumnScale(int memtab,int colid)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnScale(colid);
}

DllExport void wociGetColumnTitle(int memtab,int colid,char *str,int len)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->GetColumnTitle(colid,str,len);
}

DllExport unsigned short wociGetColumnType(int memtab,int colid)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetColumnType(colid);
}

DllExport bool wociGetLine(int memtab,unsigned int row,char *str,bool rawpos,char *colnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->GetLine(str,row,rawpos,colnm);
}

DllExport int wociGetMemtableRows(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->GetRows();
}

DllExport void wociReInOrder(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	hd_memtab[f]->ReInOrder();
}

DllExport void wociGetTitle(int memtab,char *str,int len,char *colsnm)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->GetTitle(str,len,colsnm);
}

DllExport bool wociIsIKSet(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->IsPKSet();
}

DllExport bool wociOrderByIK(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->OrderByPK();
}

DllExport unsigned int wociSearchIK(int memtab,int key)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	int rn=hd_memtab[f]->SearchPK(key);
	if(rn>=0 && !hd_memtab[f]->IsQDelete(rn)) return rn;
	return -1;
}

DllExport void wociSetColumnDisplayName(int memtab,char *colnm,char *str)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->SetColDspName(colnm,str);
	return ;
}

DllExport bool wociSetIKByName(int memtab,char *str) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetPKID(str);
}

DllExport bool wociOrderByIK(int memtab,int colid) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetPKID(colid);
}

DllExport bool wociSetSortColumn(int memtab,char *colsnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetSortColumn(colsnm);
}

DllExport int wociSetStrValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	int len=0;
	return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf,len);
}

DllExport int wociSetDateValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport int wociSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,double *bf) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport int wociSetIntValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,int *bf) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->SetValues(colid,rowstart,rownum,bf);
}

DllExport bool wociSort(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->Sort();
}

DllExport bool wociSortHeap(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SortHeap();
}

/*  Group Functions ***************************************/

DllExport bool wociSetGroupSrc(int memtab,int src) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(src-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return (int)hd_memtab[f]->SetGroupSrc(hd_memtab[t]);
}

DllExport bool wociSetIKGroupRef(int memtab,int ref,char *colnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(ref-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return (int)hd_memtab[f]->SetGroupRef(hd_memtab[t],colnm);
}

DllExport bool wociSetGroupSrcCol(int memtab,char *colsnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetGroupColSrc(colsnm);
}


DllExport bool wociSetGroupRefCol(int memtab,char *colsnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetGroupColRef(colsnm);
}

DllExport bool wociSetSrcSumCol(int memtab,char *colsnm) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->SetCalCol(colsnm);
}

DllExport bool wociGroup(int memtab,int rowstart,int rownum) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return (int)hd_memtab[f]->Group(rowstart,rownum);
}

//////////////////////////////////////////////////////////////////
// ExcelEnv function . 
//
//
//////////////////////////////////////////////////////////////////
#if !(defined(__unix) || defined(NO_EXCEL))
DllExport int wociCreateExcelEnv() {
	int t=GetFreeHandle((void **)hd_excelf,MAX_EXCELFILE);
	if(t==-1) ThrowInvalidHandle(INT_NOHANDLERES);
	WOCIExcelEnv *p=new WOCIExcelEnv();
	if(!p) ThrowInvalidHandle(INT_CREATEOBJ_FAIL);
	hd_excelf[t]=p;
	return t+HANDLE_EXCELF;
}
DllExport void wociSetDir(int excel,char *strTemplate,char *strReport)
{
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return ;
	hd_excelf[f]->SetDir(strTemplate,strReport);
}

DllExport void wociSetMemTable(int excel,int memtab)
{
	int t=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return ;
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return ;
	hd_excelf[f]->SetDataTable(hd_memtab[t]);
}

DllExport bool wociLoadTemplate(int excel,char *tempname) {
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return false;
	return (int)hd_excelf[f]->LoadTemp(tempname);
}

DllExport bool wociSelectSheet(int excel,char *sheetname) {
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return false;
	return (int)hd_excelf[f]->SelectSheet(sheetname);
}

DllExport bool wociFillData(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol, unsigned int fromrow, unsigned int colnum, unsigned int rownum,bool rawpos) {
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return false;
	return (int)hd_excelf[f]->Fill(tocol,torow,fromcol,fromrow,colnum,rownum,rawpos);
}

DllExport bool wociFillTitle(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol,unsigned int colnum) {
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return false;
	return (int)hd_excelf[f]->FillTitle(tocol,torow,fromcol,colnum);
}

DllExport bool wociSaveAs(int excel,char *filename) {
	int f=CheckHd(excel-HANDLE_EXCELF,MAX_EXCELFILE);
	if(f==-1) return false;
	return (int)hd_excelf[f]->SaveAs(filename);
}
#endif

void ErrorCheck(char *fn, int ln,int errcode)
{
	WOCIError err;
	err.SetErrPos(ln,fn);
	//err.retval=err.errcode=ercd;
	char buf[1000];
	switch(errcode) {
	case INT_INVALID_HANDLE	:
		sprintf(buf,"Exception : Invalid handle.");
		break;
	case INT_ALLOCATE_FAIL   :
		sprintf(buf,"Exception : Allocate object fail.");
		break;
	case INT_NOHANDLERES		:
		sprintf(buf,"Exception : Handle resource is exhausted");
		break;
	case INT_CREATEOBJ_FAIL	:
		sprintf(buf,"Exception : Create object fail.");
		break;
	case INT_CREATEMTMUTEX_FAIL	:
		sprintf(buf,"Exception : Create memory table's mutex object fail.");
		break;
	case INT_CREATESTMUTEX_FAIL	:
		sprintf(buf,"Exception : Create statment's mutex ojbect fail.");
		break;
	case INT_WAITMTMUTEX_FAIL	:
		sprintf(buf,"Exception : Wait for memory table mutex ojbect fail.");
		break;
	case INT_WAITSTMUTEX_FAIL	:
		sprintf(buf,"Exception : Wait for statment mutex ojbect fail.");
		break;
	default :
		sprintf(buf,"Exception : Errorcode no defined.");
		break;
	}
	err.SetErrCode(errcode);
	err.SetErrMsg(buf);
	throw(err);
}

DllExport int wociGetMemUsed(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	return hd_memtab[f]->GetMemUsed();
}

DllExport bool wociSetSortedGroupRef(int memtab,int mtref, char *colssrc)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(mtref-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return hd_memtab[f]->SetSortedGroupRef(hd_memtab[t],colssrc);
}

DllExport int wociSearch(int memtab,void **ptr) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	int rn=hd_memtab[f]->Search(ptr);
	if(rn>=0 && !hd_memtab[f]->IsQDelete(rn)) return rn;
	return -1; 
}

DllExport bool wociValuesSet(int memtab,int mtsrc,char *colto,char *colfrom,bool usedestKey,int op)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(mtsrc-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return hd_memtab[f]->ValuesSetTo(colto,colfrom,hd_memtab[t],usedestKey,op);
}

DllExport bool wociDeleteRow(int memtab,int rown) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->DeleteRow(rown);
}

DllExport int wociGetCreateTableSQL(int memtab,char *buf,const char *tabname,bool ismysql) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->GetCreateTableSQL(buf,tabname,ismysql);
}

DllExport bool wociInsertRows(int memtab,void **ptr,char *colsname,int num)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->InsertRows(ptr,colsname,num);
}

DllExport bool wociBindToStatment(int memtab,int stmt,char *colsname,int rowst)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(t==-1) return false;
	return hd_memtab[f]->BindToStatment(hd_stmt[t],colsname,rowst);
}


DllExport int wociTestTable(int sess,char *tablename) {
	int t=CheckHd(sess-HANDLE_SESS,MAX_SESSION);
	if(t==-1) return false;
	return hd_sess[t]->TestTable(tablename);
}

DllExport int wociGetRawrnBySort(int memtab,int ind) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	return hd_memtab[f]->GetRawrnBySort(ind);
}

DllExport int wociGetRawrnByIK(int memtab,int ind) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	return hd_memtab[f]->GetRawrnByPK(ind);
}

DllExport bool wociCompressBf(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->CompressBf();
}

DllExport bool wociCopyRowsTo(int memtab,int memtabto,int toStart,int start,int rowsnum) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(memtabto-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return hd_memtab[f]->CopyRowsTo(hd_memtab[t],toStart,start,rowsnum);
}

DllExport bool wociCopyColumnDefine(int memtab,int memtabfrom,const char *colsname) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(memtabfrom-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(t==-1) return false;
	return hd_memtab[f]->CopyColumnDefine(colsname,hd_memtab[t]);
}

DllExport bool wociGetColumnDesc(int memtab,void **pColDesc,int &cdlen,int &_colnm) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->GetColumnDesc(pColDesc,cdlen,_colnm);
}

DllExport bool wociExportSomeRows(int memtab,char *pData,int startrn,int rnnum)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->ExportSomeRows(pData,startrn,rnnum);
}

DllExport bool wociExport(int memtab,void *pData,int &_len,int &_maxrows,int &_rowct) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->Export(pData,_len,_maxrows,_rowct);
}

DllExport BOOL wociImport(int memtab,void *pData,int _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->Import(pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct);
}

DllExport BOOL wociAppendRows(int memtab,char *pData,int rnnum)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->AppendRows(pData,rnnum);
}
	//保留表结构，清除索引结构，数据行数置零
DllExport void wociReset(int memtab) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->Reset();
}

DllExport void wociClearIK(int memtab) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->clearIK();
}

DllExport void wociClearSort(int memtab) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->clearSort();
}

DllExport bool wociCompact(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->CompactBuf();
}

DllExport bool wociIsQDelete(int memtab,int rowrn) 
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->IsQDelete(rowrn);
}

DllExport int wociQDeletedRows(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	return hd_memtab[f]->QDeletedRows();
}

DllExport bool wociQDeleteRow(int memtab,int rownm)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->QDeleteRow(rownm);
}

DllExport int wociGetRowLen(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->GetRowLen();
}

int LockStmt(int stmt,int tm)
{
	int f=stmt;//CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return -1;
#ifdef __unix
	int rt=pthread_mutex_lock(&hd_mutex_stmt[f]);
#else
	//if(mutex_stmt_busy[f]) {
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
	//if(mutex_memtab_busy[f]) {
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

DllExport int wociWaitStmt(int stmt,int tm)
{
	int f=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return -1;
	int rt;
	if( (rt=LockStmt(f,tm))==WAIT_OBJECT_0)
	 UnlockStmt(f);
	return rt;
	//while(mutex_stmt_busy[f]) Sleep(500);
}

DllExport int wociWaitMemtable(int memtab,int tm)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	int rt;
	if ( (rt=LockMemtable(f,tm))==WAIT_OBJECT_0)
	UnlockMemtable(f);
	return rt;
	//while(mutex_stmt_busy[id_stmtused[f]]) Sleep(500);
}

DllExport int wociWaitLastReturn(int handle) {
	int f;
	if(handle>=HANDLE_MEMTAB) {
		f=CheckHd(handle-HANDLE_MEMTAB,MAX_MEMTABLE);
		if(f==-1) return -1;
		wociWaitMemtable(handle,INFINITE);
		if(retval_memtab[f]<0) {
			#ifdef WIN32
  		    AfxDumpStack();
  		    #endif
			throw *(WOCIError *)hd_memtab[f];
		}
		return retval_memtab[f];
	}
	f=CheckHd(handle-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return -1;
	wociWaitStmt(handle,INFINITE);
	if(retval_stmt[f]<0) {
		  #ifdef WIN32
		  AfxDumpStack();
		  #endif
		  throw *(WOCIError *)hd_stmt[f];
	}
	return retval_stmt[f];
}

DllExport int wociWaitTime(int handle,int time) {
	int f;
	if(handle>=HANDLE_MEMTAB) {
		f=CheckHd(handle-HANDLE_MEMTAB,MAX_MEMTABLE);
		if(f==-1) return -1;
		int ret=wociWaitMemtable(handle,time);
		return ret;
	}
	f=CheckHd(handle-HANDLE_STMT,MAX_STATMENT);
	if(f==-1) return -1;
	int ret=wociWaitStmt(handle,time);
	return ret;
}

DllExport bool wociFreshRowNum(int memtab) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
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
#else
	UnlockMemtable(r);
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
	catch(WOCIError &e) {
	  sb4 cd;
	  char *str;
	  char *fn;
	  int l;
 	  e.GetErrPos(l,&fn);
	  if(l!=-1)
	   errprintf("Error occurs at file %s line %d.\n",fn,l);
	  e.GetLastError(cd,&str);
	  errprintf (" ErrorCode: %d.  %s\n",cd,str);
	  retval_memtab[r]=-1;
	}
	catch(...) {
		//printf("A Error detected,program terminated!\n");
		retval_memtab[r]=-2;
	}
	if(asyncMode) {
		UnlockMemtable(p);
		UnlockMemtable(r);
	}
	thread_end;
}

DllExport int wociBatchSelect(int result,int param,char *colsnm)
{
	int p=CheckHd(param-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(p==-1) return false;
	int r=CheckHd(result-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(r==-1) return false;
	int t=id_stmtused[r];
	if(t==-1) return false;
	int *ptr=new int[2+strlen(colsnm)*sizeof(char)/sizeof(int)+1];
	ptr[0]=p;ptr[1]=r;strcpy((char *)(ptr+2),colsnm);
	//void *ptr[]={&p,&r,colsnm,NULL};
	if(asyncMode) {
#ifdef __unix
	LockMemtable(r,INFINITE);
	pthread_create(&hd_pthread_t[r],NULL,BatchSelect,(void *)ptr);
	LockMemtable(r,INFINITE);
#else
	_beginthread(BatchSelect,81920,(void *)ptr);
	WaitForSingleObject(inprocess[id_stmtused[r]],INFINITE);
#endif
	}
	else {
		BatchSelect(ptr);
	}
	
	if(!asyncMode) {
		return wociWaitLastReturn(result);
	}
	
	return retval_memtab[r];
}	

DllExport bool wociBatchNonSelect(int stmt,int param,char *colsnm)
{
	int f=CheckHd(param-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(t==-1) return false;
	int i;
	for(i=0;i<hd_memtab[f]->GetRows();i++){
		hd_memtab[f]->BindToStatment(hd_stmt[t],colsnm,i);
		hd_stmt[t]->Execute(1,0);
	}
	return true;
}	

DllExport void wociGetMTName(int memtab,char *bf)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return ;
	hd_memtab[f]->GetMTName(bf);
}

DllExport int wociGetMaxRows(int memtab)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return 0;
	return hd_memtab[f]->GetMaxRows();
}

DllExport int wociReadFromTextFile(int memtab,char *fn,int rowst,int rownm)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return -1;
	return hd_memtab[f]->ReadFromTextFile(fn,rowst,rownm);
}

DllExport void wociMTToTextFile(int memtab,char *fn,int rownm,char *colsnm) {
	int rows=wociGetMemtableRows(memtab);
	char mess[2000];
	//取列标题
	if(rownm==0 || rownm>rows) rownm=rows;
	FILE *fp=fopen(fn,"a+t");
	if(fp==NULL) throw;
	wociGetMTName(memtab,mess);
	fprintf(fp,"\n\n%s\n",mess);
	wociGetTitle(memtab,mess,2000,colsnm);
	fprintf(fp,mess);fprintf(fp,"\n");
	int i;
	for(i=0;i<rownm;i++) {
	  //取单行数据的文本
	  wociGetLine(memtab,i,mess,false,colsnm);
	  fprintf(fp,mess);
	  fprintf(fp,"\n");
	}
	fclose(fp);
}
int myprintf(const char *format,...);
DllExport void wociMTPrint(int memtab,int rownm,char *colsnm) {
	//取总行数
	int rows=wociGetMemtableRows(memtab);
	char mess[2000];
	//取列标题
	if(rownm==0 || rownm>rows) rownm=rows;
	wociGetTitle(memtab,mess,2000,colsnm);
	myprintf(mess);myprintf("\n");
	int i;
  
	for(i=0;i<rownm;i++) {
	  //取单行数据的文本
	  wociGetLine(memtab,i,mess,false,colsnm);
	  myprintf(mess);
	  myprintf("\n");
	}
}


DllExport void wociGetCurDateTime(char *date) {
	struct tm strtm;
    time_t tmtm;
	time( &tmtm);
	strtm=*localtime(&tmtm);
	date[0]=(strtm.tm_year+1900)/100+100;
	date[1]=(strtm.tm_year+1900)%100+100;
	date[2]=strtm.tm_mon+1;
	date[3]=strtm.tm_mday;
	date[4]=strtm.tm_hour+1;
	date[5]=strtm.tm_min+1;
	date[6]=strtm.tm_sec+1;
}
//Parametes: year(four digits),month(1-12),day(1-31),hour(0-23),minute(0-59),second(0-59).
// return a new datatime handle
DllExport void wociSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) {
	date[0]=year/100+100;
	date[1]=year%100+100;
	date[2]=(char)mon;
	date[3]=(char)day;
	date[4]=(char)hour+1;
	date[5]=(char)min+1;
	date[6]=(char)sec+1;
}

//Parameters:year,month,day ; hour,minute,second will be set to zero.
// return a new datetime handle
DllExport void wociSetDate(char *date,int year,int mon,int day){
	date[0]=year/100+100;
	date[1]=year%100+100;
	date[2]=(char)mon;
	date[3]=(char)day;
	date[4]=1;
	date[5]=1;
	date[6]=1;
}
//Format : yyyy/mm/dd hh24:mi:ss
DllExport void wociDateTimeToStr(char *date,char *str) {
	sprintf(str,"%4d/%02d/%02d %02d:%02d:%02d",
		(date[0]-100)*100+date[1]-100,date[2],date[3],date[4]-1,
		date[5]-1,date[6]-1);
}

DllExport int wociGetYear(char *date) {
	return (date[0]-100)*100+date[1]-100;
}

DllExport int wociGetMonth(char *date) {
	return date[2];
}

DllExport int wociGetDay(char *date) {
	return date[3];
}

DllExport int wociGetHour(char *date) {
	return date[4]-1;
}
DllExport int wociGetMin(char *date) {
	return date[5]-1;
}
DllExport int wociGetSec(char *date) {
	return date[6]-1;
}

DllExport void wociSetEcho(bool val) {
	WOCIError::SetEcho(val);
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

DllExport int wociDateDiff(char *d1,char *d2) {
	WOCIDate dt1(d1),dt2(d2);
	return int(difftime(dt1.GetTime_T(),dt2.GetTime_T()));
}

DllExport bool wociReplaceStmt(int memtab,int stmt)
{
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	int t=CheckHd(stmt-HANDLE_STMT,MAX_STATMENT);
	if(t==-1) return false;
	id_stmtused[f]=t;
	return hd_memtab[f]->ReplaceStmt(hd_stmt[t]);
}

DllExport int wociConvertColStrToInt(int memtab,const char *colsn,int *pcol) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->ConvertColStrToInt(colsn,pcol);
}

DllExport int wociCompareSortRow(int memtab,unsigned int row1, unsigned int row2) {
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) return false;
	return hd_memtab[f]->CompareSortRow(row1,row2);
}

DllExport int wociSaveSort(int memtab,FILE *fp){
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->SaveSort(fp);
}

DllExport void wociCopyToMySQL(int memtab,unsigned int startrow,
			unsigned int rownum,FILE *fp){
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	hd_memtab[f]->CopyToMySQL(startrow,rownum,fp);
}

DllExport int wociLoadSort(int memtab,FILE *fp){
	int f=CheckHd(memtab-HANDLE_MEMTAB,MAX_MEMTABLE);
	if(f==-1) ThrowInvalidHandle(INT_INVALID_HANDLE);
	return hd_memtab[f]->LoadSort(fp);
}

DllExport int wociGetLastError(int handle){
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

DllExport void wociList(char *pout){
	int usect=0,newct=0,busyct=0;
	int i;
	for(i=0;i<MAX_STATMENT;i++) {
		if(hd_stmt[i]) {
			if(hd_stmt[i]==(WOCIStatment *)-1) {
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
			if(hd_sess[i]==(WOCISession *)-1) {
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



/// End of file interface.cpp
