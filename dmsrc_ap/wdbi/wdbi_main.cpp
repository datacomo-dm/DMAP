#ifndef __unix
#include "stdafx.h"
#include <conio.h>
//#include "auttest.h"
//#include "WOCIExcelEnv1.h"
#else
#define getch getchar
#include <stdio.h>
#include <string.h>
#include "wdbierr.h"
#include <signal.h>
#endif 
#include "wdbi.h"

//#include "interface.h"
#include <time.h>
/////////////////////////////////////////////////////////////////////////////
// The one and only application object

//CWinApp theApp;

//using namespace std;
void _WDBIInit(char *appname) ;
void _WDBIQuit() ;
void CatchProcess() ;
void BreakAll();
extern bool asyncMode;
bool DllExport wdbi_kill_in_progress=false;
#ifndef __unix
	BOOL WINAPI ControlBreak(DWORD dwCtrlType) {
		switch(dwCtrlType) {
		case CTRL_C_EVENT:
		case CTRL_BREAK_EVENT:
			BreakAll();
			return true;
		}
		return false;
	}
#else
static void wdbi_kill(int signo)
{
	if(!wdbi_kill_in_progress) {
		wdbi_kill_in_progress=true;
		BreakAll();
		//signal(SIGABRT,SIG_IGN);
		//signal(signo, SIG_IGN);//SIG_DFL
		//throw "程序强制终止.";
	}
	signal(signo, SIG_IGN);
	  signal(SIGQUIT, SIG_IGN);
  signal(SIGKILL, SIG_IGN);
  signal(SIGTERM, SIG_IGN);
  signal(SIGINT,  SIG_IGN);
  //signal(SIGALRM, SIG_IGN);
  //signal(SIGBREAK,SIG_IGN)

}
#endif

int catchLevel0(int (*func)(void *),void *ptr) {
	return func(ptr);
}

int catchLevel1(int (*func)(void *),void *ptr) {
	try {
		return catchLevel0(func,ptr);
	}
	catch(WDBIError &e) {
	  return -1;
	}
}
	
int catchLevel2(int (*func)(void *),void *ptr) {
	try {
		return catchLevel1(func,ptr);
	}
	catch(int &e) {
		errprintf("错误代码 %d,出现异常错误,程序终止运行!\n",e);
		return -2;
	}
	catch(char *erstr) {
		errprintf("错误信息:%s\n出现异常错误,程序终止运行!\n",erstr);
		return -2;
	}
}	
	
int MainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel,int opt);
int SetEntrance(bool asyncmode,int opt);
	
DllExport int _wdbiMainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel) //modify by chxl
{
	 if(func==NULL){
     return  SetEntrance(asyncMode,WDBI_ENT_FULL);
	 }
	 else{
	   	return MainEntrance(func,asyncmode,ptr,catchLevel,WDBI_ENT_FULL);
	 }
}


// opt : 1-Need init
//         2-need destroy all resourece on exit
//         4-need break handler

DllExport int _wdbiMainEntranceEx(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel,int opt) 
{
   if(func==NULL){
       return  SetEntrance(asyncMode,opt);
	}
	else{
	    return MainEntrance(func,asyncmode,ptr,catchLevel,opt);
  }
}


int SetEntrance(bool asyncmode,int opt){
    int tracelev=2;
   if(opt & WDBI_ENT_INIT) {
    	const char *pcl=getenv("DP_TRACELEVEL");
    	if(pcl!=NULL) tracelev=atoi(pcl);
    	if(tracelev==99) WDBIError::SetTrace(true);
    	else WDBIError::SetTrace(false);
#if !(defined(__unix) || defined(NO_EXCEL))
	if (CoInitialize(NULL)!=0) 
	{ 
		MessageBox(NULL,"错误","初始化COM支持库失败!",MB_OK); 
		exit(1); 
	} 
#endif
     	asyncMode=asyncmode;
}

    if(opt&WDBI_ENT_BREAK) {
    #if !(defined(__unix))
	  SetConsoleCtrlHandler(ControlBreak,true);
    #else
    signal(SIGQUIT, wdbi_kill);
    signal(SIGKILL, wdbi_kill);
    signal(SIGTERM, wdbi_kill);
    signal(SIGINT,  wdbi_kill);
  //signal(SIGALRM, SIG_IGN);
  //signal(SIGBREAK,wdbi_kill);
#endif
   }
   return 0;
}

int MainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel,int opt)
{
    int nRetCode = 0;
	int tracelev=2;
if(opt & WDBI_ENT_INIT) {
    	const char *pcl=getenv("DP_TRACELEVEL");
    	if(pcl!=NULL) tracelev=atoi(pcl);
    	if(tracelev==99) WDBIError::SetTrace(true);
    	else WDBIError::SetTrace(false);

#if !(defined(__unix) || defined(NO_EXCEL))
	if (CoInitialize(NULL)!=0) 
	{ 
		MessageBox(NULL,"错误","初始化COM支持库失败!",MB_OK); 
		exit(1); 
	} 
#endif
     	asyncMode=asyncmode;
}

if(opt&WDBI_ENT_BREAK) {
#if !(defined(__unix))
	 SetConsoleCtrlHandler(ControlBreak,true);
#else
  signal(SIGQUIT, wdbi_kill);
  signal(SIGKILL, wdbi_kill);
  signal(SIGTERM, wdbi_kill);
  signal(SIGINT,  wdbi_kill);
  //signal(SIGALRM, SIG_IGN);
  //signal(SIGBREAK,wdbi_kill);

#endif
}
        if(catchLevel==0) 
          nRetCode=catchLevel0(func,ptr);
        else if(catchLevel==1)
          nRetCode=catchLevel1(func,ptr);
        else if(catchLevel==2)
          nRetCode=catchLevel2(func,ptr);
        else {
        	errprintf("错误捕获级别'%d'不支持(wdbiMainEntrance).",catchLevel);
        	nRetCode=-3;
        }
       
	if(opt & WDBI_ENT_CLEAN)  _WDBIQuit();
	if(opt & WDBI_ENT_BREAK) {
	 #ifndef __unix
   	   SetConsoleCtrlHandler(ControlBreak,false);
	 #endif
 	}
#if !(defined(__unix) || defined(NO_EXCEL))
	CoUninitialize();
#endif
	return nRetCode;
}
