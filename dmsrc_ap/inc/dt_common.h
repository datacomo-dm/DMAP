#ifndef __DT_COMMON_H
#define __DT_COMMON_H

#define DBPLUS_STR	"DataMerger"
#define DP_VERSION	" 管理维护工具"
/*  common header file for dt */
#ifdef TRU64_CXX
#define fseeko fseek
#define ftello ftell
#endif

#ifndef boolxx_defined
#define boolxx_defined
#if (defined(MYSQL_SERVER) && defined(WIN32)) 
#define bool BOOL
#endif
#endif

#ifndef __unix
 #include <windows.h>
 #include <direct.h>
 #include <io.h> 
 #define STRICMP _stricmp
 #include <stdio.h>
 #define fseeko(fp,offset,origin) _lseeki64(_fileno(fp),offset,origin)
 #define ftello(fp) _telli64(_fileno(fp))
 #define realpath(src,dst) _fullpath(dst,src,PATH_LEN)
 #define PATH_SEP	"\\"
 #define PATH_SEPCHAR	'\\'
 #ifndef MAX_PATH
  #define MAX_PATH _MAX_PATH
 #endif
#else
 #define PATH_SEP	"/"
 #define PATH_SEPCHAR	'/'
 #define getch getchar
 #include <sys/time.h>
 #include <unistd.h>
//#include <ctype.h>
 #ifndef AIX
/*  #include <ctype.h>*/
  #include <sys/mount.h>
 #endif
 #include <stdio.h>
 #include <pthread.h>
 #include <strings.h>
 #include <stdarg.h>
 #include <sys/stat.h>
 #include <sys/types.h>
 #define _chdir chdir
 #define _mkdir mkdir
 #define STRICMP strcasecmp
 #ifndef min
  #define min(a,b) ((a)>(b)?(b):(a))
 #endif
 #ifndef max
  #define max(a,b) ((a)>(b)?(a):(b))
 #endif
 #define MAX_PATH 400
#endif

#include "dt_global.h"
#ifdef WIN32
 #define mCopyFile(a,b) CopyFile(a,b,FALSE)
#else
 int uCopyFile(const char * src,const char *dest);
 #define mCopyFile(a,b) uCopyFile(a,b)
#endif	


#include <string.h>
#include <time.h>

#ifndef TIMEOFDAY

#ifdef CLOCK_PROCESS_CPUTIME_ID
 #define TIMEOFDAY CLOCK_PROCESS_CPUTIME_ID
#else
 #define TIMEOFDAY CLOCK_REALTIME
#endif

#endif
/*
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
*/

#ifndef DTIOExport
#ifdef WIN32
#ifdef  LIBDPIO_EXPORTS
#define DTIOExport   __declspec( dllexport ) 
#else
#define DTIOExport   __declspec( dllimport ) 
#endif
#else
#define DTIOExport
#endif
#endif 
#ifdef WIN32
 #define mSleep(msec) Sleep(msec)
#else
//毫秒转换到纳秒
 #define mSleep(msec) \
 { \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=msec%1000*1000*1000;\
   nanosleep(&req, &rem);\
 }
#endif 
	
#define mySleep(a) {if(a>10) printf("\n%d秒后运行。\n",a); mSleep(a*1000);}
#define freeinfo1(hdinfo) 
//#define ThrowWith(a,...) ThrowWithI(__LINE__,__FILE__,a,...)
//void ThrowWithI(const int ln,const char * fn,const char *format,...) ;
DTIOExport int cgetpwd(char *bf);
DTIOExport int getOption(const char *prompt,int defaultval,const int lmin,const int lmax);
DTIOExport int getString(const char *prompt,const char *defaultval,char *val);
DTIOExport int getdbcstr(char *un,char *pwd,char *sn,const char *prompt);

DTIOExport void ThrowWith(const char *format,...) ;
// dbtype :1 orcle 2 odbc 0: unsure(user select)
DTIOExport int BuildConn(int dbtype);
DTIOExport int SplitStr(const char *src,char **dst,char **pbf) ;
DTIOExport void BuildWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf);
DTIOExport void BuildPartitionWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf);
DTIOExport void decode(char *str) ;
DTIOExport void encode(char *str) ;
DTIOExport int GetFreeM(const char *path) ;
#define TM_WAIT_FOR_DISKFULL 300
DTIOExport size_t dp_fwrite(const  void  *ptr,  size_t  size,  size_t nmemb,  FILE *stream);
#ifdef __cplusplus
DTIOExport bool GetYesNo(const char *prompt,bool defval);
#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
class DTIOExport mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=(ed.tv_sec-st.tv_sec);
	ct.tv_nsec+=(ed.tv_nsec-st.tv_nsec);
	//printf("Stop at %d s,%dns\n",ct.tv_sec,ct.tv_nsec);
	st=ed;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};
class DTIOExport autotimer {
	mytimer *ptm;
public :
	autotimer(mytimer *p) {
		ptm=p;
		p->Start();
	}
	~autotimer() {
		ptm->Stop();
	}
};
#endif
class DTIOExport DbplusCert
{
	char buf[81920];
	int lines;
	DbplusCert();
	static DbplusCert *pInst;
	int DecodeFromFile(const char *fn);
	const char *getLine(const char *itemname);
public :
	static DbplusCert *getInstance() {return pInst;}
	static void ReleaseInstance() {delete pInst;}
	static void initInstance() {pInst=new DbplusCert();}
	const char *getUserTitle();
	// 1 for full,10 for std, 20 for eval.
	int getFunctionCode();
	const char *getLogo();
	const char *getCopyright(); 
	// y=0 for none expired date;
	void GetExpiredDate(int &y,int &m,int &d);
	const char *getProductId();
	double getRowLimit();
	//versiontype 1 for core,2 for bs, 3 for admin 4,for library
	const char *getVersion(int versiontype);
	void printlogo() ;
	
};

#endif

#define MAX_DST_DATAFILENUM 5000	//目标文件数量
#define MAX_MIDDLE_FILE_NUM 5000	//临时文件数量
#define DEFAULT_IDX_BLOCK_ROWNUM	1024*32 //索引数据文件的数据块
#define MAX_DST_INDEX_NUM	300	//最大索引数，含独立和依赖索引
#define MAX_DST_PART_NUM	300	//单表最大分区数
#define MAX_SRC_SYSNUM	50	//单表最大分区数
#define MIN_BLOCKRN 20	
#define MAX_BLOCKRN 64*1024
#define LOG_MSG_LEN 1002


#endif

