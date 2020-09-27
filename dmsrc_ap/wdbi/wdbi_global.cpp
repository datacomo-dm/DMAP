#include "wdbi.h"
#include <sys/stat.h>
#include <sys/types.h>
#ifdef __unix
#include <unistd.h>
struct timespec interval = {0, 20000};
#else
#include <direct.h>
#endif
 
const char *DT_ERR_MSG[]= {
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
	"Column length is invalid for corresponding type,or part index value invalid,or no buffer to store part len.",
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
	"Memory table is full",
	"Can not open text file for read",
	"Out of maximum column width",
	"Function not supported",
	"Value loss on transfer to other type",
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
	"Out of maximum length of line on output to text file",
	"   ",
	"   ",
	"   ",
};


const char *DTL_ERR_MSG[]= {
	"  ",
	"Datatable can't be found. ",
	"Not defined any datatable.  ",
	"A datatable is exist using same name.",
	"    ",
	"    ",
};
OCIEnv *envhp; 
OCIError *errhp;
DllExport char errfile[MAX_PATH]="\x0";
DllExport char lgfile[MAX_PATH]="\x0";
bool __output_to_console=false;

char WDBIError::filen[_MAX_DIR];
int WDBIError::lineid=-1;
char WDBIError::afilen[_MAX_DIR];
int WDBIError::alineid=-1;
bool WDBIError::echo=true;
bool WDBIError::trace=false;
#ifdef WIN32
#define MKDIR(dir) _mkdir(dir)
#define CHDIR(dir) _chdir(dir)
#define getcwd _getcwd
#else
#define MKDIR(dir) mkdir(dir,S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)
#define CHDIR(dir) chdir(dir)
#endif

DllExport char wdbilasterror[50000];


//bit 操作 设置bit位的值

void SetBitValue(mbyte *pData, long n ,bool val )
{
		if (val)
		{
		  int  t = n/8;
		  int offset = (7-n%8);
			pData[t] = pData[t] | (1 << offset );  
		}
		else
		{
			int  t = n/8;
		  int offset = (7-n%8);
		  mbyte by = 1 << offset;
		  by = ~by;
		  pData[t] = pData[t]&by;
		}
}

bool CheckBitValue(mbyte *pData,long n)
{
   int nS = n/8;
   int nOffset =(7-n%8);
   mbyte R = pData[nS];
   R = R & (1 << nOffset);
   return R>0?true:false;	
}



DllExport int xmkdir(const char *dir) {
	int len=strlen(dir);
	char tdir[300];
	char olddir[300];
	int rt=0;
	const char *pt=dir;
	int off=0;
	getcwd(olddir,300);
	while(*pt && pt-dir<len) {
		if(*pt=='\\' || *pt=='/' || !pt[1]) {
			if(pt-dir>0 && pt[-1]!=':') 
			{
				strcpy(tdir,dir);
				tdir[pt-dir+1]=0;
				rt=CHDIR(tdir);
				if(rt) 
				 rt=MKDIR(tdir);
				if(rt) break;
			}
		}
		pt++;
	}
	CHDIR(olddir);
	return rt;
}

DllExport int xmkdir_withfile(const char *path) {
	char tdir[300];
	strcpy(tdir,path);
	int l=strlen(tdir)-1;
	while(tdir[l]!='\\' && tdir[l]!='/' && l>0) l--;
	if(l<1) return -1;//no path (only filename)
	tdir[l+1]=0;
	return xmkdir(tdir);
}

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

DllExport void _wdbiGetCurDateTime(char *date) {
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
DllExport void _wdbiSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) {
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
DllExport void _wdbiSetDate(char *date,int year,int mon,int day){
	date[0]=year/100+100;
	date[1]=year%100+100;
	date[2]=(char)mon;
	date[3]=(char)day;
	date[4]=1;
	date[5]=1;
	date[6]=1;
}
//Format : yyyy/mm/dd hh24:mi:ss
DllExport void _wdbiDateTimeToStr(const char *date,char *str) {
	sprintf(str,"%4d/%02d/%02d %02d:%02d:%02d",
		((unsigned char)date[0]-100)*100+(unsigned char)date[1]-100,date[2],date[3],date[4]-1,
		date[5]-1,date[6]-1);
}

DllExport int _wdbiGetYear(const char *date) {
	return ((unsigned char)date[0]-100)*100+(unsigned char)date[1]-100;
}

DllExport int _wdbiGetMonth(const char *date) {
	return date[2];
}

DllExport int _wdbiGetDay(const char *date) {
	return date[3];
}

DllExport int _wdbiGetHour(const char *date) {
	return date[4]-1;
}
DllExport int _wdbiGetMin(const char *date) {
	return date[5]-1;
}
DllExport int _wdbiGetSec(const char *date) {
	return date[6]-1;
}

DllExport bool _wdbiIsEcho() {
	return WDBIError::IsEcho();
}

DllExport void _wdbiSetOutputToConsole(bool val)
{
	__output_to_console=val;
}

DllExport void _wdbiSetTraceFile(const char *fn) {
	errfile[0]=lgfile[0]=0;
	//判断是否绝对路径
#ifdef WIN32
	if(fn[1]!=':') {
#else
	if(fn[0]!='/') {
#endif
	 const char *path=NULL;
	 if(path==NULL) path=getenv("WDBI_LOGPATH");
	 if(path) {
		strcpy(errfile,path);
		strcpy(lgfile,path);
		if(path[strlen(path)-1]!=PATH_SEPCHAR) {
		 strcat(errfile,PATH_SEP);
		 strcat(lgfile,PATH_SEP);
		}
	 }
	}
	strcat(errfile,fn);
	strcat(lgfile,fn);
	static char procstarttime[100]="";
        if(procstarttime[0]==0) {
         char curdt[20];
	 _wdbiGetCurDateTime(curdt);
	 sprintf(procstarttime,"%4d%02d%02d%02d%02d%02d",
		_wdbiGetYear(curdt),_wdbiGetMonth(curdt),_wdbiGetDay(curdt),
		_wdbiGetHour(curdt),_wdbiGetMin(curdt),_wdbiGetSec(curdt));
	}
        strcat(errfile,procstarttime);
        //为减少日志管理的难度,把普通日志和错误日志合并.----2006-03-10
	strcat(errfile,".log");
	strcat(lgfile,procstarttime);
	strcat(lgfile,".log");
	xmkdir_withfile(errfile);
	xmkdir_withfile(lgfile);
}

DllExport void wdbiSetErrorFile(const char *fn){
	xmkdir_withfile(fn);
	strcpy(errfile,fn);
}

DllExport void wdbiSetLogFile(const char *fn)
{
	xmkdir_withfile(fn);
	strcpy(lgfile,fn);
}

DllExport const char * _wdbiGetErrorFile(){
	return errfile;
}

DllExport const char * _wdbiGetLogFile()
{
	return lgfile;
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
	 if(wdbilasterror!=NULL) 
	   sprintf(wdbilasterror,"[%s %s] ",dtstr,tmstr);
	}
	
	if(__output_to_console) 
	 rt=vprintf(format,vlist);
        va_end(vlist);
	va_start(vlist,format);
	if(errfp) 
	 rt=vfprintf(errfp,format,vlist);

        va_end(vlist);
	va_start(vlist,format);
	 if(wdbilasterror!=NULL) 
	  vsprintf(wdbilasterror+strlen(wdbilasterror),format,vlist);
	if(strlen(format)>2 && format[strlen(format)-1]!='\n') {
	 if(__output_to_console) 
	  printf("\n");
	 if(errfp) 
	  fprintf(errfp,"\n");
	 if(wdbilasterror!=NULL) 
	  sprintf(wdbilasterror,"\n");
	}
	fclose(errfp);
	va_end(vlist);
	return rt;
}

DllExport int lgprintf(const char *format,...) {
	va_list vlist;
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
 	va_start(vlist,format);
	 if(__output_to_console) 
	 rt=vprintf(format,vlist);
        va_end(vlist);
	va_start(vlist,format);
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
void WDBIStartup () {
#ifndef NO_OCI
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
void WDBIShutdown() {
#ifndef NO_OCI
	if (envhp)
    (void) OCIHandleFree((dvoid *) envhp, OCI_HTYPE_ENV);
#endif
}
