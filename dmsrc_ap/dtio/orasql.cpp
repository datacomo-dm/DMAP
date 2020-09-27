#include "dtio.h"
#include "dt_common.h"
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#ifdef WIN32
#include <conio.h>
#endif
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;
int Start(void *p);
int main(int argc,char **argv) {
    int nRetCode = 0;
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    if(argc!=2) {
    	return -1;
    } 
    WOCIInit("util/orasql");
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    nRetCode=wociMainEntrance(Start,true,NULL,corelev);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *p) {
   wociSetOutputToConsole(TRUE);
   wociSetEcho(FALSE);
   cmd_base cp;
   cp.GetEnv();
   if(!cp.checkvalid()) return -1;
   AutoHandle dts;
   dts.SetHandle(wociCreateSession(cp.ousrname,cp.opswd,cp.osn,DTDBTYPE_ORACLE));
   AutoMt mt(dts,10);
   mt.FetchFirst(cp.argv[1]);
   if(mt.Wait()<1) return -1;
   char line[8000];
   wociGetLine(mt,0,line,false,NULL);
   printf("%s\n",line);
   return 0;
}

