#include "dt_lib.h"
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
void TestDateCvt();
void TestIntCvt() ;
void TestCompress(int dts,char *sqlst,int maxrows,char *fn,int cmpflag) ;
int Start(void *ptr);
void ShowDoubleByte() ; 
//#define dtdbg(a) lgprintf(a)
#define dtdbg(a) 
#ifdef __unix
pthread_t hd_pthread_loop;
#endif

const char *helpmsg=
  "Options : \n"
  "-f {check,dump,mload,dload,dc,all} \n"
  "    function select,default for all except dump \n"
  "-rm [database name] [table name] \n"
  "    remove dt table\n"
  "-mv [source db name] [source table name] [dest db name] [dest table name]\n"
  "    move table\n"
  "-hmr [db name] [table name]\n"
  "    homogeneous rebuild index.\n"
  "-enc [text]\n"
  "    Encode text \n"
  "-mh  (MySQL host name or ip address)\n"
  "    for DT MySQL Server connection \n"
  "-mun (MySQL username) \n"
  "    for DT MySQL Server connection\n"
  "-mpwd [MySQL password] \n"
  "    for DT MySQL Server connection\n"
  "-osn (oracle service name) \n"
  "    for Oracle AgentServer connection\n"
  "-oun (oracle username) \n"
  "    for Oracle AgentServer connection \n"
  "-opwd [Oracle password] \n"
  "    for Oracle AgentServer connection \n"
  "-lp 	\n"
  "    loop mode,input 'quit' or 'exit' to quit \n"
  "-fr 	\n"
  "    calculate freeze time. \n"
  "-v	\n"
  "    verbose mode ,Output every SQL operation details.\n"
  "-e	\n"
  "    echo mode ,Output message to console.1 for yes,0 for no\n"
  "-wt (wait time seconds) \n"
  "-thn (thread num on dc) \n "
  "-h \n"
  "    help information \n"
  "\n---------------------------------------------------------\n"
  "Macros : ( Env var)\n"
  "DT_INDEXNUM \n"
  "   Maximum index rows number\n"
  "DT_MTLIMIT \n"
  "   Memory usage limit for one of two blocks during dump\n"
  "DT_WAITTIME \n"
  "   Wait time (seconds) while using loop mode\n"
  "DT_ECHO \n"
  "   Output message to console.1 for yes,0 for no\n"
  "DT_VERBOSE \n"
  "   Output every SQL operation details.\n"
  "DT_LOOP \n"
  "   Loop mode,input 'quit' or 'exit' to quit\n"
  "DT_THREADNUM \n"
  "   Thread number for deep compression .\n"
  "DT_FUNCODE {CHECK DUMP MLOAD DLOAD DC ALL} \n"
  "   function selection\n"
  "DT_MPASSWORD  \n"
  "   password for DT MySQL Server connection \n"
  "DT_MUSERNAME \n"
  "   user name for DT MySQL Server connection\n"
  "DT_MHOST \n"
  "   host name or ip address for DT MySQL Server connection\n"
  "DT_ORAPASSWORD \n"
  "   password  for Oracle AgentServer connection\n"
  "DT_ORAUSERNAME \n"
  "   username for Oracle AgentServer connection\n"
  "DT_ORASVCNAME \n"
  "   service name for Oracle AgentServer connection\n"
  ;

#define PARAMLEN 100
struct   cmdparam {
  static int argc;
  static char **argv;
  char orasvcname[PARAMLEN];
  char orausrname[PARAMLEN];
  char orapswd[PARAMLEN];
  char mhost[PARAMLEN];
  char musrname[PARAMLEN];
  char mpswd[PARAMLEN];
  char srcdbn[PARAMLEN];
  char srctbn[PARAMLEN];
  char dstdbn[PARAMLEN];
  char dsttbn[PARAMLEN];
  //char lgfname[PARAMLEN];
  enum {
  	DT_NULL=0,DT_CHECK,DT_DUMP,DT_MDLDR,DT_DSTLDR,DT_DEEPCMP,DT_ALL,DT_MOVE,DT_REMOVE,DT_ENCODE,DT_HOMO_REINDEX,DT_LAST
  } funcid;
  
  bool loopmode,verbosemode,echomode,freezemode;
  int waittime;
  int indexnum;
  int mtlimit;
  int threadnum;
  cmdparam() {
  	memset(this,0,sizeof(cmdparam));
	threadnum=1;freezemode=false;
  }
  
  void GetParam() {
  	for(int i=1;i<argc;i++) 
  	 ExtractParam(i,argc,argv);
  }
  

  void GetEnv() {
  	SetValue(orasvcname,getenv("DT_ORASVCNAME"));
  	SetValue(orausrname,getenv("DT_ORAUSERNAME"));
  	SetValue(orapswd,getenv("DT_ORAPASSWORD"));
  	decode(orapswd);
  	SetValue(mhost,getenv("DT_MHOST"));
  	SetValue(musrname,getenv("DT_MUSERNAME"));
  	SetValue(mpswd,getenv("DT_MPASSWORD"));
  	decode(mpswd);
        const char *fncode=getenv("DT_FUNCODE");
  	//set DT_FUNCODE or -f(-f check,-f dump,-f mload,-f dload,-f dc,-f all) parameter.
  	if(fncode==NULL) 
  	  funcid=DT_ALL;
  	else if(strcmp(fncode,"CHECK")==0) funcid=DT_CHECK;
  	else if(strcmp(fncode,"DUMP")==0) funcid=DT_DUMP;
  	else if(strcmp(fncode,"MLOAD")==0) funcid=DT_MDLDR;
  	else if(strcmp(fncode,"DLOAD")==0) funcid=DT_DSTLDR;
  	else if(strcmp(fncode,"DC")==0) funcid=DT_DEEPCMP;
  	else if(strcmp(fncode,"ALL")==0) funcid=DT_ALL;
  	else funcid=DT_ALL;
  	SetValue(loopmode,getenv("DT_LOOP"));
  	SetValue(verbosemode,getenv("DT_VERBOSE"));
	SetValue(echomode,getenv("DT_ECHO"));
  	SetValue(waittime,getenv("DT_WAITTIME"));
  	SetValue(threadnum,getenv("DT_THREADNUM"));
  	SetValue(mtlimit,getenv("DT_MTLIMIT"));
  	SetValue(indexnum,getenv("DT_INDEXNUM"));
  }
  	
  	
  void SetValue(bool &val,const char *str) {
  	if(str!=NULL) {
	 int v=atoi(str);
	 if(v==1) val=true;
	 else val=false;
  	}
  	else
  	 val=false;
  }  	
  
  void SetValue(int &val,const char *str) {
  	if(str!=NULL)
  	 val=atoi(str);
  	else
  	 val=0;
  }
  void SetValue(char *val,const char *src) {
  	if(src==NULL) {
  	  memset(val,0,PARAMLEN);
  	  return;
  	}
  	int sl=strlen(src);
  	if(sl>PARAMLEN) {
  	 memcpy(val,src,PARAMLEN);
  	 val[PARAMLEN-1]=0;
  	}
  	else strcpy(val,src);
  }
  	
  bool checkvalid() {
  	bool ret=true;
  	if(strlen(orasvcname)<1) {
  	  printf("Need svcname to connect to oracle database server ,set DT_ORASVCNAME or use -osn option.\n");
  	  ret=false;
  	}
  	if(strlen(orausrname)<1) {
  	  printf("Need username to connect to oracle database server ,set DT_ORAUSERNAME or use -oun option.\n");
  	  ret=false;
  	}
  	if(strlen(orapswd)<1) {
  	  printf("Need password to connect to oracle database server ,set DT_ORAPASSWORD or use -opwd option.\n");
  	  ret=false;
  	}
  	if(strlen(mhost)<1) {
  	  printf("Need host name or ip address to connect to MySQL database server ,set DT_MHOST or use -mh option.\n");
  	  ret=false;
  	}
  	if(strlen(musrname)<1) {
  	  printf("Need username to connect to MySQL database server ,set DT_MUSERNAME or use -mun option.\n");
  	  ret=false;
  	}
  	if(strlen(mpswd)<1) {
  	  printf("Need password to connect to MySQL database server ,set DT_MPASSWORD or use -mpwd option.\n");
  	  ret=false;
  	}
  	if(funcid==DT_NULL || funcid>=DT_LAST) {
//  		"Invalid function code '%s',use one of 'check,dump,mload,dload,all'",argv[p]);
  	 printf("Invalid function selection code ,set DT_FUNCODE or -f (check,dump,mload,dload,dc,all) option..\n");
  	  ret=false;
  	}
  	if(waittime<0 || (waittime<1 && loopmode)) {
  	  printf("Invalid wait time (seconds) ,set DT_WAITTIME or -wt parameter\n");	
  	  ret=false;
  	}
  	if(waittime<15) {
  		printf("loop wait time = %d ,force assigned to 15\n",waittime);
  		waittime=15;// less then 15 seconds.
  	}
  	if(indexnum<2000000) {
  		printf("index number assinged to %d ,force assigned to 2000000\n",indexnum);
  		indexnum=2000000;// less then 2M?
  	}
  	if(mtlimit<200000000) {
  		printf("mt limit assinged to %d, force assigned to 200000000\n",mtlimit);
  		mtlimit=200000000;// less then 200M?
  	}
	//mtlimit=650000000;
	if(threadnum<1 || threadnum>8) {
		printf("Thread number can not large then 8 or less then 1,have set to 1.\n");
		threadnum=1;
	}
  	return ret;
  }
  
  void ExtractParam(int &p,int n,char **argv) {
  	if(strcmp(argv[p],"-f")==0) {
  	  if(p+1>=n) ThrowWith("Need function code,use one of 'check,dump,mload,dload,dc,all'");
  	  p++;
  	  if(strcmp(argv[p],"check")==0) funcid=DT_CHECK;
  	  else if(strcmp(argv[p],"dump")==0) funcid=DT_DUMP;
  	  else if(strcmp(argv[p],"mload")==0) funcid=DT_MDLDR;
  	  else if(strcmp(argv[p],"dload")==0) funcid=DT_DSTLDR;
  	  else if(strcmp(argv[p],"dc")==0) funcid=DT_DEEPCMP;
  	  else if(strcmp(argv[p],"all")==0) funcid=DT_ALL;
  	  else ThrowWith("Invalid function code '%s',use one of 'check,dump,mload,dload,dc,all'",argv[p]);
  	}
  	else if(strcmp(argv[p],"-mpwd")==0) {
  	  if(p+1>=n || argv[p+1][0]!='-') {
  	  	#ifdef WIN32
  	  	printf("Input password of MySQL Server:");
  	  	gets(mpswd);
  	  	#else
  	  	strcpy(mpswd,getpass("Input password of MySQLServer:"));
  	  	#endif
  	  }
  	  else strcpy(mpswd,argv[p++]);
  	}
  	else if(strcmp(argv[p],"-opwd")==0) {
  	  if(p+1>=n || argv[p+1][0]!='-') {
  	  	#ifdef WIN32
  	  	printf("Input password of MySQL Server:");
  	  	gets(orapswd);
  	  	#else
  	  	strcpy(orapswd,getpass("Input password of MySQLServer:"));
  	  	#endif
  	  }
  	  else strcpy(orapswd,argv[p++]);
  	}
  	else if(strcmp(argv[p],"-lp")==0) {
  	 loopmode=true;
	}
  	else if(strcmp(argv[p],"-fr")==0) {
  	 freezemode=true;
	}
  	else if(strcmp(argv[p],"-v")==0) {
  	 verbosemode=true;
	}
  	else if(strcmp(argv[p],"-e")==0) {
  	 echomode=true;
	}
  	else if(strcmp(argv[p],"-wt")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need a wait time follow -wt option.");
  	  waittime=atoi(argv[++p]);
	}  		
  	else if(strcmp(argv[p],"-thn")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need a thread number follow -wt option.");
  	  threadnum=atoi(argv[++p]);
	}  		
  	else if(strcmp(argv[p],"-osn")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need oracle service name .");
  	  strcpy(orasvcname,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-oun")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need oracle user name .");
  	  strcpy(orausrname,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-mh")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need MySQL host name or address .");
  	  strcpy(mhost,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-mun")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need MySQL user name .");
  	  strcpy(musrname,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-hmr")==0) {
  	  funcid=DT_HOMO_REINDEX;
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need database name and table name on homogeneous rebuild index(-hmr).");
	  if(p+2>=n || argv[p+2][0]=='-') ThrowWith("Need database name and table name on homogeneous rebuild index(-hmr).");
  	  strcpy(srcdbn,argv[++p]);
  	  strcpy(srctbn,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-mv")==0) {
	  funcid=DT_MOVE;
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need database name and table name of source and destination on move table(-mv).");
	  if(p+2>=n || argv[p+2][0]=='-') ThrowWith("Need database name and table name of source and destination on move table(-mv).");
	  if(p+3>=n || argv[p+3][0]=='-') ThrowWith("Need database name and table name of source and destination on move table(-mv).");
	  if(p+4>=n || argv[p+4][0]=='-') ThrowWith("Need database name and table name of source and destination on move table(-mv).");
  	  strcpy(srcdbn,argv[++p]);
  	  strcpy(srctbn,argv[++p]);
  	  strcpy(dstdbn,argv[++p]);
  	  strcpy(dsttbn,argv[++p]);
	}  		
  	else if(strcmp(argv[p],"-enc")==0) {
	  funcid=DT_ENCODE;
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("No text to encode!.");
  	  strcpy(srcdbn,argv[++p]);
	}  		
  	else if(strcmp(argv[p],"-rm")==0) {
	  funcid=DT_REMOVE;
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("Need database name and table name of source on remove table(-rm).");
	  if(p+2>=n || argv[p+2][0]=='-') ThrowWith("Need database name and table name of source on remove table(-rm).");
  	  strcpy(srcdbn,argv[++p]);
  	  strcpy(srctbn,argv[++p]);
	}  		
  	else ThrowWith("Invalid option :'%s'",argv[p]);
  }

};  	
int cmdparam::argc=0;
char **cmdparam::argv=(char **)NULL;
  	
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    if(argc==2 && (strcmp(argv[1],"-h")==0 || argv[1][0]=='h') ) {
    	printf("Help messages. \n\n %s",helpmsg);
    	return 0;
    }
    WOCIInit("dt_dstldr");
    cmdparam::argc=argc;
    cmdparam::argv=(char **)argv;
    nRetCode=wociMainEntrance(Start,true,NULL,0);
    WOCIQuit(); 
    return nRetCode;
}
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif

thread_rt LoopMonite(void *ptr) {
	bool *loopend=(bool *)ptr;
	printf("\n输入 'quit' 退出循环执行模式\n.");
	while(1)
	{
		char bf[1000];
		gets(bf);
		if(strcmp(bf,"quit")==0) {
			*loopend=true;
			printf("\n循环执行模式已退出，程序将在结束当前任务后终止.\n");
			break;
		}
		else if(strlen(bf)>0) {
			printf("\n命令'%s'不可识别，只能输入'quit'终止循环执行模式.\n",bf);
		}
	}
	thread_end; 
}

void DirFullAccessTest(AutoHandle &dts) {
	mytimer tm;
	int rn;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	/***************全表直接访问测试 */
	//dts.SetHandle(wociCreateSession("system","gcmanager","gctest"));
	tm.Start();
	TableScan ts(dts);
	ts.OpenTable(3);
	int mt;
	double trn=0;
	lgprintf("Table scan begin.");
	while(mt=ts.GetNextBlockMt())
	{
		trn+=wociGetMemtableRows(mt);
	}
	lgprintf("Total rows :%f",trn);
 	lgprintf("Test end.");
 	tm.Stop();
 	lgprintf("Consuming %7.3f second .",tm.GetTime());
}

void FullAccessTest() {
	 	/****************全表访问测试 */
	mytimer tm;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	AutoHandle dts;
	int rn;
	dts.SetHandle(wociCreateSession("system","gcmanager","gctest"));
	lgprintf("全表数据访问测试.");
	//lgprintf("Define TradeOffMt");
	tm.Start();
	//TradeOffMt dtmt(0,1500000);
	AutoMt dtmt(dts,1500000);
	//AutoStmt stmt(dts);
	//stmt.Prepare("select * from dest.tab_gsmvoicdr2 where rownum<10000001");
	//dtmt.Cur()->Build(stmt);
	//dtmt.Next()->Build(stmt);
	//dtmt.FetchFirst();
	dtmt.FetchFirst("select * from dest.tab_gsmvoicdr2@mysql11 where rownum<100001");
	rn=dtmt.Wait();
	while(rn>0) {
		lgprintf("%d rows fetched.  Fetch Next ...",rn);
		dtmt.FetchNext();
 	        rn=dtmt.Wait();
 	}
 	lgprintf("Test end.");
 	tm.Stop();
 	lgprintf("Consuming %7.3f second .",tm.GetTime());
}

void StatiTest() {
	/***************全表汇总测试 */
	mytimer tm;
	//wociSetEcho(TRUE);
	//wociSetOutputToConsole(TRUE);
	AutoHandle dts;
	int rn;
	dts.SetHandle(wociCreateSession("system","gcmanager","dtagt189"));
	lgprintf("test begin.");
	tm.Start();
 	//AutoMt tmt(dts,100);
	//tmt.FetchAll("select sum(basfee/1000) tbasfee,count(*) trn from tab_gsmvoicdr_7 ");//where part_id1='860'");
	//rn=tmt.Wait();
	//wociMTPrint(tmt,0,NULL);
	TestCompress(dts,"select * from tab_gsmvoicdr1 where rownum<4001",5000,"test.dat",5) ;
 	TestCompress(dts,"select * from tab_gsmvoicdr1 where rownum<4001",5000,"test.dat",1) ;
 	TestCompress(dts,"select * from tab_gsmvoicdr1 where rownum<4001",5000,"test.dat",10) ;
 	//TestCompress(dts,"select * from cdrusr.tab_gsmvoicdr1 where rownum<4001",5000,"d:\\dttemp\\test.dat",10) ;
	lgprintf("test end.");
 	tm.Stop();
 	lgprintf("Consuming %7.3f second .",tm.GetTime());
}


 

void UserQueryTest() {
	/***************全表汇总测试 */
	mytimer tm;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	AutoHandle dts;
	int rn;
	dts.SetHandle(wociCreateSession("system","gcmanager","dtagt189"));
	//dts.SetHandle(wociCreateSession("gctest","gctest","gctest"));;
	AutoHandle srcdt;
	srcdt.SetHandle(wociCreateSession("wanggsh","wanggsh","cuyn17"));
	AutoMt condmt(dts,3000000);
	condmt.FetchAll("select subscrbid,msisdn from dest.tab_gsmvoicdr3idx@dblk_cdrsvr where rownum<10000");
	int pos=condmt.GetPos("subscrbid",COLUMN_TYPE_INT);
	int condrn=condmt.Wait();
	lgprintf("test begin.");
	int subscrbid=0; 
 	//AutoMt tmt(srcdt,200000);
	AutoMt tmt(dts,200000);
	tm.Start();
	int trn=0;
	int vara[10000];
	char partid1[100000];
	wociSetEcho(FALSE);
	int i;
	for(i=0;i<2000;i++) {
		vara[i]=*condmt.PtrInt(pos,int(rand()*(double)condrn/RAND_MAX));
		subscrbid=vara[i];
		tmt.Clear();
		tmt.FetchAll(
" SELECT PART_ID1,BILLMONTH, CALLTYPE, VISAREACODE, TERMPHONE, OTHCODE, BEGINDATE,BEGINTIME ,"
" CALLTIMELEN,NVL(BASFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',NVL(TOLLFEE,0)/1000,0) + "
" DECODE(ROAMTYPE,'0',NVL(TOLLADDFEE,0)/1000,0),"
" NVL(DISCNTBASFEE,0)/1000 + "
" NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',0,NVL(TOLLFEE,0)/1000) + "
" DECODE(ROAMTYPE,'0',0,NVL(TOLLADDFEE,0)/1000),"
" 0,"
" NVL(BASFEE,0)/1000 + NVL(LOCADDFEE,0)/1000 + NVL(TOLLFEE,0)/1000 + NVL(TOLLADDFEE,0)/1000 + "
" NVL(DISCNTBASFEE,0)/1000 + NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000"
" FROM dest.tab_gsmvoicdr3@dblk_cdrsvr"
" WHERE SUBSCRBID =  %d"
" ORDER BY BEGINDATE, BEGINTIME",
			 subscrbid);
		trn+=tmt.Wait();
		strcpy(partid1+6*i,tmt.PtrStr("part_id1",0));
	}
 	tm.Stop();
	AutoMt tmt2(srcdt,200000);
	mytimer tm1;
	tm1.Start();
	int trn1=0;
	for(i=0;i<2000;i++) {
		tmt2.Clear();
		tmt2.FetchAll(
" SELECT PART_ID1,BILLMONTH, CALLTYPE, VISAREACODE, TERMPHONE, OTHCODE, BEGINDATE,BEGINTIME ,"
" CALLTIMELEN,NVL(BASFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',NVL(TOLLFEE,0)/1000,0) + "
" DECODE(ROAMTYPE,'0',NVL(TOLLADDFEE,0)/1000,0),"
" NVL(DISCNTBASFEE,0)/1000 + "
" NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',0,NVL(TOLLFEE,0)/1000) + "
" DECODE(ROAMTYPE,'0',0,NVL(TOLLADDFEE,0)/1000),"
" 0,"
" NVL(BASFEE,0)/1000 + NVL(LOCADDFEE,0)/1000 + NVL(TOLLFEE,0)/1000 + NVL(TOLLADDFEE,0)/1000 + "
" NVL(DISCNTBASFEE,0)/1000 + NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000"
" FROM cdrusr.tab_gsmvoicdr3"
" WHERE SUBSCRBID =  %d  and part_id1='%s'"
" ORDER BY BEGINDATE, BEGINTIME",
			 vara[i],partid1+6*i);
		//printf("partid1:%s.\n",partid1+6*i);
		trn1+=tmt2.Wait();
	}
	tm1.Stop();
	lgprintf("query %d users data.\ntotal rows fetched :%d,%d\ntest end.",i,trn,trn1);
 	lgprintf("Consuming %7.3f,%7.3f second .",tm.GetTime(),tm1.GetTime());
}

#define TESTTABNAME "tab_gsmvoicdr2"

void BatchUserQueryTest(const char *un,const char *pwd,const char *svn) {
#define DBCNUM 260
#define QUERYNUM 1600
#define RECONNUM 3
	/***************全表汇总测试 */
	mytimer tm;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	int ec=0;
	int *pyhbh=new int[100000];
	int condrn=0;
	{
	AutoHandle dts;
	dts.SetHandle(wociCreateSession(un,pwd,svn));
	
	AutoMt condmt(dts,100000);
	condmt.FetchAll("select distinct subscrbid,msisdn from " 
		"dest."TESTTABNAME"idx@dblk_cdrsvr where rownum<10000");
	condrn=condmt.Wait();
	memcpy(pyhbh,condmt.PtrInt("subscrbid",0),sizeof(int)*100000);
	}
	AutoHandle dtdst[DBCNUM];
	int i=0;
	static int id=0;
	int mid=id++;
	double utm[DBCNUM];
	int urn[DBCNUM];
	int unum[DBCNUM];
	lgprintf("%d:test begin,recon %d times,loop queries %d.",mid,RECONNUM,QUERYNUM);
	int subscrbid=0; 
	wociSetEcho(FALSE);
	int dbc=0;
	for(i=0;i<DBCNUM;i++) {
		utm[i]=0.0;urn[i]=0;
		unum[i]=0;
	} 
	for(int j=0;j<RECONNUM;j++) {
	lgprintf("%d:Build conns.",j+1);
	for(i=0;i<DBCNUM;i++) {
		dtdst[i].SetHandle(wociCreateSession(un,pwd,svn));
	}
	lgprintf("%d:%d conns builded.",j+1,DBCNUM);
	AutoMt tmt(dtdst[0],20000);
	 for(i=0;i<QUERYNUM;i++) {
		tm.Restart();
		tmt.SetDBC(dtdst[dbc]);
		int uoff=i;
		try{
		subscrbid=pyhbh[uoff];
		tmt.FetchAll( 
" SELECT PART_ID1,BILLMONTH, CALLTYPE, VISAREACODE, TERMPHONE, OTHCODE, BEGINDATE,BEGINTIME ,"
" CALLTIMELEN,NVL(BASFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',NVL(TOLLFEE,0)/1000,0) + "
" DECODE(ROAMTYPE,'0',NVL(TOLLADDFEE,0)/1000,0),"
" NVL(DISCNTBASFEE,0)/1000 + "
" NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000,"
" DECODE(ROAMTYPE,'0',0,NVL(TOLLFEE,0)/1000) + "
" DECODE(ROAMTYPE,'0',0,NVL(TOLLADDFEE,0)/1000),"
" 0,"
" NVL(BASFEE,0)/1000 + NVL(LOCADDFEE,0)/1000 + NVL(TOLLFEE,0)/1000 + NVL(TOLLADDFEE,0)/1000 + "
" NVL(DISCNTBASFEE,0)/1000 + NVL(DISCNTTOLLFEE,0)/1000 + NVL(DISCNTADDFEE,0)/1000 + NVL(DISCNTINFOFEE,0)/1000"
" FROM "
TESTTABNAME
" WHERE SUBSCRBID =  %d"
" ORDER BY BEGINDATE, BEGINTIME"
			,subscrbid);
		int lrn=tmt.Wait();
		urn[dbc]+=lrn;
		printf("%d: %d %d rows\n",i+1
			,subscrbid,lrn);
		if(lrn<1) {
			printf("return 0 rows on subscrib %d,i:%d,j:%d.\n",subscrbid,i,j);
		}
		}
		catch(...) {
			printf("*************************************************************\n"
				"error ocurs, connect:%d,dbc:%d,uoff:%d.\n",i,dbc,uoff);
			dbc--;
			ec++;
			delete []pyhbh;
			throw;
		}
 		tm.Stop();
		unum[dbc]++;
		utm[dbc]+=tm.GetTime();
		dbc=(++dbc)%DBCNUM;
	}
	dbc=0;
	}
	delete []pyhbh;
	lgprintf("Test result :");
	double ttm=0;
	int tusr=0,trn=0;
	for(i=0;i<DBCNUM;i++) {
		ttm+=utm[i];
		tusr+=unum[i];
		trn+=urn[i];
	}
	lgprintf("sumary : query %d users,%d records,in %7.3f sec,err num :%d.",
		tusr,trn,ttm,ec);
	getch();
}


int Start(void *ptr) { 
	int rp=0;
    wociSetOutputToConsole(TRUE);
    cmdparam cp;
    cp.GetEnv();
    cp.GetParam();
    if(!cp.checkvalid()) return -1;
    wociSetEcho(cp.verbosemode);
    wociSetOutputToConsole(cp.echomode);
    if(cp.funcid==cmdparam::DT_ENCODE) {
    	encode(cp.srcdbn);
    	return 1;
    } 
    lgprintf("连接到Oracle&MySQL...");
    if(cp.freezemode) {
    	char dtnow[19],dtstart[19];
    	int y=0,m=0,d=0,h=0,mi=0,s=0;
    	wociGetCurDateTime(dtnow);
    	printf("Input start year(%d) :",wociGetYear(dtnow));
    	scanf("%d",&y);
    	if(y==0) y=wociGetYear(dtnow);
    	if(y<2000 || y>2100) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	printf("Input start month(%d) :",wociGetMonth(dtnow));
    	if(m==0) m=wociGetMonth(dtnow);
    	scanf("%d",&m);
    	if(m<1 || m>12) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	printf("Input start day(%d) :",wociGetDay(dtnow));
    	scanf("%d",&d);
    	if(d==0) d=wociGetDay(dtnow);
    	if(d<1 || d>31) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	printf("Input start hour :");
    	scanf("%d",&h);
    	if(h<0 || h>53) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	printf("Input start minute :");
    	scanf("%d",&mi);
    	if(mi<0 || mi>59) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	printf("Input start sec(0) :");
    	scanf("%d",&s);
    	if(s<0 || s>59) {
    		printf("Invalid input.\n");
    		return -1;
    	}
    	wociGetCurDateTime(dtnow);
    	wociSetDateTime(dtstart,y,m,d,h,mi,s);
    	int sl=wociDateDiff(dtstart,dtnow);
    	if(sl<1) {
    		printf("Freeze %d seconds,continue(Y/N)?",sl);
    		int ans=getch();
    		if(ans!='Y' && ans!='y') return -1;
    	}
	else  {
		printf("I will sleep %d seconds...",sl);
		mySleep(sl);
		printf("Wake up!\n");
	}
    }
    	
    AutoHandle dts;
    dts.SetHandle(wociCreateSession(cp.orausrname,cp.orapswd,cp.orasvcname));
    DirFullAccessTest(dts);
	//UserQueryTest();
     //StatiTest();
    //FullAccessTest();
    //BatchUserQueryTest(cp.orausrname,cp.orapswd,cp.orasvcname);
    return 1;
    wociSetTraceFile("dtadmin");

	 
 	 SysAdmin sa (dts);
	 lgprintf("取DatabaseTurbo(C) 运行参数...");
	 sa.Reload();
	 lgprintf("连接到MySQL...");
	 MySQLConn conn;
	 conn.Connect(cp.mhost,cp.musrname,cp.mpswd);
	 /************************
	 SvrAdmin::CreateDTS("gctest","gctest","gctest");
	 SvrAdmin *psa=NULL;
	 while(!(psa=SvrAdmin::GetInstance())) ;
	 /********************/
	 SysAdmin *psa=&sa;
	 {
	 	//DestLoader dstl(&sa);
	 	//while(true) {
	 	//lgprintf("数据删除测试...");
		//dstl.RemoveTable("dest","tab_gsmvoicdr_7",conn);
		//dstl.MoveTable("dest","tab_gsmvoicdr_7","dest","tab_gsmvoicdr1",conn);
		//dstl.MoveTable("dest","tab_gsmvoicdr1","dest","tab_gsmvoicdr_7",conn);
		//return 1;
	 }
		/****Not loopable********************/
	 	if(cp.funcid==cmdparam::DT_REMOVE) {
	 		DestLoader dstl(&sa);
 			lgprintf("表删除...");
   			dstl.RemoveTable(cp.srcdbn,cp.srctbn,conn);
			return 0;
		}
	 	if(cp.funcid==cmdparam::DT_MOVE) {
	 		DestLoader dstl(&sa);
 			lgprintf("表数据移动...");
   			dstl.MoveTable(cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn,conn);
			return 0;
		}
	 	if(cp.funcid==cmdparam::DT_HOMO_REINDEX) {
			MiddleDataLoader dl(psa);
	 		printf("同构索引重建：'%s.%s' ...\n",cp.srcdbn,cp.srctbn);
	 		dl.homo_reindex(conn,cp.srcdbn,cp.srctbn);
			return 0;
		}
    bool loopend=false;
    if(cp.loopmode) {
#ifdef WIN32
    	_beginthread(LoopMonite,81920,(void *)&loopend);
#else
        pthread_create(&hd_pthread_loop,NULL,LoopMonite,(void *)&loopend);
        pthread_detach(hd_pthread_loop);
#endif
	}
	 while(true) {
	 /*****************************
	 lgprintf("检查目标表结构...");
	 int tabp,srctbp;
	 while(sa.GetBlankTable(tabp,srctbp)) {
	 	 sa.CreateDT(tabp,srctbp);
	 	 sa.Reload();
	 }
	 lgprintf("检查数据导出任务...");
	 DataDump dd(psa->GetDTS(),650000000,3000000);//650M mem,1M index row
	 dd.DoDump(*psa);
	 
	 /************/
	 /********* DestLoader ********/

	 	if(cp.funcid==cmdparam::DT_CHECK || cp.funcid==cmdparam::DT_ALL) {
	 		//while(true) {
				printf("检查目标表结构...\n");
	 			int tabid;
	 			while(sa.GetBlankTable(tabid)) {
	 	 			sa.CreateDT(conn,tabid);
	 	 			sa.Reload();
	 			}
	   		//	if(cp.loopmode && cp.funcid==cmdparam::DT_CHECK && !loopend) 
	   		//		mySleep(cp.waittime)
			//	else break;
			//}
		}

	 	if(cp.funcid==cmdparam::DT_DUMP) { // || cp.funcid==cmdparam::DT_ALL) {
			DataDump dd(psa->GetDTS(),cp.mtlimit,cp.indexnum);
	 		//while(true) {
	 			printf("检查数据导出任务...\n");
			 	dd.DoDump(*psa);
 		 	        wociSetEcho(cp.verbosemode);

	   		//	if(cp.loopmode && cp.funcid==cmdparam::DT_DUMP && !loopend) 
	   		//		mySleep(cp.waittime)
			//	else break;
			//}
		}
	 	if(cp.funcid==cmdparam::DT_MDLDR || cp.funcid==cmdparam::DT_ALL) {
			MiddleDataLoader dl(psa);
	 		//while(true) {
	 			printf("检查数据整理任务,最大索引行数%d...\n",cp.indexnum);
	 			dl.Load(conn,cp.indexnum);
	   		//	if(cp.loopmode && cp.funcid==cmdparam::DT_MDLDR && !loopend) 
	   		//		mySleep(cp.waittime)
			//	else break;
			//}
		}
	 	if(cp.funcid==cmdparam::DT_DSTLDR || cp.funcid==cmdparam::DT_ALL) {
	 		DestLoader dstl(&sa);
	 		//while(true) {
	 			printf("检查数据导入任务...\n");
	   			while(dstl.Load(conn));
	   			dstl.RecreateIndex(&sa,conn);
	   		//	if(cp.loopmode && cp.funcid==cmdparam::DT_DSTLDR && !loopend) 
	   		//		mySleep(cp.waittime)
			//	else break;
			//}
		}
	 	/***********************/
	 	if(cp.funcid==cmdparam::DT_DEEPCMP || cp.funcid==cmdparam::DT_ALL) {
	 		DestLoader dstl(&sa);
	 		//while(true) {
	 			printf("检查数据二次压缩任务...\n");
	   			dstl.ReCompress(cp.threadnum);
				dstl.ReLoad(conn);
	   			dstl.RecreateIndex(&sa,conn);
	   		//	if(cp.loopmode && cp.funcid==cmdparam::DT_DEEPCMP && !loopend) 
	   		//		mySleep(cp.waittime)
			//	else break;
			//}
		}
		if(cp.loopmode &&  !loopend ) /*cp.funcid==cmdparam::DT_ALL &&*/
	  		mySleep(cp.waittime)
		else break;
		if(loopend) break;
     }
	 //wait LoopMonite thread end.
	 mySleep(1)
	 SvrAdmin::ReleaseInstance();
	 SvrAdmin::ReleaseDTS();
	 /**********************/
	 lgprintf("正常结束");
	
	 return 0;
}

#define SetBit(bf,colid) bf[colid/8]|=(0x1<<(colid%8))
#define ResetBit(bf,colid) bf[colid/8]&=~(0x1<<(colid%8))
/*
void TestDateCvt() {
	char date[10];
	char varflag[10],nullflag[10];
	wociGetCurDateTime(date);
	int dtlen=0;
	char buf[1000];
	mytimer mtm;
	mtm.Start();
	for(int k=0;k<3000000;k++) {
	    char *src=date;
		
		if(*src==0) {
			SetBit(varflag,10);
			SetBit(nullflag,10);
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
				memcpy(buf+dtlen,&mdt,sizeof(longlong));
				dtlen=sizeof(longlong);
				ResetBit(varflag,10);
				ResetBit(nullflag,10);
			}
	}
		mtm.Stop();
		printf("tm:%6.3f",mtm.GetTime());
		int stophere=1;
}
*/
/*
void TestIntCvt() {
	mytimer mtm;
	mtm.Start();
	char buf1[100];
	bool filled=true;
	int clen=8;
	int val=12345678;
	strcpy(buf1,"1234567");
	for(int i=0;i<3000000;i++) {
		//itoa(12345678,buf1,10);
		//val=atoi(buf1);
		filled=false;
		int slen= strlen(buf1);
		if (slen > clen)
			continue;
		else
		{
			char *pdst=buf1;
			memmove(pdst+(clen-slen),pdst,slen);
			for (int j=clen-slen ; j-- > 0 ;)
				*pdst++ = ' ' ;
		}
	}
	mtm.Stop();
	printf("time :%6.3f",mtm.GetTime());
	int stophere=1;
}

void ShowDoubleByte() {
	double val[10]={
		0,
		1,
		123,
		12345,
		12.34,
		123.456,
		1234.5678,
		1234.56789,
		1234567.89,
		12.3456789};
		printf("\nvalues :\n");
		int i;
		for(i=0;i<10;i++) {
			printf("%f\n",val[i]);
		}
		printf("bytes order:\n");
		for(i=0;i<10;i++) {
			char *p=(char *)&val[i];
			for(int k=0;k<8;k++) {
				unsigned int v=(unsigned char )p[k];
				printf("%02x ",v);
			}
			printf("\n");
		}
}
*/

void TestCompressMT(int dts,char *sqlst,int maxrows,char *fn,int cmpflag) 
{
	mytimer mtm;
	int reptm=10;
	lgprintf("---------Test Compress ----------------");
	lgprintf("maxrows:%d,sql:'%s',cmpflag:%d,file:'%s',repeat %d times.",
	   maxrows,sqlst,cmpflag,fn,reptm);
	AutoMt mt(dts,maxrows);
	mt.FetchAll(sqlst);
	int rl=wociGetRowLen(mt);
	int rn=mt.Wait();
	
	lgprintf("End of fetch,get %d rows,row len:%d,tot bytes:%d",
		rn,rl,rn*rl);
	dt_file df;
	int fhl=0,fl=0;
	mtm.Clear();
	mtm.Start();
	int i;
	for(i=0;i<reptm;i++)
	{
	df.Open(fn,1);
	fhl=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
	fl=df.WriteMt(mt,cmpflag);
	}
	mtm.Stop();
	lgprintf("Write file,orig len:%d,cmp to :%d,hd len :%d,ratio %5.2f,%7.3f Sec.",rl*rn,fl-fhl,fhl,(double)(rl*rn)/(fl-fhl),mtm.GetTime());
	//lgprintf("End of file writing, compress to %d bytes,file len:%d,file header len :%d.",fl-fhl,fl,fhl);
	AutoMt mt1(0,0);
	mt1.SetHandle(df.CreateMt(maxrows));
	mtm.Clear();
	mtm.Start();
	
	for(i=0;i<reptm;i++)
	{
	df.Open(fn,0);
	df.ReadMt(0,0,mt1);
	}	
	mtm.Stop();
	lgprintf("Read file,%7.3f Sec.",mtm.GetTime());
	//lgprintf("End of read file .");
	return;
}

void TestCompress(int dts,char *sqlst,int maxrows,char *fn,int cmpflag) 
{
	mytimer mtm;
	int reptm=10;
	if(cmpflag!=10) reptm=100;
	lgprintf("---------Test Compress ----------------");
	lgprintf("maxrows:%d,sql:'%s',cmpflag:%d,file:'%s',repeat %d times.",
	   maxrows,sqlst,cmpflag,fn,reptm);
	AutoMt mt(dts,maxrows);
	mt.FetchAll(sqlst);
	int rl=wociGetRowLen(mt);
	int rn=mt.Wait();
	int len=rn*rl;
	lgprintf("End of fetch,get %d rows,row len:%d,tot bytes:%d",
		rn,rl,rn*rl);
	dt_file df;
	char *psrc,*pdst;
	psrc=new char [len+1000];
	wociExportSomeRows(mt,psrc,0,rn);
	pdst=new char [len+1000];
	int fhl=0,fl=0;
	mtm.Clear();
	mtm.Start();
	int i;
	char *pwrkmem=NULL;
	int len2=len;
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len2;
			if(cmpflag==10) {
				unsigned int dstlen2=len2;
				rt=BZ2_bzBuffToBuffCompress(pdst,&dstlen2,psrc,len,1,0,0);
				dstlen=dstlen2;
			}			
			/******* lzo compress ****/
			else if(cmpflag==5) {
				if(!pwrkmem)  {
					pwrkmem = 
					new char[LZO1X_MEM_COMPRESS+204800];
                                        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+204800);
                                }
				  rt=lzo1x_1_compress((const unsigned char*)psrc,len,(unsigned char *)pdst,(unsigned int *)&dstlen,pwrkmem);
			}
			/*****/
		    /*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=compress2((Bytef *)pdst,&dstlen,(Bytef *)psrc,len,1);
			}
			else 
				ThrowWith("Invalid compress flag %d",compress);
		    if(rt!=Z_OK) {
				ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
			}
			fhl=0;
			fl=dstlen;
	}
	mtm.Stop();
	lgprintf("Write file,orig len:%d,cmp to :%d,hd len :%d,ratio %5.2f,%7.3f Sec,%7.1fK/s.",rl*rn,fl-fhl,fhl,(double)(rl*rn)/(fl-fhl),mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of file writing, compress to %d bytes,file len:%d,file header len :%d.",fl-fhl,fl,fhl);
	mtm.Clear();
	mtm.Start();
	
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len;
			/****************************/
			//bzlib2
			if(cmpflag==10) {
				unsigned int dstlen2=dstlen;
				rt=BZ2_bzBuffToBuffDecompress(psrc,&dstlen2,pdst,fl,0,0);
				dstlen=dstlen2;
			}			
			/*****/

			/******* lzo compress ****/
			else if(cmpflag==5) {
				unsigned int dstlen2=dstlen;
				rt=lzo1x_decompress((unsigned char*)pdst,fl,(unsigned char *)psrc,&dstlen2,NULL);
				dstlen=dstlen2;
			}
			/*****/
		    /*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=uncompress((Bytef *)psrc,&dstlen,(Bytef *)pdst,fl);
			}
			else 
				ThrowWith("Invalid uncompress flag %d",cmpflag);
		    if(rt!=Z_OK) {
				ThrowWith("Decompress failed,datalen:%d,compress flag%d,errcode:%d",
					fl,cmpflag,rt);
			}
	}	
	mtm.Stop();
	lgprintf("Read file,%7.3f Sec,%7.1fK/s.",mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of read file .");
	return;
}
	
