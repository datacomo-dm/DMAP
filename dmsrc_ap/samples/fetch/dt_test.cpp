#include "dt_lib.h"
#include <lzo1x.h>
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

#ifdef WIN32
#define mySleep(a) {if(a>10) printf("%d秒后运行。\n",a); Sleep(a*1000);}
#else
#define mySleep(a) {if(a>10) printf("%d秒后运行。\n",a);sleep(a);}
void freeinfo(const char *hdinfo) {
struct mallinfo mi=mallinfo();
lgprintf("free info -%s--%d",hdinfo,mi.uordblks);
//\nsmall- used:%d free:%d"
//	 "\nordin- used:%d free:%d",mi.usmblks,mi.fsmblks,mi.uordblks,mi.fordblks);
}
#endif
const char *helpmsg=
  "Options : \n"
  "-f {check,dump,mload,dload,all} function code\n"
  "-mpwd [MySQL password] \\nn"
  "-opwd [Oracle password] "
  "-lp 	//loop mode \n"
  "-v	//verbose mode \n"
  "-e	//echo mode \n"
  "-wt (wait time seconds) \n"
  "-osn (oracle service name) \n"
  "-oun (oracle username) \n"
  "-mh  (MySQL host name or address) \n"
  "-mun (MySQL username) \n"
  "\n---------------------------------------------------------\n"
  "Macros : ( Env var)\n"
  "DT_INDEXNUM \n"
  "DT_MTLIMIT \n"
  "DT_WAITTIME \n"
  "DT_ECHO \n"
  "DT_VERBOSE \n"
  "DT_LOOP \n"
  "DT_FUNCODE {CHECK DUMP MLOAD DLOAD ALL} \n"
  "DT_MPASSWORD  \n"
  "DT_MUSERNAME \n"
  "DT_MHOST \n"
  "DT_ORAPASSWORD \n"
  "DT_ORAUSERNAME \n"
  "DT_ORASVCNAME \n"
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
  //char lgfname[PARAMLEN];
  enum {
  	DT_NULL=0,DT_CHECK,DT_DUMP,DT_MDLDR,DT_DSTLDR,DT_ALL
  } funcid;
  
  bool loopmode,verbosemode,echomode;
  int waittime;
  int indexnum;
  int mtlimit;
  cmdparam() {
  	memset(this,0,sizeof(cmdparam));
  }
  
  void GetParam() {
  	for(int i=1;i<argc;i++) 
  	 ExtractParam(i,argc,argv);
  }
  

  void GetEnv() {
  	SetValue(orasvcname,getenv("DT_ORASVCNAME"));
  	SetValue(orausrname,getenv("DT_ORAUSERNAME"));
  	SetValue(orapswd,getenv("DT_ORAPASSWORD"));
  	SetValue(mhost,getenv("DT_MHOST"));
  	SetValue(musrname,getenv("DT_MUSERNAME"));
  	SetValue(mpswd,getenv("DT_MPASSWORD"));
  	const char *fncode=getenv("DT_FUNCODE");
  	//set DT_FUNCODE or -f(-f check,-f dump,-f mload,-f dload,-f all) parameter.
  	if(fncode==NULL) 
  	  funcid=DT_ALL;
  	else if(strcmp(fncode,"CHECK")==0) funcid=DT_CHECK;
  	else if(strcmp(fncode,"DUMP")==0) funcid=DT_DUMP;
  	else if(strcmp(fncode,"MLOAD")==0) funcid=DT_MDLDR;
  	else if(strcmp(fncode,"DLOAD")==0) funcid=DT_DSTLDR;
  	else if(strcmp(fncode,"ALL")==0) funcid=DT_ALL;
  	else funcid=DT_ALL;
  	SetValue(loopmode,getenv("DT_LOOP"));
  	SetValue(verbosemode,getenv("DT_VERBOSE"));
	SetValue(echomode,getenv("DT_ECHO"));
  	SetValue(waittime,getenv("DT_WAITTIME"));
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
  	if(funcid==DT_NULL || funcid>DT_ALL) {
//  		"Invalid function code '%s',use one of 'check,dump,mload,dload,all'",argv[p]);
  	 printf("Invalid function selection code ,set DT_FUNCODE or -f (check,dump,mload,dload,all) option..\n");
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
  	return ret;
  }
  
  void ExtractParam(int &p,int n,char **argv) {
  	if(strcmp(argv[p],"-f")==0) {
  	  if(p+1>=n) ThrowWith("Need function code,use one of 'check,dump,mload,dload,all'");
  	  p++;
  	  if(strcmp(argv[p],"check")==0) funcid=DT_CHECK;
  	  else if(strcmp(argv[p],"dump")==0) funcid=DT_DUMP;
  	  else if(strcmp(argv[p],"mload")==0) funcid=DT_MDLDR;
  	  else if(strcmp(argv[p],"dload")==0) funcid=DT_DSTLDR;
  	  else if(strcmp(argv[p],"all")==0) funcid=DT_ALL;
  	  else ThrowWith("Invalid function code '%s',use one of 'check,dump,mload,dload,all'",argv[p]);
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
    nRetCode=wociMainEntrance(Start,true,NULL);
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

int Start(void *ptr) { 
	int rp=0;
    wociSetOutputToConsole(TRUE);
    cmdparam cp;
    cp.GetEnv();
    cp.GetParam();
    if(!cp.checkvalid()) return -1;
    wociSetEcho(cp.verbosemode);
    wociSetOutputToConsole(cp.echomode);
    lgprintf("连接到Oracle&MySQL...");
    AutoHandle dts;

	 {
	 	dts.SetHandle(wociCreateSession("wanggsh","wanggsh","cuyn17"));
 		lzo_init();

	 	TestCompress(dts,"select * from cdrusr.tab_gsmvoicdr1 where rownum<4001",5000,"/dds/dtdata/dttemp/test.dat",5) ;
	 	TestCompress(dts,"select * from cdrusr.tab_gsmvoicdr1 where rownum<4001",5000,"/dds/dtdata/dttemp/test.dat",1) ;
	 	TestCompress(dts,"select * from cdrusr.tab_gsmvoicdr1 where rownum<4001",5000,"/dds/dtdata/dttemp/test.dat",10) ;
	 	TestCompress(dts,"select * from cdrusr.tab_gsmvoicdr1 where rownum<4001",5000,"/dds/dtdata/dttemp/test.dat",0) ;
	 	return 1;
	 	lgprintf("Define TradeOffMt");
		TradeOffMt dtmt(0,1500000);
		//AutoMt dtmt(dts,500000);
		//AutoMt dtmt(dts,30);
		AutoStmt stmt(dts);
		stmt.Prepare("select * from cdrusr.tab_gsmvoicdr2 ");
		//dtmt.FetchFirst("select * from cdrusr.tab_gsmvoicdr2 ");
		dtmt.Cur()->Build(stmt);
		dtmt.Next()->Build(stmt);
		//dtmt.Build(stmt);
		//lgprintf("memory used, mt1 :%d,mt2:%d",wociGetMemUsed(*dtmt.Cur()),
		//   wociGetMemUsed(*dtmt.Next()));
		//准备数据索引表插入变量数组
		lgprintf("fetch first...");
		freeinfo("Fetch first" );
		dtmt.FetchFirst();
		int rn=dtmt.Wait();
		freeinfo("after first wait");
		while(rn>0) {
			lgprintf("%d rows fetched.  Fetch Next ...",rn);
			dtmt.FetchNext();
	 	        rn=dtmt.Wait();
	 	        freeinfo("After wait");
	 	}
	 	return 1;
	}
	
	 dts.SetHandle(wociCreateSession(cp.orausrname,cp.orapswd,cp.orasvcname));
	 
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
    bool loopend=false;
    if(cp.loopmode) {
#ifdef WIN32
    	_beginthread(LoopMonite,81920,(void *)&loopend);
#else
        pthread_create(&hd_pthread_loop,NULL,LoopMonite,(void *)&loopend);
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
	 		while(true) {
				lgprintf("检查目标表结构...");
	 			int tabp,srctbp;
	 			while(sa.GetBlankTable(tabp,srctbp)) {
	 	 			sa.CreateDT(tabp,srctbp);
	 	 			sa.Reload();
	 			}
	   			if(cp.loopmode && cp.funcid==cmdparam::DT_CHECK && !loopend) 
	   				mySleep(cp.waittime)
				else break;
			}
		}

	 	if(cp.funcid==cmdparam::DT_DUMP || cp.funcid==cmdparam::DT_ALL) {
			DataDump dd(psa->GetDTS(),cp.mtlimit,cp.indexnum);
	 		while(true) {
	 			lgprintf("检查数据导出任务...");
			 	dd.DoDump(*psa);
	   			if(cp.loopmode && cp.funcid==cmdparam::DT_DUMP && !loopend) 
	   				mySleep(cp.waittime)
				else break;
			}
		}
	 	if(cp.funcid==cmdparam::DT_MDLDR || cp.funcid==cmdparam::DT_ALL) {
			MiddleDataLoader dl(psa);
	 		while(true) {
	 			lgprintf("检查数据整理任务...");
	 			while(dl.Load(conn,cp.indexnum));
	   			if(cp.loopmode && cp.funcid==cmdparam::DT_MDLDR && !loopend) 
	   				mySleep(cp.waittime)
				else break;
			}
		}
	 	if(cp.funcid==cmdparam::DT_DSTLDR || cp.funcid==cmdparam::DT_ALL) {
	 		DestLoader dstl(&sa);
	 		while(true) {
	 			lgprintf("检查数据导入任务...");
	   			dstl.Load();
	   			dstl.RecreateIndex(&sa,conn);
	   			if(cp.loopmode && cp.funcid==cmdparam::DT_DSTLDR && !loopend) 
	   				mySleep(cp.waittime)
				else break;
			}
		}
	 	/***********************/
		if(cp.loopmode && cp.funcid==cmdparam::DT_ALL && !loopend ) 
	  		mySleep(cp.waittime)
		else break;
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

void TestCompress(int dts,char *sqlst,int maxrows,char *fn,int cmpflag) 
{
	mytimer mtm;
	lgprintf("---------Test Compress ----------------");
	lgprintf("maxrows:%d,sql:'%s',cmpflag:%d,file:'%s'.",
	   maxrows,sqlst,cmpflag,fn);
	AutoMt mt(dts,maxrows);
	mt.FetchAll(sqlst);
	int rl=wociGetRowLen(mt);
	int rn=mt.Wait();
	
	char *blockbf=new char[rl*rn+10];
	wociExportSomeRows(mt,blockbf,0,wociGetMemtableRows(mt));
	char *cmpbf=new char[rl*rn];
	char *pwrkmem=new char[LZO1X_999_MEM_COMPRESS];
	unsigned int dstlen=rl*rn;
	unsigned int len=rl*rn;
	FILE *fp=fopen("mtdata.tst","w+b");
	fwrite(blockbf,1,len,fp);
	fclose(fp);
	//long to int converting mayby error on BIGENDIEN diferrent host.
	//rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,(unsigned int *)&dstlen,pwrkmem);
	//for(int k=0;k<100;k++)
	int rt=0;
	mtm.Restart();
	 for(int i=0;i<100;i++)
	 rt=lzo1x_1_compress((const unsigned char*)blockbf,len,(unsigned char *)cmpbf,(unsigned int *)&dstlen,pwrkmem);
	 mtm.Stop();
	 lgprintf("comp tm :%7.4f",mtm.GetTime());
	unsigned int dstlen2=dstlen;
	mtm.Restart();
	 for(int i=0;i<100;i++)
		rt=lzo1x_decompress((unsigned char*)cmpbf,dstlen,(unsigned char *)blockbf,&dstlen2,NULL);
	 mtm.Stop();
	 lgprintf("decomp tm :%7.4f",mtm.GetTime());
	lgprintf("lzo cmp to %d ,decomp to %d.",dstlen,dstlen2);
	delete []blockbf;
	delete []cmpbf;
	delete []pwrkmem;
	
	lgprintf("End of fetch,get %d rows,row len:%d,tot bytes:%d",
		rn,rl,rn*rl);
	dt_file df;
	int fhl=0,fl=0;
	mtm.Restart();
	int reptm=10;
	for(int i=0;i<reptm;i++)
	{
	df.Open(fn,1);
	fhl=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
	fl=df.WriteMt(mt,cmpflag);
	}
	mtm.Stop();
	lgprintf("Write file,orig len:%d,cmp to : %d,hd len :%d,ratio %5.2f,%7.3f Sec.",rl*rn,fl-fhl,fhl,(double)(rl*rn)/(fl-fhl),mtm.GetTime());
	//lgprintf("End of file writing, compress to %d bytes,file len:%d,file header len :%d.",fl-fhl,fl,fhl);
	AutoMt mt1(0,0);
	mt1.SetHandle(df.CreateMt(maxrows));
	mtm.Restart();
	for(int i=0;i<reptm;i++)
	{
	df.Open(fn,0);
	df.ReadMt(0,0,mt1);
	}	
	mtm.Stop();
	lgprintf("Read file,%7.3f Sec.",mtm.GetTime());
	//lgprintf("End of read file .");
	return;
}
	
