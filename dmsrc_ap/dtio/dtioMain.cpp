#include "dtio.h"
#include "dt_common.h"
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#ifdef WIN32
#include <conio.h>
#endif

#define MAX_TABLES 1000
const char *helpmsg=
  " dpio是数据备份恢复工具，有两种运行模式：交互方式和命令方式。\n"
  " \n"
  "参数 : \n"
  " \n"
  " 交互方式 在dpio命令之后直接输入功能名称，进入交互式界面。功能名称：\n"
  "pack  : 数据备份 \n"
  "unpack : 数据完整恢复 \n"
  "listpack : 列出已备份文件的详细内容\n"
  "restore : 快速恢复 \n"
  "bindpack : 参数文件生成 \n"
  " \n"
  "-at 目标表完整列表\n"
  "    缺省只列出未备份或上次备份后修改过的表。\n"
  " \n"
  " \n"
  " 命令方式在输入命令时指定完整的参数: \n"
  "-f {pack,restore,unpack,list,rebuild} \n"
  "    可以选择其中的一个: \n"
  "    pack: <数据库名>  <表名> <备份文件名> \n"
  "        数据备份 \n"
  "    restore: <数据库名>  <表名> <新的表名> <备份文件名> \n"
  "        快速恢复(直接到备份文件中访问,仅占用极少的恢复空间) \n"
  "        恢复到数据名下的新表名上，新表名可以和备份时的名称相同 \n"
  "        恢复前确保新表名在数据库中不存在 \n"
  "        注意：快速恢复直接到备份文件中查询，不能删除或更名备份文件 \n"
  "              如果是共享存储方式，请确保恢复时的备份文件在" DBPLUS_STR "运行的主机\n"
  "              上可以访问并且具有相同的路径名称。\n"
  "    unpack: <数据库名>  <表名> <备份文件名> \n"
  "        数据恢复(完整恢复,需要预留恢复空间) \n"
  "    list: <备份文件名> [<数据库名> <表名> [log] ]\n "
  "        列出备份文件或者参数文件(.DTP)中的内容 \n" 
  "	   可选参数： <数据库名> <表名> \n"
  "          列出指定表的存储明细。 \n"
  "          未指定表时仅列出全部表和数据文件的目录。\n"
  "        可选参数：log \n"
  "          同时列出指定表的日志数据。 \n"
  "    rebuild: <数据库名>  <表名> \n"
  "        重建目标表的基本参数文件(.DTP文件) \n"
  "\n "
  "\n "
  "下面的选项对两种运行模式均有效\n "
  "-dict {sys,dp,all} 数据字典选项\n"
  "    可以选择其中的一个: \n"
  "    sys: 含mysql系统表 \n"
  "    dp: 含dp参数表 \n"
  "    all:含mysql系统表和dp参数表\n"
  "     这个选项控制备份时是否包含系统表和参数表.\n"
  "     如果要恢复系统表或参数表，必须使用dpunpack命令.\n"
  "-basepath [path] \n"
  "    数据库的基本路径(数据路径).\n"
  "    缺省从参数表查询 \n"
  "-datapath [path] \n"
  "    完整恢复时的数据文件存放路径.\n"
  "    缺省为备份时的路径\n"
  "-s <50-20000> \n"
  "    单个文件的最大长度,缺省是4500,单位MB\n"
  "-ai	\n"
  "    备份全部索引数据.\n"
  "    缺省只备份主索引 \n"
  "-mh  (dsn)\n"
  "    到本地" DBPLUS_STR " Server 的ODBC 连接名 \n"
  "-mun (username) \n"
  "    到本地" DBPLUS_STR " Server 的用户名\n"
  "-mpwd [password] \n"
  "    到本地 " DBPLUS_STR " Server 的连接密码\n"
  "-v	\n"
  "    详细输出模式.\n"
  "-nv	\n"
  "    取消详细输出模式.\n"
  "-e	\n"
  "   允许控制台输出执行信息.\n"
  "-ne	\n"
  "   取消允许控制台输出执行信息.\n"
  "-h \n"
  "    帮助信息 \n"
  "\n---------------------------------------------------------\n"
  "使用以下环境变量: \n"
  "DP_SERVERIP \n"
  "   服务器的IP地址 \n"
  "DP_MPASSWORD  \n"
  "    到本地" DBPLUS_STR " Server 的连接密码\n"
  "    要设置为加密后的密码\n"
  "    使用参数 -enc <密码明文> 加密\n"
  "DP_MUSERNAME \n"
  "    到本地" DBPLUS_STR " Server 的用户名\n"
  "DP_DSN \n"
  "    到本地" DBPLUS_STR " Server 的ODBC 连接名 \n"
  "DP_ECHO \n"
  "   控制台输出执行信息.1:打开,0:关闭\n"
  "DP_VERBOSE \n"
  "    详细输出模式.1:打开,0:关闭\n"
  ;

struct   cmdparam:public cmd_base {
  char basedir[PARAMLEN];
  char dbname[PARAMLEN];
  char tabname[PARAMLEN];
  char newtabname[PARAMLEN];
  char filename[PARAMLEN];
  char datadir[PARAMLEN];
  bool all_index;
  int splitlen;
  bool all_tables;
  bool withlog;
  enum {
  	INC_NULL=0,INC_SYS,INC_DP,INC_ALL,INC_LAST
  } inc;
  enum {
  	DP_NULL=0,DP_PACK,DP_RESTORE,DP_UNPACK,DP_LIST,DP_REBUILD,DP_LAST
  } funcid;
  
  cmdparam():cmd_base() {
  	splitlen=4500;
  	inc=INC_NULL;
  	funcid=DP_NULL;
  	basedir[0]=dbname[0]=tabname[0]=newtabname[0]=filename[0]=datadir[0]=0;
  	all_index=false;
  	all_tables=false;
  }
  
  void GetEnv() {
        cmd_base::GetEnv();
  	funcid=DP_NULL;
  }
  
  bool checkvalid() {
  	bool ret=cmd_base::checkvalid();
  	if(funcid==DP_NULL || funcid>=DP_LAST) {
  	 printf("错误的功能代码,请使用-h参数查看帮助..\n");
  	  ret=false;
  	}
  	if(inc<INC_NULL || inc>=INC_LAST) {
  	 printf("错误的包含代码,请使用-h参数查看帮助..\n");
  	  ret=false;
  	}
  	if(splitlen>20000 || splitlen<50) {
  	 printf("错误的文件大小%d,必须是50-20000..\n",splitlen);
  	 ret=false;
  	}
  	return ret;
  }
 
  bool ExtractParam(int &p,int n,char **argv) {
  	if(cmd_base::ExtractParam(p,n,argv)) return true;
  	if(strcmp(argv[p],"-f")==0) {
  	  if(p+1>=n) ThrowWith("缺少功能代码,使用'pack,restore,unpack,list,rebuild'之一");
  	     p++;
  	  if(strcmp(argv[p],"pack")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("备份需要指定数据库名.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("备份需要指定表名.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("备份需要指定文件名.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
	    funcid=DP_PACK;
  	  }
  	  else if(strcmp(argv[p],"restore")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("快速恢复需要指定数据库名.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("快速恢复需要指定表名.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("快速恢复需要指定恢复的表名.");
	    if(p+4>=n || argv[p+4][0]=='-') ThrowWith("快速恢复需要指定文件名.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(newtabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
  	  	funcid=DP_RESTORE;
  	  }
  	  else if(strcmp(argv[p],"unpack")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("恢复需要指定数据库名.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("恢复需要指定表名.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("恢复需要指定文件名.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
  	  	funcid=DP_UNPACK;
  	  }
  	  else if(strcmp(argv[p],"list")==0) {
	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("list需要指定文件名.");
  	     strcpy(filename,argv[++p]);
  	    if(p+2<n) {
  	     strcpy(dbname,argv[++p]);
  	     strcpy(tabname,argv[++p]);
  	     if(p+1<n && strcmp(argv[++p],"log")==0)
  	     	withlog=true;
  	     else withlog=false;
  	    }
  	    else dbname[0]=tabname[0]=0;
  	    funcid=DP_LIST;
  	  }
  	  else if(strcmp(argv[p],"rebuild")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("rebuild需要指定数据库名.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("rebuild需要指定表名.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    funcid=DP_REBUILD;
  	  }
  	  else ThrowWith("错误的功能代码 '%s',使用'pack,restore,unpack,list,rebuild'之一",argv[p]);
  	}
  	else if(strcmp(argv[p],"-dict")==0) {
  	  if(p+1>=n) ThrowWith("缺少包含代码,使用'sys,dp,all'之一");
  	     p++;
  	  if(strcmp(argv[p],"sys")==0) inc=INC_SYS;
  	  else if(strcmp(argv[p],"dp")==0) inc=INC_DP;
  	  else if(strcmp(argv[p],"all")==0) inc=INC_ALL;
  	  else ThrowWith("错误的包含代码 '%s',使用'sys,dp,all'之一",argv[p]);
  	}
  	else if(strcmp(argv[p],"-ai")==0) {
  	 all_index=true;
	}
  	else if(strcmp(argv[p],"-at")==0) {
  	 all_tables=true;
	}
  	else if(strcmp(argv[p],"-basepath")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith(" -basepath 选项需要指定路径.");
  	  strcpy(basedir,argv[++p]);
	}
  	else if(strcmp(argv[p],"-datapath")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith(" -datapath 选项需要指定路径.");
  	  strcpy(datadir,argv[++p]);
	}
  	else if(strcmp(argv[p],"-s")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-s 选项需要指定文件大小(MB).");
  	  splitlen=atoi(argv[++p]);
	}  		
	
  	else ThrowWith("错误的命令参数 :'%s'",argv[p]);
	return true;
  }

};  	
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;


int Start(void *p);
enum RUNMODE {
 DTIO_NULL=0,DTIO_PACK=10,DTIO_UNPACK,DTIO_LIST,DTIO_BIND ,DTIO_DETACH };
RUNMODE runmode;
bool alltables=false;
int main(int argc,char **argv) {
    
    int nRetCode = 0;
    if(argc<2 || (argc==2 && (strcmp(argv[1],"-h")==0 ||  strcmp(argv[1],"--h")==0 || 
         strcmp(argv[1],"--help")==0 || strcmp(argv[1],"help")==0 || strcmp(argv[1],"-help")==0) ) ){
    	printf("帮助信息. \n\n %s",helpmsg);
    	return 0;
    }
   if(argc==3 && strcmp(argv[1],"pack")==0 && strcmp(argv[2],"-at")==0)  {
   	runmode=DTIO_PACK;alltables=true;
   }
   else if(argc==2) {
     if(strcmp(argv[1],"pack")==0) runmode=DTIO_PACK;
     else if(strcmp(argv[1],"unpack")==0) runmode=DTIO_UNPACK;
     else if(strcmp(argv[1],"listpack")==0) runmode=DTIO_LIST;
     else if(strcmp(argv[1],"bindpack")==0) runmode=DTIO_BIND;
     else if(strcmp(argv[1],"restore")==0) runmode=DTIO_DETACH;
   }
   else runmode=DTIO_NULL;
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    WOCIInit("dp_pack");
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    nRetCode=wociMainEntrance(Start,true,NULL,corelev);
    WOCIQuit(); 
    printf("\n");
    return nRetCode;
}
const char *GetDPLibVersion();
cmdparam *pcp=NULL;
int Start(void *p) {
   wociSetOutputToConsole(TRUE);
   DbplusCert::initInstance();
   DbplusCert::getInstance()->printlogo();
    printf(DBPLUS_STR " 基础库版本 :%s \n",GetDPLibVersion());
   cmdparam cp;
   cp.all_tables=alltables;
   pcp=&cp;
   cp.GetEnv();
   wociSetEcho(cp.verbosemode);
   wociSetOutputToConsole(cp.echomode);
   dtioMain dm;
   char date[20];
  	wociGetCurDateTime(date);
  	int y,m,d;
        DbplusCert::getInstance()->GetExpiredDate(y,m,d);
        if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m) 
  	  ThrowWith("您用的" DBPLUS_STR "版本太老，请更新后使用(Your " DBPLUS_STR " is too old,please update it)!");
   switch(runmode) {
   	case DTIO_PACK:
   	 dm.BackupPrepare();
   	 dm.Backup();
   	 break;
   	case DTIO_UNPACK:
   	 dm.RestorePrepare();
   	 dm.PreviousRestore();
   	 break;
   	case DTIO_LIST:
   	 dm.List();
   	 break;
   	case DTIO_BIND:
   	 dm.BindPrepare();
   	 dm.PreviousBind();
   	 break;
   	case DTIO_DETACH:
   	 dm.DetachPrepare();
   	 dm.PreviousDetach();
   	 break;
   	case DTIO_NULL:
   	{
          cp.GetParam();
	   if(!cp.checkvalid()) return -1;
   	  dm.RunByParam(&cp);
   	}
   	 break;
   	default:
   	 printf("不支持的运行模式.\n");
   }
   DbplusCert::ReleaseInstance();
   return 0;
}

void checkPathEnd(char *basedir)
{
	 if(basedir[strlen(basedir)-1]!=PATH_SEPCHAR)
	  strcat(basedir,PATH_SEP);
}

void getBasePath(AutoMt &mt,char *basedir) {
	 mt.FetchAll("select * from dp.dp_path where pathtype='msys'");
	 if(mt.Wait()!=1) ThrowWith("查找基本目录失败，请检查dp_path表和同义词定义.");
	 strcpy(basedir,mt.PtrStr("pathval",0));
	 checkPathEnd(basedir);
	}

void dtioMain::RunByParam(void *_cp)
{
	cmdparam *cp=(cmdparam *)_cp;
	if(cp->funcid!=cmdparam::DP_LIST) {
	 dts.SetHandle(wociCreateSession(cp->musrname,cp->mpswd,cp->mhost,DTDBTYPE_ODBC));
	 conn.Connect(cp->serverip,cp->musrname,cp->mpswd,NULL,cp->port);
	 myparam=dtparam=false;
	 if(cp->inc==cmdparam::INC_SYS || cp->inc==cmdparam::INC_ALL) 
	 	myparam=true;
	 else if(cp->inc==cmdparam::INC_DP || cp->inc==cmdparam::INC_ALL) 
	 	dtparam=true;
	 psoledidxmode=!cp->all_index;
	 AutoMt mt(dts);
	  if(strlen(cp->basedir)>0) {
	 	strcpy(basedir,cp->basedir);
	 	checkPathEnd(basedir);
	  }
	  else getBasePath(mt,basedir);
	}
	switch(cp->funcid) {
	case cmdparam::DP_PACK:
		dtdata=true;
		DEFAULT_SPLITLEN=(int8B)cp->splitlen*1024*1000;
		strcpy(streamPath,cp->filename);
		Backup(cp->dbname,cp->tabname);
		break;
	case cmdparam::DP_UNPACK:
	 	myparam=dtparam=false;
		dtdata=true;
		strcpy(streamPath,cp->filename);
		PreviousRestore(cp->dbname,cp->tabname,cp->datadir[0]!=0?cp->datadir:NULL);
		break;
	case cmdparam::DP_REBUILD:
		CreateBind(cp->dbname,cp->tabname);
		break;
	case cmdparam::DP_LIST:
		List(cp->filename,cp->withlog,cp->dbname[0]==0?NULL:cp->dbname,cp->tabname[0]==0?NULL:cp->tabname);
		break;
	case cmdparam::DP_RESTORE:
	 	myparam=dtparam=false;
		dtdata=true;
		strcpy(streamPath,cp->filename);
		PreviousDetach(cp->dbname,cp->tabname,cp->newtabname);
		break;
	}
}

dtioMain::dtioMain() {
	myparam=dtparam=false;configfiles=psoledidxmode=false;
	basedir[0]=streamPath[0]=destdir[0]=0;pdtio=NULL;
}

dtioMain::~dtioMain() {
	if(pdtio!=NULL) delete pdtio;
}

void dtioMain::BindPrepare() {
	printf(DBPLUS_STR "参数文件生成 \n"
		" 使用 " DBPLUS_STR "参数文件生成程序,可以把某个" DBPLUS_STR "表的参数存储到文件中。\n"
	    " 注意:  本程序必须在目标" DBPLUS_STR " 数据库主机上运行！\n");
	char un[30],pwd[80],sn[100];
	//printf("连接到DBPlus数据库，请确保连接到本机DBPlus实例.\n");
	//getdbcstr(un,pwd,sn,"用户名");
  dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);
  //dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
        getBasePath(mt,basedir);

	printf(DBPLUS_STR "基础路径:%s.\n",basedir);
	myparam=dtparam=false;
	psoledidxmode=false;
	mt.FetchAll("select tabid ,databasename ,tabname ,recordnum ,(totalbytes/1024/1024) "
	//mt.FetchAll("select tabid as \"表编号\",databasename as \"数据库\",tabname as \"表名\",recordnum as \"记录数\",(totalbytes/1024/1024) as\"数据量\""
		" from dp.dp_table where recordnum>0");
	mt.Wait();
	wociSetColumnDisplay(mt,NULL,0,"表编号",-1,-1);
	wociSetColumnDisplay(mt,NULL,1,"数据库",-1,-1);
	wociSetColumnDisplay(mt,NULL,2,"表名",-1,-1);
	wociSetColumnDisplay(mt,NULL,3,"记录数",-1,0);
	wociSetColumnDisplay(mt,NULL,4,"数据量",-1,0);

	//dtmt.SetMt(mt,false);//单选
	dtmt.SetMt(mt);
	dtmt.DoSel("目标表选择(数据量单位为MB)");
}

void dtioMain::PreviousBind() {
	int sel=dtmt.GetFirstSel();
	int mt=dtmt.GetMt();
	while(sel!=-1) {
		char dbn[200];
		char tbn[200];
		wociGetStrValByName(mt,"databasename",sel,dbn);
		wociGetStrValByName(mt,"tabname",sel,tbn);
		CreateBind(dbn,tbn);
		sel=dtmt.GetNextSel(sel);
	}
}

void dtioMain::CreateBind(const char *dbn,const char *tbn) {
	sprintf(streamPath,"%s%s/%s.DTP",basedir,dbn,tbn);
	printf("准备建立'%s.%s'的参数文件...\n",dbn,tbn);
	if(pdtio!=NULL) delete pdtio;
	pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamWriteInit(DTP_BIND);
	DoBind(dbn,tbn);
	printf("建立索引表...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	printf("开始生成参数文件...\n");
	pdtio->SetWrite(true);
	pdtio->StreamWriteInit(DTP_BIND);
	DoBind(dbn,tbn);
	char fullpath[300];
	sprintf(fullpath,"%s.%s",dbn,tbn);
	conn.FlushTables(fullpath);
	printf("结束...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	//delete pdtio;
}

void dtioMain::DoBind(const char *dbn,const char *tbn) {
	//	 assert(pdtio);
	//保存备份数据源的MySQL基本数据目录
	pdtio->SetOutDir(basedir);
	if(pdtio==NULL) 
		ThrowWith("pdtio is nil in dtioMain::DoBind.");
	dtioDTTable dtt(dbn,tbn,pdtio,false);
	dtt.FetchParam(dts);
	dtt.SerializeParam();
}

void dtioMain::BackupPrepare() {
	printf(DBPLUS_STR "备份\n"
		" 使用 " DBPLUS_STR "备份程序,可以把数据库配置表、" DBPLUS_STR "配置表、" DBPLUS_STR "表备份到文件系统、远程主机或综合备份系统。\n"
	    " 注意:  本程序必须在备份或恢复的目标" DBPLUS_STR "数据库主机上运行！\n");
	char un[30],pwd[80],sn[100];
//	printf("连接到DBPlus数据库，请确保连接到本机DBPlus实例.\n");
//	getdbcstr(un,pwd,sn,"用户名");
//	dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
  dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
         if(pcp->all_tables)
           printf("完全备份.\n");
         else 
	   printf("增量备份.\n");
	AutoMt mt(dts,MAX_TABLES);
	 getBasePath(mt,basedir);
	printf(DBPLUS_STR "基础路径:%s.\n",basedir);
	getString("目标路径及文件名:","dpdump.dtp",streamPath);
	DEFAULT_SPLITLEN=(int8B)getOption("文件最大(50MB-20000MB):",4500,50,20000)*1024*1000;
	myparam=false;//GetYesNo("是否备份数据库配置表(Y/N)<no>:",false);
	dtparam=false;//GetYesNo("是否备份DBPlus配置表(Y/N)<no>:",false);
	dtdata=true;//GetYesNo("是否备份DBPlus表(Y/N)<yes>:",true);
	if(dtdata) {
		psoledidxmode=GetYesNo("仅备份" DBPLUS_STR "表主索引(Y/N)<yes>:",true);
		mt.FetchAll("select tabid ,databasename ,tabname,recordnum ,(totalbytes/1024/1024)"
		//mt.FetchAll("select tabid \"表编号\",databasename \"数据库\",tabname \"表名\",recordnum \"记录数\",totalbytes/1024/1024 \"数据量\""
			" from dp.dp_table where recordnum>0 %s",pcp->all_tables?"":" and cdfileid=2");
		mt.Wait();
		if(mt.GetRows()<1) 
			ThrowWith("%s没找到需要备份的表.",pcp->all_tables?"":"增量备份时");
		wociSetColumnDisplay(mt,NULL,0,"表编号",-1,-1);
		wociSetColumnDisplay(mt,NULL,1,"数据库",-1,-1);
		wociSetColumnDisplay(mt,NULL,2,"表名",-1,-1);
		wociSetColumnDisplay(mt,NULL,3,"记录数",-1,0);
		wociSetColumnDisplay(mt,NULL,4,"数据量",-1,0);
		dtmt.SetMt(mt);
		dtmt.DoSel("备份目标表选择(数据量单位为MB)");
	}
}

void dtioMain::Backup(const char *dbname,const char *tabname) {
	printf("准备备份...\n");
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamWriteInit();
	DoBackup(dbname,tabname);
	printf("建立索引表...\n");
	//wociGeneTable(pdtio->GetContentMt()->GetMt(),"dt_test",dts);
	//wociAppendToDbTable(pdtio->GetContentMt()->GetMt(),"dt_test",dts,true);
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	printf("开始备份...\n");
	pdtio->SetWrite(true);
	pdtio->StreamWriteInit();
	DoBackup(dbname,tabname);
	if(dtdata) 
	{
	        AutoStmt stmt(dts);
		if(dbname==NULL) {
		 int sel=dtmt.GetFirstSel();
		 int mt=dtmt.GetMt();
		 while(sel!=-1) {
			char dbn[200];
			char tbn[200];
			wociGetStrValByName(mt,"databasename",sel,dbn);
			wociGetStrValByName(mt,"tabname",sel,tbn);
			stmt.DirectExecute("update dp.dp_table set cdfileid=3 where databasename=lower('%s') and tabname=lower('%s')",dbn,tbn);
			sel=dtmt.GetNextSel(sel);
		 }
		}
		else stmt.DirectExecute("update dp.dp_table set cdfileid=3 where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
	}
	printf("备份结束...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
}

void dtioMain::DoBackup(const char *dbname,const char *tabname) {
	//	 assert(pdtio);
	//保存备份数据源的MySQL基本数据目录
	pdtio->SetOutDir(basedir);
	if(pdtio==NULL) 
		ThrowWith("pdtio is nil in dtioMain::DoBackup.");
	if(myparam) {
		dtioMySys msys(pdtio);
		msys.AddTables();
	}
	if(dtparam) {
		dtioDTSys dtsys(pdtio);
		dtsys.AddTables();
	}
	if(dtdata) 
	{
		dtioDTTableGroup dtg(dts,DTIO_UNITTYPE_NORMDTTAB,"Normal " DBPLUS_STR " table",pdtio,psoledidxmode);
		if(dbname==NULL) {
		 int sel=dtmt.GetFirstSel();
		 int mt=dtmt.GetMt();
		 while(sel!=-1) {
			char dbn[200];
			char tbn[200];
			wociGetStrValByName(mt,"databasename",sel,dbn);
			wociGetStrValByName(mt,"tabname",sel,tbn);
			dtg.AddTable(dbn,tbn);
			sel=dtmt.GetNextSel(sel);
		 }
		}
		else dtg.AddTable(dbname,tabname);
	}
}

void dtioMain::List(const char *filename,bool withlog,const char *dbn,const char *tbn) {
	if(filename==NULL) getString("目标路径及文件名:","dpdump.dtp",streamPath);
	else strcpy(streamPath,filename);
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	
	int mt,rn,i;
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	mt=wociCreateMemTable();
	{
		dtioMySys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("指定的备份文件中不含数据库参数表.\n");
		}
		else {
		wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,PATH_LEN,0);
		wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,PATH_LEN,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
		        wociInsertRows(mt,ptr,NULL,1);
		}
		printf("数据库系统表:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	{
		dtioDTSys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("指定的备份文件中不含" DBPLUS_STR "参数表数据.\n");
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
			wociInsertRows(mt,ptr,NULL,1);
		}
		printf(DBPLUS_STR "参数表:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	{
		double st,len;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("指定的备份文件中不含" DBPLUS_STR "表数据.\n");
			dtdata=false;
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
			//if((dbn==NULL || strcmp(dbname,dbn)==0) &&(tbn==NULL || strcmp(tabname,tbn))
			     wociInsertRows(mt,ptr,NULL,1);
		}
		printf( DBPLUS_STR "表:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	wocidestroy(mt);
	printf( DBPLUS_STR "存储项目:\n");
	wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	if(dbn!=NULL && tbn!=NULL) {
	 dtioDTTable dtt(dbn,tbn,pdtio,false);
 	 dtt.DeserializeParam();
	 printf( DBPLUS_STR "参数明细:\n");
	 dtt.GetParamMt()->PrintDetail(withlog);
	}
	delete pdtio;
	pdtio=NULL;
}

void dtioMain::RestorePrepare()
{
	printf( DBPLUS_STR "恢复 \n"
		" 使用" DBPLUS_STR "恢复程序,可以恢复数据库配置表、" DBPLUS_STR "配置表、" DBPLUS_STR "表。\n"
	    " 注意:  本程序必须在需要恢复的目标" DBPLUS_STR "数据库主机上运行！\n");
	char un[30],pwd[80],sn[100];
	printf("连接到" DBPLUS_STR "数据库，请确保连接到本机" DBPLUS_STR "实例.\n");
	//仅供调试
	//strcpy(un,"dtsys");strcpy(pwd,"gcmanager");strcpy(sn,"dtagt");
	//getdbcstr(un,pwd,sn,"用户名");
	//dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);

	//printf("连接到DT主数据库(MySQL)，请确保连接到本机DT实例.\n");
	//getdbcstr(un,pwd,sn,"用户名");
	//strcpy(un,"root");strcpy(pwd,"cdr0930");strcpy(sn,"dtsvr");
	sn[0]=0;
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
	 getBasePath(mt,basedir);
	
	//getString("数据库基本目标路径:",basedir,basedir);
	//下面一句仅调试时使用
	//getString("数据库基本目标路径:","/dbsturbo/dttemp/",basedir);
	if(*(basedir+strlen(basedir)-1)!=PATH_SEPCHAR) strcat(basedir,PATH_SEP);
	getString("目标路径及文件名:","dpdump.dtp",streamPath);
	myparam=false;//GetYesNo("是否恢复数据库配置表(Y/N)<no>:",false);
	dtparam=false;//GetYesNo("是否恢复DBPlus配置表(Y/N)<no>:",false);
	dtdata=true;//GetYesNo("是否恢复DBPlus表(Y/N)<yes>:",true);
}

int dtioMain::SetSequence() {
	AutoMt mt(dts,100);
	mt.FetchAll("select max(tabid) mtid from dp.dp_table");
	mt.Wait();
	int mxid=mt.GetInt("mtid",0);
	mt.FetchAll("select max(sysid) maxid from dp.dp_datasrc");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);
	mt.FetchAll("select max(pathid) maxid from dp.dp_path");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);
	mt.FetchAll("select max(mdfid) maxid from dp.dp_middledatafile");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);

	mt.FetchAll("select * from dp.dp_seq");
	mt.Wait();
	AutoStmt stmt(dts);
	if(mt.GetInt("id",0)<mxid) 
		stmt.DirectExecute("update dp.dp_seq set id=%d",mxid);
	return 0;
}

//备份时的目标表可能含有多个独立索引，但只备份主索引，因此需要重建索引
// 建立DTP文件以前，mysqld看到的目标表是空表。 索引只在目标表上建立，索引表不修改
// 2008-06-06: mysql51移植： 如果是3.2.9的备份文件 不能建立表，否则按新的5.1格式来解释数据文件格式，引起错误
int dtioMain::CreateDataTableAndIndex(const char *dbname,const char *tbname,dtparams_mt *pdtm) {
	//AutoMt mt(dts,100);
	char fulltbname[300];
	//sql语句缓冲，建立目标表可能有很多字段，预留足够空间
	char sqlbf[3000];
	sprintf(fulltbname,"%s.%s",dbname,tbname);
	AutoMt destmt(dts,10);
	destmt.FetchFirst("select * from %s",fulltbname);
	destmt.Wait();
	/*
	mt.FetchAll("select tabid from dt_table where databasename='%s' and tabname='%s'",dbname,tbname);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("找不到目标表。");
	int tabid=mt.GetInt("tabid",0);
	mt.FetchAll("select filename from dt_datafilemap where tabid=%d and fileflag=0 limit 2",tabid);
	rn=mt.Wait();
	if(rn<1) ThrowWith("创建目标表结构时找不到数据文件。");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	AutoMt destmt(0,10);
	destmt.SetHandle(idf.CreateMt(1));
	bool exist=conn.TouchTable(fulltbname);
	if(exist) {
	*/
	//printf("table %s has exist,dropped.\n",fulltbname);
	//2008 06-06 : 保留备份时表的定义，不重建
	//sprintf(sqlbf,"drop table %s",fulltbname);
	//conn.DoQuery(sqlbf);
	//}
	//建立目标标及其表结构的描述文件
	//wociGetCreateTableSQL(destmt,sqlbf,fulltbname,true);
	//printf("%s.\n",sqlbf);
	//conn.DoQuery(sqlbf);
	//mSleep(300);
	int i;
	while(true) { 
	 AutoMt idxmt(dts,10);
	 idxmt.FetchFirst("show index from %s",fulltbname);
	 if(idxmt.Wait()<1) break;
	 sprintf(sqlbf,"drop index %s on %s",
		 idxmt.PtrStr("key_name",0),fulltbname);
	  lgprintf("删除索引:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
	}
	int rn=pdtm->GetTotIndexNum();
	if(rn<1)
	 ThrowWith("找不到%s表的索引。",fulltbname);
	for(i=0;i<rn;i++) { 
	  //建立独立索引
	  //int id=mt.GetInt("indexid",i);
	  //char colsname[300];
	  //strcpy(colsname,mt.PtrStr("columnsname",i));
	  sprintf(sqlbf,"create index %s_%d on %s(%s)",
		  tbname,i+1,
		  fulltbname,pdtm->GetString(GetDPIndexAll(pdtio),"columnsname",i));
	  lgprintf("建立索引:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
	}
	return 0;
}

void dtioMain::PreviousRestore(const char *dbn,const char *tbn,const char *ndtpath)
{
	printf("准备恢复...\n");
	bool cmdmode=dbn!=NULL;
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	int mt,rn,i;
	MTChooser mysel,syssel,dtsel;
	mt=wociCreateMemTable();
	if(!cmdmode && myparam  && GetYesNo("包括基本权限设置在内的MySQL参数表将被替换,确认吗?(Y/N)<no>:",false)) {
		printf("包括基本权限设置在内的MySQL参数表将被替换,确认吗");
		dtioMySys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("指定的备份文件中不含数据库参数表.\n");
			myparam=false;
		}
		else {
		  wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,PATH_LEN,0);
		  wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,PATH_LEN,0);
		  wociBuild(mt,rn);
		  for(i=0;i<rn;i++) {
		  	dm.SearchTable(i,dbname,tabname);
		  	wociInsertRows(mt,ptr,NULL,1);
		  }
		  mysel.SetMt(mt);
		  mysel.DoSel("数据库系统表恢复选择");
		  int sel=mysel.GetFirstSel();
		  while(sel!=-1) {
		  	dm.Restore(conn,sel);		
		  	sel=mysel.GetNextSel(sel);
		  }
		  dm.AfterRestoreAll(conn);
		}
	}
	if(!cmdmode && dtparam && GetYesNo("恢复后的" DBPLUS_STR "参数将丢失上次备份以来的所有修改,不能确保与" DBPLUS_STR "数据文件吻合,确认要恢复吗?(Y/N)<no>:",false)) {
		dtioDTSys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("指定的备份文件中不含" DBPLUS_STR "参数表数据.\n");
			dtparam=false;
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
			wociInsertRows(mt,ptr,NULL,1);
		}
		syssel.SetMt(mt);
		syssel.DoSel(DBPLUS_STR "参数表恢复选择");
		int sel=syssel.GetFirstSel();
		while(sel!=-1) {
			dm.Restore(conn,sel);
			sel=syssel.GetNextSel(sel);
		}
		dm.AfterRestoreAll(conn);
		}
	}
	if(dtdata || cmdmode) {
		double st,len;
		int sel=-1;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("指定的备份文件中不含" DBPLUS_STR "表数据.\n");
			dtdata=false;
		}
		else {
		if(!cmdmode) {
		   wociClear(mt);
		   wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,60,0);
		   wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,80,0);
		   wociBuild(mt,rn);
		   for(i=0;i<rn;i++) {
		   	map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
		   	wociInsertRows(mt,ptr,NULL,1);
		   }
		   dtsel.SetMt(mt);
		   dtsel.DoSel( DBPLUS_STR "目标表恢复选择");
		   sel=dtsel.GetFirstSel();
		}
		char fulldbn[300];
		char optpath[PATH_LEN];
		char fullpath[PATH_LEN];
		optpath[0]=0;
		
		while(sel!=-1 || cmdmode) {
			if(!cmdmode) {
			 wociGetStrValByName(mt,"databasename",sel,dbname);
			 wociGetStrValByName(mt,"tabname",sel,tabname);
			 sel=dtsel.GetNextSel(sel);
			 char prmpt[100];
			 if(optpath[0]==0) {
			 	sprintf(prmpt,"输入'%s.%s'表的数据恢复路径<缺省按备份时路径>:",dbname,tabname);
			 	getString(prmpt,"",optpath);
			 }
			 else {
			 	sprintf(prmpt,"输入'%s.%s'表的数据恢复路径:",dbname,tabname);
			 	getString(prmpt,optpath,optpath);
			 }
			}
			else {
				strcpy(dbname,dbn);
				strcpy(tabname,tbn);
				if(ndtpath!=NULL) strcpy(optpath,ndtpath);
			}
			dtioDTTable dtt(dbname,tabname,pdtio,false);
			bool overwritable,duprecords;
			char msg[3000];
			sprintf(msg,"%s.%s",dbname,tabname);
			dtt.DeserializeParam();
			if(conn.TouchTable(msg)) 
				ThrowWith("表'%s'已存在,无法恢复.\n",msg);
			dtt.GetParamMt()->restoreCheck(dts,overwritable,duprecords,msg);
			if(duprecords && !overwritable) {
				printf("%s\n",msg);
				if(cmdmode) break;
				continue;
			}
			if(duprecords) {
			 printf("%s\n",msg);
			 if(cmdmode) break;
			 if(!GetYesNo("要恢复的表已存在,数据非空,覆盖吗? Yes/No(no):",false)) continue;
			}
			SetSequence();
			try {
			if(optpath[0]==0)
				dtt.Deserialize(dts);
			else {
				if(*(optpath+strlen(optpath)-1)!=PATH_SEPCHAR) strcat(optpath,PATH_SEP);
				dtt.Deserialize(dts,optpath);
			}
			printf("重建目标表结构:'%s.%s'.\n",dbname,tabname);
			if(pdtio->GetVersion()==DTIO_VERSION)
			 CreateDataTableAndIndex(dbname,tabname,dtt.GetParamMt());
			else 
				lgprintf("重要提示： 从原有版本的备份文件恢复数据，请注意检查独立索引结构的一致性!");
		  }
		  catch (...) {
		  	printf("数据恢复异常，删除表%s.%s:\n",dbname,tabname);
		    sprintf(msg,"dpadmin -rm %s %s",dbname,tabname);
		    system(msg);
		    throw;
		  }
			sprintf(fullpath,"%s%s/%s.DTP",basedir,dbname,tabname);
			dtioStreamFile *ldtio=NULL;
			ldtio=new dtioStreamFile(basedir);
			ldtio->SetStreamName(fullpath);
			ldtio->SetWrite(false);
			ldtio->StreamWriteInit(DTP_BIND);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio);
			printf("生成参数文件...\n");
			ldtio->SetWrite(true);
			ldtio->StreamWriteInit(DTP_BIND);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio);
			delete ldtio;
			ldtio=NULL;
			
			sprintf(fulldbn,"%s.%s",dbname,tabname);
			conn.FlushTables(fulldbn);
			if(strlen(msg)>0)
			lgprintf(msg);
			lgprintf("恢复完成");
			if(cmdmode) break;
		}
	     }
	}
	wocidestroy(mt);
	
	//delete pdtio;
}

void dtioMain::DoRestore()
{
}

void dtioMain::DetachPrepare()
{
	printf( DBPLUS_STR "快速恢复 \n"
		" 使用" DBPLUS_STR "快速恢复程序,需要和原始备份文件一起组成完整的数据。\n"
	    " 注意:  本程序必须在需要恢复的目标" DBPLUS_STR "数据库主机上运行！\n");
	char un[30],pwd[80],sn[100];
	printf("连接到" DBPLUS_STR "数据库，请确保连接到本机" DBPLUS_STR "实例.\n");
	//仅供调试
	//strcpy(un,"dtsys");strcpy(pwd,"gcmanager");strcpy(sn,"dtagt");
	//getdbcstr(un,pwd,sn,"用户名");
	//dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);

	//printf("连接到DBPlus主数据库(MySQL)，请确保连接到本机DBPlus实例.\n");
	//getdbcstr(un,pwd,sn,"用户名");
	//strcpy(un,"root");strcpy(pwd,"cdr0930");strcpy(sn,"dtsvr");
	sn[0]=0;
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
	 getBasePath(mt,basedir);
	
	//getString("数据库基本目标路径:",basedir,basedir);
	//下面一句仅调试时使用
	//getString("数据库基本目标路径:","/dbsturbo/dttemp/",basedir);
	if(*(basedir+strlen(basedir)-1)!=PATH_SEPCHAR) strcat(basedir,PATH_SEP);
	getString("目标路径及文件名:","dpdump.dtp",streamPath);
	myparam=dtparam=false;
	dtdata=true;
}

void dtioMain::PreviousDetach(const char *dbn,const char *tbn,const char *newtbn)
{
	printf("准备恢复...             \n");
	bool cmdmode=dbn!=NULL;
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	if(pdtio->GetStreamType()!=FULL_BACKUP) 
		ThrowWith("指定的文件不是原始备份文件!");
	
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	int mt,rn,i;
	MTChooser dtsel;
	mt=wociCreateMemTable();
	if(dtdata) {
		double st,len;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("指定的备份文件中不含" DBPLUS_STR "表数据.\n");
			dtdata=false;
		}
		else {
		int sel=-1;
		if(!cmdmode) {
		 wociClear(mt);
		 wociAddColumn(mt,"databasename","数据库",COLUMN_TYPE_CHAR,60,0);
		 wociAddColumn(mt,"tabname","表名",COLUMN_TYPE_CHAR,80,0);
		 wociBuild(mt,rn);
		 for(i=0;i<rn;i++) {
			map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
			wociInsertRows(mt,ptr,NULL,1);
		 }
		 dtsel.SetMt(mt);
		 dtsel.DoSel( DBPLUS_STR "目标表恢复选择");
		 sel=dtsel.GetFirstSel();
		}
		char fullpath[300];
		
		dtioStream *ldtio=NULL;
		while(sel!=-1 || cmdmode) {
			char desttabname[300];
			if(!cmdmode) {
			 wociGetStrValByName(mt,"databasename",sel,dbname);
			 wociGetStrValByName(mt,"tabname",sel,tabname);
			 sel=dtsel.GetNextSel(sel);
			 getString("恢复到表名:",tabname,desttabname);
			}
			else {
				strcpy(dbname,dbn);
				strcpy(tabname,tbn);
				strcpy(desttabname,newtbn==NULL?tabname:newtbn);
			}
			sprintf(fullpath,"%s%s/%s.DTP",basedir,dbname,desttabname);
			
			dtioDTTable dtt(dbname,tabname,pdtio,false);
			bool overwritable,duprecords;
			char msg[3000];
			dtt.DeserializeParam();
			sprintf(msg,"%s.%s",dbname,desttabname);
			if(conn.TouchTable(msg)) 
				ThrowWith("表'%s'已存在,无法恢复.\n",msg);
			//printf("建立索引表...\n");
			//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
			
			dtt.DeserializeDestTab(desttabname);
			dtt.DeserializeIndex(desttabname);
			CreateDataTableAndIndex(dbname,desttabname,dtt.GetParamMt());

			printf("开始生成参数文件...\n");
			if(ldtio!=NULL) delete ldtio;
			ldtio=new dtioStreamFile(basedir);
			ldtio->SetStreamName(fullpath);
			ldtio->SetWrite(false);
			ldtio->StreamWriteInit(DTP_DETACH,streamPath);
			ldtio->SetOutDir(basedir);
			dtt.GetParamMt()->restoreCheck(dts,overwritable,duprecords,msg);
			dtt.SerializeParam(ldtio,desttabname);

			ldtio->SetWrite(true);
			ldtio->StreamWriteInit(DTP_DETACH,streamPath);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio,desttabname);
			
			delete ldtio;
			
			ldtio=NULL;
			sprintf(fullpath,"%s.%s",dbname,desttabname);
			conn.FlushTables(fullpath);
			printf("结束...\n");
			//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
			//delete pdtio;
			if(cmdmode) break;
		}
		if(ldtio!=NULL) delete ldtio;
		}
	}
	wocidestroy(mt);
	
	//delete pdtio;
}

