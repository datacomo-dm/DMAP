#ifndef __DT_CMD_BASE_H
#define __DT_CMD_BASE_H
#include <string.h>
#define PARAMLEN 300

struct cmd_base {
  static int argc;
  static char **argv;
  char mhost[PARAMLEN];
  char musrname[PARAMLEN];
  char mpswd[PARAMLEN];
  char osn[PARAMLEN];
  char ousrname[PARAMLEN];
  char opswd[PARAMLEN];
  char serverip[PARAMLEN];
  int port;

  bool loopmode,verbosemode,echomode,freezemode;
  int waittime;
  cmd_base() {
  	memset(this,0,sizeof(this));
  }
  void GetParam() {
  	for(int i=1;i<argc;i++) 
  	 ExtractParam(i,argc,argv);
  }
  virtual void GetEnv() {
  	SetValue(serverip,getenv("DP_SERVERIP"));
  	SetValue(mhost,getenv("DP_DSN"));
  	SetValue(musrname,getenv("DP_MUSERNAME"));
  	SetValue(mpswd,getenv("DP_MPASSWORD"));
  	decode(mpswd);
  	SetValue(osn,getenv("DP_OSN"));
  	SetValue(ousrname,getenv("DP_OUSERNAME"));
  	SetValue(opswd,getenv("DP_OPASSWORD"));
  	decode(opswd);
  	SetValue(loopmode,getenv("DP_LOOP"));
  	SetValue(verbosemode,getenv("DP_VERBOSE"));
	SetValue(echomode,getenv("DP_ECHO"));
  	SetValue(waittime,getenv("DP_WAITTIME"));
  	SetValue(port,getenv("DP_MPORT"));
	if(port<1) port=3306;
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

  virtual bool ExtractParam(int &p,int n,char **argv) {
	if(strcmp(argv[p],"-mpwd")==0) {
  	   if(p+1>=n || argv[p+1][0]!='-') {
  	  	#ifdef WIN32
  	  	printf("输入MySQL服务器密码:");
  	  	gets(mpswd);
  	  	#else
  	  	strcpy(mpswd,getpass("输入" DBPLUS_STR "服务器密码:"));
  	  	#endif
  	  }
  	  else strcpy(mpswd,argv[p++]);
  	}
  	else if(strcmp(argv[p],"-mh")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("需要指定" DBPLUS_STR "主机名或地址.");
  	  strcpy(mhost,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-mun")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("需要指定" DBPLUS_STR "用户名.");
  	  strcpy(musrname,argv[++p]);
  	}
	else if(strcmp(argv[p],"-opwd")==0) {
  	  if(p+1>=n || argv[p+1][0]!='-') {
  	    ThrowWith("需要指定Oracle密码.");
  	  }
  	  else strcpy(opswd,argv[p++]);
  	}
  	else if(strcmp(argv[p],"-osn")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("需要指定Oracle连接名");
  	  strcpy(osn,argv[++p]);
  	}
  	else if(strcmp(argv[p],"-oun")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("需要指定Oracle用户名.");
  	  strcpy(ousrname,argv[++p]);
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
  	else if(strcmp(argv[p],"-nv")==0) {
  	 verbosemode=false;
	}
  	else if(strcmp(argv[p],"-ne")==0) {
  	 echomode=false;
	}
  	else if(strcmp(argv[p],"-wt")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith(" -wt 选项需要指定时间(秒).");
  	  waittime=atoi(argv[++p]);
	}
	else return false;
	return true;
  }
  
  virtual bool checkvalid() {
  	bool ret=true;
   	if(waittime<0 || (waittime<1 && loopmode)) {
  	  printf("Invalid wait time (seconds) ,set DP_WAITTIME or -wt parameter\n");	
  	  ret=false;
  	}
  	if(waittime<15) {
  		printf("loop wait time = %d ,force assigned to 15\n",waittime);
  		waittime=15;// less then 15 seconds.
  	}
 	if(strlen(mhost)<1) {
  	  printf("Need ODBC DSN name to connect to " DBPLUS_STR " server ,set DP_DSN or use -mh option.\n");
  	  ret=false;
  	}
  	if(strlen(musrname)<1) {
  	  printf("Need username to connect to " DBPLUS_STR " server ,set DP_MUSERNAME or use -mun option.\n");
  	  ret=false;
  	}
  	if(strlen(mpswd)<1) {
  	  printf("Need password to connect to " DBPLUS_STR " database server ,set DP_MPASSWORD or use -mpwd option.\n");
  	  ret=false;
  	}
	return ret;
  }
};

#endif
