#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <vector>
#include <string>
#include "GetPot"

using namespace std;

class wdbiparam {
public:
	string un,pwd,dsn;
	int type;
	wdbiparam()
	{
		type=-1;
	}
	bool validParam() {return type!=-1;}
	void SetParam(vector<string> &pa) {
		if(pa.size()!=4) 
			ThrowWith("错误:数据库连接参数格式错误,需要四个参数，只有%d个",pa.size());
		if(strcasecmp(pa[3].c_str(),"odbc")==0) 
			type=1;
		else if(strcasecmp(pa[3].c_str(),"oracle")==0) 
		         type=0;
		else ThrowWith("错误:数据库连接参数格式错误,类型只能选odbc或oracle,不能是'%s'.",pa[3].c_str());
		un=pa[0];pwd=pa[1];dsn=pa[2];
	}
	int buildConn() {
		//printf("Build wociCreateSession('%s','%s','%s','%s')",
		//    un.c_str(),pwd.c_str(),dsn.c_str(),type==0?"DTDBTYPE_ORACLE":"DTDBTYPE_ODBC");
		char dpwd[300];
		strcpy(dpwd,pwd.c_str());
		return wociCreateSession(un.c_str(),dpwd,dsn.c_str(),type==0?DTDBTYPE_ORACLE:DTDBTYPE_ODBC);
	}
};

struct grpvars {
	string varname;
	vector<string> values;
	int pos;
	int getSize() {return values.size();}
	grpvars() {pos=0;depoffset=-1;}
	int depoffset;
};


class varsarray {
	vector<grpvars> vars;
	vector<int> lpvar;
	int lplevel;
public:
	int getlpcount() {
		int s=lpvar.size();
		int rt=1;
		for(int i=0;i<s;i++) {
		 rt*=vars[lpvar[i]].getSize();
		}
		return rt;
	}
		
	//所有变量的取值,空格分隔
	string getlpvars() {
		string res;
		res="";
		int s=lpvar.size();
		for(int i=0;i<s;i++) {
		 int off=lpvar[i];
		 res+=vars[off].values[vars[off].pos];
		 res+=" ";
		}
		return res;
	}
	
	void pushint(const char *varname,int value) {
		vector<string> var;
		var.clear();
		char svalue[20];
		sprintf(svalue,"%d",value);
		var.push_back(svalue);
		push(varname,var);
	}
	varsarray() {
		vars.clear();lplevel=-1;
		char curdate[10];
		char mthvalue[20];
		wociGetCurDateTime(curdate);
		int mth=wociGetMonth(curdate);
		int pmth=mth==1?12:(mth-1);
		int nmth=mth==12?1:(mth+1);
		pushint("curr_date",wociGetYear(curdate)*10000+mth*100+wociGetDay(curdate));
		pushint("this_yearmonth",wociGetYear(curdate)*100+mth);
		pushint("prev_yearmonth",mth==1?((wociGetYear(curdate)-1)*100+12):(wociGetYear(curdate)*100+mth-1));
		pushint("next_yearmonth",mth==12?((wociGetYear(curdate)+1)*100+1):(wociGetYear(curdate)*100+mth+1));
		pushint("this_month",mth);
		pushint("prev_month",pmth);
		pushint("next_month",nmth);
		pushint("this_month_m6",(mth+5)%6+1);
		pushint("prev_month_m6",(pmth+5)%6+1);
		pushint("next_month_m6",(nmth+5)%6+1);
		pushint("this_month_m4",(mth+3)%4+1);
		pushint("prev_month_m4",(pmth+3)%4+1);
		pushint("next_month_m4",(nmth+3)%4+1);
		pushint("this_month_m2",mth%2);
		pushint("prev_month_m2",pmth%2);
		pushint("next_month_m2",nmth%2);
	
	}
	
	bool checkvarexist(const char *varn) {
		int s= vars.size();
		int i=0;
		for(i=0;i<s;i++) {
		  if(vars[i].varname.compare(varn)==0) return true;
		}
		return false;
	}
	
	void push(const char *varn,vector<string> &var) {
		if(checkvarexist(varn))
		    ThrowWith("错误:变量'%s'重复定义或使用了关键字作变量.",varn);
		grpvars v;
		v.varname=varn;
		v.values=var;
		vars.push_back(v);
	}
	//call following routine after push all objects
	void buildrelation()
	{
		int s= vars.size();
		int i=0;
		for(i=0;i<s;i++) {
		  if(strncmp(vars[i].values[0].c_str(),"conn:",strlen("conn:"))==0)
		   {
		   	string dep=vars[i].values[0];
		   	dep.replace(0,strlen("conn:"),"");
		   	int j;
		   	for(j=0;j<s;j++)
		   	{
		   		if(vars[j].varname.compare(dep)==0) {
		   		  if(strncmp(vars[j].values[0].c_str(),"conn:",strlen("conn:"))==0 || vars[j].depoffset>=0)
		   		   	ThrowWith("错误:变量'%s'关联层次大于1('%s'有关联要求),请修正.",vars[i].varname.c_str(),vars[j].varname.c_str());
		   		  if(vars[j].getSize()!=vars[i].getSize()-1) 
		   		   	ThrowWith("错误:变量'%s'的数量与'%s'不一致',无法关联.",vars[i].varname.c_str(),vars[j].varname.c_str());
	   			  vars[i].depoffset=j;
	   			  break;
	   			}
	   		}
	   		if(j==s) 
	   			ThrowWith("错误:变量'%s'找不到连接变量'%s'.",vars[i].varname.c_str(),dep.c_str());
	   		vars[i].values.erase(vars[i].values.begin());
	   	   }
	   	}
	   	for(i=0;i<s;i++) {
		  if(vars[i].depoffset==-1)
		   lpvar.push_back(i);
		}	
	   	lplevel=lpvar.size()-1;
	}
	
	bool changepos(int off,int pos=-1) {
		if(pos==-1) pos=vars[off].pos+1;
		if(vars[off].getSize()<=pos) return false;
		int s=vars.size();
		vars[off].pos=pos;
		for(int i=0;i<s;i++) {
		  if(vars[i].depoffset==off)		
		   vars[i].pos=pos;
		}
		return true;
	}
	
	void first() {
		lplevel=lpvar.size()-1;
		int s=vars.size();
		for(int i=0;i<s;i++) {
		   vars[i].pos=0;
		}
	}
	
	bool next() {
		if(!changepos(lpvar[lplevel]))
		{ //check up first
			do{
			 lplevel--;
			 if(lplevel<0) return false;
			}
			while(!changepos(lpvar[lplevel]));
			for(int i=lplevel+1;i<lpvar.size();i++) {
			   changepos(lpvar[i],0);
			}
			lplevel=lpvar.size()-1;
		}	  
		return true;
	}
	
	const char * getvar(const char *varname) {
		int s=vars.size();
		for(int i=0;i<s;i++) {
		   if(vars[i].varname.compare(varname)==0) return vars[i].values[vars[i].pos].c_str();
		}
	   	ThrowWith("错误:找不到变量'%s'.",varname);
	   	return "NO Value";
	}
	
	string getvars() {
		string res;
		res="";
		int s=vars.size();
		for(int i=0;i<s;i++) {
		 res+=vars[i].values[vars[i].pos];
		 res+="  ";
		}
		return res;
	}
	
	void format(char *buf,const char *fmt,vector<string> usedvars)
	{
	  //va_list vlist;
	  const char *p[100];
	  int s=usedvars.size();
 	  for(int i=0;i<s;i++) {
		 p[i]=(const char*)getvar(usedvars[i].c_str());
	  }
          int of=0;
          while(*fmt) {
            if(*fmt=='%') {
             fmt++;
             if(*fmt=='%') *buf++='%';
             else if(*fmt!='s') ThrowWith("format error ,%%%c",*fmt);
             else {strcpy(buf,p[of]);
                   buf+=strlen(p[of++]);
             }
             fmt++;
            }
            else *buf++=*fmt++;
          }
	  *buf=0;
	  //vlist=(va_list )p;
	  //vsprintf(buf,fmt,vlist);
	}
};

		
	
class grpparam {
public:
	bool contonerr;
	int repdelay;// minutes
	bool haverefdata; 
	bool verboselog;
	bool logecho;
	int smgmode;	//# 0：不通知 1：错误通知，发生错误时通知 2：简要通知，发生错误或全部执行完成时通知 
			//# 3: 详细通知,包括执行的每个阶段
	string smgroup;
	string logfile;
	string title;
	
	int startoffset; //开始偏移量，默认为1（从第一个开始）
	
	varsarray vararr;
	
	int srcrowcount;
	vector<string> srcvars;
	wdbiparam srcdb;
	string srccolsforref;
	string srcsql;
	string srcgroupcols;
	string srccalcols;
	
	int refrowcount;
	vector<string> refvars;
	wdbiparam refdb;
	string refordercols;
	string refgroupcols;
	string refsql;
	
	int resrowcount;
	vector<string> resvars;
	string restabname;
	wdbiparam resdb;
	//1:重建 2:追加 3:清理
	int resstoretype;
	string cleancondition;
	int cleanRows;

private:	
	bool getWDBIParam(const char *vpath,wdbiparam &p,GetPot &cl)
	{
	  vector<string> tmp;
	  getArray(vpath,tmp,cl);
	  if(tmp.size()==0) return false;
	  p.SetParam(tmp);
	  return true;
	}
	
	void getArray(const char *vpath,vector<string> &res,GetPot &cl) {
	 res.clear();
   	 int vnum=cl.vector_variable_size(vpath);
   	 int lp=0;
   	 while(lp<vnum) 
     	  res.push_back(cl(vpath,"",lp++));
	}
	
	void pushVarArray(vector<string> &varnames,GetPot &cl) {
		int s=varnames.size();
		string varpath;
		for(int i=0;i<s;i++) {
			if(vararr.checkvarexist(varnames[i].c_str())) continue;
			varpath="变量/";
			varpath+=varnames[i];
			vector<string> res;
			getArray(varpath.c_str(),res,cl);
			if(res.size()<1) 
   	 		 ThrowWith("找不到配置项: '%s'",varpath.c_str());
			vararr.push(varnames[i].c_str(),res);
		}
	}
public:
  string getlpvars() {
  	return vararr.getlpvars();
  }
  
  const char * getvar(const char *varname) {
  	return vararr.getvar(varname);
  }	
  
  string getvars() {
  	return vararr.getvars();
  }

  void first() {
  	vararr.first();
  }
  
  bool next() {
  	return vararr.next();
  }
  
  grpparam(const char *filename) {
	GetPot cl(filename);
   	title=cl("基本选项/统计标题","公用分组统计");
   	logfile=cl("基本选项/日志文件",""); //bigroup/common.log
   	if(strlen(logfile.c_str())<1)
   	 logfile="bigroup/"+title+".log";
   	
   	smgmode=cl("基本选项/短信通知",0);
	startoffset=cl("基本选项/循环起始",1);
   	smgroup=cl("基本选项/短信组","公用分组统计");
   	haverefdata=cl("基本选项/引用数据",0)==1;
   	repdelay=cl("基本选项/错误重试间隔",0);
   	contonerr=cl("基本选项/错误时继续循环",0)==1;
   	verboselog=cl("基本选项/详细输出",0)==1;
   	logecho=cl("基本选项/日志回显",1)==1;
   	
   	
   	srcrowcount=cl("源数据/记录数",50000);
   	if(srcrowcount<1)
      		ThrowWith("错误:缺少 源数据/记录数.\n");
   	if(!getWDBIParam("源数据/连接参数",srcdb,cl))
   		ThrowWith("错误:源数据/连接参数 缺少或格式错误!");
   	
   	getArray("源数据/变量",srcvars,cl);
   	if(srcvars.size()>0)
   	 pushVarArray(srcvars,cl);
   	if(haverefdata)
   	  srccolsforref=cl("源数据/引用关联字段","");
   	srcsql=cl("源数据/SQL","");
   	srcgroupcols=cl("源数据/分组字段","");
	srccalcols=cl("源数据/汇总字段","");
   	 	
   	if(haverefdata) {
	 refrowcount=cl("引用数据/记录数",50000);
  	if(refrowcount<1)
      		ThrowWith("错误:缺少 引用数据/记录数.\n");
	 getArray("引用数据/变量",refvars,cl);
   	if(refvars.size()>0)
   	 pushVarArray(refvars,cl);
   	 refsql=cl("引用数据/SQL","");
   	 if(!getWDBIParam("引用数据/连接参数",refdb,cl))
   	 	lgprintf("未指定 引用数据/连接参数,默认使用数据源的连接!");
	 refordercols=cl("引用数据/排序字段","");
	 refgroupcols=cl("引用数据/分组字段","");
	}
	
	resrowcount=cl("结果数据/记录数",50000);
  	if(resrowcount<1)
      		ThrowWith("错误:缺少 结果数据/记录数.\n");
	getArray("结果数据/变量",resvars,cl);
	if(resvars.size()>0)
	 pushVarArray(resvars,cl);
	restabname=cl("结果数据/结果表名","");
   	if(!getWDBIParam("结果数据/连接参数",resdb,cl) )
 	 lgprintf("未指定 结果数据/连接参数,默认使用数据源的连接!");
	
// /强制重建 1 /首次重建 2/追加 3 /清理 4(4暂不支持) /二进制文件 5/文本文件 6/
//如果是清理,还需要写清理条件
//如果是追加和清理方式，而结果表不存在，则自动建立
//如果是重建方式，则不论结果表是否存在，都重新建立。
//
//首次重建,统计进程循环过程中第一次使用表时重建. 注意只有连续使用表的第一次重建,也就是说,即便
//  是第二次使用表,但紧接的前一次表名不同,则会删除重建.
//
//  如果是5/6(文件方式),则一律为覆盖重建 

	resstoretype=cl("结果数据/结果集存储",2);
	if(resstoretype==3) {
		cleancondition=cl("结果数据/清理条件","");
		cleanRows=cl("结果数据/清理提交记录",10000);
	}
	
      	// This must the last call of variable reading.
      	vararr.buildrelation();
	first();
  }
};	
	

int smprintf(const char *smgroup,const char *format,...){
	va_list vlist;
	va_start(vlist,format);
	static char smcontent[8000];
	vsprintf(smcontent,format,vlist);
	va_end(vlist);
	//trim length of content:
	strcpy(smcontent+70,"...");
	AutoHandle dst;
	dst.SetHandle(wociCreateSession("dtuser","readonly","//130.86.12.80/dtagt",DTDBTYPE_ORACLE));
	AutoMt mt(dst,100);
	mt.FetchFirst("select gid from sms_group where gname='%s'",smgroup);
	if(mt.Wait()!=1) 
		ThrowWith("Error: short message send to a unknown group '%s',content '%s'.",smgroup,smcontent);
	lgprintf("短消息通知: %s(%s).",smcontent,smgroup);
	try{
	 AutoStmt st(dst);
	 st.DirectExecute("insert into sms_content values (%d,'%s',sysdate,null,1998)",*mt.PtrInt(0,0),smcontent);
	}
	catch(...) {
		wociRollback(dst);
		throw;
	}
	return strlen(smcontent);
}


//公用日志文件名
#define LOGPATH "bi/group"
int Start(void *ptr);
int argc;
char  **argv;  
int main(int _argc,char *_argv[]) {
    int nRetCode = 0;
    argc=_argc;
    argv=_argv;
    if(argc!=2) {
    	printf("使用方法:bigroup <control_file>.\n");
    	return 1;
    }
    WOCIInit(LOGPATH);
    nRetCode=wociMainEntrance(Start,true,argv[1],2);
    WOCIQuit(); 
    return nRetCode;
}


int Start(void *ctrlfile) {
    wociSetTraceFile("bigroupcommon.log");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(TRUE);
    grpparam gp((const char *)ctrlfile);
    wociSetTraceFile(gp.logfile.c_str());
    wociSetOutputToConsole(TRUE);
    wociSetEcho(gp.verboselog);
    lgprintf("开始分组统计: %s.",gp.title.c_str());
    AutoHandle dts;
    lgprintf("连接到源数据库...");
    
    dts.SetHandle(gp.srcdb.buildConn());
    mytimer mt_1;
    mt_1.Start();
    AutoHandle dtref;
    if(gp.haverefdata && gp.refdb.validParam())
    {
      lgprintf("连接到引用数据库...");
      dtref.SetHandle(gp.refdb.buildConn());
    }
    AutoHandle dtresult;
    if(gp.resdb.validParam()) {
     lgprintf("连接到结果数据库...");
     dtresult.SetHandle(gp.resdb.buildConn());
    }
    char sqlbuf[8000];
    int lpct=0;
    double totrows=0,lprows=0,refrows=0;
    char lastrestable[1000];
    memset(lastrestable,0,sizeof(lastrestable));
    AutoMt mtref(gp.refdb.validParam()?dtref:dts,gp.refrowcount);
    do{
	if(gp.startoffset>lpct+1) {
      lgprintf("第%d次循环跳过.",lpct+1);
      gp.vararr.format(lastrestable,gp.restabname.c_str(),gp.resvars);
      lpct++;
      continue;
     }
     mytimer mt_2;
     mt_2.Start();
     try {
     TradeOffMt mt(0,gp.srcrowcount);
     AutoStmt stmt(dts);
     gp.vararr.format(sqlbuf,gp.srcsql.c_str(),gp.srcvars);
     stmt.Prepare(sqlbuf);
     mt.Cur()->Build(stmt);
     mt.Next()->Build(stmt);
     AutoMt result(dts,gp.resrowcount);
     if(gp.haverefdata) {
     	if(gp.refvars.size()>0 || refrows<1) {
     	 lgprintf("取得引用数据...");
     	 gp.vararr.format(sqlbuf,gp.refsql.c_str(),gp.refvars);
     	 mtref.FetchAll(sqlbuf);
     	 mtref.Wait();
     	 lgprintf("引用数据排序...");
     	 wociSetSortColumn(mtref,gp.refordercols.c_str());
     	 wociSort(mtref);
     	 refrows=mtref.GetRows();
     	}
        result.SetGroupParam(*mt.Cur(),gp.srcgroupcols.c_str(),gp.srccalcols.c_str(),gp.srccolsforref.c_str(),mtref,gp.refgroupcols.c_str());
     }
     else result.SetGroupParam(*mt.Cur(),gp.srcgroupcols.c_str(),gp.srccalcols.c_str());
     mt.FetchFirst();
     int rn=mt.Wait();
     while (rn>0)
     {
    	totrows+=rn;
    	mt.FetchNext();
	wociSetGroupSrc(result,*mt.Cur());
        wociGroup(result,0,rn);
    	rn=mt.Wait();
     }
     if(result.GetRows()>0) {
      gp.vararr.format(sqlbuf,gp.restabname.c_str(),gp.resvars);
      bool forcecreate=false;
      if(gp.resstoretype==1 || (gp.resstoretype==2 && strcmp(sqlbuf,lastrestable)!=0) ) forcecreate=true;
      result.CreateAndAppend(sqlbuf,gp.resdb.validParam()?dtresult:dts,forcecreate);
      wociCommit(gp.resdb.validParam()?dtresult:dts);
      strcpy(lastrestable,sqlbuf);
     }
     mt_2.Stop();
     }
     catch(...) {
     	if(gp.smgmode>0) smprintf(gp.smgroup.c_str(),"系统通知:统计''%s''第%d次运行时异常!",gp.title.c_str(),lpct+1);
	lgprintf("错误:统计'%s'第%d次运行时异常!",gp.title.c_str(),lpct+1);
     	if(gp.contonerr) {
     		if(gp.repdelay>0) {
     		 lgprintf("%d分钟后继续...",gp.repdelay);
     		 sleep(gp.repdelay*60);
     		 continue;
     		}
     	}
     	throw;
     }
     if(gp.smgmode==3) smprintf(gp.smgroup.c_str(),"系统通知:统计''%s''第%d次运行,%.0f秒.",gp.title.c_str(),lpct+1,mt_2.GetTime());
     lgprintf("第%d次循环运行时间%f秒 正常结束",++lpct,mt_2.GetTime());
   } while(gp.next());
   mt_1.Stop();
   lgprintf("运行总时间%f秒 正常结束",mt_1.GetTime());
   if(gp.smgmode==2) smprintf(gp.smgroup.c_str(),"系统通知:统计''%s''共运行%d次,%.0f秒.",gp.title.c_str(),lpct+1,mt_1.GetTime());
   return 0;
}
