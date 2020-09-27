#include <iostream>
#include <vector>
#include <string>
#include "GetPot"

void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[3000];
	vsprintf(msg,format,vlist);
	printf(msg);
	va_end(vlist);
	throw msg;
}

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
		return wociCreateSession('%s','%s','%s','%s')",
		    un.c_str(),pwd.c_str(),dsn.c_str(),type==0?"DTDBTYPE_ORACLE":"DTDBTYPE_ODBC");
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
	
	varsarray() {
		vars.clear();lplevel=-1;
	}
	void push(const char *varn,vector<string> &var) {
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
	  va_list vlist;
	  const char *p[100];
	  int s=usedvars.size();
 	  for(int i=0;i<s;i++) {
		 p[i]=(const char*)getvar(usedvars[i].c_str());
	  }
	  vlist=(char *)p;
	  vsprintf(buf,fmt,vlist);
	}
};

		
	
class grpparam {
public:
	bool contonerr;
	int repdelay;// minutes
	bool haverefdata; 
	int smgmode;	//# 0：不通知 1：错误通知，发生错误时通知 2：简要通知，发生错误或全部执行完成时通知 
			//# 3: 详细通知,包括执行的每个阶段
	string loggroup;
	string logfile;
	string title;
	
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
   	title=cl("基本选项/统计标题","");
   	logfile=cl("基本选项/日志文件","");
   	
   	smgmode=cl("基本选项/短信通知",0);
   	loggroup=cl("基本选项/日志组","");
   	haverefdata=cl("基本选项/引用数据",0)==1;
   	repdelay=cl("基本选项/错误重试间隔",0);
   	contonerr=cl("基本选项/错误时继续循环",0)==1;
   	loggroup=cl("基本选项/日志组","");
   	
   	srcrowcount=cl("源数据/记录数",0);
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
	 refrowcount=cl("引用数据/记录数",0);
  	if(refrowcount<1)
      		ThrowWith("错误:缺少 引用数据/记录数.\n");
	 getArray("引用数据/变量",refvars,cl);
   	if(refvars.size()>0)
   	 pushVarArray(refvars,cl);
   	 refsql=cl("引用数据/SQL","");
   	 if(!getWDBIParam("引用数据/连接参数",refdb,cl))
   	 	printf("未指定 引用数据/连接参数,默认使用数据源的连接!\n");
	 refordercols=cl("引用数据/排序字段","");
	 refgroupcols=cl("引用数据/分组字段","");
	}
	
	resrowcount=cl("结果数据/记录数",0);
  	if(resrowcount<1)
      		ThrowWith("错误:缺少 结果数据/记录数.\n");
	getArray("结果数据/变量",resvars,cl);
	if(resvars.size()>0)
	 pushVarArray(resvars,cl);
	restabname=cl("结果数据/结果表名","");
   	if(!getWDBIParam("结果数据/连接参数",resdb,cl) )
 	 printf("未指定 结果数据/连接参数,默认使用数据源的连接!\n");

	
	//1:重建 2:追加 3:清理
	resstoretype=cl("结果数据/结果集存储",0);
	if(resstoretype==3) {
		cleancondition=cl("结果数据/清理条件","");
		cleanRows=cl("结果数据/清理提交记录",0);
	}
	
   	resrowcount=cl("结果数据/记录数",0);
   	if(resrowcount<1)
      		ThrowWith("错误:缺少 结果数据/记录数.\n");
      	
      	// This must the last call of variable reading.
      	vararr.buildrelation();
	first();
  }
};	
	
/*
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
*/

int main () {
 grpparam gp("group.ini");
 do{
   string v;
   v=gp.getvars();
   printf("VARS: %s.\n",v.c_str());
   v=gp.getlpvars();
   printf("  LOOPVARS: %s.\n",v.c_str());
 } while(gp.next());
 
 return 1; 
}
   

