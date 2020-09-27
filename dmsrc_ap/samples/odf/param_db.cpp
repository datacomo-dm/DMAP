
#include "odf.h"
#include <stdlib.h>
   param_db::param_db(const char *un,const char *pwd,const char *sn)
   {
   	dbc=wociCreateSession(un,pwd,sn,DTDBTYPE_ORACLE);
   }
   
   param_db::~param_db() {
   	if(dbc>0) wocidestroy(dbc);
   }
   
   char * param_db::GetDSPath(int ds_id,char *path)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll("select p.path_val pathval from tabt_dataset ds,tabt_path p where ds.ds_id=%d and p.path_id=ds.ext_pathid and ds.ext_pathid!=0",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d 或抽取路径无效!",ds_id);
   	strcpy(path,mt.PtrStr("pathval",0));
   	return path;
   }
   
   void param_db::GetClientPath(int ds_id,int client_id,char *path)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll("select p.path_val pathval from tabt_dsbind dsb,tabt_path p where "
   	         " dsb.ds_id=%d and dsb.client_id=%d and p.path_id=dsb.local_path_id and p.path_id!=0",ds_id,client_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d 对应的客户-%d 或装入路径无效!",ds_id,client_id);
   	strcpy(path,mt.PtrStr("pathval",0));
   }
   
   int param_db::BuildSrcDBC(int ds_id) {
   	char authcode[100];
   	AutoMt mt(dbc,1);
   	mt.FetchAll("select dbs.db_username un,dbs.db_password authcode,dbs.dsn_name dsn ,dbs.db_type dbtype"
		" from tabt_dataset ds,tabt_dbsys dbs"
   	  " where ds.ds_id=%d and dbs.sysid=ds.sysid and ds.sysid!=0",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d 或源系统无效!",ds_id);
   	strcpy(authcode,mt.PtrStr("authcode",0));
   	decode(authcode);
   	return wociCreateSession(mt.PtrStr("un",0),authcode,mt.PtrStr("dsn",0),mt.GetInt("dbtype",0)==0?DTDBTYPE_ORACLE:DTDBTYPE_ODBC);
   }
   
   int param_db::BuildDstDBC(int ds_id,int client_id)
   {
   	char authcode[100];
   	AutoMt mt(dbc,1);
   	mt.FetchAll("select dbs.db_username un,dbs.db_password authcode,dbs.dsn_name dsn,dbs.db_type dbtype "
		" from tabt_dsbind ds,tabt_dbsys dbs"
   	  " where ds.ds_id=%d and ds.client_id=%d and dbs.sysid=ds.dest_sysid ",ds_id,client_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d 客户-%d 或目标系统无效!",ds_id,client_id);
   	strcpy(authcode,mt.PtrStr("authcode",0));
   	decode(authcode);
   	return wociCreateSession(mt.PtrStr("un",0),authcode,mt.PtrStr("dsn",0),mt.GetInt("dbtype",0)==0?DTDBTYPE_ORACLE:DTDBTYPE_ODBC);
   }
   //true for valid time,false for limited time.
   int param_db::GetCompressFlag(int ds_id) 
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll("select compress_flag  from tabt_dataset ds "
   	  " where ds_id=%d ",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d CompressFlag无效!",ds_id);
   	return mt.GetInt("compress_flag",0);
   }
   
   int param_db::GetBlockRows(int ds_id)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select blockrows  from tabt_dataset ds "
   	  " where ds_id=%d ",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d BlockRows无效!",ds_id);
   	return mt.GetInt("blockrows",0);
   }
   
   int param_db::GetFileSys(int ds_id)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select ext_filesysid  from tabt_dataset ds "
   	  " where ds_id=%d ",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d ext_filesysid无效!",ds_id);
   	return mt.GetInt("ext_filesysid",0);
   }
    
    bool param_db::GetBlockDel(int ds_id)
    {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select ext_blockdel from tabt_dataset ds "
   	  " where ds_id=%d ",ds_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d ext_blockdel无效!",ds_id);
   	return mt.GetInt("ext_blockdel",0)==1;
    }
    
    bool param_db::CheckTimeLimit(int ds_id)
    {
    	AutoMt mt(dbc,1);
   	mt.FetchAll(" select sysdate from dual ");
   	mt.Wait();
   	char nowdate[10];
   	int ny,nm,nd,nh,nmi;
   	mt.GetDate("sysdate",0,nowdate);
   	ny=wociGetYear(nowdate);
   	nm=wociGetMonth(nowdate);
   	nd=wociGetDay(nowdate);
   	nh=wociGetHour(nowdate);
   	nmi=wociGetMin(nowdate);
   	int nhm=nh*60+nmi;
   	int nmd=nm*31+nd;
   	int nymd=ny*400+nm*31+nd;
   	mt.FetchAll("select * from tabt_tmlimit where ds_id=%d and isvalid=1",ds_id);
   	int rn=mt.Wait();
   	for(int i=0;i<rn;i++) {
   		int ltype=mt.GetInt("ltype",i);
   		char dtst[40],dted[40];
   		int y,m,d,h,mi;
   		int y1,m1,d1,h1,mi1;
   		mt.GetStr("begin_val",i,dtst);
   		mt.GetStr("end_val",i,dted);
   		switch(ltype) {
   			//格式为hh24:mi,例如 01:12. 时间限制类型要求格式必须正确。
   			case 1:	
   			  sscanf(dtst,"%02d:%02d",&h,&mi);
   			  if(h<0 || h>23 || mi<0 || mi >59) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  sscanf(dted,"%02d:%02d",&h1,&mi1);
   			  if(h1<0 || h1>23 || mi1<0 || mi1 >59) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  if(nhm>=h*60+mi && nhm<=h1*60+mi1) return false;
   			  break;
   			//  格式为dd,每月固定日期。
   			case 2:
   			  d=atoi(dtst);d1=atoi(dted);
   			  if(d<1 || d>31 || d1<1 ||d1>31)
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  if(nd>=d && nd<=d1) return false;
   			  break;
   			  //格式为mmdd
   			case 3:
   			  sscanf(dtst,"%02d%02d",&m,&d);
   			  if(m<1 || m>12 || d<1 || d >31) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  sscanf(dted,"%02d%02d",&m1,&d1);
   			  if(m1<1 || m1>12 || d1<1 || d1 >31) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  if(nmd>=m*31+d && nmd <=m1*31+d1) return false;
   			  break;
   			  //格式为mmdd
   			case 4:
   			  sscanf(dtst,"%04d%02d%02d",&y,&m,&d);
   			  if(y<2000 || y>2100 || m<1 || m>12 || d<1 || d >31) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  sscanf(dted,"%04d%02d%02d",&y1,&m1,&d1);
   			  if(y1<2000 || y1>2100 || m1<1 || m1>12 || d1<1 || d1 >31) 
   			    ThrowWith("时间格式错误。数据集%d,顺序号:%d.",ds_id,i);
   			  if(nymd>=y*400+m*31+d && nymd <=y1*400+m1*31+d1) return true;
   			  break;
   			  //格式为yyyymmddhh24mi
   			case 5:
   			  ThrowWith("类型5的时间限制暂不支持");
   			  break;
   			default :
   			  ThrowWith("不能识别的时间限制类型:%d",ltype);
   			  break;
   		}
   	}
   	return true;
    }
    
    int param_db::GetParamSQL(int ds_id,char *sql)
    {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select sql.sqltext sqlt from tabt_sql sql,tabt_dataset ds "
   	  " where ds_id=%d and sqlid=ext_paramsqlid and sqlid!=0 and sqltype=2",ds_id);
   	if(mt.Wait()==1) 
   	  strcpy(sql,mt.PtrStr("sqlt",0));
   	else return 0;
   	mt.FetchAll("select ext_paramrowlimit from tabt_dataset where ds_id=%d",ds_id);
   	mt.Wait();
   	return mt.GetInt("ext_paramrowlimit",0);
    }

    //必须存在，如果字段值为空则抛出异常
    void param_db::GetExtSQL(int ds_id,char *sql)
    {
   	  AutoMt mt(dbc,1);
   	  mt.FetchAll(" select sql.sqltext sqlt from tabt_sql sql,tabt_dataset ds "
   	    " where ds_id=%d and sqlid=ext_sqlid and sqlid!=0 and sqltype=1",ds_id);
   	  if(mt.Wait()!=1) 
   	    ThrowWith("数据集-%d 或ext_sqlid无效!",ds_id);
   	  strcpy(sql,mt.PtrStr("sqlt",0));
    }
    
    int param_db::ClientLogin(char *username,char *pswd)
	{
   	 AutoMt mt(dbc,1);
   	 mt.FetchAll(" select client_id,client_name ,client_password from tabt_client"
   	  " where client_login_name='%s'",username);
	 if(mt.Wait()<1) return 0;
	 char ps[100];
	 strcpy(ps,mt.PtrStr("client_password",0));
	 decode(ps);
	 if(strcmp(pswd,ps)!=0) return 0;
	 strcpy(username,mt.PtrStr("client_name",0));
	 return mt.GetInt("client_id",0);
	}

    bool param_db::ChangePswd(char *username,char *pswd,char *newpswd)
	{
   	 AutoMt mt(dbc,1);
   	 mt.FetchAll(" select client_id,client_name ,client_password from tabt_client"
   	  " where client_login_name='%s'",username);
	 if(mt.Wait()<1) return false;
	 char ps[100];
	 strcpy(ps,mt.PtrStr("client_password",0));
	 decode(ps);
	 if(strcmp(pswd,ps)!=0) return false;
	 strcpy(ps,newpswd);
	 encode(ps);
	 AutoStmt st(dbc);
	 st.DirectExecute("update tabt_client set client_password='%s' "
		 " where client_login_name='%s' ",ps,username);
	 return true;
	}

    bool param_db::AllowCreateTab(int ds_id,int client_id)
    {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select nvl(createtable_deny,0) deny from tabt_dsbind "
   	  " where ds_id=%d and client_id=%d",ds_id,client_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d 客户-%d 或ext_sqlid无效!",ds_id,client_id);
   	return mt.GetInt("deny",0)==0;
    }
    
   //如果没有SQL语句，但允许建表，则从数据块文件生成建表SQL.
   bool param_db::GetCreateTabSQL(int ds_id,int client_id,bool &denycreate,char *sql)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select sql.sqltext sqlt from tabt_sql sql,tabt_dsbind dsb "
   	  " where ds_id=%d and client_id=%d and sqlid=create_sqlid and sqlid!=0 and sqltype=4 and nvl(createtable_deny,0)=0",ds_id,client_id);
   	if(mt.Wait()!=1) return false;
   	strcpy(sql,mt.PtrStr("sqlt",0));
	denycreate=AllowCreateTab(ds_id,client_id);
   	return true;
   }

   //只要有建立目标表的动作，就要检查索引是否需要建立。
   bool param_db::GetIndexSQL(int ds_id,int client_id,char *sql)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select sql.sqltext sqlt from tabt_sql sql,tabt_dsbind dsb "
   	  " where ds_id=%d and client_id=%d and sqlid=index_sqlid and sqlid!=0 and sqltype=5 ",ds_id,client_id);
   	if(mt.Wait()!=1) return false;
   	strcpy(sql,mt.PtrStr("sqlt",0));
   	return true;
   }

   //需要参数块意味着清除数据的SQL语句中存在动态绑定参数，没用合法的参数块就不能运行。
   bool param_db::GetClearSQL(int ds_id,int client_id,bool &useparamblock,bool &repeat,char *sql)
   {
   	AutoMt mt(dbc,1);
   	mt.FetchAll(" select nvl(paramset_flag,0) flag,nvl(clr_before_ins,0) clr from tabt_dsbind "
   	  " where ds_id=%d and client_id=%d",ds_id,client_id);
   	if(mt.Wait()!=1) 
   	  ThrowWith("数据集-%d/客户端-%d 无效!",ds_id,client_id);
   	if(mt.GetInt("clr",0)==0) return false;
   	useparamblock=mt.GetInt("flag",0)!=0;
   	mt.FetchAll(" select sql.sqltext sqlt,sql.repeat rep from tabt_sql sql,tabt_dsbind dsb "
   	  " where ds_id=%d and client_id=%d and sqlid=clear_sqlid and sqlid!=0 and sqltype=3 ",ds_id,client_id);
   	if(mt.Wait()!=1) {
   	  mt.FetchAll("select dest_tab_name from tabt_dsbind where ds_id=%d and client_id=%d",ds_id,client_id);
   	  mt.Wait();
   	  sprintf(sql,"trancate table %s",mt.PtrStr("dest_tab_name",0));
   	  repeat=false;
   	  return true;
   	}
   	  //ThrowWith("数据集-%d 客户-%d 或clear_sqlid无效!",ds_id,client_id);
   	strcpy(sql,mt.PtrStr("sqlt",0));
   	repeat=mt.GetInt("rep",0)!=0;
	return true;
  }

      // refer to codetab for code means.
   int param_db::GetExtStatus(int _ds_id)
   {
   	AutoMt mt(dbc,500);
   	mt.FetchAll("select ext_status from tabt_dataset where ds_id=%d",_ds_id);
   	int rn=mt.Wait();
   	if(rn<1) 
   	  ThrowWith("数据集-%d/客户端-%d 无效!",_ds_id);
   	return mt.GetInt("ext_status",0);
   }
   
   int param_db::GetLoadStatus(int ds_id,int client_id,int ds_sn)
   {
   	AutoMt mt(dbc,500);
   	mt.FetchAll("select status from tabt_dssn_client where ds_id=%d and client_id=%d and ds_sn=%d",ds_id,client_id,ds_sn);
   	int rn=mt.Wait();
   	if(rn<1) 
   	  ThrowWith("数据集-%d/客户端-%d 无效!",ds_id,client_id);
   	return mt.GetInt("status",0);
   }
