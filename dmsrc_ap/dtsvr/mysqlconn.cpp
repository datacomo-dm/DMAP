#include "dt_common.h"
#include "mysqlconn.h"
#include "AutoHandle.h"
#ifndef MYSQL_SERVER
MySQLConn::MySQLConn() {
	basedir[0]=0;
	memset(host,0,sizeof(host));
	memset(username,0,sizeof(username));
	memset(password,0,sizeof(password));
	memset(dbname,0,sizeof(dbname));
	myData=NULL;
	result=NULL;
}

void MySQLConn::FlushTables(const char *tab)
{
	char sqlbf[200];
	sprintf(sqlbf,"flush tables %s",tab);
	while(DoQuery(sqlbf)==-1) { //target table is busy,can't flush now.
		printf("刷新%s遇忙，等待10秒...\n",tab);
		mSleep(10000);
	}
	//安全起见，尽量不要在mysqld刷新期间做后续操作。但这个时间也不能太长，否则数据切换的‘空白数据’时间太长。
	mSleep(200);
	lgprintf("'%s'表已刷新.",tab);
}

void MySQLConn::SelectDB(const char *db) {
	if(!myData) ThrowWith("Connection has not build while select db:%s\n",db);
	if ( mysql_select_db( myData, db ) < 0 ) {
		mysql_close( myData ) ;
		ThrowWith( "Can't select the %s database !\n", dbname ) ;
	}
}

void MySQLConn::Connect(const char *host,const char *username,const char *password,const char *dbname,unsigned int portnum) {
	if(host) strcpy(this->host,host);
	else this->host[0]=0;
	if(username) strcpy(this->username,username);
	else strcpy(this->username,"root");
	if(password) strcpy(this->password,password);
	else strcpy(this->password,"root");
	if(dbname) strcpy(this->dbname,dbname);
	else strcpy(this->dbname,"mysql");
	if(portnum!=0) this->portnum=portnum;
	else portnum=3306;
	if ( (myData = mysql_init((MYSQL*) 0)) && 
		mysql_real_connect( myData, host,username,password,NULL,portnum,
		NULL, 0 ) )
	{
		if ( mysql_select_db( myData, this->dbname ) < 0 ) {
			mysql_close( myData ) ;
			myData=NULL;
			ThrowWith( "Can't select the '%s' database !\n", this->dbname ) ;
		}
	}
	else {
		myData=NULL;
		ThrowWith( "Can't connect to the mysql server on port %d !\n",
			portnum ) ;
	}
}

int MySQLConn::DoQuery(const char *szSQL)
{
	if(!myData) ThrowWith("Connection has not build while query:%s\n",szSQL);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	//char dstsql[3000];
	//mysql_real_escape_string(myData,dstsql,szSQL,strlen(szSQL));
	if ( mysql_query( myData, szSQL)) {//dstsql,strlen(dstsql) ) ) 
		//  ./include/mysqld_error.h :
		//   #define ER_CANT_LOCK 1015
		//   #define ER_CANT_DROP_FIELD_OR_KEY 1091
		//   #define ER_NO_SUCH_TABLE 1146
		if(mysql_errno(myData)==1091 || mysql_errno(myData)==1146) return 0;
		if(mysql_errno(myData)==1015) return -1;
		ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", szSQL ,
			mysql_errno(myData),mysql_error(myData)) ;
	}
	result=mysql_store_result(myData);
	if(!result) {
		//ERROR 1091: Can't DROP 'tab_gsmvoicdr2_2'. Check that column/key exists
		//if(mysql_errno(myData)==1091) return ;
		if (mysql_errno(myData))
		{
			ThrowWith("Error: %s\n", mysql_error(myData));
		}
		else if (mysql_field_count(myData) == 0)
		{
			// query does not return data
			// (it was not a SELECT)
			num_rows = mysql_affected_rows(myData);
			//const char *info=mysql_info(myData) ;
			//if(info) lgprintf(info);
		}
	}
	return num_rows;
}

void MySQLConn::RenameTable(const char *srctab,const char *dsttab,bool force)
{
	if(!myData) ThrowWith("Connection has not build while move table %s->%s\n",srctab,dsttab);
	char sql[300];
	if(!TouchTable(srctab)) {
		//ThrowWith("MySQL表名更改:%s->%s,无源表.\n",srctab,dsttab);
		lgprintf("MySQL表名更改:%s->%s,无源表.\n",srctab,dsttab);
		return;
	}
	sprintf(sql,"flush tables %s ",srctab);
	DoQuery(sql);
	bool ext=false;
	try {
	 if(TouchTable(dsttab)) ext=true;
	}
	catch(...) {
		//表已损坏
		ext=true;	
	}
	if(ext)
	{
		if(!force) 
			ThrowWith("MySQL表名更改:%s->%s,目标表非空.\n",srctab,dsttab);
		sprintf(sql,"drop table %s ",dsttab);
		DoQuery(sql);
	}
	// alter table ... rename ... will failed on packed table(compressed table),
	//   but on a table.cc modified mysqld,rename table command could work,but alter table ... rename to leave failed,
	// only rename .. to .. is supported in 4.0.
	/* can not rename a packed table on 5.1 or 4.0 by alter table rename ... . in 4.0 dbplus use rename table ... to .. instead.
    but in 5.1, rename table command not work(just rename frm file,not MYD/MYI files remains old name).
  modify mysql_alter_table function in sql_table.cc file to force rename a packed table using RL_READ lock instead of WRITE_ALLOW_READ lock.
        */

	//sprintf(sql,"alter table %s rename %s",srctab,dsttab);
#ifdef MYSQL_VER_51
	sprintf(sql,"alter table %s rename to %s",srctab,dsttab);
#else
	sprintf(sql,"rename table %s to %s",srctab,dsttab);
#endif
	DoQuery(sql);
}

//表的结构或文件异常时使用
/*int CleanMySQLTable(const char *path,const char *dbn,const char *tbn);
{
	char srcf[300];
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	xmkdir(srcf);
	unlink(srcf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	unlink(srcf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	unlink(srcf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MRG");
	unlink(srcf);
	return 1;
}*/

bool MySQLConn::TouchTable(const char *tabname,bool clean) {
	if(!myData) ThrowWith("Connection has not build while touchtable:%s\n",tabname);
	char sql[300];
	sprintf(sql,"desc %s",tabname);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	if ( mysql_query( myData, sql ) ) {
		int er=mysql_errno(myData);
		const char *errmsg=mysql_error(myData);
		if(mysql_errno(myData)==1146) return false;
		if(clean&& basedir[0]!=0) {
			char tabnamex[300];
			strcpy(tabnamex,tabname);
			char *p=strstr(tabnamex,".");
			if(p!=NULL) *p=PATH_SEPCHAR;
			char fn[300];
			lgprintf("表%s不能打开，将被清除.(TouchTouble failed)",tabname);			
			sprintf(fn,"%s%s.DTP",basedir,tabnamex);
			unlink(fn);
			sprintf(fn,"%s%s.frm",basedir,tabnamex);
			unlink(fn);
			sprintf(fn,"%s%s.MYI",basedir,tabnamex);
			unlink(fn);
			sprintf(fn,"%s%s.MYD",basedir,tabnamex);
			unlink(fn);
			sprintf(fn,"%s%s.MRG",basedir,tabnamex);
			unlink(fn);
			FlushTables(tabname);
			return false;
		}
		else ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", sql ,
			mysql_errno(myData),mysql_error(myData)) ;
	}
	result=mysql_store_result(myData);
	return true;
}


bool MySQLConn::TouchDatabase(const char *dbname,bool create)
{
	if(!myData) ThrowWith("Connection has not build while touchdatabase:%s\n",dbname);
	char sql[300];
	sprintf(sql,"use %s",dbname);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	if ( mysql_query( myData, sql ) ) {
		if(mysql_errno(myData)==1049) {
			if(!create) return false;
			sprintf(sql,"create database %s",dbname);
			if ( mysql_query( myData, sql ) )
			 ThrowWith( "Error occurs on create database %s !\n errcode:%d,info:%s\n", dbname ,
			   mysql_errno(myData),mysql_error(myData)) ;
		}
		else ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", sql ,
			mysql_errno(myData),mysql_error(myData)) ;
	}
	result=mysql_store_result(myData);
	return true;
}

#endif
