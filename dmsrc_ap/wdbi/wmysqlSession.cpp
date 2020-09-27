#include "wdbi.h"

#ifdef __unix
extern struct timespec interval;
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;
#define MYSQLCONN_FAILED 	-1
#define MYSQLSELECTDB_FAILED 	-2
#define MYSQL_CALLONUNCONN	-3

bool WMYSQLSession::Connect (const char *username,const char *password,const char *svcname)
{
		if(svcname) strcpy(SvcName,svcname);
		if(username) strcpy(UserName,username);
		else strcpy(UserName,"root");
		if(password) strcpy(Password,password);
		else strcpy(Password,"root");
		strcpy(this->dbname,"mysql");
		portnum=3306;
#ifndef NO_MYSQL
		try {
		if(myData) mysql_close( myData ) ;
		if ( (myData = mysql_init((MYSQL*) 0)) && 
         		mysql_real_connect( myData, SvcName,UserName,Password,NULL,portnum,
			   NULL, 0 ) )
		{
			if ( mysql_select_db( myData, this->dbname ) < 0 ) throw MYSQLCONN_FAILED;
		}
		else throw MYSQLSELECTDB_FAILED;
		}
		catch (sword e) {
			checkerr(e);
			//if(e==OCI_SUCCESS_WITH_INFO) return true;
			return false;
		}
#endif
	return true;
}


void WMYSQLSession::Commit()
{
#ifndef NO_MYSQL
	if(!myData) checkerr(MYSQL_CALLONUNCONN);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	strcpy(szSQL,"commit");
	if ( mysql_query( myData, szSQL)) 
		checkerr(0);
	result=mysql_store_result(myData);
	mysql_free_result(result);
	result=NULL;
#endif
}

void WMYSQLSession::Rollback()
{
#ifndef NO_MYSQL
	if(!myData) checkerr(MYSQL_CALLONUNCONN);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	strcpy(szSQL,"rollback");
	if ( mysql_query( myData, szSQL)) 
		checkerr(0);
	result=mysql_store_result(myData);
	mysql_free_result(result);
	result=NULL;
#endif
}

int WMYSQLSession::TestTable(char *tabname)
{
#ifndef NO_MYSQL
	if(!myData) checkerr(MYSQL_CALLONUNCONN);
	sprintf(szSQL,"desc %s",tabname);
	if(result) {
		mysql_free_result(result);
		result=NULL;
	}
	if ( mysql_query( myData, szSQL ) ) {
		 if(mysql_errno(myData)==1146) return false;
  		 checkerr(0);
	}
	result=mysql_store_result(myData);
	mysql_free_result(result);
	result=NULL;
	return true;
#endif
}

WDBIStatement WMYSQLSession::CreateStatement() {
	return (WDBIStatement)WMYSQLStatement(this,autoThrow);	
}

WDBIStatement *WMYSQLSession::NewStatement() {
	return (WDBIStatement *) new WMYSQLStatement(this,autoThrow);	
}

void WMYSQLSession::checkerr(sword status) {
  errcode=status;
  switch (status)
  {
  	case MYSQL_CALLONUNCONN:
	   sprintf(errbuf,"Operate on a unconnected MySQL session. User name '%s',Service Name '%s'.\n",UserName,SvcName);
	   break;
	case MYSQLSELECTDB_FAILED:
	   sprintf(errbuf,"Can not select database %s on Session. User name '%s',Service Name '%s'.\n",dbname,UserName,SvcName);
	   break;
#ifndef NO_MYSQL
	case MYSQLCONN_FAILED  :
	   sprintf(errbuf,"Connect to MySQL failed,Error: %s¡£User name '%s',service name '%s'.\n", mysql_error(myData),
	    UserName,SvcName);
	   break;
	default :   
	   sprintf(errbuf,"Error on session: (%d)%s. User name '%s',Service Name '%s'.\n",mysql_errno(myData),mysql_error(myData),UserName,SvcName);
	   break;
#endif
  }
}