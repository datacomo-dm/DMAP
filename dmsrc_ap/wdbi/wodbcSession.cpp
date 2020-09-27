#include "wdbi.h"
#ifndef NO_ODBC
#ifdef __unix
extern struct timespec interval;
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;
SQLHENV WODBCSession::henv;
bool WODBCSession::SetNonBlockMode()  {
  SQLRETURN rc;
  rc = SQLSetConnectAttr(hdbc,SQL_ATTR_ASYNC_ENABLE,(SQLPOINTER)SQL_ASYNC_ENABLE_ON ,0);
	chkdbc(rc);
	return true;
}

void WODBCSession ::ocheckerr(SQLRETURN status,int handletype,SQLHANDLE handle,int chkopt)
{
	SQLRETURN lrc;

  if( status == SQL_ERROR || status == SQL_SUCCESS_WITH_INFO ) 
  {
    SQLCHAR     szSqlState[6],szErrorMsg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER  pfNativeError;
    SQLSMALLINT pcbErrorMsg;
    
    lrc = SQLGetDiagRec(handletype, handle,1,     // 2: handle type:HDBC
                        (SQLCHAR *)&szSqlState,
                        (SQLINTEGER *)&pfNativeError,
                        (SQLCHAR *)&szErrorMsg,
                         SQL_MAX_MESSAGE_LENGTH-1,
                        (SQLSMALLINT *)&pcbErrorMsg);
    if(lrc == SQL_SUCCESS || lrc == SQL_SUCCESS_WITH_INFO) {
    	if(szSqlState[0]==0) return;
      sprintf(errbuf,"\n [%s][%d:%s]\n",szSqlState,pfNativeError,szErrorMsg);
    }  
    if(autoThrow) {
	strcpy(filen," ");
	lineid=-1;
	Throw();
    }
  }
}

bool WODBCSession::Connect (const char *username,const char *password,const char *svcname)
	{
    SQLRETURN rc;
    if(henv==NULL) {
     rc = SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&henv);
     chkenv(rc);   
  
     rc = SQLSetEnvAttr(henv,SQL_ATTR_ODBC_VERSION,(SQLPOINTER)SQL_OV_ODBC3,0);
     chkenv(rc);   
    }
    if(hdbc!=NULL) {
    	rc = SQLDisconnect(hdbc);
      chkdbc(rc);
      rc = SQLFreeConnect(hdbc);
      chkdbc(rc);
    }
    rc = SQLAllocHandle(SQL_HANDLE_DBC,henv, &hdbc);
    chkenv(rc);    

    rc = SQLConnect(hdbc, (SQLCHAR *)svcname, SQL_NTS, (SQLCHAR *)username, SQL_NTS, (SQLCHAR *) password, SQL_NTS);
   //db2 connection return SQL_SUCCESS_WITH_INFO but sqldiag return error 
   //shoud be continue to process
   if(rc!=SQL_SUCCESS_WITH_INFO)
    chkdbc(rc);

   rc = SQLSetConnectAttr(hdbc,SQL_ATTR_AUTOCOMMIT,(SQLPOINTER)SQL_AUTOCOMMIT_ON,0);
    chkdbc(rc);

	//  rc = SQLAllocHandle(SQL_HANDLE_STMT,*hdbc,hstmt);
  //  chkdbc(rc);
	SQLSMALLINT retlen=0;
	rc=SQLGetInfo(hdbc,SQL_DBMS_NAME,dbs_name,GENE_STR_LEN,&retlen);
	rc=SQLGetInfo(hdbc,SQL_DBMS_VER,dbs_version,GENE_STR_LEN,&retlen);
	//rc=SQLGetInfo(hdbc,SQL_DRIVER_VER,dbs_version,GENE_STR_LEN,&retlen);
		return true;
	}


void WODBCSession::Commit()
{
  SQLRETURN rc;
	rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT); 
  chkdbc(rc);
}

void WODBCSession::Rollback()
{
  SQLRETURN rc;
	rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK); 
  chkdbc(rc);
}

int WODBCSession::TestTable(const char *tabname)
{
	WODBCStatement st(this);
	return st.TestTable(tabname);
}

WDBIStatement WODBCSession::CreateStatement() {
	return (WDBIStatement)WODBCStatement(this,autoThrow);	
}

WDBIStatement *WODBCSession::NewStatement() {
	return (WDBIStatement *) new WODBCStatement(this,autoThrow);	
}

//void WODBCSession::checkerr(sword status) {
//	chkdbc(status);
//}
#endif 
