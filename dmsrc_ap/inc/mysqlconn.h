#ifndef __MYSQLCONN_H
#define __MYSQLCONN_H
#include <stdio.h>
#include <string.h>
#ifndef MYSQL_SERVER
#include <mysql.h>
#include <dtio_common.h>
class DTIOExport MySQLConn {
  MYSQL		* myData ;
  MYSQL_RES *result;
  int num_rows;
  char basedir[300];
  char host[100],username[100],password[100],dbname[100];
  unsigned int portnum;
public :
	MySQLConn() ;
	void SelectDB(const char *db) ;
	void FlushTables(const char *tab);
        void Connect(const char *host=NULL,const char *username=NULL,const char *password=NULL,const char *dbname=NULL,unsigned int portnum=0) ;
	~MySQLConn() {
		if(result) mysql_free_result(result);
		if(myData) mysql_close( myData ) ;
	}
	void SetDataBasePath(const char *path)
	{
		if(path!=NULL) strcpy(basedir,path);
	}
	int DoQuery(const char *szSQL);
	//void MoveTable(const char *srctab,const char *dsttab,bool force);
	bool TouchTable(const char *tabname,bool clean=false) ;
	bool TouchDatabase(const char *dbname,bool create=false);
	void RenameTable(const char *srctab,const char *dsttab,bool force);
};
#endif
#endif
