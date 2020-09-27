#include "dtio.h"
#ifndef MYSQL_SERVER

dtioMyTableGroup::dtioMyTableGroup(dtioStream *_dtio,int _type,const char *_desc):myio(_dtio)
	{
		type=_type;
		tabnum=0;
		dtio=_dtio;
		if(_desc!=NULL) strcpy(desc,_desc);
		else desc[0]=0;
		myio.SetType(_type);
	}

	void dtioMyTableGroup::AddTable(const char *dbn,const char *tbn,int compressflag) 
	{
		myio.SetTable(dbn,tbn);
		myio.Serialize(compressflag);
	}
	
	int dtioMyTableGroup::GetTableNum() 
	{
		return myio.GetTableNum();
	}
	
	int dtioMyTableGroup::SearchTable(int rid,char *dbn,char *tbn)
	{
		myio.SetTableByID(rid);
		myio.GetDbName(dbn);
		myio.GetTableName(tbn);
		return 0;
	}
	
	int dtioMyTableGroup::Restore(MySQLConn &conn,int rid)
	{
		myio.SetTableByID(rid);
		myio.Deserialize();
		char dbn[120],tbn[120];
		myio.GetDbName(dbn);myio.GetTableName(tbn);
		AfterEachRestore(conn,dbn,tbn);
		return 0;
	}
	
	int dtioMyTableGroup::RestoreAll(MySQLConn &conn)
	{
		int tabn=GetTableNum();
		for(int i=0;i<tabn;i++) 
			Restore(conn,i);
		AfterRestoreAll(conn);
		return 0;
	}

#endif
