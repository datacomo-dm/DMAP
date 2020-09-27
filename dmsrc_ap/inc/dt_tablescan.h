#ifndef DT_TABLESCAN_H
#define DT_TABLESCAN_H
// This is declaration of DT_Scan's inherit class 
#include "dt_svrlib.h"
#include "dt_scan.h"


class TableScan :public DT_Scan,file_mt {
	int tabid;
	char databasename[100];
	char tablename[200];
	int firstfid;
	int recordnum;
	int dts;
	int openfirstfile(int fid) ;
public :
	const char * GetDbName() ;
	const char * GetTbName() ;
	TableScan(int _dts);
	int OpenTable(int _tabid) ;
	int OpenTable(const char *dbname,const char *tbname);
	int GetNextBlockMt() ;
	virtual ~TableScan() ;
};
#endif
