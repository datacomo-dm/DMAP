
#include "dt_tablescan.h"

DT_Scan *CreateDTScan(int _dts) {
   return new TableScan(_dts);
}

int TableScan::openfirstfile(int fid) {
		AutoMt mt(dts,10);
		mt.FetchAll("select * from dt_datafilemap where fileid=%d",
			fid);
		int rn=mt.Wait();
		if(rn<1) 
			ThrowWith("Data file not found ,fileid:%d,table :'%s.%s'.",
			     fid,databasename,tablename);
		Open(mt.PtrStr("filename",0),0,fid);
		return 1;
}
	
const char * TableScan::GetDbName() { return databasename;}
	
const char * TableScan::GetTbName() { return tablename;}
	
TableScan::TableScan(int _dts):file_mt(false) {dts=_dts;};
	
int TableScan::OpenTable(int _tabid) {
		AutoMt mt(dts,10);
		mt.FetchAll("select * from dt_table where tabid=%d "
			,_tabid);
		int rn=mt.Wait();
		if(rn<1) 
			ThrowWith("Table id :%d could not be found!",_tabid);
		if(mt.GetInt("recordnum",0)<1) 
			ThrowWith("Table id:%d is empty.",_tabid);
		strcpy(databasename,mt.PtrStr("databasename",0));
		strcpy(tablename,mt.PtrStr("tabname",0));
		recordnum=mt.GetInt("recordnum",0);
		return openfirstfile(mt.GetInt("firstdatafileid",0));
}

int TableScan::OpenTable(const char *dbname,const char *tbname)
{
		AutoMt mt(dts,10);
		mt.FetchAll("select * from dt_table where databasename='%s' "
			" and tabname='%s' ",dbname,tbname);
		int rn=mt.Wait();
		if(rn<1) 
			ThrowWith("Table '%s.%s' could not be found!",dbname,tbname);
		if(mt.GetInt("recordnum",0)<1) 
			ThrowWith("Table '%s.%s' is empty.",dbname,tbname);
		strcpy(databasename,dbname);
		strcpy(tablename,tbname);
		recordnum=mt.GetInt("recordnum",0);
		return openfirstfile(mt.GetInt("firstdatafileid",0));
	}

int TableScan::GetNextBlockMt() {
		char *resptr;
		if(ReadMtOrBlock(-1,0,0,&resptr)==-1) return 0;
		return GetBlockRowNum();
}

TableScan::~TableScan() {};

