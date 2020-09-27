#ifndef DT_SCAN_H
#define DT_SCAN_H
class DT_Scan {
public :
	DT_Scan() {};
	virtual const char * GetDbName()=0;
	virtual const char * GetTbName()=0;
	virtual ~DT_Scan() {};
	virtual int OpenTable(int _tabid)=0;
	virtual int OpenTable(const char *dbname,const char *tbname)=0;
	virtual int GetNextBlockMt()=0;
};
DT_Scan *CreateDTScan(int _dts);
#endif
