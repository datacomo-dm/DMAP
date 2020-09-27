
#ifndef DTIO_MT_H
#define DTIO_MT_H
#define MTBLOCKFLAG  0x3171
#define MTBLOCKSTART 0x3173
#define MTNAMELEN 30
#include "dtio_common.h"
#include "AutoHandle.h"
class dtioStream;
class mttables {
protected :	
  int maxtabnum;
  int tabnum; // memory table numbers;
  int splitrows; //每个数据块的大小（数据行数).
  int *pmt;
  char *mtnames;
  bool *pdestroy;
  dtioStream *pdtio;
public :
	void Clear();
  mttables(int _maxtabnum=20,int _splitrows=-1);
  uint4 AppendMt(int mt,const char *mtname=NULL,bool copymt=true) ;
  int GetMt(const char *mtname) ;
  bool DeleteMt(const char *mtname);
  const char * GetString(const char *mtname,const char *colname,const int rid=0);
  const char * GetDate(const char *mtname,const char *colname,const int rid=0);
  void SetString(const char *mtname,const char *colname,const char *value,int rid=0);//rid=-1 indicate to set all rows
  void SetDate(const char *mtname,const char *colname,const char *value,int rid=0);//rid=-1 indicate to set all rows
  void SetInt(const char *mtname,const char *colname,int value,int rid=0);//rid=-1 indicate to set all rows
  void SetDouble(const char *mtname,const char *colname,const double value,int rid=0);//rid=-1 indicate to set all rows
  int GetInt(const char *mtname,const char *colname,const int rid=0);
  int GetRowNum(const char *mtname);
  double GetDouble(const char *mtname,const char *colname,int rid=0);
  uint8B Serialize(dtioStream *dtio,int compressflag=0,bool compact=false);
  //兼容性检查
  virtual void CheckCompact() { };
  uint8B  Deserialize(dtioStream *dtio);
  ~mttables() ;
};

class dtparams_mt:public mttables
{
	char dbname[PATH_LEN];
	char tbname[PATH_LEN];
	int dtfilenum;
	char partidxtbn[PATH_LEN];
	
	int *pfileid;
public :
	const char *PartIndexTbn(int idx,int partid) {
		sprintf(partidxtbn,"%sidx%d_p_%d",GetString("DP_TABLE","tabname",0),GetInt("DP_INDEX","indexgid",idx),
		  GetInt("DP_DATAPART","datapartid",idx));
		return partidxtbn;
	bool IsPartIndex(const char * basepath) {
		char tmpfn[DTP_FILENAMELEN];
		sprintf(tmpfn,"%s/%s/%s.frm",basepath,GetString("DP_TABLE","databasename",0),PartIndexTbn(0,0));
		FILE *fp=fopen(tmpfn,"rb");
		if(fp) {
			fclose(fp);
			return true;
		}
		return false;
	}
		dtparams_mt(const char *dbn,const char *tbn);
		int GetSoledIndexNum() {
			return wociGetMemtableRows(GetMt("DP_INDEX"));
		}
		void GetIndexMap(int *ptaboff,int *psubidxid,int *pidxfieldnum);
		int GetTotIndexNum() {
			return wociGetMemtableRows(GetMt("DP_INDEX_ALL"));
		}
		/*
		void ResetIndexTable(const char *newtbn) {
			int sn=GetSoledIndexNum();
			char name[300];
			for(int i=0;i<sn;i++) {
				sprintf(name,"%s_idx%d",newtbn,i+1);
				SetString("DT_INDEX","indextabname",name,i); //与dtioDTTable::SerializeIndex(dtioDTTable.cpp:61)必须一致
			}
		}
		*/
		void GetIndexTable(int id,char *dbn,char *tbn)
		{
			strcpy(dbn,GetString("DP_TABLE","databasename",0));
			strcpy(tbn,GetString("DP_INDEX","indextabname",id));
			if(strlen(tbn)==0) 
				sprintf(tbn,"%sidx%d",GetString("DP_TABLE","tabname",0),GetInt("DP_INDEX","indexgid",id));
		}
		void GetIndexFile(int id,char *fn)
		{
			strcpy(fn,GetString("DP_DATAFILEMAP","idxfname",id));
		}
		void GetDataFile(int id,char *fn)
		{
			strcpy(fn,GetString("DP_DATAFILEMAP","filename",id));
		}
		void GetDataFileMap(int *fnid,char *fns,int fnlen);
		int GetDataFileNum()
		{
			return wociGetMemtableRows(GetMt("DP_DATAFILEMAP"));
		}
		void GetFirstFile(int &fnid,char *filename){
			fnid=GetInt("DP_TABLE","firstdatafileid",0);
			//GetDataFile(0,filename);
			strcpy(filename,SearchFile(fnid));
		}
		uint8B GetRecordNum() ;
		void restoreCheck(int dts,bool &overwritable,bool &duprecords,char *msg);
		void adjparam(const char *newdir);
		void restore(int dts,const char *newdir);
		const char *SearchFile(int fid) ;
 	        virtual void CheckCompact();
		void FetchParam(int dts,bool psoleindexmode,dtioStream *_pdtio);
};
#endif
