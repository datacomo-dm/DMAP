#ifndef DTIO_MAP_H
#define DTIO_MAP_H
#include "dtio_common.h"

#define DTIO_MAXUNITNUM 		1000 

class DTIOExport ParamMT {
	char sortcols[300];
	int sortcolct;
	ParamMT *parentMT;
protected :
	int mt;
	void AddColumn(const char *cname,const char *dspnm,int coltp,int len,int scale=0) {
		wociAddColumn(mt,(char *)cname,(char *)dspnm,coltp,len,scale);
	}
	bool Build(int maxrows) {
		return (bool)wociBuild(mt,maxrows);
	}
	int SortByColumns( const char * first, ... )
	{
		int count = 0;
		const char *col=first;
		va_list marker;
		sortcols[0]=0;
		va_start( marker, first );     // Initialize variable arguments. 
		while( col != NULL )
		{
			strcat(sortcols,col);
			count++;
			col = va_arg( marker, const char *);
			if(col) strcat(sortcols,",");
		}
		va_end( marker );              // Reset variable arguments.      
		wociClearSort(mt) ;
		wociSetSortColumn(mt,sortcols);
		wociSort(mt);
		sortcolct=count;
		return count;
	}
	
	int Search(void *val,...) {
	 void *ptr[10];
	int count = 0;
	void *v=val;
	va_list marker;
	va_start( marker, val );     // Initialize variable arguments. 
	while( v != NULL )
	{
      ptr[count]=v;
      count++;
      v = va_arg( marker, void *);
	}
	ptr[count]=NULL;
	va_end( marker );              // Reset variable arguments.      
	return wociSearch(mt,ptr);
	}
public :
	ParamMT() {
	    mt=wociCreateMemTable();
		sortcolct=0;
		parentMT=NULL;
	}
	void SetParentMT(ParamMT *pmt) {
		parentMT=pmt;
	}
	ParamMT *GetParentMT() { return parentMT;}
	virtual ~ParamMT() {wocidestroy(mt);}
	int GetMt() {
		//BUG WRAP . 插入数据后未能有效建立排序,在返回的MT上作wociMTCompactPrint报错
		//wociSort(mt);
		return mt;
	}
	void CopyMt (int _mt) {
	 if(mt) wocidestroy(mt);
	 mt=wociCreateMemTable();
 	 wociCopyColumnDefine(mt,_mt,NULL);
 	 wociBuild(mt,wociGetMemtableRows(_mt));
	 wociCopyRowsTo(_mt,mt,0,0,wociGetMemtableRows(_mt));
	 if(sortcolct>1) {
	  	wociClearSort(mt) ;
	  	wociSetSortColumn(mt,sortcols);
	  	wociSort(mt);
	 }
	}
	int GetCopyMt() {
	  int _mt=wociCreateMemTable();
 	  wociCopyColumnDefine(_mt,mt,NULL);
 	  wociBuild(_mt,wociGetMemtableRows(mt));
	  wociCopyRowsTo(mt,_mt,0,0,wociGetMemtableRows(mt));
	  if(sortcolct>1) {
	  	wociClearSort(_mt) ;
	  	wociSetSortColumn(_mt,sortcols);
	  	wociSort(_mt);
	  }
	  return _mt;
	}
};
	
//存储单元索引映射表
//#define DTIO_UNITMAPFLAG	0x3165
class DTIOExport dtioUnitMap :public ParamMT 
{
public :
	dtioUnitMap(int unitnum=DTIO_MAXUNITNUM):ParamMT() {
	  AddColumn("TYPE","类型",COLUMN_TYPE_INT,0);
	  AddColumn("PREFIX","位置",COLUMN_TYPE_CHAR,PATH_LEN);
	  AddColumn("NAME","名称",COLUMN_TYPE_CHAR,PATH_LEN);
	  AddColumn("OFFSET","位移",COLUMN_TYPE_NUM,16,0); //逻辑偏移,不负责到物理文件的映射.
	  AddColumn("LENGTH","数量",COLUMN_TYPE_NUM,16,0);
	  Build(unitnum);
	  SortByColumns("TYPE","PREFIX","NAME",NULL);
	}
	~dtioUnitMap() { }
	int GetItemNum(int utype) {
		int *ptype;
		wociGetIntAddrByName(mt,"TYPE",0,&ptype);
		int rn=wociGetMemtableRows(mt);
		int rct=0;
		for(int i=0;i<rn;i++) {
			if(ptype[i]==utype) rct++;
		}
		return rct;
	}
	void Reset() {
	  wociReset(mt);
	  SortByColumns("TYPE","PREFIX","NAME",NULL);
	}	
	int GetItem(int utype,int rid,char *prefix,char *name,double &offset,double &length) {
		int *ptype;
		wociGetIntAddrByName(mt,"TYPE",0,&ptype);
		int rn=wociGetMemtableRows(mt);
		int rct=0;
		for(int i=0;i<rn;i++) {
			if(ptype[i]==utype) {
				if(rct==rid) {
					wociGetStrValByName(mt,"PREFIX",i,prefix);
					wociGetStrValByName(mt,"NAME",i,name);
					offset=wociGetDoubleValByName(mt,"OFFSET",i);
					length=wociGetDoubleValByName(mt,"LENGTH",i);
					return i;
				}
				rct++;
			}
		}
		return -1;
	}
	
	bool CheckUnit(int utype,const char *prefix,const char *uname) {
		int of=Search(&utype,prefix,uname,NULL);
		if(of<0) {
			return false;
		}
		return true;
	}

	bool GetUnit(int utype,const char *prefix,const char *uname,double &offset,double &length) {
		int of=Search(&utype,prefix,uname,NULL);
		if(of<0) {
			ThrowWith("在备份文件中没有'%s.%s'表.",prefix,uname);
			return false;
		}
		offset=wociGetDoubleValByName(mt,"OFFSET",of);
		length=wociGetDoubleValByName(mt,"LENGTH",of);
		return true;
	}
	
	void MoveUnit(int utype,const char *prefix,const char *uname,const char* nprefix,const char *nuname=NULL) {
		int of=Search(&utype,prefix,uname,NULL);
		if(of>=0) {
			if(nprefix!=NULL)
			wociSetStrValues(mt,1,of,1,nprefix);
			if(nuname!=NULL)
			wociSetStrValues(mt,2,of,1,nuname);
		}
	}
	
	void SetUnit(int utype,const char *prefix,const char *uname,double offset,double length) {
		int of=Search(&utype,prefix,uname,NULL);
		if(of<0) { // Add new unit
		  void *ptr[6];
		  ptr[0]=(void *)&utype;
		  ptr[1]=(void *)prefix;
			ptr[2]=(void *)uname;
			LONG64 v1,v2;
			v1=(LONG64)offset;
			v2=(LONG64)length;
			ptr[3]=&v1;
			ptr[4]=&v2;
			ptr[5]=NULL;
			wociInsertRows(mt,ptr,NULL,1);
		}
		else {
			LONG64 *pv;
			wociGetLongAddrByName(mt,"OFFSET",of,&pv);
			*pv=(LONG64)offset;
			wociGetLongAddrByName(mt,"LENGTH",of,&pv);
			*pv=(LONG64)length;
		}
	}
	
};

/*

class dtioDTMap : public ParamMt
{
public : 
	dtioUnitMap():ParamMt() {
	  AddColumn("dbname",COLUMN_TYPE_CHAR,30);
	  AddColumn("tname",COLUMN_TYPE_CHAR,50);
	  AddColumn("OFFSET",COLUMN_TYPE_DOUBLE,0); //逻辑偏移,不负责到物理文件的映射.
	  AddColumn("LENGTH",COLUMN_TYPE_DOUBLE,0);
	  Build(DTIO_MAXUNITNUM);
	  SortByColumns("dbname","dbname",NULL);
	}
	~dtioUnitMap() { }
	bool GetUnit(const char *dbname,const char *tname,double &offset,double &length) {
		int of=Search(dbname,tname);
		if(of<0) return false;
		offset=wociGetDoubleValByName(mt,"OFFSET",of);
		length=wociGetDoubleValByName(mt,"LENGTH",of);
		return true;
	}
	bool GetUnit(int id,char *dbname,char *tname,double &offset,double &length) {
		wociGetStrValByName(mt,"dbname",id,dbname);
		wociGetStrValByName(mt,"tname",id,tname);
		offset=wociGetDoubleValByName(mt,"OFFSET",id);
		length=wociGetDoubleValByName(mt,"LENGTH",id);
		return true;
	}
	int GetItemNum() { return wociGetMemtableRows(mt);}
	void SetUnit(const char *dbname,const char *tname,double offset,double length) {
		int of=Search(dbname,tname);
		int of=wociSearch(mt,ptr);
		if(of<0) { // Add new unit
		  	void *ptr[5];
		  	ptr[0]=(void *)&dbname;
			ptr[1]=(void *)tname;
			ptr[2]=&offset;
			ptr[3]=&length;
			ptr[4]=NULL;
			wociInsertRow(mt,ptr,1);
		}
		else {
			double *pv;
			wociGetStrAddrByName(mt,"OFFSET",of,&pv);
			*pv=offset;
			wociGetStrAddrByName(mt,"LENGTH",of,&pv);
			*pv=length;
		}
	}
};
*/
#endif
