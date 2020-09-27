// AutoHandle.h: interface for the AutoHandle class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_AUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_)
#define AFX_AUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
#include <string.h>
#include <stdarg.h>
#include <time.h>

#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_REALTIME
#endif

#include "wdbi_inc.h"


class  DllExport AutoHandle  
{
public:
	int handle;
	int refct;
	AutoHandle() {handle=0;refct=0;}
	AutoHandle(int id)
	{
		handle=id;		
		refct=0;
	}
	virtual void SetHandle(int hd,bool _ref=false) {
		if(handle &&refct==0) wocidestroy(handle);
		handle=hd;
		if(_ref) refct=1;
		else refct=0;
	}
	void Cap() { refct++;}
	void Rel() { refct--;}
	inline operator int() {
		return handle;
	}
	//inline virtual operator =(AutoHandle &src) {
	//	SetHandle(src.handle);
	//	Cap();
	//}
	virtual int Wait() {
		return wociWaitLastReturn(handle);
	}
	virtual ~AutoHandle(){
		if(handle>0 && refct==0) 
			wocidestroy(handle);
	}
};

class  DllExport AutoStmt :public AutoHandle {
	char sqlstmt[MAX_STMT_LEN];
	int dbc;
public :
	virtual ~AutoStmt() {};
	AutoStmt(int sess) {
		if(sess) {
			handle=wociCreateStatment(sess);
			dbc=sess;
		}
	}
	void Create(int sess) {
		int hd=wociCreateStatment(sess);
		SetHandle(hd);
		dbc=sess;
	}
	bool BindStr(int pos,char *str,int len) {
		return wociBindStrByPos(handle,pos,str,len);
	}
	bool BindDate(int pos,char *dt) {
		return wociBindDateByPos(handle,pos,dt);
	}
	bool BindInt(int pos,int *val) {
		return wociBindIntByPos(handle,pos,val);
	}
	bool BindDouble(int pos,double *val) {
		return wociBindDoubleByPos(handle,pos,val);
	}
	int Execute(int times) {
		return wociExecute(handle,times) ;
	}
	int ExecuteAt(int times,int offset) {
		return wociExecuteAt(handle,times,offset) ;
	}
#ifdef __unix
    bool vPrepare(const char *format, va_list argptr)
#else
    bool vPrepare(const char *format, va_list argptr)
#endif
	{
		vsprintf(sqlstmt,format,argptr);
		return wociPrepareStmt(handle,sqlstmt) ;
	}

	bool Prepare(const char *format,...) {
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=vPrepare(format,vlist);
		va_end(vlist);
		return rt;
	};
	
	int DirectExecute(const char *format,...) {
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		vPrepare(format,vlist);
		va_end(vlist);
		Execute(1);
		int rn=Wait();
		wociCommit(dbc);
		return rn;
	}
	// Execute but not commit
	int ExecuteNC(const char *format,...) {
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		vPrepare(format,vlist);
		va_end(vlist);
		Execute(1);
		int rn=Wait();
		return rn;
	}

};

class DllExport AutoMt :public AutoHandle {
	AutoStmt stmth;
	int dbc;
	int default_maxrows;
	int *collen;
	int *coltp;
	int colct;
	char **colptr;
	bool autoclear;
	int  *pVarLCol;
public :
	AutoMt(int _dbc,int maxrows=100):AutoHandle(),stmth(_dbc){
		dbc=_dbc;default_maxrows=maxrows;collen=NULL;colptr=NULL;
		coltp=NULL;
		pVarLCol = NULL;
		handle=wociCreateMemTable();
		autoclear=true;
	}
	AutoStmt &GetStmt() {return stmth;}
	void SetAutoClear(bool _val) {autoclear=_val;}
	virtual ~AutoMt() {
		if(collen) delete []collen;
		if(colptr) delete []colptr;
		if(coltp) delete []coltp;
		delete [ ]  pVarLCol;
	};
	inline void SetMaxRows(int maxrn) {
		default_maxrows=maxrn;
	}
	inline int GetMaxRows() {return default_maxrows;}
	inline int GetRows() { return wociGetMemtableRows(handle);}
        inline void Reset() {
		wociReset(handle);
	}
	inline int CompareRows(int row1,int row2,int *colp,int coln) {
		
		int rt;
		for(int i=0;i<coln;i++) {
			int c=colp[i];
			int l=collen[c];
			if(coltp[i]==COLUMN_TYPE_CHAR) {

				if(pVarLCol[c])
				{
				    int Cellen = 0;
                                const char *pStr1 =NULL;const char *pStr2=NULL;
				     pStr1 = GetVarLenStrAddr(c, row1, &Celllen);
				     pStr2 = GetVarLenStrAddr(c, row2, &Celllen);
				     if(rt=strcmp(pStr1,pStr2),rt!=0) return rt;
				}
				else{
                                  if(rt=strcmp(colptr[c]+l*row1,
                                     colptr[c]+l*row2),rt!=0) return rt;
					}

			}
			else {
				if(rt=memcmp(colptr[c]+l*row1,
                            colptr[c]+l*row2,l),rt!=0) return rt;
			}
		}
		return 0;
	}
	inline void Clear() {
		if(autoclear)
		wociClear(handle);
	}
	inline int getColType(int pos) {
		return coltp[pos];
	}
	inline int getColLen(int pos) {
		return collen[pos];
	}

	//类型一致性检查，要求字段类型和字段宽度一致
	inline int CompareMt(int hd)  //0 for same ,1 for differ
	{
		if(handle<1 || colct==0) return 1;
		int colct1=wociGetColumnNumber(handle);
		if(colct1!=colct) return 1;
		int *collen1=new int[colct];
		char **colptr1=new char *[colct];
		int *coltp1=new int[colct];
		wociAddrFresh(hd,colptr1,collen1,coltp1);
		int i;
		for(i=0;i<colct;i++) {
			if(collen1[i]!=collen[i]) break;
			if(coltp1[i]!=coltp[i]) break;
		}
		delete []collen1;
		delete []colptr1;
		delete []coltp1;
		if(i==colct) return 0;
		return 1;
	}


	//类型兼容性检查，字段的存储可能性检查
	//  字符型和日期型的字段类型必须一致.
	//  数值型的类型可以不一致,但都是几种数值型字段之一
	//   对字段宽度不做检查
	inline int CompatibleMt(int hd)  //0 for same ,1 for differ
	{
		if(handle<1 || colct==0) return 1;
		int colct1=wociGetColumnNumber(hd);
		if(colct1!=colct) return 1;
		int *collen1=new int[colct];
		char **colptr1=new char *[colct];
		int *coltp1=new int[colct];
		wociAddrFresh(hd,colptr1,collen1,coltp1);
		int i;
		for(i=0;i<colct;i++) {
			if(coltp1[i]!=coltp[i]) {
			 if(coltp1[i]==COLUMN_TYPE_DATE || coltp1[i]==COLUMN_TYPE_CHAR ||
			  coltp[i]==COLUMN_TYPE_DATE || coltp[i]==COLUMN_TYPE_CHAR)
				break;
			}
		}
		delete []collen1;
		delete []colptr1;
		delete []coltp1;
		if(i==colct) return 0;
		return 1;
	}
	virtual void SetHandle(int hd,bool _ref=false) {
		AutoHandle::SetHandle(hd,_ref);
		default_maxrows=wociGetMaxRows(hd);
		AddrFresh();
	}

	inline bool CreateAndAppend(const char *tablename,int _dbc=0,bool forcecreate=false,bool autocommit=true) {
		if(_dbc==0) _dbc=dbc;
		if(!wociTestTable(_dbc,tablename)) 
			wociGeneTable(handle,tablename,_dbc);
		else if(forcecreate) {
			AutoStmt st(_dbc);
			st.Prepare("drop table %s",tablename);
			st.Execute(1);
		        st.Wait();	
			wociGeneTable(handle,tablename,_dbc);
		}
		return wociAppendToDbTable(handle,tablename,_dbc,autocommit);
	}

	inline int FetchFirst() {
		return wociFetchFirst(handle,default_maxrows);
	}

	inline int FetchFirst(const char *format,...) {
		if(stmth==0) 
		  stmth.Create(dbc);
		  #ifdef __unix
		  va_list vlist;
		  #else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=stmth.vPrepare(format,vlist);
		va_end(vlist);
		if(!rt) return -1;
	        Clear();
	//int hd=wociCreateMemTable();
		wociBuildStmt(handle,stmth,default_maxrows>10?default_maxrows:10);
		AddrFresh();
		//SetHandle(hd);
		return wociFetchFirst(handle,default_maxrows);
	}

	inline int FetchNext() {
		return wociFetchNext(handle,default_maxrows);
	}

	inline int FetchAppend() {
		return wociFetchAt(handle,default_maxrows,GetRows());
	}
	
	inline void SetIKGroupParam(int dtsrc,const char *srcgrpcols,
		const char *srcsumcols,const char *srckey=NULL,int dtref=0,const char *refgrpcols=NULL) {
		wociSetGroupSrc(handle,dtsrc);
		wociSetGroupSrcCol(handle,srcgrpcols);
		if(srckey) 
			wociSetIKGroupRef(handle,dtref,srckey);
		if(refgrpcols)
			wociSetGroupRefCol(handle,refgrpcols);
//		if(srcsumcols)
			wociSetSrcSumCol(handle,srcsumcols);
		Build();
	}

	inline void SetGroupParam(int dtsrc,const char *srcgrpcols,
		const char *srcsumcols,const char *srckey=NULL,int dtref=0,const char *refgrpcols=NULL) {
		wociSetGroupSrc(handle,dtsrc);
		wociSetGroupSrcCol(handle,srcgrpcols);
		if(srckey) 
			wociSetSortedGroupRef(handle,dtref,srckey);
		if(refgrpcols)
			wociSetGroupRefCol(handle,refgrpcols);
//		if(srcsumcols)
			wociSetSrcSumCol(handle,srcsumcols);
		Build();
	}

	inline void SetDBC(int _dbc)
	{
		stmth.Create(_dbc);
		dbc=_dbc;
	}

	inline void Build(int stmt) {
		wociBuildStmt(handle,stmt,default_maxrows>10?default_maxrows:10);
		AddrFresh();
	}
	
	inline void Build() {
		wociBuild(handle,default_maxrows);
		AddrFresh();
	}
	inline int GetColumnNum() {
		return wociGetColumnNumber(handle);
	}
	inline void AddrFresh() {
		colct=wociGetColumnNumber(handle);
		if(colct<1) return;
		if(collen) delete []collen;
		if(colptr) delete []colptr;
		if(coltp) delete []coltp;
		collen=new int[colct];
		colptr=new char *[colct];
		coltp=new int[colct];
		wociAddrFresh(handle,colptr,collen,coltp);
              pVarLCol = new int[colct];
		memset(pVarLCol,0,colct*sizeof(int));
		_wdbiGetVarCol(handle, pVarLCol, NULL);  //获取那个列为变长列
	}
      inline BOOL isVarStrCol(int nCol)
      	{
            return pVarLCol[nCol];
	 }

        inline BOOL isVarStrCol(char *pColName)
       {
           int nCol = _wdbiGetColumnPosByName(handle ,pColName);
            return pVarLCol[nCol];
	 }

	 inline int GetColPosByName(char *pColName)
	 {
            return    _wdbiGetColumnPosByName(handle ,pColName);
	 }
	 
	inline void QuickCopyFrom(AutoMt *from,int toStart,int fromrn) {
		for(int i=0;i<colct;i++) {
			int len=collen[i];
			if(pVarLCol[i])
			{
			    char *pStr =new char[len];
			   memset(pStr,0,len*sizeof(char));
                        _wdbiGetStrValByPos(from->handle, i, fromrn, pStr);
			   _wdbiSetStrValues(handle, i, toStart, 1, pStr);
			   delete [ ] pStr;
			}
			else{
			memcpy(colptr[i]+len*toStart,from->colptr[i]+len*fromrn,len);

			}

		}
	}

	inline int FetchAll() {
		return wociFetchAll(handle);
	}
	inline int FetchAll(const char *format,...) {
		if(stmth==0) 
			stmth.Create(dbc);
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=stmth.vPrepare(format,vlist);
		va_end(vlist);
		if(!rt) return -1;
		//int hd=wociCreateMemTable();
		Clear();
		wociBuildStmt(handle,stmth,default_maxrows>10?default_maxrows:10);
		AddrFresh();
		//SetHandle(hd);
		return FetchAll();
	}
	
	inline int FetchAllAt(int offset,const char *format,...) {
		if(stmth==0) 
                stmth.Create(dbc);
               #ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=stmth.vPrepare(format,vlist);
		va_end(vlist);
		if(!rt) return -1;
		//int hd=wociCreateMemTable();
		//Clear();
		//wociBuildStmt(handle,stmth,default_maxrows>10?default_maxrows:10);
		//AddrFresh();
		//SetHandle(hd);
		wociReplaceStmt(handle,stmth);
		return wociFetchAllAt(handle,offset);
	}
	
	inline int GetStrAddr(const char *col,unsigned int rowst,char **pstr,int *celllen) {
		return wociGetStrAddrByName(handle,col,rowst,pstr,celllen) ;
	}

	const char * GetVarLenStrAddr(const char *col,unsigned int rowst,int *celllen){
            return _wdbiGetVarLenStrAddrByName(handle,col, rowst, celllen);
	}

	
	inline int GetDateAddr(const char *col,unsigned int rowst,char **pstr) {
		return wociGetDateAddrByName(handle,col,rowst,pstr) ;
	}
	inline int GetDoubleAddr(const char *col,unsigned int rowst,double **pstr) {
		return wociGetDoubleAddrByName(handle,col,rowst,pstr);
	}
	inline int GetIntAddr(const char *col,unsigned int rowst,int **pstr) {
		return wociGetIntAddrByName(handle,col,rowst,pstr) ;
	}
	inline int GetLongAddr(const char *col,unsigned int rowst,LONG64 **pstr) {
		return wociGetLongAddrByName(handle,col,rowst,pstr) ;
	}
	
	inline int GetStrAddr(int col,unsigned int rowst,char **pstr,int *celllen) {
		return wociGetStrAddrByPos(handle,col,rowst,pstr,celllen) ;
	}
       //获取变长的数据
       const char * GetVarLenStrAddr(const char *col,unsigned int rowst,int *celllen){
            return _wdbiGetVarStrAddrByPos(handle,col, rowst, celllen);
	}

	
	inline int GetDateAddr(int col,unsigned int rowst,char **pstr) {
		return wociGetDateAddrByPos(handle,col,rowst,pstr) ;
	}
	inline int GetDoubleAddr(int col,unsigned int rowst,double **pstr) {
		return wociGetDoubleAddrByPos(handle,col,rowst,pstr);
	}
	inline int GetIntAddr(int col,unsigned int rowst,int **pstr) {
		return wociGetIntAddrByPos(handle,col,rowst,pstr) ;
	}

	inline int GetLongAddr(int col,unsigned int rowst,LONG64 **pstr) {
		return wociGetLongAddrByPos(handle,col,rowst,pstr) ;
	}

	inline void *PtrVoid(int col,unsigned int rowst) {
		return colptr[col]+(long)rowst*collen[col];
	}

	inline char *PtrStr(int col,unsigned int rowst) {
		if (pVarLCol[col])
		{ errprintf("The %d  VarCol use char * in PtrStr,shud be use const char *",col);return NULL;}
		return colptr[col]+(long)rowst*collen[col];
	}

       inline  const char *PtrVarStr(int col,unsigned int rowst)
	{
            if(pVarLCol[col])
            	{ 
            	   int   celllen;
                 return GetVarLenStrAddr( col,rowst, &celllen);
	      }
		else{
                  (const char *)colptr[col]+(long)rowst*collen[col];
		}
	}
       inline const  char *PtrVarStr(const char *colname,unsigned int rowst) 
	{
		int col=wociGetColumnPosByName(handle,colname);
		if(col > colct){return NULL;}
		if (pVarLCol[col])
             { 
                 int   celllen;
                 return GetVarLenStrAddr( col,rowst, &celllen);
	      }
		else{
                  (const char *)colptr[col]+(long)rowst*collen[col];
		}
	}
	
	inline char *PtrStr(const char *colname,unsigned int rowst)
	{
		int col=wociGetColumnPosByName(handle,colname);
		if (pVarLCol[col])
		{ errprintf("The %s  VarCol use char * in PtrStr,shud be use const char *",colname);return NULL;}
		return colptr[col]+(long)rowst*collen[col];
	}
	inline char *PtrDate(int col,unsigned int rowst) {
		return colptr[col]+rowst*7;
	}
	inline char *PtrDate(const char *colname,unsigned int rowst) {
		int col=wociGetColumnPosByName(handle,colname);
		return colptr[col]+rowst*7;
	}
	inline double *PtrDouble(int col,unsigned int rowst) {
		return (double *)(colptr[col]+rowst*sizeof(double));
	}
	inline double *PtrDouble(const char *colname,unsigned int rowst) {
		int col=wociGetColumnPosByName(handle,colname);
		return (double *)(colptr[col]+rowst*sizeof(double));
	}
	inline int *PtrInt(int col,unsigned int rowst) {
		return (int *)(colptr[col]+rowst*sizeof(int)); 
	}
	inline int *PtrInt(const char *colname,unsigned int rowst) {
		int col=wociGetColumnPosByName(handle,colname);
		return (int *)(colptr[col]+rowst*sizeof(int)); 
	}
	inline LONG64 *PtrLong(int col,unsigned int rowst) {
		return (LONG64 *)(colptr[col]+rowst*sizeof(LONG64)); 
	}

	inline LONG64 *PtrLong(const char *colname,unsigned int rowst) {
		int col=wociGetColumnPosByName(handle,colname);
		return (LONG64 *)(colptr[col]+rowst*sizeof(LONG64)); 
	}
	
	inline void SetStr(int col,unsigned int rowst,char *str,int rowNm=1)
	{
	       if (pVarLCol[col])
		{
                  _wdbiSetStrValues(handle,col,rowst, rowNm, str);
		}
		else{
                 memcpy(colptr[col]+(long)rowst*collen[col],str,collen[col]*rowNm);
		} 
	}

        inline void SetStr(const char *colname,unsigned int rowst,char *str,int rowNm=1)
	{
	       int col=wociGetColumnPosByName(handle,colname);
		if(col > colct){return NULL;} 
	       if (pVarLCol[col])
		{
                  _wdbiSetStrValues(handle,col,rowst, rowNm, str);
		}
		else{
                 memcpy(colptr[col]+(long)rowst*collen[col],str,collen[col]*rowNm);
		} 	
	}
	
	inline void SetDate(int col,unsigned int rowst,char *str) {
		memcpy(colptr[col]+rowst*7,str,7);
	}
	inline void GetStr(int col,unsigned int rowst,char *str,int rowNm = 1) 
	{
		if(pVarLCol[col])
	       {
                  _wdbiGetStrValByPos(handle, col,  rowst, str,rowNm);
		}
		else{
                 memcpy(str,colptr[col]+(long)rowst*collen[col],collen[col]*rowNm);
		}
	}
	inline void GetDate(int col,unsigned int rowst,char *str) {
		memcpy(str,colptr[col]+rowst*7,7);
	}

	inline int GetStr(const char *col,unsigned rowst,char *pstr) {
		pstr[0]=0;
		int  rt=wociGetStrValByName(handle,col,rowst,pstr) ;
		int l=strlen(pstr);
		for(int i=1;i<l;i++) 
		{
		     if(pstr[l-i]==' ') pstr[l-i]=0;
		    else break;
		}
		return rt;
	}
	inline int GetDate(const char *col,unsigned rowst,char *pstr) {
		return wociGetDateValByName(handle,col,rowst,pstr) ;
	}
	inline double GetDouble(const char *col,unsigned rowst) {
		return wociGetDoubleValByName(handle,col,rowst);
	}
	inline int GetInt(const char *col,unsigned rowst) {
		return wociGetIntValByName(handle,col,rowst) ;
	}
	inline LONG64 GetLong(const char *col,unsigned rowst) {
		return wociGetLongValByName(handle,col,rowst) ;
	}

	inline int GetPos(const char *colname,int coltype) {
		int pos=wociGetColumnPosByName(handle,colname);
		if(wociGetColumnType(handle,pos)!=coltype) {
			errprintf("Column %s have a different type",colname);
			return -1;
		}
		return pos;
	}
};

class DllExport TradeOffMt {
	bool first;
	AutoMt firstmt,secondmt;
	bool init,pmode;
public :
	TradeOffMt(int dbc,int maxrows):firstmt(dbc,maxrows),
	secondmt(dbc,maxrows) {
		Init();pmode=false;
	}
	void SetPesuado(bool val) {
		pmode=val;
	}
	void Init() {
		init=true;first=true;
	}

	AutoMt *Cur() {
		return pmode?&firstmt:(first?&firstmt:&secondmt);
	}
	AutoMt *Next() {
		return pmode?&firstmt:(first?&secondmt:&firstmt);
	}
	void flip() {
		first=!first;
	}
	int FetchNext() {
		return Next()->FetchNext();
	}
	int FetchFirst() {
		first=true;init=true;
		return Cur()->FetchFirst();
	}
	int Wait() {
		if(!init) {
			flip();
			return Cur()->Wait();
		}
		init=false;
		return Cur()->Wait();
	}
};

#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
class DllExport mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=ed.tv_sec-st.tv_sec;
	ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
	st.tv_sec=ed.tv_sec;st.tv_nsec=ed.tv_nsec;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};

class DllExport autotimer {
	mytimer *ptm;
public :
	autotimer(mytimer *p) {
		ptm=p;
		p->Start();
	}
	~autotimer() {
		ptm->Stop();
	}
};
#endif

#endif // !defined(AFX_AUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_)
