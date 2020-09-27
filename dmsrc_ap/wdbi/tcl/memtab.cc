#include "cpptcl.h"
#include <iostream>
#include "AutoHandle.h"

using namespace std;
using namespace Tcl;
#define MAXSTR 16000
static char strresult[MAXSTR];
extern bool asyncMode;
object hello()
{
   //std::string err("msg error throwed!");
   //tcl_error er(err);
   //throw tcl_error("1234");  
   //cout << "Hello C++/Tcl!" << endl;
   interpreter i;
   object dt;
   dt.append(i,object(1));
   dt.append(i,object(2));
   dt.append(i,object(3));
   dt.append(i,object(4));
   dt.append(i,object(5));
   dt.append(i,object(6));
   dt.append(i,object(7));
   //sprintf(str,"hello, %d. ",++ct);
   return dt;
}

const char *tclMTDesc(int memtab) {
	strresult[0]=0;
 _wdbiGetColumnInfo(memtab,strresult,false);
  return strresult;
}

class dbCon:public AutoHandle {
	public :
		dbCon(const char *usn,const char *pwd,const char *svc,object const &argv){
			interpreter interp(argv.get_interp(), false);
			size_t argc = argv.length(interp);
			int dbtype=DTDBTYPE_ORACLE;
			if(argc>1) throw tcl_error("Too many parameters");
			if(argc==1) {
				object o(argv.at(interp, 0));
				if(strcmp("oracle",(const char *) o.get())==0) dbtype=DTDBTYPE_ORACLE;
				else if(strcmp("odbc",(const char *) o.get())==0) dbtype=DTDBTYPE_ODBC;
				else throw tcl_error("Invalid database type");
			}					
			handle= _wdbiCreateSession(usn,pwd,svc,dbtype) ;
			refct=0;
		};
		
		int Get() {
			return handle;
		}
		int Wait() {
			return AutoHandle::Wait();
	  }
	  void Commit() {
	  	wociCommit(handle);
	  }
	  void Rollback() {
	  	wociRollback(handle);
	  }
};

class dbStmt:public AutoStmt {
	public :
		dbStmt(int sess):AutoStmt(sess) {}
		
		void Create(int sess) {
			AutoStmt::Create(sess);
		}
		int Get() {
			return handle;
		}
	  int Wait() 
	  {
	  	AutoStmt::Wait();
	  }
		int Execute(int times) {
			return AutoStmt::Execute(times);
		}
		
		int ExecuteAt(int times,int offset) {
			return AutoStmt::ExecuteAt(times,offset);
		}
		
		int Prepare(const char *sql) {
			return AutoStmt::Prepare(sql);
		}
		
		int ExecuteSQL(const char *sql) {
			return AutoStmt::DirectExecute(sql);
		}
		
};
#define SORT_COLS 20
class dbMt:public AutoMt {
			void *ptr[SORT_COLS];
			int ival[SORT_COLS];
			LONG64 lval[SORT_COLS];
			double fval[SORT_COLS];
			ODate dval[SORT_COLS];
			
			void parseObj(object const &argv,int *coltp,int colct) {
			 interpreter interp(argv.get_interp(), false);
			 char msg[100];
			 if(colct>SORT_COLS) {
				sprintf(msg,"Too many parameters(%d) !",colct);
				errprintf(msg);
				throw tcl_error(msg);
			 }
			 size_t argc = argv.length(interp);
			 int stroff=0;
			 if(colct!=argc) {
				sprintf(msg,"Search parameters not match,need %d,but get %d !",colct,argc);
				errprintf(msg);
				throw tcl_error(msg);
			 }
			 int i;
			 for(i=0;i<colct;i++) {
				 object o(argv.at(interp, i));
				 switch(coltp[i]) {
					case COLUMN_TYPE_CHAR	: //SQLT_CHR
						ptr[i]=strresult+stroff;
            strcpy(strresult+stroff,(const char *) o.get());
            stroff+=strlen(strresult+stroff)+1;
						break;
				  case COLUMN_TYPE_FLOAT : //SQLT_FLT
				  	fval[i]=o.get<double>(interp);
				  	ptr[i]=fval+i;
				  	break;
				  case COLUMN_TYPE_INT ://SQLT_INT
				  	ival[i]=o.get<int>(interp);
				  	ptr[i]=ival+i;
				  	break;
				  case COLUMN_TYPE_BIGINT	://SQLT_LNG
				  	lval[i]=o.get<LONG64>(interp);
				  	ptr[i]=lval+i;
				  	break;
				  case COLUMN_TYPE_DATE : //SQLT_DAT
				  	dval[i]=o.get<ODate>(interp);
				  	ptr[i]=dval[i].date;
				  	break;
				 }
			}
 		  ptr[i]=NULL;
 		}
			
	public:
		dbMt(int _dbc):AutoMt(_dbc,2000) {};
			const char *Desc () {
			  return tclMTDesc(handle);
			}
		void Sort(const char *cols) {
			_wdbiSetSortColumn(handle,cols);
			_wdbiSort(handle);
		}
		int Search(object const &argv) {
			int coltp[100];
			int colct=_wdbiGetSortColType(handle,coltp);
			parseObj(argv,coltp,colct);
			return _wdbiSearch(handle,ptr);
		}
		int SearchLE(object const &argv) {
			int coltp[100];
			int colct=_wdbiGetSortColType(handle,coltp);
			parseObj(argv,coltp,colct);
			return _wdbiSearchLE(handle,ptr);
		}
		
		int InsertRow(const char *colsnm,object const &argv) {
			int coltp[100];
			int colct=_wdbiGetColumnTypeByName(handle,strlen(colsnm)==0?NULL:colsnm,coltp);
			parseObj(argv,coltp,colct);
			return _wdbiInsertRows(handle,ptr,strlen(colsnm)==0?NULL:colsnm,1);
		}
		
		void CopyFrom(dbMt *from,int fromrn,int toStart) {
			QuickCopyFrom(from,toStart,fromrn);
		}
		void dbBindToStmt(dbStmt *stmt,const char *colsname,int rowst) {
			wociBindToStatment(Get(),stmt->Get(),strlen(colsname)==0?NULL:colsname,rowst);
		}
		void SetMaxRows(int maxrn) {
			AutoMt::SetMaxRows(maxrn);
		}
		int Get() {
			return handle;
		}
	  int Wait() 
	  {
	  	AutoMt::Wait();
	  }
		int GetMaxRows() {
			AutoMt::GetMaxRows();
		}
		int GetRows() {
			return AutoMt::GetRows();
		}
		int CompareMt(int hd) {
			return AutoMt::CompareMt(hd);
		}
		int CompatibleMt(int hd) {
			return AutoMt::CompatibleMt(hd);
		}
		void SetHandle(int hd) {
			AutoMt::SetHandle(hd);
		}
	  void SetAutoClear(bool _val) {
	  	AutoMt::SetAutoClear(_val);
	  }
		//bool CreateAndAppend(const char *tablename) {
		// _dbc=0 use internal,
		bool CreateAndAppend(const char *tablename,int _dbc,bool forcecreate,bool autocommit) {
			AutoMt::CreateAndAppend(tablename,_dbc,forcecreate,autocommit);
		}
		int FetchFirst() {
			return AutoMt::FetchFirst();
		}
		int FetchFirstSQL(const char *sql) {
			return AutoMt::FetchFirst(sql);
		}
		int FetchNext() {
			return AutoMt::FetchNext();
		}
		int FetchAppend() {
			return AutoMt::FetchAppend();
		}
		void SetIKGroupParam(int dtsrc,const char *srcgrpcols,
		  const char *srcsumcols,const char *srckey,int dtref,const char *refgrpcols) {
		  AutoMt::SetIKGroupParam(dtsrc,srcgrpcols,srcsumcols,strlen(srckey)==0?NULL:srckey,dtref,strlen(refgrpcols)==0?NULL:	refgrpcols);
		}
		void SetGroupParam(int dtsrc,const char *srcgrpcols,
		 const char *srcsumcols,const char *srckey,int dtref,const char *refgrpcols) 
		 {
		 	AutoMt::SetGroupParam(dtsrc,srcgrpcols,srcsumcols,strlen(srckey)==0?NULL:srckey,dtref,strlen(refgrpcols)==0?NULL:	refgrpcols);
		}
		void SetDBC(int _dbc) {
			AutoMt::SetDBC(_dbc);
		}
		void BuildFromStmt(int _stmt) {
			AutoMt::Build(_stmt);
		}
		void Build() {
			AutoMt::Build();
		}
		int GetColumnNum() {
			return AutoMt::GetColumnNum();
		}
		void AddrFresh() {
			AutoMt::AddrFresh();
		}
		int FetchAll() {
			return AutoMt::FetchAll();
		}
		int FetchSql(const char *sql) {
			return AutoMt::FetchAll(sql);
		}
		int FetchSqlAt(int offset,const char *sql) {
			return AutoMt::FetchAllAt(offset,sql);
		}
		//_wdbiMTPrint(int memtab,int rownm,const char *colsnm,bool compact) ;
		void Print(object const &argv) {
			interpreter interp(argv.get_interp(), false);
			int rows=10;
			strresult[0]=0;// for store cols name
			bool compact=true;
			int argc=argv.length(interp);
			if(argc>0) {
	     	object o(argv.at(interp, 0));
	     	rows=o.get<int>(interp);
	     	if(rows<1) rows=GetRows();
			}
			if(argc>1) {
	     	object o(argv.at(interp, 1));
	     	strcpy(strresult,(const char *)o.get());
			}
			if(argc>2) {
	     	object o(argv.at(interp, 2));
	     	compact=o.get<bool>(interp);
			}
			if(argc>3) {
			  char msg[100];
				sprintf(msg,"Too many parameters(%d) !",argc);
				throw tcl_error(msg);
			}
			_wdbiMTPrint(handle,rows,strlen(strresult)==0?NULL:strresult,compact);
		}
		
		const char *GetStr(const char *col,unsigned int rowst,object const &argv) {
			 strresult[0]=0;
			 interpreter interp(argv.get_interp(), false);
			 bool rawpos=false;
			 char msg[100];
			 int argc=argv.length(interp);
			 if(argc>1) {
				sprintf(msg,"Too many parameters(%d) !",argc);
				throw tcl_error(msg);
			 }
	     if(argc==1) {
	     	object o(argv.at(interp, 0));
	     	rawpos=o.get<bool>(interp);
	     }
  		 _wdbiGetCell( handle,rowst,_wdbiGetColumnPosByName(handle,col),strresult,rawpos) ;
  		 return	 strresult;
		}
		
		ODate GetDate(const char *col,unsigned int rowst) {
			ODate dt;
			AutoMt::GetDate(col,rowst,dt.date);
			return dt;
		}
		/*
		double GetDouble(const char *col,unsigned rowst) {
			return AutoMt::GetDouble(col,rowst);
		}
		int GetInt(const char *col,unsigned rowst) {
			return AutoMt::GetInt(col,rowst);
		}
		LONG64 GetLong(const char *col,unsigned rowst) {
			return AutoMt::GetLong(col,rowst);
		}*/
};


class dbTwinMt:public TradeOffMt{
	dbStmt stmt;
	  
public :
		dbTwinMt(int dbc,int maxrows,const char *sql):TradeOffMt(0,maxrows),stmt(dbc) {
			if(strlen(sql)>0) 
				Prepare(sql);
		}
		void Prepare(const char *sql) {
			stmt.Prepare(sql);
			TradeOffMt::Cur()->Build(stmt.Get());
			TradeOffMt::Next()->Build(stmt.Get());
		}
		dbMt *Cur() {
			AutoMt *pmt=TradeOffMt::Cur();
			dbMt *mt=new dbMt(0);
			mt->SetHandle((int)*pmt);
			return mt;
		}
		dbMt *Next() {
			AutoMt *pmt=TradeOffMt::Next();
			dbMt *mt=new dbMt(0);
			mt->SetHandle((int)*pmt);
			return mt;
		}
		int FetchNext() {
			return TradeOffMt::FetchNext();
		}
		int FetchFirst() {
			return TradeOffMt::FetchFirst();
		}
		int Wait() {
			return TradeOffMt::Wait();
		}
};

/*  variaic search reference:
int dbSearch(int memtab,object const &argv)
{
     interpreter i(argv.get_interp(), false);
     size_t argc = argv.length(i);
     
     for (size_t indx = 0; indx != argc; ++indx)
     {
          object o(argv.at(i, indx));
          sum += o.get<int>(i);
     }
     return sum;
}
CPPTCL_MODULE(Mymodule, i)
{
     i.def("sum", sumAll, variadic());
}
*/

void tclAddColumn(int memtab,const char *name,const char *typestr,int length,int scale) {
	int ctype;
	if(strcmp(typestr,"string")==0) ctype=COLUMN_TYPE_CHAR;
	else if(strcmp(typestr,"number")==0) ctype=COLUMN_TYPE_NUM;
	else if(strcmp(typestr,"long")==0) ctype=COLUMN_TYPE_BIGINT;
	else if(strcmp(typestr,"int")==0) ctype=COLUMN_TYPE_INT;
	else if(strcmp(typestr,"date")==0) ctype=COLUMN_TYPE_DATE;
	else if(strcmp(typestr,"float")==0) ctype=COLUMN_TYPE_FLOAT;
	else throw tcl_error("Invalid type");
	_wdbiAddColumn(memtab,name,NULL,ctype,length,scale);
}

const char *dbGetColumnType(int memtab,const char *colname) {
	  switch(_wdbiGetColumnType(memtab,_wdbiGetColumnPosByName(memtab,colname))) {
	  	case COLUMN_TYPE_CHAR:return "string";
	  	case COLUMN_TYPE_NUM:return "number";
	  	case COLUMN_TYPE_BIGINT:return "long";
	  	case COLUMN_TYPE_DATE:return "date";
	  	case COLUMN_TYPE_FLOAT:return "float";
	  	default:	throw tcl_error("Invalid column type");
	  }
	  return "";
}

double tclCalculate(int memtab,const char *colnm,const char *opstr) {
	int op;
	if(strcmp(opstr,"sum")==0) op=CAL_SUM;
	else if(strcmp(opstr,"min")==0) op=CAL_MIN;
	else if(strcmp(opstr,"max")==0) CAL_MAX;
	else if(strcmp(opstr,"avg")==0) op=CAL_AVG;
	else throw tcl_error("Invalid set type");
	return _wdbiCalculate(memtab,colnm,op);
}

void tclValuesSet(int memtab,const char *colto,int mtsrc,const char *colfrom,bool usedestKey,const char *opstr) {
	int op;
	if(strcmp(opstr,"set")==0) op=VALUESET_SET;
	else if(strcmp(opstr,"add")==0) op=VALUESET_ADD;
	else if(strcmp(opstr,"sub")==0) op=VALUESET_SUB;
	else throw tcl_error("Invalid set type");
  _wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op);	
}

ODate tclGetCurDateTime() {
	ODate dt;
	_wdbiGetCurDateTime(dt.date);
	return dt;
}
bool tclGetAsyncMode() {
	return asyncMode;
}

void tclSetAsyncMode(bool mode) {
	asyncMode=mode;
}

ODate tclGetDateValByName(int memtab,const char *col,unsigned int rowst)
{
	ODate dt;
  _wdbiGetDateValByName(memtab,col,rowst,dt.date);
  return dt;
}
int tclDateDiff(ODate d1,ODate d2)
{
	return _wdbiDateDiff(d1.date,d2.date);
}

const char *tclGetCreateTableSQL(int memtab,const char *tabname) {
	strresult[0]=0;
  _wdbiGetCreateTableSQL(memtab,strresult,tabname,false) ;
  return strresult;
}

const char *tclGetColumnName(int memtab,int id) {
	strresult[0]=0;
  _wdbiGetColumnName(memtab,id,strresult);
  return strresult;
}

const char *tclGetColumnTitle(int memtab,int colid) {
	strresult[0]=0;
  _wdbiGetColumnTitle(memtab,colid,strresult,MAXSTR);
  return strresult;
}

const char *tclGetCell(int memtab,unsigned int row,int col,bool rawpos) {
	strresult[0]=0;
  _wdbiGetCell(memtab,row,col,strresult,rawpos) ;
  return strresult;
}

const char *tclGetLine(int memtab,unsigned int row,bool rawpos,const char *colsnm){
	strresult[0]=0;
  _wdbiGetLine(memtab,row,strresult,rawpos,strlen(colsnm)==0?NULL:colsnm,NULL);
  return strresult;
}
const char *tclList(){
	strresult[0]=0;
  _wdbiList(strresult);
  return strresult;
}

void tclMTToTextFile(int memtab,const char *fn,int rownm,const char *colsnm) {
    _wdbiMTToTextFile(memtab,fn,rownm,strlen(colsnm)==0?NULL:colsnm);
}

const char *tclGetTitle(int memtab,const char *colsnm){
	strresult[0]=0;
  _wdbiGetTitle(memtab,strresult,MAXSTR,strlen(colsnm)==0?NULL:colsnm,NULL);
  return strresult;
}

const char *tclDateTimeToStr(ODate date) {
	strresult[0]=0;
   _wdbiDateTimeToStr(date.date,strresult) ;
  return strresult;
}

const char *tclGetStrValByName(int memtab,const char *col,unsigned int rowst)  {
	strresult[0]=0;
  _wdbiGetStrValByName(memtab,col,rowst,strresult) ;
  return strresult;
}

const char *tclGetMTName(int memtab)  {
	strresult[0]=0;
  _wdbiGetMTName(memtab,strresult) ;
  return strresult;
}


void tclSetStrValues(int memtab,int colid,unsigned int rowstart,const char *bf) {

	_wdbiSetStrValues(memtab,colid,rowstart,1,bf) ;
}

void tclSetDateValues(int memtab,int colid,unsigned int rowstart,ODate bf) {

  _wdbiSetDateValues(memtab,colid, rowstart,1,bf.date) ;
}

void tclSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const double bf) {

  _wdbiSetDoubleValues( memtab,colid, rowstart,1,&bf);
}

void tclSetIntValues(int memtab,int colid,unsigned int rowstart,const int bf) {

	_wdbiSetIntValues(memtab,colid,rowstart,1,&bf) ;
}

void tclSetLongValues(int memtab,int colid,unsigned int rowstart,const LONG64 bf) {

	_wdbiSetLongValues(memtab,colid,rowstart,1,&bf) ;
}

ODate tclSetDateTime(int year,int mon,int day,int hour,int min,int sec) {
	ODate dt;
   _wdbiSetDateTime(dt.date,year,mon,day,hour,min,sec) ;
  return dt;
}

ODate tclSetDate(int year,int mon,int day) {
	ODate dt;
  _wdbiSetDate(dt.date,year,mon,day) ;
  return dt;
}


int tclGetYear(ODate date) {
	return _wdbiGetYear(date.date);
}

int tclGetMonth(ODate date) {
	return _wdbiGetMonth(date.date);
}

int tclGetDay(ODate date) {
	return _wdbiGetDay(date.date);
}

int tclGetHour(ODate date) {
	return _wdbiGetHour(date.date);
}

int tclGetMin(ODate date) {
	return _wdbiGetMin(date.date);
}

int tclGetSec(ODate date) {
	return _wdbiGetSec(date.date);
}


CPPTCL_MODULE(Wdbitcl, i)
{
	   WOCIInit("tcl_log");
	   wociSetTraceFile("tcl_log/test");
	   wociSetOutputToConsole(TRUE);
	   /*
	   AutoHandle dts;
	   dts.SetHandle(wociCreateSession("dtuser","readonly","//130.86.12.92:1521/dtagt",DTDBTYPE_ORACLE));
	   AutoMt mt(dts,10000);
     mt.FetchFirst("select * from dest.tab_gsmvoicdr2@dblk_cdrsvr where rownum<1001");
     int rn=mt.Wait();
	   printf("Get %d rows!\n\n",rn);
	   */
i.def("hello",                          hello);                
	   
//i.def("dbIsFixedMySQLBlock",                         _wdbiIsFixedMySQLBlock );               
i.def("dbGetStrVal",                           tclGetStrValByName );                
i.def("dbGetDateVal",                          tclGetDateValByName);                
i.def("dbGetDoubleVal",                        _wdbiGetDoubleValByName);              
i.def("dbGetIntVal",                           _wdbiGetIntValByName );                
i.def("dbGetLongVal",                          _wdbiGetLongValByName);                
i.def("dbGetCreateTableSQL",                         tclGetCreateTableSQL );              
i.def("dbGetCell",                                   tclGetCell );                        
i.def("dbGetLine",                                   tclGetLine );                        
i.def("dbGetTitle",                                  tclGetTitle);                        
i.def("dbSetStrValues",                              tclSetStrValues);                    
i.def("dbSetDateValues",                             tclSetDateValues );                  
i.def("dbSetDoubleValues",                           tclSetDoubleValues );                
i.def("dbSetIntValues",                              tclSetIntValues);                    
i.def("dbSetLongValues",                             tclSetLongValues );                  
//涉及类型不定指针数组的tcl接口实现，暂不支持
//i.def("dbSearch",                                    _wdbiSearch);                          
//i.def("dbSearchLE",                                  _wdbiSearchLE);                        

i.def("dbGetCurDateTime",                            tclGetCurDateTime);                  
i.def("dbSetDateTime",                               tclSetDateTime );                    
i.def("dbSetDate",                                   tclSetDate );                        
i.def("dbDateTimeToStr",                             tclDateTimeToStr );                  
i.def("dbGetYear",                                   tclGetYear );                        
i.def("dbGetMonth",                                  tclGetMonth);                        
i.def("dbGetDay",                                    tclGetDay);                          
i.def("dbGetHour",                                   tclGetHour );                        
i.def("dbGetMin",                                    tclGetMin);                          
i.def("dbGetSec",                                    tclGetSec);                          
i.def("dbGetMTName",                                 tclGetMTName );                      
/*
i.def("dbGetStrAddrByName",                          _wdbiGetStrAddrByName);                
i.def("dbGetDateAddrByName",                         _wdbiGetDateAddrByName );              
i.def("dbGetDoubleAddrByName",                       _wdbiGetDoubleAddrByName );            
i.def("dbGetIntAddrByName",                          _wdbiGetIntAddrByName);                
i.def("dbGetLongAddrByName",                         _wdbiGetLongAddrByName );              
i.def("dbBindLongByPos",                             _wdbiBindLongByPos );                  
i.def("dbBindStrByPos",                              _wdbiBindStrByPos);                    
i.def("dbBindDoubleByPos",                           _wdbiBindDoubleByPos );                
i.def("dbBindIntByPos",                              _wdbiBindIntByPos);                    
i.def("dbBindDateByPos",                             _wdbiBindDateByPos );                  
i.def("dbDefineStrByPos",                            _wdbiDefineStrByPos);                  
i.def("dbDefineDoubleByPos",                         _wdbiDefineDoubleByPos );              
i.def("dbDefineIntByPos",                            _wdbiDefineIntByPos);                  
i.def("dbDefineDateByPos",                           _wdbiDefineDateByPos );                
i.def("dbDefineLongByPos",                           _wdbiDefineLongByPos );                
*/
i.def("dbSetDefaultPrec",                            _wdbiSetDefaultPrec);                  
//i.def("dbMainEntrance",                              _wdbiMainEntrance);                    
//i.def("dbMainEntranceEx",                            _wdbiMainEntranceEx);                  
i.def("dbdestroy",                                   _wdbidestroy );                        
i.def("dbDestroyAll",                                _wdbiDestroyAll);                      
i.def("dbBreakAll",                                  _wdbiBreakAll);                        
i.def("dbCreateSession",                             _wdbiCreateSession );                  
i.def("dbCommit",                                    _wdbiCommit);                          
i.def("dbRollback",                                  _wdbiRollback);                        
i.def("dbSetTerminate",                              _wdbiSetTerminate);                    
i.def("dbIsTerminate",                               _wdbiIsTerminate );                    
i.def("dbSetNonBlockMode",                           _wdbiSetNonBlockMode );                
i.def("dbCreateStatment",                            _wdbiCreateStatment);                  
i.def("dbBreakAndReset",                             _wdbiBreakAndReset );                  
i.def("dbExecute",                                   _wdbiExecute );                        
i.def("dbExecuteAt",                                 _wdbiExecuteAt );                      
i.def("dbFetch",                                     _wdbiFetch );                          
i.def("dbGetStmtColumnsNum",                         _wdbiGetStmtColumnsNum );              
i.def("dbGetMtColumnsNum",                           _wdbiGetMtColumnsNum );                
i.def("dbGetFetchedRows",                            _wdbiGetFetchedRows);                  
i.def("dbGetSession",                                _wdbiGetSession);                      
// do we support unsigned short ? next line do not carry any error on tcl load shared lib.
i.def("dbGetStmtType",                               _wdbiGetStmtType );                    
i.def("dbPrepareStmt",                               _wdbiPrepareStmt );                    
i.def("dbCreateMemTable",                            _wdbiCreateMemTable);                  
i.def("dbAddColumn",                                 tclAddColumn );                      
i.def("dbBuildStmt",                                 _wdbiBuildStmt );
i.def("dbBuild",                                     _wdbiBuild );                          
i.def("dbClear",                                     _wdbiClear );                          
i.def("dbAppendToDbTable",                           _wdbiAppendToDbTable );                
i.def("dbAppendToDbTableWithColName",                _wdbiAppendToDbTableWithColName);      
i.def("dbFetchAll",                                  _wdbiFetchAll);                        
i.def("dbFetchFirst",                                _wdbiFetchFirst);                      
i.def("dbFetchAt",                                   _wdbiFetchAt );                        
i.def("dbFetchNext",                                 _wdbiFetchNext );                      
i.def("dbGeneTable",                                 _wdbiGeneTable );                      
i.def("dbSetMTName",                                 _wdbiSetMTName );                      
i.def("dbGetBufferLen",                              _wdbiGetBufferLen);                    
i.def("dbGetColumnDisplayWidth",                     _wdbiGetColumnDisplayWidth );          
i.def("dbGetColumnPosByName",                        _wdbiGetColumnPosByName);              
i.def("dbGetColumnName",                             tclGetColumnName );                  
i.def("dbGetColumnDataLenByPos",                     _wdbiGetColumnDataLenByPos );          
i.def("dbGetColumnNumber",                           _wdbiGetColumnNumber );                
i.def("dbGetColumnScale",                            _wdbiGetColumnScale);                  
i.def("dbGetColumnTitle",                            tclGetColumnTitle);                  
i.def("dbGetColumnType",                             _wdbiGetColumnType );                  
i.def("dbGetCompactLen",                             _wdbiGetCompactLen );                  
i.def("dbGetMemtableRows",                           _wdbiGetMemtableRows );                
i.def("dbIsIKSet",                                   _wdbiIsIKSet );                        
i.def("dbOrderByIK",                                 _wdbiOrderByIK );                      
i.def("dbSearchIK",                                  _wdbiSearchIK);                        
i.def("dbSearchIKLE",                                _wdbiSearchIKLE);                      
// next function has a char * parameter,but it use as const char * in fact,this will cause a parameter error
//  TODO: i need to modify wdbi interface to adapt to const char *(last parameter)
i.def("dbSetColumnDisplayName",                      _wdbiSetColumnDisplayName);            
i.def("dbSetIKByName",                               _wdbiSetIKByName );                    
i.def("dbSetSortColumn",                             _wdbiSetSortColumn );                  
i.def("dbReInOrder",                                 _wdbiReInOrder );                      
i.def("dbSort",                                      _wdbiSort);                            
i.def("dbSortHeap",                                  _wdbiSortHeap);                        
i.def("dbSetGroupSrc",                               _wdbiSetGroupSrc );                    
i.def("dbSetIKGroupRef",                             _wdbiSetIKGroupRef );                  
i.def("dbSetGroupSrcCol",                            _wdbiSetGroupSrcCol);                  
i.def("dbSetGroupRefCol",                            _wdbiSetGroupRefCol);                  
i.def("dbSetSrcSumCol",                              _wdbiSetSrcSumCol);                    
i.def("dbGroup",                                     _wdbiGroup );                          
i.def("dbCopyRowsTo",                                _wdbiCopyRowsTo);                      
i.def("dbCopyRowsToNoCut",                           _wdbiCopyRowsToNoCut );                
//i.def("dbGetColumnDesc",                             _wdbiGetColumnDesc );                  
i.def("dbCopyColumnDefine",                          _wdbiCopyColumnDefine);                
//i.def("dbExportSomeRows",                            _wdbiExportSomeRows);                  
//i.def("dbExport",                                    _wdbiExport);                          
//i.def("dbImport",                                    _wdbiImport);                          
//i.def("dbAppendRows",                                _wdbiAppendRows);                      
i.def("dbReset",                                     _wdbiReset );                          
i.def("dbClearIK",                                   _wdbiClearIK );                        
i.def("dbClearSort",                                 _wdbiClearSort );                      
i.def("dbGetMaxRows",                                _wdbiGetMaxRows);                      
i.def("dbCompact",                                   _wdbiCompact );                        
i.def("dbGetRowLen",                                 _wdbiGetRowLen );                      
//i.def("dbCreateExcelEnv",                            _wdbiCreateExcelEnv);                  
////i.def("dbSetDir",                                    _wdbiSetDir);                          
//i.def("dbSetMemTable",                               _wdbiSetMemTable );                    
//i.def("dbLoadTemplate",                              _wdbiLoadTemplate);                    
//i.def("dbSelectSheet",                               _wdbiSelectSheet );                    
////i.def("dbFillData",                                  _wdbiFillData);                        
//i.def("dbFillTitle",                                 _wdbiFillTitle );                      
//i.def("dbSaveAs",                                    _wdbiSaveAs);                          
i.def("dbGetMemUsed",                                _wdbiGetMemUsed);                      
i.def("dbSetSortedGroupRef",                         _wdbiSetSortedGroupRef );              
i.def("dbValuesSet",                                 tclValuesSet );                      
i.def("dbDeleteRow",                                 _wdbiDeleteRow );                      
//i.def("dbInsertRows",                                _wdbiInsertRows);                      
i.def("dbBindToStatment",                            _wdbiBindToStatment);                  
i.def("dbTestTable",                                 _wdbiTestTable );                      
i.def("dbGetRawrnBySort",                            _wdbiGetRawrnBySort);                  
i.def("dbGetRawrnByIK",                              _wdbiGetRawrnByIK);                    
i.def("dbCompressBf",                                _wdbiCompressBf);                      
// not found define.
//i.def("dbSearchQDel",                                _wdbiSearchQDel);                      
i.def("dbIsQDelete",                                 _wdbiIsQDelete );                      
i.def("dbQDeletedRows",                              _wdbiQDeletedRows);                    
i.def("dbQDeleteRow",                                _wdbiQDeleteRow);                      
i.def("dbWaitStmt",                                  _wdbiWaitStmt);                        
i.def("dbWaitMemtable",                              _wdbiWaitMemtable);                    
i.def("dbWaitLastReturn",                            _wdbiWaitLastReturn);                  
i.def("dbWaitTime",                                  _wdbiWaitTime);                        
i.def("dbFreshRowNum",                               _wdbiFreshRowNum );                    
i.def("dbBatchSelect",                               _wdbiBatchSelect );                    
i.def("dbMTPrint",                                   _wdbiMTPrint );                        
//i.def("dbMTPrint",                                   _wdbiMTPrint );                        

i.def("dbSetEcho",                                   _wdbiSetEcho );                        
i.def("dbIsEcho",                                    _wdbiIsEcho);                          

i.def("dbMTToTextFile",                              tclMTToTextFile);                    
// next function has a char * parameter,but it use as const char * in fact,this will cause a parameter error
//  TODO: i need to modify wdbi interface to adapt to const char *(last parameter)
i.def("dbReadFromTextFile",                          _wdbiReadFromTextFile);                
i.def("dbFetchAllAt",                                _wdbiFetchAllAt);                      
i.def("dbDateDiff",                                  tclDateDiff);                        
i.def("dbSetOutputToConsole",                        _wdbiSetOutputToConsole);              
i.def("dbSetTraceFile",                              _wdbiSetTraceFile);                    
i.def("dbSetErrorFile",                              _wdbiSetErrorFile);                    
//i.def("dbSetLogFile",                                _wdbiSetLogFile);                      
i.def("dbGetErrorFile",                              _wdbiGetErrorFile);                    
i.def("dbGetLogFile",                                _wdbiGetLogFile);                      
//i.def("dbInit",                                      _WDBIInit);                            
//i.def("dbQuit",                                      _WDBIQuit);                            
i.def("dbReplaceStmt",                               _wdbiReplaceStmt );                    
//i.def("dbConvertColStrToInt",                        _wdbiConvertColStrToInt);              
//i.def("dbSaveSort",                                  _wdbiSaveSort);                        
i.def("dbCalculate",                                 tclCalculate );                      
//i.def("dbLoadSort",                                  _wdbiLoadSort);                        
//i.def("dbCopyToMySQL",                               _wdbiCopyToMySQL );                    
i.def("dbCompareSortRow",                            _wdbiCompareSortRow);                  
i.def("dbList",                                      tclList);                            
i.def("dbGetLastError",                              _wdbiGetLastError);                    
//i.def("dbAddrFresh",                                 _wdbiAddrFresh );                      
//i.def("dbGetColumnInfo",                             _wdbiGetColumnInfo );                  
i.def("dbGetFetchSize",                              _wdbiGetFetchSize);                    
i.def("dbSetFetchSize",                              _wdbiSetFetchSize);                    
//i.def("dbReverseByteOrder",                          _wdbiReverseByteOrder);                
//i.def("dbReverseCD",                                 _wdbiReverseCD );                      
i.def("dbMTDesc",                              tclMTDesc);                    
i.def("dbSetAsyncMode",                              tclSetAsyncMode);                    
i.def("dbGetAsyncMode",                              tclGetAsyncMode);                    
i.def("dbSetColumnDisplay",                          _wdbiSetColumnDisplay);                
i.class_<dbCon>("dbCon", init<const char *,const char *,const char *, object const &>(), variadic())
          .def("id", &dbCon::Get)
          .def("Commit", &dbCon::Commit)
          .def("Rollback", &dbCon::Rollback)
          .def("Wait", &dbCon::Wait);
          	

i.class_<dbStmt>("dbStmt", init<int const &>())
          .def("Create", &dbStmt::Create)
          .def("Execute", &dbStmt::Execute)
          .def("id", &dbStmt::Get)
          .def("Wait", &dbStmt::Wait)
          .def("Prepare", &dbStmt::Prepare)
          .def("ExecuteSQL", &dbStmt::ExecuteSQL)	
          .def("ExecuteAt", &dbStmt::ExecuteAt);
          	
          	
i.class_<dbMt>("dbMt", init<int const &>())
          .def("id", &dbMt::Get)
          .def("Wait", &dbMt::Wait)
          .def("SetAutoClear",&dbMt::SetAutoClear)
          .def("GetMaxRows", &dbMt::GetMaxRows)
          .def("GetRows", &dbMt::GetRows)
          .def("CompareMt", &dbMt::CompareMt)
          .def("CopyFrom", &dbMt::CopyFrom)
          .def("CompatibleMt", &dbMt::CompatibleMt)
          .def("SetHandle", &dbMt::SetHandle)
          .def("CreateAndAppend", &dbMt::CreateAndAppend)
          .def("FetchFirst", &dbMt::FetchFirst) //
          .def("FetchNext", &dbMt::FetchNext)
          .def("FetchAll", &dbMt::FetchAll)//
          .def("FetchAppend", &dbMt::FetchAppend)
          .def("SetIKGroupParam", &dbMt::SetIKGroupParam)
          .def("SetGroupParam", &dbMt::SetGroupParam)
          .def("SetDBC", &dbMt::SetDBC)
          .def("BuildFromStmt", &dbMt::BuildFromStmt) 
          .def("Build", &dbMt::Build) 
          .def("GetColumns", &dbMt::GetColumnNum)
          .def("AddrFresh", &dbMt::AddrFresh)
          .def("FetchFirstSQL", &dbMt::FetchFirstSQL)//
          .def("FetchSQLAt", &dbMt::FetchSqlAt)//
          .def("FetchSQL", &dbMt::FetchSql)//
          //.def("GetDouble", &dbMt::GetDouble)//
          //.def("GetInt", &dbMt::GetInt)//
          //.def("GetLong", &dbMt::GetLong)//
          .def("GetDate", &dbMt::GetDate)//
          .def("GetStr", &dbMt::GetStr,variadic())//
          .def("InsertRow", &dbMt::InsertRow,variadic())//
          .def("SearchLE", &dbMt::SearchLE,variadic())//
          .def("Search", &dbMt::Search,variadic())//
          .def("Sort", &dbMt::Sort)//
          .def("Print", &dbMt::Print)//
          .def("Desc", &dbMt::Desc)//
          .def("BindToStmt", &dbMt::dbBindToStmt)//
          .def("SetMaxRows", &dbMt::SetMaxRows);

i.class_<dbTwinMt>("dbTwinMt", init<int const &,int const &,const char *>())
          .def("Prepare", &dbTwinMt::Prepare)
          .def("Cur", &dbTwinMt::Cur,factory("dbMt"))
          .def("Next", &dbTwinMt::Next,factory("dbMt"))
          .def("Wait", &dbTwinMt::Wait)
          .def("FetchFirst", &dbTwinMt::FetchFirst)
          .def("FetchNext", &dbTwinMt::FetchNext);
}


// TODO:  
//		1. Search function accept variadic parameters (need to modify wdbi lib to get sort column type and count)--ok
//	  2. Insert into mt using variadic parameters--ok
//    3. test....

