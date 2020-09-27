#include "wdbi.h"
#include <ctype.h>
#ifdef __unix 
extern struct timespec interval ;
#endif

void frprt(int val,char *str);
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern OCIEnv *envhp; 
extern OCIError *errhp;

extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;

void frprt(int val,char *str) {
	str[0]=0;
	int l;
	if(val>=1000000000) {
	 sprintf(str+strlen(str),"%d,",val/1000000000);
	 val%=1000000000;
	}
	l=strlen(str);
	if(val>=1000000 ||l>0) {
	 if(l>0)
	  sprintf(str+l,"%03d,",val/1000000);
	 else
	  sprintf(str+l,"%d,",val/1000000);
	 val%=1000000;
	}
	l=strlen(str);
	if(val>=1000 || l>0) {
	 if(l>0)
	  sprintf(str+l,"%03d,",val/1000);
	 else
	  sprintf(str+l,"%d,",val/1000);
	 val%=1000;
	}
	l=strlen(str);
	if(l>0)
	 sprintf(str+l,"%03d",val);
	else
	 sprintf(str+l,"%d",val);
}

#ifndef NO_ODBC

#define SetDate(date,year,mon,day,hour,min,sec) { date[0]=year/100+100;\
	date[1]=year%100+100;\
	date[2]=(char)mon;\
	date[3]=(char)day;\
	date[4]=hour+1;\
	date[5]=min+1;\
	date[6]=sec+1; \
}
#ifndef GetYear
#define GetYear(date) (((unsigned char)(date)[0]-100)*100+(unsigned char)(date)[1]-100)
#endif
#ifndef GetMonth
#define GetMonth(date) (date)[2]
#endif
#ifndef GetDay
#define GetDay(date) (date)[3]
#endif
#ifndef GetHour
#define GetHour(date) (date)[4]-1
#endif
#ifndef GetMin
#define GetMin(date) (date)[5]-1
#endif
#ifndef GetSec
#define GetSec(date) (date)[6]-1
#endif


static bool IsSelect(const char *sql) {
	char *psql=(char *)sql;
	while(*psql==' ') psql++;
	char sqlchk[20];
	strncpy(sqlchk,psql,6);
	sqlchk[6]=0;
	psql=sqlchk;
	while(*psql) {*psql=toupper(*psql);psql++;}
	return strncmp(sqlchk,"SELECT",6)==0 || strncmp(sqlchk,"SHOW",4)==0;
}

static bool HasParam(const char *sql) {
	return strchr(sql,'?')!=NULL;
}

WODBCStatement::WODBCStatement(WDBISession *s,bool atthr):WDBIStatement(s,atthr) {
	sthp=NULL;		
	for(int i=0;i<MAX_COLUMN;i++) {
		datebind[i]=NULL;
		datedef[i]=NULL;
		datelen[i]=NULL;
	}
}

void WODBCStatement ::ocheckerr(SQLRETURN status,int handletype,SQLHANDLE handle,int chkopt)
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
		if(lrc == SQL_SUCCESS || lrc == SQL_SUCCESS_WITH_INFO)
			sprintf(errbuf,"\n [%s][%d:%s]\nSQL:\n%s\n",szSqlState,pfNativeError,szErrorMsg,sqlstmt);
		errcode=pfNativeError;
		if(chkopt==CHECK_OPT_NOTABLE && strcmp((char *)szSqlState,"42S02")==0 ) {
			errcode=1146;
			return;
		}
		if(autoThrow && status == SQL_ERROR) {
			strcpy(filen," ");
			lineid=-1;
			Throw();
		}
	}
}

void WODBCStatement::ReleaseBind()  {
	for(int i=0;i<MAX_COLUMN;i++) {
		if(datebind[i]!=NULL)
			delete []datebind[i];
		datebind[i]=NULL;
		if(datelen[i]!=NULL)
			delete []datelen[i];
		datelen[i]=NULL;
	}
}

void WODBCStatement::ReleaseDef()  {
	for(int i=0;i<MAX_COLUMN;i++) {
		if(datedef[i]!=NULL)
			delete []datedef[i];
		datedef[i]=NULL;
	}
}

bool WODBCStatement::BindByPos(int pos,char *result,int size) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_CHAR;
	bindedlen[bindednum]=size;
	bindednum++;
	return true;
}

bool WODBCStatement::BindByPos(int pos,LONG64 *result) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_BIGINT;
	bindedlen[bindednum]=sizeof(LONG64);
	bindednum++;
	return true;
}


bool WODBCStatement::BindByPos(int pos,int *result) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_INT;
	bindedlen[bindednum]=sizeof(int);
	bindednum++;
	return true;
}

bool WODBCStatement::BindByPos(int pos,double *result) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_FLOAT;
	bindedlen[bindednum]=sizeof(double);
	bindednum++;
	return true;
}

bool WODBCStatement::BindByPos(int pos,unsigned char *result) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_NUM;
	bindedlen[bindednum]=sizeof(int);
	bindednum++;
	return true;
}

bool WODBCStatement::BindByPos(int pos,char *result) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_DATE;
	bindedlen[bindednum]=sizeof(int);
	bindednum++;
	return true;
}

bool WODBCStatement::DefineByPos(int pos,char *result,int size) {
	SQLRETURN rc;
	PrepareDefine(pos);
	rc=SQLBindCol(sthp,pos,
		SQL_C_CHAR,
		result,
		size,(SQLLEN*)pind[pos]);
	chkstmt(rc);
	return true;
}

bool WODBCStatement::DefineByPos(int pos,int *result) {
	SQLRETURN rc;
	PrepareDefine(pos);
	rc=SQLBindCol(sthp,pos,
		SQL_C_SLONG,
		result,
		4,(SQLLEN*)pind[pos]);
	chkstmt(rc);
	return true;
}

bool WODBCStatement::DefineByPos(int pos,LONG64 *result) {
	SQLRETURN rc;
	PrepareDefine(pos);
	rc=SQLBindCol(sthp,pos,
		SQL_C_SBIGINT,
		result,
		8,(SQLLEN*)pind[pos]);
	chkstmt(rc);
	return true;
}


bool WODBCStatement::DefineByPos(int pos,double *result) {
	SQLRETURN rc;
	PrepareDefine(pos);
	rc=SQLBindCol(sthp,pos,
		SQL_C_DOUBLE,
		result,
		8,(SQLLEN*)pind[pos]);
	chkstmt(rc);
	return true;
}

bool WODBCStatement::DefineByPos(int pos,char *result) {
	SQLRETURN rc;
	PrepareDefine(pos);
	if(datedef[pos-1]!=NULL) delete [] datedef[pos-1];
	datedef[pos-1]=new TIMESTAMP_STRUCT[FETCH_ROWS];
	resultdate[pos-1]=result;
	rc=SQLBindCol(sthp,pos,
		SQL_C_TYPE_TIMESTAMP,
		datedef[pos-1],
		sizeof(TIMESTAMP_STRUCT),(SQLLEN*)pind[pos]);
	chkstmt(rc);
	return true;
}

bool WODBCStatement::Prepare(const char *_sqlstmt)
{
	SQLRETURN rc;
	eof=false;
	if(NewStmt()) return false;
	if(strlen(_sqlstmt)>=MAX_STMT_LEN) return false;
	strcpy(sqlstmt,_sqlstmt);
	rc=SQLPrepare(sthp,(SQLCHAR *)sqlstmt,strlen(sqlstmt));
	chkstmt(rc);
	if(!DescribeStmt()) return false;
	bindednum=0;
	executed=false;
	executing=false;
	rowct=0;
	ReleaseBind();
	ReleaseDef();
	rc=SQLFreeStmt(sthp,SQL_UNBIND);
	chkstmt(rc);
	stmttype=OCI_STMT_SELECT;
	return true;
}


int WODBCSession::GetColumnInfo(Column_Desc *cd,int _colct,char *crttab)
{
	char fmt[200];
	strcpy(crttab,"(");
	for(unsigned int col=0;col<_colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			  //if(cd[col].dtsize>256)
			  //	ReturnIntErr(DT_ERR_OUTOFCOLUMNWIDE,0,"DataTable::GetColumnInfo");
			  if(cd[col].dtsize-1<4)
				sprintf(fmt,"%s char(%d) ",cd[col].colname,cd[col].dtsize-1);
			  else
				sprintf(fmt,"%s varchar(%d) ",cd[col].colname,cd[col].dtsize-1);
			strcat(crttab,fmt);
			break;
		case SQLT_NUM:
			if(GetDBSType()==DBSTYPE_ACCESS)
				sprintf(fmt,"%s double",cd[col].colname);
			else if(GetDBSType()==DBSTYPE_ORACLE)
				sprintf(fmt,"%s number(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			else sprintf(fmt,"%s decimal(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			strcat(crttab,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmt,"%s double ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_INT:
			if(cd[col].prec>0 && GetDBSType()==DBSTYPE_MYSQL)
			{
				sprintf(fmt,"%s integer(%d) ",cd[col].colname,cd[col].prec);
			}
			else
			if(GetDBSType()==DBSTYPE_ACCESS)
				sprintf(fmt,"%s int",cd[col].colname);
			else if(GetDBSType()==DBSTYPE_ORACLE)
				sprintf(fmt,"%s number(%d)",cd[col].colname,cd[col].prec);
			else
				sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_LNG:
			if(GetDBSType()==DBSTYPE_ACCESS)
				sprintf(fmt,"%s long",cd[col].colname);
			else if(GetDBSType()==DBSTYPE_ORACLE)
				sprintf(fmt,"%s number(%d)",cd[col].colname,cd[col].prec);
			else if(cd[col].prec>0) 
					sprintf(fmt,"%s bigint(%d) ",cd[col].colname,cd[col].prec);
			//未知的精度
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_DAT:
			if(GetDBSType()==DBSTYPE_DB2 || GetDBSType()==DBSTYPE_FIREBIRD)
			 sprintf(fmt,"%s timestamp ",cd[col].colname);
			else if(GetDBSType()==DBSTYPE_ORACLE)
			 sprintf(fmt,"%s date ",cd[col].colname);
			else
				sprintf(fmt,"%s datetime ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		default:
			if(errbuf[0]>0 &&errcode!=0) {
			 sprintf(fmt,"%s type:%d(%d) ",cd[col].colname,cd[col].type,cd[col].dtsize-1);
			 strcat(crttab,fmt);
			}
			else ReturnIntErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
			break;
		}
		if(col<_colct-1) strcat(crttab,",");
	}
	strcat(crttab,")");
	return strlen(crttab);
}

int WODBCStatement::Fetch(int rows,bool first)
{
	if(!executed) {
		int tmp=rowct;
		Execute(0);
		rowct=tmp;
	}
	SQLRETURN rc;
	if(eof) return 0;
	SQLULEN rf=0;
	int fetched=0;
	SQLSetStmtAttr(sthp, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
	SQLSetStmtAttr(sthp, SQL_ATTR_ROWS_FETCHED_PTR, &rf, 0);
	while(fetched<rows) {
		rf=fetched;
		SQLULEN off=fetched;
		//设置绑定地址的偏移量(偏移记录数)
		//SQL_ATTR_ROW_BIND_TYPE 
		SQLSetStmtAttr(sthp,SQL_ATTR_ROW_BIND_OFFSET_PTR,(SQLPOINTER)&off,0);
		//是指批量Fetch的记录数
		SQLSetStmtAttr(sthp,SQL_ATTR_ROW_ARRAY_SIZE,(SQLPOINTER)(SQLUINTEGER)(rows-fetched),0);
		if(((WODBCSession *)psess)->GetDBSType()==DBSTYPE_MYSQL)
		 rc=SQLExtendedFetch(sthp,SQL_FETCH_NEXT,fetched,&rf,NULL);
		else rc=SQLFetch(sthp);
		if(rc==SQL_STILL_EXECUTING && !GetSession()->IsTerminate()) {
			mSleep(10);
		}
		else {
			chkstmt(rc);
			if(rc==SQL_NO_DATA && rf<1) break;
			for(int col=0;col<colct;col++) {
				if(datedef[col]!=NULL) {
					char *ptr=resultdate[col];
					TIMESTAMP_STRUCT *pdt=datedef[col];
					pdt+=fetched;
					ptr+=(fetched*7);
					//short *tpind=pind[col+1];
					for(int i=0;i<rf;i++) {
					    //if(tpind!=NULL && *tpind++==SQL_NULL_DATA)// dt->year ==(short)0xcdcd || pdt->year==(short)0) 
						if(pdt->year ==(short)0xcdcd || pdt->year==(short)0) 
						 memset(ptr,0,7);
						else SetDate(ptr,pdt->year,pdt->month,pdt->day,pdt->hour,pdt->minute,pdt->second);
						ptr+=7;
						pdt++;
					}
				}
			}
			fetched+=rf;
			//break;
		}
	}
	rc=SQLFreeStmt(sthp,SQL_UNBIND);
	chkstmt(rc);
	rowct+=fetched;
	if(__output_to_console && echo) {
		char str[40];
		frprt(rowct,str);
		printf("  %s rows fetched.\r",str);
	}
#ifdef __unix
	if(__output_to_console && echo)
		fflush(stdout);
#endif
	return retval;
}

void WODBCStatement::RealBind(int process,int processed) {
	int i;
	SQLRETURN rc=0;
	rc=SQLFreeStmt(sthp,SQL_RESET_PARAMS);
	bool rebind=(bind_start+bind_num)<(processed+process);
	if(rebind)
		ReleaseBind();
	long nPerVarSecPos = 0;
	for( i=0;i<bindednum;i++) {
		switch(bindedtype[i]) {
		case COLUMN_TYPE_FLOAT:
			SQLBindParameter(sthp,i+1,
				SQL_PARAM_INPUT,SQL_C_DOUBLE,SQL_DOUBLE,
				20,4,
				((double *)bindedptr[i])+processed,
				sizeof(double),
				(SQLLEN *)((NULL_INDTYPE *)bindedind[i+1]+processed));
			//					BindByPos(i+1,((double *)bindedptr[i])+processed);
			break;
		case COLUMN_TYPE_INT:
			SQLBindParameter(sthp,i+1,
				SQL_PARAM_INPUT,SQL_C_LONG,SQL_INTEGER,
				bindedlen[i],0,
				((int *)bindedptr[i])+processed,
				sizeof(int),
				(SQLLEN *)((NULL_INDTYPE *)bindedind[i+1]+processed));
			//BindByPos(i+1,((int *)bindedptr[i])+processed);
			break;
		case COLUMN_TYPE_CHAR:
			nPerVarSecPos = processed;
			if(IsVarCharCol(i)){nPerVarSecPos = processed - m_nStartSecNum;}
			SQLBindParameter(sthp,i+1,
				SQL_PARAM_INPUT,SQL_C_CHAR,SQL_VARCHAR,
				bindedlen[i]-1,0,
				((char *)bindedptr[i])+nPerVarSecPos*bindedlen[i],
				bindedlen[i],
				(SQLLEN *)((NULL_INDTYPE *)bindedind[i+1]+processed));
			break;
		case COLUMN_TYPE_BIGINT:
			SQLBindParameter(sthp,i+1,
				SQL_PARAM_INPUT,SQL_C_SBIGINT,SQL_BIGINT,
				bindedlen[i],0,
				((char *)bindedptr[i])+processed*bindedlen[i],
				bindedlen[i],
				(SQLLEN *)((NULL_INDTYPE *)bindedind[i+1]+processed));
			break;
		case COLUMN_TYPE_DATE:
			{
				if(rebind) {
				 bind_start=processed;bind_num=process;
				 if(datebind[i]!=NULL) delete [] datebind[i];
				 if(datelen[i]!=NULL) delete [] datelen[i];
				 datebind[i]=new TIMESTAMP_STRUCT[process];
				 memset(datebind[i],0,sizeof(TIMESTAMP_STRUCT)*process);
				 datelen[i]=new SQLINTEGER [process];// interal SQLINTEGER sames as int of 32bit platform
				 char *ptr=((char *)bindedptr[i])+processed*7;
				 SQLINTEGER *dlp=datelen[i];
				 TIMESTAMP_STRUCT *pdt=datebind[i];
				 for(int r=0;r<process;r++) {
					if(ptr[0]==0) { 
					  dlp[r]=SQL_NULL_DATA;
						pdt++;
					}
					else {
						pdt->year=GetYear(ptr);
						dlp[r]=sizeof(TIMESTAMP_STRUCT);
						pdt->month=GetMonth(ptr);
						pdt->day=GetDay(ptr);
						pdt->hour=GetHour(ptr);
						pdt->minute=GetMin(ptr);
						pdt->second=GetSec(ptr);
						pdt->fraction=0;
						pdt++;
					}
					ptr+=7;				  	
				 }
				}
				SQLBindParameter(sthp,i+1,
					SQL_PARAM_INPUT,SQL_C_TYPE_TIMESTAMP,SQL_TYPE_TIMESTAMP,
					19,0,
					datebind[i]+(processed-bind_start),
					sizeof(TIMESTAMP_STRUCT),
					(SQLLEN *)(datelen[i]+(processed-bind_start)));
			}
			break;
		default :
			errprintf("\nWODBCStatement::Execute 中遇到不可识别的字段类型！\n");
			throw -2;
		}
	}
	if(bindednum>0)
	 rc=SQLSetStmtAttr(sthp,SQL_ATTR_PARAMSET_SIZE,(SQLPOINTER)(process<0?1:process),4);
	ocheckerr(rc,3,sthp,CHECK_OPT_NULL);
}

sword WODBCStatement::Execute(int times,int offset) {
	long nPerTabExtand = 0;
	SQLRETURN rc=0;
	if(times==0) eof=false;
	if(echo) lgprintf("Excuting :\n %s... ",sqlstmt);
	SQLLEN processed=0;
	bind_start=bind_num=-1;
	SQLLEN process=times-processed;
	executing=true;
	if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
	if(m_pDataTab !=NULL)
	{
	   nPerTabExtand = m_pDataTab->GetVarMaxFetchRow(process);    
	}
	if(!IsSelect(sqlstmt) && !HasParam(sqlstmt)) {
		rc=SQLExecDirect(sthp,(SQLCHAR*)sqlstmt,strlen(sqlstmt));
		chkstmt(rc);
	}	
	else while(!GetSession()->IsTerminate()){
		if(rc!=SQL_STILL_EXECUTING) {
			process=times-processed;
			if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
			if(processed<(bind_start+bind_num))
				process=(bind_start+bind_num-processed);

			if (m_pDataTab!=NULL && process > 0 && (m_nFetchWriteRow%nPerTabExtand)==0)
			{
			   for (int wrn = 0;wrn<m_nNumWriteCol;wrn++)
			   {
				   int nCol = m_nWriteCol[wrn];
				   m_pDataTab->ExtendColVarData(nCol,m_nTabSartRow+m_nFetchWriteRow,process, (char**)&bindedptr[m_nBindID[wrn]]);
			   }
			   m_nStartSecNum=m_nFetchWriteRow;
			}
			RealBind(process,processed);
		}
		rc=SQLExecute(sthp);
		chkstmt(rc);
		if(rc==SQL_STILL_EXECUTING) {
			mSleep(10); 
		}
		else {
			SQLRowCount(sthp, (SQLLEN*)&process);
			chkstmt(rc);
      // DB2 Client 8.2 to server 8.1,above 'SQLRowCount ' return wodbcStatement.cpp:CLI0115E  Invali
      // so skip error check to test....
      // if(process==-1) 
		 m_nFetchWriteRow = m_nFetchWriteRow + process;
		 if(process<0) {ReleaseDataTale();break;}  			
			processed+=process;
			if(processed>=times || process==0)//解决查询不到对应的条件，进入死循环
			{
				ReleaseDataTale();
				break;
			}  	
		}
	}

	if(GetSession()->IsTerminate()) {
		SetCancel();
		BreakAndReset();
		return retval; //been set to -1;
	}
	SQLLEN rct1;
	rc = SQLRowCount(sthp, (SQLLEN*)&rct1);
	rowct=rct1;
	if(int(rowct)<0 || int(rowct)==-1 || times==0) rowct=0;
	chkstmt(rc);
	//if(retval!=OCI_NO_DATA)
	//OCIAttrGet((dvoid *) sthp, (ub4) OCI_HTYPE_STMT,
	//               (dvoid *) &rowct, NULL,
	//                OCI_ATTR_ROW_COUNT, errhp);
	//	int num_errs;
	//	OCIAttrGet (sthp, OCI_HTYPE_STMT, &num_errs, 0,
	//            OCI_ATTR_NUM_DML_ERRORS, errhp);
	//    if (num_errs) checkerr(OCI_ERROR);
	
	if(times!=0) {
		if(echo) lgprintf(" %d rows affected.\n",times);
	}
	else if(echo) lgprintf("Done.\n");
	executing=false;
	executed=true;
	return retval;
}

sword WODBCStatement::BreakAndReset() {
	SQLRETURN rc;
	rc=SQLCancel(sthp);
	chkstmt(rc);
	return rc;
}


bool WODBCStatement::DescribeStmt()
{
	SQLRETURN rc;
	if(HasParam(sqlstmt)) {
	 SQLSMALLINT NumParams=0;
	 rc=SQLNumParams(sthp, &NumParams);
	 chkstmt(rc);
	 SQLSMALLINT datatype=0;
	 SQLULEN psize=0;
	 SQLSMALLINT dec=0,nablle=0;
	 for(int p=0;p<NumParams;p++)
	 {
		 rc=SQLDescribeParam(
			sthp,
			p+1,
			&datatype,
			&psize,
			&dec,&nablle);
		 //chkstmt(rc);
	 }
	//if(NumParams>0) return true;
	}
	if(!IsSelect(sqlstmt)) return true;
	colct=0;
	//change 2005/06/27 ,colct在Col_Desc中定义为sb4,需要使用临时变量来装换,否则,BIG_ENDDIAN系统异常
	SQLSMALLINT colct1;
	rc = SQLNumResultCols(sthp,(SQLSMALLINT*)&colct1);
	colct=colct1;
	chkstmt(rc);
	SQLSMALLINT nullable;
	SQLSMALLINT clen;
	for(SQLSMALLINT i = 0; i < colct; i++)
	{
		SQLULEN dlen;
		//change 2005/06/27 ,scale在Col_Desc中定义为sb1,需要使用临时变量来装换,否则,BIG_ENDDIAN系统异常
		SQLSMALLINT scalex;
		rc = SQLDescribeCol(sthp,i+1,(SQLCHAR*)coldsc[i].colname, COLNAME_LEN, &clen,
			(SQLSMALLINT *)&coltype[i],
			(SQLULEN *)&dlen,
			(SQLSMALLINT *)&scalex,
			&nullable);
		chkstmt(rc);
		coldsc[i].scale=scalex;
		coldsc[i].prec=dlen;
		coldsc[i].dtsize=dlen;
		coldsc[i].type=coltype[i];
		coldsc[i].colname[clen]=0;
		strcpy(coldsc[i].dispname,coldsc[i].colname);
		switch(coltype[i]) {
/* myodbc 5.1.3 return fff7 for varchar column */
		case (short)0xfff7:
		case SQL_CHAR :
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
                       //>> begin: fix dma-633
            if(dlen < 1)
            {
                strcpy(filen," ");
                lineid=-1;
                sprintf(errbuf,"\n Column[%d] is string type,Error:lengtn is 0,please check table structure.\nSQL:\n%s\n",i,sqlstmt);
                       errcode=-1;
                Throw();       
            }                  
            //<< end: fix dma-633
			coldsc[i].dtsize=dlen;
			coldsc[i].dspsize=min(clen+1,dlen);
			coldsc[i].type=SQLT_CHR;
			break;
			//				case SQL_WCHAR :
			//				case SQL_WVARCHAR :
			//				case SQL_WLONGVARCHAR :
		case SQL_BIT :
		case SQL_BINARY :
		case SQL_VARBINARY :
		case SQL_LONGVARBINARY :
			break;
		case SQL_SMALLINT :
		case SQL_INTEGER :
		case SQL_TINYINT :
			coldsc[i].type=SQLT_INT;
			break;
		case SQL_BIGINT :
			coldsc[i].type=SQLT_LNG;
			break;
		case SQL_REAL :
		case SQL_FLOAT :
		case SQL_DOUBLE :
			coldsc[i].type=SQLT_FLT;
			break;
		case SQL_DECIMAL :
		case SQL_NUMERIC :
			if(coldsc[i].prec==0 && coldsc[i-1].scale==0) {
				coldsc[i].prec=DEFAULT_NUMBER_PREC;
				coldsc[i-1].scale=DEFAULT_NUMBER_SCALE;
			}
			if(coldsc[i].prec>DEFAULT_NUMBER_PREC) coldsc[i].prec=DEFAULT_NUMBER_PREC;
			if(coldsc[i].scale<0) coldsc[i].scale=0;
			/* add type convert to fit dpadmin set PK index on dp_path etc.
			   2008/05/09.
			   */
			if(coldsc[i].prec<DEFAULT_NUMBER_PREC && coldsc[i].scale==0)
			 coldsc[i].type=SQLT_INT;
			else coldsc[i].type=SQLT_NUM;
			break;
		case SQL_TYPE_TIME :
			break;
		case SQL_TYPE_DATE :
		case SQL_TYPE_TIMESTAMP :
			coldsc[i].type=SQLT_DAT;
			break;
		case SQL_INTERVAL_MONTH :
		case SQL_INTERVAL_YEAR :
		case SQL_INTERVAL_YEAR_TO_MONTH :
		case SQL_INTERVAL_DAY :
		case SQL_INTERVAL_HOUR :
		case SQL_INTERVAL_MINUTE :
		case SQL_INTERVAL_SECOND :
		case SQL_INTERVAL_DAY_TO_HOUR :
		case SQL_INTERVAL_DAY_TO_MINUTE :
		case SQL_INTERVAL_DAY_TO_SECOND :
		case SQL_INTERVAL_HOUR_TO_MINUTE :
		case SQL_INTERVAL_HOUR_TO_SECOND :
		case SQL_INTERVAL_MINUTE_TO_SECOND:
		case SQL_GUID :
			break;
		}
	}
	return true;
}


//>> Begin , get column info ,after Prepare(const char *_sqlstmt)
int  WODBCStatement::GetColumnNum()
{
    return colct;
}
bool WODBCStatement::GetColumnName(int col,char* colname)
{
    if(col < colct && col >=0){
		strcpy(colname,coldsc[col].colname);
        return true;
    }
	else
		return false;
}
int  WODBCStatement::GetColumnType(int col)
{
    if(col < colct && col >=0)
		return coldsc[col].type;
	else
		return -1;
}
int  WODBCStatement::GetColumnSize(int col)
{
    if(col < colct && col >=0)
		return coldsc[col].dtsize;
    else
		return -1;
}
bool  WODBCStatement::SetColumnSize(int col,int size)
{
    if(col < colct && col >=0){
		coldsc[col].dtsize = size;
        return true;
    }
	else
		return false;
}
//<< End, get column info ,after Prepare(const char *_sqlstmt)



int WODBCStatement::NewStmt()
{
	SQLRETURN rc;
	if(sthp) {
		//rc=SQLFreeStmt(sthp,SQL_UNBIND);
		//chkstmt(rc);
		rc = SQLFreeStmt(sthp, SQL_DROP);
		chkstmt(rc);
	}
	rc = SQLAllocHandle(SQL_HANDLE_STMT,((WODBCSession *)psess)->hdbc,&sthp);
	((WODBCSession *)psess)->chkdbc(rc);
	ClearNullBind();
	return retval;
}

int WODBCStatement::TestTable(const char *tbname)
{
	char sql[200];
	NewStmt();
	SQLRETURN rc;
	sprintf(sql,"select * from %s",tbname);
	rc=SQLExecDirect(sthp,(SQLCHAR *)sql,strlen(sql));
	chktesttab(rc);
	if(errcode==1146) return 0;
	return 1;
}

void WODBCStatement::PrepareDefine(int pos)
{
	
if(!pind[pos]) pind[pos]=(sb2 *)(new NULL_INDTYPE[fetchsize]);
//if(!plen[pos]) plen[pos]=new sb2[fetchsize];
//if(!pret[pos]) pret[pos]=new sb2[fetchsize];
memset(pind[pos],0,sizeof(NULL_INDTYPE)*(fetchsize));
//memset(plen[pos],0,sizeof(sb2)*fetchsize);
//memset(pret[pos],0,sizeof(sb2)*fetchsize);		
}

void WODBCStatement::SetNullDateBind(int rnum)
{
}


#endif

