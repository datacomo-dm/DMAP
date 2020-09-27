#include "wdbi.h"

#ifdef __unix
extern struct timespec interval ;
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern OCIEnv *envhp;
extern OCIError *errhp;

extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;
/*
buffer_type Value  SQL Type  C Type  
MYSQL_TYPE_TINY  TINYINT  char  
MYSQL_TYPE_SHORT  SMALLINT  short int  
MYSQL_TYPE_LONG  INT  int  
MYSQL_TYPE_LONGLONG  BIGINT  long long int  
MYSQL_TYPE_FLOAT  FLOAT  float  
MYSQL_TYPE_DOUBLE  DOUBLE  double  
MYSQL_TYPE_TIME  TIME  MYSQL_TIME  
MYSQL_TYPE_DATE  DATE  MYSQL_TIME  
MYSQL_TYPE_DATETIME  DATETIME  MYSQL_TIME  
MYSQL_TYPE_TIMESTAMP  TIMESTAMP  MYSQL_TIME  
MYSQL_TYPE_STRING  CHAR   
MYSQL_TYPE_VAR_STRING  VARCHAR   
MYSQL_TYPE_TINY_BLOB  TINYBLOB/TINYTEXT   
MYSQL_TYPE_BLOB  BLOB/TEXT   
MYSQL_TYPE_MEDIUM_BLOB  MEDIUMBLOB/MEDIUMTEXT   
MYSQL_TYPE_LONG_BLOB  LONGBLOB/LONGTEXT  
*/
WMYSQLStatement::WMYSQLStatement(WDBISession *s,bool atthr):WDBIStatement(s,atthr) {
	pstmt=NULL; pInBind=pOutBind=NULL;
	pRes=NULL;pInLen=NULL;pIsNull=NULL;pOutLen=NULL;
	tmOut=NULL;tmIn=NULL;
}

bool WMYSQLStatement::BindByPos(int pos,char *result,int size) {
#ifndef NO_ORACLE
	pInBind[pos-1].buffer_type=MYSQL_TYPE_STRING;
	pInBind[pos-1].buffer= (char *)result;
	pInBind[pos-1].is_null= NULL;
	pInLen[pos-1]=size;
	isdtIn[pos-1]=0;
	pInBind[pos-1].buffer_length=size;
	pInBind[pos-1].length= &(pInLen[pos-1]);
	if(!executing) {
	 if(executed) {bindednum=0;executed=false;}
	 bindedptr[bindednum]=(void *)result;
	 bindedtype[bindednum]=COLUMN_TYPE_CHAR;
	 bindedlen[bindednum]=size;
	 bindednum++;
	}
#endif
	return true;
}
		
bool WMYSQLStatement::BindByPos(int pos,int *result) {
#ifndef NO_ORACLE
	pInBind[pos-1].buffer_type=MYSQL_TYPE_LOGN;
	pInBind[pos-1].buffer= (char *)result;
	pInBind[pos-1].is_null= NULL;
	isdtIn[pos-1]=0;
	pInLen[pos-1]=sizeof(int);
	pInBind[pos-1].buffer_length=sizeof(int);
	pInBind[pos-1].length= &(pInLen[pos-1]);
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_INT;
	bindednum++;
	}
#endif
	return true;
}

bool WMYSQLStatement::BindByPos(int pos,double *result) {
#ifndef NO_ORACLE
	pInBind[pos-1].buffer_type=MYSQL_TYPE_DOUBLE ;
	pInBind[pos-1].buffer= (char *)result;
	pInBind[pos-1].is_null= NULL;
	pInLen[pos-1]=sizeof(double);
	isdtIn[pos-1]=0;
	pInBind[pos-1].buffer_length=sizeof(double);
	pInBind[pos-1].length= &(pInLen[pos-1]);
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_FLOAT;
	bindednum++;
	}
#endif
	return true;
}

/*
bool WMYSQLStatement::BindByPos(int pos,unsigned char *result) {
#ifndef NO_ORACLE
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, 21,SQLT_NUM,
           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_NUM;
	bindednum++;
	}
#endif
	return true;
}
*/

bool WMYSQLStatement::BindByPos(int pos,char *date) {
#ifndef NO_ORACLE
/*
	tmIn[pos].year=(date[0]-100)*100+date[1]-100;
	tmIn[pos].month=date[2];
	tmIn[pos].day=date[3];
	tmIn[pos].hour=date[4]-1;
	tmIn[pos].minute=date[5]-1;
	tmIn[pos].second=date[6]-1;
*/
	pInBind[pos-1].buffer_type=MYSQL_TYPE_DATETIME ;
	pInBind[pos-1].buffer= (char *)(tmIn+pos);
	pInBind[pos-1].is_null= NULL;
	isdtIn[pos-1]=1;
	pInLen[pos-1]=sizeof(MYSQL_TIME);
	pInBind[pos-1].buffer_length=sizeof(MYSQL_TIME);
	pInBind[pos-1].length= &(pInLen[pos-1]);
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	 bindedptr[bindednum]=(void *)result;
	 bindedtype[bindednum]=MYSQL_TYPE_DATETIME;
	 bindednum++;
	}
#endif
	return true;
}


bool WMYSQLStatement::DefineByPos(int pos,char *result,int size) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	pOutBind[pos-1].buffer_type=MYSQL_TYPE_STRING;
	pOutBind[pos-1].buffer= (char *)result;
	pOutBind[pos-1].is_null= NULL;
	pOutLen[pos-1]=size;
	isdtOut[pos-1]=0;
	pOutBind[pos-1].buffer_length=size;
	pOutBind[pos-1].length= &(pOutLen[pos-1]);
#endif
	return true;
}

bool WMYSQLStatement::DefineByPos(int pos,int *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	pOutBind[pos-1].buffer_type=MYSQL_TYPE_LOGN;
	pOutBind[pos-1].buffer= (char *)result;
	pOutBind[pos-1].is_null= NULL;
	pOutLen[pos-1]=sizeof(int);
	isdtOut[pos-1]=0;
	pOutBind[pos-1].buffer_length=sizeof(int);
	pOutBind[pos-1].length= &(pOutLen[pos-1]);
#endif
	return true;
}

bool WMYSQLStatement::DefineByPos(int pos,double *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	pOutBind[pos-1].buffer_type=MYSQL_TYPE_DOUBLE;
	pOutBind[pos-1].buffer= (char *)result;
	pOutBind[pos-1].is_null= NULL;
	isdtOut[pos-1]=0;
	pOutLen[pos-1]=sizeof(double);
	pOutBind[pos-1].buffer_length=sizeof(double);
	pOutBind[pos-1].length= &(pOutLen[pos-1]);
#endif
	return true;
}

bool WMYSQLStatement::DefineByPos(int pos,char *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	pOutBind[pos-1].buffer_type=MYSQL_TYPE_DATETIME;
	pOutDate[pos-1]=result;
	pOutBind[pos-1].buffer= (char *)(tmOut+pos-1);
	pOutBind[pos-1].is_null= NULL;
	pOutLen[pos-1]=sizeof(MYSQL_TIME);
	isdtOut[pos-1]=1;
	pOutBind[pos-1].buffer_length=sizeof(MYSQL_TIME);
	pOutBind[pos-1].length= &(pOutLen[pos-1]);
#endif
	return true;
}
//13011202858
//66505830
bool WMYSQLStatement::Prepare(char *_sqlstmt)
{
#ifndef NO_ORACLE
	eof=false;
	if(NewStmt()) return false;
	if(strlen(_sqlstmt)>2000) return false;
	strcpy(sqlstmt,_sqlstmt);
	if (mysql_stmt_prepare(pstmt, sqlstmt, strlen(sqlstmt)))
		checkstmterr(mysql_stmt_errno(pstmt));
	paramct= mysql_stmt_param_count(pstmt);
	if(paramct>0) {	 
	 if(pInBind) delete []pInBind;
	 pInBind=new WDBI_MYBIND[paramct];
	 memset(pInBind,0,sizeof(WDBI_MYBIND)*paramct);
	}
	if(pRes) mysql_free_result(pRes);
	pRes = mysql_stmt_result_metadata(pstmt);
	if(!pRes) checkstmterr(mysql_stmt_errno(pstmt));
	colct= mysql_num_fields(pRes);
	if(colct>0) {
	 if(pOutBind) delete []pOutBind;
	 pOutBind=new WDBI_MYBIND[colct];
	 memset(pOutBind,0,sizeof(WDBI_MYBIND)*colct);
	}
	if(!DescribeStmt()) return false;
		
	bindednum=0;
	executed=false;
	executing=false;
	rowct=0;
#endif
	return true;
}

int WMYSQLStatement::Fetch(int rows,bool first)
{
#ifndef NO_ORACLE
	if(eof) return 0;
	int rct=0;
	while (true) {
		if(rct>=rows) break;
		for(int i=0;i<colct;i++) {
		 if(isdtOut[i]==1)
		  pOutBind[i].buffer+=pOutBind[i].buffer_length;
		if (mysql_stmt_bind_result(stmt, pOutBind))
		 checkerr(-1);
		if(mysql_stmt_fetch(stmt)) break;
	}
	while((retval=OCIStmtFetch(sthp,errhp,rows,OCI_FETCH_NEXT,OCI_DEFAULT))
		==OCI_STILL_EXECUTING && !GetSession()->IsTerminate()) {
               mSleep(10);
//#ifdef __unix
//		/*pthread_delay_np (&interval);*/
//                      usleep(10);	
//#else
//			Sleep(10);
//#endif
	}
	if(GetSession()->IsTerminate()) {
		SetCancel();
		BreakAndReset();
		return retval; //been set to -1;
	}
	else if(retval!=OCI_STILL_EXECUTING && retval!=OCI_SUCCESS) {
		checkerr(retval);
	}
		if(retval==OCI_NO_DATA) eof=true;
		if(GetErrCode()==1405 || GetErrCode()==1403) //fetched column value is null
			retval=OCI_SUCCESS_WITH_INFO;
		OCIAttrGet((dvoid *) sthp, (ub4) OCI_HTYPE_STMT,
                 (dvoid *) &rowct, NULL,
                  OCI_ATTR_ROW_COUNT, errhp);
	if(echo) lgprintf(".");
#ifdef __unix
	if(__output_to_console)
	fflush(stdout);
#endif
#endif
	return retval;
}

sword WMYSQLStatement::Execute(int times,int offset) {
#ifndef NO_ORACLE
	if(times==0) eof=false;
	if(echo) lgprintf("Excuting :\n %s... ",sqlstmt);
	int processed=0;
	int process=times-processed;
	executing=true;
	if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
	while(!GetSession()->IsTerminate()){
		//retval=OCIStmtExecute(((WOCISession *)psess)->svchp,sthp,errhp,process,offset,
		//(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL,OCI_BATCH_ERRORS );// OCI_DEFAULT);
		retval=OCIStmtExecute(((WOCISession *)psess)->svchp,sthp,errhp,process,offset,
		(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
		if(retval==OCI_STILL_EXECUTING)
                {     mSleep(10); }
//#ifdef __unix
///*pthread_delay_np (&interval);*/
//                        usleep(10);		
//#else
//			Sleep(10);
//#endif
		else if(retval==OCI_ERROR) { 
			/*
		 sb4 ec;
		 (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &ec,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
		 if(ec==1010) Sleep(1000);
		 else */
			 break;
		}
		else {
			processed+=process;
			if(processed>=times)
			 break;
			 int i;
			for( i=0;i<bindednum;i++) {
				switch(bindedtype[i]) {
				case COLUMN_TYPE_FLOAT:
					BindByPos(i+1,((double *)bindedptr[i])+processed);
					break;
				case COLUMN_TYPE_INT:
					BindByPos(i+1,((int *)bindedptr[i])+processed);
					break;
				case COLUMN_TYPE_CHAR:
					BindByPos(i+1,((char *)bindedptr[i])+processed*bindedlen[i],bindedlen[i]);
					break;
				case COLUMN_TYPE_DATE:
					BindByPos(i+1,((char *)bindedptr[i])+processed*7);
					break;
				default :
					errprintf("\nWMYSQLStatement::Execute 中遇到不可识别的字段类型！\n");
					throw -2;
				}
			}
			process=times-processed;
			if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
		}
	}
	if(GetSession()->IsTerminate()) {
		SetCancel();
		BreakAndReset();
		return retval; //been set to -1;
	}
	else if(retval!=OCI_STILL_EXECUTING && retval!=OCI_SUCCESS) {
		checkerr(retval);
	}
		if(retval!=OCI_NO_DATA)
		OCIAttrGet((dvoid *) sthp, (ub4) OCI_HTYPE_STMT,
                 (dvoid *) &rowct, NULL,
                  OCI_ATTR_ROW_COUNT, errhp);
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
#endif
	return retval;
}

bool WMYSQLStatement::DescribeStmt()
{
#ifndef NO_ORACLE
	MYSQL_FIELD *field;
	int i=0;
	while((field = mysql_fetch_field(result)))
	{
	 strcpy(coldsc[i].colname,field->name);
	 strcpy(coldsc[i].dispname,field->name);
	 coldsc[i].dtsize=field->length;
	 coldsc[i].dspsize=max(strlen(field->name)+1,field->length+1);
	 coldsc[i].prec=field->max_length;
	 coldsc[i].scale=field->decimal;
	 coldsc[i].type=field->type;
    	 switch(field->type) {
		case FIELD_TYPE_VAR_STRING:
		case FIELD_TYPE_STRING:
			coldsc[i].type=SQLT_CHR;
			break;
    	 	case FIELD_TYPE_TINY:
		case FIELD_TYPE_SHORT:
		case FIELD_TYPE_LONG:
			
		case FIELD_TYPE_INT24:
		case FIELD_TYPE_ENUM:
		case FIELD_TYPE_SET:
    	 	case FIELD_TYPE_DECIMAL:
		case FIELD_TYPE_FLOAT:  
		case FIELD_TYPE_DOUBLE:
	 		if(coldsc[i].prec==0 && coldsc[i].scale==0) {
		    		coldsc[i].prec=DEFAULT_NUMBER_PREC;
		    		coldsc[i].scale=DEFAULT_NUMBER_SCALE;
			}
			if(coldsc[i].prec>DEFAULT_NUMBER_PREC) coldsc[i].prec=DEFAULT_NUMBER_PREC;
			if(coldsc[i].scale<0) coldsc[i].scale=0;
			coldsc[i].type=SQLT_NUM;
			break;
		case FIELD_TYPE_TIMESTAMP:
		case FIELD_TYPE_DATE:
		case FIELD_TYPE_TIME:
		case FIELD_TYPE_DATETIME:
		case FIELD_TYPE_YEAR:
		case FIELD_TYPE_NEWDATE:
			coldsc[i].type=SQLT_DATE;
			coldsc[i].dtsize=7;
			break;
/*		default :
		case FIELD_TYPE_NULL:
		case FIELD_TYPE_LONGLONG:
		case FIELD_TYPE_TINY_BLOB:
		case FIELD_TYPE_MEDIUM_BLOB:
		case FIELD_TYPE_LONG_BLOB:
		case FIELD_TYPE_BLOB:
		case FIELD_TYPE_GEOMETRY:
*/
	}
	i++;
	}
#endif
	return true;
}

void WMYSQLStatement::checkerr(sword status) {
  errcode=status;
  if(status!=OCI_SUCCESS) {
  	checkstmterr(status);
  	return;
  }
  switch (status)
  {
  case OCI_SUCCESS:
	errcode=0;
    break;
  case OCI_SUCCESS_WITH_INFO:
    strcpy(errbuf,"Error - OCI_SUCCESS_WITH_INFO");
    break;
  case OCI_NEED_DATA:
    strcpy(errbuf,"Error - OCI_NEED_DATA");
	if(autoThrow) {
		strcpy(filen," ");
		lineid=-1;
		Throw();
	}
    break;
  case OCI_NO_DATA:
    strcpy(errbuf,"Error - OCI_NODATA");
    break;
  case OCI_ERROR:
#ifndef NO_ORACLE
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
	if(autoThrow && errcode!=1405 && errcode!=1403) {
		strcpy(filen," ");
		lineid=-1;
		Throw();
	}
#endif
    //(void) printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    strcpy(errbuf,"Error - OCI_INVALID_HANDLE");
	if(autoThrow) {
		strcpy(filen," ");
		lineid=-1;
		Throw();
	}
	break;
  case OCI_STILL_EXECUTING:
    strcpy(errbuf,"Error - OCI_STILL_EXECUTE");
    break;
  case OCI_CONTINUE:
    strcpy(errbuf,"Error - OCI_CONTINUE");
    break;
  default:
    break;
  }
}


void WMYSQLStatement::checkstmterr(sword status)
{
#ifndef NO_ORACLE
	int row_off=0,vsize=4;
	ub2 st_off=0;
	OCIError *errhndl;
	int num_errs;
	errbuf[0]=0;
	
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
	OCIAttrGet (sthp,OCI_HTYPE_STMT,&st_off,0,
				  OCI_ATTR_PARSE_ERROR_OFFSET,errhp);
	if(st_off) {
			//sprintf(errbuf+strlen(errbuf),"\n错误出现位置: %s.",sqlstmt);
			strcat(errbuf,"\n");
			strcat(errbuf,sqlstmt);
			errbuf[strlen(errbuf)-(strlen(sqlstmt)-st_off)]=0;
			strcat(errbuf,"^");
			strcat(errbuf,sqlstmt+st_off);
			strcat(errbuf,"\n");
	}
	
	OCIAttrGet (sthp, OCI_HTYPE_STMT, &num_errs, 0,
            OCI_ATTR_NUM_DML_ERRORS, errhp);
	if(num_errs>0) {
	(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhndl, OCI_HTYPE_ERROR,
                  (size_t) 0, (dvoid **) 0);
		strcat(errbuf,"\n以下语句绑定数据错误:\n");
		strcat(errbuf,sqlstmt);
		strcat(errbuf,"\n");
	int procne=(num_errs>20?20:num_errs);
	for(int i=0;i<procne;i++) {
		OCIParamGet(errhp, OCI_HTYPE_ERROR,errhp, (void **)&errhndl, i);
		row_off=-1;
		OCIAttrGet (errhndl, OCI_HTYPE_ERROR, (ub4 *)&row_off, (ub4 *)&vsize, 
                  OCI_ATTR_DML_ROW_OFFSET, errhp);
		if(row_off>=0) {
			sprintf(errbuf+strlen(errbuf),"\n错误出现在第%d行数据.",row_off+1);
	
	    (void) OCIErrorGet((dvoid *)errhndl, (ub4) 1, (text *) NULL, &errcode,
                        (unsigned char *)errbuf+strlen(errbuf), (ub4) sizeof(errbuf)-strlen(errbuf), OCI_HTYPE_ERROR);
		char *pf=errbuf+strlen(errbuf);
		strcpy(pf,"Binded parameter(s):\n");
		for(int j=0;j<bindednum;j++) {
			switch(bindedtype[j]) {
				case COLUMN_TYPE_FLOAT:
					sprintf(pf+strlen(pf),"%f,",((double *)bindedptr[j])[row_off]);
					break;
				case COLUMN_TYPE_INT:
					sprintf(pf+strlen(pf),"%d,",((int *)bindedptr[j])[row_off]);
					break;
				case COLUMN_TYPE_CHAR:
					sprintf(pf+strlen(pf),"'%s',",((char *)bindedptr[j])+row_off*bindedlen[j]);
					break;
				case COLUMN_TYPE_DATE:
					{
						char str[30];
						WOCIDate dt((char *)bindedptr[j]+row_off*7);
						dt.GetString(str);
						strcat(pf,str);
					}
					break;
				default :
					errprintf("\nWMYSQLStatement::Execute 中遇到不可识别的字段类型！\n");
					throw -2;
				}
		}
		}
	}
	if(num_errs>20) {
		sprintf(errbuf+strlen(errbuf),"\n.......\n错误行数%d.\n",num_errs);
	}
	OCIHandleFree(errhndl,OCI_HTYPE_ERROR);
	}
	
	else if(st_off==0 && (errcode==0||errcode==1405||errcode==1403)) {
		//WDBIError::checkerr(status);
		return;
	}
	if(autoThrow && errcode!=1405 && errcode!=1403) {
		strcpy(filen," ");
		lineid=-1;
		Throw();
	}

/*
OCI_ATTR_PARSE_ERROR_OFFSET
Mode 
READ 

Description 
Returns the parse error offset for a statement. 

Attribute Datatype 
ub2 * 



OCI_ATTR_DML_ROW_OFFSET

Mode 
READ 

Description 
Returns the offset (into the DML array) at which the error occurred. 
Attribute Datatype 
ub4 * 

*/
#endif
    //(void) printf("Error - %.*s\n", 512, errbuf);
}

int WMYSQLStatement::NewStmt()
{
	int retval=0;
#ifndef NO_ORACLE
	if(pstmt) mysql_stmt_close(pstmt);
	pstmt = mysql_stmt_init(((WMYSQLSession *)psess)->pData));
	if(pstmt==NULL) {
		retval=-1;
		checkerr(retval);
	}
#endif
	return retval;
}


int WMYSQLStatement::TestTable(char *tbname)
{
#ifndef NO_ORACLE
  return ((WMYSQLSession *)psess)->TestTable(tbname);
#endif
  return 0;
}


void WMYSQLStatement::PrepareDefine(int pos)
{
/*	
	if(!pind[pos]) pind[pos]=new sb2[fetchsize];
	if(!plen[pos]) plen[pos]=new sb2[fetchsize];
	if(!pret[pos]) pret[pos]=new sb2[fetchsize];
	memset(pind[pos],0,sizeof(sb2)*fetchsize);
	memset(plen[pos],0,sizeof(sb2)*fetchsize);
	memset(pret[pos],0,sizeof(sb2)*fetchsize);
*/	
}



void WDBIStatement::ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params)
	{
	 SetErrPos(ln,fn);
	 if(ercd==DT_ERR_OCIERR) {
		char *msg;
		GetLastError(errcode,&msg);
		strcpy(errbuf,msg);
		retval=ercd;
	 }
	 else
	 {
		retval=errcode=ercd;
		sprintf(errbuf,"Exception : %s on statement. \n,sql text : '%s'.row num:%d,column num:%d,binded column num:%d\n."
		        ,DT_ERR_MSG[ercd],sqlstmt==NULL?"NULL":sqlstmt,rowct,colct,bindednum);
	 }
	 if(autoThrow) Throw();
	}
}