#include "wdbi.h"

#ifdef __unix
extern struct timespec interval ;
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];


extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;

int WDBIStatement::DEFAULT_NUMBER_PREC =10;
int WDBIStatement::DEFAULT_NUMBER_SCALE =3;


#ifndef NO_OCI
extern OCIEnv *envhp;
extern OCIError *errhp;
WOCIStatement::WOCIStatement(WDBISession *s,bool atthr):WDBIStatement(s,atthr) {
		sthp=NULL;		
	  for(int i=0;i<MAX_COLUMN;i++) {
			dfp[i]=NULL;
			bdp[i]=NULL;
	  }
}

bool WOCIStatement::BindByPos(int pos,char *result,int size) {
#ifndef NO_ORACLE
     if(result != NULL) //变长字段不用绑定，到执行的时候在绑定
     {
         	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, size,SQLT_STR,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	       {
		         checkerr(retval);
		         return false;
	       }
	 }
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedptr[bindednum]=(void *)result;
	bindedpos[bindednum]=pos;
	bindedtype[bindednum]=COLUMN_TYPE_CHAR;
	bindedlen[bindednum]=size;
	bindednum++;
	}
#endif
	return true;
}
		
bool WOCIStatement::BindByPos(int pos,int *result) {
#ifndef NO_ORACLE
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, sizeof(int),SQLT_INT,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) sizeof(int), (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedpos[bindednum]=pos;
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_INT;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatement::BindByPos(int pos,LONG64 *result) {
#ifndef NO_ORACLE
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, sizeof(LONG64),SQLT_NUM,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) sizeof(LONG64), (ub4 *) 0, OCI_DEFAULT))
//           (dvoid *) result, sizeof(LONG64),SQLT_FLT,
//           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) sizeof(LONG64), (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedpos[bindednum]=pos;
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_BIGINT;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatement::BindByPos(int pos,double *result) {
#ifndef NO_ORACLE
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, sizeof(double),SQLT_FLT,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedpos[bindednum]=pos;
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_FLOAT;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatement::BindByPos(int pos,unsigned char *result) {
#ifndef NO_ORACLE
	if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) result, 21,SQLT_NUM,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
	if(!executing) {
	if(executed) {bindednum=0;executed=false;}
	bindedpos[bindednum]=pos;
	bindedptr[bindednum]=(void *)result;
	bindedtype[bindednum]=COLUMN_TYPE_NUM;
	bindednum++;
	}
#endif
	return true;
}

bool WOCIStatement::BindByPos(int pos,char *result) {
#ifndef NO_ORACLE
	if(!executing) {
	 if(executed) {bindednum=0;executed=false;}
	 bindedpos[bindednum]=pos;
	 bindedptr[bindednum]=(void *)result;
	 bindedtype[bindednum]=COLUMN_TYPE_DATE;
	 bindednum++;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,char *result,int size) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, size,SQLT_STR,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,int *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, sizeof(int),SQLT_INT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,LONG64 *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, sizeof(LONG64),SQLT_INT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,double *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, sizeof(double),SQLT_FLT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,unsigned char *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, 21,SQLT_NUM,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::DefineByPos(int pos,char *result) {
#ifndef NO_ORACLE
	PrepareDefine(pos);
	if(retval=OCIDefineByPos(sthp, &(dfp[pos]), errhp, pos,
           (dvoid *) result, 7,SQLT_DAT,
           (dvoid *) pind[pos], (ub2 *) plen[pos], (ub2 *) pret[pos], OCI_DEFAULT))
	{
		checkerr(retval);
		return false;
	}
#endif
	return true;
}

bool WOCIStatement::Prepare(const char *_sqlstmt)
{
#ifndef NO_ORACLE
	eof=false;
	if(NewStmt()) return false;

	if(strlen(_sqlstmt)>=MAX_STMT_LEN) return false;

	strcpy(sqlstmt,_sqlstmt);
	if(retval=OCIStmtPrepare(sthp, errhp, (text *)sqlstmt,
                                (ub4) strlen((char *) sqlstmt),
                                (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT/*OCI_NO_SHARING*/))
	{
		checkerr(retval);
		return false;
	}
	if(!DescribeStmt()) return false;
	OCIHandleFree(sthp,OCI_HTYPE_STMT);
	sthp=NULL;
	if(NewStmt()) return false;
    	if(retval=OCIStmtPrepare(sthp, errhp, (text *)sqlstmt,
                                (ub4) strlen((char *) sqlstmt),
                                (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT/*OCI_NO_SHARING*/))
	{
		checkerr(retval);
		return false;
	}

	bindednum=0;
	executed=false;
	executing=false;
#endif
	return true;
}

int WOCIStatement::Fetch(int rows,bool first)
{
#ifndef NO_ORACLE
	if(eof) return 0;
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
	if(__output_to_console && echo) {
		char str[40];
		frprt(rowct,str);
		printf("  %s rows fetched.\r",str);
	}

#ifdef __unix
	if(__output_to_console && echo)
	 fflush(stdout);
#endif
#endif
	return retval;
}

sword WOCIStatement::Execute(int times,int offset) {
#ifndef NO_ORACLE
	if(times==0) eof=false;
	if(echo) lgprintf("Excuting :\n %s... ",sqlstmt);
	int processed=0;
	int process=times-processed;
	executing=true;
	if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
	if(m_pDataTab !=NULL)
	{
	   process = m_pDataTab->GetVarMaxFetchRow(process);    
	}
	int i;
	for( i=0;i<bindednum;i++) {
		switch(bindedtype[i]) {
		case COLUMN_TYPE_BIGINT:
			{
  			 double *pd=(double *)bindedptr[i];
  			 LONG64 *pl=(LONG64 *)bindedptr[i];
  			 int lct=0;
  			 while(lct++<process) *pd++=(double)*pl++;
  			}
		}
	}
	SetNullDateBind(process);
	while(!GetSession()->IsTerminate()){

        ////////////////////////////VarLen by chxl 2013.8.24
    if (m_pDataTab!=NULL && process > 0)
		{
			for (int wrn = 0;wrn<m_nNumWriteCol;wrn++)
		   {
			   int nCol = m_nWriteCol[wrn];
			   m_pDataTab->ExtendColVarData(nCol,m_nTabSartRow+m_nFetchWriteRow,process, (char**)&bindedptr[m_nBindID[wrn]]);
         int pos = nCol + 1;
			   if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           (dvoid *) bindedptr[m_nBindID[wrn]], bindedlen[m_nBindID[wrn]],SQLT_STR,
           (dvoid *) bindedind[pos], (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
	         {
		              checkerr(retval);
		              return false;
	         } 
			}
		}
		////////////////////////////
		//retval=OCIStmtExecute(((WOCISession *)psess)->svchp,sthp,errhp,process,offset,
		//(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL,OCI_BATCH_ERRORS );// OCI_DEFAULT);
		retval=OCIStmtExecute(((WOCISession *)psess)->svchp,sthp,errhp,process,offset,
		(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
		if(retval==OCI_STILL_EXECUTING)
                {     mSleep(10);continue; }
//#ifdef __unix
///*pthread_delay_np (&interval);*/
//                        usleep(10);		
//#else
//			Sleep(10);
//#endif
			//恢复LONG64;
		for( i=0;i<bindednum;i++) {
		    switch(bindedtype[i]) {
			case COLUMN_TYPE_BIGINT:
			{
  		 	double *pd=(double *)bindedptr[i];
  		 	LONG64 *pl=(LONG64 *)bindedptr[i];
  		 	int lct=0;
  		 	while(lct++<process) *pl++=(LONG64)*pd++;
  			}
  		    }
  		}
		if(retval==OCI_ERROR) { 
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
            m_nFetchWriteRow = m_nFetchWriteRow + process;
			if(processed>=times || process == 0)//解决查询不到死循环问
			 break;
			for( i=0;i<bindednum;i++) {
				switch(bindedtype[i]) {
				case COLUMN_TYPE_BIGINT:
				case COLUMN_TYPE_FLOAT:
					BindByPos(i+1,((double *)bindedptr[i])+process);
					break;
				case COLUMN_TYPE_INT:
					BindByPos(i+1,((int *)bindedptr[i])+process);
					break;
				case COLUMN_TYPE_CHAR:
					if(!IsVarCharCol(i)|| m_pDataTab==NULL){
					   BindByPos(i+1,((char *)bindedptr[i])+process*bindedlen[i],bindedlen[i]);
					}
					break;
				case COLUMN_TYPE_DATE:
					BindByPos(i+1,((char *)bindedptr[i])+process*7);
					break;
				default :
					errprintf("\nWOCIStatement::Execute 中遇到不可识别的字段类型！\n");
					throw -2;
				}
			}
			process=times-processed;
			if(process>EXECUTE_TIMES) process=EXECUTE_TIMES;
			if(m_pDataTab !=NULL)
	        {
	            process = m_pDataTab->GetVarMaxFetchRow(process);    
	        }
			for( i=0;i<bindednum;i++) {
				switch(bindedtype[i]) {
				case COLUMN_TYPE_BIGINT:
				{
  			 	double *pd=(double *)bindedptr[i];
  			 	LONG64 *pl=(LONG64 *)bindedptr[i];
  			 	int lct=0;
  			 	while(lct++<process) *pd++=(double)*pl++;
  				}
			}
			}
			SetNullDateBind(process);
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
		if(echo) lgprintf(" %d rows affected.\n",rowct);
	}
	else if(echo) lgprintf("Done.\n");
	executing=false;
	executed=true;
#endif
	return retval;
}

sword WOCIStatement::BreakAndReset() {
#ifndef NO_ORACLE
	try {
	if(retval=OCIBreak(((WOCISession *)psess)->svchp,errhp))
		throw retval;
#ifdef SQLT_TIME
#ifndef __unix
	if(retval=OCIReset(((WOCISession *)psess)->svchp,errhp))
		throw retval;
#endif
#endif
	}
	catch (int &e) {
		checkerr(e);
	}
	if(autoThrow) 
		Throw();
	
#endif
	return retval;
}

bool WOCIStatement::DescribeStmt()
{
#ifndef NO_ORACLE
	try {
	OCIParam *colhd;     /* column handle */
	
	//(void) OCIHandleAlloc((dvoid *) envhp, (dvoid **)&colhd,
    //                    (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);
	if(retval=OCIAttrGet(sthp,OCI_HTYPE_STMT,&stmttype,
		0,OCI_ATTR_STMT_TYPE,errhp)) throw retval;
	if(stmttype!=OCI_STMT_SELECT) throw 0;
	while((retval=OCIStmtExecute(((WOCISession *)psess)->svchp,sthp,errhp,1,0,
		(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DESCRIBE_ONLY))==OCI_STILL_EXECUTING && !GetSession()->IsTerminate()){
              mSleep(10);
//#ifdef __unix
///*pthread_delay_np (&interval);*/
//                        usleep(10);
//
//#else
//		Sleep(10);
//#endif
	}
	
	if(retval)
		throw retval;
/* Get the number of columns in the query */
    if(retval=OCIAttrGet(sthp, OCI_HTYPE_STMT, &colct, 
                      0, OCI_ATTR_PARAM_COUNT, errhp))
					  throw retval;

	for (ub4 i = 1; i <= colct; i++)
	{
		/* get parameter for column i */
		if(retval=OCIParamGet(sthp, OCI_HTYPE_STMT, errhp, (dvoid **)&colhd, i)) 
			throw retval;
		/* get data-type of column i */
		if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].type, 0, OCI_ATTR_DATA_TYPE, errhp))
					throw retval;
		if(coldsc[i-1].type==SQLT_NUM) {
		    sb2 prec;
			OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &prec, 0, OCI_ATTR_PRECISION, errhp);
                    coldsc[i-1].prec=prec;
                    sb1 scale;
			OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &scale, 0, OCI_ATTR_SCALE, errhp);
                    coldsc[i-1].scale=scale;
			if(coldsc[i-1].prec==0 && (coldsc[i-1].scale==0 || scale==(sb1)0x81) ) {
				coldsc[i-1].prec=DEFAULT_NUMBER_PREC+5;
			    coldsc[i-1].scale=DEFAULT_NUMBER_SCALE+5;
			    coldsc[i-1].type=SQLT_FLT;
			}
			if(coldsc[i-1].prec>DEFAULT_NUMBER_PREC) coldsc[i-1].prec=DEFAULT_NUMBER_PREC+5;
			if(((int)scale)<0) {
				 coldsc[i-1].scale=DEFAULT_NUMBER_SCALE+5;
				 coldsc[i-1].type=SQLT_FLT;
			}
		}
		//JIRA DM-112
		else if(coldsc[i-1].type==SQLT_TIMESTAMP) {
			coldsc[i-1].type=SQLT_DAT;coldsc[i-1].dtsize=7;
		}else if(coldsc[i-1].type==SQLT_RID || coldsc[i-1].type==SQLT_RDD) {
			coldsc[i-1].type=SQLT_CHR;coldsc[i-1].dtsize=20;
		}
		else if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &coldsc[i-1].dtsize, 0, OCI_ATTR_DATA_SIZE, errhp))
					throw retval;
		char *str;
		ub4 nml;
		ub2 charset;
		if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                    &str, &nml, OCI_ATTR_NAME, errhp))
					throw retval;
		memcpy(coldsc[i-1].colname,str,nml);
		coldsc[i-1].colname[nml]=0;
		//if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                //    &charset, 0, OCI_ATTR_CHARSET_ID, errhp))
		//			throw retval;
		//printf("col:%s,charset:%d.\n",str,charset);
		//if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
                //    &coldsc[i-1].dtsize, 0, OCI_ATTR_DATA_SIZE, errhp))
		//			throw retval;
		//if(retval=OCIAttrGet(colhd, OCI_DTYPE_PARAM,
        //            &coldsc[i-1].dspsize, 0, OCI_ATTR_DISP_SIZE, errhp))
		//			throw retval;
		coldsc[i-1].dspsize=nml+1;
		coldsc[i-1].dispname[0]=0;

        //>> begin: fix dma-633
        if(coldsc[i-1].dspsize <= 1)
        {
           strcpy(filen," ");
           lineid=-1;
           sprintf(errbuf,"\n Column[%d] is string type,Error:lengtn is 0,please check table structure.\nSQL:\n%s\n",i,sqlstmt);
		   errcode=-1;
           Throw();	
        }              	
        //<< end: fix dma-633
	}
	
	}
	catch (int &e) {
		checkerr(e);
		if(e!=0)
			return false;
	}
#endif
	return true;
}




//>> Begin , get column info ,after Prepare(const char *_sqlstmt)
int  WOCIStatement::GetColumnNum()
{
    return colct;
}
bool WOCIStatement::GetColumnName(int col,char* colname)
{
    if(col < colct && col >=0){
		strcpy(colname,coldsc[col].colname);
        return true;
    }
	else
		return false;
}
int  WOCIStatement::GetColumnType(int col)
{
    if(col < colct && col >=0)
		return coldsc[col].type;
	else
		return -1;
}
int  WOCIStatement::GetColumnSize(int col)
{
    if(col < colct && col >=0)
		return coldsc[col].dtsize;
    else
		return -1;
}
bool  WOCIStatement::SetColumnSize(int col,int size)
{
    if(col < colct && col >=0){
		coldsc[col].dtsize = size;
        return true;
    }
	else
		return false;
}

//<< End, get column info ,after Prepare(const char *_sqlstmt)


void WOCIStatement::checkerr(sword status) {
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


void WOCIStatement::checkstmterr(sword status)
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
				case COLUMN_TYPE_BIGINT:
					sprintf(pf+strlen(pf),"%lld,",((LONG64 *)bindedptr[j])[row_off]);
					break;
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
					errprintf("\nWOCIStatement::Execute 中遇到不可识别的字段类型！\n");
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

int WOCIStatement::NewStmt()
{
#ifndef NO_ORACLE
	if(sthp) OCIHandleFree(sthp,OCI_HTYPE_STMT);
	if(retval=OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &sthp,
			OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0)) {
		 checkerr(retval);
         sthp=NULL;
	}
#endif
	ClearNullBind();
	return retval;
}


int WOCIStatement::TestTable(const char *tbname)
{
#ifndef NO_ORACLE
  sword     retval;
  OCIDescribe  *dschp = (OCIDescribe *)0;

  OCIParam *parmp; 
//  ub4       parmcnt;
  ub2       numcols;

  if((retval=OCIHandleAlloc((dvoid *) envhp, (dvoid **) &dschp,
                           (ub4) OCI_HTYPE_DESCRIBE,
                           (size_t) 0, (dvoid **) 0)))
						   return -1;

  if ((retval = OCIDescribeAny(((WOCISession *)psess)->svchp, errhp, (dvoid *)tbname,
                               (ub4) strlen((char *) tbname),
                               OCI_OTYPE_NAME, (ub1)1,
                               OCI_PTYPE_TABLE, dschp)) != OCI_SUCCESS)
  {
	retval=0;
  }
  else
  {
                                           /* get the parameter descriptor */
    retval=OCIAttrGet((dvoid *)dschp, (ub4)OCI_HTYPE_DESCRIBE,
                         (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
                         (OCIError *)errhp);
	checkerr(retval);
                                        /* Get the attributes of the table */
                                               /* column list of the table */
                                                      /* number of columns */
    retval=OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &numcols, (ub4 *) 0,
                         (ub4) OCI_ATTR_NUM_COLS, (OCIError *)errhp);
                                               /* now describe each column */
	checkerr(retval);
    retval=numcols;
  }
  OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);
#endif
  return retval;	
}

/*** 2005/06/17 17:07 增加日期类型BIND时的空值指示 *********/
void WOCIStatement::SetNullDateBind(int rnum) {
	rnum=rnum<1?1:rnum;
	for(int i=0;i<bindednum;i++) {
		if(bindedtype[i]==COLUMN_TYPE_DATE) {
		   if(pind[i]) delete [] pind[i];
		   pind[i]=new sb2[rnum];
		   memset(pind[i],0,sizeof(sb2)*rnum);
		   char *pdt=(char *)(bindedptr[i]);
		   for(int j=0;j<rnum;j++) {
		   	if(pdt[j*7]==0) pind[i][j]=-1;
		   }
		   int pos=bindedpos[i];
		   if(retval=OCIBindByPos(sthp, &(bdp[pos]), errhp, pos,
           		(dvoid *) pdt, 7,SQLT_DAT,
           		(dvoid *) pind[i], (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT))
		   {
			checkerr(retval);
			return ;
		   }
		}
	}
}

void WOCIStatement::PrepareDefine(int pos)
{
	if(!pind[pos]) pind[pos]=new sb2[fetchsize];
	memset(pind[pos],0,sizeof(sb2)*fetchsize);
/*	
	if(!plen[pos]) plen[pos]=new sb2[fetchsize];
	if(!pret[pos]) pret[pos]=new sb2[fetchsize];
	memset(plen[pos],0,sizeof(sb2)*fetchsize);
	memset(pret[pos],0,sizeof(sb2)*fetchsize);
*/	
}


int WOCISession::GetColumnInfo(Column_Desc *cd,int _colct,char *crttab)
{
	char fmt[200];
	strcpy(crttab,"(");
	for(unsigned int col=0;col<_colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			sprintf(fmt,"%s varchar2(%d) ",cd[col].colname,cd[col].dtsize-1);
			strcat(crttab,fmt);
			break;
		case SQLT_NUM:
			sprintf(fmt,"%s number(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			strcat(crttab,fmt);
			break;
		case SQLT_FLT:
			sprintf(fmt,"%s float ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_INT:
		case SQLT_LNG:
			if(cd[col].prec>0) {
					sprintf(fmt,"%s number(%d) ",cd[col].colname,cd[col].prec);
			}
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_DAT:
			sprintf(fmt,"%s date ",cd[col].colname);
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

#endif

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

