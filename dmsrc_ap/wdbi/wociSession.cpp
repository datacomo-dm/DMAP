#include "wdbi.h"
 
#ifdef __unix
extern struct timespec interval;
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

 
extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;

#ifndef NO_OCI
extern OCIEnv *envhp;
extern OCIError *errhp;


bool WOCISession::SetNonBlockMode()  {
#ifndef NO_ORACLE
	if(retval=OCIAttrSet ((dvoid *) svrhp, (ub4) OCI_HTYPE_SERVER, 
                           (dvoid *) 0, (ub4) 0, 
                           (ub4) OCI_ATTR_NONBLOCKING_MODE, errhp)) {
				checkerr(retval);
				return false;
			}
#endif
	return true;
}

bool WOCISession::Connect (const char *username,const char *password,const char *svcname)
	{
#ifndef NO_ORACLE
	try {
			authp = (OCISession *) 0;
  			/* server contexts */
			(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svrhp, OCI_HTYPE_SERVER,
                   (size_t) 0, (dvoid **) 0);

			(void) OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX,
                   (size_t) 0, (dvoid **) 0);

			if(retval=OCIServerAttach( svrhp, errhp, (text *)svcname, strlen((const char *)svcname), 0)) throw retval;

			/* set attribute server context in the service context */
			if(retval=OCIAttrSet( (dvoid *) svchp, OCI_HTYPE_SVCCTX, (dvoid *)svrhp,
                     (ub4) 0, OCI_ATTR_SERVER, (OCIError *) errhp)) throw retval;
			(void) OCIHandleAlloc((dvoid *) envhp, (dvoid **)&authp,
                        (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);

			if(retval=OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
                 (dvoid *) username, (ub4) strlen((char *)username),
                 (ub4) OCI_ATTR_USERNAME, errhp)) throw retval;

			if(retval=OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
                 (dvoid *) password, (ub4) strlen((char *)password),
                 (ub4) OCI_ATTR_PASSWORD, errhp)) throw retval;
			if(retval=OCIAttrSet((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX,
                   (dvoid *) authp, (ub4) 0,
                   (ub4) OCI_ATTR_SESSION, errhp)) throw retval;
			if(retval=OCISessionBegin ( svchp,  errhp, authp, OCI_CRED_RDBMS,
                          (ub4) OCI_DEFAULT)) checkerr(retval);

		}
		catch (sword e) {
			checkerr(e);
			if(e==OCI_SUCCESS_WITH_INFO) return true;
			return false;
		}
//#ifdef WIN32
//    if(!SetNonBlockMode()) return false;
//#endif
#endif
		strcpy(UserName,username);
		strcpy(Password,password);
		strcpy(SvcName,svcname);
		return true;
	}


void WOCISession::Commit()
{
#ifndef NO_ORACLE
	OCITransCommit(svchp, errhp, (ub4) 0);
#endif
}

void WOCISession::Rollback()
{
#ifndef NO_ORACLE
	OCITransRollback(svchp, errhp, (ub4) 0);
#endif
}

int WOCISession::TestTable(const char *tabname)
{
	WOCIStatement st(this);
	return st.TestTable(tabname);
}


WDBIStatement WOCISession::CreateStatement() {
	return (WDBIStatement)WOCIStatement(this,autoThrow);	
}

WDBIStatement *WOCISession::NewStatement() {
	return (WDBIStatement *) new WOCIStatement(this,autoThrow);	
}



void WOCISession::checkerr(sword status) {
  errcode=status;
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
#endif

WDBIStatement WDBISession::CreateStatement() {
ReturnStmtErr(DT_ERR_NOTSUPPORT,0,"CreateStatement");
return WDBIStatement(this);
}

WDBIStatement *WDBISession::NewStatement() { 
	ReturnNullErr(DT_ERR_NOTSUPPORT,0,"NewStatement"); 
}
void WDBISession::ErrorCheck(const char *fn, int ln, int ercd,int parami,const char *params)
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
		sprintf(errbuf,"Exception : %s on Session. User name '%s',Service Name '%s'.\n",DT_ERR_MSG[ercd],UserName,SvcName);
	}
	sprintf(errbuf+strlen(errbuf),"\nCall param1:%d,param2:%s.\n",
 		 parami,(params==NULL||params[0]==0)?"NULL":params);
	if(autoThrow) Throw();
}
