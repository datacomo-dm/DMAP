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

	DataTableLink::DataTableLink(bool athr) {
		pHome=NULL;
		autoThrow=athr;
	}
	
	DataTableLink::~DataTableLink() {
		DataTable *pT=pHome;
		while(pT) {
			DataTable *pT2=pT->GetNextTable();
			delete pT;
			pT=pT2;
		}
	}
	
	DataTable *DataTableLink::FirstTab() {return pHome;}
	
	DataTable *DataTableLink::LastTab() {
		DataTable *pT=pHome;
		if(!pT) ReturnNullErr(DTL_ERR_EMPTYLINK,0,NULL);
		while(pT->GetNextTable()) pT=pT->GetNextTable();
		return pT;
	}
	
	DataTable *DataTableLink::AddTable(const char *name,bool atthr) {
		DataTable *pdt;
		if(ExistTable(name)) ReturnNullErr(DTL_ERR_ALREADYEXIST,0,NULL);
		pdt=new DataTable(atthr);
		pdt->SetName(name);
		DataTable *pT=pHome==NULL?NULL:LastTab();
		if(!pT) 
			pHome=pdt;
		else 
			pT->SetNextTable(pdt);
		return pdt;
	}
	
	DataTable *DataTableLink::FindTable(const char *name) {
		DataTable *pdt=pHome;
		while(pdt) {
			if(pdt->MatchName(name)) return pdt;
			pdt=pdt->GetNextTable();
		}
		ReturnNullErr(DTL_ERR_DTNOTFOUND,0,NULL);
	}
	bool DataTableLink::DeleteTable(const char *name) {
		if(!pHome) 
			ReturnErr(DTL_ERR_EMPTYLINK,0,NULL);
		if(pHome->MatchName(name)) {
			DataTable *pt=pHome->GetNextTable();
			delete pHome;
			pHome=pt;
			return true;
		}
		DataTable *pdt=pHome;
		while(pdt->GetNextTable()) {
			if(pdt->GetNextTable()->MatchName(name)) {
				pdt->SetNextTable(pdt->GetNextTable()->GetNextTable());
				delete pdt->GetNextTable();
				return true;
			}
			pdt=pdt->GetNextTable();
		}
		ReturnErr(DTL_ERR_DTNOTFOUND,0,NULL);
	}

void DataTableLink::ErrorCheck(const char *fn,int ln,int ercd,int parami,const char *params)
{
	SetErrPos(ln,fn);
		retval=errcode=ercd;
		sprintf(errbuf,"Datatable Linker Exception : %s .",DTL_ERR_MSG[ercd]);
		//strcpy(errbuf,DT_ERR_MSG[ercd]);
	if(autoThrow) Throw();
}

bool DataTableLink::ExistTable(const char *nm)
{
		DataTable *pdt=pHome;
		while(pdt) {
			if(pdt->MatchName(nm)) return true;
			pdt=pdt->GetNextTable();
		}
		return false;
}
