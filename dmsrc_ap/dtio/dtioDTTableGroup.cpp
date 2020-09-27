#include "dtio.h"

dtioDTTableGroup::dtioDTTableGroup(int _dts,int _type,const char *_desc,dtioStream *_dtio,bool psoledidxonly) {
	type=_type;
	strcpy(desc,_desc);
	dtio=_dtio;
	dts=_dts;
	psoledidxmode=psoledidxonly;
	pdtiodttab=NULL;
}

dtioDTTableGroup::~dtioDTTableGroup( ) {
	if(pdtiodttab) delete pdtiodttab;
}

int dtioDTTableGroup::AddTable(const char *dbn,const char *tbn,bool paramonly)
{
	dtioDTTable dtt(dbn,tbn,dtio,psoledidxmode);
	dtt.FetchParam(dts);
	if(paramonly) 
		dtt.SerializeParam();
	else dtt.Serialize();
	return 1;
}

int dtioDTTableGroup::GetTableNum() 
{
	return 1;
}

int dtioDTTableGroup::SearchTable(int rid,char *dbn,char *tbn)
{
	return 1;
}

int dtioDTTableGroup::SearchIDByName(const char *dbn,const char *tbn)
{
	return 1;
}

int dtioDTTableGroup::Restore(int rid,bool fullmode)
{
	return 1;
}

int dtioDTTableGroup::RestoreAll(bool fullmode)
{
	return 1;
}

