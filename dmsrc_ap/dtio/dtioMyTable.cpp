#include "dtio.h"

dtioMyTable::dtioMyTable(dtioStream *_dtio)
{
	dbname[0]=0;
	tablename[0]=0;
	dtio=_dtio;
	tbtype=0;
}

void dtioMyTable::GetDbName(char *dbn) {
	strcpy(dbn,dbname);
}

void dtioMyTable::GetTableName(char *tn) {
	strcpy(tn,tablename);
}

void dtioMyTable::SetTable(const char *dbn,const char *tbn) {
	strcpy(dbname,dbn);strcpy(tablename,tbn);
}

uint8B dtioMyTable::Serialize(int compressflag){
	dtiofile df(dtio,true);
	char fn[PATH_LEN];
	uint8B st,len;
	st=dtio->GetOffset();
	sprintf(fn,"%s%s/%s.frm",dtio->GetOutDir(),dbname,tablename);
	df.Serialize(fn,DTIO_UNITTYPE_MFRMFILE,dbname);
	sprintf(fn,"%s%s/%s.MRG",dtio->GetOutDir(),dbname,tablename);
	FILE *fp=fopen(fn,"rb");
	if(fp) //merged table
	{
	  fclose(fp);
	  df.Serialize(fn,DTIO_UNITTYPE_MMRGFILE,dbname);
	}
	else {
	 sprintf(fn,"%s%s/%s.MYI",dtio->GetOutDir(),dbname,tablename);
	 df.Serialize(fn,DTIO_UNITTYPE_MMYIFILE,dbname);
	 sprintf(fn,"%s%s/%s.MYD",dtio->GetOutDir(),dbname,tablename);
	 df.Serialize(fn,DTIO_UNITTYPE_MMYDFILE,dbname);
	}
	len=dtio->GetOffset()-st;
	dtio->GetContentMt()->SetUnit(tbtype,dbname,tablename,(double)st,(double)len);
	return dtio->GetOffset();
}

uint8B  dtioMyTable::Deserialize(const char *newtbn) {
	dtiofile df(dtio,true);
	char fn[PATH_LEN];
	
	sprintf(fn,"%s.frm",tablename);
	df.DeserializeInit(dbname,fn,DTIO_UNITTYPE_MFRMFILE);
	sprintf(fn,"%s%s/%s.frm",dtio->GetInBaseDir(),dbname,newtbn!=NULL?newtbn:tablename);
	df.Deserialize(fn);
	
	sprintf(fn,"%s.MRG",tablename);
	if(dtio->GetContentMt()->CheckUnit(DTIO_UNITTYPE_MMRGFILE,dbname,fn))
	{
	 df.DeserializeInit(dbname,fn,DTIO_UNITTYPE_MMRGFILE);
	 sprintf(fn,"%s%s/%s.MRG",dtio->GetInBaseDir(),dbname,newtbn!=NULL?newtbn:tablename);
	 df.Deserialize(fn);
	}
	else 
	{
	 sprintf(fn,"%s.MYI",tablename);
	 df.DeserializeInit(dbname,fn,DTIO_UNITTYPE_MMYIFILE);
	 sprintf(fn,"%s%s/%s.MYI",dtio->GetInBaseDir(),dbname,newtbn!=NULL?newtbn:tablename);
	 df.Deserialize(fn);

	 sprintf(fn,"%s.MYD",tablename);
	 df.DeserializeInit(dbname,fn,DTIO_UNITTYPE_MMYDFILE);
	 sprintf(fn,"%s%s/%s.MYD",dtio->GetInBaseDir(),dbname,newtbn!=NULL?newtbn:tablename);
	 df.Deserialize(fn);
	}
	return dtio->GetOffset();
}

void dtioMyTable::SetType(int type)
{
	tbtype=type;
}

int dtioMyTable::GetTableNum()
{
	return dtio->GetContentMt()->GetItemNum(tbtype);
}

void dtioMyTable::SetTableByID(int id)
{
	double off,len;
	dtio->GetContentMt()->GetItem(tbtype,id,dbname,tablename,off,len);
}
