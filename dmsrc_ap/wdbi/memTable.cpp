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

bool MemTable::SetGroupSrc(DataTable *src) {
	if(dtSrc==NULL)
	 keycolsrcct=0;
	if(src==NULL)
		ReturnErr(DTG_ERR_PARMSRCISNULL,0,"MemTable::SetGroupSrc");
	dtSrc=src;
	return true;
}

bool MemTable::SetGroupRef(DataTable *ref,unsigned int skey) {
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF,0,"MemTable::SetGroupRef");
	if(ref==NULL)
		ReturnErr(DTG_ERR_PARMREFISNULL,0,"MemTable::SetGroupRef");
	if(ref->GetColumnNum()<1) 
		ReturnErr(DTG_ERR_NOCOLUMNSINREF,0,"MemTable::SetGroupRef");
	if(!ref->IsPKSet()) ReturnErr(DTG_ERR_MUSTSETREFPK,0,"MemTable::SetGroupRef");
	if(dtSrc->GetColumnType(skey)!=SQLT_INT) 
		ReturnErr(DTG_ERR_PKMUSTINT_INSRCDT,0,"MemTable::SetGroupRef");
	dtRef=ref;
	srckey=skey;
	return true;
}

bool MemTable::SetGroupColSrc(int *colarray)//-1 means end
{
	int i=0;
  if(m_pColBuff[0][0]) 
		ReturnErr(DTG_ERR_SETONBUILDEDDTP,0,"MemTable::SetGroupColSrc");
	if(colct>0 || rowct>0)
		ReturnErr(DTG_ERR_CLEARBEFOREUSING,0,"MemTable::SetGroupColSrc");
	while(*colarray!=-1) {
		if(*colarray>dtSrc->GetColumnNum()) return false;
		grpbyColSrc[i++]=*colarray;
		char cn[COLNAME_LEN];
		dtSrc->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,dtSrc->GetColumnType(*colarray),dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL,0,"MemTable::SetGroupColSrc");
	}
	return true;
}

bool MemTable::SetGroupColRef(int *colarray)//-1 means end
{
	int i=0;
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF,0,"MemTable::SetGroupColRef");
	if(!dtRef) ReturnErr(DTG_ERR_INVALIDREFDT,0,"MemTable::SetGroupColRef");
	while(*colarray!=-1) {
		if(*colarray>dtRef->GetColumnNum()) ReturnErr(DTG_ERR_OUTOFREFCOL,0,"MemTable::SetGroupColRef");
		grpbyColRef[i++]=*colarray;
		char cn[COLNAME_LEN];
		dtRef->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,dtRef->GetColumnType(*colarray),dtRef->GetColumnLen(*colarray),
			dtRef->GetColumnScale(*colarray));
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL,0,"MemTable::SetGroupColRef");
	}
	return true;
}


bool MemTable::SetCalCol(int *colarray)//only apply to src,-1 means end
{
	int i=0;
	char cn[COLNAME_LEN];
	if(!dtSrc) ReturnErr(DTG_ERR_INVALIDSRCDT,0,"MemTable::SetCalCol");
	while(*colarray!=-1) {
		if(*colarray>dtSrc->GetColumnNum()) ReturnErr(DTG_ERR_OUTOFSRCCOL,0,"MemTable::SetCalCol");
		calCol[i++]=*colarray;
		unsigned short tp=dtSrc->GetColumnType(*colarray);
		if(tp!=SQLT_NUM && tp!=SQLT_FLT && tp!=SQLT_INT && tp!=SQLT_LNG)
			ReturnErr(DTG_ERR_NEEDNUMERICCOLTYPE,0,"MemTable::SetCalCol");
		dtSrc->GetColumnName(*colarray,cn);
		AddColumn(cn,cn,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		/*
		char cn2[COLNAME_LEN];
		strcpy(cn2,cn);
		strcat(cn,"_min");
		AddColumn(cn2,cn2,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		strcpy(cn2,cn);
		strcat(cn,"_max");
		AddColumn(cn2,cn2,tp,
			dtSrc->GetColumnLen(*colarray),
			dtSrc->GetColumnScale(*colarray));
		*/
		colarray++;
		if(i>MAX_GROUPCOL) ReturnErr(DTG_ERR_OUTOFMAXDTGCOL,0,"MemTable::SetCalCol");
	}
	strcpy(cn,"rows_count");
	AddColumn(cn,cn,SQLT_LNG);
	return true;
}

bool MemTable::Group(int _rowstart,int _rownum) {
        long rowstart=_rowstart,rownum=_rownum;
	if(!dtSrc) ReturnErr(DTG_ERR_INVALIDSRCDT,rownum,"MemTable::Group");
	char *bfsrc[MAX_COLUMN];//Full reference;
	char *bfref[MAX_COLUMN];// Full reference;
	if(rownum==0) rownum=dtSrc->GetRows();
	unsigned short srctp[MAX_COLUMN],reftp[MAX_COLUMN]; //Column type of both src and ref group column
	int sct=0,rct=0; // Column number of both src and ref group ones.
	char *gpsrc[MAX_COLUMN];//Group reference;
	char *gpref[MAX_COLUMN];//Group reference;
	// Set column reference point ,group number,and group coumn type.
	if(dtRef) {
		dtRef->RefAllCol(rowstart,bfref);
		//if(!dtRef->IsPKSet()) ReturnErr(DTG_ERR_MUSTSETREFPK);
	}
	else if(grpbyColRef[0]!=-1)
		ReturnErr(DTG_ERR_INVALIDREFDT,rownum,"MemTable::Group");
	if(rowstart<0) ReturnErr(DTG_ERR_INVALIDSTARTROW,rownum,"MemTable::Group");
	if(rownum<0) ReturnErr(DTG_ERR_INVALIDROWNUM,rownum,"MemTable::Group");
	if(rowstart+rownum>dtSrc->GetRows())
		ReturnErr(DTG_ERR_OUTOFSRCROWS,rowstart+rownum,"MemTable::Group");
	dtSrc->RefAllCol(rowstart,bfsrc);
	while(grpbyColSrc[sct]!=-1) {
		srctp[sct]=dtSrc->GetColumnType(grpbyColSrc[sct]);
		gpsrc[sct]=bfsrc[grpbyColSrc[sct]];
		sct++;
	}
	while(grpbyColRef[rct]!=-1) {
		reftp[rct]=dtRef->GetColumnType(grpbyColRef[rct]);
		gpref[rct]=bfref[grpbyColRef[rct]];
		rct++;
	}
	// Begin group for every rows at source datatable
	void *ptr[MAX_COLUMN];
	int src;
	for(long strid=0;strid<rownum;strid++) {
		long rid=strid;//dtSrc->GetRawrnBySort(strid);
		long rffd=0; // Search result from Reference datatable, row number base 0;
		if(dtRef) {
			if(srckey!=-1)
			 rffd=dtRef->SearchPK(*((int *)(bfsrc[srckey]+sizeof(int)*rid)));
			else {
				for(src=0;src<keycolsrcct;src++) {
					ptr[src]=bfsrc[keycolsrc[src]]+rid*dtSrc->GetColumnLen(keycolsrc[src]);
				}
				ptr[src]=NULL;
				rffd=dtRef->Search(ptr);
			}
			if((int)rffd==-1)
				continue;
		}
		int i;
		for( i=0;i<sct;i++) 
			ptr[i]=gpsrc[i]+cd[i].dtsize*(long)rid;
		for(;i<sct+rct;i++)
			ptr[i]=gpref[i-sct]+cd[i].dtsize*(long)rffd;
		ptr[i]=NULL;
		//Search exists rows.
		// this is sorted version
		
		i=Search(ptr);
		if(i==-1) i=rowct;
		
		// Compare exists rows in this group datatable.
		//Replaced by Sorted version
		/*
		for( i=0;i<rowct;i++) {
			for(int j=0;j<sct;j++) { // Compare at source 
				int moff=i*cd[j].dtsize; // Offset of this group datatable
				int soff=rid*cd[j].dtsize; // Offset at source datatable
				if(srctp[j]==SQLT_INT) {
					if(*((int *)(cd[j].pCol+moff))!=*((int *)(gpsrc[j]+soff))) break;
				}
				else {
					if(stricmp(cd[j].pCol+moff,gpsrc[j]+soff)!=0) break;
				}
			}
			if(j!=sct) continue; // Not match at this group row .
			for(int k=0;k<rct;k++) { //Compare every group column at reference
				int moff=i*cd[j+k].dtsize;// Offset of this group datatable
				int soff=rffd*cd[j+k].dtsize;// Offset of referenced key row ;
				if(reftp[k]==SQLT_INT) {
					if(*((int *)(cd[j+k].pCol+moff))!=*((int *)(gpref[k]+soff))) break;
				}
				else {
					if(stricmp(cd[j+k].pCol+moff,gpref[k]+soff)!=0) break;
				}
			}
			if((j+k)==(sct+rct)) break; //when ==sct , match at this i row.
		}
		*/
		if(i==rowct) //not found matched item,add one.
		{
			if(rowct>=maxrows) ReturnErr(DTG_ERR_OUTOFDTGMAXROWS,rowct,"MemTable::Group");
			// sum columns group by (both src and ref);
			int rof=sct+rct;
			int k;
			for( k=0;calCol[k]!=-1;k++) {
			 int r=rof+k;//*3;
			 ptr[r]=bfsrc[calCol[k]]+(long)rid*cd[rof+k].dtsize;
			 ptr[r+1]=ptr[r];
			 ptr[r+2]=ptr[r];
			}
			LONG64 tmp=1;
			ptr[rof+k/**3*/]=&tmp;
			InsertRows(ptr,NULL,1);
			/*Replace by sorted version
			for(int j=0;j<sct;j++) { // Add source dt group column
				int dtsize=cd[j].dtsize;
				int moff=i*dtsize;
				int soff=rid*dtsize;
				memcpy(cd[j].pCol+moff,gpsrc[j]+soff,dtsize);
			}
 			for(int l=0;l<rct;l++) { //add reference dt group column
				int dtsize=cd[j+l].dtsize;
				int moff=i*dtsize;// Offset of this group datatable
				int soff=rffd*dtsize;// Offset of referenced key row ;
				memcpy(cd[j+l].pCol+moff,gpref[l]+soff,dtsize);
			}
			j=sct+rct;
			for(int k=0;calCol[k]!=-1;k++) { // Add calculate column.
				int r=j+k;
				int moff=i*cd[r].dtsize;
				int soff=rid*cd[r].dtsize;
				if(cd[r].type==SQLT_INT) {
					*((int *)(cd[r].pCol+moff))=*((int *)(bfsrc[calCol[k]]+soff));
				}
				else {
					*((double *)(cd[r].pCol+moff))=*((double *)(bfsrc[calCol[k]]+soff));
				}
			}
			
			int moff=i*cd[j+k].dtsize; // Counter column;
			*((int *)(cd[j+k].pCol+moff))=1; // Counter=1;
			rowct++;
			*/
			
		}
		else 
		{
			int j=sct+rct;
			int k;
			for( k=0;calCol[k]!=-1;k++) {
				int r=j+k;//*3;
				long moff=(long)i*cd[r].dtsize;
				long soff=(long)rid*cd[r].dtsize;
				if(cd[r].type==SQLT_LNG) {
					LONG64 v=*((LONG64 *)(bfsrc[calCol[k]]+soff));
					*((LONG64 *)(cd[r].pCol+moff))+=v;
					/*
					if(v<*((int *)(cd[r+1].pCol+moff)) )
						*((int *)(cd[r+1].pCol+moff))=v;
					if(v>*((int *)(cd[r+2].pCol+moff)) )
						*((int *)(cd[r+2].pCol+moff))=v;
						*/
				}
				else if(cd[r].type==SQLT_INT) {
					int v=*((int *)(bfsrc[calCol[k]]+soff));
					*((int *)(cd[r].pCol+moff))+=v;
					/*
					if(v<*((int *)(cd[r+1].pCol+moff)) )
						*((int *)(cd[r+1].pCol+moff))=v;
					if(v>*((int *)(cd[r+2].pCol+moff)) )
						*((int *)(cd[r+2].pCol+moff))=v;
						*/
				}
				else {
					//*((double *)(cd[r].pCol+moff))+=*((double *)(bfsrc[calCol[k]]+soff));
					double v=*((double *)(bfsrc[calCol[k]]+soff));
					*((double *)(cd[r].pCol+moff))+=v;
					/*
					if(v<*((double *)(cd[r+1].pCol+moff)) )
						*((double *)(cd[r+1].pCol+moff))=v;
					if(v>*((double *)(cd[r+2].pCol+moff)) )
						*((double *)(cd[r+2].pCol+moff))=v;
						*/
				}
			}
			long moff=(long)i*cd[j+k].dtsize; // Counter column;
			(*((LONG64 *)(cd[j+k].pCol+moff)))++; // Counter +1;
		}
	}
	return true;			
}



bool MemTable::Build(ub4 mxr,bool noadj,bool importmode)
{
	if(!DataTable::Build(mxr,noadj,importmode)) return false;
	if(dtSrc==NULL) return true;
	int sct=0,rct=0; // Column number of both src and ref group ones.
	while(grpbyColSrc[sct]!=-1) {
		sct++;
	}
	while(grpbyColRef[rct]!=-1) {
		rct++;
	}
	int ptr[MAX_COLUMN];
	int i;
	for( i=0;i<sct+rct;i++) 
		ptr[i]=i;
	ptr[i]=-1;
	//if(
		SetSortColumn(ptr) ;//) return false;
	return Sort();
	
	//return true;
}


bool MemTable::SetCalCol(const char *colsnm)
{
	int gpr[100];
	gpr[0]=-1;
	if(colsnm!=NULL && *colsnm!=0 )
		dtSrc->ConvertColStrToInt(colsnm,gpr);
	return SetCalCol(gpr);

}

bool MemTable::SetGroupColRef(const char *colsnm)
{
	int gpr[100];
	if(!dtRef->ConvertColStrToInt(colsnm,gpr)) return false;
	return SetGroupColRef(gpr);
}

bool MemTable::SetGroupColSrc(const char *colsnm)
{
	int gpr[100];
	if(!dtSrc->ConvertColStrToInt(colsnm,gpr)) return false;
	return SetGroupColSrc(gpr);
}

//21.112.1.52
bool MemTable::SetSortedGroupRef(DataTable *ref, const char *colssrc)
{
	if(!dtSrc) ReturnErr(DTG_ERR_SETSRCBEFOREREF,0,colssrc);
	if(ref==NULL)
		ReturnErr(DTG_ERR_PARMREFISNULL,0,colssrc)
	if(ref->GetColumnNum()<1) 
		ReturnErr(DTG_ERR_NOCOLUMNSINREF,0,colssrc);
	if(!dtSrc->ConvertColStrToInt(colssrc,keycolsrc)) return false;
	while(keycolsrc[keycolsrcct]!=-1) {
		if(ref->sortcolumn[keycolsrcct]==-1) ReturnErr(DTG_ERR_SORTEDCOLNOTFOUND,0,colssrc);
		if(ref->GetColumnType(ref->sortcolumn[keycolsrcct])!=
				dtSrc->GetColumnType(keycolsrc[keycolsrcct]))
				ReturnErr(DTG_ERR_NOTMATCHSORTEDCOLTYPE,0,colssrc);

		keycolsrcct++;
	}
	if(keycolsrcct!=ref->nSortColumn) ReturnErr(DTG_ERR_SOMESORTEDCOLNOTUSED,0,colssrc);
	dtRef=ref;
	return true;
}

bool MemTable::SetGroupRef(DataTable *ref, const char *colnm)
{
	int gpr[100];
	if(!dtSrc->ConvertColStrToInt(colnm,gpr)) return false;
	return SetGroupRef(ref,gpr[0]);
}
