#include "stdafx.h"
#include <winsock2.h>
#ifndef __unix
#define svctxt "cuyn12"
#else
#define svctxt "cuyn12"
#endif
#include <stdio.h>
#include "woci8intface.h"
#include <conio.h>
#define MAXCDI 2
struct ColDescData {
	int id,colnum;
	char detail[200];
};
struct ColDescInfo {
	int cdnum;
	ColDescData cdd[MAXCDI];
};


ColDescInfo cdi={0x2,
{
	{
		0x5,0x3,"\x01\x03\x34\x5e\x12"
				"\x11\x56\x13\x55\x05",
	},
	{
		0x4,0x5,"\x11\x12\x12\x13\xc3\xc5"
				"\x16\x44\x76\xf3\xc4",
	},
}
};

int start(void *p);
int main(int argc, char* argv[])
{
	//u_long i=0x123456;
	//u_long r=htonl(i);
	//return 1;
	wociMainEntrance(start,true,NULL);
	return 0;
} 


#define QS_ITEMNUM 15
#define QS_FILENANE "queryserver.dat"
class QueryServer {
	struct tag_stat {
		int yjct,yjhit;
		int dact,dahit;
		int wjct,wjhit;
	} stat;
	enum {
		DAIDX=0,WJIDX,YJIDX,SSIDX,TCIDX
	};
	struct tag_QS {
		int mtrows;
		int mt;
		bool valid;
		char grpcols[255];
		time_t lsttm,nxttm,rldtm;//lsttm used with update data's SQL parameter
		char sqlstmt[255];
		char chkupdsqlstmt[255];
		char schkupdsqlstmt[255];
		char updsqlstmt[255];
        char pkcol[128];
		char sortcols[128];
		struct {
			int period[2]; //周期间隔(秒),0(<1) 无效 ; 2个时断(7:00-22:00-7:00)
			struct {
				char hour,min;//时(0-23) ,分(0-59) (-1 ||<0 无效)
			}  fix[5];
		} update;//update policy 
	} qs[QS_ITEMNUM];
	bool valid; //data is valid;
	int yydb;
	int yxsfmt;
	// no reader/writer lock implementted
	bool ReadLock();
	bool ReadUnlock();
	bool WriteLock();
	bool WriteUnlock();
	void SetNextTime(int idx) ;
	time_t GetHostTime() ;
	void showtm(int idx) {
			CTime ctm1=CTime(qs[idx].lsttm);
			CTime ctm2=CTime(qs[idx].nxttm);
			CTime ctm3=CTime(qs[idx].rldtm);
			printf("idx:%d lst--%02d:%02d:%02d  nexttm -%02d:%02d:%02d rldtm--%02d:%02d:%02d \n",idx,ctm1.GetHour(),ctm1.GetMinute(),ctm1.GetSecond(),
				ctm2.GetHour(),ctm2.GetMinute(),ctm2.GetSecond(),
				ctm3.GetHour(),ctm3.GetMinute(),ctm3.GetSecond());
	}
	// 0: none 1:update 2:reload
	int CheckTime(int idx)
	{
		if(!qs[idx].valid) return 0;
		time_t now=GetHostTime();
		if(now>qs[idx].rldtm) {
			//showtm(idx);
			return 2;
		}
		if(now>qs[idx].nxttm && qs[idx].chkupdsqlstmt[0]>0) {
			//showtm(idx);
			return 1;
		}
		return 0;
	}

	// the most complex method
	void BatchUpdate(int idx);
	void ReloadData(int idx);
	int ReloadAndCalc(int idx,char *tbn=NULL,char *part=NULL,int ny=0);
	void Reload(int idx);
	int GetSFNY(char *xq, char *jz);
public :
	QueryServer() {};
	~QueryServer() {};
	void Init();
	int SUpdate(char *dhhm,int yhbh,int idc,int idx,bool forceupd);
	int Run(void *p) {
		for(int i=0;i<QS_ITEMNUM;i++) {
			if(CheckTime(i)==2) {
				Reload(i);
				//if(i==0) {
				//}
			}
		}
		//SaveData();
		for(i=0;i<QS_ITEMNUM;i++) {
			if(CheckTime(i)==1) {
				BatchUpdate(i);
				SaveData(i);
			}
		}
		return 1;
	}
	void ShowTmAll() {
		showtm(DAIDX);
		showtm(WJIDX);
		showtm(YJIDX);
		showtm(SSIDX);
		showtm(TCIDX);
	}
	int SaveData(int idx=-1){
		FILE *fp=fopen("e:\\queryserver\\desc.dat","w+b");
		int len=sizeof(qs);
		fwrite(&len,1,sizeof(int),fp);
		fwrite(&qs,1,sizeof(qs),fp);
		fclose(fp);
		char *bf;
		for(int i=0;i<QS_ITEMNUM;i++) {
			if(idx>=0 && i!=idx) continue;
			int cdlen,colnm;
			int mt=qs[i].mt;
			char fn[100];
			if(mt<1) continue;
			if(wociGetMemtableRows(mt)<1) continue;
			sprintf(fn,"e:\\queryserver\\memtable%d.dat",i+1);
			fp=fopen(fn,"w+b");
			wociGetColumnDesc(mt,(void **)&bf,cdlen,colnm);
			fwrite(&cdlen,1,sizeof(int),fp);
			fwrite(&colnm,1,sizeof(int),fp);
			fwrite(bf,1,cdlen,fp);
            int rn=wociGetMemtableRows(mt);
			int rlen=wociGetRowLen(mt);
			char *pbf=new char[rn*rlen];
			lgprintf("写文件%s,行数%d,字节:%d...",fn,rn,rn*rlen);
			wociExportSomeRows(mt,pbf,0,rn);
			fwrite(&rn,sizeof(int),1,fp);
			fwrite(&rlen,sizeof(int),1,fp);
			fwrite(pbf,rn*rlen,1,fp);
			fclose(fp);
			delete []pbf;
		}
		//lgprintf("保存数据成功.\n");
		return 1;
	}
	
	int LoadData()
	{
		lgprintf("读本地数据文件...");
		FILE *fp=fopen("e:\\queryserver\\desc.dat","rb");
		if(fp==NULL) return 0;
		int len=sizeof(qs);
		fread(&len,1,sizeof(int),fp);
		struct tag_QS tmp[QS_ITEMNUM];
		fread(&tmp,1,sizeof(qs),fp);

		fclose(fp);
		for(int i=0;i<QS_ITEMNUM;i++) {
			int cdlen,colnm;
			char fn[100];
			sprintf(fn,"e:\\queryserver\\memtable%d.dat",i+1);
            fp=fopen(fn,"rb");
			if(fp==NULL) {
				qs[i].mt=0;
				continue;
			}
			lgprintf("读%s文件...",fn);
			qs[i].mt=wociCreateMemTable();
			int mt=qs[i].mt;
			fread(&cdlen,1,sizeof(int),fp);
			fread(&colnm,1,sizeof(int),fp);
			char *pcd=new char[cdlen];
			fread(pcd,1,cdlen,fp);
			//wociGetColumnDesc(mt,(void **)&bf,cdlen,colnm);
            int rn=0;//wociGetMemtableRows(mt);
			int rlen=0;//wociGetRowLen(mt);
			fread(&rn,sizeof(int),1,fp);
			fread(&rlen,sizeof(int),1,fp);
			char *pbf=new char[rn*rlen];
			fread(pbf,rn*rlen,1,fp);
			lgprintf("构造内存表...");
			wociImport(mt,pbf,rn*rlen,pcd,colnm,qs[i].mtrows,rn);
			//wociExportSomeRows(mt,pbf,0,rn);
			
			delete []pcd;
			delete []pbf;
			lgprintf("排序...");
			if(qs[i].pkcol[0]!=0) {
				wociSetIKByName(mt,qs[i].pkcol);
				wociOrderByIK(mt);
			}
			if(qs[i].sortcols[0]!=0) {
				wociSetSortColumn(mt,qs[i].sortcols);
				wociSort(mt);
				//if(wociLoadSort(mt,fp)<0) {
				//	lgprintf("Load sort data failed");
				//	wociSort(mt);
				//}
			}
			lgprintf("读入成功.");
			qs[i].lsttm=tmp[i].lsttm;
			qs[i].nxttm=tmp[i].nxttm;
			SetNextTime(i);
			fclose(fp);
		}
		return 1;
	}
	//-1: sys or net error,0:no user,1:suc
	int get_balfee(char *dhhm,double &ret,int idc=0) {
		void *ptr[2];
		ptr[0]=dhhm;
		ptr[1]=NULL;
		stat.dact++;
		int ps=wociSearch(qs[DAIDX].mt,ptr);
		int yhbh=0;
		char jszh[50];
		int xyce=0;
		if(ps<0) {
			stat.dahit++;
			int stmt=wociCreateStatment(idc==0?yydb:idc);
			int mt=wociCreateMemTable();
			int rt=0;
			try {
			wociPrepareStmt(stmt,qs[DAIDX].schkupdsqlstmt);
			wociBuildStmt(mt,stmt,2);
			wociBindStrByPos(stmt,1,dhhm,strlen(dhhm)+1);
			wociFetchAll(mt);
			rt=wociWaitLastReturn(mt);
			}
			catch(...) {
				wocidestroy(stmt);
				wocidestroy(mt);
				return -1;
			}
			wocidestroy(stmt);
			if(rt==1) {
				wociCopyRowsTo(mt,qs[DAIDX].mt,wociGetMemtableRows(qs[DAIDX].mt),0,1);
				yhbh=wociGetIntValByName(mt,"yhbh",0);
				wociGetStrValByName(mt,"jszh",0,jszh);
				xyce=wociGetIntValByName(mt,"xyce",0);
				SUpdate(NULL,yhbh,idc==0?yydb:idc,YJIDX,true);
				SUpdate(NULL,yhbh,idc==0?yydb:idc,WJIDX,true);
				wocidestroy(mt);
			}
			else {
				wocidestroy(mt);return 0;
			}
		}
		else {
			yhbh=wociGetIntValByName(qs[DAIDX].mt,"yhbh",ps);
			wociGetStrValByName(qs[DAIDX].mt,"jszh",ps,jszh);
			SUpdate(NULL,yhbh,idc==0?yydb:idc,YJIDX,false);
			SUpdate(NULL,yhbh,idc==0?yydb:idc,WJIDX,false);
			xyce=wociGetIntValByName(qs[DAIDX].mt,"xyce",ps);
		}
		ptr[0]=jszh;
		double xyd=0,yjk=0,qf=0;
		ps=wociSearch(qs[YJIDX].mt,ptr);
		if(ps>=0) {
			xyd=(double)wociGetIntValByName(qs[YJIDX].mt,"xyd",ps);
			yjk=(double)wociGetIntValByName(qs[YJIDX].mt,"yjk",ps);
		}
		ps=wociSearchIK(qs[WJIDX].mt,yhbh);
		if(ps>=0) 
			qf=wociGetDoubleValByName(qs[WJIDX].mt,"hf",ps);
		ret=xyd+yjk+xyce-qf;
		return 1;
	}
	//-1: sys or net error,0:no user,1:invalid password,2:suc
	int user_cert(char *dhhm,char *mm);
	void ShowStat(void);
};

void QueryServer::Reload(int idx)
{
	if(idx==WJIDX) 
		ReloadAndCalc(idx);
	else if(idx==SSIDX) {
	}
	else ReloadData(idx);
}

int QueryServer::SUpdate(char *dhhm,int yhbh,int idc,int idx,bool forceupd)
{
	if(idx!=YJIDX && idx!=WJIDX) return 0;
	int ps=-1;
	if(yhbh==0) {
		void *ptr[2];
		ptr[0]=(void *)dhhm;
		ptr[1]=NULL;
		ps=wociSearch(qs[DAIDX].mt,ptr);
		if(ps<0) return -1;
		yhbh=wociGetIntValByName(qs[DAIDX].mt,"yhbh",ps);
	}
	else {
		ps=wociSearchIK(qs[DAIDX].mt,yhbh);
		if(ps<0) return -1;
	}
	char xq[10];
	wociGetStrValByName(qs[DAIDX].mt,"xq",ps,xq);
	int stmt=wociCreateStatment(idc);
	int mt=wociCreateMemTable();
	if(idx==YJIDX) stat.yjct++;
	else stat.wjct++;
	try{
	if(!forceupd) {
	wociPrepareStmt(stmt,qs[idx].schkupdsqlstmt);
	char dt[20];
	CTime ctm=CTime(qs[idx].lsttm);
	//回退10分钟
	ctm-=CTimeSpan(0,0,10,0);
	wociSetDateTime(dt,ctm.GetYear(),ctm.GetMonth(),
		ctm.GetDay(),ctm.GetHour(),ctm.GetMinute(),ctm.GetSecond());
	wociBindStrByPos(stmt,1,xq,10);
	wociBindIntByPos(stmt,2,&yhbh);
	wociBindDateByPos(stmt,3,dt);
	wociBuildStmt(mt,stmt,2);
	wociFetchAll(mt);
	int rt=wociWaitLastReturn(mt);
	if(rt!=1) {wocidestroy(stmt);wocidestroy(mt);return 0;}
	}
	wociPrepareStmt(stmt,qs[idx].updsqlstmt);
	if(idx==YJIDX) {
		char jszh[50];
		stat.yjhit++;
		wociGetStrValByName(qs[DAIDX].mt,"jszh",ps,jszh);
		wociBindStrByPos(stmt,1,xq,10);
		wociBindStrByPos(stmt,2,jszh,50);
		wociBuildStmt(mt,stmt,2);
		wociFetchAll(mt);
		int rs=wociWaitLastReturn(mt);
		double *pnxyd,*pnyjk;
		double *pxyd,*pyjk;
		wociGetDoubleAddrByName(qs[idx].mt,"xyd",0,&pxyd);
		wociGetDoubleAddrByName(mt,"xyd",0,&pnxyd);
		wociGetDoubleAddrByName(qs[idx].mt,"yjk",0,&pyjk);
		wociGetDoubleAddrByName(mt,"yjk",0,&pnyjk);
		if(rs!=1) {
			errprintf("预交异动无账号:%d\n",yhbh);
		}
		else {
		void *sc[2];
		sc[1]=NULL;
		sc[0]=(void *)(jszh);
		int ps2=wociSearch(qs[idx].mt,sc);
		if(ps2<0) {
			wociCopyRowsTo(mt,qs[idx].mt,wociGetMemtableRows(qs[idx].mt),0,1);
		}
		else { 
			pxyd[ps2]=pnxyd[0];pyjk[ps2]=pnyjk[0];
		}
		}
	}
	else {
		char jz[10];
		stat.wjhit++;
		wociGetStrValByName(qs[DAIDX].mt,"jz",ps,jz);
		int ny=GetSFNY(xq,jz);
		wociBindStrByPos(stmt,1,xq,10);
		wociBindIntByPos(stmt,2,&yhbh);
		wociBindIntByPos(stmt,3,&ny);
		wociBuildStmt(mt,stmt,2);
		wociFetchAll(mt);
		int rs=wociWaitLastReturn(mt);
		double *pwj,*phf;
		wociGetDoubleAddrByName(qs[idx].mt,"hf",0,&pwj);
		wociGetDoubleAddrByName(mt,"hf",0,&phf);
		int ps2=wociSearchIK(qs[idx].mt,yhbh);
		if(ps2<0 && rs==1) {
			wociCopyRowsTo(mt,qs[idx].mt,wociGetMemtableRows(qs[idx].mt),0,1);
		}
		else if(ps2>=0 && rs==1)
			pwj[ps2]=phf[0];
	}
	}
	catch(...) {
		wocidestroy(stmt);
		wocidestroy(mt);
		throw;
	}
	wocidestroy(stmt);
	wocidestroy(mt);
	return 1;
}

void QueryServer::BatchUpdate(int idx)
{
	int instd=0,updtd=0;
	int stmt=wociCreateStatment(yydb);
	int updstmt=wociCreateStatment(yydb);
	int mt=wociCreateMemTable();
	int updmt=wociCreateMemTable();
	int lsttm=GetHostTime();
	lgprintf("\n\n/*********************更新(%d)数据开始********/",idx);
	try{
	char dt[20];
	CTime ctm=CTime(qs[idx].lsttm);
	//回退10分钟
	ctm-=CTimeSpan(0,0,10,0);
	wociSetDateTime(dt,ctm.GetYear(),ctm.GetMonth(),
		ctm.GetDay(),ctm.GetHour(),ctm.GetMinute(),ctm.GetSecond());
	wociPrepareStmt(stmt,qs[idx].chkupdsqlstmt);
	wociBindDateByPos(stmt,1,dt);
	wociPrepareStmt(updstmt,qs[idx].updsqlstmt);
	wociBuildStmt(updmt,updstmt,2);
	//CTime ctm2=CTime(qs[idx].nxttm);
	//lgprintf("dt--%02d:%02d:%02d  nexttm -%02d:%02d:%02d\n",ctm.GetHour(),ctm.GetMinute(),ctm.GetSecond(),
	//	ctm2.GetHour(),ctm2.GetMinute(),ctm2.GetSecond());
	if(idx==DAIDX) {
		wociBuildStmt(mt,stmt,500000);
		wociFetchAll(mt);
		int rt=wociWaitLastReturn(mt);
		int *pyhbh;
		char *pnzt,*pzt,*pxq;
		int nzt_len,zt_len,xq_len;
		wociSetSortColumn(mt,"xq,rq");
		wociSort(mt);
		wociGetIntAddrByName(mt,"yhbh",0,&pyhbh);
		wociGetStrAddrByName(mt,"nzt",0,&pnzt,&nzt_len);
		wociGetStrAddrByName(mt,"xq",0,&pxq,&xq_len);
		wociGetStrAddrByName(qs[DAIDX].mt,"zt",0,&pzt,&zt_len);
		lgprintf("档案更新,%d用户.",rt);
		for(int i=0;i<rt;i++) {
			int ps=wociGetRawrnBySort(mt,i);
			int ps2=wociSearchIK(qs[DAIDX].mt,pyhbh[ps]);
			if(ps2<0) {
				wociBindIntByPos(updstmt,1,pyhbh+ps);
				wociBindStrByPos(updstmt,2,pxq+ps*xq_len,xq_len);
				wociFetchAll(updmt);
				int rt=wociWaitLastReturn(updmt);
				if(rt==1) {
					wociCopyRowsTo(updmt,qs[idx].mt,wociGetMemtableRows(qs[idx].mt),0,1);
					instd++;
				}
				else {
					errprintf("状态异动无档案:%d\n",pyhbh[ps]);
				}
			}
			else {
				strcpy(pzt+ps2*zt_len,pnzt+ps*nzt_len);
				updtd++;
			}
		}
		lgprintf("档案更新结束,加入%d,修改%d.",instd,updtd);
	}
	else if(idx==YJIDX) {
		wociBuildStmt(mt,stmt,5000);
		wociFetchFirst(mt,5000);
		int rt=wociWaitLastReturn(mt);
		if(rt==5000) {
			lgprintf("预交更新数据大于5000,做全表扫描.");
			Reload(idx);
			return;
		}
        int *pyhbh;
		char *pjszh,*pxq;
		int jszh_len,xq_len;
		double *pnxyd,*pnyjk;
		double *pxyd,*pyjk;
		wociSetSortColumn(mt,"xq,yhbh");
		wociSort(mt);
		wociGetIntAddrByName(mt,"yhbh",0,&pyhbh);
		wociGetStrAddrByName(qs[DAIDX].mt,"xq",0,&pxq,&xq_len);
		wociGetStrAddrByName(qs[DAIDX].mt,"jszh",0,&pjszh,&jszh_len);
		wociGetDoubleAddrByName(qs[idx].mt,"xyd",0,&pxyd);
		wociGetDoubleAddrByName(updmt,"xyd",0,&pnxyd);
		wociGetDoubleAddrByName(qs[idx].mt,"yjk",0,&pyjk);
		wociGetDoubleAddrByName(updmt,"yjk",0,&pnyjk);
		char *pdhhm;
		int dhhm_len;
		wociGetStrAddrByName(qs[DAIDX].mt,"dhhm",0,&pdhhm,&dhhm_len);

		void *sc[2];
		sc[1]=NULL;
		lgprintf("预交更新,%d用户.",rt);
		FILE *fp=fopen("e:\\queryserver\\testdn.dat","w+b");
		fwrite(&dhhm_len,1,sizeof(int),fp);

		for(int i=0;i<rt;i++) {
			int rp=wociGetRawrnBySort(mt,i);
			int ps=wociSearchIK(qs[DAIDX].mt,pyhbh[rp]);
			if(ps<0) {
				errprintf("预交异动无档案:%d\n",pyhbh[rp]);
			}
			else {
				wociBindStrByPos(updstmt,1,pxq+ps*xq_len,xq_len);
				wociBindStrByPos(updstmt,2,pjszh+ps*jszh_len,jszh_len);
				wociFetchAll(updmt);
				int rs=wociWaitLastReturn(updmt);
				if(rs!=1) {
					errprintf("预交异动无账号:%d\n",pyhbh[rp]);
				}
				else {
					sc[0]=(void *)(pjszh+ps*jszh_len);
					int ps2=wociSearch(qs[idx].mt,sc);
					if(ps2<0) {
						wociCopyRowsTo(updmt,qs[idx].mt,wociGetMemtableRows(qs[idx].mt),0,1);
						instd++;
					}
					else { 
						pxyd[ps2]=pnxyd[0];pyjk[ps2]=pnyjk[0];
						updtd++;
						fwrite(pdhhm+ps*dhhm_len,1,dhhm_len,fp);
					}
				}
			}
		}
		fclose(fp);
		lgprintf("预交更新结束,加入%d,修改%d.",instd,updtd);
	}
	else if(idx==WJIDX) {
		wociBuildStmt(mt,stmt,5000);
		wociFetchFirst(mt,5000);
		int rt=wociWaitLastReturn(mt);
		if(rt==5000) {
			lgprintf("欠费更新数据大于5000,做全表扫描.");
			Reload(idx);
			return;
		}
        int *pyhbh;
		char *pjz,*pxq;
		int jz_len,xq_len;
		double *pwj,*phf;
		wociGetIntAddrByName(mt,"yhbh",0,&pyhbh);
		wociSetSortColumn(mt,"xq,yhbh");
		wociSort(mt);
		wociGetStrAddrByName(qs[DAIDX].mt,"xq",0,&pxq,&xq_len);
		wociGetStrAddrByName(qs[DAIDX].mt,"jz",0,&pjz,&jz_len);
		wociGetDoubleAddrByName(qs[idx].mt,"hf",0,&pwj);
		wociGetDoubleAddrByName(updmt,"hf",0,&phf);
		lgprintf("欠费更新,%d用户.",rt);
		/*
		char *prxq=new char[xq_len*rt];
		int *pryhbh=new int[rt];
		int *prny=new int[rt];
		for(int i=0;i<rt;i++) {
			int ps=wociSearchIK(qs[DAIDX].mt,pyhbh[i]);
			if(ps<0) {
				errprintf("欠费异动无档案:%d\n",pyhbh[i]);
			}
			else {
				prny[i]=GetSFNY(pxq+ps*xq_len,pjz+ps*jz_len);
				strcpy(prxq+xq_len*i,pxq+ps*xq_len);
				pryhbh[i]=pyhbh[i];
			}
		}
		wociBindStrByPos(updstmt,1,pxq+ps*xq_len,xq_len);
		wociBindIntByPos(updstmt,2,pyhbh+i);
		wociBindIntByPos(updstmt,3,&ny);
		wociBuildStmt(updmt,updstmt,rt);
		wociExecute(updstmt,rt);
		rt=wociWaitLastReturn(updstmt);
		*/
		for(int i=0;i<rt;i++) {
			int rp=wociGetRawrnBySort(mt,i);
			int ps=wociSearchIK(qs[DAIDX].mt,pyhbh[rp]);
			if(ps<0) {
				errprintf("欠费异动无档案:%d\n",pyhbh[rp]);
			}
			else {
				int ny=GetSFNY(pxq+ps*xq_len,pjz+ps*jz_len);
				wociBindStrByPos(updstmt,1,pxq+ps*xq_len,xq_len);
				wociBindIntByPos(updstmt,2,pyhbh+rp);
				wociBindIntByPos(updstmt,3,&ny);
				wociFetchAll(updmt);
				int rs=wociWaitLastReturn(updmt);
				int ps2=wociSearchIK(qs[idx].mt,pyhbh[rp]);
				if(ps2<0 && rs==1) {
					wociCopyRowsTo(updmt,qs[idx].mt,wociGetMemtableRows(qs[idx].mt),0,1);
					instd++;
				}
				else if(ps2>=0) {
					if(rs==1)
					pwj[ps2]=phf[0];
					else pwj[ps2]=0;
					updtd++;
				}
			}
		}
		lgprintf("欠费更新结束,加入%d,修改%d.",instd,updtd);
	}
 }
 catch(...) {
	 errprintf("更新(%d)数据出现错误\n",idx);	 
	 wocidestroy(stmt);
	 wocidestroy(mt);
	 wocidestroy(updstmt);
	 wocidestroy(updmt);
	 throw;
 }
 lgprintf("/******************更新(%d)数据结束***********/",idx);	 
 qs[idx].lsttm=lsttm;
 SetNextTime(idx);
 wocidestroy(stmt);
 wocidestroy(mt);
 wocidestroy(updstmt);
 wocidestroy(updmt);
}

int QueryServer::GetSFNY(char *xq, char *jz)
{
	void *ptr[3];
	ptr[0]=xq;
	ptr[1]=jz;
	ptr[2]=NULL;
	int idx=wociSearch(yxsfmt,ptr);
	int rt=wociGetIntValByName(yxsfmt,"ny",idx);
	return rt;
}


void QueryServer::ReloadData(int idx) {
	char tbuffer[19];
	_strtime( tbuffer );
	time_t lsttm=GetHostTime();
	lgprintf("数据 %02d 开始扫描-%s.",idx,tbuffer);
	int stmt=wociCreateStatment(yydb);
	wociPrepareStmt(stmt,qs[idx].sqlstmt);
	int mt=wociCreateMemTable();
	try {
	wociBuildStmt(mt,stmt,qs[idx].mtrows);
	wociFetchAll(mt);
	wociWaitLastReturn(mt);
	if(qs[idx].pkcol[0]!=0) {
		wociSetIKByName(mt,qs[idx].pkcol);
	    wociOrderByIK(mt);
	}
	if(qs[idx].sortcols[0]!=0) {
		wociSetSortColumn(mt,qs[idx].sortcols);
		wociSort(mt);
	}
	}
	catch(...) {
		wocidestroy(mt);
		wocidestroy(stmt);
		throw;
	}
	if(qs[idx].mt>0) wocidestroy(qs[idx].mt);
	qs[idx].mt=mt;
	wocidestroy(stmt);
	_strtime(tbuffer);
	lgprintf("数据 %02d 扫描结束-%s.",idx,tbuffer);
	qs[idx].lsttm=lsttm;
	SetNextTime(idx);

}

int QueryServer::ReloadAndCalc(int idx,char *tbn,char *part,int ny) {
	//int idx=WJIDX;
	char tbuffer[19];
	int FETCHN=50000;
	_strtime( tbuffer );
	time_t lsttm=GetHostTime();
	lgprintf("数据 %02d 开始扫描-%s.",idx,tbuffer);
	int stmt=wociCreateStatment(yydb);
	char sql[300];
	if(ny>0)
	sprintf(sql,qs[idx].sqlstmt,tbn,part,ny);
	else strcpy(sql,qs[idx].sqlstmt);
	wociPrepareStmt(stmt,sql);
	int tmt[2];
	int mt;
	tmt[0]=wociCreateMemTable();
	tmt[1]=wociCreateMemTable();
	mt=wociCreateMemTable();
	try {
	wociAddColumn(mt,"yhbh",NULL,COLUMN_TYPE_INT,0,0);
	wociAddColumn(mt,"hf",NULL,COLUMN_TYPE_NUM,10,2);
	wociBuild(mt,qs[idx].mtrows);
	if(qs[idx].pkcol[0]!=0) {
		wociSetIKByName(mt,qs[idx].pkcol);
	    wociOrderByIK(mt);
	}
	wociBuildStmt(tmt[0],stmt,FETCHN);
	wociBuildStmt(tmt[1],stmt,FETCHN);
	wociFetchFirst(tmt[0],FETCHN);
	int *pyhbh,*pny;
	double *phf,*pwj;
	char *pxq,*pjz;
	int yhbh;
	double wjhf;
	void *pptr[3];
	pptr[0]=(void *)&yhbh;
	pptr[1]=(void *)&wjhf;
	pptr[2]=NULL;
	int xq_len,jz_len;
	int damt=qs[DAIDX].mt;
	wociGetStrAddrByName(damt,"xq",0,&pxq,&xq_len);
	wociGetStrAddrByName(damt,"jz",0,&pjz,&jz_len);
	wociGetDoubleAddrByName(mt,"hf",0,&pwj);
	int rt=wociWaitLastReturn(tmt[0]);
	int cur=0;
	while(rt>0) {
		int nxt=cur==0?1:0;
		wociFetchNext(tmt[nxt],FETCHN);
		wociGetIntAddrByName(tmt[cur],"yhbh",0,&pyhbh);
		wociGetDoubleAddrByName(tmt[cur],"hf",0,&phf);
		wociGetIntAddrByName(tmt[cur],"ny",0,&pny);
		for(int i=0;i<rt;i++) {
			yhbh=pyhbh[i];
			int ps=wociSearchIK(damt,yhbh);
			if(ps>=0) {
				int sny=GetSFNY(pxq+ps*xq_len,pjz+ps*jz_len);
				if(idx==WJIDX && pny[i]>sny) continue;
				if(idx!=WJIDX && pny[i]<=sny) continue;
				int ps2=wociSearchIK(mt,yhbh);
				if(ps2<0) {
					wjhf=phf[i];
					wociInsertRows(mt,pptr,NULL,1);
				}
				else 
					pwj[ps2]+=phf[i];
			}
			//else 
			//	lgprintf("欠费无档案:%d\n",yhbh);
		}
		cur=nxt;
		rt=wociWaitLastReturn(tmt[nxt]);
	}
	if(qs[idx].sortcols[0]!=0) {
		wociSetSortColumn(mt,qs[idx].sortcols);
		wociSort(mt);
	}
	}
	catch(...) {
		wocidestroy(mt);
		wocidestroy(tmt[0]);
		wocidestroy(tmt[1]);
		wocidestroy(stmt);
		throw;
	}
	if(idx==WJIDX) {
	 if(qs[idx].mt>0) wocidestroy(qs[idx].mt);
	 qs[idx].mt=mt;
	}
	wocidestroy(tmt[0]);
	wocidestroy(tmt[1]);
	wocidestroy(stmt);
	_strtime(tbuffer);
	lgprintf("数据 %02d 扫描结束-%s.",idx,tbuffer);
	qs[idx].lsttm=lsttm;
	SetNextTime(idx);
	return mt;
}

void QueryServer::Init()
	{
		memset(qs,0,sizeof(qs));
		memset(&stat,0,sizeof(stat));
		valid=false;
		//用户档案资料
		qs[DAIDX].valid=true;
		strcpy(qs[DAIDX].sqlstmt,"select dhhm,yhbh,xq,jz,tch,mm,xyce,jszh,zt,mff,yhlb,qbtf,sx from tyt_yhda");
		strcpy(qs[DAIDX].updsqlstmt,"select dhhm,yhbh,xq,jz,tch,mm,xyce,jszh,zt,mff,yhlb,qbtf,sx from tyt_yhda where yhbh=:yhbh and xq=:xq");
		strcpy(qs[DAIDX].chkupdsqlstmt,"select xq,yhbh,nzt,rq from tyt_ztyd where rq>=:dt1");
		strcpy(qs[DAIDX].schkupdsqlstmt,"select dhhm,yhbh,xq,jz,tch,mm,xyce,jszh,zt,mff,yhlb,qbtf,sx from tyt_yhda where dhhm=:dhhm");
		qs[DAIDX].mtrows=2000000;
		strcpy(qs[DAIDX].pkcol,"yhbh");
		strcpy(qs[DAIDX].sortcols,"dhhm");
		qs[DAIDX].update.fix[0].hour=4;
		qs[DAIDX].update.fix[0].min=0;
		qs[DAIDX].update.fix[1].hour=12;
		qs[DAIDX].update.fix[1].min=-1;
		qs[DAIDX].update.fix[2].hour=19;
		qs[DAIDX].update.fix[2].min=0;
		qs[DAIDX].update.fix[3].min=-1;
		qs[DAIDX].update.period[0]=40*60;// 
		qs[DAIDX].update.period[1]=90*60;// 

		//预交款资料
		qs[YJIDX].valid=true;
		strcpy(qs[YJIDX].sqlstmt,"select jszh,nvl(xyte,0)+nvl(xydj,0) * 100 as xyd, nvl(yjje,0)-nvl(syje,0)+nvl(tzje,0) yjk"
			" from tyt_yhzh");
		strcpy(qs[YJIDX].updsqlstmt,"select jszh,nvl(xyte,0)+nvl(xydj,0) * 100 as xyd, nvl(yjje,0)-nvl(syje,0)+nvl(tzje,0) yjk"
			" from tyt_yhzh where xq=:xq and jszh=:jszh");
		strcpy(qs[YJIDX].chkupdsqlstmt," select distinct xq,yhbh from tyt_fwyd where slrq>=:dt1 and "//slsj<=:dt2 and "
          " fx in ('Y2','Y0','Y1','L6','L7','L8','LD','LE') ");
		strcpy(qs[YJIDX].schkupdsqlstmt," select yhbh from tyt_fwyd where xq=:xq and nvl(yhbh,0)=:yhbh and slrq>=:dt1 and "//slsj<=:dt2 and "
          " fx in ('Y2','Y0','Y1','L6','L7','L8','LD','LE') and rownum<2");
		qs[YJIDX].mtrows=3000000;
		//strcpy(qs[YJIDX].pkcol,"yhbh");
		strcpy(qs[YJIDX].sortcols,"jszh");
		qs[YJIDX].update.fix[0].hour=4;
		qs[YJIDX].update.fix[0].min=30;
		qs[YJIDX].update.fix[1].hour=12;
		qs[YJIDX].update.fix[1].min=-1;
		qs[YJIDX].update.fix[2].hour=19;
		qs[YJIDX].update.fix[2].min=30;
		qs[YJIDX].update.fix[3].min=-1;
		qs[YJIDX].update.period[0]=40*60;// 1 hour
		qs[YJIDX].update.period[1]=90*60;// 1.5 hours

		//欠费资料
		qs[WJIDX].valid=true;
		strcpy(qs[WJIDX].sqlstmt,"select yhbh,nvl(zje,0)-nvl(je,0) as hf,ny"
			" from thh_hfzd where nvl(jff,0)=0 ");
		strcpy(qs[WJIDX].updsqlstmt,"select yhbh,sum(nvl(zje,0)-nvl(je,0)) as hf"
			" from thh_hfzd where nvl(jff,0)=0 and xq=:xq  and yhbh=:yhbh and ny <= :ny group by yhbh");
		strcpy(qs[WJIDX].chkupdsqlstmt," select distinct xq,yhbh from tyt_fwyd where slrq>=:dt1 and "//slsj<=:dt2 and "
          " fx in ('L1','L0','L2','L5','L9','LC','LF') ");
		strcpy(qs[WJIDX].schkupdsqlstmt," select yhbh from tyt_fwyd where xq=:xq and nvl(yhbh,0)=:yhbh and slrq>=:dt1 and "//slsj<=:dt2 and "
          " fx in ('L1','L0','L2','L5','L9','LC','LF') and rownum<2");
		qs[WJIDX].mtrows=2000000;
//		qs[WJIDX].needgrp=true;
		strcpy(qs[WJIDX].pkcol,"yhbh");
		//strcpy(qs[WJIDX].sortcols,"jszh");
		qs[WJIDX].update.fix[0].hour=5;
		qs[WJIDX].update.fix[0].min=0;
		qs[WJIDX].update.fix[1].hour=13;
		qs[WJIDX].update.fix[1].min=-1;
		qs[WJIDX].update.fix[2].hour=20;
		qs[WJIDX].update.fix[2].min=0;
		qs[WJIDX].update.fix[3].min=-1;
		qs[WJIDX].update.period[0]=40*60;// 1 hour
		qs[WJIDX].update.period[1]=90*60;// 1.5 hours

		//实时话费资料
		qs[SSIDX].valid=false;
		strcpy(qs[SSIDX].sqlstmt,"select yhbh,nvl(fee,0)-nvl(jms,0) hf "
			" from %s PARTITION('%s') where bm not in (36,37)and bm<70 and ny>%d ");
		strcpy(qs[SSIDX].updsqlstmt,"select yhbh,sum(nvl(fee,0)-nvl(jms,0)) sshf "
			" from %s where xq=:xq and yhbh=:yhbh bm not in (36,37)and bm<70 and ny>:ny group by yhbh");
		//strcpy(qs[SSIDX].chkupdsqlstmt," select distinct xq,yhbh from tyt_fwyd where slrq>=:dt1 and "//slsj<=:dt2 and "
        //  " fx in ('L1','L0','L2','L5','L9','LC','LF') ");
		qs[SSIDX].mtrows=2000000;
//		qs[SSIDX].needgrp=true;
		strcpy(qs[SSIDX].pkcol,"yhbh");
		//strcpy(qs[SSIDX].sortcols,"jszh");
		//qs[SSIDX].update.fix[0].hour=5;
		qs[SSIDX].update.fix[0].min=-1;
		//qs[SSIDX].update.fix[1].hour=13;
		//qs[SSIDX].update.fix[1].min=0;
		//qs[SSIDX].update.fix[2].hour=20;
		//qs[SSIDX].update.fix[2].min=0;
		//qs[SSIDX].update.fix[3].min=-1;
		qs[SSIDX].update.period[0]=40*60;// 1 hour
		qs[SSIDX].update.period[1]=90*60;// 1.5 hours
		
		//套餐资料
		qs[TCIDX].valid=true;
		strcpy(qs[TCIDX].sqlstmt,"select xq,jz,tcdm,tcjb from tbm_tc ");
		//strcpy(qs[TCIDX].updsqlstmt,"select yhbh,sum(nvl(fee,0)-nvl(jms,0)) sshf "
		//	" from thh_hfmx1 where xq=:xq and yhbh=:yhbh bm not in (36,37)and bm<70 and ny>:ny");
		//strcpy(qs[TCIDX].chkupdsqlstmt," select distinct xq,yhbh from tyt_fwyd where slrq>=:dt1 and "//slsj<=:dt2 and "
        //  " fx in ('L1','L0','L2','L5','L9','LC','LF') ");
		qs[TCIDX].mtrows=20000;
		//strcpy(qs[TCIDX].pkcol,"yhbh");
		strcpy(qs[TCIDX].sortcols,"xq,jz,tcdm");
		qs[TCIDX].update.fix[0].hour=5;
		qs[TCIDX].update.fix[0].min=0;
		qs[TCIDX].update.fix[1].hour=12;
		qs[TCIDX].update.fix[1].min=-1;
		qs[TCIDX].update.fix[2].hour=19;
		qs[TCIDX].update.fix[2].min=0;
		qs[TCIDX].update.fix[3].min=-1;
		qs[TCIDX].update.period[0]=-1;
		yydb=wociCreateSession("yngsm","mcmunicom","cuyn12");
		int stmt=wociCreateStatment(yydb);
		int mt=wociCreateMemTable();
		wociPrepareStmt(stmt,"select xq,jz,max(ny) ny from thh_yxsf where bz=2 group by xq,jz");
		wociBuildStmt(mt,stmt,100);
		wociFetchAll(mt);
		wociWaitLastReturn(mt);
		wocidestroy(stmt);
		yxsfmt=mt;
		wociSetSortColumn(mt,"xq,jz");
		wociSort(mt);
	}

time_t QueryServer::GetHostTime() {
		int stmt=0,mt=0;
		char pdt[15];
		try{
		stmt=wociCreateStatment(yydb);
		wociPrepareStmt(stmt,"select sysdate from dual");
		mt=wociCreateMemTable();
		wociBuildStmt(mt,stmt,2);
		wociFetchAll(mt);
		wociWaitLastReturn(mt);
		wociGetDateValByName(mt,"sysdate",0,pdt);
		}
		catch(...) {
		if(mt>0)
			wocidestroy(mt);
		if(stmt>0)
			wocidestroy(stmt);
		throw;
		}
		wocidestroy(mt);
		wocidestroy(stmt);
		CTime ctm(wociGetYear(pdt),wociGetMonth(pdt),wociGetDay(pdt),
			wociGetHour(pdt),wociGetMin(pdt),wociGetSec(pdt));
		return ctm.GetTime();
	}
	//Set next update data time,reload use fix time only;
void QueryServer::SetNextTime(int idx) {
		time_t tmp=qs[idx].lsttm;
		CTime ctm=CTime(tmp);
		if(ctm.GetHour()<7 || ctm.GetHour()>=22) {
			tmp+=qs[idx].update.period[1];
			CTime ctm1=CTime(tmp);
			if(ctm1.GetHour()>=7 && ctm1.GetHour()<22) qs[idx].nxttm=qs[idx].lsttm+qs[idx].update.period[0];
			else qs[idx].nxttm=tmp;
		}
		else {
			tmp+=qs[idx].update.period[0];
			CTime ctm1=CTime(tmp);
			if(ctm1.GetHour()<7 || ctm1.GetHour()>=22) qs[idx].nxttm=qs[idx].lsttm+qs[idx].update.period[1];
			else qs[idx].nxttm=tmp;
		}
		//check and update the last reload time;
		// search improperly idx;
		for(int i=0;i<5;i++) {
			if(qs[idx].update.fix[i].min <0) break;
			if(ctm.GetHour()*60+ctm.GetMinute()<qs[idx].update.fix[i].hour*60+qs[idx].update.fix[i].min) break;
		}
		if(i<5 && qs[idx].update.fix[i].min >=0) { // not last element
					ctm=CTime(ctm.GetYear( ),ctm.GetMonth(),ctm.GetDay(),
						qs[idx].update.fix[i].hour,
						qs[idx].update.fix[i].min,0);
					qs[idx].rldtm=ctm.GetTime();
		}
		else {
					//next day
					ctm+=CTimeSpan(1,0,0,0);
					ctm=CTime(ctm.GetYear( ),ctm.GetMonth(),ctm.GetDay(),
						qs[idx].update.fix[0].hour,
						qs[idx].update.fix[0].min,0);
					qs[idx].rldtm=ctm.GetTime();
		}
	}



int start(void *p) {
	wociSetEcho(false);
	wociSetOutputToConsole(true);
	char tbuffer [19];
	lgprintf("\n\n\n查询服务 启动");
	QueryServer qs;
	qs.Init();
	qs.LoadData();
	qs.ShowTmAll();
	while(true) {
	  qs.Run(NULL);
	  Sleep(120000);
	}
	double rt=0,tot=0;
	int dhhm_len;
	FILE *fp=fopen("e:\\queryserver\\testdn.dat","rb");
	int ct=0;
	int bg=GetTickCount();
	if(fp) {
		fread(&dhhm_len,1,sizeof(int),fp);
		char dhhm[30];
		while(fread(dhhm,1,dhhm_len,fp)>0) {
			qs.get_balfee(dhhm,rt);
			//lgprintf("dhhm:%s ret :%f",dhhm,rt);
			tot+=rt;
			ct++;
		}
		fclose(fp);
	}
	lgprintf("查询用户数:%d,总金额%f,tm:%f",ct,tot,(GetTickCount()-bg)/1000.0);
	qs.SaveData();
	lgprintf("查询服务 退出");
	return 1;
	
	int sess=wociCreateSession("yngsm","mcmunicom",svctxt);

	int stmt=wociCreateStatment(sess);
	int mt=wociCreateMemTable();
	wociPrepareStmt(stmt,"select yhbh,mm from tyt_yhda where xq='AQ' and dhhm=:dhhm");
	wociBuildStmt(mt,stmt,100);
	char *bf=new char[1000];
	int dh_len=16;
	strcpy(bf,"13008696057");
	strcpy(bf+dh_len,"13308843706");
	strcpy(bf+dh_len*2,"13008696034");
	wociBindStrByPos(stmt,1,bf,dh_len);
	wociExecute(stmt,3);
	wociWaitLastReturn(stmt);
	wociFreshRowNum(mt);
	wociMTPrint(mt,10,NULL);
	delete []bf;
	return 1;
	try {
	/*
	wociPrepareStmt(stmt,"select nvl(rtrim(call_type),' '),start_date||start_time,\
 nvl(call_duration,0),nvl(rtrim(called_code),' '),\
 nvl(rtrim(msc),' '),\
 nvl(rtrim(home_area_code),' '),\
 nvl(rtrim(caller_type),' '),nvl(rtrim(is_local),' '),\
 nvl(rtrim(caller_user_type),' '), nvl(cfee,0)*100,\
 nvl(lfee,0)*100,nvl(rtrim(trunk_groupout),' '),\
 nvl(rtrim(trunk_groupin),' '),nvl(rtrim(roam_type),' '),\
 nvl(rtrim(service_type),' '), nvl(rtrim(service_code),' '),\
 nvl(rtrim(visit_area_code),' '),\
 nvl(rtrim(opp_area_code),' '),\
 nvl(rtrim(msisdn),' '),nvl(rtrim(other_party),' ')\
from local_cdr2 where caller_user_type='0' and caller_type='U'");
*/
	//wociPrepareStmt(stmt,"select dhhm,yhbh,xq,jz,tch,mm,xyce,jszh,zt,mff,yhlb from tyt_yhda  ");//14 minutes
	//wociPrepareStmt(stmt,"select yhbh,nvl(zje,0)-nvl(je,0) as wjhf from thh_hfzd where xq='AQ' and nvl(jff,0)=0");//6'3''
	//wociPrepareStmt(stmt,"select yhbh,sum(nvl(zje,0)-nvl(je,0)) as wjhf from thh_hfzd where xq='AQ' and nvl(jff,0)=0 group by yhbh");//12'27''
	//wociPrepareStmt(stmt,"select jszh,nvl(xyte,0)+nvl(xydj,0) * 100 as xyd, nvl(yjje,0)-nvl(syje,0)+nvl(tzje,0) yjk from tyt_yhzh");//8'01'' 2450727rows
//	wociPrepareStmt(stmt,"select yhbh,dhhm,mm,khsj from tyt_yhda ");
//	wociPrepareStmt(stmt,"SELECT yhbh,nvl(roam_type,' '),nvl(call_type,' '),nvl(caller_type,' '),nvl(caller_user_type,' '),nvl(is_local,' '),nvl(cfee_times,0)  FROM local_cdr1 where XQ='AQ'");//where rownum<200000");
	//wociPrepareStmt(stmt,"select dhhm,yhbh,jszh,zt,tch,mm,xyce from tyt_yhda ");
	//wociPrepareStmt(stmt," select yhbh,sum(nvl(fee,0)-nvl(jms,0)) sshf "
    //    " from thh_hfmx1 where  bm not in (36,37)and bm<70 "
    //    " group by yhbh ");// 9'31'' 1001120rows
	wociPrepareStmt(stmt," select yhbh,nvl(fee,0)-nvl(jms,0) sshf "
        " from thh_hfmx1 where bm not in (36,37)and bm<70 "); //6'12''(11'51'') 4894864rows
        //" group by yhbh ");
	
	_strtime( tbuffer );
	printf("Begin :%s\n",tbuffer);
	wociBuildStmt(mt,stmt,8000000);
	wociFetchAll(mt);
	wociWaitLastReturn(mt);
	//wociSetIKByName(mt,"yhbh");
	//wociOrderByIK(mt);
	//wociMTPrint(mt,50,NULL);
	}
	catch(...) {
		printf("release resource.\n");
		delete []bf;
		throw;
	}
	delete []bf;
	_strtime( tbuffer );
	printf("Begin :%s\n",tbuffer);
	getch();
	return 0;
}


void QueryServer::ShowStat(void)
{
	printf("/********  查询服务缓冲统计 ***************/\n");
	printf("档案 %d次 ,缓冲%d次.\n",stat.dact,stat.dahit);
	printf("预交 %d次 ,缓冲%d次.\n",stat.yjct,stat.yjhit);
	printf("欠费 %d次 ,缓冲%d次.\n",stat.wjct,stat.wjhit);
	printf("/******************************************/\n");
}
