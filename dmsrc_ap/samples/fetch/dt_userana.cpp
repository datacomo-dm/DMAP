#include "dt_lib.h"
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h> 
#endif
#include <stdlib.h>
#include <stdio.h>
int Start(void *ptr);
int ny=200403;
char RES_TAB_NAME[30]= "tab_userana_200403";
char TABID[30]="tab_gsmvoicdr3";
//#define TABID 3
#define FIRSTOFF 1
  	
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    if(argc!=3) {
    	printf("Usage: dtana [yyyymm] [tabname].\nDatabase name use default -'dest'.\n");
    	return -1;
    }
    ny=atoi(argv[1]);
    strcpy(TABID,argv[2]);
    strcpy(RES_TAB_NAME,argv[2]);
    strcat(RES_TAB_NAME,"_ana");
    WOCIInit("dt_userana");
    nRetCode=wociMainEntrance(Start,true,NULL);
    WOCIQuit(); 
    return nRetCode;
}

void DirFullAccessTest(AutoHandle &dts) {
	mytimer tm;
	mytimer tm1,tm2;
	int rn;
	wociSetEcho(TRUE);
	wociSetOutputToConsole(TRUE);
	int firstdow=0;
	{
		AutoMt tmp(dts,10);
		tmp.FetchAll("select to_number(to_char(to_date('%d01','yyyymmdd'),'d')) from dual",ny);
		tmp.Wait();
		firstdow=*tmp.PtrInt(0,0)-1;
	}
	/***************全表直接访问测试 */
	tm.Start();
	TableScan ts(dts);
	ts.OpenTable("dest",TABID);
	int mt;
	int trn=0;
	lgprintf("Table scan begin.");
	tm1.Clear();
	tm2.Clear();
	int oldrm=0;
	AutoMt resmt(0,2000000);
	//不考虑月份字段
/*00*/	wociAddColumn(resmt,"user_id","用户标识",COLUMN_TYPE_NUM,10,0);		
/*00*/	wociAddColumn(resmt,"part_id1","用户标识",COLUMN_TYPE_CHAR,4,0);

/*01*/	wociAddColumn(resmt,"call_times","总通话次数",COLUMN_TYPE_NUM,10,0);
/*02*/	wociAddColumn(resmt,"call_dura","总通话时长",COLUMN_TYPE_NUM,10,0);
/*03*/	wociAddColumn(resmt,"call_loc_times","本地通话次数",COLUMN_TYPE_NUM,10,0);
/*04*/	wociAddColumn(resmt,"call_loc_dura","本地通话时长",COLUMN_TYPE_NUM,10,0);
/*05*/	wociAddColumn(resmt,"call_nation_ldc_times","国内长途通话次数",COLUMN_TYPE_NUM,10,0);
/*06*/	wociAddColumn(resmt,"call_nation_ldc_dura","国内长途通话时长",COLUMN_TYPE_NUM,10,0);
/*07*/	wociAddColumn(resmt,"call_inter_ldc_times","国际长途通话次数",COLUMN_TYPE_NUM,10,0);
/*08*/	wociAddColumn(resmt,"call_inter_ldc_dura","国际长途通话时长",COLUMN_TYPE_NUM,10,0);
/*09*/	wociAddColumn(resmt,"call_ip_ldc_times","IP长途通话次数",COLUMN_TYPE_NUM,10,0);
/*10*/	wociAddColumn(resmt,"call_ip_ldc_dura","IP长途通话时长",COLUMN_TYPE_NUM,10,0);
/*11*/	wociAddColumn(resmt,"call_roam_times","漫游通话次数",COLUMN_TYPE_NUM,10,0);
/*12*/	wociAddColumn(resmt,"call_roam_dura","漫游通话时长",COLUMN_TYPE_NUM,10,0);
/*13*/	wociAddColumn(resmt,"calling_times","主叫通话次数",COLUMN_TYPE_NUM,10,0);
/*14*/	wociAddColumn(resmt,"calling_dura","主叫通话时长",COLUMN_TYPE_NUM,10,0);
/*15*/	wociAddColumn(resmt,"called_times","被叫通话次数",COLUMN_TYPE_NUM,10,0);
/*16*/	wociAddColumn(resmt,"called_dura","被叫通话时长",COLUMN_TYPE_NUM,10,0);
/*17*/	wociAddColumn(resmt,"called_mobile_center_times","移动客服联系用户次数",COLUMN_TYPE_NUM,10,0);
/*18*/	wociAddColumn(resmt,"called_mobile_center_dura","移动客服联系用户时长",COLUMN_TYPE_NUM,10,0);
/*19*/	wociAddColumn(resmt,"called_unicom_center_times","联通客服联系用户次数",COLUMN_TYPE_NUM,10,0);
/*20*/	wociAddColumn(resmt,"called_unicom_center_dura","联通联系用户时长",COLUMN_TYPE_NUM,10,0);
/*21*/	wociAddColumn(resmt,"call_trans_times","呼转通话次数",COLUMN_TYPE_NUM,10,0);
/*22*/	wociAddColumn(resmt,"call_trans_dura","呼转通话时长",COLUMN_TYPE_NUM,10,0);
/*23*/	wociAddColumn(resmt,"call_trans_mobile_times","呼转移动号码次数",COLUMN_TYPE_NUM,10,0);
/*24*/	wociAddColumn(resmt,"call_trans_mobile_dura","呼转移动号码时长",COLUMN_TYPE_NUM,10,0);
/*25*/	wociAddColumn(resmt,"call_trans_unicom_times","呼转联通号码次数",COLUMN_TYPE_NUM,10,0);
/*26*/	wociAddColumn(resmt,"call_trans_unicom_dura","呼转联通号码时长",COLUMN_TYPE_NUM,10,0);
/*27*/	wociAddColumn(resmt,"call_job_times","工作时段通话次数",COLUMN_TYPE_NUM,10,0);
/*28*/	wociAddColumn(resmt,"call_job_dura","工作时段通话时长",COLUMN_TYPE_NUM,10,0);
/*29*/	wociAddColumn(resmt,"call_idel_times","非工作时段通话次数",COLUMN_TYPE_NUM,10,0);
/*30*/	wociAddColumn(resmt,"call_idel_dura","非工作时段通话时长",COLUMN_TYPE_NUM,10,0);
/*31*/	wociAddColumn(resmt,"call_weekend_times","周末通话次数",COLUMN_TYPE_NUM,10,0);
/*32*/	wociAddColumn(resmt,"call_weekend_dura","周末通话时长",COLUMN_TYPE_NUM,10,0);
/*33*/	wociAddColumn(resmt,"call_disc_times","优惠时段通话次数",COLUMN_TYPE_NUM,10,0);
/*34*/	wociAddColumn(resmt,"call_disc_dura","优惠时段通话时长",COLUMN_TYPE_NUM,10,0);
/*35*/	wociAddColumn(resmt,"call_noma_times","非优惠时段通话次数",COLUMN_TYPE_NUM,10,0);
/*36*/	wociAddColumn(resmt,"call_noma_dura","非优惠时段通话时长",COLUMN_TYPE_NUM,10,0);
/*37*/	wociAddColumn(resmt,"dist_total","拨打不同号码个数",COLUMN_TYPE_NUM,10,0);
/*38*/	wociAddColumn(resmt,"dist_in","拨入不同号码个数",COLUMN_TYPE_NUM,10,0);
/*39*/	wociAddColumn(resmt,"dist_out","拨出不同号码个数",COLUMN_TYPE_NUM,10,0);
/*40*/	wociAddColumn(resmt,"smt_time","短信次数",COLUMN_TYPE_NUM,10,0);
	resmt.Build();
	wociSetSortColumn(resmt,"user_id");
	wociSort(resmt);
	AutoMt relt(0,10000);
	AutoMt relin(0,10000);
	AutoMt relout(0,10000);
	wociAddColumn(relt,"termphone","对方号码",COLUMN_TYPE_CHAR,24,0);		
	wociAddColumn(relin,"termphone","对方号码",COLUMN_TYPE_CHAR,24,0);		
	wociAddColumn(relout,"termphone","对方号码",COLUMN_TYPE_CHAR,24,0);		
	relt.Build();
	relin.Build();
	relout.Build();
	wociSetSortColumn(relt,"termphone");
	wociSetSortColumn(relin,"termphone");
	wociSetSortColumn(relout,"termphone");
	wociSort(relt);	
	wociSort(relin);
	wociSort(relout);
	int olduid=0;
	tm1.Start();
	mt=ts.GetNextBlockMt();
	tm1.Stop();
	if(!mt) return;
	AutoMt cdrmt(0);
	cdrmt.SetHandle(mt,true);
	// Get column index pos from cdrmt:
	int cuidp=cdrmt.GetPos("subscrbid",COLUMN_TYPE_INT);
	int cdurp=cdrmt.GetPos("CALLTIMELEN",COLUMN_TYPE_INT);
	int cctp=cdrmt.GetPos("CALLTYPE",COLUMN_TYPE_CHAR); //2char
	int crtp=cdrmt.GetPos("ROAMTYPE",COLUMN_TYPE_CHAR); //1char
	int ctmtp=cdrmt.GetPos("TERM_TYPE",COLUMN_TYPE_CHAR); //1char
	int cilp=cdrmt.GetPos("ISLOCAL",COLUMN_TYPE_CHAR); //1char
	int ctotp=cdrmt.GetPos("TOLLTYPE",COLUMN_TYPE_CHAR); //1char
	int ctmp=cdrmt.GetPos("TERMPHONE",COLUMN_TYPE_CHAR); //1char
	int cbdtp=cdrmt.GetPos("BEGINDATE",COLUMN_TYPE_CHAR); //1char
	int cbtmp=cdrmt.GetPos("BEGINTIME",COLUMN_TYPE_CHAR); //1char
	int ctpp=cdrmt.GetPos("THIRDPARTY",COLUMN_TYPE_CHAR); //1char
	int cstp=cdrmt.GetPos("SERVICETYPE",COLUMN_TYPE_CHAR); //1char
	int cpid1p=cdrmt.GetPos("PART_ID1",COLUMN_TYPE_CHAR); //1char
	while(mt)
	{
		tm2.Start();
		rn=wociGetMemtableRows(mt);
		trn+=rn;
		if(trn%200000<oldrm) {
			printf(".");fflush(stdout);
		}
		oldrm=trn%200000;
		int r=0;
		cdrmt.SetHandle(mt,true);
		for(r=0;r<rn;r++) {
			if(strcmp(cdrmt.PtrStr(cpid1p,r),"860")!=0) continue;
			void *ptr[3];
			ptr[0]=cdrmt.PtrInt(cuidp,r);
			ptr[1]=NULL;
			int rp=wociSearch(resmt,ptr);
			if(rp<0) {
				ptr[1]=cdrmt.PtrStr(cpid1p,r);
				ptr[2]=NULL;
				wociInsertRows(resmt,ptr,"user_id,part_id1",1);
				ptr[1]=NULL;
				rp=wociSearch(resmt,ptr);
				if(rp<0) throw("insert into result mt failed!");
			}
			int dur=*cdrmt.PtrInt(cdurp,r);
			//总次数/时长
			bool voicesvc=strcmp(cdrmt.PtrStr(cstp,r),"000")==0;
			if(voicesvc) {
				(*resmt.PtrInt(FIRSTOFF+1,rp))++;
				(*resmt.PtrInt(FIRSTOFF+2,rp))+=dur;
			}
			//本地
			if(voicesvc && cdrmt.PtrStr(cilp,r)[0]=='0' && cdrmt.PtrStr(crtp,r)[0]=='0') {
				(*resmt.PtrInt(FIRSTOFF+3,rp))++;
				(*resmt.PtrInt(FIRSTOFF+4,rp))+=dur;
			}
			//国内长途
			if(voicesvc && cdrmt.PtrStr(ctotp,r)[0]=='1' || cdrmt.PtrStr(ctotp,r)[0]=='2') {
				(*resmt.PtrInt(FIRSTOFF+5,rp))++;
				(*resmt.PtrInt(FIRSTOFF+6,rp))+=dur;
			}
			//国际长途
			else if(voicesvc && cdrmt.PtrStr(ctotp,r)[0]=='3') {
				(*resmt.PtrInt(FIRSTOFF+7,rp))++;
				(*resmt.PtrInt(FIRSTOFF+8,rp))+=dur;
			}
			//IP
			if(voicesvc && strncmp( cdrmt.PtrStr(ctmp,r),"17",2)==0) {
				(*resmt.PtrInt(FIRSTOFF+9,rp))++;
				(*resmt.PtrInt(FIRSTOFF+10,rp))+=dur;
			}
			//漫游
			if(voicesvc && cdrmt.PtrStr(crtp,r)[0]!='0') {
				(*resmt.PtrInt(FIRSTOFF+11,rp))++;
				(*resmt.PtrInt(FIRSTOFF+12,rp))+=dur;
			}
			//主叫
			if(voicesvc &&strncmp(cdrmt.PtrStr(cctp,r),"01",2)==0) {
				(*resmt.PtrInt(FIRSTOFF+13,rp))++;
				(*resmt.PtrInt(FIRSTOFF+14,rp))+=dur;
			}
			//被叫
			if(voicesvc &&strncmp(cdrmt.PtrStr(cctp,r),"02",2)==0) {
				(*resmt.PtrInt(FIRSTOFF+15,rp))++;
				(*resmt.PtrInt(FIRSTOFF+16,rp))+=dur;
			}
			//移动客服
			if(voicesvc &&strncmp(cdrmt.PtrStr(ctmp,r),"186",3)==0) {
				(*resmt.PtrInt(FIRSTOFF+17,rp))++;
				(*resmt.PtrInt(FIRSTOFF+18,rp))+=dur;
			}
			//联通客服
			if(voicesvc &&strncmp(cdrmt.PtrStr(ctmp,r),"1001",4)==0) {
				(*resmt.PtrInt(FIRSTOFF+19,rp))++;
				(*resmt.PtrInt(FIRSTOFF+20,rp))+=dur;
			}
			//呼转
			if(strcmp(cdrmt.PtrStr(cstp,r),"002")==0) {
				(*resmt.PtrInt(FIRSTOFF+21,rp))++;
				(*resmt.PtrInt(FIRSTOFF+22,rp))+=dur;
				//呼转移动
				char *thirdparty=cdrmt.PtrStr(ctpp,r);
				if(strncmp(thirdparty,"13",2)==0) {
					if(thirdparty[2]>'4') {
						(*resmt.PtrInt(FIRSTOFF+23,rp))++;
						(*resmt.PtrInt(FIRSTOFF+24,rp))+=dur;
				        }
					//呼转联通
					else {
						(*resmt.PtrInt(FIRSTOFF+25,rp))++;
						(*resmt.PtrInt(FIRSTOFF+26,rp))+=dur;
					}
				}
			}
			//begin week day and hour
			char *bgdt=cdrmt.PtrStr(cbdtp,r);
			char bghr[10];
			strcpy(bghr,cdrmt.PtrStr(cbtmp,r)+2);
			bghr[2]=0;
			int day=(atoi(bgdt+6)-1+firstdow)%7; //0-6
			int hour=atoi(bghr);
			//周末
			if(voicesvc && (day==0 || day==6)) {
				(*resmt.PtrInt(FIRSTOFF+31,rp))++;
				(*resmt.PtrInt(FIRSTOFF+32,rp))+=dur;
			}
			//工作时段
			if(voicesvc && hour>=8 && hour<19 && day!=0 && day!=6) {
				(*resmt.PtrInt(FIRSTOFF+27,rp))++;
				(*resmt.PtrInt(FIRSTOFF+28,rp))+=dur;
			}
			else if(voicesvc){
				//非工作时段
				(*resmt.PtrInt(FIRSTOFF+29,rp))++;
				(*resmt.PtrInt(FIRSTOFF+30,rp))+=dur;
			}
			//优惠时段
			if(voicesvc && (hour<8 || hour>=22)) {
				(*resmt.PtrInt(FIRSTOFF+33,rp))++;
				(*resmt.PtrInt(FIRSTOFF+34,rp))+=dur;
			}
			//非优惠时段
			else if(voicesvc) {
				(*resmt.PtrInt(FIRSTOFF+35,rp))++;
				(*resmt.PtrInt(FIRSTOFF+36,rp))+=dur;
			}
			
			//交往圈
			
			if(olduid!=*cdrmt.PtrInt(cuidp,r)) {
				//void *ptr[2];
				ptr[0]=cdrmt.PtrStr(ctmp,r);
				ptr[1]=NULL;
				relt.Reset();
				relin.Reset();
				relout.Reset();
				wociSetSortColumn(relt,"termphone");
				wociSetSortColumn(relin,"termphone");
				wociSetSortColumn(relout,"termphone");
				wociSort(relt);	
				wociSort(relin);
				wociSort(relout);
				if(voicesvc) {
					wociInsertRows(relt,ptr,NULL,1);
					if(strcmp(cdrmt.PtrStr(cctp,r),"01")==0) 
						wociInsertRows(relout,ptr,NULL,1);
					else
				        	wociInsertRows(relin,ptr,NULL,1);
				}
				olduid=*cdrmt.PtrInt(cuidp,r);
			}
			else if(voicesvc){
				//void *ptr[2];
				ptr[0]=cdrmt.PtrStr(ctmp,r);
				ptr[1]=NULL;
				if(wociSearch(relt,ptr)<0) 
				  wociInsertRows(relt,ptr,NULL,1);
				if(strcmp(cdrmt.PtrStr(cctp,r),"01")==0) 
				{
				 if(wociSearch(relout,ptr)<0) 
				   wociInsertRows(relout,ptr,NULL,1);
				}
				else {
				 if(wociSearch(relin,ptr)<0) 
				   wociInsertRows(relin,ptr,NULL,1);
				}
			}
			(*resmt.PtrInt(FIRSTOFF+37,rp))=wociGetMemtableRows(relt);
			(*resmt.PtrInt(FIRSTOFF+38,rp))=wociGetMemtableRows(relout);
			(*resmt.PtrInt(FIRSTOFF+39,rp))=wociGetMemtableRows(relin);
			
			//短信
			if(strcmp(cdrmt.PtrStr(cstp,r),"003")==0) {
				(*resmt.PtrInt(FIRSTOFF+40,rp))++;
			}
		}
		tm2.Stop();
		//if(trn>2000000) break;
		tm1.Start();
		mt=ts.GetNextBlockMt();
		tm1.Stop();
	}
	wociReInOrder(resmt);
	rn=wociGetMemtableRows(resmt);
	lgprintf("Total rows :%d,group to %d rows",trn,rn);
 	lgprintf("Test end.");
// 	if(rn>10)
// 	 wociMTPrint(resmt,10,NULL);
 	resmt.CreateAndAppend(RES_TAB_NAME,dts);
	wociCommit(dts);
 	tm.Stop();
 	lgprintf("Consuming %7.3f second(rd:%.3f,grp:%.3f) .",tm.GetTime(),tm1.GetTime(),tm2.GetTime());
}

int Start(void *ptr) { 
    AutoHandle dts;
    dts.SetHandle(wociCreateSession("system","gcmanager","dtagt189"));
    DirFullAccessTest(dts);
    return 1;
}

