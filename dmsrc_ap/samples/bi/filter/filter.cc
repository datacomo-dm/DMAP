 #ifdef WIN32
 #include <windows.h>
 #include <process.h>
 #include <io.h>
 #else
 #include <unistd.h>
 #endif
 #include <stdlib.h>
 #include <stdio.h>
 #include "AutoHandle.h"
 int Start(void *ptr);
 int argc;
 char **argv; 	
 int main(int _argc,char *_argv[]) {
     WOCIInit("stat");
     argc=_argc;
     argv=_argv;
     int nRetCode=wociMainEntrance(Start,true,NULL,2);
     WOCIQuit(); 
     return nRetCode;
 }

 int Start(void *ptr) { 
     wociSetOutputToConsole(TRUE);
     lgprintf("连接到数据库...");
     AutoHandle dts;
     dts.SetHandle(wociCreateSession("sunhua","sunh123","//130.86.12.30/ubisp.ynunicom.com",DTDBTYPE_ORACLE));
     mytimer mt_1;
     mt_1.Start();
     AutoMt usrtab(dts,2400000);
     usrtab.FetchAll("select areaid,subscrbid,msisdn,imei,firsttime,lasttime,calltimes from ods.ODS_IMEI_INFO_VALID where rownum<1");
     usrtab.Wait();
     wociSetSortColumn(usrtab,"imei");
     wociSort(usrtab);
     AutoMt imeishort(dts,3000000);
     imeishort.FetchAll("select subscrbid,imei,firsttime from ods.ods_imei_user_current");
     imeishort.Wait();
     wociSetSortColumn(imeishort,"imei");
     wociSort(imeishort);
     AutoMt imeiall(dts,50000);
     imeiall.FetchFirst("select areaid,subscrbid,msisdn,imei,firsttime,lasttime,calltimes from ods.ODS_IMEI_INFO_VALID where calltimes>4");
     int rn=imeiall.Wait();
     void *search[2];
     search[1]=NULL;
     int addct=0;
     while(rn>0) {
     	for(int i=0;i<rn;i++) {
     		//if(imeiall.GetInt("calltimes",i)<5) continue;
     		search[0]=imeiall.PtrStr("imei",i);
     		int op=wociSearch(imeishort,search);
     		if(op<0) continue;
     		if(imeiall.GetInt("subscrbid",i)==imeishort.GetInt("subscrbid",op)) continue;
     		if(memcmp(imeiall.PtrDate("lasttime",i),imeishort.PtrDate("firsttime",op),7)>0) continue;
     		if(i==34897) {
     			int breakhere=1;
     		}
     		int op2=wociSearch(usrtab,search);
     		if(op2<0 || usrtab.GetInt("calltimes",op2)<imeiall.GetInt("calltimes",i))  
     		  wociCopyRowsToNoCut(imeiall,usrtab,op2,i,1);
	}
	imeiall.FetchNext();
	rn=imeiall.Wait();
     }
     wociSetSortColumn(imeishort,"subscrbid");
     wociSort(imeishort);
     rn=usrtab.GetRows();
     for(int i=0;i<rn;i++) {
     	search[0]=usrtab.PtrInt("subscrbid",i);
	int op=wociSearch(imeishort,search);
	if(op>=0) wociQDeleteRow(usrtab,i);
     }
     wociCompressBf(usrtab);
     usrtab.CreateAndAppend("qhl_repeatent_st1",dts,true);
     mt_1.Stop();
     lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
 }

