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
 void Stati(const char *table_name,char *part_id);
 #define OUTFILENAME "hotfee.txt"
 int Start(void *ptr) { 
     wociSetOutputToConsole(TRUE);
     char s_tab_name[1024];
     char s_part[100];
     int rn;
     if (argc != 2)
     {
         printf("命令行参数格式: bal 年月\n例如 : bal 200507\n");
         exit(0);
     }
     lgprintf("连接到本地数据库...");
     AutoHandle dts;
     dts.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.12.18:1522/obs9i",DTDBTYPE_ORACLE));
     AutoHandle cas;
     cas.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.12.17:1526/cas9i",DTDBTYPE_ORACLE));
     mytimer mt_1;
     mt_1.Start();
     //40万纪录的存储区
     //wociSetDefaultPrec(10,0);
     AutoMt usrtab(dts,2400000);
     wociAddColumn(usrtab,"rfee",NULL,COLUMN_TYPE_NUM,10,2);
     wociAddColumn(usrtab,"rentfee",NULL,COLUMN_TYPE_NUM,10,2);
     usrtab.SetAutoClear(false);
     usrtab.FetchAll("select subscrbid from obs.tab_subscrbprcpln where prcplnid=23763 and areaid='086860'"
      " and begtime<sysdate and nvl(endtime,sysdate+1)>sysdate");
     usrtab.Wait();
     printf("Get %d rows.\n",usrtab.GetRows());
     wociSetSortColumn(usrtab,"subscrbid");
     wociSort(usrtab);
     //wociSetIKByName(usrtab,"subscrbid");
     //wociOrderByIK(usrtab);
     //取实时话费
     wociSetDefaultPrec(10,2);
     AutoMt srctab(cas,50000);
     srctab.FetchFirst("select subscrbid,(fee+dfee)/1000.0 as rfee from yncas.tab_acc_gsm_hot_fee_860_1 where month>=%s "
     "and to_number(substr(acctitmid,-4,4)) in(1,2,3,4,5,6,7,28,29,30,31,32,72,73,74,76,77,98,99,100,121,122,143,144,187,188,189,229,230,231,252,253,254,275,276,277) "
     ,argv[1]);
     rn=srctab.Wait();
     while(rn) {
     	wociValuesSet(usrtab,srctab,"subscrbid;rfee","subscrbid;rfee",true,VALUESET_ADD);
     	srctab.FetchNext();
     	rn=srctab.Wait();
     }
 
     //取日租汇总
     srctab.FetchFirst("select subscrbid,(fee+dfee)/1000.0 as rfee from yncas.tab_acc_gsm_day_rent_860_1 where month>=%s and acctitmid='105003'",argv[1]);
     rn=srctab.Wait();
     while(rn) {
     	wociValuesSet(usrtab,srctab,"subscrbid;rfee","subscrbid;rfee",true,VALUESET_ADD);
     	wociValuesSet(usrtab,srctab,"subscrbid;rentfee","subscrbid;rfee",true,VALUESET_ADD);
     	srctab.FetchNext();
     	rn=srctab.Wait();
     }
     
     //having...
     rn=usrtab.GetRows();
     double *pv=usrtab.PtrDouble("rfee",0);
     int i=0;
     while(i++<rn) {
     	if(*pv<55 || *pv>65) wociQDeleteRow(usrtab,i-1);
     	pv++;
     }
     wociCompressBf(usrtab);
     //结果数据存到文本文件
     printf("Write to text file '%s'...\n",OUTFILENAME);
     wociSetMTName(usrtab,"GSM非销户用户清单");
     unlink(OUTFILENAME);
     wociMTToTextFile(usrtab,OUTFILENAME,0,NULL);
     
     mt_1.Stop();
     lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
 }

