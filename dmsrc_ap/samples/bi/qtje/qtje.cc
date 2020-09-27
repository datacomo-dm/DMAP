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
 #define OUTFILENAME "newqtje.txt"
 int Start(void *ptr) { 
     wociSetOutputToConsole(TRUE);
     char s_tab_name[1024];
     char s_part[100];
     lgprintf("连接到本地数据库...");
     AutoHandle dts;
     dts.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.12.18:1522/obs9i",DTDBTYPE_ORACLE));
     AutoHandle ca30;
     ca30.SetHandle(wociCreateSession("ods_temp","ods_temp","//130.86.12.30:1521/ubisp.ynunicom.com oracle",DTDBTYPE_ORACLE));
     mytimer mt_1;
     AutoMt prcpln(dts,2000000);
     prcpln.FetchAll("select subscrbid,prcplnid,0.0 qtje from obs.tab_subscrbprcpln where prcplntypeid<100 "
                         "  and nvl(endtime,sysdate+1)>sysdate and begtime<sysdate and areaid='086860'");
     int rn=prcpln.Wait();
     wociSetSortColumn(prcpln,"subscrbid");
     wociSort(prcpln);
     AutoMt qtje(dts,100000);
     qtje.FetchAll("select prcplnid,nvl(prcplncmptid,0) prcplncmptid from obs.tab_prcplnprc where prcplnparastr='qtje'");
     qtje.Wait();
     wociSetSortColumn(qtje,"prcplnid");
     wociSort(qtje);
     void *search[2];
     int svalue;
     search[0]=&svalue;
     search[1]=NULL;
     int i=0;
     for(i=0;i<rn;i++) {
     	svalue=prcpln.GetInt("prcplnid",i);
     	int p=wociSearch(qtje,search);
     	if(p>=0)
     	 *prcpln.PtrDouble("qtje",i)=qtje.GetDouble("prcplncmptid",p);
     }
     AutoMt cred(ca30,500000);
     cred.FetchAll("select * from tab_subscrb_grade_200703 where cgrade in ('A','B') ");
     int rn1=cred.Wait();
     for(i=0;i<rn1;i++) {
     	svalue=cred.GetInt("subscrbid",i);
     	int p=wociSearch(prcpln,search);
     	if(p>=0) 
     		*cred.PtrDouble("qtje",i)=prcpln.GetDouble("qtje",p);
     }
     //结果数据存到文本文件
     printf("Write to text file '%s'...\n",OUTFILENAME);
     wociSetMTName(cred,"停机额度");
     unlink(OUTFILENAME);
     wociMTToTextFile(cred,OUTFILENAME,0,NULL);
     mt_1.Stop();
     lgprintf("运行时间%f秒 正常结束",mt_1.GetTime());
 }

