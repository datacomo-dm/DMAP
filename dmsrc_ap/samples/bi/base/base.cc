 #include "AutoHandle.h"
 int Start(void *ptr);
 int main() {
     WOCIInit("stat/exec/base");
     int nRetCode=wociMainEntrance(Start,true,NULL,2);
     WOCIQuit(); 
     return nRetCode;
 }
 int Start(void *ptr) { 
     /*
     wociSetOutputToConsole(TRUE);
     AutoHandle ds(wociCreateSession("root","dbplus03","dp",DTDBTYPE_ODBC));
     AutoMt db(ds,1000);
     db.FetchAll("show databases");
     db.Wait();
     wociMTPrint(db,0,NULL);
     */
     char psql[300];
     strcpy(psql,"select a,b,a.b from a,b where a='xx' a.b='a.b' andc='xx'");
     char *prefsrctbn="b";
     char *prefsrcowner="a";
     char *tsrctbn="aa";
     char *tsrcowner="bb";
     char tmp[300];
     char tsrcfull[300],treffull[300];
     sprintf(tsrcfull,"%s.%s",tsrcowner,tsrctbn);
     sprintf(treffull,"%s.%s",prefsrcowner,prefsrctbn);
     strcpy(tmp,psql);
     if(strcmp(prefsrctbn,tsrctbn)!=0 || strcmp(prefsrcowner,tsrcowner)!=0) {
  	        char extsql[5000];
		char *sch=strstr(tmp," from ");
		char *schx=strstr(tmp," where ");
		if(schx) *schx=0;
		if(sch) {
		   sch+=6;
		   strncpy(extsql,psql,sch-tmp);
		   extsql[sch-tmp]=0;
		   bool fullsrc=true;
		   int tablen=strlen(treffull);
		   char *sch2=strstr(sch,treffull);
		   if(sch2==NULL) {
		   	sch2=strstr(sch,prefsrctbn);
		   	fullsrc=false;
		   	// other database name than prefsrcowner
			tablen=strlen(prefsrctbn);
		   }
		   if(sch2) {
		   	// any chars between 'from' and tabname ?
		        strncpy(extsql+strlen(extsql),psql+(sch-tmp),sch2-sch);
		        // replace new tabname
		        strcat(extsql,tsrcfull);
		        //padding last part 
		        strcat(extsql,psql+(sch2-tmp)+tablen);
		        strcpy(psql,extsql);
		      }
		    }
		  }
		printf("->%s\n",psql);
     return 0;
     /*
		char curdate[10];
		char mthvalue[20];
		wociGetCurDateTime(curdate);
		int mth=wociGetMonth(curdate);
		int pmth=mth==1?12:(mth-1);
		int nmth=mth==12?1:(mth+1);
		printf("curr_date:%d\n",wociGetYear(curdate)*10000+mth*100+wociGetDay(curdate));
		printf("this_yearmonth:%d\n",wociGetYear(curdate)*100+mth);
		printf("prev_yearmonth:%d\n",mth==1?((wociGetYear(curdate)-1)*100+12):(wociGetYear(curdate)*100+mth-1));
		printf("next_yearmonth:%d\n",mth==12?((wociGetYear(curdate)+1)*100+1):(wociGetYear(curdate)*100+mth+1));
		printf("this_month:%d\n",mth);
		printf("prev_month:%d\n",pmth);
		printf("next_month:%d\n",nmth);
		printf("this_month_m6:%d\n",(mth+5)%6+1);
		printf("prev_month_m6:%d\n",(pmth+5)%6+1);
		printf("next_month_m6:%d\n",(nmth+5)%6+1);
		printf("this_month_m4:%d\n",(mth+3)%4+1);
		printf("prev_month_m4:%d\n",(pmth+3)%4+1);
		printf("next_month_m4:%d\n",(nmth+3)%4+1);
		printf("this_month_m2:%d\n",mth%2);
		printf("prev_month_m2:%d\n",pmth%2);
		printf("next_month_m2:%d\n",nmth%2);
    */
 }

