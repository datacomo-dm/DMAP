#include <stdlib.h>
#include <stdio.h>

#include "mt_worker.h"
#include "AutoHandle.h"
void ThreadWorker3s(ThreadList *tlist,void **params) {
	printf("Begin of worker3s.\n");
	sleep(3);
	printf("End of worker3s.\n");
	tlist->SetReturnValue(1);
}

void ThreadWorker5s(ThreadList *tlist,void **params) {
	printf("Begin of worker5s.\n");
	sleep(5);
	printf("End of worker5s.\n");
	tlist->SetReturnValue(1);
}

void TestThreadNormal() {
	ThreadList tl1(ThreadWorker3s),tl2(ThreadWorker5s);
	tl1.Start(1);
	tl2.Start(1);
	tl1.Wait();
	tl2.Wait();
}

int  Query1(void *ptr) {
	printf("Query1 Start...\n");
	AutoHandle dts(wociCreateSession("root","dbplus03","dp",DTDBTYPE_ODBC);
	AutoMt mt(dts,1000);
	mt.FetchAll("select * from dp.dp_table where tabid between 148880 and 148968");
	mt.Wait();
	printf("Query1 End...\n");
}

int Query2(void *ptr) {
	printf("Query2 Start...\n");
	AutoHandle dts(wociCreateSession("root","dbplus03","dp",DTDBTYPE_ODBC);
	AutoMt mt(dts,1000);
	mt.FetchAll("select * from dp.dp_table where tabid between 148880 and 148968");
	mt.Wait();
	printf("Query2 End...\n");
}

void QueryDBTest1(ThreadList *tlist,void **params) {
	int nRetCode=wociMainEntrance(Query1,true,NULL,2);
}

void QueryDBTest2(ThreadList *tlist,void **params) {
	int nRetCode=wociMainEntrance(Query2,true,NULL,2);
}

void TestThreadQuery() {
	ThreadList tl1(QueryDBTest1),tl2(QueryDBTest2);
	tl1.Start(1);
	tl2.Start(1);
	tl1.Wait();
	tl2.Wait();
}

int main() {
		printf("Test Normal Thread concurrent.\n");
	  TestThreadNormal();
	  int nRetCode = 0;
    WOCIInit("woci_thread");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
		printf("Test DBQuery Thread concurrent.\n");
    //nRetCode=wociMainEntrance(Start,true,NULL,2);
    TestThreadQuery();
    WOCIQuit(); 
	  return nRetCode;
}
