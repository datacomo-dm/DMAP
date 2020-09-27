#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "bloom.h"
#include "bloom_test.h"
#include "AutoHandle.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "hash.h"
int Start(void *ptr);
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("bloomtest");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetTraceFile("bftest/fetch");
    wociSetEcho(TRUE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    dts.SetHandle(wociCreateSession("root","dbplus03","dp",DTDBTYPE_ODBC));
    
    mytimer mtm;
    mtm.Start();
    AutoMt mt(dts,1000000);
    int totrn=0;
    mt.FetchFirst("select distinct imei from zkb.tab_20120524_dma1 limit 65536");
    int rn=mt.Wait();
    totrn+=rn;
    mtm.Stop();
    lgprintf("Fetched %d rows in %.2f sec.\n",totrn,mtm.GetTime());
    bloom blf(32*1024*8,3,RSHash,JSHash,APHash);
    char *pbuf;
    int clen;
    mt.GetStrAddr(0,0,&pbuf,&clen);
    int i;
    for(i=0;i<rn;i++) {
    	blf.add(pbuf,strlen(pbuf));pbuf+=clen;
    }
    blf.write("bf.data");
    mt.GetStrAddr(0,0,&pbuf,&clen);   
    for(i=0;i<rn;i++) {
    	if(!blf.check(pbuf,strlen(pbuf)))
    		throw "bad buffer!!";
    	pbuf+=clen;
    }
    
    // use next different rows to test:
    mtm.Restart();
    mt.FetchFirst("select distinct imei from zkb.tab_20120524_dma1 limit 65536,65536");
    rn=mt.Wait();
    mtm.Stop();
    lgprintf("Fetched %d rows in %.2f sec.\n",totrn,mtm.GetTime());
    int failct=0;
    mt.GetStrAddr(0,0,&pbuf,&clen);
    for(i=0;i<rn;i++) {
    	if(blf.check(pbuf,strlen(pbuf)))
    		failct++;	
    	pbuf+=clen;
    }
    lgprintf("Failed %d,frate %.4f%%.\n",failct,(double)failct*100/rn);
    // use next different rows to test:
    mtm.Restart();
    mt.FetchFirst("select distinct imei from zkb.tab_20120524_dma1 limit 196608,65536");
    rn=mt.Wait();
    mtm.Stop();
    lgprintf("Fetched %d rows in %.2f sec.\n",totrn,mtm.GetTime());
    failct=0;
    mt.GetStrAddr(0,0,&pbuf,&clen);
    for(i=0;i<rn;i++) {
    	if(blf.check(pbuf,strlen(pbuf)))
    		failct++;	
    	pbuf+=clen;
    }
    lgprintf("Failed %d,frate %.4f%%.\n",failct,(double)failct*100/rn);
    
    lgprintf("---------------combination test add by liujs----------------------\n");

    char *paddbuf,*pcheckbuf;
        
    AutoMt mt1(dts,65536*5);
    mt1.FetchFirst("select distinct imei from zkb.tab_20120524_dma1 limit 0,327680");
    mt1.Wait(); 
    mt1.GetStrAddr(0,0,&paddbuf,&clen);
    
    AutoMt mt2(dts,65536*5);
    mt2.FetchFirst("select distinct imei from zkb.tab_20120524_dma1 limit 327681,327680");
    rn = mt2.Wait(); 
    mt2.GetStrAddr(0,0,&pcheckbuf,&clen);
    
    int ComNum = 3;
    bloom_test oblf(32*1024*8);
	#if 1
    lgprintf("1> --------- find same \n");
    rn = 65535;
    oblf.CombinationTest(paddbuf,clen,rn,paddbuf,clen,rn,ComNum);

    lgprintf("1> --------- find diff \n");
    rn = 65535;
    oblf.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
    return 1;
    #endif
    
    #if 0  // 32KB-256KB缓存对64KB数据Hash测试
    rn = 65535;
    lgprintf("2> ---------32KB find diff \n");
    for(i=2;i<7;i++)
    {
    	ComNum = i;
        oblf.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}
		
    lgprintf("3> ---------64KB find diff \n");
	bloom_test oblf_2(64*1024*8);
    for(i=2;i<7;i++)
    {
    	ComNum = i;
        oblf_2.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}	
	
    lgprintf("4> ---------128KB find diff \n");
	bloom_test oblf_3(128*1024*8);
    for(i=2;i<7;i++)
    {
    	ComNum = i;
        oblf_3.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}		
	
	lgprintf("5> ---------256KB find diff \n");
	bloom_test oblf_4(256*1024*8);
    for(i=2;i<7;i++)
    {
    	ComNum = i;
        oblf_4.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}	
	#endif
	
	#if 0
	// 128KB缓存对128K数据进行测试
	lgprintf("6> ---------128KB find 128K diff \n");
    rn = 65535*2; // 128K
	bloom_test oblf_5(128*1024*8);
    for(i=2;i<6;i++)
    {
    	ComNum = i;
        oblf_5.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}	

	// 256KB缓存对256K数据进行测试
	lgprintf("7> ---------256KB find 256K diff \n");
    rn = 65535*4;
	bloom_test oblf_6(256*1024*8);
    for(i=2;i<6;i++)
    {
    	ComNum = i;
        oblf_6.CombinationTest(paddbuf,clen,rn,pcheckbuf,clen,rn,ComNum);
	}	
	#endif
	
	int sourceRow,findRow;
	#if 0
	// 64K记录Set 128KB 缓存，对128K记录查找测试
	lgprintf("8> ---------64K记录Set 128KB 缓存，对128K记录查找测试 \n");
	bloom_test oblf_8(128*1024*8);
	sourceRow = 65536;
	findRow = 65536*2;
    for(i=2;i<6;i++)
    {
    	ComNum = i;
        oblf_8.CombinationTest(paddbuf,clen,sourceRow,pcheckbuf,clen,findRow,ComNum);
	}	

	// 64K记录Set 256KB 缓存，对256K记录查找测试
	lgprintf("9> ---------64K记录Set 256KB 缓存，对256K记录查找测试 \n");
	bloom_test oblf_9(256*1024*8);
	sourceRow = 65536;
	findRow = 65536*4;
    for(i=2;i<6;i++)
    {
    	ComNum = i;
        oblf_9.CombinationTest(paddbuf,clen,65536,pcheckbuf,clen,rn,ComNum);
	}	
	
	
	#endif
	
    return 1;
}
	
