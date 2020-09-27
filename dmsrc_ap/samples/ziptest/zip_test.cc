#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "zlib.h"      // zip À„∑®

#ifndef BIGENDIAN        // ¥Û∂À–°∂À
#define _revDouble(V)
#define _revInt(V)
#define _revShort(V)
#else
#define _revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }
       
 #define _revInt(V)   { char def_temp[4];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			              memcpy(V,def_temp,sizeof(int)); }
			                       
 #define _revShort(V)  { char def_temp[2];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(short)); }
#endif

#define LEN 1024
int Start(void *ptr) 
{ 
	char pdst[LEN],psrc[LEN];
	int cmpflag = 1;
	long dstlen = LEN;
	long datalen = LEN/2;
	char psrc_copy[LEN];
    strcpy(psrc,"Hello Word");
    strcpy(psrc_copy,"Hello Word");
    datalen = strlen(psrc);
    printf("-----------------------------------\n");
    printf("srclen : %d,\nsrcdata:%s\n",datalen,psrc);
    int rt = compress2((Bytef *)pdst,(uLongf*)&dstlen,(Bytef *)psrc,datalen,cmpflag);    
    if(rt!=Z_OK) 
    {            
    	printf("***compress2 error return : %d \n",rt);
        return -1;
    }    
    long depressLen = dstlen;
    dstlen = LEN;
    rt=uncompress((Bytef *)psrc,(uLongf*)&dstlen,(Bytef *)pdst,depressLen);

    if(rt!=Z_OK) 
    {            
    	printf("***uncompress error return : %d \n",pthread_self(),rt);
        return -1;
    }     
    
    if(memcmp(psrc,psrc_copy,datalen)!=0)
    {
        printf("***software zlib memcmp(psrc,psrc_copy,datalen) != 0 \n");
        return -1;
    }
    
    printf("-----------------------------------\n");
    printf("dstlen : %d,\ndstdata is following:\n",depressLen);
    for(int j=0;j<depressLen;j++)
    {
       printf("%d ",pdst[j]);
       if(j%15 == 0 && j != 0) printf("\n");       
    }    
    printf("\n");
    printf("-----------------------------------\n");
    
    printf("compress/decompress test ok.\n");
    return 1;
}

int main(int argc,char *argv[])  
{             
    int nRetCode = 0;
    WOCIInit("zip_test");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false);
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

	