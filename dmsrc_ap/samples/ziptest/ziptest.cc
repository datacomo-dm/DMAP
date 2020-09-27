#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <sys/types.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "zlib.h"
#include "ucl.h"
#include "ucl_asm.h"
#include "lzo1x.h"
#include "lzo2a.h"
#include "lzo_asm.h"
#include "bzlib.h"
#include "dt_svrlib.h"
#include "snappy.h"
#include "lzma.h"
int Start(void *ptr);
int
decompress_lzma_raw(const std::string &src, std::string &dst)
{
    int ret = 0;
    uint8_t outbuf[4096];
    lzma_stream strm = LZMA_STREAM_INIT;

    strm.next_in = (uint8_t *)src.data();
    strm.avail_in = src.size();

    ret = lzma_stream_decoder (&strm, 99 * 1024 * 1024, LZMA_CONCATENATED);
    //ret = lzma_alone_decoder (&strm, 99 * 1024 * 1024);
    if (ret == LZMA_OK) {
        dst.clear();
        strm.next_out = outbuf;
        strm.avail_out = sizeof outbuf;
        while ((strm.avail_in != 0) && (ret == LZMA_OK)) {
            ret = lzma_code (&strm, LZMA_FINISH);
	    if(ret!=LZMA_OK && ret!=LZMA_STREAM_END) break;
            dst.append((char *)outbuf, sizeof (outbuf) - strm.avail_out);
            strm.next_out = outbuf;
            strm.avail_out = sizeof outbuf;
            if (ret == LZMA_STREAM_END) {
                ret = 0;
                break;
            }
        }
    }

    lzma_end(&strm);

    return dst.size()>0?0:1;
}

void testdecode(int xoriv,int type) {
        FILE *fp =fopen("/tmp/ucweb.bin","rb");
        char buf[3000];
        int rd=fread(buf,1,3000,fp);
        printf("read %d bytes from file.\n",rd);
	for(int rv=0;rv<rd;rv++) {
	 buf[rv]=buf[rv]^xoriv;
	}
        int rt=0;
        unsigned long dstlen2=6000;
        char buf2[6000];
	int failedct=0;
	for(int off=0;off<min(rd/2,20);off++) {
	for(int len=max(rd-off-8,4);len<rd-off+1;len++) {
  	  if(type==1)
		rt=lzo2a_decompress_safe((const unsigned char*)buf+off,len,(unsigned char *)buf2,&dstlen2,NULL);
	  if(type==2)
                rt=lzo1x_decompress_safe((const unsigned char*)buf+off,len,(unsigned char *)buf2,&dstlen2,NULL);
	  if(type==3)
		rt=snappy::IsValidCompressedBuffer((const char *)buf+off,len)?0:1;
	  if(type==4)
                rt=uncompress((Bytef *)buf2,(uLongf*)&dstlen2,(Bytef *)buf+off,len);
	  if(type==5) {
		std::string srcstring,dststring;
		srcstring.assign(buf+off,len);
		rt=decompress_lzma_raw(srcstring,dststring);
	  }
	  if(rt==0) {
		printf("decode return %d and got %d bytes from %d input bytes.\n",rt,(int)dstlen2,len);
	  }
	  else
	    failedct++;
	}
	}
	printf(" decompress failed %d times.\n",failedct);
}


int main(int argc,char *argv[]) {
       try{
      /*testdecode(0x00,1);
      testdecode(0x00,2);
      testdecode(0x00,3);
      testdecode(0x00,4);
      */
      testdecode(0x00,5);

      /*testdecode(0xff,1);
      testdecode(0x55,1);
      testdecode(0xaa,1);
      testdecode(0xff,2);
      testdecode(0x55,2);
      testdecode(0xaa,2);
      testdecode(0xff,3);
      testdecode(0x55,3);
      testdecode(0xaa,3);
      testdecode(0xff,4);
      testdecode(0x55,4);
      testdecode(0xaa,4);
      */
      testdecode(0xff,5);
      testdecode(0x55,5);
      testdecode(0xaa,5);
    }
    catch(...) {};
	return 0;
    int nRetCode = 0;
    WOCIInit("samp/ziptest/");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

void TestCompress(AutoMt &mt,const char *fn,int cmpflag) ;

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("连接到数据库...\n");
    dts.SetHandle(BuildConn(0));
    char sql[10000];
    while(true) {
     sql[0]=0;
     getString("ZIPTST_SQL",NULL,sql);
     if(sql[strlen(sql)-1]==';') sql[strlen(sql)-1]=0;
     if(strcmp(sql,"quit")==0 || strcmp(sql,"exit")==0) return 0;
     try {
     AutoMt mt(dts,2);
     mt.FetchFirst(sql);
     int rn=mt.Wait();
     int l=wociGetRowLen(mt);
     printf("单行记录长度%d字节.\n",l);
     int meml=getOption("测试数据块大小(KB)<500>",500,20,2000);
     mt.SetMaxRows(meml*1024/l);
     mt.FetchFirst(sql);
     rn=mt.Wait();
     if(rn*l<20000) {
     	printf("数据量太小,至少要20KB!");
     	continue;
     }
     printf("记录数:%d.\n",rn);
     int sel=getOption("\n1.普通压缩(类型1).\n2.快速压缩(类型5).\n3.中密压缩(类型8).\n4.深度压缩(类型10).\n5.全部\n   选择压缩类型(<0>)",5,1,5);
     printf("x: MB/s\n");
     printf("类型 格式\t字节数\t比率\t压缩x\t解压x\t重组x\t综合x\tDOx\t重组时间比例\n");
     switch(sel) {
     case 5:
     	TestCompress(mt,"test.dat",5);
        TestCompress(mt,"test.dat",1);
     	TestCompress(mt,"test.dat",8);
     	TestCompress(mt,"test.dat",10);
     	break;
     case 1:
     	TestCompress(mt,"test.dat",1);
	break;     
     case 2:
     	TestCompress(mt,"test.dat",5);
	break;     
     case 3:
     	TestCompress(mt,"test.dat",8);
	break;     
     case 4:
     	TestCompress(mt,"test.dat",10);
	break;     
     }		
	}
	catch(...) {}
   }     
    return 0;
}

//类型	格式	字节数	比率	压MB/s	解MB/s	重组MB/s	综合MB/s	DiskOut(MB/s)	重组时间比例

void TestCompress(AutoMt &mt,const char *fn,int cmpflag) 
{
	mytimer mtm;
	int reptm=10;
	//if(cmpflag!=10) reptm=100;
	//lgprintf("---------Test Compress ----------------");
	//lgprintf("maxrows:%d,cmpflag:%d,repeat %d times.",
	//   maxrows,cmpflag,reptm);
	//AutoMt mt(dts,maxrows);
	//mt.FetchAll(sqlst);
	int rl=wociGetRowLen(mt);
	int rn=mt.GetRows();
	int len=rn*rl;
	//printf("\n---压缩类型:%d---原始字节数:%d---\n",cmpflag,len);
	//printf("\n压缩类型\t压缩字节数\t比率\n压缩(MB/s)\t---\n",cmpflag,len);
	//lgprintf("End of fetch,get %d rows,row len:%d,tot bytes:%d",
	//	rn,rl,rn*rl);
	double tm1=0;
        int mt1;
        char *resptr=NULL;
	file_mt df;
        df.Open("dt_file_my.dat",1);
        int off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMt(mt,cmpflag)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        mtm.Stop();
        printf("%2d  原始\t%d\t%.2f\t%.1f\t%.1f\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,len/mtm.GetTime()/1024/1024);
        //printf("未转换格式压缩为%d字节,比率%.2f,压缩%.1fMB/s,解压缩%.1fMB/s.\n",off,(double)len/off,len/tm1/1024/1024,len/mtm.GetTime()/1024/1024);
        df.Close();
        df.Open("dt_file_my.dat",1);
        off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMySQLMt(mt,cmpflag)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        char rowbuf[1024*2048];
        mtm.Stop();
        double tm2=mtm.GetTime();
        mtm.Start();
        int mlen=df.GetMySQLRowLen();
        int trn=df.GetBlockRowNum();
        //for(int pi=0;pi<trn;pi++)
        //	df.rebuildrow(resptr,rowbuf,trn,pi);
        df.rebuildblock(resptr,rowbuf,trn);
        //校验数据最后一行，置于开始：
        memcpy(rowbuf,rowbuf+trn*mlen-mlen,mlen);
        mtm.Stop();
        printf("%2d  按列\t%d\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f%%\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,
        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);

        //printf("按列存储压缩为%d字节,比率%.2f.\n"
        //       "  压缩%.1fMB/s,解压%.1fMB/s,重组%.1fMB,综合%.1fMB(压缩后%.1fMB) (重组时间占%%%.1f).\n",
       // 	off,(double)len/off,len/tm1/1024/1024,
       // 	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
        df.Close();
        df.Open("dt_file_my.dat",1);
        off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMySQLMt(mt,cmpflag,false)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        mtm.Stop();
        tm2=mtm.GetTime();
        mtm.Start();
        char rowbuf2[30000];
        for(int pi=0;pi<trn;pi++,resptr+=mlen)
        	memcpy(rowbuf2,resptr,mlen);
        mtm.Stop();
        if(memcmp(rowbuf2,rowbuf,mlen)!=0) 
        	printf("\n\n block no match.\n\n");
        printf("%2d  按行\t%d\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f%%\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,
        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
//        printf("按行存储压缩为%d字节,比率%.2f.\n"
//               "  压缩%.1fMB/s,解压%.1fMB/s,重组%.1fMB,综合%.1fMB(压缩后%.1fMB) (重组时间占%%%.1f).\n",
//        	off,(double)len/off,len/tm1/1024/1024,
//        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
//        printf("row order (%d),%.2f.\n",off,(double)len/off);
        return;
	char *psrc,*pdst;
	psrc=new char [len+1000];
	wociExportSomeRows(mt,psrc,0,rn);
	pdst=new char [len+1000];
	int fhl=0,fl=0;
	mtm.Clear();
	mtm.Start();
	int i;
	char *pwrkmem=NULL;
	int len2=len;
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len2;
			if(cmpflag==10) {
				unsigned int dstlen2=len2;
				rt=BZ2_bzBuffToBuffCompress(pdst,&dstlen2,psrc,len,1,0,0);
				dstlen=dstlen2;
			}			
			/******* lzo compress ****/
			else if(cmpflag==5) {
				if(!pwrkmem)  {
					pwrkmem = 
					new char[LZO1X_MEM_COMPRESS+204800];
                                        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+204800);
                                }
                                unsigned long dstlen2=len2; 
				rt=lzo1x_1_compress((const unsigned char*)psrc,len,(unsigned char *)pdst,&dstlen2,pwrkmem);
				dstlen=dstlen2;
			}
			/*** zlib compress ***/
			else if(cmpflag==8) {
				rt = ucl_nrv2d_99_compress((Bytef *)psrc,len,(Bytef *)pdst, (unsigned int *)&dstlen,NULL,5,NULL,NULL);
			}
		    	/*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=compress2((Bytef *)pdst,&dstlen,(Bytef *)psrc,len,1);
			}
			else 
				ThrowWith("Invalid compress flag %d",compress);
		    	if(rt!=Z_OK) {
				ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
			}
			fhl=0;
			fl=dstlen;
	}
	mtm.Stop();
	lgprintf("Write file,%d->%d,ratio %5.2f,%7.3f Sec,%7.1fK/s.",rl*rn,fl-fhl,(double)(rl*rn)/(fl-fhl),mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of file writing, compress to %d bytes,file len:%d,file header len :%d.",fl-fhl,fl,fhl);
	mtm.Clear();
	mtm.Start();
	
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len;
			/****************************/
			//bzlib2
			if(cmpflag==10) {
				unsigned int dstlen2=dstlen;
				rt=BZ2_bzBuffToBuffDecompress(psrc,&dstlen2,pdst,fl,0,0);
				dstlen=dstlen2;
			}			
			/******* lzo compress ****/
			else if(cmpflag==5) {
				unsigned long dstlen2=dstlen;
				//rt=lzo1x_decompress_asm_fast((unsigned char*)pdst,fl,(unsigned char *)psrc,&dstlen2,NULL);
				rt=lzo1x_decompress((unsigned char*)pdst,fl,(unsigned char *)psrc,&dstlen2,NULL);
				dstlen=dstlen2;
			}
			/*** zlib compress ***/
			else if(cmpflag==8) {
				//rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pdst,fl,(Bytef *)psrc,(unsigned int *)&dstlen,NULL);
				rt = ucl_nrv2d_decompress_8((Bytef *)pdst,fl,(Bytef *)psrc,(unsigned int *)&dstlen,NULL);
				//ucl_nrv2d_decompress_asm_fast_8
			}
		        /*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=uncompress((Bytef *)psrc,&dstlen,(Bytef *)pdst,fl);
			}
			else 
				ThrowWith("Invalid uncompress flag %d",cmpflag);
		    	if(rt!=Z_OK) {
				ThrowWith("Decompress failed,datalen:%d,compress flag%d,errcode:%d",
					fl,cmpflag,rt);
			}
	}	
	mtm.Stop();
	lgprintf("Read file,%7.3f Sec,%7.1fK/s.",mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of read file .");
	return;
}
	
