#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void OutputPack(const char *buf) {
  printf("-------------------------------\n"
         "pack_file:%d  ,  pack_addr:%d   \n"
         "local_min : %ld (%lx) , local_max: %ld(%lx) , sum :%ld (%lx) \n"
         "no_obj :%u , no_null :%u. \n\n",
               *((int *)buf),*((int *)(buf+4)),
               *((long *)(buf+8)),*((long *)(buf+8)),*((long *)(buf+16)),*((long *)(buf+16)),*((long *)(buf+24)),*((long *)(buf+24)),
               *((unsigned short *)(buf+32)),*((unsigned short *)(buf+34))
         );

}
#define PACKFILE(buf) *((int *)buf)
#define PACKOBJS(buf) *((unsigned short *)(buf+32))
#define PACKNULLS(buf) *((unsigned short *)(buf+34))
#define PACKADDR(buf) *((int *)(buf+4))
#define BUFLEN 1024*1024*20

class PackFile {
	int attr_number;
	int pack_file;
	FILE *fp;
	char *zipbuf,*data_full;
	uint *nulls;
	uint *lenbuf;
	bool writemode;
	uint no_obj,no_nulls;
	char compressmode;//C/S/I/L/s:char/short/int/long/string
public:
	PackFile(const char *dpnfile,int cmode,bool forwrite) {
		attr_number=atoi(dpnfile+2);
		pack_file=-1;
		zipbuf=new char[BUFLEN];
		data_full=new char[BUFLEN];
		nulls=new uint[2048];//2048*32  64k
		lenbuf=new uint[1024*64];
		compressmode=cmode;
		writemode=forwrite;
		fp=NULL;
	}
	
	template<typename etype> void DecompressAndInsertNulls(NumCompressor<etype> & nc, uint *& cur_buf)
	{
    nc.Decompress(data_full, (char*)((cur_buf + 3)), *cur_buf, no_obj - no_nulls, (etype) * (_uint64*)((cur_buf + 1)));
    etype *d = ((etype*)(data_full)) + no_obj - 1;
    etype *s = ((etype*)(data_full)) + no_obj - no_nulls - 1;
    for(int i = no_obj - 1;d > s;i--){
        if(IsNull(i))
            --d;
        else
            *(d--) = *(s--);
    }
	}
	
	void Uncompress(uint no_obj,uint no_nulls) {
		// read head info
		char *cur_buf=zipbuf;
		// decode null
		this->no_obj=no_obj;
		this->no_nulls=no_nulls;
		unsigned short null_buf_size = (*(unsigned short*) cur_buf);
		assert(null_buf_size <= 8192);
		BitstreamCompressor bsc;
		bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
		uint nulls_counted = 0;
		for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
		assert(no_nulls==nulls_counted);
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
		if(compressmode == 'C') {
				NumCompressor<uchar> nc;
				DecompressAndInsertNulls(nc, cur_buf);
		} else if(compressmode == 'S') {
				NumCompressor<ushort> nc;
				DecompressAndInsertNulls(nc, cur_buf);
		} else if(compressmode == 'I') {
				NumCompressor<uint> nc;
				DecompressAndInsertNulls(nc, cur_buf);
		} else if(compressmode == 'L'){
				NumCompressor<_uint64> nc;
				DecompressAndInsertNulls(nc, cur_buf);
		} else if(compressmode == 's') {

		} else asert(0);
		
		
	}
	
	// return objs(not null)
	int Read(int offset,int len) {
		assert(len<BUFLEN);
		fseek(fp,offset,SEEK_SET);
		assert(fread(zipbuf,len,1,fp)==1);
		return zipbuf;
	}
	
	void SetPackFile(int pfile) {
		if(pfile==pack_file) 
			return;
		pack_file=pfile;
		char filename[300];
		sprintf(filename,"TA%05d%09d.ctb",attr_number,pack_file);
		if(fp!=NULL) 
			fclose(fp);
		fp=fopen(filename,writemode?"wb":"rb");
		if(fp==NULL)
			throw "Packfile open failed!";
	}
	
	~PackFile() {
		if(fp!=NULL)
			fclose(fp);
		delete [] lenbuf;
		delete [] nulls;
		delete [] data_full;
		delete [] zipbuf;
	}
};

int main(int argc,char ** argv) {
   if(argc!=5) {
     printf("Usage:\n "
     				"   createpindex <fullpath> <dpnsection> <packtype> <packindexdb name>.\n"
     				"dpnsection: a range : all or 0-10 or 10-(10 to end) or -20 (begin to 20) or 20 (only 20th).\n"
     				"packtype <C|S|I|L|S>:char short int long string.\n"
     				"packindexdb name: should contain column id and partition name.\n"
     				"    create pack index for a partition of a column.\n"
     				"    use partinfo to find partiton name and dpnsection first.");
     return 0;
   }
   // check dpn file :
   FILE *fp=fopen(argv[1],"rb");
   if(fp==NULL) {
     printf("Open file '%s' failed.\n",argv[1]);
     return -1;
   }
   fseek(fp,0,SEEK_END);
   long fsize=ftell(fp);
   if(fsize%37!=0) {
     printf("File '%s' size %ld,not a dpn file(multiby 37),mod 37 :%d.\n",argv[1],fsize,fsize%37);
     fclose(fp);
     return -2;
   }
   //only one pack? giveup
   int maxpack=fsize/37-2;
   if(maxpack<1) {
     printf("Empty file.\n");
     return -3;
   }
   //parse dpn section
   int startp=0,endp=maxpack-1;
   if(strchr(argv[2],'-')!=NULL) {
     int slen=strlen(argv[2]);
     char p[100];
     strcpy(p,argv[2]);
     if(p[0]=='-') endp=atoi(p+1);
     else if(p[slen-1]=='-') {
        p[slen-1]=0;
        startp=atoi(p);
     }
     else {
        char *ends=strchr(p,'-');
        *ends=0;ends++;
        startp=atoi(p);endp=atoi(ends);
     }
   }
   else if(strstr(argv[2],"all")==NULL)
       startp=endp=atoi(argv[2]);
   
   printf("DPN file :%s, total packs:%d.\n",argv[1],maxpack+1);
   if(startp>maxpack-1) startp=maxpack-1;
   if(startp<0) startp=0;
   //including last pack?
   bool lastpack=false;
   if(endp>maxpack-1) {
   	endp=maxpack-1;
   	lastpack=true;
   }
   if(endp<0) endp=0;
   fseek(fp,0,SEEK_SET);
   char buf[100],lastpackbuf[100];
   fread(buf,37,1,fp);
   //printf("LastPack(0):\n");
   //OutputPack(buf);
   fread(lastpackbuf,37,1,fp);
   //printf("LastPack(1):\n");
   //OutputPack(buf);
   fseek(fp,(startp+2)*37,SEEK_SET);

   while(startp<=endp) {
     fread(buf,37,1,fp);
     if(*((int *)buf)>=0) {
       printf("Pack %d(offset :%x) :\n",startp,(startp+2)*37);
       OutputPack(buf);
     }
     startp++;
   }
   return 1;
}
