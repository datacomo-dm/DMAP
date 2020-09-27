#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void OutputPack(const char *buf) {
  long file=((long)(*((int *)buf))<<32)+*((int *)(buf+4));
  printf("-------------------------------\n"
         "pack_file:%d  ,  pack_addr:%d (P%lX-%X) \n"
         "local_min : %ld (%lx) , local_max: %ld(%lx) , sum :%ld (%lx) \n"
         "no_obj :%u , no_null :%u. \n\n",
               *((int *)buf),*((int *)(buf+4)),file>>12,file&0xfff,
               *((long *)(buf+8)),*((long *)(buf+8)),*((long *)(buf+16)),*((long *)(buf+16)),*((long *)(buf+24)),*((long *)(buf+24)),
               *((unsigned short *)(buf+32)),*((unsigned short *)(buf+34))
         );

}

int main(int argc,char ** argv) {
   if(argc!=3) {
     printf("Usage:\n dpninfo <filename> <dpnsection>.\ndpnsection: a range : 0-10 or 10-(10 to end) or -20 (begin to 20) or 20 (only 20th) or P<riakkey>.\n");
     return 0;
   }
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
   int maxpack=fsize/37-1;
   if(maxpack<1) {
     printf("Empty file.\n");
     return -3;
   }
   long seekfile=-1,seekaddr=-1;
   int startp=0,endp=maxpack-1;
   if(argv[2][0]=='p' ||argv[2][0]=='P') {
    //seek by riak-key
       sscanf(argv[2],"P%lX-%lX",&seekfile,&seekaddr);
       seekaddr+=(seekfile<<12);
       seekfile=(seekaddr>>32);
       seekaddr&=0xffffffff;
   }
   else if(strchr(argv[2],'-')!=NULL) {
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
   if(endp>maxpack-1) endp=maxpack-1;
   if(endp<0) endp=0;
   fseek(fp,0,SEEK_SET);
   char buf[100];
   fread(buf,37,1,fp);
   if(seekfile==-1) {
	printf("LastPack(0):\n");
   	OutputPack(buf);
   }
   fread(buf,37,1,fp);
   if(seekfile==-1) {
   	printf("LastPack(1):\n");
   	OutputPack(buf);
   }
   long startobj=1;
   if(startp==endp) {
   	//scan all pack's no_obj and sum it
   	for(int i=0;i<startp;i++) {
     fread(buf,37,1,fp);
     startobj+=(*((unsigned short *)(buf+32))+1);
   	}
   	printf("Start object of this pack:%ld.\n",startobj);
   }
   else fseek(fp,(startp+2)*37,SEEK_SET);

   while(startp<=endp) {
     fread(buf,37,1,fp);
     if(*((int *)buf)>=0) {
       if(seekfile!=-1) {
        if(*((int *)buf)!=seekfile || *((int *)(buf+4))!=seekaddr)
        { 
          startp++;   
	  continue;
        }
       }
       printf("Pack %d(offset :%x) :\n",startp,(startp+2)*37);
       OutputPack(buf);
     }
     startp++;
   }
   return 1;
}
