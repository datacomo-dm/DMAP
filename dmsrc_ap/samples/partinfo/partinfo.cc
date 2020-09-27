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

int main(int argc,char ** argv) {
    if(argc!=2) {
      printf("Usage:\n partinfo <filename>.\n");
      return 0;
    }
    char *p_riak_version = getenv("RIAK_HOSTNAME");
    char filename[300];
    sprintf(filename,argv[1]);
    FILE *fp=fopen(filename,"rb");
    if(fp==NULL) {
      printf("Open file '%s' failed.\n",filename);
      return -1;
    }
    char buf[20];
    fread(buf,1,8,fp);
    bool version15=true;
    if(memcmp(buf,"PARTIF15",8)!=0) {
   	    if(memcmp(buf,"PARTINFO",8)!=0) {
     	    printf("文件格式错误:'%s'.\n",filename);
     	    fclose(fp);
     	    return -1;
        }
        version15=false;
    }
    int attrid;
    fread(&attrid,sizeof(int),1,fp);
    short parts;
    fread(&parts,sizeof(short),1,fp);
    printf("分区总数:%d.\n-----------------------\n",(int)parts);
    long total_objs=0;
    for(int i=0;i<parts;i++) {
    	fread(buf,1,8,fp);
    	if(memcmp(buf,"PARTPARA",8)!=0) {
    	    printf("%d分区格式错误:'%s'.\n",i+1,filename);
    	    fclose(fp);
    	    return -1;
        }
        short namelen;
        fread(&namelen,sizeof(short),1,fp);
        char partname[300];
        fread(partname,namelen,1,fp);
        partname[namelen]=0;
        int firstpack,lastpack,lastpackobj,lastsavepack,lastsavepackobj,savesessionid;
        int savepos=-1,lastsavepos=-1;     
        long lastfileid=0;
        fread(&firstpack,sizeof(uint),1,fp);
        fread(&lastpack,sizeof(uint),1,fp);
        fread(&lastpackobj,sizeof(uint),1,fp);
        fread(&lastsavepack,sizeof(uint),1,fp);
        fread(&lastsavepackobj,sizeof(uint),1,fp);
        fread(&savesessionid,sizeof(uint),1,fp);
        fread(&attrid,sizeof(uint),1,fp);
        if(version15) {
            if((p_riak_version == NULL) || (strlen(p_riak_version)==0)){
            	fread(&lastfileid,sizeof(int),1,fp);
            }else{
             	fread(&lastfileid,sizeof(long),1,fp);
            }
            fread(&savepos,sizeof(int),1,fp);
            fread(&lastsavepos,sizeof(int),1,fp);
        }
        int vectsize;
        fread(&vectsize,sizeof(uint),1,fp);
        printf("分区 '%s' :\n",partname);
        printf("     firstpack :%d\n",firstpack);
        printf("     lastpack :%d\n",lastpack);
        printf("     lastpackobj :%d\n",lastpackobj);
        printf("     lastsavepack :%d\n",lastsavepack);
        printf("     lastsavepackobj :%d\n",lastsavepackobj);
        printf("     number of files :%d\n",vectsize);
        printf("       file list: ");
        for(int j=0;j<vectsize;j++) {
        	if((p_riak_version == NULL) || (strlen(p_riak_version)==0)){
               int fileid=0;
               fread(&fileid,sizeof(int),1,fp);
               printf("%d%s",fileid,j+1==vectsize?"\n":",");
           }else{
               long fileid=0;
               fread(&fileid,sizeof(long),1,fp);
               printf("%ld%s",fileid,j+1==vectsize?"\n":",");        	
           }        	
        }
        if(version15) {
        	printf("     lastfileid :%ld\n",lastfileid);
        	printf("     savepos :%d\n",savepos);
        	printf("     lastsavepos :%d\n",lastsavepos);
        }
        int packsize;
        fread(&packsize,sizeof(uint),1,fp);
        printf("     Number of packlist:%d\n",packsize);
        printf("       packlist: ");
        long part_objs=0;
        for(int j=0;j<packsize;j++) {
        	int packstart=0,packend=0,lastobjs=0;
        	fread(&packstart,sizeof(uint),1,fp);
        	fread(&packend,sizeof(uint),1,fp);
        	fread(&lastobjs,sizeof(uint),1,fp);
        	printf("R:%d-%d,O:%d%s",packstart,packend,lastobjs,j+1==packsize?"\n":";");
            long list_objs=(packend-packstart)*65536+lastobjs;
        	total_objs+=list_objs;
        	part_objs+=list_objs;
        }
        printf("       PartObjs: %ld.\n",part_objs);
    }
    printf("Total objs:%ld.\n",total_objs);
    fclose(fp);
	 	 
   return 1;
}
