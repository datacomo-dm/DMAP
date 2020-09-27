#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc,char **argv) {
   if(argc!=2 && argc!=3) {
     printf("Usage: fixobjs <path> <newobjs>.\n <path>: column info file located.\n newobjs: optional;set new object number,list current value if ommitted.\n");
     return 1;
   }
   int colid=0;
   struct stat stat_info;
	 bool is_bswitch= (0 == stat("ab_switch", &stat_info));
	 long newobjs=0;
	 bool setnew=argc==3;
	 if(setnew)
	   newobjs=atol(argv[2]);
   while(true) {
      char fn[300];
      sprintf(fn,"%s/%s%05d.ctb",argv[1],is_bswitch?"TB":"TA",colid);
      FILE *fp=fopen(fn,setnew?"r+b":"rb");
      if(fp==NULL) {
        if(colid==0) {
        	printf("no column info file in this directory or no permits to operate.\n");
        	return 2;
        }
        break;
      }
      if(setnew) {
         fseek(fp,9,SEEK_SET);
         fwrite(&newobjs,1,sizeof(long),fp);
         fclose(fp);
      }
      else {
         long objs=0;
         fseek(fp,9,SEEK_SET);
         fread(&objs,1,sizeof(long),fp);
         printf("Column %d objs:%ld.\n",colid+1,objs);
         fclose(fp);
      }
      colid++;
   }
   return 1;
}


