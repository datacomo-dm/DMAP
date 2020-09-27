#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <sys/mount.h>
#include <unistd.h> 
#include <linux/unistd.h>
#include <sys/stat.h>
#include <sys/vfs.h>

// #include <unistd.h>

//       char *getpass( const char * prompt );
int GetFreeM(const char *path) {
#ifdef WIN32
   char drive[_MAX_DRIVE];
   char dir[_MAX_DIR];
   char fname[_MAX_FNAME];
   char ext[_MAX_EXT];
   _diskfree_t dskinfo;
   _splitpath( path, drive, dir, fname, ext );
   _getdiskfree(drive[0]-'a'+1,&dskinfo);
   double freebytes=(double)dskinfo.avail_clusters*dskinfo.bytes_per_sector*dskinfo.sectors_per_cluster;
   return (int )(freebytes/(1024*1024));
#else
   struct statfs freefs;
   if(statfs(path,&freefs)==0)
     return (int)((double)freefs.f_bsize*freefs.f_bavail/(1024*1024));
   return 20480;
#endif
}
class abc {
  public :
   abc() {};
   ~abc() {printf("i will quit.\n");}
};

 abc GetABC() {return abc();}

_syscall0(pid_t,gettid)
int main()
{
   char a[]="123456789",b[]="456";
   abc myabc=GetABC();
   memcmp(a,b,5);
   strcmp(a,b);
   printf("\abcdef.\rABCDEF\n.");
   char *lst=(char *)memccpy(a,b,'6',5);
   if(lst) *--lst=' ';
   printf("a:%s,b:%s.\n",a,b);
   char str[100];
   //noecho();
   strcpy(str,getpass("Enter password:"));
   //echo();
   printf("\n Get :%s.\n",str);
   printf("Begin sleep.\n");
   struct timespec req,rem;
   req.tv_sec=0;
   req.tv_nsec=991000000;   
   nanosleep(&req, &rem);
   //usleep(300000);
   printf("Sleep end.\n");
   printf("PID :%d, TID:%d.\n",getpid(),gettid());
   printf("free :%dMB.",GetFreeM("/dbsturbo/dttemp/cas/"));
   return 1;
    char *p=new char[200000000];
   int ct=1;
   if(p!=NULL) {
    printf("allocate memory 2G  suc.\n");
    while (true) {
        memset(p,0,200000000);
    	printf("\nalloc %dM ... press q to exit or any other key to continue.",ct++*200);
    	int ch=getchar();
    	if(ch=='q') break;
        p=new char[200000000];
        if(!p) {
          printf("allocate failed.\n");
	  return 1;
	}
    }
    //memset(p,0,650000000);
   }
   else {
    printf("allocate memory 2G failed.\n"); 
   }
   printf("begin alloc another...\n");
   char *p2=new char[650000000];
   if(p2!=NULL) {
    printf("alloc another 560M suc.\n");
    memset(p2,0,650000000);
    //memset(p2,0,650000000);
   }
   else
    printf("alloc another 650M failed.\n");
   printf("begin alloc third...\n");
   char *p3=new char[650000000];
   if(p3!=NULL) {
    printf("alloc another 560M suc.\n");
    memset(p3,0,650000000);
    //memset(p3,0,650000000);
   }
   else
    printf("alloc third 650M failed.\n");
   if(p) delete [] p;
   if(p2) delete []p2;
   if(p3) delete []p3;
return 1;
}
