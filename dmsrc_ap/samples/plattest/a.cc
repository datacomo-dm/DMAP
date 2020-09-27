#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <time.h>

 #include <unistd.h>
#include <math.h>
#include <sys/mount.h>
class mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=(ed.tv_sec-st.tv_sec);
	ct.tv_nsec+=(ed.tv_nsec-st.tv_nsec);
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};

class tst {
int a;
int b;
public :
 void seta(int _a) {a=_a;}
 void setb(int _b) {b=_b;}

};

void catchtest() {
  try {
    throw "Test123";
  }
  catch(...) {
    printf("I catch it\n");
    throw;
  }
}
 
int main()
{
   double val=0;
   char hstname[100];
   struct statfs freefs;
   printf("Begin sleep.\n");
   usleep(3000);
   printf("Sleep end.\n");
   statfs("/dbsturbo/dev/wgsh/dtsvr",&freefs);
   try {
     catchtest();
   }
   catch(char *str) {
     printf("Is it '%s' ?\n",str);
     throw; 
   }
   printf("block size:%d,free:%d blocks,free:%ld Kbytes.\n",freefs.f_fsize,freefs.f_bavail,freefs.f_fsize*freefs.f_bavail/1024);
   val=atof("-inf");
   printf("value :%10.3f, isnanf %d,finite:%d\n",val,isnan(val),finite(val));
   gethostname(hstname,100);
   printf(" processid :%d,hostname:%s.\n",getpid(),hstname);
   return 0;
printf("test\n");
   printf(".");
   fflush(stdout);
  
 mytimer mtm;
 mtm.Start(); 
 struct timespec st;
 clock_gettime(TIMEOFDAY,&st);
 printf("sec %d ,usec %d\n",st.tv_sec,st.tv_nsec);  
 printf("clock %7.4f \n",st.tv_sec+st.tv_nsec/1e9);


 sleep(3);
 mtm.Stop();
clock_gettime(TIMEOFDAY,&st);
 printf("sec %d ,usec %d\n",st.tv_sec,st.tv_nsec);
printf("clock %7.4f,tm:%7.4f \n",st.tv_sec+st.tv_nsec/1e9,mtm.GetTime());
sleep(6);
clock_gettime(TIMEOFDAY,&st);
 printf("sec %d ,usec %d\n",st.tv_sec,st.tv_nsec);

printf("clock %7.4f \n",st.tv_sec+st.tv_nsec/1e9);
printf("max :%d\n",max(10,20));
   return 1;
   char *p=new char[650000000];
   if(p!=NULL) {
    printf("allocate memory 650M suc.\n");
    while (true) {
    	memset(p,0,650000000);
    	printf("\npress q to exit or any other key to continue.");
    	int ch=getchar();
    	if(ch=='q') break;
    }
    //memset(p,0,650000000);
   }
   else {
    printf("allocate memory 650M failed.\n"); 
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
tst t;
t.seta(1);  
return 1;
}
