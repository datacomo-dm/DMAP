#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <sys/mount.h>
#include <unistd.h> 
#include <sys/stat.h>
#include <sys/vfs.h>
struct abc {
 int m1;
};
#define MEMALLOCSIZE 2000*1024*1024
int main()
{
   char a[]="123456789",b[]="456";
   printf("Sizeof abc %d.\n",sizeof(abc));
   char *pbf=(char *)malloc(MEMALLOCSIZE);
   if(pbf) {
      free(pbf);
      printf("alloc %d bytes succ.\n",MEMALLOCSIZE);
   }
   else printf("alloc %d bytes failed,errno:%d.\n",MEMALLOCSIZE,errno);
   return 1;
}
