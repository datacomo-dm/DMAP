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
#define p32 sizeof(void *)
struct abc {
 int m1;
#if p32=32
 int block;
#endif
};
int main()
{
   char a[]="123456789",b[]="456";
   printf("Sizeof abc %d.\n",sizeof(abc));
   return 1;
}
