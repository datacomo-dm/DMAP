#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define BUFLEN 1021*101
int main()
{
   int file=open("/root/ora.tar.gz",O_RDONLY |O_DIRECT);
   //int file=open("/dbsturbo/dtdata/cas/cdr/gsm/1_2000029_301276.dat",O_RDONLY );
   char *p=new char[BUFLEN];
   while(true) {
    ssize_t rdd=read(file,p,BUFLEN); 
    if(rdd!=BUFLEN) break;
   }
   printf("Read file end.\n");
   return 1;
}
