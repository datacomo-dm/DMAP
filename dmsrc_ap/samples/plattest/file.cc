#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

 int main() {

  FILE *fp =fopen("test.dat","w+b");
  char tmp[1000*1000];
  int l=fwrite(tmp,1000*1000,1,fp);
  printf("write %d bytes,error%d,errno:%d.\n",l,ferror(fp),errno);
  fgets(tmp,10,stdin);
  fwrite(tmp,1000*1000,1,fp);
  fclose(fp);
  return 0;
}

