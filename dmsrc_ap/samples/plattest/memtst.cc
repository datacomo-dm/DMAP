#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dt_common.h"
#define BUFLEN 1024*1024*10
#define TOMB(a) (a)/1024/1024
unsigned char *buf;
unsigned char *buf1;

void cptst(int bl) {
  mytimer tm;
  unsigned char *src=buf;
  unsigned char *dst=buf1;
  int ct=BUFLEN/bl;
  src=buf,dst=buf1;
  tm.Start();
// using while
  int wct=ct;
  int bct;
  while(wct--) {
	  bct=bl;
	  while(bct--);
	   *src++=*dst++;
  }
  tm.Stop();
  double wtm=tm.GetTime();  

  src=buf,dst=buf1;
  tm.Restart();
  int mct=ct;
  while(mct--) {
	 memcpy(dst,src,bl);
         dst+=bl;src+=bl;
  }
  tm.Stop();
  double mtm=tm.GetTime();

  src=buf,dst=buf1;
  tm.Restart();
  for(int fct=0;fct<ct;fct++) {
	  bct=bl;
	  for(bct=0;bct<bl;bct++)
	   *src++=*dst++;
  }
  tm.Stop();
  double ftm=tm.GetTime();  

  printf("%d\t%.1fMB/s\t:%.1fMB/s\t%.1fMB/s\n",bl,TOMB(BUFLEN/wtm),TOMB(BUFLEN/ftm),TOMB(BUFLEN/mtm));
}


int main()
{
  char *srcpath="/app/dbplus/var";///var/smo/smt_c1x.DTP";
  char realval[4250];
  realval[0]=0;
  realpath(srcpath,realval);
  if(realval[0]==0) printf("bad.\n");
  else printf("real %s.\n",realval);
  return 0; 
  printf("Test memory block copy.\n");
  printf("blocklen\twhile\tfor\tmemcpy\n");
  buf=new unsigned char[BUFLEN];
  buf1=new unsigned char[BUFLEN];
  int bl=1024*4;
  do {
	cptst(bl);
	bl/=2;
  }
  while(bl>0);
  delete []buf;
  delete []buf1;
  return 0;
}

