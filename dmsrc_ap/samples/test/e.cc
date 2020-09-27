
#include <unistd.h>
#include <stdio.h>
#include <string.h>


typedef unsigned long long uint8B;
typedef long long int8B;
typedef unsigned int uint4;
typedef int int4;
#define DTIO_VERSION 0x1010
#define MAXCONTFILE 20
#define DTIO_STREAMFLAG 0xa511
#define PATH_LEN 250 //120
int main()
{ 
#ifdef sunos
 printf("Symbol LINUX is defined.\n");
#endif
  long long xx=9000113223ll;
  printf("%-lld\n",xx);
}
