#include <stdarg.h>
#include <stdio.h>
int listva(int i,...) {
 va_list va;
 int b1;
 int *p[10];
 int b2;
 int a,b,c;
 a=1322,b=2311,c=3122;
 p[0]=&a;p[1]=&b;p[2]=&c;
 va_start(va,b2);
 int *pa=va_arg(va,int *);
 printf("value: %d.\n",*pa);
 pa=va_arg(va,int *);
 printf("value1: %d.\n",*pa);

 va_end(va);
}
int main(int argc,char **argv) {
  listva(3,1,2,3);
  printf("hello.\n");
};
