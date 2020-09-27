#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc,char **argv) {

 if(argc==1) {
   /*int rt=system(argv[1]);*/
   int rt=execlp("ls","ls","-lrt",NULL);
   printf("\n. the command returned %d.\n",rt);
   return 0; 
 }
 int rt=system("ls -lrt");
 wait(&rt);
 printf("\n command returned %d.\n",rt);
 return 0;
}
