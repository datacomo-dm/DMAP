#include <stdio.h>
#include <stdlib.h>
#include <string.h>

double getfreecpu(int &cpuct) {
        FILE *fp=fopen("/proc/stat","rt");
        char *line=NULL;
        size_t len=0;
        int user=1,nice=0,system=0,idle=0,iowait=0,irq=0,softirq=0;
        cpuct=0;
        while(getline(&line,&len,fp)!=-1) {
                if(strncmp(line,"cpu ",4)==0) {
                        sscanf(line+5,"%d %d %d %d %d %d %d",&user,&nice,&system,&idle,&iowait,&irq,&softirq);
                }
                else if(strncmp(line,"cpu",3)==0) cpuct++;
                else break;
        }
        if(cpuct==0) cpuct==1;
        if(line) free(line);
        fclose(fp);
        return (double)cpuct*idle/(user+nice+system+idle+iowait+irq+softirq);
}

int main() {

   int cpuct=0;
   double freec=getfreecpu(cpuct);
   printf("%d cpus detected. %.3f idle.\n",cpuct,freec);
   return 0;
}


