
#include <unistd.h>
#include <stdio.h>
#include <string.h>
enum DTIO_STREAM_TYPE {
	FULL_BACKUP,DTP_BIND,DTP_DETATCH
};

int main() {
 #ifdef __linux
 printf("Symbol LINUX is defined.\n");
 #endif
 double dv=12399.001;
 unsigned char *pdv=(unsigned char *)&dv; 
 printf("sizeof off_t:%d\n",sizeof(off_t));
 printf("sizeof log log :%d\n",sizeof(long long));
 printf("sizeof size_t:%d\n",sizeof(size_t));
 printf("Float:%.*f\n",2,12399.001); 
 printf("str:%-10s,int %-6d\n","abcd",1234);
 printf("str:%-10s,strlen%d.\n","测试长度",strlen("测试长度"));
 printf("%-10d   %d%%.\r",12333,12);
printf("%-10d   %d%%.\r",22333,32);
printf("%-10d   %d%%.\n",32333,42);
printf("%02x,%02x,%02x,%02x,%02x,%02x,%02x,%02x.\n",pdv[0],pdv[1],pdv[2],pdv[3]
,pdv[4],pdv[5],pdv[6],pdv[7]);
 printf("sizeof enum :%d.\n",sizeof(DTIO_STREAM_TYPE)); 
 
}

