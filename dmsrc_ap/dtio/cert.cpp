#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include "dt_common.h"
#include "dtio_common.h"
#ifdef WIN32
#include <sys/types.h>
#include <sys/stat.h>
#endif
#define CHARENC(a)  ((a)>25?('0'+(a)-26):('A'+(a)))
#define CHARDEC(a)  ((a)<'A'?((a)-'0'+26):((a)-'A'))
const char *GetDPLibVersion();
#define C_decode  mystrcpy01
#ifndef WIN32
//#include "zlib/zlib.h"
#endif
extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);
DbplusCert *DbplusCert::pInst=NULL;
void C_decode(char *strx) {
	unsigned char *str=(unsigned char *)strx;
	unsigned char cd[129];
	unsigned char str1[129];
	int i;
	memcpy(cd,str,128);
	cd[128]=0;
	//Decode 
	str1[62]=(CHARDEC(cd[126])<<4)+CHARDEC(cd[127]);
	for(i=0;i<62;i++) {
		int off=(str1[62]+i)%20;
		str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
		str1[i]^=str1[62];
	}
	//ver1
	//int pos[]={44,37,47,14,7,0,10,1,4,12,5,15,16,26,17,20,28,21,31,22,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	//       };
	int pos[]={44,37,47,14,17,20,28,21,31,22,7,0,10,1,4,12,5,15,16,26,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	         };
	for(i=0;i<62;i++) {
		str[pos[i]]=str1[i];
	}
	str[62]=0;
}

const char *DbplusCert::getUserTitle()
{
	return getLine("usertitle");
}
	// 1 for full,10 for std, 20 for eval.
int DbplusCert::getFunctionCode()
{
	const char *fc=getLine("functioncode");
	if(strcmp(fc,"FULL")==0) return 1;
	if(strcmp(fc,"STAND")==0) return 10;
	if(strcmp(fc,"EVAL")==0) return 20;
	ThrowWith("授权证书错误.");
	return 20;
}

const char *DbplusCert::getLogo()
{	
	return getLine("logo");
}

const char *DbplusCert::getCopyright()
{
	return getLine("copyright");
}

// y=0 for none expired date;
void DbplusCert::GetExpiredDate(int &y,int &m,int &d)
{
	const char *fc=getLine("expiredate");
	if(getFunctionCode()!=20 || strcmp(fc,"none")==0) {
		y=m=d=0;
		return;
	}
	char tmp[5];
	strncpy(tmp,fc,4);tmp[4]=0;
	y=atoi(tmp);
	strncpy(tmp,fc+4,2);tmp[2] = 0;
	m=atoi(tmp);
	strncpy(tmp,fc+4+2,2);tmp[2] = 0;
	d=atoi(tmp);
}

const char *DbplusCert::getProductId()
{
	return getLine("productid");
}

double DbplusCert::getRowLimit()
{
	const char *fc=getLine("rowlimit");
	double rl=atof(fc);
	if(rl<1) rl=0;
	return rl;
}
	
//versiontype 1 for core,2 for bs, 3 for admin 4,for library
const char *DbplusCert::getVersion(int versiontype)
{
	switch(versiontype) {
		case 1: return getLine("core_version");
		case 2:return getLine("bs_version");
		case 3:return getLine("admin_version");
		case 4:return getLine("lib_version");
	}
	return NULL;
}

DbplusCert::DbplusCert() {
	char tmp[300];
	strcpy(tmp,DBPLUS_STR);
	int l=strlen(tmp);
	for(int i=0;i<l;i++) tmp[i]=toupper(tmp[i]);
	strcat(tmp,"_HOME");
	const char *homepath=getenv(tmp);
	if(homepath==NULL) 
		ThrowWith("找不到" DBPLUS_STR "安装位置.");
	char fn[300];
#ifdef WIN32
	sprintf(fn,"%s\\cert.dat",homepath);
#else
	sprintf(fn,"%s/.cert.dat",homepath);
#endif
	FILE *fp=fopen(fn,"rb");
	if(fp==NULL) 
		ThrowWith("找不到授权证书.");
	fclose(fp);
	DecodeFromFile(fn);
}

const char *DbplusCert::getLine(const char *itemname)
{
	for(int i=0;i<lines;i++) {
		if(strncmp(buf+128*i,itemname,strlen(itemname))==0)
		{
			return buf+128*i+strlen(itemname)+1;//跳过等号
		}
	}
	ThrowWith("授权证书项目不符:%s.",itemname);
	return NULL;
}

void DbplusCert::printlogo()
{
	printf("%s.\n",getUserTitle());
	printf("--%s",getLogo());
	switch(getFunctionCode()) {
		case 1:printf("企业版 ");break;
		case 10:printf("标准版 ");break;
		case 20: printf("试用版 ");break;
	}
	printf(" 版本%s%s.\n",GetDPLibVersion(),sizeof(long)==8?"(64位)":"(32位)");
	printf("    %s.\n",getCopyright());
}
	
int DbplusCert::DecodeFromFile(const char *fn)
{
    int crco=0,crcc=0;
    buf[0]=0;
    struct stat fs;
    stat(fn,&fs);
    int len=fs.st_size;
    FILE *fp=fopen(fn,"rb");
    if(fp==NULL) {
    	ThrowWith("找不到授权证书.");
    	return -1;
    }
    fread(buf,256,1,fp);
    fread(&crco,sizeof(int),1,fp);
    revInt(&crco);
    fread(buf+256,len-256-sizeof(int),1,fp);
    fclose(fp);
    len-=sizeof(int);
    if(crco!=crc32(crcc,(const Bytef *)buf,len)){
     printf("授权证书错误.%x---%x.\n",crco,crc32(crcc,(const Bytef *)buf,len));
     ThrowWith("授权证书错误.\n");
     return -2;
    }
    int codevalue=0x19710224;
    revInt(&codevalue);
    int *tp=(int *)buf;
    int *tpe=tp+len/sizeof(int);
    while(tp!=tpe) {*tp^=codevalue;tp++;}
    if(len%128!=0) {
       ThrowWith("授权证书错误.\n");
    	return -3;
    }
    lines=0;
    for(int i=0;i<len;i+=128)
    {
    	C_decode(buf+i);
    	lines++;
    }
    if(strncmp(getVersion(4),GetDPLibVersion(),2)!=0) { 
       ThrowWith("授权证书版本错误.\n");
    	return -4;
    }
    return len/128;
}

