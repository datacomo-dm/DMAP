#include "dt_common.h"
#include <stdlib.h>
#include "AutoHandle.h"
#include <errno.h>

#ifdef WIN32
#include <conio.h>
#endif
DTIOExport size_t dp_fwrite(const  void  *ptr1,  size_t  size,  size_t nmemb1,  FILE *stream) {
	return fwrite(ptr1,size,nmemb1,stream);
	char *ptr=(char *)ptr1;
	size_t nmemb=nmemb1*size;
	//LONG64 fsz=ftello(stream);
	//lseek(fileno(stream),fsz,SEEK_SET);
	while(true) {
	 size_t r=fwrite(ptr,1,nmemb,stream);
	 if((int)r>0) {
	 	 nmemb-=r;ptr+=r;
	 }
	 if(nmemb!=0) {
  	  int er=errno;
#ifdef WIN32
		if (er == ENOSPC ) {
#else
		if (er == ENOSPC || er == EDQUOT) {
#endif
			printf("文件系统满或超过限额,%d秒后重试!",TM_WAIT_FOR_DISKFULL);
			mySleep(20);//TM_WAIT_FOR_DISKFULL);
			continue;
		}
	  //if((int)r>0) 
	  //	  continue;
	  break;
	 }
	 return (nmemb1*size-nmemb)/size;
	}
}

DTIOExport void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[300000];
	vsprintf(msg,format,vlist);
	errprintf(msg);
	va_end(vlist);
	throw msg;
}

DTIOExport int cgetpwd(char *bf) {
#ifdef __unix
	strcpy(bf,getpass(""));	
	return strlen(bf);
#else
	bf[0]=0;
	int l=0;
	int ch=0;
	while(true) {
		ch=getch();
		if(ch==8) {
			//back space 
			if(l>0) bf[--l]=0;
		}
		else if(ch==10 || ch==13) break;
		else {
			bf[l++]=ch;bf[l]=0;
		}
	}
	return l;
#endif
}
/*
int dtioMain::cgetpwd(char *bf) {
	strcpy(bf,getpass(""));	
	return strlen(bf);
}
*/
// Not use 0 as a option.
DTIOExport int getOption(const char *prompt,int defaultval,const int lmin,const int lmax) {
	char choose[20];
	while(true) {
		if(defaultval>0)
			printf("\n%s(%d):",prompt,defaultval);
		else printf("\n%s:",prompt);
		choose[0]=0;
		fgets(choose,20,stdin);
		if(strlen(choose)>0)
			choose[strlen(choose)-1]=0;
		if(choose[0]==0 && defaultval>0) return defaultval;
		if(choose[0]==0) continue;
		int ich=atoi(choose);
		if(ich<lmin || ich>lmax) printf("输入错误，超过允许值(%d-%d).\n",lmin,lmax);
		else if(ich>0) return ich;
	}
}

DTIOExport int getString(const char *prompt,const char *defaultval,char *val) {
	char choose[20000];
	while(true) {
		if(defaultval!=NULL)
			printf("\n%s(%s):",prompt,defaultval);
		else printf("\n%s:",prompt);
		choose[0]=0;
		fgets(choose,20000,stdin);
		if(strlen(choose)>0)
			choose[strlen(choose)-1]=0;
		if(choose[0]==0 && defaultval!=NULL) {strcpy(val,defaultval);return strlen(defaultval);}
		if(choose[0]==0) continue;
		strcpy(val,choose) ;
		return strlen(val);
	}
}

DTIOExport int getdbcstr(char *un,char *pwd,char *sn,const char *prompt) {
	char dbcstr[1000];
	char strpwd[100];
	int state=0;
	*sn=0;*pwd=0;*un=0;
	while(true) {
		printf("\n%s%s:",prompt,state==0?"(用户名)":(state==1?"(密码)":"(服务名)"));
		if(state==1) {
			if(cgetpwd(strpwd)>1) {
				strcpy(pwd,strpwd);
				if(*sn) return 1;
				else state=2;
			}
			continue;
		}
		dbcstr[0]=0;
		fgets(dbcstr,1000,stdin);
		if(strlen(dbcstr)>0) dbcstr[strlen(dbcstr)-1]=0;
		int n=strlen(dbcstr);
		if(n<1) continue;
		if(state==2) { strcpy(sn,dbcstr);return 1;}
		char *sep=strstr(dbcstr,"@");
		if(sep!=NULL) { 
			strcpy(sn,sep+1);
			*sep=0;
		}
		sep=strstr(dbcstr,PATH_SEP);
		if(sep!=NULL) {
			*sep=0;
			strcpy(un,dbcstr);
			strcpy(pwd,++sep);
		}
		else strcpy(un,dbcstr);
		if(*pwd==0) state=1;
		else if(*sn==0) state=2;
		else return 1;
	}
	return 1;
}

DTIOExport bool GetYesNo(const char *prompt,bool defval) {
	char str[10];
	while(true) {
		printf("\n%s",prompt);
		fgets(str,10,stdin);
		int ch=str[0];
		if(ch=='\n') return defval;
		if(ch=='y' || ch=='Y') return true;
		if(ch=='n' || ch=='N') return false;
	}
	return defval;
}

#ifdef __unix
DTIOExport int uCopyFile(const char * src,const char *dest) {
	FILE *fsrc=fopen(src,"rb");
	if(fsrc==NULL) 
		ThrowWith("Open source file error while copy file '%s' to '%s'",
 	  	   src,dest);
	FILE *fdest=fopen(dest,"w+b");
	if(fdest==NULL) {
		ThrowWith("Create dest file error while copy file '%s' to '%s'",
			src,dest);
		fclose(fsrc);
		return -2;
	}
	char buf[1024*256];
	for(;;) {
		int l=fread(buf,1,1024*256,fsrc);
		if(l>0) {
			l=fwrite(buf,1,l,fdest);
			if(l<1) {
				fclose(fsrc);
				fclose(fdest);
				ThrowWith("Wirte file error while copy file '%s' to '%s'",
					src,dest);
			}
		}
		else break;
		
	}
	fclose(fsrc);
	fclose(fdest);
	return 1;		
}
#endif


DTIOExport int GetFreeM(const char *path) {
#ifdef WIN32
   char drive[_MAX_DRIVE];
   char dir[_MAX_DIR];
   char fname[_MAX_FNAME];
   char ext[_MAX_EXT];
   _diskfree_t dskinfo;
   _splitpath( path, drive, dir, fname, ext );
   _getdiskfree(drive[0]-'a'+1,&dskinfo);
   double freebytes=(double)dskinfo.avail_clusters*dskinfo.bytes_per_sector*dskinfo.sectors_per_cluster;
   return (int )(freebytes/(1024*1024));
#else
   //struct statfs freefs;
   //if(statfs(PATH_SEP,&freefs)==0)
   //  return (int)((double)freefs.f_bsize*freefs.f_bavail/(1024*1024));
   return 20480;
#endif
}

DTIOExport int SplitStr(const char *src,char **dst,char **pbf) {
		char *bf=*pbf;
		int sn=0;
		while(*src) {
			const char *se=src;
			while(*se && *se!=',') se++;
			strncpy(bf,src,se-src);
			bf[se-src]=0;
			dst[sn++]=bf;
			bf+=se-src+1;
			src=se;
			if(*src==',') src++;
		}
		*pbf=bf;
		return sn;
	}

DTIOExport void BuildWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf)
	{
		strcpy(sqlbf," where ");
		char bf[10000];
		char *pbf=bf;
		char *cars[60],*bars[60],*ears[60];
		int cn=SplitStr(cols,cars,&pbf);
		int bn=SplitStr(bvals,bars,&pbf);
		int en=SplitStr(evals,ears,&pbf);
		if(cn<1) ThrowWith("Encounter a null partiotion column(s) name");
		if(cn!=bn || bn!=cn) ThrowWith("Mismatch column(s) and value(s) of partiton info");
		for(int i=0;i<cn;i++) 
			sprintf(sqlbf+strlen(sqlbf),"%s %s>=%s and %s<%s",
				i==0?"":" and ",cars[i],bars[i],cars[i],ears[i]);
	}

//多字段的分区配置信息最多有可能拆分成两个SQL子条件。
DTIOExport 	void BuildPartitionWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf)
	{
		strcpy(sqlbf," where ");
		char bf[10000];
		char *pbf=bf;
		char *cars[60],*bars[60],*ears[60];
		int cn=SplitStr(cols,cars,&pbf);
		int bn=SplitStr(bvals,bars,&pbf);
		int en=SplitStr(evals,ears,&pbf);
		if(cn<1) ThrowWith("Encounter a null partiotion column(s) name");
		if(cn!=bn || bn!=cn) ThrowWith("Mismatch column(s) and value(s) of partiton info");
		bool allsame=true;
		//只需要设置一个条件
		bool only1c=false;
		strcpy(sqlbf," where (");
		int i;
		
		for(i=0;i<cn;i++) {
		 if(allsame && i+1==cn) 
		    only1c=true;
		 int cr=strcmp(bars[i],ears[i]);
		 //起始/终止值相同
		 if(cr==0 && allsame) {
			if(strcmp(bars[i],"null")==0)
		 	 sprintf(sqlbf+strlen(sqlbf)," %s %s is null ", i==0?"(":"and",cars[i]);
			else
		 	sprintf(sqlbf+strlen(sqlbf)," %s %s=%s ", i==0?"(":"and",cars[i],bars[i]);
		 }
		 else  {
		 	sprintf(sqlbf+strlen(sqlbf)," %s %s>=%s ",
				i==0?"(":" and ",cars[i],bars[i]);
			if(only1c)
		 	  sprintf(sqlbf+strlen(sqlbf)," and %s<=%s ",cars[i],ears[i]);
		 	else if(allsame)
		 	  sprintf(sqlbf+strlen(sqlbf)," and %s<%s ",cars[i],ears[i]);
			//只需要设置一个条件
			allsame=false;
		 }
		}
		strcat(sqlbf,")");
		if(!only1c) {
			strcat(sqlbf," or ");
			allsame=true;
		        for(i=0;i<cn;i++) {
		 	 	int cr=strcmp(bars[i],ears[i]);
			 	//起始/终止值相同
			 	if(cr==0 && allsame) {
					if(strcmp(bars[i],"null")==0)
		 			 sprintf(sqlbf+strlen(sqlbf)," %s %s is null ", i==0?"(":"and",cars[i]);
					else
		 			sprintf(sqlbf+strlen(sqlbf)," %s %s=%s ", i==0?"(":"and",cars[i],bars[i]);
		 	 	}
		 	 	else  {
		 	 		if(allsame)
		 	 	  	 sprintf(sqlbf+strlen(sqlbf)," %s %s=%s ",
						i==0?"(":" and ",cars[i],ears[i]);
					else
		 	 	  	 sprintf(sqlbf+strlen(sqlbf)," %s %s<=%s ",
						i==0?"(":" and ",cars[i],ears[i]);
					allsame=false;
				}
			}
			strcat(sqlbf,")");
		}
		strcat(sqlbf,")");
	}

#ifdef WIN32
DTIOExport  char *dirname(char *dirc) 
{
   char path_buffer[_MAX_PATH];
   char drive[_MAX_DRIVE];
   char dir[_MAX_DIR];
   char fname[_MAX_FNAME];
   char ext[_MAX_EXT];
   static char path[_MAX_PATH];
   _fullpath( path_buffer, dirc, _MAX_PATH ) ;
   _splitpath( path_buffer, drive, dir, fname, ext );
   //need reserved unix-like path stype on win32 platform
   if(dirc[0]=='/') { 
	   strcpy(path,dirc);
       path[strlen(dirc)-strlen(fname)-strlen(ext)-1]=0;
   }
   else
     sprintf(path,"%s%s",drive,dir);
   return path;
}

DTIOExport char *basename(char *dirc)
{
   char path_buffer[_MAX_PATH];
   char drive[_MAX_DRIVE];
   char dir[_MAX_DIR];
   char fname[_MAX_FNAME];
   char ext[_MAX_EXT];
   static char path[_MAX_PATH];
   _fullpath( path_buffer, dirc, _MAX_PATH ) ;
   _splitpath( path_buffer, drive, dir, fname, ext );
   sprintf(path,"%s%s",fname,ext);
   return path;
}
#endif

// dbtype :1 orcle 2 odbc 0: unsure(user select)
DTIOExport int BuildConn(int dbtype) {
	char un[30],pwd[80],sn[100];
	int opt=dbtype==0?getOption("数据库类型:\n  1.Oracle\n  2.ODBC\n请选择<1>:",1,1,2):dbtype;
	getdbcstr(un,pwd,sn,"连接参数");
	return wociCreateSession(un,pwd,sn,opt==1?DTDBTYPE_ORACLE:DTDBTYPE_ODBC);
}
