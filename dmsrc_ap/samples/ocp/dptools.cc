#include <stdio.h>

int cgetpwd(char *bf) {
      strcpy(bf,getpass(""));	
	return strlen(bf);
}

// Not use 0 as a option.
int getOption(const char *prompt,int defaultval,const int lmin,const int lmax) {
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

int getString(const char *prompt,const char *defaultval,char *val) {
	char choose[2000];
	while(true) {
		if(defaultval!=NULL)
		 printf("\n%s(%s):",prompt,defaultval);
		else printf("\n%s:",prompt);
		choose[0]=0;
		fgets(choose,200,stdin);
		if(strlen(choose)>0)
		choose[strlen(choose)-1]=0;
		if(choose[0]==0 && defaultval!=NULL) {strcpy(val,defaultval);return strlen(defaultval);}
		if(choose[0]==0) continue;
		strcpy(val,choose) ;
		return strlen(val);
	}
}
		
int getdbcstr(char *un,char *pwd,char *sn,const char *prompt) {
	char dbcstr[1000];
	char strpwd[100];
	int state=0;
	*sn=0;*pwd=0;*un=0;
	while(true) {
		printf("\n%s%s:",prompt,state==0?"":(state==1?"(密码)":"服务名"));
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
		sep=strstr(dbcstr,"/");
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

