#ifndef DTIO_MTCHOOSER_H
#define DTIO_MTCHOOSER_H

#include "dtio_common.h"

class DTIOExport MTChooser {
 int mt;
 bool multi_se,atleastone;
 int selected;
 int rn;
 bool *sel;
public :
  int GetFirstSel() {
  	return GetNextSel(-1);
  }
  void DoSel(const char *title) {
  	if(mt==0 || rn==0) ThrowWith("Operator on a empty memtable(MTChooser::DoSel)");
  	char cmd[100];
  	bool refresh=true;
  	bool quit=false;
  	while(!quit) {
  		if(refresh) {
  		 if(title) printf("\n%s\n",title);
  		 wociMTCompactPrint(mt,0,NULL);
  		 refresh=false;
  		}
  		printf("\n请选择(h-help):");
  		memset(cmd,0,sizeof(cmd));
  		fgets(cmd,100,stdin);
  		int s=0;
  		switch(cmd[0]) {
  			case 'a':
  			 if(!multi_se) {
  			 	printf("只能选一个!\n");
  			 	break;
  			 }
  			 SelAll();
  			 refresh=true;
  			 break;
  			case 'c':
  			 UnselAll();
  			 refresh=true;
  			 break;
  			case 'r':
  			  if(atleastone && selected<1) {
  			  	printf("必须至少选择一项!\n");
  			  }
  			  else quit=true;
  			break;
  			case 'h':
  			printf("\n+/- nn 增加或清除某一项　\n"
  			        " r 确定并返回\n"
  			        " a 全部选中\n"
  			        " c 全部清除\n");
  			break;
  			default:
  			 s=atoi(cmd);
  			 if(s==0) {
  				printf("输入错误");
  				break;
  			 }
  			 if(s<-rn || s>rn) {
  			 	printf("序号输入错误(1-%d)\n.",rn);
  			 	continue;
  			 }
  			 if(cmd[0]=='+' || s>0)
  			 	Sel(s);
  			 else
  				Unsel(-s);   
  			 refresh=true;
  			 break;
  		}
  	}
  }
  
  void UnselAll() {
	for(int i=0;i<rn;i++) {
		if(sel[i]) Unsel(i+1);
	}
	selected=0;
  }  	
  void SelAll() {
	for(int i=0;i<rn;i++) {
	 if(!sel[i]) Sel(i+1);
	}
	selected=rn;
  }
  void Unsel(int p) {
  	p--;
  	if(p<rn && p>-1) {
  		wociSetStrValues(mt,1,p,1,"[ ]");
  		if(sel[p])  selected--;
  		sel[p]=false;
  	}
  }
  void Sel(int p) {
  	p--;
  	if(p<rn && p>-1) {
  		if(!multi_se) {
  			for(int i=0;i<rn;i++) {
  				if(sel[i]) Unsel(i+1);
  			}
  		}
  		wociSetStrValues(mt,1,p,1,"[*]");
  		if(!sel[p])  selected++;
  		sel[p]=true;
  	}
  }
  int GetMt() { 
  	if(mt==0 || rn==0) ThrowWith("Operator on a empty memtable(MTChooser::GetMt)");
  	return mt;
  }
  int GetNextSel(int p) {
  	for(p++;p<rn;p++) {
	 if(sel[p]) return p;
	}
	return -1;
  }  		
  MTChooser() {
   mt=0;
   multi_se=true;atleastone=true;sel=NULL;
   selected=0;rn=0;
  }
  ~MTChooser() {
  	if(mt>0) wocidestroy(mt);
  	if(sel!=NULL) delete []sel;
  }
  void SetMt(int _mt,bool _multi_sel=true,bool _atleastone=true) {
   mt=wociCreateMemTable();
   atleastone=_atleastone;
   multi_se=_multi_sel;
   wociAddColumn(mt,"mt_chsid","序号",COLUMN_TYPE_INT,4,0);
   wociAddColumn(mt,"mt_chsflag","选择",COLUMN_TYPE_CHAR,4,0);
   wociCopyColumnDefine(mt,_mt,NULL);
   rn=wociGetMemtableRows(_mt);
   wociBuild(mt,rn);
   int cn=wociGetColumnNumber(_mt);
   if(sel!=NULL) delete[]sel;
   sel=new bool[rn];
   char *psel=new char[rn*4];
   int *porder=new int[rn];
   for(int i=0;i<rn;i++) {
   	sel[i]=false;
   	strcpy(psel+i*4,"[ ]");
   	porder[i]=i+1;
   }
   selected=0;
   int *ctp=new int [cn];
   void **ptr=new void *[cn+3];
   int *clen=new int [cn];
   wociAddrFresh(_mt,(char **)(ptr+2),clen,ctp);
   ptr[0]=porder;
   ptr[1]=psel;
   ptr[cn+2]=NULL;
   wociInsertRows(mt,ptr,NULL,rn);
   delete []psel;
   delete []porder;
   delete []ctp;
   delete []ptr;
   delete []clen;
  }
};

#endif
