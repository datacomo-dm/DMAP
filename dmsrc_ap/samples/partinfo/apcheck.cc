#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

class DPN {
	FILE *fp;
	bool state0;
  long no_packs;
  int file,offset;
  long cur_pack;
  long fsize;
  unsigned short no_obj,no_null;
public:
	
	int File() {return file;}
	int Offset() {return offset;}
	int NoObj() {return no_obj;}
	int NoNull() { return no_null;}
	long CurPack() {return cur_pack;}
	DPN(const char *filename,bool _state0) {
	 fp=fopen(filename,"rb");
	 state0=_state0;
   cur_pack=0;
   if(fp==NULL) {
     printf("Open file '%s' failed.\n",filename);
     throw -1;
   }
   fseek(fp,0,SEEK_END);
   fsize=ftell(fp);
   if(fsize%37!=0) {
     printf("File '%s' size %ld,not a dpn file(multiby 37),mod 37 :%d.\n",filename,fsize,fsize%37);
     throw -2;
   }
   no_packs=fsize/37-1;
   if(no_packs<1) {
     printf("Empty file.\n");
     throw -3;
   }
   //seek to first pack:
   fseek(fp,37*2,SEEK_SET);
	}
	// empty packs
	// 0 based pack no
	bool Load(long pack) {
		assert(pack+1<=no_packs	&& pack>=0);	
		if(pack+1==no_packs) {
			fseek(fp,state0?37:0,SEEK_SET);
		}
		else fseek(fp,pack*37+37*2,SEEK_SET);
		cur_pack=pack;
		return Load();
	}
	//Load Next
	bool Load() {
		char buf[37];
		if(fread(buf,37,1,fp)!=1) {
			 throw -3;
		}
		file=*((int *)buf);
		offset=*((int *)(buf+4)),
		no_obj=*((unsigned short *)(buf+32));
		no_null=*((unsigned short *)(buf+34));
		return file>=0;
	}
	void PackSeek(int seekfile,long seek1,long seek2) {
		fseek(fp,0,SEEK_SET);
		long packn=-2;
		long off=0;
		do {
			if(Load()) {
				if(file==seekfile && offset>=seek1 and offset<=seek2) {
					printf("pack<%ld> store at %d.\n",packn,offset);
				}
			}
			packn++;
			off+=37;
		}
		while(packn+1<no_packs);
	}
	~DPN() {
		if(fp)
			fclose(fp);
	}
};

class PackFile {
	FILE *fp;
	char filename[300];
	unsigned short no_obj,no_null;
	unsigned int total_size;
	char optimal_mode;
	long offset;
	long fsize;
	char *path;
	int attr_number;
	int fileid;
public :
	unsigned int TotalSize() {return total_size;}
	PackFile(char *_path,int _attr_number,int _fileid) {
		fileid=_fileid;
		path=_path;
		attr_number=_attr_number;
		fp=NULL;
		if(fileid>=0)
			OpenFile();
  }
  void OpenFile() {
  	if(fileid<0) return;
  	sprintf(filename,"%s/TA%05d%09d.ctb",path,attr_number,fileid);
  	if(fp) 
  		fclose(fp);
		fp=fopen(filename,"rb");
		if(fp==NULL) {
     printf("Open file '%s' failed.\n",filename);
     throw -1;
    }
		fseek(fp,0,SEEK_END);
		fsize=ftell(fp);
		fseek(fp,0,SEEK_SET);
    offset=0;
  }
  
  ~PackFile() {
  	if(fp)
  		fclose(fp);
  }
  void ReadPack() {
  	char head[9];
  	if(fread(head,9,1,fp)!=1) {
     printf("read pack head in file '%s' failed at %ld(%x).\n",filename,offset,offset);
     throw -1;
    }
  	total_size=*(int *)head;
  	no_obj=*(unsigned short *)(head+5);
  	no_null=*(unsigned short *)(head+7);
  	if(total_size<1)  {
     printf("read a empty pack head in file '%s'  at %ld(%x).\n",filename,offset,offset);
     //printf("seek none zero content position.\n",filename,offset,offset);
     throw -1;
    }
  }
  
  void Scan(int _fileid=-1) {
  	if(_fileid!=-1 && _fileid!=fileid) {
  		fileid=_fileid;
  		if(fileid<0) return;
  		OpenFile();
  	}

  	printf("Scan file '%s' ...\n",filename);
		fseek(fp,0,SEEK_SET);
    offset=0;
  	do {
    	ReadPack();
  		fseek(fp,total_size-9,SEEK_CUR);
    	offset+=total_size;
    }
    while(offset<fsize);
    if(offset!=fsize) 
    	printf("last pack exceed end of file '%s' :%ld-->%ld.\n",filename,offset,fsize);
    else
    	printf("OK.\n",offset,fsize);
  }
  
  void CheckPack(DPN &dpn) {
  	if(dpn.File()<0) return;
  	if(fileid!=dpn.File()) {
  		fileid=dpn.File();
  		OpenFile();
  	}
  	
  	if(fseek(fp,dpn.Offset(),SEEK_SET)==-1) {
  		printf("Seek to pack %d failed in file '%s',offset:%ld.\n",dpn.CurPack(),filename,dpn.Offset());
  		throw -1;
  	}
  	
  	offset=dpn.Offset();
  	ReadPack();
  	if(dpn.NoObj()!=no_obj) {
  		printf("Pack %d check failed in file '%s'(offset :%d),no_obj not same in dpn(%d) and packfile(%d).\n",dpn.CurPack(),
  		   filename,offset,dpn.NoObj(),no_obj);
  		throw -1;
		}  		
  	if(dpn.NoNull()!=no_null) {
  		printf("Pack %d check failed in file '%s'(offset :%d),no_null not same in dpn(%d) and packfile(%d).\n",dpn.CurPack(),
  		   filename,offset,dpn.NoNull(),no_null);
  		throw -1;
		}  		
  }
  
};
	
const char* pmsg = "apcheck: 1).校验已装入表对应列的数据信息\n"
  "  apcheck [tbpath(in)-表数据存放路径] [colIndex(in)-列索引,从0开始]\n"
  "      example : apcheck /app/dma/var/dm_test/tbtest1.bht 2 \n\n"
  
  "apcheck: 2).校验已装入表对应列的所有数据信息\n"
  "  apcheck [tbpath(in)-表数据存放路径] [colIndex(in)-列索引,从0开始] [all]\n"
  "      example : apcheck /app/dma/var/dm_test/tbtest1.bht 2 all \n\n"
  
  "apcheck: 3).检索已装入表dpn文件中的数据\n"
  "  apcheck [tbpath(in)-表数据存放路径] [colIndex(in)-列索引,从0开始] [file(in)-文件号] [startpos(in)-文件的起始位置] [endpos(in)-文件中的终止位置]\n"
  "      example : apcheck /app/dma/var/dm_test/tbtest1.bht 1 1 100 2000\n\n";
  
int main(int argc,char ** argv) {
   if(argc!=3 && argc!=6 && argc!=4) {
     printf(pmsg);
     return 0;
   }
   char filename[300];
   int attr_number=atoi(argv[2]); 
   sprintf(filename,"%s/PART_%05d.ctl",argv[1],attr_number);
   FILE *fp=fopen(filename,"rb");
   if(fp==NULL) {
     printf("Open file '%s' failed.\n",filename);
     return -1;
   }
   char buf[20];
   fread(buf,1,8,fp);
   bool oldversion = false;
   if(memcmp(buf,"PARTIF15",8)!=0) {
   	 if(memcmp(buf,"PARTINFO",8) != 0){
     	 printf("文件格式错误:'%s'.\n",filename);
	     fclose(fp);
    	 return -1;
   	 	}
	 	oldversion = true;
	 }
	 try {
	 char dpnfile[300];
	 sprintf(dpnfile,"%s/TA%05dDPN.ctb",argv[1],attr_number);
	 DPN dpn(dpnfile,true);
	 if(argc==6) {
	 	int sfile=atoi(argv[3]);
	 	long seek1=atol(argv[4]);
	 	long seek2=atol(argv[5]);
	 	printf("Seek dpn %s located in file %d between %ld--%ld.\n",dpnfile,sfile,seek1,seek2); 
	 	dpn.PackSeek(sfile,seek1,seek2);
	 	printf("Seek End.\n");
	 	return 0;
	 }
	 bool cbpart=argc==4;// check by partition
	 bool checkall=(argc==4 && strcasecmp(argv[3],"all")==0);
	 int attrid;
	 fread(&attrid,sizeof(int),1,fp);
	 short parts;
	 fread(&parts,sizeof(short),1,fp);
	 printf("分区总数:%d.\n-----------------------\n",(int)parts);
	 long total_objs=0;
	PackFile pf(argv[1],attr_number,-1);
	 for(int i=0;i<parts;i++) {
	 	 fread(buf,1,8,fp);
	 	 if(memcmp(buf,"PARTPARA",8)!=0) {
     	    printf("%d分区格式错误:'%s'.\n",i+1,filename);
     	    fclose(fp);
     	    return -1;
        }
     short namelen;
     fread(&namelen,sizeof(short),1,fp);
     char partname[300];
     fread(partname,namelen,1,fp);
     partname[namelen]=0;
     int firstpack,lastpack,lastpackobj,lastsavepack,lastsavepackobj,savesessionid;
     fread(&firstpack,sizeof(uint),1,fp);
     fread(&lastpack,sizeof(uint),1,fp);
     fread(&lastpackobj,sizeof(uint),1,fp);
     fread(&lastsavepack,sizeof(uint),1,fp);
     fread(&lastsavepackobj,sizeof(uint),1,fp);
     fread(&savesessionid,sizeof(uint),1,fp);
     fread(&attrid,sizeof(uint),1,fp);
	 
     int vectsize;
	 if(!oldversion) {
         fread(buf,sizeof(uint),1,fp);//skip lastfileid
         fread(buf,sizeof(int),1,fp);//skip savepos
         fread(buf,sizeof(int),1,fp);//skip lastsavepos
     }
     fread(&vectsize,sizeof(uint),1,fp);
     printf("分区 '%s' :\n",partname);
     printf("     firstpack :%d\n",firstpack);
     printf("     lastpack :%d\n",lastpack);
     printf("     lastpackobj :%d\n",lastpackobj);
     printf("     number of files :%d\n",vectsize);
     printf("       file list: ");
     for(int j=0;j<vectsize;j++) {
     	int fileid=0;
     	fread(&fileid,sizeof(uint),1,fp);
     	printf("%d%s",fileid,j+1==vectsize?"\n":",");
     	if(!cbpart)
     		pf.Scan(fileid);
     }
     int packsize;
     fread(&packsize,sizeof(uint),1,fp);
     printf("     Number of packlist:%d\n",packsize);
     printf("       packlist: ");
     long part_objs=0;
	 
     for(int j=0;j<packsize;j++) {
     	int packstart=0,packend=0,lastobjs=0;
     	fread(&packstart,sizeof(uint),1,fp);
     	fread(&packend,sizeof(uint),1,fp);
     	fread(&lastobjs,sizeof(uint),1,fp);
     	printf("R:%d-%d,O:%d%s",packstart,packend,lastobjs,j+1==packsize?"\n":";");
     	if(!cbpart) {
     		dpn.Load(packstart);
      	pf.CheckPack(dpn);
     		dpn.Load(packend);
     		pf.CheckPack(dpn);
     	}
     	else if(checkall || strcasecmp(partname,argv[3])==0) {
     		int st=packstart;
     		int lastfile=-1;
     		long lastpos=0;
     		printf(" check from %ld to %ld.\n",packstart,packend);
     		for(;st<=packend;st++) {
     			dpn.Load(st);
      		pf.CheckPack(dpn);
      		//列出不连续的数据包
      		if( dpn.File()>=0 &&(lastfile!=dpn.File() || lastpos!=dpn.Offset())) 
      			printf("		Pack %ld file %d ,start:%d.\n",dpn.CurPack(),dpn.File(),dpn.Offset());
      		if(dpn.File()>=0) {
      		lastfile=dpn.File();
      		lastpos=dpn.Offset()+pf.TotalSize();
      	}
     		}
     		printf(" check section end.\n");
     	}
      long list_objs=(packend-packstart)*65536+lastobjs;
     	total_objs+=list_objs;
     	part_objs+=list_objs;
     }
     printf("       PartObjs: %ld.\n",part_objs);
	 }
	 printf("Total objs:%ld.\n",total_objs);
	 fclose(fp);
	}
	catch(int &e) {
		printf("Detected error during check.!\n");
	}
  return 1;
}



