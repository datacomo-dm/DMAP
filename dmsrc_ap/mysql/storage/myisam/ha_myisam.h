/* Copyright (C) 2000-2006 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

/* class for the the myisam handler */

#include <myisam.h>
#include <ft_global.h>
#include <ctype.h>

#define HA_RECOVER_NONE		0	/* No automatic recover */
#define HA_RECOVER_DEFAULT	1	/* Automatic recover active */
#define HA_RECOVER_BACKUP	2	/* Make a backupfile on recover */
#define HA_RECOVER_FORCE	4	/* Recover even if we loose rows */
#define HA_RECOVER_QUICK	8	/* Don't check rows in data file */
/* Modified by DT project--BEGIN--*/
#include "dt_common.h"
#include "dtio_common.h"
#include "dtio.h"
#include "dt_svrlib.h"
#ifndef byte
typedef unsigned char byte;
#endif
#include <sys/types.h>
#ifdef __linux
#include <linux/unistd.h>
#include <pthread.h>
#include <ctype.h>
#endif
/* Modified by DT project--END--*/
extern ulong myisam_sort_buffer_size;
extern TYPELIB myisam_recover_typelib;
extern ulong myisam_recover_options;

/* Modified by DT project--BEGIN--*/
extern my_bool dp_para_mode;

#define vsafecall(realcall) try {\
        realcall;\
} \
	catch(char *err) {\
		THD *thd=current_thd;\
		strncpy(thd->dtmsg,err,MYSQL_ERRMSG_SIZE);\
	}\
	catch(WDBIError &er) {\
		int ec;\
		THD *thd=current_thd;\
		char *pmsg;\
		er.GetLastError(ec,&pmsg);\
		strncpy(thd->dtmsg,pmsg,MYSQL_ERRMSG_SIZE);\
	}

#define safecall(realcall,rc) try {\
	                    return realcall;\
} \
	catch(char *err) {\
		THD *thd=current_thd;\
		strncpy(thd->dtmsg,err,MYSQL_ERRMSG_SIZE);\
		return rc;\
	}\
	catch(WDBIError &er) {\
		int ec;\
		THD *thd=current_thd;\
		char *pmsg;\
		er.GetLastError(ec,&pmsg);\
		strncpy(thd->dtmsg,pmsg,MYSQL_ERRMSG_SIZE);\
		return rc;\
	}
	
#define SAFECALL_RC -1 //ER_ERROR_MESSAGES
#define BLOCK_START		'\x5a'
#define BLOCK_ISVALID	'\x9e'
//#define BLOCK_DELETED	'\x01'
#define BLOCK_UPDATED	'\x03'

	struct blockhdr {
	 char startflag,validflag; // validflag :maybe deleted or updated
	 long blocklen;
	 long recuid,row_num;
	 char svcnum[20];
	 char partid[10];
	 blockhdr() {
		startflag=BLOCK_START;
		validflag=BLOCK_ISVALID;
	 }
	};
struct mycd {
	int clen,scale,ctype;
	char *pbuf;
};
struct vtsfref {
	//这个不一定表示文件真实编号，只是一个内部表示号，这是为了节省编号空间。
	//  最大文件数 ：65536
	int fnid:16;
	// 单个数据块内的记录编号，最大 ：65536
	unsigned int recordid:16;//;
	// 数据块偏移量，对大: 4G
	unsigned int blockoffset;
};

#define MAX_UINT 0xffffffff
struct cache_block {
	char *buffer;
	// fnid ==-1 means filling,0 means empty
	int fnid,bstart,blen,readct,lru,tabid;
	bool havadm;
	char *dmbuffer;
};
extern char pCacheLocktype[];
extern short int pCacheLockid[];
extern int clct;

class dp_cache {
	char *pbuffer;
	char *pdmbuffer;
	int totread,hitread;
	cache_block *pcacheblock;
	int max_blocks;
	int max_blocklen;
  pthread_mutex_t lock;
	static dp_cache *p_DP_CACHE;
	dp_cache(int _max_blocks,int _max_blocklen) {
		//32 byte aligned.
		max_blocks=_max_blocks;max_blocklen=(_max_blocklen/64+1)*64;
		pbuffer=new char[max_blocks*max_blocklen];
		pcacheblock=new cache_block[max_blocks];
		if(pbuffer==NULL || pcacheblock==NULL) 
			ThrowWith("GLOBAL CACHE内存申请失败，请检查内存区设置是否太大: %d (MB).",
				max_blocks*max_blocklen);
		totread=hitread=0;
		memset(pcacheblock,0,sizeof(cache_block)*max_blocks);
		int dml=(MAX_BLOCKRN+1)/8;
		pdmbuffer=new char[max_blocks*dml];
		memset(pdmbuffer,0,max_blocks*dml);
		for(int blockid=0;blockid<max_blocks;blockid++) {
			pcacheblock[blockid].buffer=pbuffer+max_blocklen*blockid;
			pcacheblock[blockid].dmbuffer=pdmbuffer+dml*blockid;
		}
		//lock=PTHREAD_MUTEX_INITIALIZER;;
		(void) pthread_mutex_init(&lock,MY_MUTEX_INIT_FAST);
	}
	~dp_cache() {
                delete [] pcacheblock;
		delete [] pbuffer;
		delete [] pdmbuffer;
		pthread_mutex_destroy(&lock);
	}
public:
	static void initInstance(int _max_blocks,int _max_blocklen) {
		if(p_DP_CACHE==NULL) {
			p_DP_CACHE=new dp_cache(_max_blocks,_max_blocklen);
		}
	}
	static dp_cache *getInstance() {
		return p_DP_CACHE;
	}
	static void releaseInstance() {
		if(p_DP_CACHE!=NULL) {
			delete p_DP_CACHE;
			p_DP_CACHE;
		}
	}
	//在cache中查找指定fnid/blockstart健值的数据块,返回true则命中cache,返回false时,如果bid!=MAX_UINT,则内部已锁定
	//  一个block用于从磁盘读取数据.
	bool getBlock(char **pbf,char **pdm,int tabid,int fnid,int blockstart,int &bid,int &bflen,bool &delmsk) {
		int blockid;
		bool rt=false;
		bid=MAX_UINT;
		if(bflen>max_blocklen) return false;
		unsigned int maxlru=0;
		int lrublockid=-1;
        (void) pthread_mutex_lock(&lock);
		totread++;
		cache_block * pcb=pcacheblock;
		for(blockid=0;blockid<max_blocks;blockid++,pcb++) {
			pcb->lru++;
			if(pcb->fnid==fnid && pcb->tabid==tabid && pcb->bstart ==blockstart)
			{
				//所有block的lru都加1 ,因此,即便已命中 cache,也要继续循环
				if(bid!=MAX_UINT) { // 已经命中数据，找到重复的数据块
					//重复的数据块：由于读过程中的竞争，有可能产生重复的数据块，
					if(pcb->readct==0) {//未使用的重复数据块，可以释放
						pcb->fnid=pcb->lru=pcb->bstart=pcb->tabid=0;
					}
					//else if(pcacheblock[bid].readct==1) {
						//释放另一个
					//}
					continue;
				}
				*pbf=pcb->buffer ;
				*pdm=pcb->dmbuffer;
				delmsk=pcb->havadm;
				pcb->readct++;
				pcb->lru=0;
				bflen=pcb->blen;
				hitread++;
				//bid=hitblk=blockid;
				bid=blockid;
			}
			else if(pcb->fnid==0) { // a empty block
				maxlru=MAX_UINT;lrublockid=blockid;
			}
			else // 记录 最近最少使用的块(当前未被锁定写入(pcb->fnid!=MAX_UINT)block
			     // 也未被锁定读(pcb->readct==0))
				if(pcb->lru>maxlru && pcb->readct==0 && pcb->fnid!=MAX_UINT  ) {
				maxlru=pcb->lru;lrublockid=blockid;
			}
		}
		if(bid==MAX_UINT && lrublockid>=0) {
			pcb=pcacheblock+lrublockid;
			//MAX_FNID用户锁定block,不被其它线程使用
			pcb->fnid=MAX_UINT;
			*pbf=pcb->buffer ;
			*pdm=pcb->dmbuffer;
			pcb->havadm=false;
			bid=lrublockid;
			pcb->lru=0;
			bflen=max_blocklen;
		}
		else if(bid!=MAX_UINT) rt=true;
		(void) pthread_mutex_unlock(&lock);
		return rt;
	}
	
	void ReleaseFilling(int blockid,int tabid,int fnid,int bstart,int bflen,bool delmsk) {
		(void) pthread_mutex_lock(&lock);
		cache_block * pcb=pcacheblock+blockid;
		if(pcb->fnid==MAX_UINT)
		{
			pcb->fnid=fnid;pcb->bstart=bstart;pcb->tabid=tabid;
			if(pcb->readct!=0)
			 fprintf(stderr,"Release a dirty " DBPLUS_STR " index cache,%d,%d,%d,%d.\n",blockid,tabid,fnid,bstart);
			pcb->readct=1;
			pcb->lru=0;
			pcb->havadm=delmsk;
			pcb->blen=bflen;
		}
		else	fprintf(stderr,"Release a unlocked " DBPLUS_STR " index cache,%d,%d,%d,%d.\n",blockid,tabid,fnid,bstart);
		(void) pthread_mutex_unlock(&lock);
		// else  MUST BE A ERROR
	}
	
	void RepStatus(char *buf) {
		(void) pthread_mutex_lock(&lock);
		int readct=0,fillct=0,emptyct=0;
		int blockid;
		cache_block * pcb=pcacheblock;
		for(blockid=0;blockid<max_blocks;blockid++,pcb++) {
			if(pcb->fnid==MAX_UINT) fillct++;
			if(pcb->fnid==0) emptyct++;
			if(pcb->readct>0) 
				readct++;
		}
		sprintf(buf,"bn:%d/bl:%d/r:%d/f:%d/e:%d/t%d/hr:%d%%",
			max_blocks,max_blocklen,readct,fillct,emptyct,totread,totread==0?0:((int)hitread*100/totread));
		(void) pthread_mutex_unlock(&lock);
	}
	
	void FlushTab(int tabid) {	
		(void) pthread_mutex_lock(&lock);
		int blockid;
		cache_block * pcb=pcacheblock;
		for(blockid=0;blockid<max_blocks;blockid++,pcb++) {
			if(pcb->tabid==tabid && pcb->readct==0) {pcb->fnid=0;pcb->tabid=0;pcb->readct=0;}
		}
		(void) pthread_mutex_unlock(&lock);
	}
	
	/*void ForceFlushTab(int tabid) {	
		(void) pthread_mutex_lock(&lock);
		int blockid;
		cache_block * pcb=pcacheblock;
		for(blockid=0;blockid<max_blocks;blockid++,pcb++) {
			if(pcb->tabid==tabid ) {pcb->fnid=0;pcb->tabid=0;pcb->readct=0;}
		}
		(void) pthread_mutex_unlock(&lock);
	}*/

	void Reset(int blockid) { //filling then failed
		(void) pthread_mutex_lock(&lock);
		cache_block * pcb=pcacheblock+blockid;
		if(pcb->fnid==MAX_UINT)
			pcb->fnid=pcb->lru=pcb->readct=0;
		else	fprintf(stderr,"Reset a unlocked " DBPLUS_STR " index cache,%d.\n",blockid);

		(void) pthread_mutex_unlock(&lock);
	}

	void ReleaseReading(int blockid) {
		(void) pthread_mutex_lock(&lock);
		cache_block * pcb=pcacheblock+blockid;
		if(pcb->readct>0) 
		 pcb->readct--;
		(void) pthread_mutex_unlock(&lock);
	}
};

#define DTP_FILENAMELEN		180
#define DTP_TABLENAMELEN	80
class dtp_map {
	int totidxnum;
	int tabid;
	int soledidxnum;
	uint8B maxrows;
	int blocksize;
	int datfilenum;
	int *pfnid;
	
	char *pfilename;
	int *idxused,*ptaboff,*psubidxid,*pidxfieldnum;
	TABLE_LIST **tables;
	char *idxtbname;
	char idxdbname[DTP_TABLENAMELEN];
	char firstfn[DTP_FILENAMELEN];
	char sourceStream[DTP_FILENAMELEN];
	int firstfnid;
	bool fromStream;
	//int curfnid;
	//int curidxid;
public :
	int GetBlockSize() {return blocksize;}
	const char *GetSourceStream() {if(fromStream) return sourceStream;else return NULL;}
	bool IsFromSourceStream() {return fromStream;}
	TABLE_LIST **GetTables() {return tables;}
	int GetTotIdxNum() {return totidxnum;}
	LONG64 GetMaxRows() { return (LONG64)maxrows;}
	int GetTableOff(int inx) {return ptaboff[inx];}
	int GetSubIdx(int inx) {return psubidxid[inx];}
	int GetFieldNum(int inx) {return pidxfieldnum[inx];}
	int Extra(int curidxoff,enum ha_extra_function function) {
		TABLE *t_table = tables[curidxoff]->table; 
		if(t_table)
			return t_table->file->extra(function);
		return 0;
	}
	void ResetIndexTable() {
	  for(int i=0;i<soledidxnum;i++) 
	  	tables[i]->table=NULL;
	}

	int IndexEnd(int curidxoff) 
	{
	  if(curidxoff>=0 && tables[curidxoff]->table)
		//For Mysql 4.1 and above , use ha_index_end instead of index_end;
       //return tables[curidxoff].table->file->ha_index_end();
	     return tables[curidxoff]->table->file->ha_index_end();//	index_end();
	  return 0;
	}
	const char *GetFirstFile() {return firstfn;}
	int GetFirstFileID() {return firstfnid;}
	int GetTabID() { return tabid;}
	dtp_map(struct st_table *parent,dtparams_mt &dtmts,bool _fromStream=false,const char *stream=NULL) {
		//cuffnid=-1;
		//curidxid=-1;
		fromStream=_fromStream;
		if(fromStream) {
			strcpy(sourceStream,stream);
		}
		datfilenum=dtmts.GetDataFileNum();
		soledidxnum=dtmts.GetSoledIndexNum();
		totidxnum=dtmts.GetRowNum("DP_INDEX_ALL");
		blocksize=dtmts.GetInt("DP_TABLE","recordlen",0)*
			dtmts.GetInt("DP_TABLE","maxrecinblock",0);
		maxrows=dtmts.GetRecordNum();
		dtmts.GetFirstFile(firstfnid,firstfn);
		tabid=dtmts.GetInt("DP_TABLE","tabid",0);
		pfnid=new int[datfilenum];
		pfilename=new char[datfilenum*DTP_FILENAMELEN];
		//dtmts.GetDataFileMap(int *fnid,char *fns,int fnlen);
		dtmts.GetDataFileMap(pfnid ,pfilename ,DTP_FILENAMELEN);
		
		idxused=new int[totidxnum];
		memset(idxused,0,sizeof(int)*totidxnum);
		
		ptaboff=new int[totidxnum];
		psubidxid=new int[totidxnum];
		pidxfieldnum=new int[totidxnum];
		dtmts.GetIndexMap(ptaboff,psubidxid,pidxfieldnum);
		idxtbname=new char[DTP_TABLENAMELEN*soledidxnum];
		tables=new TABLE_LIST *[soledidxnum];
		bzero((char*) tables,sizeof(TABLE_LIST *)*soledidxnum);
	  for(int i=0;i<soledidxnum;i++) 
				dtmts.GetIndexTable(i,idxdbname,idxtbname+i*DTP_TABLENAMELEN);

	}
	void attach_child_index_table(struct st_table *parent);
	void clone(dtp_map *parent) {
				int i;
		for(i=0;i<soledidxnum;i++) {
				tables[i]->table=parent->tables[i]->table;
		}
	}
	const char *GetFileName(int fnid) {
		for(int i=0;i<datfilenum;i++) {
			if(fnid==pfnid[i]) return pfilename+i*DTP_FILENAMELEN;
		}
		ThrowWith("File id :%d not found !");
		return NULL;
	}
	~dtp_map() {
		if(pfnid!=NULL) delete []pfnid;
		if(pfilename!=NULL) delete []pfilename;
		if(idxused!=NULL) delete []idxused;
		if(ptaboff!=NULL) delete []ptaboff;
		if(psubidxid!=NULL) delete []psubidxid;
		if(idxtbname!=NULL) delete []idxtbname;
		if(pidxfieldnum!=NULL) delete []pidxfieldnum;
		for(int i=0;i<soledidxnum;i++) 
		  if(tables && tables[i])
	  		tables[i]->table=NULL;
		if(tables!=NULL) delete []tables;
	}
};

class vtsfFile {
	MI_INFO **minfo;
	//AutoMt idxmt;
	int mt ;// result data set.
	/*AutoMt idxcachemt;
	int	idxcachemaxrn;
	int	idxcachep;
	*/ 
	uchar frm_version; //FRM_VER+1 : old 4.0 DBPlus ; FRM_VER+4 :new 5.1 
	int mysqlrowlen;
	int tabid;
	char *resptr;// Result buffer,allocated and maintenanced by dt_file class.
	char *resdmptr;// delete mask array address
	bool havedm;
	file_mt file;
	dtp_map *pdtpmap;
	//dp_cache dpcache;
	//int maxrows;
	//int tabpos;
	//int soleidxnum;
	//int totidxnum;
	//psa could not store as a member,it must be get from SvrAdmin everytime.
	//SvrAdmin *psa;
	vtsfref cur_ref;
	//THD  *thd;
	//TABLE_LIST *tables;
	int cacheblockid;
	int cachebflen;
	int colct;
	mycd *pmycd;	
	int row_num_ib; //row num in current block
	int next_start_pos; //file read start pos currenttly.
	int deffnid;
	int rowct;
	//int *ptaboff,*psubidxid,*pidxfieldnum;
	//bool idxused[100];
  //MYSQL_LOCK *lock;
	int lkbal;
	int curidxid,curidxoff,subcuridxid,idxfieldnum;
        //char firstfn[300];
	/*
	int def_maxrn;
	int lmtrn;
	int indexmt;
	blockhdr cur_bhdr;
	int row_len;
	
	int rec_begin_num;//begin record number of current block at a query(ROWNUM of oracle);
	int firstdb_start;//first block offset of this file.
	FILE *fp;
	*/
	void CheckAndReleaseCache();
	bool checkDM(ulonglong pos);
public :
	TABLE * GetTable(int idx);
	enum INDEX_TAG {
		INDEX_NEXT,INDEX_NEXT_SAME,INDEX_FIRST,INDEX_LAST,INDEX_READ_LAST,INDEX_PREV,INDEX_READ_IDX
	} ;
	int GetTabID() { return tabid;}
	vtsfFile (MI_INFO **info,uchar _frm_version):/*idxmt(0,10),*/file(dp_para_mode!=0)/*,idxcachemt(0,10000)*/ {
		//memset(filename,0,sizeof(filename));mycd=NULL;
		row_num_ib=next_start_pos=0;//psa=NULL;
		minfo=info;
		frm_version=_frm_version;
		//thd=NULL;
		mt=0;pmycd=NULL;deffnid=0;rowct=0;resptr=NULL;havedm=false;resdmptr=NULL;
		curidxoff=-1;lkbal=0;
		//lock=NULL;
		pdtpmap=NULL;
		/*wociClear(idxcachemt);
		idxcachemaxrn=idxcachemt.GetMaxRows();
		idxcachep=0;
		idxcachemt.SetMaxRows(idxcachern);
		wociAddColumn(idxcachemt,"fnid",NULL,COLUMN_TYPE_INT,9,0);
		wociAddColumn(idxcachemt,"blockstart",NULL,COLUMN_TYPE_INT,9,0);
		wociAddColumn(idxcachemt,"blocksize",NULL,COLUMN_TYPE_INT,9,0);
		wociAddColumn(idxcachemt,"startrow",NULL,COLUMN_TYPE_INT,9,0);
		wociAddColumn(idxcachemt,"idx_rownum",NULL,COLUMN_TYPE_INT,9,0);
		idxcachemt.Build();
		*/
		cacheblockid=MAX_UINT;
		//rec_begin_num=0;
		//def_maxrn=500;lmtrn=0;pmycd=NULL;fp=NULL; 
	} 
	void clonevtsf(vtsfFile *parent) 
	{
		pdtpmap->clone(parent->pdtpmap);
	}
	void attach_child_index_table(struct st_table *parent){
		pdtpmap->attach_child_index_table(parent);
	}
	int getRowNum() {return row_num_ib;};
	int getRecord(byte *buf,ulonglong pos,size_t reclen);
	int loadBlock(int fnid,int pos,int storesize=0) ;
	int loadNextBlock();
	char *getCurRef() {
		return (char *)&cur_ref;
	}
	inline LONG64 GetMaxRows() {return pdtpmap->GetMaxRows();}
  
	ha_rows records_in_range(int inx,
			   key_range *min_key,
                                    key_range *max_key)
	{
		safecall(
		raw_records_in_range(inx,
				       min_key,
                                    max_key),
		 (ha_rows)SAFECALL_RC);
	}    	
				    	
	ha_rows raw_records_in_range(int inx,
			   key_range *min_key,
                                    key_range *max_key);
        int rnd_end() {
        	safecall(raw_rnd_end(),SAFECALL_RC);
        }
	int raw_rnd_end();
	int index_end() {
		safecall(raw_index_end(),SAFECALL_RC);
	}
	int raw_index_end();
    	int rrnd(MI_INFO *info,byte *buf,byte *pos) {
    		safecall(raw_rrnd(info,buf,pos),SAFECALL_RC);
    	}
    	int raw_rrnd(MI_INFO *info,byte *buf,byte *pos);
	int index_init(uint idx,bool sorted) {
		safecall(raw_index_init(idx,sorted),SAFECALL_RC);
	}
	int raw_index_init(uint idx,bool sorted);
	int heap_extra(MI_INFO *info,enum ha_extra_function function) {
		safecall(raw_heap_extra(info,function),SAFECALL_RC);
	}
	int raw_heap_extra(MI_INFO *info,enum ha_extra_function function);
	int scan(MI_INFO *info,byte *record) {
		safecall(raw_scan(info,record),SAFECALL_RC);
	}
	int raw_scan(MI_INFO *info,byte *record);
	int scan_init(MI_INFO *info) {
		safecall(raw_scan_init(info),SAFECALL_RC);
	}
	int raw_scan_init(MI_INFO *info);
	
	int init(dtp_map *_map) {
		safecall(raw_init(_map),SAFECALL_RC);	
	}
	int raw_init(dtp_map *_map) ;
	int index_read(MI_INFO *info,byte * buf, const byte * key,
		 key_part_map keypart_map, enum ha_rkey_function find_flag){
		 safecall(raw_index_read(info,buf,key,keypart_map,find_flag),SAFECALL_RC);	
	}
	int raw_index_read(MI_INFO *info,byte * buf, const byte * key,
		 key_part_map keypart_map, enum ha_rkey_function find_flag);
	int index_next(MI_INFO *info,byte * buf,INDEX_TAG indextag,const byte *key=NULL, key_part_map keypart_map=0, enum ha_rkey_function find_flag=HA_READ_KEY_EXACT,uint idx=0) {
		safecall(raw_index_next(info,buf,indextag,key,keypart_map,find_flag,idx),SAFECALL_RC);
	}
	int raw_index_next(MI_INFO *info,byte * buf,INDEX_TAG indextag,const byte *key, key_part_map keypart_map, enum ha_rkey_function find_flag,uint idx);

	~vtsfFile () ;
private:
	void RebuildDC();
	void setNullBit(byte *buf,int colid);
};
//pid_t gettid(void);
/* Modified by DT project--END--*/

class ha_myisam: public handler
{
  MI_INFO *file;
  ulonglong int_table_flags;
  char    *data_file_name, *index_file_name;
  bool can_enable_indexes;
  int repair(THD *thd, MI_CHECK &param, bool optimize);

/* Modified by DT project--BEGIN--*/
  my_bool vtsf; 
  vtsfFile *pvtsfFile;
/* Modified by DT project--END--*/

    /* Modified by DT project--BEGIN--*/
  int index_init(uint idx,bool sorted)
  {
	if(vtsf) {
		pvtsfFile->index_init(idx,sorted);
	}
	return handler::index_init(idx,sorted);
  }
  int index_end() {
	  if(vtsf) pvtsfFile->index_end();return handler::index_end();
  }
/* Modified by DT project--END--*/  

 public:
 /* Modified by DT project--BEGIN--*/
  int rnd_end();
  virtual int GetTabID() {
  	if(vtsf) return pvtsfFile->GetTabID();
  	return 0;
  }
  void clonevtsf(ha_myisam *parent)  {
  	if(vtsf) pvtsfFile->clonevtsf(parent->pvtsfFile);
  }
  void BuildVtsf(char *path,char *db,char *tbn) {
	  char *fn=new char[strlen(path)+10];
        	strcpy(fn,path);
        	strcat(fn,".DTP");
        	FILE *fp=fopen(fn,"rb");
        	if(fp!=NULL) {
        		fclose(fp);
        		dtioStreamFile *pdtio=new dtioStreamFile("./");
			pdtio->SetStreamName(fn);
			pdtio->SetWrite(false);
			pdtio->StreamReadInit(); 
			DTIO_STREAM_TYPE stp=pdtio->GetStreamType();
			if(stp!=DTP_BIND && stp!=DTP_DETACH)
				ThrowWith("指定的文件'%s'类型%d不是参数文件!",stp,fn);
			char *tp=tbn;
      while(*tp) {*tp=tolower(*tp);++tp;}
			dtioDTTable dtt(db,tbn,pdtio,false);
			dtt.DeserializeParam();
			if(dtt.GetParamMt()->GetRecordNum()>0) {
				pvtsfFile=new vtsfFile(&file,table_share->frm_version);
			 vtsf=TRUE;
			 pvtsfFile->init(new dtp_map(table,*dtt.GetParamMt(),stp==DTP_DETACH,stp==DTP_DETACH?pdtio->GetAttachFileName():NULL));
		  }
		  delete pdtio;
   }
		delete []fn;
  }
/* Modified by DT project--END--*/

  ha_myisam(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_myisam() {
  	    /* Modified by DT project--BEGIN--*/
	  int_table_flags=01;
	  if(vtsf) {
		if(pvtsfFile) delete pvtsfFile;
	  }
	/* Modified by DT project--END--*/
	}
  handler *clone(MEM_ROOT *mem_root);
  const char *table_type() const { return "MyISAM"; }
  const char *index_type(uint key_number);
  const char **bas_ext() const;
  ulonglong table_flags() const { return int_table_flags; }
  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return ((table_share->key_info[inx].algorithm == HA_KEY_ALG_FULLTEXT) ?
            0 : HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE |
            HA_READ_ORDER | HA_KEYREAD_ONLY);
  }
  uint max_supported_keys()          const { return MI_MAX_KEY; }
  uint max_supported_key_length()    const { return MI_MAX_KEY_LENGTH; }
  uint max_supported_key_part_length() const { return MI_MAX_KEY_LENGTH; }
  uint checksum() const;

  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int write_row(uchar * buf);
  int update_row(const uchar * old_data, uchar * new_data);
  int delete_row(const uchar * buf);
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag);
  int index_read_idx_map(uchar *buf, uint index, const uchar *key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag);
  int index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map);
  int index_next(uchar * buf);
  int index_prev(uchar * buf);
  int index_first(uchar * buf);
  int index_last(uchar * buf);
  int index_next_same(uchar *buf, const uchar *key, uint keylen);
  int ft_init()
  {
    if (!ft_handler)
      return 1;
    ft_handler->please->reinit_search(ft_handler);
    return 0;
  }
  FT_INFO *ft_init_ext(uint flags, uint inx,String *key)
  {
    return ft_init_search(flags,file,inx,
                          (uchar *)key->ptr(), key->length(), key->charset(),
                          table->record[0]);
  }
  int ft_read(uchar *buf);
  int rnd_init(bool scan);
  int rnd_next(uchar *buf);
  int rnd_pos(uchar * buf, uchar *pos);
  int restart_rnd_next(uchar *buf, uchar *pos);
  void position(const uchar *record);
  int info(uint);
  int extra(enum ha_extra_function operation);
  int extra_opt(enum ha_extra_function operation, ulong cache_size);
  int reset(void);
  int external_lock(THD *thd, int lock_type);
  int delete_all_rows(void);
  int disable_indexes(uint mode);
  int enable_indexes(uint mode);
  int indexes_are_disabled(void);
  void start_bulk_insert(ha_rows rows);
  int end_bulk_insert();
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
  void update_create_info(HA_CREATE_INFO *create_info);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
			     enum thr_lock_type lock_type);
  virtual void get_auto_increment(ulonglong offset, ulonglong increment,
                                  ulonglong nb_desired_values,
                                  ulonglong *first_value,
                                  ulonglong *nb_reserved_values);
  int rename_table(const char * from, const char * to);
  int delete_table(const char *name);
  int check(THD* thd, HA_CHECK_OPT* check_opt);
  int analyze(THD* thd,HA_CHECK_OPT* check_opt);
  int repair(THD* thd, HA_CHECK_OPT* check_opt);
  bool check_and_repair(THD *thd);
  bool is_crashed() const;
  bool auto_repair() const { return myisam_recover_options != 0; }
  int optimize(THD* thd, HA_CHECK_OPT* check_opt);
  int restore(THD* thd, HA_CHECK_OPT* check_opt);
  int backup(THD* thd, HA_CHECK_OPT* check_opt);
  int assign_to_keycache(THD* thd, HA_CHECK_OPT* check_opt);
  int preload_keys(THD* thd, HA_CHECK_OPT* check_opt);
  bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
#ifdef HAVE_REPLICATION
  int dump(THD* thd, int fd);
  int net_read_dump(NET* net);
#endif
#ifdef HAVE_QUERY_CACHE
  my_bool register_query_cache_table(THD *thd, char *table_key,
                                     uint key_length,
                                     qc_engine_callback
                                     *engine_callback,
                                     ulonglong *engine_data);
#endif
  MI_INFO *file_ptr(void)
  {
    return file;
  }
 /* Modified by DT project--BEGIN--*/
  // force enable full table scan order by index,
  //   default is disable,but for inndb bdb is enable.
  key_map all_key_map;
  key_map *keys_to_use_for_scanning() { all_key_map.set_all(); return &all_key_map;}
/* Modified by DT project--END--*/
};
