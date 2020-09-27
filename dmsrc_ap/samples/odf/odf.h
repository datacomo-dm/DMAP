#ifndef TAB_TRANSFER_SYS_HEADER
#define TAB_TRANSFER_SYS_HEADER

#ifdef WIN32
#include <windows.h>
#endif

#include "AutoHandle.h"
#include "dt_common.h"
#include "dt_svrlib.h"
//缺省：  自动抛出 char * 异常
//	  大部分标识号以0位非法值，从一开始编号
#define DS_NAME_LEN 50
#define PATH_LEN 300
#define SQL_LEN 2900
#define TABNAME_LEN 40
#define CONNHOST_LEN 50
//参数控制数据库为客户端/服务端共用，提供一致的控制。
//参数库类提供了到参数库的连接对象和相关性小的简单操作
class param_db {
   int dbc;
public :
   param_db(const char *un,const char *pwd,const char *sn);
   char * GetDSPath(int ds_id,char *path);
   void GetClientPath(int ds_id,int client_id,char *path);
   int BuildSrcDBC(int ds_id);
   int BuildDstDBC(int ds_id,int client_id);
   //true for valid time,false for limited time.
   int GetCompressFlag(int ds_id);
   int GetBlockRows(int ds_id);
   int GetFileSys(int ds_id);
   bool GetBlockDel(int ds_id);
   bool CheckTimeLimit(int ds_id);
   operator int () {return dbc;}
   //返回值表示参数是否存在
   int GetParamSQL(int ds_id,char *sql);
   int ClientLogin(char *username,char *pswd);
   bool ChangePswd(char *username,char *pswd,char *newpswd);

   //必须存在，如果字段值为空则抛出异常
   void GetExtSQL(int ds_id,char *sql);
   
   bool AllowCreateTab(int ds_id,int client_id);
   
   //如果没有SQL语句，但允许建表，则从数据块文件生成建表SQL.
   //返回false，则未定义SQL。
   bool GetCreateTabSQL(int ds_id,int client_id,bool &denycreate,char *sql);
   //只要有建立目标表的动作，就要检查索引是否需要建立。
   bool GetIndexSQL(int ds_id,int client_id,char *sql);
   //需要参数块意味着清除数据的SQL语句中存在动态绑定参数，没用合法的参数块就不能运行。
   //返回false则没有清除操作
   bool GetClearSQL(int ds_id,int client_id,bool &useparamblock,bool &repeat,char *sql);

      // refer to codetab for code means.
   int GetExtStatus(int ds_id);
   int GetLoadStatus(int ds_id,int client_id,int ds_sn);
	~param_db();
};

class dataset_task {
   param_db *pdb;
 //  int ds_id;
 //  int ext_status;
public :
   dataset_task(param_db *_pdb);
   // return false for no legal task
   // 
   //     only check ds_id but not lock status.
   bool GetExtTask(int &ds_id,int &ext_status);
   bool GetLoadTask(int client_id,int &ds_id,int &ext_lstsn);
   // Extract and load complete,but no 'clean' performed.
   bool GetCompleteTask(int &ds_id,int &ds_sn);
   int GetTimeoutTask();
   bool GetFirstLoadStatus(int client_id,int &ds_id,int &ds_sn,int &status);
};

class filetrans {
  param_db *pdb;
  bool connected;
  bool isopen;
  static void *envhdr;
  static void *connhdr;
  void *filehdr;
  int client_id;
  int ds_id;
  char host[CONNHOST_LEN],user[CONNHOST_LEN],authcode[CONNHOST_LEN];
  int fstype; //refer to codetab .
  void clear();
  //bool Connect();
  bool ConnectFTP();
public :
  static void InitTransfer();
  static void QuitTransfer();
  filetrans(param_db *_pdb);
  void SetContext(int _ds_id,int _client_id);
  
  //void Open(fileid);
  void Transfer(int fileid);
  virtual ~filetrans();
};
  
  //数据块的存储管理类，包括本地和远程的支持
class datablockstore {
   param_db *pdb;
   filetrans *pfs;
   int ds_sn;
   int ds_id;
   int client_id;
   // throw a exception while 
   void checksvrparam();
   void checkcltparam();
   char *GetExtFileName(char *fn,int block_type,int &fid);
   //void GetExtFeatureStr(char *msg);
   int TranslateToFileID(int type,int id);
public :
   datablockstore(param_db *_pdb,filetrans *_pfs);
   //如果不关心客户端属性，可以置_client_id=0
   void setdataset(int _ds_sn,int _ds_id,int _client_id);
   //GetExtracted block file numbers in table TABT_EXTREC
   int GetBlockNum(int ds_sn,int type);
   // id spec a sequence of current extract task(dataset).
   void WriteBlock(int mt,int type);
   //Check block file's error,list block file information
   bool CheckBlockFile(int type,int id);
   bool CheckBlockFile(int fileid);
   
   void EraseBlock(int fileid);
   void EraseBlock(int type,int id);
   //下面四个方法需要客户端标识
   void EraseLocalBlock(int type,int id);
   void EraseLocalBlock(int fileid);
   void TransferBlock(int type,int id);
   void TransferBlock(int fileid);
   
   void GetExtBlockProp(int type,int id,char *filename,int &fileid,int &flen,int &rownum);
   
   int BuildBlockMT(int type,int id);
   int BuildBlockMT(int fileid);
};

class dataset_svr {
   param_db *pdb;
   datablockstore *pdbs;
   int ds_id;
   int ds_sn;
   char ds_name[DS_NAME_LEN];
   int block_maxrows;
   int compress_flag;
   bool blockdel;
   void beginExt() ;
   void endExt(int status,int rn) ;
   void abortExt() ;
   //Lock and change status,if lock failed ,return false,other error throw exception;
   bool lockForExt();
   bool lockForClear();
   void abortClear();
  public :
   dataset_svr(param_db *_pdb,datablockstore *_pdbs,int _ds_id);
   // return false for lock failed ,throw exception for other errors.
   bool DoExtract();
   bool DoClear(int _ds_sn);
};

class destload {
   param_db *pdb;
   datablockstore *pdbs;
   int client_id;
   int ds_id;
   int ds_sn;
   bool LockForLoad() ;
   void changeLoadStatus(int status) ;
   void internalLoad();
public :
   destload(param_db *_pdb,datablockstore *_pdbs,int _client_id,int ds_id, int ds_sn);
   void Load();
};

#endif
