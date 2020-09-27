#ifndef TBLOADER_IMPL_DEF_H
#define TBLOADER_IMPL_DEF_H
/********************************************************************
  file : tbloader_impl.h
  desc : define the tbloader internal function
  author: liujianshun,201304
********************************************************************/
#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <openssl/md5.h>   // MD5
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "zlib.h"      // zip 算法
#include "dt_svrlib.h" // SysAdmin *psa
#include "ThreadList.h"
#include "dt_lib.h"

#include <vector>
#include <string>
using namespace std;

// 文件头类型
#define _MAX_ROWS   100000+20
const int conNameLen = 256;
const int conMd5Len = 16;
const int conMd5StrLen = 16*2;
const int conAppendMem5MB = 5*1024*1024;
const int conAppendMem1MB = 1024*1024;

// 全局日志开关
extern bool g_logInit;      // 全局日志初始化标志
// 系统路径
extern char g_sysPath[conNameLen];

typedef enum _ColumnType{_INT,_LONG,_FLOAT,_DOUBLE,_STRING,_BYTE,}_ColumnType;
typedef enum _EngineType{MyISAM=1,Brighthouse}_EngineType;   // 数据库引擎类型

#ifndef BIGENDIAN        // 大端小端
#define _revDouble(V)
#define _revLong _revDouble
#define _revInt(V)
#define _revShort(V)
#else
#define _revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }

#define _revLong _revDouble

#define _revShort(V)   { short def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          *((short *)(V))=def_temp;}
 
#define _revInt(V)	{ int def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			  *(int *)(V) = def_temp; }
#endif

// 自动锁
typedef pthread_mutex_t Lock;
class AutoLock 
{
private:
	Lock* m_lock;
public:
    AutoLock(Lock* lock):m_lock(lock)
	{
		pthread_mutex_lock(m_lock);
	}
	virtual ~AutoLock()
	{
		pthread_mutex_unlock(m_lock);
	}
};

// 性能调试时间测试类，必须在wdbi初始化以后，释放之前使用
class Timer
{
private:
#ifdef DEBUG
    mytimer m_tm;
    int m_id;
    char m_title[128];  
#endif
public:
    Timer(const int id,const char* title){
#ifdef DEBUG
        m_id = id;
        int len = strlen(title)>=127 ? 127 : strlen(title);
        strncpy(m_title,title,len);
        m_title[len] = 0;
        m_tm.Start();
#endif
    }
    virtual ~Timer(){
#ifdef DEBUG    	
        m_tm.Stop();
        lgprintf("id=[%d] [process:%s] 使用时间[%02f]",m_id,m_title,m_tm.GetTime());   	
#endif
    }	
};

// 调试信息输出
#ifdef DEBUG
#define dbg_lgprintf  lgprintf
#else
#define dbg_lgprintf  
#endif

// tbloader ,tbexporter类的公共部分
class pub_info
{
public:
	pub_info(){m_hasPreparedMt = false;m_id = 0;m_running = false;};
	virtual ~pub_info(){};
protected:
    bool            m_running;                   
    int             m_id;          
    bool            m_hasPreparedMt;   // tradeoffmt是否已经准备好
public:
    inline bool getRunFlag(){return m_running;};
    inline void setRunFlag(bool b) { m_running = b;};
    inline void setId(int id){m_id = id;};
    inline int  getId(){ return m_id;};
};

/*
dsn  ：数据源(data source name)
user : 连接数据库用户名
pwd  : 连接数据库密码
dbname : 数据库名称
tbname : 表名称
fn   ：要进行导入表的数据文件 
fn_ext: 导出文件名称
tbEngine:引擎类型
*/
typedef struct _InitInfo
{
    char funcode[128];                // -load_file,-gen_file
    char dsn[128];
    char user[128];
    char pwd[128];
    char dbname[64];
    char tbname[64];
    char fn[256];
    char fn_ext[256];
	_EngineType tbEngine;
	_InitInfo(){
		tbEngine = MyISAM;
	}
}_InitInfo,*_InitInfoPtr;

/*
第一部分
头文件位置 （int） 4字节 [X]—— 头部长度，列头部信息的总的长
头文件 (String) X字节 [head[6]] 
头文件内容 (使用;切分)
columnName(列名);
columnDBType(数据库中类型);
columnType(java类型);
columnCName;
columnPrecision;
columnScale 
例如：c1,c2;INT,INT;INT,INT;col1,col2;2,2;12,12
*/
typedef struct _ColumnInfo
{
    char columnName[conNameLen];
    _ColumnType  columnDBType;
    _ColumnType  columnType;
    char columnCName[conNameLen];          // 中文描述，不用
    int  columnPrecision;                  // 精度，不用
    int  columnScale;                      // 范围，不用
    _ColumnInfo(){
		memset(columnName,0,conNameLen);
		columnDBType = _INT;
		columnType = _INT;
		memset(columnCName,0,conNameLen);
		columnPrecision = 0;
		columnScale = 0;
    }
}_ColumnInfo,*_ColumnInfoPtr;
typedef vector<_ColumnInfoPtr>            _ColumnInfoPtrVector;
typedef vector<_ColumnInfoPtr>::iterator  _ColumnInfoPtrIter; 
#define separater_columns  ';'            // 列间分隔符
#define separater_items    ','            // 节点间分隔符  
typedef char* HerderPtr;           // 数据头部内容


#define CLOSE_FILE(filehandle)\
do{\
if((filehandle)!=NULL){\
fclose((filehandle));\
(filehandle) = NULL;\
}\
}while(0);

/*
第三部分：
各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
数据总数 （Long） 8字节
数据块额定行数 （int） 4字节
数据块个数 （int） 4字节 [y]
*/
#define	 CalculateBlockStartPos(filesize,blkNum,blkIdx) ((filesize)-16-(((blkNum)-(blkIdx))*8))
typedef  struct _BlockSumary
{
    unsigned long filesize;         // 文件字节数
    int        blockNum;            // 记录块个数
    int        rowNum;              // 记录块额定行数
    long       rowSum;              // 总的记录行数
    vector<long>   BlkStartPosVec;  // 各个数据块的起始位置
    _BlockSumary(){
		filesize = 0;
		blockNum = 0;
		rowSum = 0;
		BlkStartPosVec.clear();
    }
}_BlockSumary,*_BlockSumaryPtr;

/*
第二部分数据块结构：
数据块大小 （int） 4字节
数据起始位置（Long） 8字节
数据结束位置 （Long） 8字节
数据MD5(String) 32字节
数据内容（GZIP压缩）

每一行数据结构 解压后的数据格式：
行数据大小 （int） 4字节 
列空值情况 （byte[columnName.length]） 1 == 空值 0== 非空 
（例如 011000 ，根据columnType 取出相应数据类型，提供取值）
数据 （根据数据类型长度读取，字符串类型，前4字节为字符串长度）
*/
typedef struct _BlockInfo
{
    int   blocksize;               // 数据块大小，整个数据块大小
    long  startPos;
    long  endPos;
    char  md5sum[conMd5StrLen];           // md5sum of uncompression data
    int   compressed_data_size;           // endpos - startpos
    _BlockInfo(){
	    blocksize = 0;
		startPos = 0;
		endPos = 0;
		memset(md5sum,0,conMd5StrLen);
		compressed_data_size = 0;
    }
}_BlockInfo,*_BlockInfoPtr;
typedef unsigned char* LinePtr;           // 数据部分
typedef unsigned char* NullPtr;           // 列空值部分 
enum{EMPTY_NO = 0x0,EMPTY_YES=0x1};       // 0:非空，1:空

/*
    func:ReleaseBuff 
    desc:release malloc locate memory
*/
void ReleaseBuff(void* p);


/*
    func: LocateBuff(int rowNum,int rowLen,int colnum)
    desc: locate buffer for data block use
    params:
        rowNum[in]: data block row number
        rowLen[in]: row len
        colnum[in]: data block columns
        pbuff[in/out]: the data buffer 
    return: the buffer len
*/
int LocateBuff(const int rowNum,const int rowLen,const int colnum/* 列*/,unsigned char** pbuff);

/*
    func: ParserColumnInfo(const char* header,_ColumnInfoPtrVector& colInfoVec)
    desc: 解析列信息
    param:
          header[in] : column header info 
          colInfoVec[in/out]:存储数据块列信息的队列
    return : success 0,error -1 
*/
int  ParserColumnInfo(const HerderPtr header,_ColumnInfoPtrVector& vec);


/*
    func: GetColumnHeader(FILE* pFile,char * pHeader)
    desc: 获取文件的头部
    param:
           pFile[in]: 数据块文件句柄
           pHeader[in/out]: 文件头部
           headerlen[in]:读取的长度
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen);
int  WriteColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen);

/*
    func: GetColumnHeaderLen(FILE* pFile,char * pHeader)
    desc: 获取文件的头部长度
    param:
           pFile[in]: 数据块文件句柄
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeaderLen(FILE* pFile);
int  WriteColumnHeaderLen(FILE* pFile,int headerlen);

/*   
     func: GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
     desc: 获取列的相关信息，存入队列中
     param:
           pFile[in]: 数据块文件句柄
           colInfoVec[in/out]:存储数据块列信息的队列
     return: success 0,error -1
*/
int  GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec);


/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: 获取文件的概要信息
	 param:
           pFile[in]: 数据块文件句柄
           blkSumaryInfo[in/out]:文件概要信息结构(块个数，块额定行数，总记录数，块起始位置)
     returns: 0 success,-1 error
*/
int GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo);

/*
     func: WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo)
	 desc: 写入文件的概要信息
	 param:
           pFile[in]: 数据块文件句柄
           blkSumaryInfo[in]:文件概要信息结构(块个数，块额定行数，总记录数，块起始位置)
     returns: 0 success,-1 error
*/
int WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo);

/*
    func: GetBlockData(FILE* pFile,int blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in/out]: origin buffer
         origin_len[in/out]: origin len
     return 0:success -1:error
*/
int GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,char* porigin_buff,long& origin_len);


/*
    func: GetBlockData(FILE* pFile,int blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
     return 0:success -1:error
*/
int WriteBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,const char* porigin_buff,const long origin_len);



/*
    func: ParserOriginData(const char* porigin_buff,const int origin_len,AutoMt* mt,_ColumnInfoPtrVector& colInfoVec)
    desc: Parser origin data into AutoMt
    param:
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
		 mt:AutoMt
		 colInfoVec:columnInfo
     return 0:success -1:error
*/
int ParserOriginData(const char* porigin_buff,const int origin_len,int memtab,_ColumnInfoPtrVector& colInfoVec);

/*
    func: DecompressDataBlock(const char* compressed_data,const int compressed_len,char * origin_data,int& origin_len);
    desc: Decompress the gzip data to origin data
    param:
        compressed_data[in]: compressed data
        compressed_len[in]: compressed data len
        origin_data[in/out]: origin data buff
        origin_len[in/out]: orgint data len
     return success 0,failed -1
*/
int  UnzipDataBlock(const char* compressed_data,const long compressed_len,char * origin_data,long& origin_len);


/*
    func:ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len);l
    desc:compressed the zip data
    param:
        compressed_data[in/out]: compressed data
        compressed_len[in/out]: compressed data len
        origin_data[in]: origin data buff
        origin_len[in]: orgint data len
    return success 0,failed -1
	note : use compress2 ,zip algorithm, throw the zlib library 
*/
int ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len);

/*
     func: ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
     desc: 释放存储列相关信息队列队形
     param:
           colInfoVec[in/out]:存储数据块列信息的队列
*/
void ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec);

/*
     func:DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb)
     desc:通过文件文件方式，直接将mt写入数据库文件(不是insert操作)
     param:
          dbname[in]:数据库名称
          tbname[in]:表名称
          memtb[in]:memtable结构，内部存储了已经填充好的数据
          pdts[in]:连接mysql数据库，刷新表的时候使用
          syspath[数据库根目录]
          lastmt[in]:是否是最后一个memtable,true:最后一个mt,这时候需要压缩文件
     return:
          deal rows: successed 
          -1: failed 
*/
int DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb,AutoHandle*  pdts,const char* sysPath,bool lastmt);

/*
    func:GenLoadFileFromMt 
    desc:将mt转换成等待装入的二进制文件
    param:
          dbname[in]:数据库名称
          tbname[in]:表名称
          memtb[in]:memtable结构，内部存储了已经填充好的数据
          ib_datafile[in/out]:写入数据文件的文件对象句柄
          status:文件状态[_STATUS_OPEN,_STATUS_WRITING,_STATUS_CLOSE]
          fname[in]:转换写入的文件名称
          lastpack[in]:是否是最后一个包，强制关闭文件
    return:
          0: 成功
          -1: 失败
    note:生成等待装入的数据文件用，BrightHouse引擎使用
*/
enum write_file_status{_STATUS_UNKNOWN,_STATUS_OPEN,_STATUS_WRITING,_STATUS_CLOSE,};  // 写入文件状态
int GenLoadFileFromMt(const char* dbname,const char* tbname,int memtb,const char* fname,IBDataFile* ib_datafile,write_file_status status,bool lastpack);

/*
    func:LoadFile2Table 
    desc:将二进制文件装入brighthouse引擎数据库
    param:
          dbname[in]:数据库名称
          tbname[in]:表名称
          pdts[in]:获取目标表名称和相关操作使用的
          syspath[in]:存储路径
          fname[in]:转换写入的文件名称
    return:
          0: 成功
          -1: 失败
    note:将生成后的文件Load进入BrightHouse引擎
*/
int LoadFile2Table(const char* dbname,const char* tbname,AutoHandle*  pdts,const char* syspath,const char* fname);

/*
    func: LoadSysPath
    desc: 获取数据库系统路径
    param: 
          sysPath[in/out]:系统路径
    return: 0 成功
            -1 失败
    note:全局只初始化一次
*/
int LoadSysPath(char* sysPath);
#endif
