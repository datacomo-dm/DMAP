#ifndef TBLOADER_HELPER_DEF_H
#define TBLOADER_HELPER_DEF_H

#include "tbloader_impl.h"

/********************************************************************
  file : tbloader_helper.h
  desc : define the tbloader java call interface
  author: liujianshun,201304
  note: support multiple thread use
********************************************************************/

enum STATUS{status_unknown,unstart,hasstarted,parserheader,writedata,hasstoped};
enum OPER_TYPE{oper_unknown,write_files,insert_db};

class tbloader_helper:public pub_info
{
public:
    tbloader_helper();
    virtual ~tbloader_helper();

public:
    int   start(_InitInfoPtr pInitObj,int operType,const char* logpth);   // 初始化
    int   stop();                                                         // 卸载
    int   parserColumnInfo(const HerderPtr header,const int headerlen);   // 解析包头
    int   writePackData(const char* porigin_buff,const long origin_len,const int rownum);  // 写入数据块 
    inline STATUS getStatus(){return m_status;};
    inline void setStatus(STATUS st){m_status = st;};
    inline OPER_TYPE getOperType(){ return m_opertype;};
    void  getMd5Sum(char * pmd5sum);
    inline void setSysPath(const char* psysPath){strcpy(m_sysPath,psysPath);};
	
protected:	
    // 初始化信息
    _InitInfo            m_stInitInfo;   
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
    _ColumnInfoPtrVector  m_colInfoVec; 	   // 存储列头部的信息	
    /*
    第三部分：
    各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
    数据总数 （Long） 8字节
    数据块额定行数 （int） 4字节
    数据块个数 （int） 4字节 [y]
    */
    _BlockSumary   m_blkSumaryInfo; 	// 块的概况信息
    
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
    unsigned char  *m_pCompressed_data;	
    long            m_compressed_len;
    AutoHandle     *m_dtd;
    int             m_colNum;       // 列数
    AutoStmt       *m_srcst;
    mytimer         m_tm;
    TradeOffMt     *m_mt;
    bool            m_firstEnter;
    bool            m_DirectWriteTable;   
	
protected:// 写文件用
    unsigned char   m_md5sum[conMd5Len];
    FILE           *m_pFile;

protected:
    // Engine BrightHouse表转入使用
    IBDataFile     *m_ib_datafile;
    char            m_ib_dfname[conNameLen];
    write_file_status m_ib_file_status; 
    char            m_sysPath[conNameLen];

protected:
    pthread_mutex_t m_workMutex;                 // 防止上层调用重入
    STATUS          m_status;                    // 内初始化状态
    OPER_TYPE       m_opertype;                  // 操作类型，写数据库或者文件

protected:
    unsigned char*  m_pthreadbuff;                  // 线程使用的工作buffer
    int             m_pbuffcapacity;                // 线程使用的工作buffer容量
    int             m_pthreadbufflen;               // 线程使用工作buff长度
    int             m_pthreadRows;                  // 线程工作的mt记录行数
    int             m_pthreadStatus;                // 线程工作返回状态，上一次的
    ThreadList      m_threadList;                   // 线程池对象
public:
    void            procWorkThread(void * ptr);     // 数据处理工作函数   
    int             procWritePackData(const char* porigin_buff,const int origin_len,const int rownum);  // 写入数据块 
};

#endif
