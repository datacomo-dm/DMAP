#ifndef TBEXPORT_DEF_H
#define	TBEXPORT_DEF_H
#include "tbloader_impl.h"

/********************************************************************
  file : tbexport.h
  desc : define the export table to cvs file internal function
  author: liujianshun,201304
********************************************************************/
enum EXPROT_STATUS{exp_unstart,exp_hasstarted,exp_doing,exp_hasstoped};
class tbexport:public pub_info
{
public:
    tbexport();
    virtual ~tbexport();

public:
    int   start(_InitInfoPtr pInitObj,const char* psql,const char* logpth);   // 初始化
    int   stop();                                                       // 卸载
    int   doStart();     // 开始启动线程导出数据
    inline long getRowSum(){ return m_rowSum;};
    inline long getCurrentRows(){ return m_curRowIdx;};
    
    inline EXPROT_STATUS getStatus(){return m_status;};
    inline void setStatus(EXPROT_STATUS st){m_status=st;};
	
protected:
    ThreadList      m_threadList;                // 线程池对象

public:
    void         procExportData(void * ptr);     // 导出数据线程函数    
    
protected:	
    // 初始化信息
    _InitInfo            m_stInitInfo;  
    char           *m_psql;   
    AutoHandle     *m_dtd;
    long            m_rowSum;       // 文件行数
    long            m_curRowIdx;    // 当前导出行数
    AutoStmt       *m_srcst;
    mytimer         m_tm;
    TradeOffMt     *m_mt;
	
protected:
    EXPROT_STATUS   m_status;                    // 内初始化状态
};

#endif
