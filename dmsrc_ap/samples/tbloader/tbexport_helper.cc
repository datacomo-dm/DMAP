#include "tbexport_helper.h"


tbexport::tbexport()
{
    m_psql = NULL;
    m_mt = NULL;
    m_curRowIdx = 0;
    m_srcst = NULL;
    m_dtd = NULL;
    m_psql = NULL;
    m_status = exp_unstart;
    m_rowSum = 0;
    m_hasPreparedMt = false;
}

tbexport::~tbexport()
{
    if(m_mt != NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if(m_srcst != NULL)
    {
        delete m_srcst;
        m_srcst = NULL;	
    }
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL;
    }
    m_curRowIdx = 0;
    ReleaseBuff(m_psql);
    m_rowSum = 0;
    m_hasPreparedMt = false;
}

void StrToLower(char *str) {
    while(*str!=0) 
    {
        *str=tolower(*str);
        str++;
    }
}
/*
-1: 数据库连接失败 
1: 已经初始化过，不能重复调用
0: 成功
*/
int   tbexport::start(_InitInfoPtr pInitObj,const char* psql,const char* logpth)
{
    if (m_status != exp_unstart&& m_status != exp_hasstoped)
    {
        return 1;
    }
    // 拷贝对象
    memcpy(&m_stInitInfo,pInitObj,sizeof(m_stInitInfo));
    _InitInfoPtr pobj = &m_stInitInfo;
       
    m_tm.Clear();
    m_tm.Start();
    
    //--1.  连接目标数据库
    try
    {
        if(m_dtd != NULL)
        {
            delete m_dtd;
            m_dtd = NULL; 	
        }
        m_dtd = new AutoHandle();
        m_dtd->SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    }
    catch(...)
    {
        lgprintf("id=[%d] can not connect db[%s-->%s:%s]",m_id,pobj->dsn,pobj->user,pobj->pwd);
        return -1;
    }
    
    //--2. 获取总的记录数
    if(m_psql == NULL){
        m_psql = (char*)malloc(conAppendMem1MB);
    }
    strcpy(m_psql,psql);
    StrToLower(m_psql);
    std::string strsql = m_psql;
    int len = strsql.size();
    int from_pos = strsql.find("from");
    int limit_pos = strsql.find("limit");
    AutoMt _mt(*m_dtd,10);
    try
    {
        if(limit_pos != std::string::npos) // 存在limit的情况
        {
            _mt.FetchFirst("select count(1) ct from ( %s ) __www_datacomo_com_tbexport_",strsql.c_str());
        }
        else  // 不存在limit的情况
        {           
            _mt.FetchFirst("select count(1) ct from %s",strsql.substr(from_pos+5,len-from_pos).c_str());
        }
        _mt.Wait();
        m_rowSum = _mt.GetLong("ct",0);
    }
    catch(...)
    {
        lgprintf("id=[%d] execute sql:[select count(1) ct from  %s] error.",m_id,strsql.substr(from_pos+5,len-from_pos-5).c_str());
        return -1;	
    }
    
    //--3. 构建目标数据库表MT结构
    if(m_mt == NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    m_mt = new TradeOffMt(0,_MAX_ROWS);
    
    if(m_srcst == NULL)
    {
        delete m_srcst;
        m_srcst = NULL;
    }	
    m_srcst = new AutoStmt(*m_dtd);

    m_hasPreparedMt = false;
	
    m_status = exp_hasstarted;
    m_running = true;
    return 0; 
}

int   tbexport::stop()
{     
    if(!m_running){
    	return 0;
    }
    if(m_status != exp_doing){
        lgprintf("id=[%d] do not begin to export data .",m_id);
        return -1;
    }
    m_threadList.Wait(); // 等待线程退出        
    m_status = exp_hasstoped;
    if(m_mt != NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if(m_srcst != NULL)
    {
        delete m_srcst;
        m_srcst = NULL;	
    }
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL;
    }
    lgprintf("id=[%d] export csv finish.",m_id);
    m_running = false;
    return 0;
}

// 线程池调用函数
void pthread_export_func(ThreadList* obj)
{
    tbexport* pThis = (tbexport*)obj->GetParams()[0];
    pThis->procExportData((void*)pThis);
}

// 启动线程开始导出数据
int   tbexport::doStart()
{
    if(m_status == exp_doing)
    {
        lgprintf("id=[%d] do exporting...",m_id);
        return 0;
    }
    
    if(m_status != exp_hasstarted)
    {
        lgprintf("id=[%d] can not do exporting,status = %d",m_id,m_status);
        return -1;
    }
    
    void * params[2];
    params[0] = (void*)this;
    m_threadList.Start(params,2,pthread_export_func);
    
    lgprintf("id=[%d] begin to exporting cvs .",m_id);
    
    m_status = exp_doing;
    m_tm.Clear();
    m_tm.Start();
    return 0;
}

void  tbexport::procExportData(void * ptr)
{
    tbexport* pobj = (tbexport*)ptr;
	if(!pobj->m_hasPreparedMt)
    {
        pobj->m_srcst->Prepare(m_psql);
        pobj->m_mt->Cur()->Build(*m_srcst);
        pobj->m_mt->Next()->Build(*m_srcst);
        pobj->m_hasPreparedMt = true;
    }
	
    pobj->m_mt->FetchFirst();
    int rrn = pobj->m_mt->Wait();
	bool has_title = true;
	pobj->m_curRowIdx = 0;
    while(rrn>0)
    {
        pobj->m_mt->FetchNext();
        wociMTToTextFileStr(*pobj->m_mt->Cur(),pobj->m_stInitInfo.fn_ext,rrn,NULL,has_title);
		has_title = false;
        pobj->m_curRowIdx+=rrn;  
        lgprintf("id=[%d] export csv file[%s] rows[%d],sum rows[%d].",pobj->m_id,pobj->m_stInitInfo.fn_ext,pobj->m_curRowIdx,pobj->m_rowSum);    
        rrn=pobj->m_mt->Wait();
    }
    pobj->m_tm.Stop();
    lgprintf("id=[%d] 共处理%d行(时间%.2f秒)。\n",pobj->m_id,pobj->m_curRowIdx,pobj->m_tm.GetTime());
    pobj->m_running = exp_hasstoped;
}

