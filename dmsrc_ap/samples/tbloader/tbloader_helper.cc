#include "tbloader_helper.h"
tbloader_helper::tbloader_helper()
{
    m_pCompressed_data = NULL;
    memset(m_sysPath,0,conNameLen);
    m_srcst = NULL;
    m_mt = NULL;
    m_status = status_unknown;
    m_compressed_len = 0;
    m_firstEnter = false;
    memset(m_md5sum,0,conMd5Len);
    m_running = false;
    m_pFile = NULL;
    m_dtd = NULL;
	memset(m_ib_dfname,0,conNameLen);
    m_ib_file_status = _STATUS_UNKNOWN;
    pthread_mutex_init(&m_workMutex,NULL);
    m_pthreadbuff = NULL;
    m_pbuffcapacity = 0;
    m_pthreadbuff = NULL;
    m_ib_datafile = NULL;
    m_hasPreparedMt = false;
}

tbloader_helper::~tbloader_helper()
{
    if(m_pCompressed_data != NULL){
       free(m_pCompressed_data);
       m_pCompressed_data = NULL;
    }
    if (NULL != m_mt)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if (NULL != m_srcst)
    {
        delete m_srcst;
        m_srcst = NULL;
    }
    if(m_dtd != NULL)
    {
    	delete m_dtd;
        m_dtd = NULL;
    }
    if(m_pthreadbuff != NULL)
    {
       free(m_pthreadbuff);
	   m_pthreadbuff = NULL;
    }
    if(m_ib_datafile != NULL)
    {
       delete m_ib_datafile;
       m_ib_datafile = NULL; 	
    }
    CLOSE_FILE(m_pFile);
	memset(m_ib_dfname,0,256);
    pthread_mutex_destroy(&m_workMutex);
    m_hasPreparedMt = false;
}

int   tbloader_helper::start(_InitInfoPtr pInitObj,int operType,const char* logpth)
{
    AutoLock alk(&m_workMutex); // 确保外面同步调用
    if (m_status != unstart && m_status != hasstoped)
    {
        lgprintf("id=[%d] tbloader_helper::start status = %d error .",m_id,m_status);
        return 1;
    }
    Timer t1(m_id,"tbloader_helper::start 完整过程");
    m_opertype = (OPER_TYPE)operType;
    // 拷贝对象
    memcpy(&m_stInitInfo,pInitObj,sizeof(m_stInitInfo));
    _InitInfoPtr pobj = &m_stInitInfo;    
    
    memset(m_md5sum,0,conMd5Len);
    if(pobj->tbEngine != MyISAM && pobj->tbEngine != Brighthouse)
    {
        lgprintf("id=[%d] table engine type [%d] error .",m_id,pobj->tbEngine);
        return -1;
    }
    m_tm.Clear();
    m_tm.Start();
    
    if(m_opertype == insert_db)
    {
    	Timer t5(m_id,"tbloader_helper::start 插入数据库的整个初始化过程");
        try
        {
        	Timer t2(m_id,"tbloader_helper::start 连接目标数据库");
            //--1.  连接目标数据库
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
            lgprintf("id=[%d] can not connect database [%s--->%s:%s].",m_id,pobj->dsn,pobj->user,pobj->pwd);
            return -1;
        }
        
        char *pDirectWriteTable = getenv("DirectWriteTable");
        m_DirectWriteTable = false;
        if (pDirectWriteTable != NULL && (atoi(pDirectWriteTable) == 1)){
            m_DirectWriteTable = true;
        }
    	
        if(m_ib_datafile != NULL)
        {
            delete m_ib_datafile;	
            m_ib_datafile = NULL; 	
        }
        m_ib_datafile = new IBDataFile();
        m_hasPreparedMt = false;
    }
    else  // 写入文件操作
    {
        dbg_lgprintf("id=[%d] tbloader_helper::start init file oper.",m_id);
        CLOSE_FILE(m_pFile);
        
        m_compressed_len = conAppendMem5MB;
        if(m_pCompressed_data != NULL){
           free(m_pCompressed_data);
           m_pCompressed_data = NULL;
        }
        m_pCompressed_data = (unsigned char *)malloc(m_compressed_len);
    }
    if(m_pbuffcapacity == 0)
    {
        m_pbuffcapacity = conAppendMem5MB*2;
        m_pthreadbuff =(unsigned char*)malloc(m_pbuffcapacity);
    }
    
    // 清空 m_blkSumaryInfo
    m_blkSumaryInfo.blockNum = 0;
    m_blkSumaryInfo.rowNum= 0;
    m_blkSumaryInfo.rowSum = 0;
    m_blkSumaryInfo.BlkStartPosVec.clear();
    m_ib_file_status = _STATUS_UNKNOWN;
    
    // 清空m_colInfoVec
    ReleaseColInfo(m_colInfoVec);
        
    m_status = hasstarted;
    dbg_lgprintf("id=[%d] tbloader_helper::start end.",m_id);
    m_running = true;
    m_ib_dfname[0] = 0;
    return 0; 
}

int   tbloader_helper::stop()
{
    if(!m_running){
        return 0;	
    }
    // 等待数据包处理结束
    int  wait_times = 0;    
    
    Timer t1(m_id,"tbloader_helper::stop 完整过程");
	
    AutoLock alk(&m_workMutex); // 内部等待锁，等待线程执行OK 
    
    // 等待写入线程结束
    m_threadList.Wait();
    
    dbg_lgprintf("id=[%d] tbloader_helper::stop() begin.",m_id);
	// 检测上一个数据包是否写入正确
    if(m_pthreadStatus != 0)
    {
        lgprintf("id=[%d] tbloader_helper::writePackData last pack return %d ",m_id,m_pthreadStatus);
        m_status = hasstoped;
		return m_pthreadStatus;
    }
    int err_code = 0;
    
    // 处理最后一次数据
    if(m_opertype == insert_db)
    {
        try
        {
            if(!m_firstEnter && (m_mt!=NULL) && (m_mt->Cur()->GetRows()>0))
            {
                if(m_stInitInfo.tbEngine == MyISAM) // MyISAM引擎的表写入
                {
                    if(m_DirectWriteTable)
                    {   // 通过直接写表文件的方式，将数据写入数据库中 
                	    dbg_lgprintf("id=[%d] start tbloader_helper::stop to write mt to file,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        if(-1 == DirectWriteMt2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_dtd,m_sysPath,true))
                        {
                            lgprintf("insert to table [%s.%s] error.",m_stInitInfo.dbname,m_stInitInfo.tbname);
                            err_code = -1;
                        }
                        dbg_lgprintf("id=[%d] end tbloader_helper::stop to write mt to file,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                     }
                     else
                     {   // 通过insert插入数据库中记录
                        char tb[500];
                        sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        dbg_lgprintf("id=[%d] start tbloader_helper::stop to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                        dbg_lgprintf("id=[%d] end tbloader_helper::stop to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                     }
                }
                else
                {
                    if(m_DirectWriteTable)
                    {
                        lgprintf("id=[%d] status = %d,向文件 %s 追加 %d 行记录,写入结束[是].",m_id,m_ib_file_status,m_ib_dfname,m_mt->Cur()->GetRows());
                    
                        // 最后一个mt的时候，将数据进行装入	
                        if(m_ib_file_status == _STATUS_WRITING){
                            m_ib_file_status = _STATUS_CLOSE;		
                        }
                        if(0 == GenLoadFileFromMt(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_ib_dfname,m_ib_datafile,m_ib_file_status,true))
                        {
                            if(0 != LoadFile2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,m_dtd,m_sysPath,m_ib_dfname))  
                            { 
                                err_code = -1;
                            }						
                        }
                        else
                        {
                            err_code = -1;
                        }
                    }
                    else
                    {
                        char tb[500];
                        sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        dbg_lgprintf("id=[%d] start tbloader_helper::stop to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                    }
                }
                
                wociReset(*m_mt->Cur());
            }
        }
        catch(...)
        {
         	lgprintf("id=[%d] tbloader_helper::stop insert table[%s.%s] error , m_DirectWriteTable = %d.",
         	     m_id,m_stInitInfo.dbname,m_stInitInfo.tbname,m_DirectWriteTable?1:0);
            err_code = 1;   	
        }
    }
    else
    {   // 写入最后的文件2013/4/19 18:03:24
        dbg_lgprintf("id=[%d] tbloader_helper::writePackData packnum = %d,rowNum=%d,rowSum=%d",m_id,m_blkSumaryInfo.blockNum,m_blkSumaryInfo.rowNum,m_blkSumaryInfo.rowSum);
        if(m_pFile != NULL){
        	if(-1 == WriteFileSumaryInfo(m_pFile,m_blkSumaryInfo)){
                err_code = -1;
            }
        }			       
    }
    m_tm.Stop();    
    lgprintf("id=[%d] 共处理%d行(时间%.2f秒)。",m_id,m_blkSumaryInfo.rowSum,(m_status > unstart) ? m_tm.GetTime():0);
    
    // 清空 m_blkSumaryInfo
    m_blkSumaryInfo.blockNum = 0;
    m_blkSumaryInfo.rowNum= 0;
    m_blkSumaryInfo.rowSum = 0;
    m_blkSumaryInfo.BlkStartPosVec.clear();
    
    // 清空m_colInfoVec
    ReleaseColInfo(m_colInfoVec);
    if(m_pCompressed_data != NULL){
       free(m_pCompressed_data);   
       m_pCompressed_data = NULL; 
    }                             
    
    // 关闭文件
    CLOSE_FILE(m_pFile);
    
    //--2. 构建目标数据库表MT结构
    if(m_mt != NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    m_hasPreparedMt = false;
 
    if(m_ib_datafile != NULL){
    	delete m_ib_datafile;
    	m_ib_datafile = NULL;
    }   	
    
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL; 	
    }     
    
    if(m_srcst != NULL)
    {
        delete m_srcst;
        m_srcst = NULL;
    }
    
    dbg_lgprintf("id=[%d] tbloader_helper::stop() end.",m_id);
    m_status = hasstoped;
    m_running = false;
    m_ib_dfname[0] = 0;
    //m_threadList.Terminate();
    return err_code;
}

/*
return 
0: 成功
-1: 文件写入失败
1: 文件头数据重复调用写入
2: 数据长度校验失败
3: 未初始化 tl_initStorerForFile 或者 tl_initStorerForDB
4: 数据头内容解析错误
5: 数据头和目标表结构不一致
*/
int   tbloader_helper::parserColumnInfo(const HerderPtr header,const int headerlen)
{
	AutoLock alk(&m_workMutex); 
    if (m_status < hasstarted)
    {
        lgprintf("id=[%d] tbloader_helper::parserColumnInfo status[%d] error.",m_id,m_status);
        return 3;
    }
    if (m_status == parserheader)
    {
        lgprintf("id=[%d] tbloader_helper::parserColumnInfo status[%d] error.",m_id,m_status);
        return 1;
    }
    /*
    columnName(列名);
    columnDBType(数据库中类型);
    columnType(java类型);
    columnCName;
    columnPrecision;
    columnScale 
    例如：col1,col2;dbtype1,dbtype2;type1,type2;cc1,cc2;prec1,prec2;scale1,scale2;
    */
    Timer t1(m_id,"tbloader_helper::parserColumnInfo 完整过程");
    if (-1 == ParserColumnInfo(header,m_colInfoVec)){
        lgprintf("id=[%d] Parser column header error ",m_id);
        return 4;
    }
   
    dbg_lgprintf("id=[%d] begin tbloader_helper::parserColumnInfo opertype = %d",m_id,m_opertype);
    if(m_opertype == insert_db)   // 插入数据库中，需要校验头部
    {
    	try
        {
            Timer t2(m_id,"tbloader_helper::parserColumnInfo 校验列宽度");
            //--1. 分配读取文件所需要的缓存buff,及列宽度
            if(m_dtd == NULL)
            {
                lgprintf("id=[%d] tbloader_helper::parserColumnInfo m_dtd == NULL",m_id);
            	return 5;
            }
            AutoMt mt2(*m_dtd,10);
            mt2.FetchFirst("select * from %s.%s limit 2",m_stInitInfo.dbname,m_stInitInfo.tbname);
            mt2.Wait();
            m_colNum = wociGetColumnNumber(mt2);
            lgprintf("校验表结构格式..");
            if(m_colNum == m_colInfoVec.size())
            {
                char cn[256];      
                for(int col = 0;col < m_colNum;col++)
                {        
            		wociGetColumnName(mt2,col,cn);   
            		if(strcasecmp(m_colInfoVec[col]->columnName,cn) !=0)
            		{
                        lgprintf("id=[%d] 数据头结构与表 %s.%s 结构不一致.",m_id,m_stInitInfo.dbname,m_stInitInfo.tbname);
                        return 5;
            		}	
            	} 	
            }
            else
            {
                lgprintf("id=[%d] 数据头结构与表 %s.%s 结构不一致.",m_id,m_stInitInfo.dbname,m_stInitInfo.tbname);
                return 5;
            }
         }
         catch(...)
         {
             lgprintf("id=[%d] tbloader_helper::parserColumnInfo insert_db table %s.%s error.",m_id,m_stInitInfo.dbname,m_stInitInfo.tbname);
         	 return 5;
         }
         
         m_ib_file_status = _STATUS_OPEN; 
         m_ib_dfname[0] = 0; 
    }
    else     
    {   
         Timer t3(m_id,"tbloader_helper::parserColumnInfo 写入包头内容");
         // 写入文件中，直接写入文件即可
         CLOSE_FILE(m_pFile);
         m_pFile = fopen(m_stInitInfo.fn_ext,"wb");
         if( NULL == m_pFile)
         {
    	       lgprintf("id=[%d] Open file %s error ",m_id,m_stInitInfo.fn_ext);
    	       return -1;	
         }
		 lgprintf("id=[%d] Open file %s ok! ",m_id,m_stInitInfo.fn_ext);
    
         // 写入文件头长度
        int _headerlen = headerlen;
        if(-1 == WriteColumnHeaderLen(m_pFile,_headerlen))
        {   
    	     lgprintf("id=[%d] WriteColumnHeaderLen error.",m_id);   
             return -1;
        }
		dbg_lgprintf("id=[%d] write column header len to file %s ok! ",m_id,m_stInitInfo.fn_ext);
    	
    	// 写入文件头数据
        if(-1 == WriteColumnHeader(m_pFile,header,_headerlen))
        { 
    	     lgprintf("id=[%d] WriteColumnHeader error.",m_id);
             return -1;
        }   
		dbg_lgprintf("id=[%d] write column header info to file %s ok! ",m_id,m_stInitInfo.fn_ext);
    }
    dbg_lgprintf("id=[%d] end tbloader_helper::parserColumnInfo opertype = %d",m_id,m_opertype);
    m_status = parserheader;
    m_firstEnter = true;
    m_pthreadStatus = 0;
    return 0;
}

// 返回md5校验值的字符串，32字符
void  tbloader_helper::getMd5Sum(char * pmd5sum)
{
    AutoLock alk(&m_workMutex); 
    char md5array[conMd5StrLen];
    md5array[0]= 0;
    for(int i=0;i<conMd5Len;i++)
    {
        sprintf(md5array+strlen(md5array),"%02x",m_md5sum[i]);
    }
    strcpy(pmd5sum,md5array);
    lgprintf("id=[%d] getMd5Sum[%s].",m_id,md5array);
}


// 线程池调用函数
void pthread_load_func(ThreadList* obj)
{
    tbloader_helper* pThis = (tbloader_helper*)obj->GetParams()[0];
    pThis->procWorkThread((void*)pThis);
}

/*
返回值:
0: 成功
-1: 文件写入失败
1: 文件头未写入过,tl_writeHeadInfo未调用成功过
2: 数据长度校验失败
3: 未初始化 tl_initStorerForFile 或者 tl_initStorerForDB
4: 数据包错误
*/
int   tbloader_helper::writePackData(const char* porigin_buff,const long origin_len,const int rownum)
{
    if (m_status == status_unknown)
    {
        lgprintf("id=[%d] tbloader_helper::parserColumnInfo status[%d] error.",m_id,m_status);
        return 3;
    }
    if (m_status < parserheader)
    {
        lgprintf("id=[%d] tbloader_helper::parserColumnInfo status[%d] error.",m_id,m_status);
        return 1;
    }
    
	AutoLock alk(&m_workMutex);// 阻塞异步调用
	
	m_threadList.Wait();
    
	
    // 校验md5值
    MD5((unsigned char*)porigin_buff,origin_len,m_md5sum);

#ifdef DEBUG
    char md5array[conMd5StrLen];
    md5array[0] = 0;
    for(int i=0;i<conMd5Len;i++)
    {
        sprintf(md5array+strlen(md5array),"%02x",m_md5sum[i]);
    }
    lgprintf("id=[%d] in writePackData getMd5Sum[%s].",m_id,md5array);
#endif	

    // 准备数据
	int  _status = 0;   
    if(origin_len >= m_pbuffcapacity)
    {
        m_pbuffcapacity = origin_len+conAppendMem5MB/3;
        free(m_pthreadbuff);
        m_pthreadbuff = NULL;
        m_pthreadbuff = (unsigned char*)malloc(m_pbuffcapacity);
    }

    // 拷贝数据,长度,记录数,线程状态
    memcpy(m_pthreadbuff,porigin_buff,origin_len);
    m_pthreadbuff[origin_len] = 0;
    m_pthreadbufflen = origin_len;
    m_pthreadRows = rownum;
    // 返回上一次处理的状态
	_status = m_pthreadStatus;

	// 启动线程解析数据
    void *params[2];
    params[0] = (void*)this;
    m_threadList.Start(params,2,pthread_load_func);	
    return _status;
}


// 线程工作函数
void	tbloader_helper::procWorkThread(void * ptr)
{
    tbloader_helper* pThis = (tbloader_helper*)ptr;    
    dbg_lgprintf("id=[%d] start procWorkThread",pThis->m_id);
    pThis->m_pthreadStatus = pThis->procWritePackData((const char*)pThis->m_pthreadbuff,pThis->m_pthreadbufflen,pThis->m_pthreadRows); 
    dbg_lgprintf("id=[%d] exit procWorkThread",pThis->m_id);
}

// 线程函数执行体结构
int tbloader_helper::procWritePackData(const char* porigin_buff,const int origin_len,const int rownum)
{
    if (m_status == status_unknown)
    {
        lgprintf("id=[%d] tbloader_helper::procWritePackData status[%d] error.",m_id,m_status);
        return 3;
    }
    if (m_status < parserheader)
    {
        lgprintf("id=[%d] tbloader_helper::procWritePackData status[%d] error.",m_id,m_status);
        return 1;
    }

    Timer t1(m_id,"tbloader_helper::procWritePackData 完整过程");

    // 在线程中准备mt，能够节省时间
    if(!m_hasPreparedMt && m_opertype == insert_db)
    {
        try
        {        	
            //--2. 构建目标数据库表MT结构
            int _maxrow = min((rownum*10+20),_MAX_ROWS);
            if(m_mt != NULL)
            {
                delete m_mt;
                m_mt = NULL;
            }
            m_mt = new TradeOffMt(0,_maxrow);
            
            if(m_srcst != NULL)
            {
                delete m_srcst;
                m_srcst = NULL;
            }
            m_srcst = new AutoStmt(*m_dtd);
    	
            Timer t4(m_id,"tbloader_helper::start 获取目标表结构");
            m_srcst->Prepare("select * from %s.%s limit 2",m_stInitInfo.dbname,m_stInitInfo.tbname);
            wociBuildStmt(*m_mt->Cur(),*m_srcst,_maxrow);   
            //wociBuildStmt(*m_mt->Next(),*m_srcst,_MAX_ROWS);        // 分配内存
            m_mt->SetPesuado(true);
            m_mt->Cur()->AddrFresh();
            //m_mt->Next()->AddrFresh();          
        }
        catch(...)
        {
            lgprintf("id=[%d] get table structure error[table: %s:%s].",m_id,m_stInitInfo.dbname,m_stInitInfo.tbname);
            m_pthreadStatus=-31;
            return m_pthreadStatus;
        }
        m_hasPreparedMt = true;
    }
	
    // 处理记录行数
    int parserRows = 0;
    
    if(m_opertype == write_files) //<< 插入文件操作
    {
        if (m_compressed_len == 0 || (m_compressed_len <= (origin_len+ conAppendMem5MB/2)) )
        {
            if(m_pCompressed_data != NULL){
               free(m_pCompressed_data);
               m_pCompressed_data = NULL;
            }                    
            m_compressed_len = conAppendMem5MB + origin_len;
            m_pCompressed_data = (unsigned char *)malloc(m_compressed_len);
        }
        
        // 块的起始位置
        long _blokStartPos = ftell(m_pFile);
        m_blkSumaryInfo.BlkStartPosVec.push_back(_blokStartPos);
        
        // 修改压缩缓存
        long compressed_len = m_compressed_len;
        dbg_lgprintf("id=[%d] begin tbloader_helper::writePackData  to write data compressed_len = %d,origin_len=%d.",m_id,compressed_len,origin_len);
        if(0 != WriteBlockData(m_pFile,_blokStartPos,(char*)m_pCompressed_data,compressed_len,(const char*)porigin_buff,origin_len))
        {
            lgprintf("id=[%d] WriteBlockData return error.",m_id);
            return -31;
        }
        dbg_lgprintf("id=[%d] end tbloader_helper::writePackData  to write data compressed_len = %d,origin_len=%d.",m_id,compressed_len,origin_len);
       
        // 该包的记录行数
        parserRows = rownum;
    	
    }
    else //<< 插入数据库操作
    {    
    	if(m_mt == NULL)                  
        { 
            lgprintf("id=[%d] tbloader_helper::procWritePackData m_mt==NULL error.",m_id);
            return -10;
        }
        try
        {
            if(!m_firstEnter && (m_mt->Cur()->GetRows() + m_blkSumaryInfo.rowNum) >= m_mt->Cur()->GetMaxRows())
            {
                if(m_stInitInfo.tbEngine == MyISAM) // MyISAM引擎的表写入
                {
                    if(m_DirectWriteTable)
                    {   // 通过直接写表文件的方式，将数据写入数据库中 
                        dbg_lgprintf("id=[%d] start tbloader_helper::writePackData to write mt to file,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        if(-1 == DirectWriteMt2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_dtd,m_sysPath,false))
                        {
                            lgprintf("insert to table [%s.%s] error.",m_stInitInfo.dbname,m_stInitInfo.tbname);
                            return -21;
                        }
                        dbg_lgprintf("id=[%d] end tbloader_helper::writePackData to write mt to file,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                    }
                    else
                    {   // 通过insert插入数据库中记录
                        char tb[500];
                        sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        dbg_lgprintf("id=[%d] start tbloader_helper::writePackData to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                        dbg_lgprintf("id=[%d] start tbloader_helper::writePackData to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                    }
                }
                else //if(m_stInitInfo.tbEngine == Brighthouse)  // brighthouse引擎的表
                {
                    if(m_DirectWriteTable)
                    { 
                       // 不断的写入待转入的文件              
                       dbg_lgprintf("id=[%d] status = %d,向文件 %s 追加 %d 行记录,写入结束[否].",m_id,m_ib_file_status,m_ib_dfname,m_mt->Cur()->GetRows());
                       if(0 != GenLoadFileFromMt(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_ib_dfname,m_ib_datafile,m_ib_file_status,false))
                       {
                           // 最后文件写完在进行装入    
                           // if(0 != LoadFile2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,m_psa,fname))  
                           // { 
                           //    return -1;
                           // }
                          return -1;
                       }
                       else
                       {
                           if(m_ib_file_status == _STATUS_OPEN){
                               m_ib_file_status = _STATUS_WRITING;    	
                       	   }
                       }
                    }// end if(m_DirectWriteTable)
                    else
                    {
                        char tb[500];
                        sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        dbg_lgprintf("id=[%d] start tbloader_helper::writePackData to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                        wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                        dbg_lgprintf("id=[%d] start tbloader_helper::writePackData to write mt into table,mt rows = %d.",m_id,m_mt->Cur()->GetRows());
                    }
                }
                wociReset(*m_mt->Cur());
            }
            
            // 文件名称和状态确定
            if(m_stInitInfo.tbEngine == Brighthouse && m_firstEnter)  // brighthouse引擎的表
            {
               if(m_DirectWriteTable)
               { 
                  // 防止多进程转入文件重名
                  if(m_ib_file_status == _STATUS_OPEN || strlen(m_ib_dfname) == 0)
                  {
                      char current_tm[8];
                      wociGetCurDateTime(current_tm);
                      int yy,mm,dd,hh,mi,ss;
                      yy = wociGetYear(current_tm);
                      mm = wociGetMonth(current_tm);
                      dd = wociGetDay(current_tm);
                      hh = wociGetHour(current_tm);
                      mi = wociGetMin(current_tm);
                      ss = wociGetSec(current_tm);
                      sprintf(m_ib_dfname,"%s.%s.datafile[%04d%02d%02d-%02d%02d%02d].data",
                        m_stInitInfo.dbname,m_stInitInfo.tbname,yy,mm,dd,hh,mi,ss);
                  }
               }
            }
            
            m_firstEnter = false;
            dbg_lgprintf("id=[%d] start tbloader_helper::writePackData  to parse data origin_len = %d.",m_id,origin_len);
            parserRows = ParserOriginData((char*)porigin_buff,origin_len,*m_mt->Cur(),m_colInfoVec);
            dbg_lgprintf("id=[%d] end tbloader_helper::writePackData  to parse data origin_len = %d.",m_id,origin_len);
            if (parserRows<=0)
            {
                lgprintf("Do ParserOriginData return error.");
                return parserRows;
            }
            if(rownum != -1 && parserRows != rownum)
            {
                lgprintf("ParserOriginData return rows %d != input row %d .",parserRows,rownum); 	
            }      
        }
        catch(...)
        {
        	lgprintf("tbloader_helper::stop insert table[%s.%s] error , m_DirectWriteTable = %d.",  
        	     m_stInitInfo.dbname,m_stInitInfo.tbname,m_DirectWriteTable?1:0);                   
            return -22;	
        }
    }
    
    m_blkSumaryInfo.rowSum += parserRows;
    m_blkSumaryInfo.blockNum ++;
    if(m_blkSumaryInfo.blockNum+1 % 1000 ==0){
        lgprintf("id=[%d] write pack %d ....",m_id,m_blkSumaryInfo.blockNum);
    }
        
    // 设置额定记录行数
    if(m_blkSumaryInfo.rowNum <= parserRows){
        m_blkSumaryInfo.rowNum = parserRows;
    }
	
    lgprintf("id=[%d] tbloader_helper::writePackData packnum = %d,rowNum=%d,rowSum=%d",m_id,m_blkSumaryInfo.blockNum,m_blkSumaryInfo.rowNum,m_blkSumaryInfo.rowSum);
    
    m_status = writedata;    
    return 0;
}


