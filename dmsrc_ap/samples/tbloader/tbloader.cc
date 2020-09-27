#include "tbloader_helper.h"

/*
     func:Start_loadfile(void *ptr)
     desc:从文件装载入库
     param:
          ptr[in]:连接数据库等相关信息
*/
int Start_loadfile(void *ptr);

/*
     func:Start_loadfile_new(void* ptr)
     desc : 从文件装入数据库
     param:
         ptr[in]:连接数据库等相关信息
*/
int Start_loadfile_new(void* ptr);

/*
     func:Start_genfile(void *ptr)
     desc:从文件读取文件，到生成文件
     param:
          ptr[in]:连接数据库等相关信息
*/
int Start_genfile(void *ptr);


/*
   func:HelperDisplay()
   desc:帮助信息显示
*/
void HelperDisplay();

int main(int argc,char *argv[])  
{                   
    if(argc < 2){
        printf("tbloader:调用参数输入错误.\n");
		HelperDisplay();
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
	strcpy(stInitInfo.funcode,argv[index++]);
	if(strcasecmp(stInitInfo.funcode,"-h") == 0 || strcasecmp(stInitInfo.funcode,"-help") == 0)
	{
        HelperDisplay();
		return 0;
	}
	else if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 && argc == 9)
	{
        strcpy(stInitInfo.dsn,argv[index++]);
        strcpy(stInitInfo.user,argv[index++]);
        strcpy(stInitInfo.pwd,argv[index++]);
        strcpy(stInitInfo.dbname,argv[index++]);
        strcpy(stInitInfo.tbname,argv[index++]);
        if(strcasecmp("MyISAM",argv[index]) == 0)
        {
            stInitInfo.tbEngine = MyISAM;
        }
        else if(strcasecmp("BrightHouse",argv[index]) == 0)
        {
            stInitInfo.tbEngine = Brighthouse;        
        }
        else
        {              
            printf("tbloader:调用参数输入错误.数据库表引擎类型<MyISAM,BrightHouse>错误.\n");
            HelperDisplay();
            return 0;
        }
        index++;
        strcpy(stInitInfo.fn,argv[index++]);
	}
	else if(strcasecmp(stInitInfo.funcode,"-gen_file") == 0 && argc == 9)
	{
        strcpy(stInitInfo.dsn,argv[index++]);
        strcpy(stInitInfo.user,argv[index++]);
        strcpy(stInitInfo.pwd,argv[index++]);
        strcpy(stInitInfo.dbname,argv[index++]);
        strcpy(stInitInfo.tbname,argv[index++]);
        strcpy(stInitInfo.fn,argv[index++]);
		strcpy(stInitInfo.fn_ext,argv[index++]);
	}
	else
	{
        printf("tbloader:调用参数输入错误.\n");
		HelperDisplay();
        return 0;	
	}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    int nRetCode = 0;
    WOCIInit("tbloader");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);	
#ifndef TBLOADER_HELPER_TEST
	if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 ){
        nRetCode=wociMainEntrance(Start_loadfile,true,(void*)&stInitInfo,2);
	}
	else if(strcasecmp(stInitInfo.funcode,"-gen_file") == 0){
		nRetCode=wociMainEntrance(Start_genfile,true,(void*)&stInitInfo,2);
	}
#else
    if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 ){
        nRetCode=Start_loadfile_new((void*)&stInitInfo);
	}
#endif

    WOCIQuit(); 
    
    return nRetCode;
}


/*
     func:Start_loadfile(void *ptr)
     desc:从文件装载入库
     param:
          ptr[in]:连接数据库等相关信息
*/
int Start_loadfile(void *ptr) 
{ 
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
	_ColumnInfoPtrVector  g_colInfoVec; 	   // 存储列头部的信息
	
	
	/*
	第三部分：
	各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
	数据总数 （Long） 8字节
	数据块额定行数 （int） 4字节
	数据块个数 （int） 4字节 [y]
	*/
	_BlockSumary   g_blkSumaryInfo; 	// 块的概况信息
	
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
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer

	_InitInfoPtr pobj = (_InitInfoPtr)ptr;
    AutoHandle dtd;
	mytimer tm;
	tm.Start();
    
	//--1.  连接目标数据库
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));

    char m_sysPath[conNameLen];	
    m_sysPath[0] = 0;
    LoadSysPath(m_sysPath);
    
    //--2. 构建目标数据库表MT结构
    TradeOffMt mt(0,_MAX_ROWS);
    AutoStmt srcst(dtd);
    srcst.Prepare("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
    wociBuildStmt(*mt.Cur(),srcst,_MAX_ROWS);
    //wociBuildStmt(*mt.Next(),srcst,_MAX_ROWS);
    mt.SetPesuado(true);
    mt.Cur()->AddrFresh();
    //mt.Next()->AddrFresh();
    
    //--3. 打开文件
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}

    //--4. 获取文件的列信息(列名称等信息,列数目,存入队列)
    if (-1 == GetColumnInfo2Vector(pFile,g_colInfoVec)){ 
        ReleaseColInfo(g_colInfoVec);
        fclose(pFile);pFile = NULL;
        return -1;
    }
    
    //--5. 获取文件的概况信息(文件大小，数据块数目，每一个数据块行数，每一个数据库的起始位置)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
    
    //--6. 分配读取文件所需要的缓存buff,及列宽度
	int rowLen = 0;
    int colNum = 0;
    AutoMt mt2(dtd,10);
    mt2.FetchFirst("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
	mt2.Wait();
	rowLen = wociGetRowLen(mt2);
    colNum = wociGetColumnNumber(mt2);
    lgprintf("校验表结构格式...");
    if(colNum == g_colInfoVec.size())
    {
       char cn[256];      
       for(int col = 0;col < colNum;col++)
       {        
       	   wociGetColumnName(mt2,col,cn);   
           if(strcasecmp(g_colInfoVec[col]->columnName,cn) !=0)
           {
           	  lgprintf("文件%s 结构与表 %s.%s 结构不一致，请重新选择文件.",pobj->fn,pobj->dbname,pobj->tbname);
       	      ReleaseColInfo(g_colInfoVec);
		      fclose(pFile);pFile = NULL;
		      return -1;
           }	
       } 	
    }
    else
    {
    	lgprintf("文件%s 结构与表 %s.%s 结构不一致，请重新选择文件.",pobj->fn,pobj->dbname,pobj->tbname);
       	ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
	long  _compressed_len,compressed_len;
	long  _origin_len,origin_len;
    long  _min_rows = g_blkSumaryInfo.rowNum > g_blkSumaryInfo.rowSum ? g_blkSumaryInfo.rowSum :g_blkSumaryInfo.rowNum;
	_compressed_len = LocateBuff(_min_rows,rowLen,colNum,&g_pCompressed_data);
	_origin_len = LocateBuff(_min_rows,rowLen,colNum,&g_pOrigin_data);

    //--7. 从文件中解析数据，填充MT并将MT中数据Append到数据库中
    lgprintf("读取文件解析入库...");
	bool  has_error = false;
	bool  firstEnter = true;
	long  deal_rows = 0;   // 已经处理的记录行数
	long  rn = 0;
	int  blkIdx = 0;       // block 索引，开始处理数据
	long  block_start_pos;
    bool  dealfinish = false;  // 处理结束
	while ( (rn>0 && !has_error && !firstEnter) || firstEnter)
	{
		if(!firstEnter)
		{
            char *pDirectWriteTable = getenv("DirectWriteTable");
            bool b_DirectWriteTable = false;
            if (pDirectWriteTable != NULL && (atoi(pDirectWriteTable) == 1)){
                b_DirectWriteTable = true;
            }
            if(b_DirectWriteTable)
            {   // 通过直接写表文件的方式，将数据写入数据库中 
                if(-1 == DirectWriteMt2Table(pobj->dbname,pobj->tbname,*mt.Cur(),&dtd,m_sysPath,dealfinish/*是否是最后一个mt*/))
				{
				    lgprintf("insert to table [%s.%s] error.",pobj->dbname,pobj->tbname);
					has_error = true;
				}
            }
			else
            {   // 通过insert插入数据库中记录
			    char tb[500];
                sprintf(tb,"%s.%s",pobj->dbname,pobj->tbname);
                wociAppendToDbTable(*mt.Cur(),tb,dtd,true);
            }
			
            wociReset(*mt.Cur());
		}
		firstEnter = false;
        if(dealfinish){
        	break;
        }

		while(1)
		{
			compressed_len = origin_len = _compressed_len;
			if (dealfinish){
				break;			
			}

			block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
			
            if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // 最后一个数据块处理
				dealfinish = true;
            }

			if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
			{
				lgprintf("get GetBlockData return error.");
				has_error = true;
				break;
			}
			else
			{
				int parserRows = ParserOriginData((char*)g_pOrigin_data,origin_len,*mt.Cur(),g_colInfoVec);
				if (parserRows<=0)
				{
					lgprintf("get ParserOriginData return error.");
					has_error = true;
					break;
				}
				else
				{
					deal_rows += parserRows;
					if ( (mt.Cur()->GetRows() + g_blkSumaryInfo.rowNum) >= mt.Cur()->GetMaxRows()) // 判断记录数是否满足
					{
						// 一个 mt 已经装满数据了，准备进行写入数据了
						break;
					}					 
				}
			}// end else
		} // end while(1)
		rn = mt.Cur()->GetRows();
	} // while (rn > 0 && !has_error)     

	tm.Stop();
	if(!has_error){
        printf("\n共处理%d行(时间%.2f秒)。\n",deal_rows,tm.GetTime());
	}

    //--8. 释放已经分配好的内存资源
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	ReleaseColInfo(g_colInfoVec);
	fclose(pFile);pFile = NULL;

    return 1;
}



/*
     func:Start_genfile(void *ptr)
     desc:从文件读取文件，到生成文件
     param:
          ptr[in]:连接数据库等相关信息
*/
int Start_genfile(void *ptr)
{ 
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
	_ColumnInfoPtrVector  g_colInfoVec; 	   // 存储列头部的信息
	
	
	/*
	第三部分：
	各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
	数据总数 （Long） 8字节
	数据块额定行数 （int） 4字节
	数据块个数 （int） 4字节 [y]
	*/
	_BlockSumary   g_blkSumaryInfo; 	// 块的概况信息
	_BlockSumary   g_blkSumaryInfo_ext;     // 导出文件的块的概况信息
	
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
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer
	
	_InitInfoPtr pobj = (_InitInfoPtr)ptr;
    AutoHandle dtd;
	mytimer tm;
	tm.Start();
    
	//--1.  连接目标数据库
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    
    //--2. 打开文件
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}
	FILE *pFileExt = NULL;
	pFileExt = fopen(pobj->fn_ext,"wb");
	if( NULL == pFileExt){
		lgprintf("Open file %s error \n",pobj->fn_ext);
		CLOSE_FILE(pFile);
		return -1;	
	}

    //--3. 获取文件的列信息(列名称等信息,列数目,存入队列)
	// 3.1 read header len
	int headLen = 0;
	headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
    }
    if(-1 == WriteColumnHeaderLen(pFileExt,headLen))
    {    
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
        return -1;
	}
	
	// 3.2 read header
	HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
	}
    if(-1 == WriteColumnHeader(pFileExt,header,headLen))
    {
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
    }
	
	// 3.3 parser column info
    if (-1 == ParserColumnInfo(header,g_colInfoVec)){
		lgprintf("Parser file header error \n");
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		ReleaseBuff(header);
		return -1;
    }
    ReleaseBuff(header);

    
    //--4. 获取文件的概况信息(文件大小，数据块数目，每一个数据块行数，每一个数据库的起始位置)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
    
    //--5. 分配读取文件所需要的缓存buff,及列宽度
	int rowLen = 0;
    int colNum = 0;
    AutoMt mt2(dtd,10);
    mt2.FetchFirst("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
	mt2.Wait();
	rowLen = wociGetRowLen(mt2);
    colNum = wociGetColumnNumber(mt2);
    lgprintf("校验表结构格式...");
    if(colNum == g_colInfoVec.size())
    {
       char cn[256];      
       for(int col = 0;col < colNum;col++)
       {        
       	   wociGetColumnName(mt2,col,cn);   
           if(strcasecmp(g_colInfoVec[col]->columnName,cn) !=0)
           {
           	  lgprintf("文件%s 结构与表 %s.%s 结构不一致，请重新选择文件.",pobj->fn,pobj->dbname,pobj->tbname);
       	      ReleaseColInfo(g_colInfoVec);
		      fclose(pFile);pFile = NULL;
		      return -1;
           }	
       } 	
    }
    else
    {
    	lgprintf("文件%s 结构与表 %s.%s 结构不一致，请重新选择文件.",pobj->fn,pobj->dbname,pobj->tbname);
       	ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
	long  _compressed_len,compressed_len;
	long  _origin_len,origin_len;
    long  _min_rows = g_blkSumaryInfo.rowNum > g_blkSumaryInfo.rowSum ? g_blkSumaryInfo.rowSum :g_blkSumaryInfo.rowNum;
	_compressed_len = LocateBuff(_min_rows,rowLen,colNum,&g_pCompressed_data);
	_origin_len = LocateBuff(_min_rows,rowLen,colNum,&g_pOrigin_data);

    //--7. 从文件中解析数据，压缩后写入文件
    lgprintf("读取文件解析转存文件...");
	bool  has_error = false;
	long  rn = 0;
	int  blkIdx = 0;       // block 索引，开始处理数据
	long  block_start_pos;
    bool  dealfinish = false;  // 处理结束
	while (!has_error)
	{
	    compressed_len = origin_len = _compressed_len;
		if (dealfinish){
			break;			
		}

		block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
        if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // 最后一个数据块处理
			dealfinish = true;
        }

		if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
		{
			lgprintf("get GetBlockData return error.");
			has_error = true;
			break;
		}
		else
		{
		    // 块的起始位置
		    long _blokStartPos = ftell(pFileExt);
			g_blkSumaryInfo_ext.BlkStartPosVec.push_back(_blokStartPos);

			// 修改压缩缓存
			compressed_len = _compressed_len;
			if(0 != WriteBlockData(pFileExt,_blokStartPos,(char*)g_pCompressed_data,compressed_len,(const char*)g_pOrigin_data,origin_len))
			{
                lgprintf("get WriteBlockData return error.");
  			    has_error = true;
				break;
			}
            if(blkIdx %(g_blkSumaryInfo.blockNum / 10 + 1) == 0 || blkIdx == g_blkSumaryInfo.blockNum){
			    lgprintf("正在转换处理第%d个包，共计%d个包...",blkIdx,g_blkSumaryInfo.blockNum);
            }
		}// end else
	} // end while(1)
	
    //--8. 从文件中解析数据，压缩后写入文件
    if(!has_error)
	{
        g_blkSumaryInfo_ext.blockNum = g_blkSumaryInfo.blockNum;
    	g_blkSumaryInfo_ext.rowNum = g_blkSumaryInfo.rowNum;
	    g_blkSumaryInfo_ext.rowSum = g_blkSumaryInfo.rowSum;	
        if(-1 == WriteFileSumaryInfo(pFileExt,g_blkSumaryInfo_ext)){
            has_error = true;
        }			
	}
	tm.Stop();
	if(!has_error){
        printf("\n共处理%d行(时间%.2f秒)。\n",g_blkSumaryInfo.rowSum,tm.GetTime());
	}

    //--9. 释放已经分配好的内存资源
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	ReleaseColInfo(g_colInfoVec);
	CLOSE_FILE(pFile);
	CLOSE_FILE(pFileExt);

    return 1;
}




/*
   func:HelperDisplay()
   desc:帮助信息显示
*/
void HelperDisplay()
{
   const char * pMsg = "tbloader 帮助信息提示\n"
   	"参数信息:\n\n"
   	"-help \n"
   	"         帮助信息\n\n"
    "-load_file\n"
    "         [ODBC数据源][数据库用户名][数据库密码][数据库名称][表名称][引擎类型][装入的文件]\n"
    "         example : tbloader -load_file dsn user pwd dbname tbname engine<MyISAM,BrightHouse> fn\n\n"
#ifndef TBLOADER_HELPER_TEST
    "-gen_file\n"    
    "         [ODBC数据源][数据库用户名][数据库密码][数据库名称][表名称][装入的文件][生成的文件]\n"
    "         example : tbloader -gen_file dsn user pwd dbname tbname fin fout\n\n";
#else
    "tbloader_test 程序\n";
#endif
   printf(pMsg);
}

//---------------------------------------------------------------------------------

/*
     func:Start_loadfile_new(void* ptr)
     desc : 从文件装入数据库
     param:
         ptr[in]:连接数据库等相关信息
*/
int Start_loadfile_new(void* ptr)
{	
    _BlockSumary   g_blkSumaryInfo; 	// 块的概况信息
	
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer

	_InitInfoPtr pobj = (_InitInfoPtr)ptr;

	// tbloader_helper 对象
	tbloader_helper tbloader_obj;
	int test_times = 0;
    char sysPath[conNameLen];
    LoadSysPath(sysPath);
test_begin:
    printf("test times %d \n",test_times++);
	tbloader_obj.setId(test_times);
	tbloader_obj.setStatus(unstart);
    tbloader_obj.setSysPath(sysPath);
	if(0 != tbloader_obj.start(pobj,insert_db,"tbloader"))
    {
        return -1;
    }             
    //--3. 打开文件
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}

       // 3.1 read header len
	int headLen = 0;
	headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
	    CLOSE_FILE(pFile);
	    return -1;
    }	
	// 3.2 read header
	HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		return -1;
	}

    // 设置头部
    if(0 != tbloader_obj.parserColumnInfo(header, headLen))
    {		
       ReleaseBuff(header);		
       CLOSE_FILE(pFile);
       return -1;
    }
    
    //--5. 获取文件的概况信息(文件大小，数据块数目，每一个数据块行数，每一个数据库的起始位置)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		CLOSE_FILE(pFile);
		return -1;
    }
    
    //--6. 分配读取文件所需要的缓存buff,及列宽度
    long  _compressed_len,compressed_len;
    long  _origin_len,origin_len;

    // 这个是测试的，内存设置为10M
    _compressed_len = _origin_len= conAppendMem5MB*2;
	
    g_pCompressed_data = (unsigned char *)malloc(_compressed_len);
    g_pOrigin_data = (unsigned char *)malloc(_compressed_len);

    //--7. 从文件中解析数据，填充MT并将MT中数据Append到数据库中
    lgprintf("读取文件解析入库...");
	bool  has_error = false;
	long  deal_rows = 0;   // 已经处理的记录行数
	long  rn = 0;
	int  blkIdx = 0;       // block 索引，开始处理数据
	long  block_start_pos;
    bool  dealfinish = false;  // 处理结束
    while(!has_error)
    {
    	compressed_len = origin_len = _compressed_len;
    	if (dealfinish){
    		break;			
    	}
    
    	block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
    	
        if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // 最后一个数据块处理
    		dealfinish = true;
        }
    	                     
    	if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
    	{
    		lgprintf("get GetBlockData return error.");
    		has_error = true;
    		break;
    	}
    	else
    	{
    	     int _test_row_num = -1; // -1表示，不校验记录行数
    	     if(0!=tbloader_obj.writePackData((const char*)g_pOrigin_data,origin_len,_test_row_num))
    	     {
    	         has_error= true;
    	         break;
    	     }
             deal_rows += _test_row_num;
    	}// end else
    } // end while(1)
    
    int _ret = 0;
    _ret = tbloader_obj.stop();
    
    //--8. 释放已经分配好的内存资源
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	CLOSE_FILE(pFile);
	if(!has_error &&_ret==0){
        goto test_begin;
    }
    return 1;
}
