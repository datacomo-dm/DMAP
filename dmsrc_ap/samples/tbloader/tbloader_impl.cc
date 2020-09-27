#include "tbloader_impl.h"


bool g_logInit = false;      // 全局日志初始化标志,在tbloader_jni和tbexporter.jni中用
char g_sysPath[conNameLen];  // 系统路径

/*
    func:ReleaseBuff 
    desc:release malloc locate memory
*/
void ReleaseBuff(void* p){
    if(NULL != p){
        free(p); p = NULL;
    }      
}

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
int LocateBuff(const int rowNum,const int rowLen,const int colnum/* 列*/,unsigned char** pbuff)
{
   ReleaseBuff(*pbuff);	
   int len = rowNum*rowLen +(colnum * 4/* 字符串类型，前4字节为字符串长度 */ + 4/* 每行数据大小 */ + colnum/*列空情况*/);
   len += conAppendMem5MB;   // 加大5MB
   *pbuff = (unsigned char*)malloc(len);
   return len;
}


/*
    func: GetColumnHeaderLen(FILE* pFile,char * pHeader)
    desc: 获取文件的头部长度
    param:
           pFile[in]: 数据块文件句柄
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeaderLen(FILE* pFile)
{
    // read header len
    int headLen = 0;
    if(1 != fread(&headLen,sizeof(int),1,pFile)){
        lgprintf("Read file header len error\n");
        return -1;
    }
    _revInt(&headLen);
    return headLen;
}
int  WriteColumnHeaderLen(FILE* pFile,int headerlen)
{
    _revInt(&headerlen);
    if(1 != fwrite(&headerlen,sizeof(int),1,pFile)){
        lgprintf("Write file header len error\n");
        return -1;
    }
    _revInt(&headerlen);
    return headerlen;
}


/*
    func: GetColumnHeader(FILE* pFile,char * pHeader)
    desc: 获取文件的头部
    param:
           pFile[in]: 数据块文件句柄
           pHeader[in/out]: 文件头部
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen)
{
    if(1 != fread(pHeader,headerlen,1,pFile)){
        lgprintf("Read file header error \n");
        return -1;
    }
    return headerlen;
}
int  WriteColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen)
{
    if(1 != fwrite(pHeader,headerlen,1,pFile)){
        lgprintf("Write file header error \n");
        return -1;
    }
    return headerlen;
}


/*   
     func: GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
     desc: 获取列的相关信息，存入队列中
     param:
           pFile[in]: 数据块文件打开句柄
           colInfoVec[in/out]:存储数据块列信息的队列
     return: success 0,error -1
*/
int  GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
{
    // read header len
    int headLen = 0;
    headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
        return -1;
    }
    
    
    // read header
    HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
        ReleaseBuff(header);
        return -1;
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
    if (-1 == ParserColumnInfo(header,colInfoVec)){
        lgprintf("Parser file header error \n");
        ReleaseBuff(header);
        return -1;
    }
    ReleaseBuff(header);
    return 0;
}

/*
     func: ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
     desc: 释放存储列相关信息队列队形
     param:
           colInfoVec[in/out]:存储数据块列信息的队列
*/
void ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
{
    while(!colInfoVec.empty())
    {
       for(_ColumnInfoPtrIter iter=colInfoVec.begin();iter != colInfoVec.end();iter++){
           _ColumnInfoPtr pObj = NULL;
           pObj = (_ColumnInfoPtr)(*iter);
           colInfoVec.erase(iter);
           if(NULL != pObj){
              delete pObj;
              pObj = NULL;
           }
           break;	   
       }
   }
}

/*
    func: ParserColumnInfo(const char* header,_ColumnInfoPtrVector& colInfoVec)
    desc: 解析列信息
    param:
          header[in] : column header info 
          colInfoVec[in/out]:存储数据块列信息的队列
    return : success 0,error -1 
*/
int  ParserColumnInfo(const HerderPtr header,_ColumnInfoPtrVector& vec)
{
    std::string parserHeader = header;
    std::string columnsNameInfo;    // 列名组合:col1,col2,col3
    std::string columnsDBType;      // 列类型组合
    std::string columnItem;
    size_t separater_pos = 0;
    int  column_num = 0; 
    int  colIndex = 0;
    
    // get columnNames : col1,col2,col3
    separater_pos = parserHeader.find_first_of(separater_columns);
    if (separater_pos == string::npos){
        return -1;
    }
    columnsNameInfo = parserHeader.substr(0,separater_pos);
    
    // ignore the columnType 
    parserHeader = parserHeader.substr(separater_pos+1,parserHeader.size()-separater_pos-1);
    separater_pos = parserHeader.find_first_of(separater_columns);
    
    // get columnDBTypes : ct1,ct2,ct3
    parserHeader = parserHeader.substr(separater_pos+1,parserHeader.size()-separater_pos-1);
    separater_pos = parserHeader.find_first_of(separater_columns);
    if (separater_pos == string::npos)
    {
        return -1;
    }
    columnsDBType = parserHeader.substr(0,separater_pos);
    
    // get columns number 
    const char *p = columnsNameInfo.c_str();
    while (*p++){
        if (*p == separater_items){
            column_num++;    // get column num
        }
    }
    column_num++;            // tail
    
    // generate the column objects vector
    for (int i=0;i<column_num;i++){
        _ColumnInfoPtr obj = NULL;
        obj = new _ColumnInfo();
        if(NULL != obj){
           vec.push_back(obj);
        }
    }
    
    // parser the column name
    separater_pos = columnsNameInfo.find_first_of(separater_items);
    colIndex = 0;
    while (separater_pos != string::npos)
    { 
        columnItem = columnsNameInfo.substr(0,separater_pos);
        columnsNameInfo = columnsNameInfo.substr(separater_pos+1,columnsNameInfo.size()-separater_pos-1);
        separater_pos = columnsNameInfo.find_first_of(separater_items);
        
        // copy column name
        strcpy(vec[colIndex]->columnName,columnItem.c_str());
        colIndex++;
    }
    strcpy(vec[colIndex]->columnName,columnsNameInfo.c_str());   // add tail
    
    // parser the column type
    separater_pos = columnsDBType.find_first_of(separater_items);
    colIndex = 0;
    while (separater_pos != string::npos)
    { 
        columnItem = columnsDBType.substr(0,separater_pos);
        columnsDBType = columnsDBType.substr(separater_pos+1,columnsDBType.size()-separater_pos-1);
        separater_pos = columnsDBType.find_first_of(separater_items);
        
        // set column type
        if (strcmp(columnItem.c_str(),"STRING") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _STRING;
        }
        else if (strcmp(columnItem.c_str(),"INT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _INT;
        }
        else if (strcmp(columnItem.c_str(),"LONG") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _LONG;
        }
        else if (strcmp(columnItem.c_str(),"FLOAT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _FLOAT;
        }
        else if (strcmp(columnItem.c_str(),"DOUBLE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _DOUBLE;			
        }
        else if (strcmp(columnItem.c_str(),"BYTE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _BYTE;
        }
        else
        {
            printf("column type error \n");
            return -1;
        }
        colIndex++;
    }
    if(true)
    {   // add tail 
        // set column type
        if (strcmp(columnsDBType.c_str(),"STRING") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _STRING;
        }
        else if (strcmp(columnsDBType.c_str(),"INT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _INT;
        }
        else if (strcmp(columnsDBType.c_str(),"LONG") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _LONG;
        }
        else if (strcmp(columnsDBType.c_str(),"FLOAT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _FLOAT;
        }
        else if (strcmp(columnsDBType.c_str(),"DOUBLE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _DOUBLE;			
        }
        else if (strcmp(columnsDBType.c_str(),"BYTE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _BYTE;
        }
        else
        {
            printf("column type error \n");
            return -1;
        }
    }
    
    return 0;
}

/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: 获取文件的概要信息
	 param:
           pFile[in]: 数据块文件打开句柄
           blkSumaryInfo[in/out]:文件概要信息结构(块个数，块额定行数，总记录数，块起始位置)

     note:
	 各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
	 数据总数 （Long） 8字节
	 数据块额定行数 （int） 4字节
	 数据块个数 （int） 4字节 [y]
*/
int  GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
{
    fseek(pFile,0,SEEK_END); // seek end
    blkSumaryInfo.filesize = ftell(pFile);  // get file size

	//-- file block number
	int Pos = 0;
	Pos -= sizeof(blkSumaryInfo.blockNum);
	fseek(pFile,Pos,SEEK_END);
	if (1 != fread(&blkSumaryInfo.blockNum,sizeof(blkSumaryInfo.blockNum),1,pFile)){
        lgprintf("read block number error");
        return -1;
	}
	_revInt(&blkSumaryInfo.blockNum);
    
	//-- file row number
	Pos -= sizeof(blkSumaryInfo.rowNum);
	fseek(pFile,Pos,SEEK_END); 
	if (1 != fread(&blkSumaryInfo.rowNum,sizeof(blkSumaryInfo.rowNum),1,pFile)){
		lgprintf("read row number error");
		return -1;
	}
    _revInt(&blkSumaryInfo.rowNum);
	
	//-- file rows sum
	Pos -= sizeof(blkSumaryInfo.rowSum);
    fseek(pFile,Pos,SEEK_END);
	if (1 != fread(&blkSumaryInfo.rowSum,sizeof(blkSumaryInfo.rowSum),1,pFile)){
		lgprintf("read row sum error");
		return -1;
	}
    _revDouble(&blkSumaryInfo.rowSum);

	//-- file block start possition vector
	//-- 逐个读取数据，每一个是8字节，顺序读取更快
	unsigned long firstBlockLocate = CalculateBlockStartPos(blkSumaryInfo.filesize,blkSumaryInfo.blockNum,0);
    fseek(pFile,firstBlockLocate,SEEK_SET);
    for (int i=0;i<blkSumaryInfo.blockNum;i++){
        unsigned long blockStartPos = 0;
        if(1 != fread(&blockStartPos,sizeof(long),1,pFile))
        {
        	lgprintf("read block start position error .");
            break;	
        }
        _revDouble(&blockStartPos);
		blkSumaryInfo.BlkStartPosVec.push_back(blockStartPos);
    }   
	
	return 0;
}

/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: 获取文件的概要信息
	 param:
           pFile[in]: 数据块文件打开句柄
           blkSumaryInfo[in/out]:文件概要信息结构(块个数，块额定行数，总记录数，块起始位置)

     note:
	 各数据块位置 (Long)[] 第一个数据块位置 filesize - 16 -([y]*8) ,第二个数据块位置 filesize - 16 -([y-1]*8) 以此类推
	 数据总数 （Long） 8字节
	 数据块额定行数 （int） 4字节
	 数据块个数 （int） 4字节 [y]
*/
int WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo)
{
    if(pFile == NULL){
        lgprintf("WriteFileSumaryInfo-->pFileHandle = NULL"); 	
        return -1;
    }
    
	//-- file block start possition vector
	if(blkSumaryInfo.blockNum != blkSumaryInfo.BlkStartPosVec.size())
	{
	    lgprintf("block number check error.");
        return -1;
	}
	
	//-- 写入每一个块的起始位置
    unsigned long blockStartPos = 0;
    for (int i=0;i<blkSumaryInfo.blockNum;i++){
		blockStartPos = blkSumaryInfo.BlkStartPosVec[i];
        _revDouble(&blockStartPos);
        if(1 != fwrite(&blockStartPos,sizeof(long),1,pFile))
        {
        	lgprintf("write block start position error .");
            break;	
        }
    }   

	//-- file rows sum
	long _rowSum = blkSumaryInfo.rowSum;
	_revDouble(&_rowSum);
	if (1 != fwrite(&_rowSum,sizeof(_rowSum),1,pFile)){
		lgprintf("write row sum error");
		return -1;
	}

	//-- file row number
	int _rowNum = blkSumaryInfo.rowNum;
	_revInt(&_rowNum);
	if (1 != fwrite(&_rowNum,sizeof(_rowNum),1,pFile)){
		lgprintf("write row number error");
		return -1;
	}

    //-- file block number
    int _blockNum = blkSumaryInfo.blockNum;
	_revInt(&_blockNum);
	if (1 != fwrite(&_blockNum,sizeof(_blockNum),1,pFile)){
        lgprintf("write block number error");
        return -1;
	}	
	return 0;
}


/*
    func: GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in/out]: origin buffer
         origin_len[in/out]: origin len
     return 0:success -1:error

	 数据块大小 （int） 4字节,数据包大小
	 数据起始位置（Long） 8字节
	 数据结束位置 （Long） 8字节
	 数据MD5(String) 32字节
	 数据内容（ZIP压缩）
*/
int GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,char* porigin_buff,long& origin_len)
{ 
     fseek(pFile,blockStartPos,SEEK_SET);
	 
     _BlockInfo blkInfo;
	 //-- origin block data size
	 if (1 != fread(&blkInfo.blocksize,sizeof(blkInfo.blocksize),1,pFile)){
         lgprintf("Read block info error,read origin data size error.");
         return -1;
	 }
    _revInt(&blkInfo.blocksize);
     
	 //-- read compressed start pos ,end pos,and get compressed size
	 if (1 != fread(&blkInfo.startPos,sizeof(blkInfo.startPos),1,pFile)){
		 lgprintf("Read block info error,read start pos error.");
		 return -1;
	 }
	 if (1 != fread(&(blkInfo.endPos),sizeof(blkInfo.endPos),1,pFile)){
		 lgprintf("Read block info error,read end pos error.");
		 return -1;
	 }
	 _revDouble(&blkInfo.startPos);  // 无效
	 _revDouble(&blkInfo.endPos);    // 无效

	 //-- read origin data md5sum 
     if(1 != fread(blkInfo.md5sum,conMd5StrLen,1,pFile)){
		 lgprintf("Read block info error,read md5sum error.");
		 return -1;
	 }
     
     // 压缩后数据大小
     int  _len_len = conMd5StrLen + sizeof(blkInfo.startPos) + sizeof(blkInfo.endPos) + sizeof(blkInfo.blocksize);
	 blkInfo.compressed_data_size = blkInfo.blocksize - _len_len;
	 
	 if(blkInfo.compressed_data_size > compressed_len - 100)
	 {
	     lgprintf("Compressed data len error.");
	     return -1;	
	 }
	 //-- read compressed data
     if (1 != fread(pcompressed_buff,blkInfo.compressed_data_size,1,pFile)){
		 lgprintf("Read block info error,read compressed buff error.");
		 return -1;
     }
	 compressed_len = blkInfo.compressed_data_size;

	 //-- get origin data
	 if(0 != UnzipDataBlock(pcompressed_buff,compressed_len,porigin_buff,origin_len)){
		 lgprintf("Read block info error,unzip data block error.");
		 return -1;
	 }
	 
	 // check data md5sum
	 
     unsigned char md[conMd5Len] = {0}; // 16 bytes     ,不能定义成char,否则会出现sprintf出来的md5array中存在乱码
	 // get md5 hush code from data data source
	 MD5((unsigned char*)porigin_buff,origin_len,md);
	 char md5array[conMd5StrLen];
	 memset(md5array,0,conMd5StrLen);
     for(int i=0;i<conMd5Len;i++)
     {
         sprintf(md5array+strlen(md5array),"%02x",md[i]);
     }
     
     if(strncmp(md5array,blkInfo.md5sum,conMd5StrLen) != 0)
     {
         lgprintf("data md5 check error.");
         return -1;	
     }
     
     return 0;
}

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

   	 数据块大小 （int） 4字节,数据包大小
	 数据起始位置（Long） 8字节
	 数据结束位置 （Long） 8字节
	 数据MD5(String) 32字节
	 数据内容（ZIP压缩）
*/
int WriteBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,const char* porigin_buff,const long origin_len)
{
    fseek(pFile,blockStartPos,SEEK_SET);
    
    //-- 压缩数据
    if(0 != ZipDataBlock(porigin_buff,origin_len,pcompressed_buff,compressed_len)){
        return -1;
    }
    
    _BlockInfo blkInfo;
    
    // 共计52
    int  _len_len = conMd5StrLen + sizeof(blkInfo.startPos) + sizeof(blkInfo.endPos) + sizeof(blkInfo.blocksize);
    //-- write block data size
    blkInfo.blocksize = compressed_len + _len_len;
    _revInt(&blkInfo.blocksize);
    if (1 != fwrite(&blkInfo.blocksize,sizeof(blkInfo.blocksize),1,pFile)){
        lgprintf("Write block info error,write origin data size error.");
        return -1;
    }
    _revInt(&blkInfo.blocksize);
    
    //-- write compressed start pos ,end pos,and get compressed size,2个值都是无效的
    _revDouble(&blkInfo.startPos);
    if (1 != fwrite(&blkInfo.startPos,sizeof(blkInfo.startPos),1,pFile)){
        lgprintf("Write block info error,read start pos error.");
        return -1;
    }
    _revDouble(&blkInfo.endPos);
    if (1 != fwrite(&(blkInfo.endPos),sizeof(blkInfo.endPos),1,pFile)){
        lgprintf("Write block info error,read end pos error.");
        return -1;
    }
    
    //-- write origin data md5sum 
    unsigned char md[conMd5Len] = {0}; // 16 bytes     ,不能定义成char,否则会出现sprintf出来的md5array中存在乱码
    // get md5 hush code from data data source
    MD5((unsigned char*)porigin_buff,origin_len,md);
    for(int i=0;i<conMd5Len;i++)
    {
        sprintf(blkInfo.md5sum+strlen(blkInfo.md5sum),"%02x",md[i]);
    }
    if(1 != fwrite(blkInfo.md5sum,conMd5StrLen,1,pFile)){
        lgprintf("Write block info error,write md5sum error.");
        return -1;
    }
    
    // 压缩后数据大小
    blkInfo.compressed_data_size = blkInfo.blocksize - _len_len;
    compressed_len = blkInfo.compressed_data_size;
    
    //-- write compressed data
    if (1 != fwrite(pcompressed_buff,blkInfo.compressed_data_size,1,pFile)){
        lgprintf("Write block info error,Write compressed buff error.");
        return -1;
    }
    
    return 0;   
}



/*
    func: ParserOriginData(const char* porigin_buff,const int origin_len,AutoMt* mt,_ColumnInfoPtrVector& colInfoVec)
    desc: Parser origin data into AutoMt
    param:
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
		 mt:AutoMt
		 colInfoVec:columnInfo
     return: parse rows,-1 == error 
            
	 每一行数据结构 ：  
	 [len][null_info][data]
	 [---------len--------]
	 行数据大小 （int） 4字节 
	 列空值情况 （byte[columnName.length]） 1 == 空值 0== 非空 
	 （例如 011000 ，根据columnType 取出相应数据类型，提供取值）
	 数据 （根据数据类型长度读取，字符串类型，前4字节为字符串长度）
	 
	 ☆注意：时间和字符串都是字符串的形式给出数据，如果遇到字符串类型的数据
	 需要判断该列是否是时间类型，如果该列是时间类型，需要对数据进行转换处理
	 
*/
int ParserOriginData(const char* porigin_buff,const int origin_len,int memtab,_ColumnInfoPtrVector& colInfoVec)
{
    AutoMt mt(0,10);
    mt.SetHandle(memtab,true);
    int deal_rows = 0;
    // current row index in mt
    long mtrows = mt.GetRows();
    int null_array_num = colInfoVec.size();
    int line_len = 0;
    int origin_pos = 0;
    int retCode = 0;
    int colindex = 0;
    while(origin_pos < origin_len)
    {
        // line 长度
        memcpy(&line_len,porigin_buff+origin_pos,sizeof(line_len));
        _revInt(&line_len);
        origin_pos += sizeof(line_len);
        
        // column 空值情况
        const NullPtr pnulls = (const NullPtr)porigin_buff+origin_pos;
        origin_pos += null_array_num;
        
        // 一行中列对应的值的信息
        const LinePtr plineInfo = (const LinePtr)porigin_buff+origin_pos;
        origin_pos += (line_len - null_array_num - sizeof(line_len));   //行长度
        int line_pos = 0;
        
        int col_len = 0;
        int col_type = 0;
        // 逐列值填充
        for (colindex=0;colindex<null_array_num;colindex++)
        {                         
            // 再次判断是否越界
            if(origin_pos > origin_len)
            {
                lgprintf("ParserOriginData len error , origin_pos = %d,origin_len = %d,colindex = %d,deal_rows = %d,line_len = %d\n",
                        origin_pos,origin_len,colindex+1,deal_rows+1,line_len);  
                retCode = -11;
                break;
            }
            if (pnulls[colindex] == EMPTY_YES) // 该列是空值
            {
                // 不进行任何处理
                wociSetNull(mt,colindex,mtrows,true);
                mt.SetStr(colindex,mtrows,"");
                continue;
            }
            else
            {
                // 判断列的类型，根据类型进行填充数据
                int strLen = 0;
                switch (colInfoVec[colindex]->columnDBType)
                {
                case _INT:
                    _revInt(plineInfo+line_pos);
                    memcpy(mt.PtrInt(colindex,mtrows),plineInfo+line_pos,sizeof(int));
                    line_pos += sizeof(int);
                    break;
                case _LONG:
                    _revDouble(plineInfo+line_pos);
                    memcpy(mt.PtrLong(colindex,mtrows),plineInfo+line_pos,sizeof(long));					
                    line_pos += sizeof(long);
                    break;
                case _FLOAT:
                    _revInt(plineInfo+line_pos);
                    memcpy(mt.PtrDouble(colindex,mtrows),plineInfo+line_pos,sizeof(float));
                    line_pos += sizeof(float);
                    break;
                case _DOUBLE:
                    _revDouble(plineInfo+line_pos);
                    memcpy(mt.PtrDouble(colindex,mtrows),plineInfo+line_pos,sizeof(double));
                    line_pos += sizeof(double);
                    break;
                case _STRING:
                    _revInt(plineInfo+line_pos);
                    memcpy(&strLen,plineInfo+line_pos,sizeof(int));
                    line_pos += sizeof(int);
                    
                    //--------------------------------------------------------------------------------
                    col_len = mt.getColLen(colindex);
                    col_type = mt.getColType(colindex);
                    
                    if(col_type == COLUMN_TYPE_DATE)//时间类型处理
                    {
                        // 时间类型字符串不应该大于19
                        if(strLen > 19){
                            lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] 日期类型数据格式错误.",colindex,col_len,strLen);
                            retCode = -12; 	
                            break;
                        }
                        int of=0,y,m,d,hh,mm,ss;
                        char *dptr=(char*)plineInfo+line_pos;
                        while(*dptr==' ') dptr++;
                        if(*dptr<'0' || *dptr>'9') {
                        	lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] 日期类型数据格式错误.",colindex,col_len,strLen);
                        	retCode = -12;
                            break;
                        }
                        if(dptr[4]>='0' && dptr[4]<='9') {
                        	//没有分隔符,例如：2012-11-12 12:12:12,2012/12/12 12/12/12
                        	char r=dptr[4];
                        	dptr[4]=0;
                        	y=atoi(dptr);dptr+=4;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	m=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	d=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	hh=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	mm=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	ss=atoi(dptr);dptr+=2;
                        }
                        else {
                        	// 存在分隔符，例如：2012-11-12 12:12:12,2012/12/12 12/12/12
                        	y=atoi(dptr+of);of+=5;
                        	m=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	d=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	hh=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	mm=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	ss=atoi(dptr+of);
                        }
                        if(y<1900 || m<1 || m>12 || d<1 || d>31 ||hh<0 || hh>60 || mm<0 || mm>60 ||ss<0 || ss>60) {
                        	lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] 日期类型数据[%04d-%02d-%02d %02d-%02d-%02d]格式错误.",
                        	     colindex,col_len,strLen,y,m,d,hh,mm,ss); 
                            retCode = -14;
                            break;
                        }
                        wociSetDateTime(mt.PtrDate(colindex,mtrows),y,m,d,hh,mm,ss);                    	
                    }
                    else // 字符串类型
                    {
                        if( col_len < strLen){
                            lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] 字符串类型长度格式错误.",colindex,col_len,strLen);
                    	    retCode = -15;
                        }                                          	
                        
						// strncpy(mt.PtrStr(colindex,mtrows),(char*)plineInfo+line_pos,strLen);
                        // mt.PtrStr(colindex,mtrows)[strLen] = 0;	                        
						char *_p = new char[strLen+2];
						strncpy(_p,(char*)plineInfo+line_pos,strLen);
                        _p[strLen]=0;
						mt.SetStr(colindex,mtrows,_p);
						delete [] _p;	
                    }           
                    //------------------------------------------------------------------------------------                                     
                    // 时间类型是字符串给出的，可能长度是19，例如:2012-12-12 12:12:12
                    line_pos += strLen;
                    break;
                default:
                    lgprintf("column type error , column index %d ",colindex); 	
                    retCode = -16;
                }// end switch  
               
                if(0!=retCode){
                   break; 
                }                  
            } // end else(EMPTY_NO)
        }// end for (int colindex=0;colindex<null_array_num;colindex++)
                
        // 数据长度部分追加
        if(line_pos != (line_len - null_array_num - sizeof(line_len)))
        {
           lgprintf("ParserOriginData len error , origin_pos = %d,origin_len = %d,colindex = %d,deal_rows = %d,line_len = %d,line_pos = %d.\n",
                        origin_pos,origin_len,colindex+1,deal_rows+1,line_len,line_pos);  
           retCode = -17;
        }
        
        if(retCode != 0){
            break;	
        }
        
        deal_rows++; 
        mtrows++;      
    }// end while(origin_pos < origin_len)
    
    if(retCode == 0){ // 数据正确
       _wdbiSetMTRows(mt,mtrows);
    }
    else
    {
       // 最后处理的行设置为空行
       for (colindex=0;colindex<null_array_num;colindex++){
          wociSetNull(mt,colindex,mtrows,true);
		  mt.SetStr(colindex,mtrows,"");
       }
       mtrows -= 1;  // 减少一行
       _wdbiSetMTRows(mt,mtrows);
       #if 1
       return retCode;
       #else 
       return deal_rows;
       #endif
    }
    return deal_rows;
}



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
int DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb,AutoHandle*  pdts,const char* sysPath,bool lastmt)
{
    char fn_myd[300];    // 数据文件
    char fn_tmd[300];    // 临时文件
    char fn_myi[300];    // 管理文件
    const char *pathval=sysPath;
    int  mt_rows = 0;
    // 获取数据库的文件名称,例如:/app/dma/var/dp/test_01.MYI
    sprintf(fn_myi,"%s/%s/%s.MYI",pathval,dbname,tbname);   // 管理数据
    
    // 1. 打开MYI文件
    FILE *fp_myi =NULL;
    fp_myi=fopen(fn_myi,"rb");
    if(fp_myi==NULL){
        lgprintf("插入数据库%s.%s,打开文件%s失败.",dbname,tbname,fn_myi);
        return -1;	 
    }
    
    // 2. 读取文件中MYI的记录数
    fseek(fp_myi,28,SEEK_SET);
    LONG64 file_row_num = 0;
    if(1 != fread(&file_row_num,8,1,fp_myi)){
        fclose(fp_myi);fp_myi = NULL;
        lgprintf("插入数据库%s.%s,读取文件%s失败.",dbname,tbname,fn_myi);
        return -1;	 
    }
    _revDouble(&file_row_num);
    
    fclose(fp_myi);
    fp_myi = NULL;
    
    // 获取数据库的文件名称,例如:/app/dma/var/dp/test_01.MYD
    sprintf(fn_myd,"%s/%s/%s.MYD",pathval,dbname,tbname);   // 数据文件
    
    // 3. 打开MYD文件
    FILE *fp_myd =NULL;
    fp_myd=fopen(fn_myd,"ab");
    if(fp_myd==NULL){
        lgprintf("插入数据库%s.%s,打开文件%s失败.",dbname,tbname,fn_myd);
        return -1;	 
    }
    
    // 4. 写入MYD文件数据内容
    fseek(fp_myd,0,SEEK_END);
    wociCopyToMySQL(memtb,0,0,fp_myd);
    
    // 记录行数添加
    mt_rows =  wociGetMemtableRows(memtb);
    file_row_num += mt_rows;
    
    // 5. 获取MYD文件大小，关闭MYD文件
    LONG64 myd_file_size = 0;
    fseek(fp_myd,0,SEEK_END);
    myd_file_size = ftell(fp_myd);	
    fclose(fp_myd);fp_myd = NULL;
    
    // 6. 将记录条数,MYD文件大小写入MYI文件中
    fp_myi=fopen(fn_myi,/*"wb"*/ "r+b");
    if(fp_myi==NULL){
        lgprintf("插入数据库%s.%s,打开文件%s失败.",dbname,tbname,fn_myi);
        return -1;	 
    }			
    
    //7. 写入记录数据	  
    fseek(fp_myi,28,SEEK_SET);
    _revDouble(&file_row_num);
    dp_fwrite(&file_row_num,1,8,fp_myi);
    
    // reset deleted records count.
    char tmp[20];
    memset(tmp,0,20);
    dp_fwrite(tmp,1,8,fp_myi);
    
    // 写入文件大小
    fseek(fp_myi,68,SEEK_SET);	
    _revDouble(&myd_file_size);
    dp_fwrite(&myd_file_size,1,8,fp_myi);
    
    // 关闭文件
    fseek(fp_myi,0,SEEK_END);
    fclose(fp_myi); 
    fp_myi = NULL;
    
    // 8. FlushTable(刷新表)
    char _tb[300];
    sprintf(_tb,"%s.%s",dbname,tbname);
    lgprintf("写入表%s,%d行记录...",_tb,mt_rows);
    
    _revDouble(&myd_file_size);
    
    // 9 压缩数据,最后一个mt处理完成后
    char cmdline[500];
    int rt;
    if(lastmt && myd_file_size>2*1024*1024)
    {
        // 9.0 删除临时文件(上一次执行失败的)
        sprintf(fn_tmd,"%s/%s/%s.TMD",pathval,dbname,tbname);   // 管理数据
        unlink(fn_tmd);
        
        // 9.1 myisampack 压缩表		
        lgprintf("压缩表表:%s....",_tb);
        printf("pack:%s\n",fn_myi);
        sprintf(cmdline,"myisampack -v %s",fn_myi);
        rt=system(cmdline) ;
        lgprintf("数据库表%s.%s，文件%s压缩%s.",dbname,tbname,fn_myi,(rt != 0 ? "失败":"成功"));
        if(rt){
            lgprintf("myisampack 执行失败:cmd = %s ",cmdline);
            return -1;
        }
  
        // 9.2 myisamchk MYI 表
        sprintf(cmdline,"myisamchk -rqvn --tmpdir=\"%s\" %s ",pathval,fn_myi);
        printf(cmdline);
        rt=system(cmdline);
        if(rt){
            lgprintf("myisamchk 执行失败:cmd = %s ",cmdline);
            return -1;
        }		
		lgprintf("\n");

        AutoStmt st(*pdts);
        st.DirectExecute("flush table %s.%s",dbname,tbname);
    }
    if(lastmt && myd_file_size <= 2*1024*1024){
        AutoStmt st(*pdts);
        st.DirectExecute("flush table %s.%s",dbname,tbname);
    }    

    return 0;
}


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
          lastpack[in]:是否是最后一个包，强制调用CloseWriter
    return:
          0: 成功
          -1: 失败
    note:生成等待装入的数据文件用，BrightHouse引擎使用
*/
int GenLoadFileFromMt(const char* dbname,const char* tbname,int memtb,const char* fname,IBDataFile* ib_datafile,write_file_status status,bool lastpack)
{
    // 如果文件存在，先删除文件
    long rn = wociGetMemtableRows(memtb);
    if(rn == 0){
        lgprintf("status = %d,向文件 %s 追加 %d 行记录.\n",status,fname,rn);
        return 0;	
    }
    // lgprintf("status = %d,向文件 %s 追加 %d 行记录.\n",status,fname,rn);
    if(status == _STATUS_OPEN)
    {
        unlink(fname);
        ib_datafile->SetFileName((char*)fname);
        ib_datafile->CreateInit(2*1024*1024);   // 设置缓存2MB
    }
    if( 0 == ib_datafile->Write(memtb))
    {
        ib_datafile->CloseWriter();
        lgprintf("write mt to file %s failed.\n",fname);
        return -1;
    }
	if(status == _STATUS_CLOSE || lastpack)
	{
       lgprintf("write mt to file %s close.\n",fname);
       ib_datafile->CloseWriter();
    }
    return 0;
}

/*
    func:LoadFile2Table 
    desc:将二进制文件装入brighthouse引擎数据库
    param:
          dbname[in]:数据库名称
          tbname[in]:表名称
          pdts[in]:获取目标表名称和相关操作使用的
          syspath[in]:数据库存放路径
          fname[in]:转换写入的文件名称
    return:
          0: 成功
          -1: 失败
    note:将生成后的文件Load进入BrightHouse引擎
    只考虑一个索引的情况，或者没有索引的情况
*/
void ServerLoadData(ThreadList *tl)
{
    int dbs=(long)tl->GetParams()[0];
    const char *loadsql=(const char *)tl->GetParams()[1];
    AutoStmt st(dbs);
    st.ExecuteNC("set @bh_dataformat='binary'");
    st.ExecuteNC("set autocommit=0");
    st.ExecuteNC(loadsql);
    lgprintf("数据提交...");
    st.ExecuteNC("commit");
    lgprintf("提交成功.");
    tl->SetReturnValue(TRET_OK);
}
void PipeData(ThreadList *tl) {
	IBDataFile *pibdatafile=(IBDataFile *) tl->GetParams()[0];
	const char *pipefilename=(const char*)tl->GetParams()[1];
	// open with 'wb' options!!
	FILE *ibfile=fopen(pipefilename,"wb");//(FILE *)tl->GetParams()[1];
	if(ibfile==NULL) 
		ThrowWith("Open pipe file '%' failed.",pipefilename);
	FILE *keep_file=(FILE *)tl->GetParams()[2];
	/* exception catched in caller*/
	try
	{
	
		LONG64 recimp=pibdatafile->Pipe(ibfile,keep_file);
		tl->GetParams()[3]=(void *)recimp;
		//tl->SetParam(3,(void *)recimp);
		tl->SetReturnValue(TRET_OK);	
	}
	catch(...)
	{        
		fclose(ibfile);
		throw;
	}
	fclose(ibfile);
	lgprintf("数据传入结束.");
}
void UpdateLoadPartition(int connectid,const char* tabpath,const char* partname,const char* syspath)
{
	char loadfile[300];
	sprintf(loadfile,"%s/%s/loadinfo.ctl",syspath,tabpath);
	FILE *fp=fopen(loadfile,"w+b");
	if(fp==NULL)
		ThrowWith("load ctl file '%s' open failed.",loadfile);
	if(fwrite("LOADCTRL",1,8,fp)!=8) {
		fclose(fp);
		ThrowWith("load ctl file write failed.");
	}
	fwrite(&connectid,sizeof(int),1,fp);
	short len=strlen(partname);
	fwrite(&len,sizeof(short),1,fp);
	fwrite(partname,1,len,fp);
	fclose(fp);
}
int LoadFile2Table(const char* dbname,const char* tbname,AutoHandle*  pdts,const char* syspath,const char* fname)
{ 
    // 分区名称
    char partname[128];  
    sprintf(partname,"%s_%s_part01",dbname,tbname);
    char* filename = (char*)fname;

    // 会话id获取
    AutoMt mdf(*pdts,MAX_DST_DATAFILENUM);
    mdf.FetchFirst("select connection_id() cid");
    mdf.Wait();
    int connectid=(int)mdf.GetLong("cid",0);

    // 分区文件名称
	char tabpath[300];
    sprintf(tabpath,"%s/%s.bht",dbname,tbname);
	lgprintf("装入数据,分区:%s,连接号:%d.",partname,connectid);
    const char *pathval=syspath;
    UpdateLoadPartition(connectid,tabpath,partname,pathval);
	
    // 管道文件
	IBDataFile ibdatafile;
    char cmd[2000];
    char ibfilename[300];
    sprintf(ibfilename,"%s/%s/%s_pipe.bin",pathval,tabpath,fname);
    sprintf(cmd,"rm -f %s",ibfilename);

    lgprintf("开始数据导入,表%s.%s分区%s,文件%s 。",dbname,tbname,partname,filename);
    try 
    {
        system(cmd);
        sprintf(cmd,"mkfifo %s",ibfilename);
        FILE *keep_file=NULL;
#ifdef KEEP_LOAD_FILE
        char bkfilename[300];
        sprintf(bkfilename,"/tmp/bhloader_restore_%d.dat",getpid());
        keep_file=fopen(bkfilename,"w+b");		
#endif	
        if(system(cmd)==-1){
            lgprintf("表%s.%s,分区%d,创建管道文件%s失败.",dbname,tbname,partname,ibfilename);
            return -1;
        }
	
	   /* 
		sprintf(cmd,
		"mysql -S /tmp/mysql-dma.sock -u%s -p%s <<EOF &\n"
		"set @bh_dataformat='binary';\n"
		"set autocommit=0;\n"
		"load data infile '%s' into table %s.%s;\n"
		"commit ;\n"
		"EOF",getenv("DP_CUSERNAME"),getenv("DP_CPASSWORD"),ibfilename,mdf.PtrStr("databasename",0),tbname1);
		int cmdrt=0;
		cmdrt=system(cmd);
		if(cmdrt==-1) ThrowWith既胧О堋?);
		lgprintf("等待进程就绪");
		sleep(3);
	   */		 
	
        FILE *fptest=fopen(filename,"rb");
        if(fptest==NULL) {
            lgprintf("表%.%s,分区%s,打开数据文件%s失败.",dbname,tbname,partname,filename);
            return -1;
        }
        fclose(fptest);fptest = NULL;
	
        //启动服务端线程
        ThreadList tload;
        void *params[2];
        params[0]=( void *)(int)(*pdts);
        char loadsql[3000];
        sprintf(loadsql,"load data infile '%s' into table %s.%s",ibfilename,dbname,tbname);
        params[1]=( void *)loadsql;
        lgprintf("开始转换 %s->%s",filename,ibfilename);
        
        void *pipeparams[4];
        pipeparams[0]=(void *)&ibdatafile;
        pipeparams[1]=(void *)ibfilename;
        pipeparams[2]=(void *)keep_file;
        pipeparams[3]=(void *)NULL;
        ibdatafile.SetFileName(filename);
			
        //启动管道数据写入线程
        ThreadList *tpipe=tload.AddThread(new ThreadList());
        tload.SetWaitOne();
        tload.Start(params,2,ServerLoadData);	
        tpipe->Start(pipeparams,4,PipeData);
        try 
        {
            while(ThreadList *pthread=tload.WaitOne()) 
            {
                if(pthread==tpipe) 
                {
                    LONG64 recimp=(long )tpipe->GetParams()[3];
                    if(recimp!=-1l){
						 lgprintf("已导入 %ld 行记录.",recimp);
                    }
                }
                else
                {
                    lgprintf("数据库导入结束.");
                }
            }
        }
        catch(...) 
        {
            if(tload.GetExceptedThread()==tpipe) {
                lgprintf("文件写入异常,终止数据库导入...");
                tload.Terminate();
            }
            else {
                lgprintf("数据库导入异常,终止文件写...");
                tpipe->Terminate();
            }
			#ifdef KEEP_LOAD_FILE
				fclose(keep_file);
			#endif
            return -1;
        }
#ifndef KEEP_LOAD_FILE
        unlink(ibfilename);
        unlink(fname);       // 装入成功后，删除文件
#endif
        return 0;
    }
    catch(...)
    {  
        lgprintf("Load file %s to table %s.%s error.\n",fname,dbname,tbname);
        return -1;
    }

	return 0;
}


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
int ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len)
{
    int rt = 0;
    long _compressed_len = compressed_len;
    rt=compress2((Bytef *)compressed_data,(uLongf*)&compressed_len,(Bytef *)origin_data,origin_len,1);
    if(rt!=Z_OK) 
    {
        lgprintf("Compress failed,return code:%d\n",rt);
        return -1;
    }
    if(compressed_len >= _compressed_len){
        lgprintf("Compress failed,data len error[buffer len %d != compressed len %d].\n",_compressed_len ,compressed_len);
        return -1;  
    }
    return 0;
}


/*
    func: DecompressDataBlock(const char* compressed_data,const int compressed_len,char * origin_data,int& origin_len);
    desc: Decompress the zip data to origin data
    param:
        compressed_data[in]: compressed data
        compressed_len[in]: compressed data len
        origin_data[in/out]: origin data buff
        origin_len[in/out]: orgint data len
     return success 0,failed -1
	 note : use uncompress ,zip algorithm, throw the zlib library
*/
int  UnzipDataBlock(const char* compressed_data,const long compressed_len,char * origin_data,long& origin_len)
{
    int rt = 0;
    rt=uncompress((Bytef *)origin_data,(uLongf*)&origin_len,(Bytef *)compressed_data,compressed_len);
    if(rt!=Z_OK) 
    {
        lgprintf("Decompress failed,return code:%d\n",rt);
        return -1;
    }
    return 0;
}


/*
    func: LoadSysPath
    desc: 获取数据库系统路径
    param: 
          sysPath[in/out]:系统路径
    return: 0 成功
            -1 失败
    note:全局只初始化一次
*/
int LoadSysPath(char* sysPath)
{
	char *_sysPath = getenv("TBLOADER_SYS_PATH");
	if(NULL == _sysPath){
	    lgprintf("id=[0],获取系统路径[TBLOADER_SYS_PATH = NULL],请检查mysql系统主目录环境变量.");
		return -1;
	}	
	strcpy(sysPath,_sysPath);
    lgprintf("id=[0],获取系统路径[syspath = %s]成功！",sysPath);
    return 0;
}



