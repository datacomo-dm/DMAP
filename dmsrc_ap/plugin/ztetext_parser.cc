#include "dt_common.h"
#include "AutoHandle.h"
#include "ztetext_parser.h"
#include "dumpfile.h"
#include <string.h> 



extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);

#define zkb_revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }
       
 #define zkb_revInt(V)   { char def_temp[4];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			              memcpy(V,def_temp,sizeof(int)); }
			                       
 #define zkb_revShort(V)  { char def_temp[2];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(short)); }



IFileParser *BuildParser(void) {
	return new ZteTxtParse();
}

#ifndef TMP_LEN
#define TMP_LEN 100
#endif 

#define ExtractDate(optionstr,value,len,func)	{\
	const char *opt=pWrapper->getOption(optionstr,"");\
	int offset=0;\
	/* value=-1;  // warning:delete 20130114, Fix bug DM-206 */  \
	if(strlen(opt)>0) {\
		assert(strlen(opt)<TMP_LEN);\
		if(opt[strlen(opt)-1]=='d') {\
			char _tmp[TMP_LEN];\
			strcpy(_tmp,opt);\
			_tmp[strlen(opt)-1] = 0;\
			offset=atoi(_tmp);\
			value=ExtractNum(offset,dbname,len);\
		} \
		else if(opt[strlen(opt)-1]=='t') { \
			char _tmp[TMP_LEN];\
			strcpy(_tmp,opt);\
			_tmp[strlen(opt)-1] = 0;\
			offset=atoi(_tmp);\
			value=ExtractNum(offset,tabname,len); \
		} \
		else if(opt[0]=='p') { \
			value=func(datapartdate);\
		} \
		else value=atoi(opt);\
	}\
}


int ZteTxtParse::ExtractNum(int offset,const char *info,int len) {
	int value =0;
	char numchar[20];
	if(offset<0) {
		strncpy(numchar,info+strlen(info)+offset,len);
		numchar[len]=0;
		value=atoi(numchar);
	}
	else {
		strncpy(numchar,info+offset,len);
		numchar[len]=0;
		value=atoi(numchar);
	}
	return value;
}
bool ZteTxtParse::SwitchOpt(char *deststr,char *opt,int val,int len)
{
	char sourcestr[300];
	strcpy(sourcestr,deststr);
	char *strdest=strcasestr(sourcestr,opt);
	if(strdest) {
		char patt[20];
		sprintf(patt,"%%0%dd",len);
		if(val<0) ThrowWith("未设置files:%s参数",opt+1);
		strncpy(deststr,sourcestr,strdest-sourcestr);
		deststr[strdest-sourcestr]=0;

		sprintf(deststr+strlen(deststr),patt,val);
		strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
		return true;
	}
	return false;
}
bool ZteTxtParse::SwitchOpt(char *deststr,char *opt,char *value) {
	char sourcestr[300];
	strcpy(sourcestr,deststr);
	char *strdest=strcasestr(sourcestr,opt);
	if(strdest) {
		//char patt[10];
		//sprintf(patt,"%%0%dd",len);
		if(!value) ThrowWith("参数%s的值无效",opt+1);
		strncpy(deststr,sourcestr,strdest-sourcestr);
		deststr[strdest-sourcestr]=0;
		strcat(deststr,value);
		strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
		return true;
	}
	return false;
}


// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// 返回值：
//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
//  0：暂无文件
//  1：取得有效的文件
// checkonly : dma-111,只检查有没有文件，不采集
int ZteTxtParse::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
	AutoMt mt(dbc,10);
	mt.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
	if(mt.Wait()<1) ThrowWith("Table id:%d not found.",tabid);
	char tabname[200],dbname[200];
	strcpy(dbname,mt.PtrStr(0,0));
	strcpy(tabname,mt.PtrStr(1,0));
	char datapartdate[30];
	mt.FetchAll("select begintime from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
	if(mt.Wait()<1) ThrowWith("Table id:%d,datapart:%d not found in dp.dp_datapart.",tabid,datapartid);
	memcpy(datapartdate,mt.PtrDate(0,0),7);
	// 根据控制文件截取年月日等信息，存入yyyy,mm,dd,hour中
	ExtractDate("files:year",yyyy,4,wociGetYear);
	ExtractDate("files:month",mm,2,wociGetMonth);  
	ExtractDate("files:day",dd,2,wociGetDay);  
	ExtractDate("files:hour",hour,2,wociGetHour);  
	char filepatt[300];
	strcpy(filepatt,pWrapper->getOption("files:filename",""));
	while(true) {
		if(SwitchOpt(filepatt,(char*)"$year",yyyy,4)) continue;
		if(SwitchOpt(filepatt,(char*)"$month",mm,2)) continue;
		if(SwitchOpt(filepatt,(char*)"$day",dd,2)) continue;
		if(SwitchOpt(filepatt,(char*)"$hour",hour,2)) continue;
		if(SwitchOpt(filepatt,(char*)"$table",tabname)) continue;
		if(SwitchOpt(filepatt,(char*)"$db",dbname)) continue;
		break;
	}
	// Begin:设置是否删除源文件，默认删除[1:删除，0:不删除] 2012-11-02
	fm.SetDeleteFileFlag(pWrapper->getOption("files:delsrcdata",1));
	// End: 设置是否删除源文件，默认删除[1:删除，0:不删除]            

	//>>begin:fix dma-926 获取修正后的路径信息,用于处理FILENAME中含有路径的情况
	char temp_path[300];
	if(strlen(pWrapper->getOption("ftp:path","")) > 0)
	{
		if(strlen(fix_ftp_path)==0 || strlen(fix_filepatt) == 0)
		{
			splite_path_name(filepatt,temp_path,fix_filepatt);
			sprintf(fix_ftp_path,"%s/%s",pWrapper->getOption("ftp:path",""),temp_path);
		}
	}
	if(strlen(pWrapper->getOption("local:path","")) > 0)
	{
		if(strlen(fix_ftp_path)==0 || strlen(fix_filepatt) == 0)
		{
			splite_path_name(filepatt,temp_path,fix_filepatt);
			sprintf(fix_localpath,"%s/%s",pWrapper->getOption("local:path","./"),temp_path);
		}		
	}
	//<<end:fix dma-926 
	
	// if not set datetime ,set to now
	char now[10];
	wociGetCurDateTime(now);
	if(yyyy<0) yyyy=wociGetYear(now);
	if(mm<0) mm=wociGetMonth(now);
	if(dd<0) dd=wociGetDay(now);
	if(hour<0) hour=wociGetHour(now);
	
	char backuppath[300];
	sprintf(backuppath,"%s/%s.%s",
		pWrapper->getOption("files:backuppath","./"),dbname,tabname);
	sprintf(backuppath+strlen(backuppath),"/part%d",datapartid);
	
	xmkdir(backuppath);
	int rt=commonGetFile(backuppath,false,fix_filepatt,sp,tabid,datapartid,checkonly);
	if(rt==0&&frombackup) rt=commonGetFile(backuppath,true,fix_filepatt,sp,tabid,datapartid,checkonly);
	return rt;
}
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// 必须是不再有文件需要处理的时候，才能调用这个检查函数
bool ZteTxtParse::ExtractEnd(int _dbc) {
	wociSetEcho(false);
	AutoMt mt(_dbc,10);
	//不能加and procstate<=1 ，可能最后的一部分文件还没有进入状态1
	mt.FetchAll("select ifnull(sum(recordnum),0) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d ",
		tabid,datapartid);
	mt.Wait();
	// 注释下面一行 原因 DMA-64
	//if(  mt.GetDouble("srn",0)==0) return false; //文件还没有处理完。
	double srn=mt.GetDouble("srn",0);
	mt.FetchAll("select case status when 10 then 2 when 1 then 2 else status end status,max(fileseq) xseq,min(fileseq) mseq,count(*) ct,sum(recordnum) srn "
		" from dp.dp_filelog where tabid=%d and datapartid=%d group by case status when 10 then 2 when 1 then 2 else status end ",
		tabid,datapartid);
	int rn = mt.Wait();
        int maxseq=0;
        int minseq=0;
        LONG64 filect=0;
	if(rn != 0){
		if(mt.Wait()!=1) return false; //文件还没有处理完。
		if(mt.GetDouble("status",0)!=2) return false;//文件还没有处理完。

		maxseq=mt.GetInt("xseq",0);
		minseq=mt.GetInt("mseq",0);
		filect=mt.GetLong("ct",0);
		lgprintf("最小文件号:%d,最大文件号:%d,文件数:%ld.",minseq,maxseq,mt.GetLong("ct",0));
	}
	else{
		// TODO: nothing ,fix dma-700
	}	
	//应移动要求，去掉序号连续性检查
	
	// Begin: 如果是crc产生的文件序号，不进行校验
	if(pWrapper->getOption("files:crcsequence",0)==0 && rn>0)
	{
		  // 是否校验序号
		if(pWrapper->getOption("files:sequencecheck",1)!=0) {
			if(minseq!=1 || maxseq-minseq+1!=mt.GetLong("ct",0)) {
				lgprintf("文件序号不完整，应该是序号1-%d,应该有%d个文件，实际有%d个文件....",
				     maxseq,maxseq,mt.GetLong("ct",0));
				return false;//文件不完整
			}
		}		
       }
  // End: 如果是crc产生的文件序号，不进行校验
	/* 记录数的检查在这里会出现：
	文件已经分解到内存表，但是内存表还没满，还没做预处理，也就没有写到dp_middledatafile表
	因此记录数核对异常
	DMA-64
	if(pWrapper->getOption("recordnumcheck",1)!=0) {
	if(srn!=mt.GetDouble("srn",0)) {
	lgprintf("记录数不一致，中间数据为%.0f行,数据文件为%.0f行.\n",srn,mt.GetDouble("srn",0));
	return false;
	}
	}
	*/
	if(pWrapper->getOption("files:forceenddump",0)!=1) {
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(date_add('%04d-%02d-%02d',interval 1 day),interval '%d:%d' hour_minute))",
			yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60
			);
		mt.Wait();
		LONG64 tm1=mt.GetLong(0,0);
		lgprintf("距离截止时间:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//还没到延迟时间
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(max(endproctime),interval %d minute)) from dp.dp_filelog where tabid=%d and datapartid=%d",
			pWrapper->getOption("files:endcheckdelay",45),tabid,datapartid);
		if(mt.Wait()!=1) return false;
		tm1=mt.GetLong(0,0);
		lgprintf("距离文件延迟:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//还没到最后一个文件处理结束的延迟时间
	}	
	lgprintf("文件抽取结束，共%d个文件(序列号%d-%d)。",filect,minseq,maxseq);
	return true;
}

int ZteTxtParse::GetFileSeq(const char *filename) {
	// Jira-49：add two control parameters to file load .
	char seqbuf[50];		
	int backoff=pWrapper->getOption("files:sequencebackoffset",0);
	int seqlen=pWrapper->getOption("files:sequencelen",4);
	
	//Begin: 文件序号crc产生
	if(pWrapper->getOption("files:crcsequence",0)==1)
	{
	    int fileSeq = 0; // 文件序号
		  fileSeq = crc32(0,(const Bytef *)filename,(uInt)strlen(filename));
		  return fileSeq;
	}
	//End:文件序号crc产生
	
	if(backoff<0) backoff=0;
	const char *pseq=filename+strlen(filename)-backoff-1;
	while(!isdigit(*pseq) && pseq>filename) pseq--;
	// last digit should be included 
	pseq++;
	if(pseq-filename<seqlen) seqlen=pseq-filename;
	strncpy(seqbuf,pseq-seqlen,seqlen);
	seqbuf[seqlen]=0;
	pseq=seqbuf;
	while(*pseq) {
		if(!isdigit(*pseq)) {
			strcpy((char *)pseq,pseq+1);
		}
		else pseq++;
	}
	return atoi(seqbuf);
}
int ZteTxtParse::parsercolumn(int memtab,const char *cols,int *colsflag) {
	memset(colsflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	if(!cols || strlen(cols)<1) return 0;
	int colct=0;
	int colarray[MAX_COLUMN];
	int cvtct=wociConvertColStrToInt(memtab,cols,colarray);
	for(int i=0;i<cvtct;i++)
		colsflag[colarray[i]]=1;
	return cvtct;
}
// fixflag collen use 0 base index
int ZteTxtParse::parserfixlen(int memtab,const char *cols,int *fixflag,int *collen){
	//split colname
	memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
	if(!cols || strlen(cols)<3) return 0;
	char colname[COLNAME_LEN];
	const char *pcol=cols;
	int cni=0;
	int colct=0;
	memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
	while(*pcol) {
		if(*pcol==':' || !pcol[1]) {
			colname[cni]=0;
			cni=0;
			int idx=wociGetColumnPosByName(memtab,colname);
			fixflag[idx]=1;
			collen[idx]=atoi(pcol+1);
			colct++;
			while(*pcol && *pcol!=',') pcol++;
			if(!pcol) break;
		}
		else colname[cni++]=*pcol;
		pcol++;
	}
	return colct;	
}

// return value:
//  -1: a error occurs.例如内存表字段结构不匹配
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and 
// sequence of file in dp dictions tab.
int ZteTxtParse::WriteErrorData(const char *line) {
	char errfile[300];
	strcpy(errfile,pWrapper->getOption("files:errordatapath",""));
	//未设置错误文件存放路径，则不报错错误数据
	if(errfile[0]==0) return 0;
	if(errfile[strlen(errfile)-1]!='/') strcat(errfile,"/");
	strcat(errfile,basefilename.c_str());
	strcat(errfile,".err");
	FILE *err=fopen(errfile,"a+t");
	if(err==NULL) {
		lgprintf("输出错误到文件'%s'失败，请检查权限!",errfile);
	}
	//不需要加回车
	if(fputs(line,err)<1)
		lgprintf("输出错误到文件'%s'失败，可能是文件系统满!",errfile);
	fclose(err);
	return 1;
}


// 截取字符串及长度，add by liujs 例如： AAA:1,BBB:2,DDD:4,CCC:8,
// 获取1,2,4,8,及判断是否正确
int ZteTxtParse::GetColumnInfo(const char* pColumnWidthInfo,int memtab,/*char* pColumnsName,*/int * columnsId)
{
    std::string str;
    std::string strColName="";           //列名
    std::string strNameTemp,strLenTemp;  //列名，长度
    str = pColumnWidthInfo;              
    int iPos = 0;                           
    int temp = 0;                        
    int first=0;                    
    int colIndex = 0;                   // 列索引
    int colType =0;                     // 列类型             
    int colLen = 0;                     // 列宽度(控制文件中指定的宽度)
    iPos = str.find_first_of(':',0);
    while(iPos>0)
    {
        // 查找列名
        if(first==0)
        {
            strNameTemp = str.substr(0,iPos);
            first=1;
        }
        else
        {
            strColName += ",";
            strNameTemp = str.substr(0,iPos);
        }
        strColName += strNameTemp;
        
        // 移除已经截取过的列名
        str = str.substr(iPos+1,str.size());
        
        // 查找分隔符“,”的位置
        iPos = str.find_first_of(',',0);
        // 获取长度
        if(iPos <=0 )
        {
            temp = str.find_first_of(':',0);
            if(temp<=0)
            {
                strLenTemp = str.substr(0,str.size());
            }
            else
            {
                ThrowWith("请检查配置文件是否正确，内容：%s!",pColumnWidthInfo);
            }
        }
        else
        {
            strLenTemp = str.substr(0,iPos);
            str = str.substr(iPos+1,str.size());
        }	
        
        // 通过列名获取列索引，列宽度，列类型
        colIndex = wociGetColumnPosByName(memtab,strNameTemp.c_str());
        colType = wociGetColumnType(memtab,colIndex);
        colLen = atoi(strLenTemp.c_str());
        columnsId[colIndex] = colLen;
           
        // 判断列的类型是否正确
        if(COLUMN_TYPE_INT != colType && COLUMN_TYPE_BIGINT != colType)
        {    
            ThrowWith("转换的数据类型不对，请确保'%s'列的数据类型错误!",strNameTemp.c_str());
        }
                
        // 根据类型判断数据长度是否正确
        if(colLen !=1 && colLen != 2 && colLen != 4 && colLen != 8)
        {
            ThrowWith("转换的数据类型宽度不对，请确保'%s'列的数据宽度为[1,2,4,8]!",strNameTemp.c_str()); 	
        }
        if(COLUMN_TYPE_INT == colType && (0>=colLen || 4<colLen))
        {
            ThrowWith("截取的数据长度不对，请确保'%s'列的数据长度为[1-4]!",strNameTemp.c_str());
        }
        if(COLUMN_TYPE_BIGINT == colType && (4>=colLen || 8<colLen))
        {
            ThrowWith("截取的数据长度不对，请确保'%s'列的数据长度为[1-4]!",strNameTemp.c_str());
        }
                
        // 查找分隔符“:”
        iPos = str.find_first_of(':',0);
    }
    //strcpy(pColumnsName,strColName.c_str());
    return 0;
}

int ZteTxtParse::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
	AutoMt mt(0,10);
	mt.SetHandle(memtab,true);
	char sepchar[10];
	errinfile=0;
	if(fp==NULL)
	{
		fp=fopen(filename.c_str(),"rb");
		if(fp==NULL) ThrowWith("文件'%s'打开失败!",filename.c_str());
		//检查字段名称
		strcpy(line,pWrapper->getOption("files:header",""));
		if(strlen(line)<1) {
			if(fgets(line,MAXLINELEN,fp)==NULL) ThrowWith("读文件'%s'失败!",filename.c_str());
			curoffset=1;  //行数
		}
		else curoffset=0;  //行数
		int sl=strlen(line);   //记录长度
		if(line[sl-1]=='\n') line[sl-1]=0;
		if(line[0]=='*') {
			colct=wociGetColumnNumber(memtab);  //获取字段数
			for(int i=0;i<colct;i++) 
				colspos[i]=i;
		}
		else colct=wociConvertColStrToInt(memtab,line,colspos);  //获取字段数
		filerows=0;		
		
    #if 1 // add by liujs		
    // begin: 获取真实的列宽度,存入对应列的数组内
    memset(realColLenAry,0x0,MAX_COLUMNS*sizeof(int));
    memset(fileColLenAry,0x0,MAX_COLUMNS*sizeof(int));
    fileRowLen = 0;
    
    // 获取文件中指定列的宽度
    char * pColumnWidthInfo = NULL;
    pColumnWidthInfo = (char*)malloc(2048);
    strcpy(pColumnWidthInfo,pWrapper->getOption("files:column_width",""));
    GetColumnInfo(pColumnWidthInfo,memtab,fileColLenAry);
    if(NULL != pColumnWidthInfo)
    {
        free(pColumnWidthInfo);
        pColumnWidthInfo = NULL;
    }		
    
    int i,j;
    int colType = 0;
    for(i=0;i<colct;i++) 
    {
        // 获取列类型，如果是字符串类型，则列宽度需要减1
        colType = wociGetColumnType(memtab,colspos[i]);
        
        // 对应列的真实长度
        if(fileColLenAry[colspos[i]] == 0)
        {
            // 没有指定长度的字段，从mt中获取，获取的时候长度需要减1
            if(COLUMN_TYPE_CHAR == colType)
            {
                realColLenAry[colspos[i]] = wociGetColumnDataLenByPos(mt,colspos[i]) - 1;
            }
            else if(COLUMN_TYPE_DATE == colType)
            {
            	  // 时间长度为14字节的文本---》数据库中为7字节存储
            	  realColLenAry[colspos[i]] = 14;
            }
            else
            {
            	  // 未指定的整形数据
            	  realColLenAry[colspos[i]] = wociGetColumnDataLenByPos(mt,colspos[i]); 
            }
        }
        else
        {
            // 已经指定字段的长度，用文件中指定的
            realColLenAry[colspos[i]] = fileColLenAry[colspos[i]];
        }
    }	
    // 考虑到顺序的颠倒的情况，及过滤列，如果所有字段都获取数据，下面代码不起作用
    for(i=0;i<colct;i++) 
    {
        // 获取列类型，如果是字符串类型，则列宽度需要减1
        colType = wociGetColumnType(memtab,colspos[i]);
        
        // 获取对应列之前的所有列的长度数据，获取数据库中存在，文件中存在，但是不抽取的数据长度
        for(int j=0;j<colspos[i];j++)
        {
            if(realColLenAry[j] == 0)
            {
               	// 没有指定长度的字段，从mt中获取，CHAR获取的时候长度需要减1
                if(COLUMN_TYPE_CHAR == colType)
                {
                    realColLenAry[j] = wociGetColumnDataLenByPos(mt,colspos[j]) - 1;
                }
                else if(COLUMN_TYPE_DATE == colType)
                {
                	  // 时间长度为14字节的文本---》数据库中为7字节存储
                	  realColLenAry[j] = 14;
                }
                else
                {
                	  // 未指定的整形数据
                	  realColLenAry[j] = wociGetColumnDataLenByPos(mt,colspos[j]); 
                }
            }
            else
            {
                // 已经存储了数据列的宽度，不用添加数据长度
                continue;
            }
        }
    }	
    // end：获取真实的列宽度
    
    // begin: 获取每次读取的数据长度
    for(i=0;i<MAX_COLUMNS;i++)
    {
        fileRowLen += realColLenAry[i];
    }
    // end: 获取每次读取的数据长度		
    #endif
		
		lgprintf("文件'%s'开始处理.",filename.c_str());
	}
		int *pnszVarCol = new int[MAX_FILE_COLUMNS];
        int *pnszTypeLen = new int[MAX_FILE_COLUMNS];
		memset(pnszVarCol,0,sizeof(int)*MAX_FILE_COLUMNS);
		memset(pnszTypeLen,0,sizeof(int)*MAX_FILE_COLUMNS);
		
		int rows=mt.GetRows();
		int maxrows=mt.GetMaxRows();
		int nWrNum = 1000000000;
		_wdbiGetVarCol(mt.handle, pnszVarCol, pnszTypeLen);
		char **pWrBf = new char* [MAX_FILE_COLUMNS];
		for(int t = 0;t<colct;t++)
		{
            if(pnszVarCol[t])
            {
               int nRow = (TEN_M_SIZE/pnszTypeLen[t])-1;
		       nWrNum = nWrNum > nRow?nRow:nWrNum;
			}
		}
		for(int t=0;t<colct;t++)
		{
		  if(pnszVarCol[t])
		  {
		     pWrBf[t] = new char[nWrNum * pnszTypeLen[t]];
			 memset(pWrBf[t],0,nWrNum*pnszTypeLen[t]);
		  }
			else
			{
			   pWrBf[t] = NULL;
			}  	
		}




	char colname[300];
	char ipaddr[20];
	
	// colum start pos
	int colPos = 0;
	// colum width
	int colWidth = 0;
	bool ipcheck=pWrapper->getOption("files:ipconvert",0)==1;
	int recordfmtcheck=pWrapper->getOption("files:recordfmtcheck",1);
	
	// 截取日期时间类型字段用
    char temp1[5] = {0};
	char temp2[3] = {0};
	int _pos = 0;
	
    unsigned long value_8B = 0;
    unsigned int value_4B = 0;
    unsigned short value_2B = 0;

	curoffset = 0;
	long nVarRow = 0;
	int nCol = 0;
	int nVarLevelRow = 0;
	while(rows<maxrows)
	{
	    curoffset ++;
		if(1!=fread(line,fileRowLen,1,fp))
		{
			 // read error or finish
			 break;
		}
		// 获取起始读取的偏移量 
		colPos = 0;
		for(int i=0;i<colct;i++) 
		{   
			// 实际获取数据的列宽度
			colWidth = realColLenAry[colspos[i]];
			nCol = colspos[i];
			switch(mt.getColType(colspos[i])) 
			{
			case COLUMN_TYPE_CHAR	:
				
				if(ipcheck) 
				{
					wociGetColumnName(mt,colspos[i],colname);
					if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr((line + colPos),'.')==NULL) {
						int ip=0;
						memcpy(&ip, line + colPos, 4);
						unsigned char *bip=(unsigned char*)&ip;
						//TODO : this only work on little-endian system
						char ipaddr[20] = {0};
						sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
						if(ip==0) ipaddr[0]=0;
						if(pnszVarCol[nCol])
				       {
				          memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],ipaddr,strlen(ipaddr));
				       }
					   else{
                           strcpy(mt.PtrStrNoVar(nCol,rows),ipaddr);
					   }
						//strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
						//mt.SetStr(colspos[i],rows,ipaddr);
						
					}
				}
				else
				{	
				    if(pnszVarCol[nCol])
				    {
                       memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],line+colPos,colWidth);
					}
					else{
                        memcpy(mt.PtrStrNoVar(colspos[i],rows),line+colPos,colWidth);
					}
				   
				}
				//*(mt.PtrStr(colspos[i],rows)+colWidth) = '\0';
				break;
			case COLUMN_TYPE_FLOAT	:
				ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				break;
			case COLUMN_TYPE_INT	:
				if(1 == colWidth)
				{
					*(mt.PtrInt(colspos[i],rows)) = (unsigned char)*(line+colPos);
				}
				else if(2 == colWidth)
				{
					zkb_revShort(line+colPos); 
		            value_2B = 0;
					memcpy(&value_2B,line+colPos,colWidth);
  				   *mt.PtrInt(colspos[i],rows)= value_2B;			      
				}				 		
				else if(4 == colWidth)
				{
					zkb_revInt(line+colPos);
					value_4B = 0;		
					memcpy(&value_4B,line+colPos,colWidth);
					*mt.PtrInt(colspos[i],rows)= value_4B;						
				}
				else{
					ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				}
				break;
			case COLUMN_TYPE_BIGINT	:
				zkb_revDouble(line+colPos);
				value_8B = 0;
				memcpy(&value_8B,line+colPos,colWidth);
				*mt.PtrLong(colspos[i],rows)= value_8B;			
				break;
			case COLUMN_TYPE_DATE	: 
				int year;
				short mon,day,hour,min,sec;
				_pos = 0;
				strncpy(temp1,line+colPos,4);
				year = atoi(temp1);
				_pos+=4;
				
				strncpy(temp2,line+colPos+_pos,2);
				mon = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				day = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				hour = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				min = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				sec = atoi(temp2);				
				
				if(year != 0 && mon !=0 && day != 0)
				{ // 如果年月日均为空，说明是数据是空的
				    if(year<1900 || mon<1 || mon>12 || day<1 || day>31 || hour<0 || hour>60 || min<0 || min>60 ||sec<0 || sec>60) 
				    {
  			    	    ThrowWith("文件'%s'第%d行第%d字段错误的日期！",filename.c_str(),filerows+1,i+1);
				    	errinfile++;
				    	break;
				    }
			    }
				wociSetDateTime(mt.PtrDate(colspos[i],rows),year,mon,day,hour,min,sec);
				break;
			case COLUMN_TYPE_NUM :
			default :
				ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				break;
			}// end switch
			// 每一行位置移动
			colPos += realColLenAry[colspos[i]];
		}  // end for
		filerows++;
		rows++;
		nVarRow++;
		if(nVarRow == nWrNum)
		{
        for(int i=0;i<colct;i++)
        {  
			     if(pnszVarCol[colspos[i]])
           {
                mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
				        memset(pWrBf[colspos[i]],0,nWrNum*pnszTypeLen[colspos[i]]*sizeof(char));
			     }
			}
      nVarLevelRow =nVarLevelRow + nVarRow;
			nVarRow = 0;
			_wdbiSetMTRows(mt,rows);
		}
	}// end while

	 for(int i=0;i<colct;i++)
   { 
       if(pnszVarCol[colspos[i]])
       {
          mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
			    delete[] pWrBf[colspos[i]];
	     }
	 }
	_wdbiSetMTRows(mt,rows);

	delete[] pnszTypeLen;
	delete[] pnszVarCol;
	
	if(rows==maxrows) return 0;
	AutoStmt st(dbc);
	/*
	st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now() where tabid=%d and datapartid=%d and fileseq=%d",
		filerows,tabid,datapartid,fileseq);
	*/
		/***************文件解析结果记录*****************/
	//获取错误数据存储位置
	{
	  char errfilename[300];
	  strcpy(errfilename,pWrapper->getOption("files:errordatapath","/tmp"));
	  //未设置错误文件存放路径，则不报错错误数据
	  if(errfilename[0]==0) return 0;
	  if(errfilename[strlen(errfilename)-1]!='/') strcat(errfilename,"/");
	  strcat(errfilename,basefilename.c_str());
	  strcat(errfilename,".err");
	  st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now(),errrows=%d,errfname='%s' where tabid=%d and datapartid=%d and fileseq=%d",filerows,errinfile,errfilename,tabid,datapartid,fileseq);
  }
	st.Execute(1);
	st.Wait();
	lgprintf("文件'%s'处理结束，共%d行记录，文件%d行，错误%d行。",filename.c_str(),filerows,curoffset,errinfile);
	fclose(fp);fp=NULL;
	if(errinfile>0)
		sp.log(tabid,datapartid,106,"文件'%s'处理结束，共%d行记录，文件%d行，错误%d行。日志文件: %s ",filename.c_str(),filerows,curoffset,errinfile,_wdbiGetLogFile());
	if(fm.SrcFileIsZipped() || (pWrapper->getOption("files:keeppickfile",0)!=1 && pWrapper->getOption("files:skippickup",0)!=1) )
		unlink(filename.c_str());
	
	//>> Begin: delete src file ,after parsed src file,DM-216
	fm.DeleteSrcFile();
	//<< End:
	
	curoffset=0;
	return 1;
}
