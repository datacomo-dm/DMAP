/* gprs_parser.h --

   This file is part of the DataMerger data extrator.

   Copyright (C) 2010 DataComo
   All Rights Reserved.
   
   Wang Guosheng
   <wangguosheng@datacomo.com>
   http://www.datacomo.com
   http://mobi5.cn
 */

#ifndef COMMONTXT_PARSER_H
#define COMMONTXT_PARSER_H
#include "dumpfile.h"

class ZteTxtParse: public FileParser {
	char *fieldval ;
	char *linebak;
	int yyyy,dd,mm,hour;
	int ExtractNum(int offset,const char *info,int len) ;
	bool SwitchOpt(char *deststr,char *opt,int val,int len);
	bool SwitchOpt(char *deststr,char *opt,char *value) ;
	//返回分解后的字段数量,colidx存放字段索引，collen存放固定长度
	int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
	int parsercolumn(int memtab,const char *cols,int *colsflag);
	
	// add by liujs
	// 真实的列宽度（不需要转换的列，数据长度与数据库中一样，需要转换的列，数据长度文件中指定）
	int realColLenAry[MAX_COLUMNS];
	// 文件中需要转换的列的长度，例如：col1:1,col2:4,col3:5;内存存储1,4,5
	int fileColLenAry[MAX_COLUMNS];
	//每次读取多少字节数据
	int fileRowLen;
	// 截取字符串及长度，add by liujs 例如： SSS:12,BBB:23,DDD:24
	// 得到：SSS,BBB,DDD   及 12，23，24
    int GetColumnInfo(const char* pColumnWidthInfo,int memtab,/*char* pColumnsName,*/int * columnsId);
public :
	ZteTxtParse():FileParser(){
		linebak =new char[MAXLINELEN];
		fieldval =new char[MAXLINELEN];
		
		memset(realColLenAry,0x0,MAX_COLUMNS*sizeof(int));
		memset(fileColLenAry,0x0,MAX_COLUMNS*sizeof(int));
		fileRowLen = 0;
		
		yyyy = dd = mm = hour = -1;
	}
	~ZteTxtParse() {
		delete []linebak;delete []fieldval;
	}
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// 返回值：
	//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
	//  0：暂无文件
	//  1：取得有效的文件
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// 必须是不再有文件需要处理的时候，才能调用这个检查函数
	virtual bool ExtractEnd(int _dbc) ;
	virtual int WriteErrorData(const char *line) ;
	virtual int GetFileSeq(const char *filename) ;
	
	// return value:
	//  -1: a error occurs.例如内存表字段结构不匹配
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};

IFileParser *BuildParser(void);
/*
int tabid,int datapartid) ;
FileParser *BuildParser(void);
int tabid,int datapartid) ;
FileParser *BuildParser(void);
*/
#endif
