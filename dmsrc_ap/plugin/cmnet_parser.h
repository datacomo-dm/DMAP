/* gprs_parser.h --

   This file is part of the DataMerger data extrator.

   Copyright (C) 2010 DataComo
   All Rights Reserved.
   
   Wang Guosheng
   <wangguosheng@datacomo.com>
   http://www.datacomo.com
   http://mobi5.cn
 */
#ifndef CMNET_PARSER_H
#define CMNET_PARSER_H
#include "dumpfile.h"

class ZJCmnetParser: public FileParser {

	char yyyy[20],dd[10],mm[10];
public :
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// 返回值：
	//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
	//  0：暂无文件
	//  1：取得有效的文件
	// virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid) ;
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false);
	
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// 必须是不再有文件需要处理的时候，才能调用这个检查函数
	virtual bool ExtractEnd(int _dbc) ;
	
	virtual int GetFileSeq(const char *filename) ;
	virtual int WriteErrorData(const char *line){ThrowWith("ZJCmnetParser WriteErrorData Error");return -1;};
	
	// return value:
	//  -1: a error occurs.例如内存表字段结构不匹配
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};
IFileParser *BuildParser(void);
#endif
