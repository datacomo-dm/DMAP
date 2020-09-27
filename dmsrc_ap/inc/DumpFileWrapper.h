/* DumpFileWrapper.h --

控制文件及插件转载部分，与文件操作相关分离开来

Copyright (C) 2012 DataComo
All Rights Reserved.

liujianshun
<liujianshun@datacomo.com>
http://www.datacomo.com
http://mobi5.cn

JIRA : DM-201
*/
#ifndef DUMPFILEWRAPPER_DEF_H
#define DUMPFILEWRAPPER_DEF_H
#include "IDumpfile.h"
#include "dt_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <string>
#include <dlfcn.h>

// dynamic load as library(plugin) ,should come later.
typedef void * (*MODULE_HANDLE)();
# if (defined(__linux__)||defined(__std_plugin_sym__))
#define BUILDPARSER_SYM "_Z11BuildParserv"
# else
#define BUILDPARSER_SYM "BuildParser__Fv"
#endif

class LibAdmin 
{
protected:
	void *handle;
	IFileParser *pFileParser;
public:
	LibAdmin(const char *libname) ;
	IFileParser *getParser() ;
	~LibAdmin();//需要释放后向定义的pFileParser，在cc文件中定义
};

class DumpFileWrapper : public IDumpFileWrapper
{
protected:
	std::map<std::string,std::string> options;
	LibAdmin *pla;
	int split(char *text,char **tokens,char delimter) ;
public :
	~DumpFileWrapper() ;
	IFileParser *getParser() ;

	// 装入控制文件参数，以及文件分解模块
	DumpFileWrapper(const char *ctlfile);
	// 取控制参数
	// keyval:对于按照类别分开的参数，统一按照section:item的格式，比如
	//    ftp:username
	const char *getOption(const char *keyval,const char *defaults) ;
	const char getOption(const char *keyval,const char defaults) ;
	int getOption(const char *keyval,int defaults) ;
	double getOption(const char *keyval,double defaults);
};

#endif
 
