#ifndef COLUMN_PERSER_DEF_H
#define COLUMN_PERSER_DEF_H

/*********************************************************************************
*   说明: 该类用于文本数据解析过程中的数据转换使用,
*         在 virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) 函数中进行调用
*         例如:
*             1. wins 系统中应用,将HTTP列中内容进行截取转换得到其中的关键字和下载内容
*    
*   作者: 刘建顺
*    
*   时间: 2014/04/04
*/
#include <stdio.h>
#include <dlfcn.h>
#include "dt_common.h"
#include "wdbi_inc.h"
#include "iconv.h"
#include <string>
#include <assert.h>
#include <vector>

#ifndef MAX_SEARCH_ENGINES
#define  MAX_SEARCH_ENGINES  50
#endif

#ifndef  MAX_BUF_SIZE
#define  MAX_BUF_SIZE  256
#endif

class Column_parser{
public:
    Column_parser();
    virtual ~Column_parser();

private:
    enum {SET_UNKNOW=-1,SET_GBK=1,SET_UTF8};
    
    typedef struct stru_adms_search_engines_info{ // 关键字查找算法
        stru_adms_search_engines_info(){
            lSearchId = -1;
            cEncodingType = SET_UNKNOW;
            cKeyType = 0;
            cMatchStrStart[0]=cMatchStrEnd[0]=cHostName[0]= cMatchStrFilter[0] = 0;
        }
        int lSearchId;
        int cEncodingType;
        int cKeyType;
        char cMatchStrStart[MAX_BUF_SIZE];
        char cMatchStrEnd[MAX_BUF_SIZE];
        char cHostName[MAX_BUF_SIZE];
        char cMatchStrFilter[MAX_BUF_SIZE];
    }stru_adms_search_engines_info,*stru_adms_search_engines_info_ptr;
    stru_adms_search_engines_info adms_search_engines_info[MAX_SEARCH_ENGINES];

    typedef struct stru_iconv{
        stru_iconv(){
            is_open = false;
        }
        bool is_open;
        iconv_t cd;
    }stru_iconv,*stru_iconv_ptr;
    stru_iconv  st_iconv_gbk;     // code_convert 内部使用
    stru_iconv  st_iconv_utf8;    // code_convert 内部使用

private:
    std::vector<std::string> download_extern_name_list;

private:
    void init_adms_and_key_info();   // 初始化查找关键字的匹配对象  
    int _get_keyword_from_http(const stru_adms_search_engines_info* st,const std::string& http,std::string& keyword); 
    
private:
    int  is_text_utf8(const char* in_stream,const int stream_len);  // 1: utf8  0:other
    int  change_buff_info(char* oribuf);
    int  code_convert(char *from_charset,char *to_charset,const char *inbuf, size_t inlen,char *outbuf, size_t& outlen);
    
public: // wins http 转换使用,获取关键字和下载内容
    int   get_keyword_from_http(const char* phttp,std::string& keyword);  //  return keyword len
    int   get_download_from_http(const char* phttp,std::string& download);
    void  set_download_extern_name(const char* p_download_extern_name);

public: // TODO : other
};
typedef Column_parser* Column_parser_ptr;

#endif
