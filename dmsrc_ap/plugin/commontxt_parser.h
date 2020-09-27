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
#include "column_parser.h"
    
class CommonTxtParser: public FileParser {
    char *fieldval ;
    char *linebak;
    int yyyy,dd,mm,hour;
    int ExtractNum(int offset,const char *info,int len) ;
    bool SwitchOpt(char *deststr,char *opt,int val,int len);
    bool SwitchOpt(char *deststr,char *opt,char *value) ;
    //返回分解后的字段数量,colidx存放字段索引，collen存放固定长度
    int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
    int parsercolumn(int memtab,const char *cols,int *colsflag);
    //>> begin:fix DMA-604,需要调整列宽度的列
    bool colsAdjustWidth[MAX_FILE_COLUMNS]; // 存储的值与colspos相对应
    //<< end:fix dma-604  


private: 
    //>> begin: fix dma-1086, http解析
    Column_parser _column_parser_obj;            // http 解析查询关键字和下载应该使用
    #define HTTP_DOWNLOAD_LEN  512
    typedef struct stru_Http_translate{
        stru_Http_translate(){
            init();
        }
        void init(){
            _http_col = _key_col = _download_col = -1;
            _key_col_value[0] = 0;
            _download_col_value[0] = 0;
            _update_check = 0;
        }
        bool is_valid(){
            if(_http_col != -1){
                if(_key_col != -1 || _download_col != -1){
                    return true;
                }
            }
            return false;
        }
        int  get_column_count(){
            if(is_valid()){
                if(_key_col != -1 && _download_col != -1){
                    return 2;
                }else{
                    return 1;
                }
            }
        }
        int _http_col;                                 // http 内容列,http列，关键字列，下载列必须是按这个顺序排序

        int _key_col;                                  // 关键字列
        char _key_col_value[HTTP_DOWNLOAD_LEN];        // 有效的关键字
        
        int _download_col;                             // 下载列
        char _download_col_value[HTTP_DOWNLOAD_LEN];   // 有效的下载列

        int _update_check;               // 用于判断从http中解析出来的[关键字和下载内容是否已经更新到关键字和下载内容列中,更新完成后应该0,更新前应该是列的有效数]
    }stru_Http_translate,*stru_Http_translate_ptr;
    stru_Http_translate _st_http_translate_obj;
    //<< end: fix dma-1086

private:
    //>> begin: fix dma-794(入库时间),dma-941(入库文件名称)
    typedef struct stru_external{
        stru_external(){
            init();
        }
        void init(){
            _load_date_col_index = _load_filename_col_index = -1;
        }
        bool is_valid(){
            if(_load_filename_col_index != -1 || 
                _load_filename_col_index != -1)
                return true;
            else 
                return false;
        }
        int  get_column_count(){
            if(is_valid()){
                if(_load_filename_col_index != -1 && 
                    _load_date_col_index != -1){
                    return 2;
                }else{
                    return 1;
                }
            }
        }
        int _load_date_col_index;      // 装入时间
        int _load_filename_col_index;   // 装入列文件名称
    }stru_external,*stru_external_ptr;
    stru_external _st_external_obj;
    
    //>> end: fix dma-794,dma-941

private:
    // 判断列数是否准确
    // _file_col_num: 文件中确实存在的列
    // _header_col_num: files::header中定义的列
    // _st_ext: 入库时间和入库文件名称列
    // _st_http: http 解析列信息
    // _msg: 出错提示信息
    bool check_column_count(const int _file_col_num,const int _header_col_num,stru_external& _st_ext,stru_Http_translate& _st_http,std::string& msg);

    // 将解析过程中发现的错误内容写入文件完成后进行压缩操作,fix dma-1159
    bool CompressErrData();
    
public :
    CommonTxtParser():FileParser(){
        linebak =new char[MAXLINELEN];
        fieldval =new char[MAXLINELEN];
        
        yyyy = dd = mm = hour = -1;
    }
    ~CommonTxtParser() {
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
#endif
