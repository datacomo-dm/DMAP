
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#ifndef DUMPFILE_H
#define DUMPFILE_H
#include "dt_common.h"
#include <stdio.h>
#include <map>
#include <string>
#include <dlfcn.h>
#include "dt_svrlib.h"
#include "IDumpfile.h"
#include "dmncftp.h"
#include "hdfs.h"
#include <vector>

#define MAX_COLUMNS 500
class file_man
{
    FILE *flist;
    bool ftpmode;
    //duplicated process file
    bool ascmode;
    //忽略本地复制文件
    bool skippickup;
    //忽略文件备份
    bool skipbackup;
    char lines[300];
    //bool firstpart;
    //char *ptr;
    char LISTFILE[300];
    char bakuperrfile[300];
    char host[100],username[100],passwd[100],path[300],localpath[300];
    char localfile[300];
    // 是否删除原文件 ,add by liujs
    enum DELETE_FILE_FLAG
    {
      DELETE_NO = 0, // 不删除文件
      DELETE_YES = 1, // 删除文件
    };
    enum REPLASE_FTP_GET_PARAM // 替换1: 表示需要替换ftpget文件的参数%2F
    {
      REPLASE_NO = 0,  // 不替换
      REPLASE_YES = 1, // 替换
    };
    int delFileFlag;           // 删除文件标识
    //>> Begin: delete src file 20130131
    char current_file[300];    // 当前正在处理的文件名称，删除源文件使用，包括完整路径
    long current_filesize;     // 记录当前文件大小
    bool is_backup_dir;        // 是否是backup目录,如果是backup目录，则不删除源文件
    //<< end:
    
    FTPClient ftpclient;

    // hadoop 连接对象
    hdfsFS  hdfsclient;
    char hdfs_path[300];
    char backup_dir[300];
    bool hdfs_mode;
    long hdfs_getfilesize(hdfsFS hf,const char* path,const char* file);
    int hdfs_copy_file_to_local(hdfsFS hf,const char* path,const char* file,const char* localfile);
public:
    std::string _get_file_title();
    file_man();
    ~file_man() ;
    void SetLocalPath(const char *p);
    void SetAscMode(bool val) ;
    void SetSkipPickup(bool val) ;
    void SetSkipBackup(bool val) ;
    void FTPDisconnect();
    void listHdfs(const char* nodename,const int port,const char* path,const char* filepatt=NULL);
    void list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt);
    bool listhasopen() {return flist!=NULL;}
    void clearlist() {if(flist) fclose(flist);flist=NULL;unlink(LISTFILE);}
    void listlocal(const char *_path,const char *filepatt); 
    const char *getlocalfile(const char *fn) ;
    //获取文件并做备份，如果backupfile==null,或者backupfile[0]=0,则不备份
    //  如果文件不是.gz,则备份时做gzip压缩
    bool getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid);
    bool getnextfile(char *fn,char *full_fn = NULL);
    // 设置删除标志
    inline void SetDeleteFileFlag(const int delFlag){delFileFlag = delFlag;};
    inline bool GetFtpMode(){return ftpmode;};
    inline void SetIsBackupDir(bool isBackup){ is_backup_dir = isBackup;};
    inline bool GetIsBackupDir(){return is_backup_dir;};
    int  DeleteSrcFile();
    inline long GetCurFileSize(){return current_filesize;};
    inline void SetCurrentFile(const char* fn){
        strcpy(current_file,fn);
    }
    bool SrcFileIsZipped() {
        return strcmp(current_file+strlen(current_file)-3,".gz")==0;
    }
	inline char* GetPath(){return path;};
    inline void SetBackupDir(const char* pbackup_dir){
        strcpy(backup_dir,pbackup_dir);
    }
};

//plugin must implemtation subclass of FileParser 
// construction a subclass of FileParser to process a 
//  specified table,once table parse end,destruction object,and next
//  table build another parser
#define MAXLINELEN 40000

#define MAX_FILE_COLUMNS  600
#define BACKUP_LOGFILE "backup.log"

#ifndef _int64_
#ifdef __GNUC__
typedef long long int _int64;
#endif
#define _int64_
#endif

#define PLUS_INF_64     _int64(0x7FFFFFFFFFFFFFFFULL)
#define MINUS_INF_64    _int64(0x8000000000000000ULL)

class FileParser : public IFileParser{
public:
    char fix_ftp_path[300];     // 修正过后的ftp路径,为支持filename中的目录部分移动到path中
    char fix_localpath[300];    // 修改过后的local路径,为支持filename中的目录部分移动到path中
    char fix_cur_pach_md5[34];  // 当前路径的MD5值,fix DMA-1315:多路径下文件重名问题解决处理
    char fix_filepatt[300];     // 修正过后的filename
    char fix_filename[300];     // 文件名称
    // 将路径名称中的目录名称和文件名称分别取出来 
    void splite_path_name(const char* path,char* dir,char* name);

public:
    std::vector<std::string> str_path_vector;    // 路径名称列表,从多路径下采集文件
    int  current_path_index ;                    // 当前路径索引
    void splite_path_info(const char* str_path_list,std::vector<std::string>& str_path_vector);
protected :
    int dbc;
    int filerows;
    int fileseq;
    std::string filename;
    // file must be able to hold larger than 4GB contents
    FILE *fp;
    file_man fm;
    int curoffset;//offset line
    int tabid;
    int datapartid;
    int colspos[MAX_FILE_COLUMNS];
    void *colptr[MAX_FILE_COLUMNS];
    int colct;
    char *line;

    // 数据库表中存在的列(字符串列)，但是文件中不存在,在填充mt的时候，需要将其设置为null
    int nulls_colspos[MAX_FILE_COLUMNS];
    int nulls_colct;
    
    //不包含路径的文件名
    std::string basefilename;
    int errinfile;
    int ignore_rows;

    IDumpFileWrapper *pWrapper;
    //公用文件抽取函数
    //  从  FTP SERVER/本地路径/备份目录 抽取源文件，如果是压缩文件(.gz),则解压缩
    //    除备份目录外抽取文件方式以外，将文件做备份，如果源文件不是压缩的，则压缩备份文件，否则，直接备份
    //    backuppath: 备份文件的路径
    //    frombackup:是否从备份路径抽取文件
    //    filepatt:文件查找模式(可以含通配符)
    //  返回值：1, 抽取到有效文件, 0,没抽取到文件,2抽取文件错误，但应该忽略
    // 使用到的参数：
    //     files:localpath,抽取文件目标路径
    //     ftp:textmode/ftp:host/ftp:username/ftp:password/ftp:path, 抽取文件的ftp参数
    //     local:path
    // 取到有效文件时，文件名在filename成员
    int commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;

public :
    FileParser() ;
    virtual ~FileParser();

    virtual int GetErrors() {return errinfile;}
    virtual int WriteErrorData(const char *line) = 0;
    //这个函数必须在对象构造后，在其它成员函数之前调用
    virtual void SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) ;
    // fill filename member,the wrapper check conflict concurrent process,
    // if one has privillege to process current file ,then invoke DoParse
    // 返回值：
    //  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
    //  0：暂无文件
    //  1：取得有效的文件
    virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) = 0;
    // if end ,update task to next step(with lock)
    // critical function !
    // checked every file parser completed.
    // 必须是不再有文件需要处理的时候，才能调用这个检查函数
    virtual bool ExtractEnd(int _dbc) = 0;
    virtual int GetIdleSeconds() ;
    // return value:
    //  -1: a error occurs.例如内存表字段结构不匹配
    //  0:memory table has been filled full,and need to parse again
    //  1:file parse end
    //while parser file ended,this function record file process log and 
    // sequence of file in dp dictions tab.
    virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) = 0;
    virtual int GetFileSeq(const char *filename) = 0;
    virtual void PluginPrintVersion() ;
};

#endif


