
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#include "dumpfile.h"
#include "dmncftp.h"
#include <openssl/md5.h>   // MD5
//>> Begin: 针对FTP list & get 失败后进行重试5次操作，每次重试进行等待2分钟，DM-198 , add by liujs
#define FTP_GET_LIST_RETRY_TIMES    3
#define FTP_OPER_FAIL_SLEEP_SEC     1
//<< End: add by liujs



/*
 *  功能: 将路径中多余的'/'符号取出掉
 *  参数: pattern[in/out]
 *  例如: commontext_test//101206_*.gz ---> commontext_test/101206_*.gz]
 */
void convert_path_pattern(char * pattern)
{
    char tmp[500]= {0};
    char *p = pattern;
    int idx = 0;
    while (*p) {
        if( (idx>0) && (tmp[idx-1]=='/') && (tmp[idx-1] == *p) ) {
            p++;
        } else {
            tmp[idx] = *p;
            idx ++;
            p++;
        }
    }

    strcpy(pattern,tmp);
}

void file_man::FTPDisconnect()
{
    ftpclient.Close();
}
file_man::file_man()
{
    memset(localfile,0,300);
    flist=NULL;
    memset(lines,0,sizeof(lines));
    ascmode=true;
    ftpmode=false;
    //天津移动本地修改
    skippickup=false;
    skipbackup=false;
    sprintf(LISTFILE,"%s/lst/filelist%d.txt",getenv("DATAMERGER_HOME"),getpid());
    strcpy(localpath,"./");

    bakuperrfile[0]=0;
    const char *path=NULL;
    if(path==NULL) path=getenv("WDBI_LOGPATH");
    if(path) {
        strcpy(bakuperrfile,path);

        if(path[strlen(path)-1]!=PATH_SEPCHAR) {
            strcat(bakuperrfile,PATH_SEP);
        }
    }
    strcpy(bakuperrfile,BACKUP_LOGFILE);

    delFileFlag = DELETE_YES;
    memset(current_file,0,300);
    current_filesize = 0;

    is_backup_dir = false;

    hdfs_mode = false;
    hdfsclient = NULL;

}
file_man::~file_man()
{
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);
    if(hdfsclient != NULL) {
        hdfsDisconnect(hdfsclient);
        hdfsclient = NULL;
    }

    memset(current_file,0,300);
}
void file_man::SetLocalPath(const char *p)
{
    strcpy(localpath,p);
}
void file_man::SetAscMode(bool val)
{
    ascmode=val;
}
void file_man::SetSkipPickup(bool val)
{
    skippickup=val;
}
void file_man::SetSkipBackup(bool val)
{
    skipbackup=val;
}

void file_man::list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt)
{
    char remotepath[500];
    strcpy(host,_host);
    strcpy(username,_username);
    strcpy(passwd,_passwd);

    //if(_path[0]=='/')
    //  sprintf(path,"%%2F%s",_path+1);
    //else
    strcpy(path,_path);
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);

    // remote path end with '/' cause list result no path prefix,only filename
    sprintf(remotepath,"%s/%s",     path,filepatt);

    convert_path_pattern(remotepath);

    lgprintf("begin to list ftp , pattern [%s].....",remotepath);

    ftpclient.SetContext(_host,_username,_passwd);
    ftpclient.ListFile(remotepath,LISTFILE);
    lgprintf("获取文件列表 ...");

    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("文件列表失败.打开文件列表%s失败.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    ftpmode=true;
}


// 获取文件大小
long file_man::hdfs_getfilesize(hdfsFS hf,const char* path,const char* file)
{
    char path_file[300];
    sprintf(path_file,"%s/%s",path,file);
    if(hdfsclient == NULL) {
        ThrowWith("hdfs_getfilesize 获取文件%s 大小失败!",path_file);
    }

    hdfsFileInfo * phdfsFileInfo  = hdfsGetPathInfo(hdfsclient,path_file);
    if(phdfsFileInfo == NULL) {
        ThrowWith("hdfs_getfilesize 获取文件%s 大小失败!",path_file);
    }
    hdfsFreeFileInfo(phdfsFileInfo,1);
    return phdfsFileInfo->mSize;

}

// 创建本地文件，通过read，write的方式
#define GZ_BUFLEN 1024*64
int file_man::hdfs_copy_file_to_local(hdfsFS hf,const char* path,const char* file,const char* localfile)
{
    char path_file[300];
    sprintf(path_file,"%s/%s",path,file);
    if(hdfsclient == NULL) {
        ThrowWith("hdfs_copy_file_to_local 获取文件%s ---> %s失败,hdfsclient is null!",path_file,localfile);
    }

    hdfsFile    hdfs_src_file = hdfsOpenFile(hf,path_file,O_RDONLY,0,0,0);
    if(NULL == hdfs_src_file) {
        ThrowWith("hdfs_copy_file_to_local  打开文件%s失败,hdfsclient is null!",path_file);
    }

    FILE *local_dest_fp=fopen(localfile,"wb");
    if(local_dest_fp==NULL) {
        hdfsCloseFile(hf,hdfs_src_file);
        ThrowWith("hdfs_copy_file_to_local:%s打开失败.",localfile);
    }
    char buf[GZ_BUFLEN];
    long wrted=0;
    int errcode=0;
    int readed=hdfsRead(hf,hdfs_src_file,buf,GZ_BUFLEN);
    while(readed>0) {
        int w=fwrite(buf,readed,1,local_dest_fp);
        if(w<1) {
            hdfsCloseFile(hf,hdfs_src_file);
            fclose(local_dest_fp);
            ThrowWith("hdfs_copy_file_to_local:%s->%s写入数据错误.",path_file,localfile);
        }
        wrted+=w;
        readed= readed=hdfsRead(hf,hdfs_src_file,buf,GZ_BUFLEN);
    }
    if(readed == -1) {
        hdfsCloseFile(hf,hdfs_src_file);
        fclose(local_dest_fp);
        ThrowWith("hdfs_copy_file_to_local:read %s 错误.",path_file);
    }
    fclose(local_dest_fp);
    hdfsCloseFile(hf,hdfs_src_file);

    return wrted;
}

// 将hdfs hdfsListDirectory 出来的文件进行获取文件名称
// file:/tmp/pth/tmp/data_2013010206_14.txt --> /tmp/pth/tmp/data_2013010206_14.tx
std::string _get_file_name(const char* ls_hdfs_file)
{
    // printf("begin:--> %s \n",ls_hdfs_file);
    const char *p=ls_hdfs_file;
    std::string _hdfs_file(p+5);
    // printf("end:--> %s \n",_hdfs_file.c_str());
    return _hdfs_file;
}

void file_man::listHdfs(const char* nodename,const int port,const char* path,const char* filepatt)
{
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);

    if(NULL != hdfsclient) {
        hdfsDisconnect(hdfsclient);
        hdfsclient = NULL;
    }
    hdfsclient = hdfsConnect(nodename,port);
    if(hdfsclient == NULL) {
        ThrowWith("连接hdfs文件系统[nodename=%s,port=%s] 失败.\n",nodename,port);
    }

    strcpy(hdfs_path,path);  // 重新获取文件大小使用

    // api : hadoop fs -ls /tmp/*.cpp | ls  -1
    char path_pattern[300] = {0};
#if 0
    // hdfsListDirectory 返回NULL
    sprintf(path_pattern,"%s/%s",path,filepatt);
#else
    sprintf(path_pattern,"%s",path);
#endif

    lgprintf("begin to list hdfs , pattern [%s].....",path_pattern);

    int hdfs_num_entries = 0;
    hdfsFileInfo * phdfsFileInfoAry = hdfsListDirectory(hdfsclient,path_pattern,&hdfs_num_entries);
    if( NULL == phdfsFileInfoAry) {
        lgprintf(" file_man::listHdfs  hdfsListDirectory(%p,%s,%p) return NULL",hdfsclient,path_pattern,&hdfs_num_entries);
        return ;
    } else {
        // 获取文件信息存入list文件中
        FILE *fp=fopen(LISTFILE,"wt");
        if(fp==NULL) {
            ThrowWith("结果文件%s打开失败.",LISTFILE);
        }
        //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        for(int i=0; i<hdfs_num_entries; i++) {
            if(phdfsFileInfoAry[i].mKind == kObjectKindFile) {
                std::string _hdfs_file = _get_file_name(phdfsFileInfoAry[i].mName);
                fprintf(fp,"%s\n",_hdfs_file.c_str());
            }
        }
        fclose(fp);
    }

    lgprintf("获取文件列表 ...");
    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("文件列表失败.打开文件列表%s失败.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    hdfs_mode = true;

    hdfsFreeFileInfo(phdfsFileInfoAry,hdfs_num_entries);

}

std::string file_man::_get_file_title()
{
    std::string _get_file_title;
    if(hdfs_mode)
        _get_file_title = "HDFS GET";
    else if(ftpmode)
        _get_file_title = "FTP GET";
    else
        _get_file_title = "LOCAL COPY";

    return _get_file_title;
}

void file_man::listlocal(const char *_path,const char *filepatt)
{
    char localpath[300];
    if(flist) fclose(flist);
    flist=NULL;
    strcpy(path,_path);
    remove(LISTFILE);

    sprintf(localpath,"%s/%s",_path,filepatt);
    convert_path_pattern(localpath);
    lgprintf("begin to listlocal , pattern [%s].....",localpath);
    listfile(localpath,LISTFILE);

    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("文件列表失败.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    ftpmode=false;
}

const char *file_man::getlocalfile(const char *fn)
{
    if(fn!=NULL) {
        memset(localfile,0,300);
        sprintf(localfile,"%s/%s",skippickup?path:localpath,fn);
    }
    return localfile;
}

typedef struct backup_file_info{ // add by liujs
    int  backup_type;      //备份类型:[1-localcopyfile][2-gzipfile]
    char src_file[256];    //源文件
    char dst_file[256];    //备份文件
}backup_file_info,*backup_file_info_ptr;

void* OnBackupFileProc(void *params) // 备份文件的线程
{
    backup_file_info_ptr _pobj = NULL;
	_pobj = (backup_file_info_ptr)params;
    
    backup_file_info thread_param;
    thread_param.backup_type = _pobj->backup_type;
    strcpy(thread_param.src_file,_pobj->src_file);
    strcpy(thread_param.dst_file,_pobj->dst_file);
    
    pthread_detach(pthread_self()); 

    const char* psrc_file = thread_param.src_file;
    const char* pdst_file = thread_param.dst_file;
    if(thread_param.backup_type  == 1){
        localcopyfile(psrc_file,pdst_file);
    }else if(thread_param.backup_type  == 2) {
        gzipfile(psrc_file,pdst_file);
    }    
    else {
        printf("OnBackupFileProc thread_param.backup_type [%d] error",thread_param.backup_type);
    }
    return NULL;
}

//获取文件并做备份，如果backupfile==null,或者backupfile[0]=0,则不备份
// 如果文件不是.gz,则备份时做gzip压缩
// filename 外部使用:filename=fm.getlocalfile(localfile);
bool file_man::getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid)
{
    lgprintf("file_man::getfile begin to getfile %s , ftpmode = %d,hdfs_mode = %d \n",fn,ftpmode?1:0,hdfs_mode?1:0);

    char ls_current_file_Info[300] = {0};
    char ls_current_file_name[300] = {0};
    long ls_current_file_size = 0;
    char tmpfile[300] = {0};
    //tmf.Start();
    char *end=fn+strlen(fn)-1;
    while(*end<' ') *end--=0;

    if(ftpmode && !is_backup_dir) { // ftp 采集，并且不是backup目录的时候
        // 记录当前的文件名称(完整的文件名称)
        sprintf(current_file,"%s/%s",path,fn);
        ls_current_file_size=ftpclient.FileSize(current_file);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        remove(tmpfile);

        lgprintf("FTP Get %s->%s ...",current_file,tmpfile);
        ftpclient.GetFile(current_file,tmpfile);

        if(strcmp(fn+strlen(fn)-4,".ack")==0) {
            //删除本地的确认文件(空文件)
            remove(getlocalfile(fn));
            current_file[strlen(current_file)-4]=0;
            tmpfile[strlen(tmpfile)-4]=0;
            ftpclient.GetFile(current_file,tmpfile);
        }
    } else if(hdfs_mode && !is_backup_dir) { // hadoop hdfs 并且不是backup目录的时候
        sprintf(current_file,"%s/%s",hdfs_path,fn);

        ls_current_file_size=hdfs_getfilesize(hdfsclient,hdfs_path,fn);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        remove(tmpfile);

        lgprintf("hdfs Get %s->%s ...",current_file,tmpfile);
        hdfs_copy_file_to_local(hdfsclient,hdfs_path,fn,tmpfile);
    } else if(!is_backup_dir) { // 本地文件 并且不是backup目录的时候
        // 记录当前的文件名称(完整的文件名称)
        memset(current_file,0,300);
        sprintf(current_file,"%s/%s",path,fn);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        ls_current_file_size=localfilesize(current_file);

        // local file
        if(!skippickup) {
            localcopyfile(current_file,tmpfile);
            if(strcmp(fn+strlen(fn)-4,".ack")==0) {
                //如果文件存在，删除本地的确认文件(空文件)
                if( -1 == remove(getlocalfile(fn))) {
                    // lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",getlocalfile(fn));
                }
                current_file[strlen(current_file)-4]=0;
                tmpfile[strlen(tmpfile)-4]=0;
                localcopyfile(current_file,tmpfile);
            }
        } // skippickup endif
    }


    struct stat st;
    int retry_times = 0;
    char bk_dir_file[300];
retry_begin:  // fix dma-1040
    if(!is_backup_dir) {
        if(stat(getlocalfile(fn),&st)) { // 成功返回0
            ThrowWith("文件采集失败:%s.",fn);
        }
    } else {
        sprintf(bk_dir_file,"%s/%s",backup_dir,fn);
        if(stat(bk_dir_file,&st)) { // 成功返回0
            ThrowWith("从备份目录文件采集失败:%s.",bk_dir_file);
        }
    }

    // 对比文件大小
    //long _f_size = localfilesize(getlocalfile(fn));
    if(!is_backup_dir && st.st_size != ls_current_file_size) {
        if(retry_times < 10) {
            lgprintf("源文件大小(%ld)与采集到的文件大小(%ld)不一样，重试次数(%d).",ls_current_file_size,st.st_size,retry_times);
            sleep(1);
            retry_times++;
            goto retry_begin;
        }
        ThrowWith("源文件大小(%ld)与采集到的文件大小(%ld)不一样，程序退出.",ls_current_file_size,st.st_size);
    }
    current_filesize = st.st_size ;  // 记录文件大小，插件中存入dp.dp_filelog中用到

    //---------------------------------------------------------------------------------
    // 1>  来自非备份目录的文件,且需要进行备份操作的
    if( !is_backup_dir && (backupfile && backupfile[0]!=0) && !skipbackup) {
        if(strcmp(backupfile+strlen(backupfile)-4,".ack")==0)
            backupfile[strlen(backupfile)-4]=0;

        // 如果是压缩文件，直接将压缩文件拷贝到备份目录
        if(strcmp(fn+strlen(fn)-3,".gz") == 0) {
            strcat(backupfile,".gz");
            
#ifdef BACKUP_BLOCK           
            localcopyfile(getlocalfile(fn),backupfile);
#else
            // TODO: 会不会出现getlocalfile(fn) 还没有压缩完成就被删除的情况
            pthread_t backup_pthread_id ;
            backup_file_info thread_param;
            thread_param.backup_type = 1;
            strcpy(thread_param.src_file,getlocalfile(fn));
            strcpy(thread_param.dst_file,backupfile);
            
            if(pthread_create(&backup_pthread_id,NULL,OnBackupFileProc,(void*)&thread_param)!=0)
            {
                lgprintf("can't create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
                return -1;      
            }
            sleep(1);
            lgprintf("create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
#endif
        } else { // 如果不是压缩文件,将其进行压缩放入备份目录
            strcat(backupfile,".gz");
            remove(backupfile);
#ifdef BACKUP_BLOCK          
            gzipfile(getlocalfile(fn),backupfile);
#else
            pthread_t backup_pthread_id ;
            backup_file_info thread_param;
            thread_param.backup_type = 2;
            strcpy(thread_param.src_file,getlocalfile(fn));
            strcpy(thread_param.dst_file,backupfile);
            
            if(pthread_create(&backup_pthread_id,NULL,OnBackupFileProc,(void*)&thread_param)!=0)
            {
                lgprintf("can't create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
                return -1;      
            }
            sleep(1);
            lgprintf("create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
#endif
        }
    } else { // if(!is_backup_dir && skipbackup){
        // 如果是本地目录采集,并且不需要备份的,不需要进行任何处理操作
        // 外部使用文件方法:filename=fm.getlocalfile(localfile);
    }

    // 压缩文件
    char _file_name[300];
    char _temp1[300],_temp2[300];

    if(is_backup_dir) { // 2>  从备份目录来压缩文件，进行解压缩 到当前目录
        int remove = 0;
        strcpy(_file_name,bk_dir_file);
        if(strcmp(_file_name+strlen(_file_name)-3,".gz")==0) { // 解压文件, x.gz ------> x
            _file_name[strlen(_file_name)-3] = 0;
            remove =3;
        }

        fn[strlen(fn)-remove]=0;
        strcpy(_temp1,bk_dir_file);
        strcpy(_temp2,getlocalfile(fn));
        gunzipfile(_temp1,_temp2);

        if(stat(_temp2,&st)) {
            lgprintf("文件解压缩失败:%s,忽略错误...",_temp1);
            sp.log(tabid,datapartid,106,"文件解压缩失败:%s,忽略错误...",_temp1);
            return false;
        }
    } else { // 3>   不是备份目录来的文件，且不需要备份（需要备份的，在if(backupfile && backupfile[0]!=0 && !skipbackup) 中已经备份）
        strcpy(_file_name,fn);

        if(strcmp(fn+strlen(fn)-3,".gz")==0) {  // 解压文件,x.gz -----> x
            _file_name[strlen(_file_name)-3] = 0;

            strcpy(_temp2,getlocalfile(_file_name));
            remove(_temp2);

            strcpy(_temp1,getlocalfile(fn));

            // x.gz -----------> x 文件
            gunzipfile(_temp1,_temp2);

            fn[strlen(fn)-3]=0;
            if(stat(_temp2,&st)) {
                lgprintf("文件解压缩失败:%s,忽略错误...",_temp1);
                sp.log(tabid,datapartid,106,"文件解压缩失败:%s,忽略错误...",_temp1);
                return false;
            }

            // 删除ftp采集到本地的.gz压缩文件,
            // 删除本地路!skippickup情况下采集到本地的文件,
            // 因为已经解压过了,不需要进行保留了
            if(ftpmode || (!ftpmode && !hdfs_mode && !skippickup )) {
                lgprintf("删除压缩文件[%s]...",_temp1);
                remove(_temp1);
            }
        } else {
            // 不是压缩文件，不需要进行压缩操作，不需要进行重命名等操作
            // todo nothing
            // 外部使用文件方法:filename=fm.getlocalfile(localfile);
        }
    }

    return true;
}

bool file_man::getnextfile(char *fn,char *full_fn)
{
    struct stat stat_info;
    if(0 != stat(LISTFILE, &stat_info)) { // fix bug
        return false;
    }

    if(!flist) flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("文件列表失败.");
    while(fgets(lines,300,flist)!=NULL) {
        int sl=strlen(lines);
        if(lines[sl-1]=='\n') lines[sl-1]=0;
        if(full_fn != NULL) { // 完整文件名称的获取
            strcpy(full_fn,lines);
        }
        char *pfn=lines+strlen(lines)-1;
        //找到文件名的开始位置（排除路径部分)
        while(pfn>lines) if(*pfn--=='/')  {
                pfn+=2;
                break;
            }
        strcpy(fn,pfn);
        return true;
    }
    fclose(flist);
    flist=NULL;

    //>> begin: fix DM-264
    remove(LISTFILE);
    //<< end:fix dm-264

    return false; // reach eof
}

//>> DM-216 , 20130131，删除源文件
int  file_man::DeleteSrcFile()
{
    if(strlen(current_file)< 1) {
        lgprintf("文件不存在，删除源文件失败.");
        return -1;
    }

    convert_path_pattern(current_file);
    if(ftpmode) { // ftp 模式删除FTP服务端文件
        if(DELETE_YES == delFileFlag) {
            try {
                lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
                ftpclient.Delete(current_file);
                // 删除确认.ack文件
                if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                    current_file[strlen(current_file)-4] = 0;
                    lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
                    ftpclient.Delete(current_file);
                }
            } catch(...) {
                // 忽略ftp删除文件提示:"remote delete failed."错误,而导致的进程退出采集
            }
        }
    } else if(hdfs_mode) { // hdfs
        if(DELETE_YES == delFileFlag) {
            lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
            hdfsDelete(hdfsclient,current_file,0);
            // 删除确认文件
            if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4] = 0;
                hdfsDelete(hdfsclient,current_file,0);
            }
        }
    } else { // 删除本地文件,backup目录的不删除
        if( !is_backup_dir &&  DELETE_YES == delFileFlag) {
            lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
            remove(current_file);
            if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4]=0;
                remove(current_file);
            }
        } // end if(DELETE_YES == delFileFlag)

    } // end else

    return 0;
}

// 将路径名称中的多个路径分别取出来
// str_path_list[in]: 路径名称列表，例如:"/app/dir1;/app/dir2;/app/dir3"
// str_path_vector[in/out]: 目录名称队列
#ifndef SEPERATE_FLAG
#define SEPERATE_FLAG   ';'
#endif
void FileParser::splite_path_info(const char* str_path_list,std::vector<std::string>& str_path_vector)
{
    int _tmp_list_len = strlen(str_path_list) + 100;
    char *_tmp_lst = new char[_tmp_list_len]; // fix dma-1288
    strcpy(_tmp_lst,str_path_list);
    const char* p = _tmp_lst;
    char *q = _tmp_lst + (strlen(_tmp_lst)-1);

    while((*p) == SEPERATE_FLAG) {
        p++;
    };
    while((*q) == SEPERATE_FLAG) {
        *q = 0;
        q--;
    };

    char _path[300];
    int _path_len = 0;
    while(*p) {
        if(*p == SEPERATE_FLAG) { // 找到多个路径中的其中一个(不是最后一个)
            strncpy(_path,(p-_path_len),_path_len);
            _path[_path_len] = 0;
            std::string _tmp_item(_path);
            str_path_vector.push_back(_tmp_item);
            _path_len = 0;
        } else {
            _path_len++;
        }
        p++;
    }
    // 最后一个路径,或者只有一个路径的情况
    {
        strncpy(_path,(p-_path_len),_path_len);
        _path[_path_len] = 0;
        std::string _tmp_item(_path);
        str_path_vector.push_back(_tmp_item);
    }

    delete [] _tmp_lst;
    _tmp_lst = NULL;
}


// 将路径名称中的目录名称和文件名称分别取出来
// path[in]: 路径名称，例如:"/20131110/*.txt.gz"
// dir[in/out]: 目录名称，例如:"/20131110/"
// name[in/out]: 文件名称，例如:"*.txt.gz"
void FileParser::splite_path_name(const char* path,char* dir,char* name)
{
    int slen=strlen(path);
    char _dir[300];
    strcpy(_dir,path);
    char *pend = _dir+strlen(_dir)-1;
    int pos = 0;
    while (*pend && *pend != '/') {
        pos++;
        pend--;
    }
    *(pend+1) = 0;
    if(*pend == '/') {// find dir
        strcpy(name,path+(slen-pos));
        strcpy(dir,_dir);
    } else {
        dir[0]=0;
        strcpy(name,path);
    }

    lgprintf("FileParser::splite_path_name: '%s' to '%s','%s'.",path,dir,name);
}


//限制条件：
//  第一次调用frombackup,必须是上一个ftp/local已经处理完
int FileParser::commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly)
{
    char localfile[300];
    AutoMt mt(dbc,10);

    // lgprintf("commonGetFile : bgein to list file \n");

    if(frombackup && !fm.listhasopen() ) {
        char backfilepatt[300];
        sprintf(backfilepatt,"%s_%s",fix_cur_pach_md5,filepatt);

        if(strcmp(backfilepatt+strlen(backfilepatt)-4,".ack")==0)
            backfilepatt[strlen(backfilepatt)-4]=0;

        // BEGIN: fix DMA-447,20130109
        if(strcmp(backfilepatt+strlen(backfilepatt)-3,".gz")!=0)
            strcat(backfilepatt,".gz");
        // END : fix DMA-447

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录
        //<< End:fix Dm-263

        fm.listlocal(backuppath,backfilepatt);
    } else if(!fm.listhasopen() && strlen(pWrapper->getOption("hadoop:namenode",""))>1) {
        //hadoop list file
        fm.listHdfs(pWrapper->getOption("hadoop:namenode","default"),
                    pWrapper->getOption("hadoop:port",0),
                    pWrapper->getOption("hadoop:path","/tmp"),
                    filepatt);

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录
        //<< End:fix Dm-263
    } else if(!fm.listhasopen() && strlen(pWrapper->getOption("ftp:host",""))<1) {
        //本地文件模式
        fm.listlocal(fix_localpath,filepatt);
        fm.SetSkipPickup(pWrapper->getOption("files:skippickup",0)==1);
        fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录
        //<< End:fix Dm-263
    } else if(!fm.listhasopen()) {
        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录
        //<< End:fix Dm-263

        //FTP模式
        fm.SetAscMode(pWrapper->getOption("ftp:textmode",1)==1);

        //>> Begin: FTP get 文件失败后,进行重试5次，每次等待2分钟 ,modify by liujs
        int ftpListTimes = 0;
        while(1) {
            try {
                ftpListTimes++; // FTP List times

                fm.list(pWrapper->getOption("ftp:host",""),
                        pWrapper->getOption("ftp:username",""),
                        pWrapper->getOption("ftp:password",""),
                        fix_ftp_path,filepatt);

                // list 文件正常，未抛出异常
                break;
            } catch(char* e) {
                if(ftpListTimes <= FTP_GET_LIST_RETRY_TIMES) {
                    lgprintf("FTP LIST 文件失败，等待1秒钟后继续重试,FTP LIST执行次数(%d),当前次数(%d)",FTP_GET_LIST_RETRY_TIMES,ftpListTimes);
                    sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                    fm.FTPDisconnect();
                    continue;
                } else {
                    throw e;
                }
            } catch(...) {
                ThrowWith("FTP LIST 文件列表失败，未知错误");
            }
        }// end while
        //<< End: modify by liujs
    }
    fm.SetLocalPath(pWrapper->getOption("files:localpath","./"));

    //lgprintf("commonGetFile : finish to list file \n");

    bool ec=wociIsEcho();
    wociSetEcho(pWrapper->getOption("files:verbose",0)==1);
    while(fm.getnextfile(localfile,fix_filename)) {
        char realfile[300];
        char realfile_bk[300];
        strcpy(realfile,localfile);
        if(strcmp(realfile+strlen(realfile)-4,".ack")==0)
            realfile[strlen(realfile)-4]=0;

        // 记录下文件名称,dma-941使用到
        if(strcmp(fix_filename+strlen(fix_filename)-4,".ack")==0)
            fix_filename[strlen(fix_filename)-4]=0;

        // 浙江移动DMA-868
        strcpy(realfile_bk, realfile);
        if(strcmp(realfile+strlen(realfile)-3,".gz")==0) {
            realfile[strlen(realfile)-3] = 0;
        }


        //>> begin: fix dma-1315:获取多路径下文件重名的路径md5值
        if(!fm.GetIsBackupDir()) { // 如果不是backup目录来的文件,在文件名称前加MD5
            char _md5_fn[256];
            sprintf(_md5_fn,"%s_%s",fix_cur_pach_md5,realfile);
            strcpy(realfile,_md5_fn);
        }
        //<< end: fix dma-1315:获取多路径下文件重名的路径md5值


        // crc前，去除.gz 扩展符号
        fileseq=GetFileSeq(realfile);

        //Begin: crc产生序号，crc序号不校验
        if(pWrapper->getOption("files:crcsequence",0)==0) {
            if(fileseq<=0) ThrowWith("文件序号错误(%d):%s",fileseq,localfile);
        }
        //End:src产生序号

        // fix dm-210,add filename query
        mt.FetchAll("select filename from dp.dp_filelog where tabid=%d and datapartid=%d and fileseq=%d and filename = '%s'",
                    tabid,datapartid,fileseq,realfile);

        if(mt.Wait()>0) {//已经有处理记录
            // lgprintf("文件序号(%d)错误，两个文件'%s' ---'%s' 序列号重复!",fileseq,mt.PtrStr("filename",0),realfile_bk);
            char _cur_file_name[300];
            sprintf(_cur_file_name,"%s/%s",fm.GetPath(),realfile_bk);
            fm.SetCurrentFile(_cur_file_name);
            if(!fm.GetIsBackupDir()) {
                // lgprintf("文件序号(%d)错误，两个文件'%s' ---'%s' 序列号重复!",fileseq,mt.PtrStr("filename",0),realfile_bk);
                fm.DeleteSrcFile();
            }
            continue;
        }

        char backfilename[300];
        backfilename[0]=0;
        if(!fm.GetIsBackupDir()) {
            sprintf(backfilename,"%s/%s",backuppath,realfile);
        }
        AutoStmt st(dbc);
        try {
            if(!checkonly) {
                char temp[300] ;
                if(backfilename[0] != 0) {
                    sprintf(temp,"%s.gz",backfilename);
                } else {
                    sprintf(temp,"%s/%s.gz",backuppath,realfile);
                }

                st.Prepare("insert into dp.dp_filelog values("
                           "%d,%d,'%s','%s',0,%d,sysdate(),null,0,0,0,'NULL',0)",
                           tabid,datapartid,realfile,temp,fileseq);
                st.Execute(1);
                st.Wait();
            }
        } catch(...) {
            //另一个进程在处理这个文件？只有正确写入dp_filelog的进程可以处理文件
            continue;
        }

        bool filesuc=true;
        if(!checkonly) {
            //>> Begin: FTP get 文件失败后,进行重试5次，每次等待2分钟 ,modify by liujs
            int ftpGetTimes = 0;
            while(1) {
                try {
                    ftpGetTimes++;
                    // warning : DM-263 , 不能在此处设置
                    // fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录

                    bool _rm_file = !frombackup;// 是否删除文件参数
                    fm.SetBackupDir(backuppath);
                    fm.getfile(localfile,backfilename,_rm_file,sp,tabid,datapartid);
                    // get 文件正常，为抛出异常
                    break;
                } catch(char* e) {
                    if(ftpGetTimes <= FTP_GET_LIST_RETRY_TIMES) {
                        lgprintf("%s文件失败，等待1秒钟后继续重试,%s执行次数(%d),当前次数(%d)",fm._get_file_title().c_str(),
                                 fm._get_file_title().c_str(),FTP_GET_LIST_RETRY_TIMES,ftpGetTimes);
                        sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                        fm.FTPDisconnect();
                        continue;
                    } else {
                        // throw e;
                        break; // fix dma-1196
                    }
                } catch(...) {
                    ThrowWith("%s 文件失败，未知错误", fm._get_file_title().c_str());
                }
            }// END while
            //<< End: modify by liujs

            // fix dma-1196
            if(ftpGetTimes >=FTP_GET_LIST_RETRY_TIMES) {
                lgprintf("==>ERROR<==获取文件[%s]失败,跳过该文件进行获取下一个文件.",localfile);

                // 删除无效记录,fix dma-1197
                st.DirectExecute("delete from  dp.dp_filelog  where tabid = %d and datapartid =%d and status = 0 and fileseq=%d",
                                 tabid,datapartid,fileseq);

                continue;
            }

            basefilename=localfile;  // 不含有路径的文件名称,输出错误文件内容使用
            filename=fm.getlocalfile(localfile);
            if(!filesuc) {
                //忽略错误，直接修改为处理结束的提交状态
                st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=0"
                                 ,tabid,datapartid);
            }
        }

        // 更新文件大小
        st.DirectExecute("update dp.dp_filelog set filesize=%ld where tabid=%d and datapartid=%d and fileseq=%d",
                         fm.GetCurFileSize(),tabid,datapartid,fileseq);

        wociSetEcho(ec);
        return filesuc?1:2;
    }
    wociSetEcho(ec);
    return 0;
}


FileParser::FileParser()
{
    fp=NULL;
    fix_ftp_path[0]=0;
    fix_localpath[0]=0;
    fix_cur_pach_md5[0]=0;
    fix_filepatt[0]=0;
    fix_filename[0]=0;
    current_path_index = -1;
    str_path_vector.clear();
    line = NULL;
    line=new char[MAXLINELEN];
}
//这个函数必须在对象构造后，在其它成员函数之前调用
void FileParser::SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw)
{
    dbc=_dbc;
    tabid=_tabid;
    fp=NULL;
    pWrapper=pdfw;
    datapartid=_datapartid;
    fileseq=0;
};
// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// 返回值：
//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
//  0：暂无文件
//  1：取得有效的文件
//int  FileParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {return 0;};
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// 必须是不再有文件需要处理的时候，才能调用这个检查函数
//bool FileParser::ExtractEnd() {return true;};
int FileParser::GetIdleSeconds()
{
    return pWrapper->getOption("files:seekinterval", 5);
}
// return value:
//  -1: a error occurs.例如内存表字段结构不匹配
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and
// sequence of file in dp dictions tab.
//int FileParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {return -1;};
//int FileParser::GetFileSeq(const char *filename) {return 1;}

FileParser::~FileParser()
{
    if(fp) fclose(fp);
    if(line != NULL) {
        delete []line;
        line = NULL;
    }
    fix_ftp_path[0]=0;
    fix_cur_pach_md5[0]=0;
    fix_localpath[0]=0;
    fix_filepatt[0]=0;
    fix_filename[0]=0;
    current_path_index = -1;
    str_path_vector.clear();
}

#ifndef PLUGIN_VERSION
#define PLUGIN_VERSION "plugin 1.6.0"
#endif
void FileParser::PluginPrintVersion()
{
    lgprintf("PLUGIN-[%s]",PLUGIN_VERSION);
}


