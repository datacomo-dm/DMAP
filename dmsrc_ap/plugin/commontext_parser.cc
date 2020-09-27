#include "dt_common.h"
#include "AutoHandle.h"
#include "commontxt_parser.h"
#include <string.h> 

#ifndef min
 #define min(a,b) ((a)>(b)?(b):(a))
#endif
#ifndef max
 #define max(a,b) ((a)>(b)?(a):(b))
#endif


extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);

IFileParser *BuildParser(void) {
    return new CommonTxtParser();
}
#ifndef TMP_LEN
#define TMP_LEN 100
#endif 

#define ExtractDate(optionstr,value,len,func)   {\
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

int CommonTxtParser::ExtractNum(int offset,const char *info,int len) {
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
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,int val,int len)
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
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,char *value) {
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
int CommonTxtParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
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
    fm.SetDeleteFileFlag(pWrapper->getOption("files:delsrcdata",0));
    // End: 设置是否删除源文件，默认删除[1:删除，0:不删除]

begin_to_list_file_path: // fix : dma-1061

    //>>begin:fix dma-926 获取修正后的路径信息,用于处理FILENAME中含有路径的情况
    char temp_path[300];
    if(strlen(pWrapper->getOption("ftp:path","")) > 0){ // 处理ftp多路径
        if( (str_path_vector.empty()) || (current_path_index == -1) ){ // 加载路径到内存中
            splite_path_info(pWrapper->getOption("ftp:path",""),str_path_vector);
            current_path_index = 0;
            assert(str_path_vector.size()>0);
        }
            
        if(strlen(fix_ftp_path)==0 || strlen(fix_filepatt) == 0){
            splite_path_name(filepatt,temp_path,fix_filepatt);
            sprintf(fix_ftp_path,"%s/%s",str_path_vector[current_path_index].c_str(),temp_path);

            //>> begin: fix dma-1315:获取多路径下文件重名的路径md5值
            {
                unsigned char md[16] = {0}; // 16 bytes     ,不能定义成char,否则会出现sprintf出来的md5array中存在乱码
                MD5((unsigned char*)fix_ftp_path,strlen(fix_ftp_path),md);
                for(int i=0;i<16;i++){
                    sprintf(fix_cur_pach_md5+strlen(fix_cur_pach_md5),"%02x",md[i]);
                }
            }
            //<< end: fix dma-1315:获取多路径下文件重名的路径md5值
        }
    }
    if(strlen(pWrapper->getOption("local:path","")) > 0){ // 处理本地目录多路径   
        if((str_path_vector.empty()) || (current_path_index == -1)){ // 加载路径到内存中
            splite_path_info(pWrapper->getOption("local:path",""),str_path_vector);
            current_path_index = 0;
            assert(str_path_vector.size()>0);
        }
        if(strlen(fix_localpath)==0 || strlen(fix_filepatt) == 0){
            splite_path_name(filepatt,temp_path,fix_filepatt);
            sprintf(fix_localpath,"%s/%s",str_path_vector[current_path_index].c_str(),temp_path);

            //>> begin: fix dma-1315:获取多路径下文件重名的路径md5值
            {
                unsigned char md[16] = {0}; // 16 bytes     ,不能定义成char,否则会出现sprintf出来的md5array中存在乱码
                MD5((unsigned char*)fix_localpath,strlen(fix_localpath),md);
                for(int i=0;i<16;i++){
                    sprintf(fix_cur_pach_md5+strlen(fix_cur_pach_md5),"%02x",md[i]);
                }
            }
            //<< end: fix dma-1315:获取多路径下文件重名的路径md5值
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
    sprintf(backuppath,"%s/%s.%s",pWrapper->getOption("files:backuppath","./"),dbname,tabname);
    sprintf(backuppath+strlen(backuppath),"/part%d",datapartid);
    xmkdir(backuppath);

    int rt= 0;
    
    // 先到源数据目录中获取数据
    rt = commonGetFile(backuppath,false,fix_filepatt,sp,tabid,datapartid,checkonly);
    if((rt==0) && frombackup) { // 源数据路径中获取不对文件,到备份目录中获取
        rt=commonGetFile(backuppath,true,fix_filepatt,sp,tabid,datapartid,checkonly);
    }

    if(rt == 0) { // 没有获取到文件,从下一个目录获取文件,
        if(current_path_index != (str_path_vector.size()-1)){ // 没有所有目录都检索文件完成
            current_path_index += 1;
            fix_ftp_path[0]= 0;
            fix_localpath[0] = 0;
            fix_cur_pach_md5[0] = 0;
            fix_filepatt[0] = 0;
            goto begin_to_list_file_path;   // 处理下一个目录中的文件
        }
    }
    return rt;
}
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// 必须是不再有文件需要处理的时候，才能调用这个检查函数
bool CommonTxtParser::ExtractEnd(int _dbc) {
    wociSetEcho(false);
    AutoMt mt(_dbc,10);
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
    if(pWrapper->getOption("files:crcsequence",0)==0 && rn > 0)
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

    */
    if(pWrapper->getOption("files:forceenddump",0)!=1) {
        mt.FetchAll("select timestampdiff(MINUTE,now(),date_add(date_add('%04d-%02d-%02d',interval 1 day),interval '%d:%d' hour_minute))",
            yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60);
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

int CommonTxtParser::GetFileSeq(const char *filename) {
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
int CommonTxtParser::parsercolumn(int memtab,const char *cols,int *colsflag) {
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
int CommonTxtParser::parserfixlen(int memtab,const char *cols,int *fixflag,int *collen){
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
int CommonTxtParser::WriteErrorData(const char *line) {
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

// 将解析过程中发现的错误内容写入文件完成后进行压缩操作,fix dma-1159
bool CommonTxtParser::CompressErrData(){
    char errfile[300];
    strcpy(errfile,pWrapper->getOption("files:errordatapath",""));
    //未设置错误文件存放路径，则不报错错误数据
    if(errfile[0]==0) return false;
    if(errfile[strlen(errfile)-1]!='/') strcat(errfile,"/");
    strcat(errfile,basefilename.c_str());
    strcat(errfile,".err");
    gzipfile(errfile); // gzip file and delete src file
    lgprintf("gzip file %s .",errfile);
    return true;
}


// 黑龙江电信一个文件中存在多个表的数据，需要将是这个表的数据进行写入错误文件，但是不需要输出到:$DATAMERGER_HOME/log/dump/*.log中
#define HLG_SKIP_PRINTF_LOG "HLG_SKIP_PRINTF_LOG"

// 判断列数是否准确
// _file_col_num: 文件中确实存在的列
// _header_col_num: files::header中定义的列
// _st_ext: 入库时间和入库文件名称列
// _st_http: http 解析列信息
// _msg: 出错提示信息
bool CommonTxtParser::check_column_count(const int _file_col_num,const int _header_col_num,stru_external& _st_ext,stru_Http_translate& _st_http,std::string& msg){
    int ct = _file_col_num;
    int colct = _header_col_num;
    char _msg[1024];

    if(_st_ext.is_valid()&&_st_http.is_valid()){
        if(_st_ext.get_column_count() == 2 && _st_http.get_column_count() == 2 && (ct != colct-4)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,入库文件名列,HTTP关键字列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-4,ct);
        }else if(_st_ext.get_column_count() == 2 && _st_http.get_column_count() == 1 && (ct != colct-3)){
            if(_st_ext._load_date_col_index == -1){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库文件名列,HTTP关键字列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-3,ct);
            }else if(_st_ext._load_filename_col_index == -1){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,入库文件名列,HTTP关键字列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-3,ct);
            }else {
               return true;
            }            
        }else if(_st_ext.get_column_count() == 1 && _st_http.get_column_count() == 2 && (ct != colct-3)){
            if(_st_http._key_col== -1){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库文件名列,入库时间列,,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-3,ct);
            }else if(_st_http._download_col== -1){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含不含入库时间列,入库文件名列,HTTP关键字列)，实际有%d个.",filename.c_str(),curoffset,colct-3,ct);
            }else{
               return true;
            }            
        }else{
            if(_st_ext._load_date_col_index != -1 && _st_http._key_col!= -1 && (ct != colct-2)){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,HTTP关键字列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
            }else if(_st_ext._load_date_col_index != -1 && _st_http._download_col!= -1 && (ct != colct-2)){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
            }else if(_st_ext._load_date_col_index == -1 && _st_http._download_col!= -1 && (ct != colct-2)){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库文件名称列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
            }else if(_st_ext._load_date_col_index == -1 && _st_http._key_col!= -1 && (ct != colct-2)){
                sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库文件名称列,HTTP下载列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
            }else{            
                return true;           
            }
        }    
     }else if(_st_ext.is_valid()){
        if(_st_ext.get_column_count() == 2 && (ct != colct-2)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,入库文件名列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
        }else if(_st_ext.get_column_count() == 1 && _st_ext._load_date_col_index != -1 && (ct != colct-1)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列)，实际有%d个.",filename.c_str(),curoffset,colct-1,ct);
        }else if(_st_ext.get_column_count() == 1 && _st_ext._load_filename_col_index != -1 && (ct != colct-1)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库文件名称列)，实际有%d个.",filename.c_str(),curoffset,colct-1,ct);
        }else{
            return true;
        }        
    }else if(_st_http.is_valid()){
        if(_st_http.get_column_count() == 2 && (ct != colct-2) ){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含入库时间列,入库文件名列)，实际有%d个.",filename.c_str(),curoffset,colct-2,ct);
        }else if(_st_http.get_column_count()==1 && _st_http._key_col != -1 && (ct != colct-1)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含HTTP搜索关键字列)，实际有%d个.",filename.c_str(),curoffset,colct-1,ct);
        }else if(_st_http.get_column_count()==1 && _st_http._download_col != -1 && (ct != colct-1)){
            sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段(不含HTTP下载内容列)，实际有%d个.",filename.c_str(),curoffset,colct-1,ct);
        }else{
            return true;
        }
    }else if(ct != colct){
        sprintf(_msg,"文件'%s'第%d行格式错误,需要%d个字段，实际有%d个.",filename.c_str(),curoffset,colct,ct);
    }else{
        return true;
    }
    msg = _msg;
    return false;    
}




// 返回
// 0: 内存表满，当前文件未处理完
// 1: 内存表未满，当前文件已处理完
// 3: 内存表满，当前文件已处理完
int CommonTxtParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
    AutoMt mt(0,10);
    mt.SetHandle(memtab,true);
    //mt.Reset();
    char sepchar[10];
    if(fp==NULL){
        fp=fopen(filename.c_str(),"rt");
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

        //>> begin: 设置需要填充为null的字符串的列,add by liujs
        int _colct=mt.GetColumnNum();  //获取字段数     
        nulls_colct = 0;
        if(_colct != colct){ // files::header 不是* 或者完整列的情况
            for(int i=0;i<_colct;i++){
                if(mt.getColType(i) == COLUMN_TYPE_CHAR && mt.GetColLen(i) >8){ // 字符串，长度超过8
                    bool need_set_null = true;
                    for(int j=0;j<colct;j++){
                        if(colspos[j] == i){     // 不需要设置null
                            need_set_null = false;
                            break;
                        }
                    }

                    if(need_set_null) {//  需要设置为null的
                        nulls_colspos[nulls_colct] = i;
                        nulls_colct++;
                    }
                }
            }
        }       
        //<< end: 设置需要填充为null的字符串的列
        

        //>> begin DMA-604 : 设置需要调整列宽度的列,20130411
        strcpy(line,pWrapper->getOption("files:adjustcolumnswidth",""));
        if(strlen(line)<1){
            for(int _i = 0;_i<colct;_i++){  // 没有需要调整列宽度的列
                colsAdjustWidth[_i] = false;
            }
        }else{
            sl=strlen(line);  
            if(line[sl-1]=='\n') line[sl-1]=0;
            
            // 1. 所有字符串的列都需要调整宽度
            if(line[0]=='*'){
               lgprintf("所有的字符串的列,都需要调整列宽度,[files:adjustcolumnswidth=*]");
               for(int _i = 0 ;_i<colct;_i++){  // 只有字符串的列需要调整
                   if( COLUMN_TYPE_CHAR == mt.getColType(colspos[_i])){
                       colsAdjustWidth[_i] = true; 
                   }else{ 
                       colsAdjustWidth[_i] = false; 
                   }
               }
            }else{
            
            // 2.  部分列需要调整宽度
            size_t  separater_pos = 0;
            std::string columnsNameInfo = std::string(line);
            // 添加一个","到列尾部，方便处理最后一列
            columnsNameInfo += ", ";
            const char separater_items=',';
            std::string columnItem;
            separater_pos = columnsNameInfo.find_first_of(separater_items);
            int colindex = 0;
            while (separater_pos != std::string::npos){ 
                columnItem = columnsNameInfo.substr(0,separater_pos);
                columnsNameInfo = columnsNameInfo.substr(separater_pos+1,columnsNameInfo.size()-separater_pos-1);
                separater_pos = columnsNameInfo.find_first_of(separater_items);
                
                // 判断输入的列是否存在，如果不存在就提示错误
                bool _exist = false;
                for(int _i = 0;_i<colct;_i++){
                    char _cn[256];
                    wociGetColumnName(memtab,colspos[_i],_cn);
                    if(strcasecmp(_cn,columnItem.c_str()) == 0){
                        _exist = true;
            
                        // 判断列类型是否正确，如果列类型不正确就退出
                        if(COLUMN_TYPE_CHAR != mt.getColType(colspos[_i])){
                            ThrowWith("该列[%s]字段类型不是字符串的,不支持自动调整列宽度.",columnItem.c_str());
                        }
                        colsAdjustWidth[_i] = true;
                        break;
                    }else{
                        colsAdjustWidth[_i] = false;
                    }
                }
            
                // 没有找到该列
                if(!_exist){
                    ThrowWith("该列[%s]字段不是有效字段，不支持自动调整列宽度.",columnItem.c_str());
                }           
            }
			}
        }       
        //<< end: DMA-604 设置需要调整列宽度的列

        //>> begin: dma-1086 解析关键字和下载列
        {
            char _http_col[256];
            char _col_key[256];
            char _col_download[256];
            strcpy(_http_col,pWrapper->getOption("http_translate:http_col",""));
            if(strlen(_http_col)>1){
                strcpy(_col_key,pWrapper->getOption("http_translate:key_col",""));
                strcpy(_col_download,pWrapper->getOption("http_translate:download_col",""));
                if(strcasecmp(_col_key,_col_download) == 0){
                    ThrowWith("http_translate:key_col 和 http_translate:download_col 不能指定为同一列.");
                }

                // 查找有效的列
                int _col_key_len = strlen(_col_key);
                int _col_download_len = strlen(_col_download);

                _st_http_translate_obj.init();
                char _cn[256];                
                
                if( _col_key_len > 1 || _col_download_len > 1){
                    for(int _i = 0;_i<colct;_i++){
                        if(mt.getColType(colspos[_i]) != COLUMN_TYPE_CHAR) continue;

                        wociGetColumnName(memtab,colspos[_i],_cn);

                        if(strcasecmp(_cn,_http_col) == 0){
                            _st_http_translate_obj._http_col = colspos[_i];
                        }else if(_col_key_len>1 && strcasecmp(_cn,_col_key) == 0){
                            _st_http_translate_obj._key_col = colspos[_i];
                        }else if(_col_download_len>1 && strcasecmp(_cn,_col_download) == 0){
                            _st_http_translate_obj._download_col= colspos[_i];
                        }
                    }

                    if(_st_http_translate_obj._http_col == -1){
                        ThrowWith("Http列不存在,请确认http_translate:http_col是否填写正确.");
                    }

                    if(_st_http_translate_obj._key_col == -1){
                        ThrowWith("关键字列不存在,请确认http_translate:key_col是否填写正确.");
                    }

                    if(_st_http_translate_obj._download_col == -1){
                        ThrowWith("下载列不存在,请确认http_translate:download_col是否填写正确.");
                    }

                    // http 列必须和下载关键字列和下载项列连在一起,并且http列必须在最前面
                    if(_col_key_len > 1){
                        if(_st_http_translate_obj._http_col >=_st_http_translate_obj._key_col || 
                            (_st_http_translate_obj._key_col-_st_http_translate_obj._http_col) > _st_http_translate_obj.get_column_count()){
                            ThrowWith("http 列必须和下载关键字列和下载项列连在一起,并且http列必须在最前面");
                        }
                    }

                    if(_col_download_len>1){
                        if(_st_http_translate_obj._http_col >=_st_http_translate_obj._download_col || 
                            (_st_http_translate_obj._download_col - _st_http_translate_obj._http_col) > _st_http_translate_obj.get_column_count()){
                            ThrowWith("http 列必须和下载关键字列和下载项列连在一起,并且http列必须在最前面");
                        }
                    }

                    // 设置下载项的扩展文件名称
                    char _extern_name[2048];
                    strcpy(_extern_name,pWrapper->getOption("http_translate:download_pattern",""));
                    if(strlen(_extern_name)<1){
                        ThrowWith("http 下载项文件扩展名http_translate:download_pattern不能为空.");
                    }
                    _column_parser_obj.set_download_extern_name(_extern_name);
                }
            }
        }
        //<< end: dma-1086 解析关键字和下载列

        //>> begin: fix dma-794(入库时间),dma-941(入库文件名称)
        {
            char _loaddatecol[256];
            char _loadfilecol[256];
            strcpy(_loaddatecol,pWrapper->getOption("external:loaddatecol",""));
            strcpy(_loadfilecol,pWrapper->getOption("external:loadfilecol",""));
            int _loaddatecol_len = strlen(_loaddatecol);
            int _loadfilecol_len = strlen(_loadfilecol);

            _st_external_obj.init();
            if(_loaddatecol_len>1 || _loadfilecol_len>1){
                char _cn[256];
                for(int i=0;i<colct;i++){
                    wociGetColumnName(mt,colspos[i],_cn);
                    if(mt.getColType(colspos[i]) == COLUMN_TYPE_DATE){
                        if(strcasecmp(_cn,_loaddatecol) == 0){
                            _st_external_obj._load_date_col_index = colspos[i]; // 找到需要设置装入时间列的索引
                        }
                    }
                    
                    if(mt.getColType(colspos[i]) == COLUMN_TYPE_CHAR){
                        if(strcasecmp(_cn,_loadfilecol) == 0){
                            _st_external_obj._load_filename_col_index = colspos[i];// 找到需要设置装入时间列的索引
                        }
                    }
                }
            }            
        }
        //<< end: fix dma-794(入库时间),dma-941(入库文件名称)
        
        filerows=0;
        errinfile=0;
        ignore_rows = 0;
        
        lgprintf("文件'%s'开始处理.",filename.c_str());
    }

    /////////////////////////////////////chxl//////////////////
    int *pnszVarCol = new int[MAX_FILE_COLUMNS];
    int *pnszTypeLen = new int[MAX_FILE_COLUMNS];
    memset(pnszVarCol,0,sizeof(int)*MAX_FILE_COLUMNS);
    memset(pnszTypeLen,0,sizeof(int)*MAX_FILE_COLUMNS);

    int nWrNum = 1000000000;
    
    _wdbiGetVarCol(mt.handle, pnszVarCol, pnszTypeLen);
    char **pWrBf = new char* [MAX_FILE_COLUMNS];
    // get lowest rownum in var colums to fit in 10M buffer
    for(int t = 0;t<colct;t++){
        if(pnszVarCol[colspos[t]]){
            int nRow = (TEN_M_SIZE/pnszTypeLen[colspos[t]])-1;
            nWrNum = nWrNum > nRow?nRow:nWrNum;
        }
    }
    // allocate 10M buffer for each var column
    for(int t=0;t<colct;t++){
        if(pnszVarCol[colspos[t]]){
            pWrBf[colspos[t]] = new char[nWrNum * pnszTypeLen[colspos[t]]];
            memset(pWrBf[colspos[t]],0,nWrNum*pnszTypeLen[colspos[t]]);
        }else{
            pWrBf[colspos[t]] = NULL;
        }   
    }
    //////////////////////////////////chxl//////////////////////
    int rows=mt.GetRows();
    int maxrows=mt.GetMaxRows();
    char colname[300];
    char ipaddr[20];

    char separator[8] = {0};
    char separator_tmp[8] = {0};
    int separator_len = 0;
    char enclosedby = pWrapper->getOption("files:enclosedby",'\x0');
    strcpy(separator_tmp, pWrapper->getOption("files:separator",""));

    //>> begin: fix dma-1191:支持二进制分隔符
    if(strncasecmp(separator_tmp,"0x",2)== 0){
        separator[0] = atoi(separator_tmp+2);
        separator[1] = 0;
    }else{
        strcpy(separator,separator_tmp);
    }
    //<< end : fix dma-1191
    
    separator_len = strlen(separator);
    bool ischaracter = separator_len == 1;
   

    char escape=pWrapper->getOption("files:escape",'\x0');
    bool fixedmode=pWrapper->getOption("files:fixedmode",0)==1;
    int isfixed[MAX_COLUMN],fixlen[MAX_COLUMN];
    int fixcols=parserfixlen(memtab,pWrapper->getOption("files:fixlengthcols",""),isfixed,fixlen);
    int asciicvt[MAX_COLUMN];
    bool ipcheck=pWrapper->getOption("files:ipconvert",0)==1;
    int asccvt=parsercolumn(memtab,pWrapper->getOption("files:asciivalues",""),asciicvt);
    if(fixedmode) ThrowWith("定长文件格式暂不支持!");
    int valuelen[MAX_COLUMN];
    int recordfmtcheck=pWrapper->getOption("files:recordfmtcheck",1);
    int postoperculum = pWrapper->getOption("files:postoperculum",0)==1; //0表示末尾不带分隔符
    bool ignoreLastColAnalyse = pWrapper->getOption("files:ignorelastcolanalyse",0) == 1;// 忽略最后一列转义字符和分隔符的分析，JIRA DMA-474  
    bool addsecond = pWrapper->getOption("files:addsecond",0)==1;//[北京电信需求]对时间字段中的秒进行补0处理,机器生成的时间没有秒，例如：{"2014-05-12 12:30","2014-05-12 13:04"}
    int appendlstcol = pWrapper->getOption("files:appendlstcol",0); // 部分数据不全的时候,需要填充最后n列的数据
    
    // 黑龙江电信一个文件中存在多个表的数据，需要将是这个表的数据进行写入错误文件，但是不需要输出到:$DATAMERGER_HOME/log/dump/*.log中
    bool skip_printf_log = false;
    char *_p_env = getenv(HLG_SKIP_PRINTF_LOG);
    if(_p_env != NULL && 1 == atoi(_p_env)) {
        skip_printf_log = true;
    }else{
        char *_p_env2 = getenv("SKIP_PRINTF_LOG");
        if(_p_env2 != NULL && 1 == atoi(_p_env2)) {
            skip_printf_log = true;
        }        
    }

    
    //>> begin: fix dma-794
    char now[10];
    wociGetCurDateTime(now);
    //<< end: fix dma-794

    ////////////////////
    long nVarRow = 0;
    int nCol = 0;
    long nVarLevelRow = rows;;

    /////////////////
    while(rows<maxrows && fgets(line,MAXLINELEN,fp)){
        curoffset++; //行号
        int sl=strlen(line); //记录长度

        strcpy(linebak,line);

        //truncate \r\n seq:
        if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;
        if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;

        // >> begin : fix dma-1316
        for(int i=0;i<appendlstcol;i++){
            strcat(line,separator);
        }
        sl = strlen(line);
        // >> end : fix dma-1316
        
        if(postoperculum && 0 == strncmp(line + (sl - separator_len), separator, separator_len)){
            line[sl - separator_len]=0;
            sl -= separator_len;
        }

        if(line[0]==0) continue; //允许空行
        char *ptrs[MAX_FILE_COLUMNS];
        char *ps=line;   //头指针
        char *psle=line + sl -1;  //尾指针
        memset(valuelen,0,colct*sizeof(int));
        ptrs[0]=line;
        bool enclose=false;
        int ct=0;  //实际字段数
        //initial first column
        ptrs[ct]=ps;
        bool isthisfix=isfixed[colspos[ct]];
        bool skipline=false;
        //定长字段不作转义符检测
        while(*ps){
            if(ct==colct) { 
                // fix dma-1446:自动跳过文本文件后面的列,delete by liujs 
                // ct++;break;
                ct--;
                break;       // 直接跳出即可            
            }
            isthisfix=isfixed[colspos[ct]];
            if(isthisfix) {
                //定长格式
                char *pse=ps+fixlen[colspos[ct]]-1;
                if(pse>psle) {
                    if(recordfmtcheck!=0) {
                        ThrowWith("文件'%s'第%d行格式错误,定长字段(%d)超过这行的长度%d.",filename.c_str(),
                            curoffset,ct+1,pse-psle);
                    }

                    if(!skip_printf_log){
                        lgprintf("文件'%s'第%d行格式错误,定长字段(%d)超过这行的长度%d.",filename.c_str(),
                        curoffset,ct+1,pse-psle);
                    }
                    skipline=true;
                    errinfile++;
                    WriteErrorData(linebak);
                    break;
                }

                valuelen[colspos[ct]]=pse-ps+1;
                //定长字段不支持封装字符符号！！
                ps+=fixlen[colspos[ct]];

                //DMA-152
                if(!ischaracter){
                    /* string */
                    /*trim left separator*/
                    if(0 == strncmp(ps, separator, separator_len)){
                        ps += separator_len;
                    }
                }else{
                    /* charactor */
                    if(*ps==separator[0] && separator[0]!=0){ ps++;}
                }           
                ptrs[++ct]=ps;
            }else{ //特殊符号做转义？
                //变长格式
                if(*ps==enclosedby && enclosedby!=0) { // 当前字符是封装符
                    ps++;
                    if(/*enclose && */ps[-2]==escape &&escape!=0) {strcpy(ps-2,ps-1);continue;}
                    if(enclose) {ps[-1]=0;enclose=false;continue;}
                    ps[-1]=0;ptrs[ct]=ps; //delete enclosed char and 
                    enclose=true;
                }
                
                if(!ischaracter){// 多字符分隔符
                    // 分隔符是字符串的情况
                    /* trim left separator */
                    if(0 == strncmp(ps, separator, separator_len)){
                        ps += separator_len;
                        if(ps[-separator_len] == escape && escape!=0){
                            strcpy(ps - separator_len, ps); continue;
                        }
                        memset(ps-separator_len, 0x0, separator_len);
                        ptrs[++ct]=ps;
                    }else ps++;                      
                } // end if(!ischaracter)
                else{
                    // 单字符分隔符
                    if(*ps!=enclosedby){// fix dma-1160
                        if(*ps++==separator[0] && !enclose){
                            if(ps[-2]==escape && escape!=0){
                                strcpy(ps-2,ps-1);continue;
                            }
                            ps[-1]=0;
                            ptrs[++ct]=ps;
                        }   
                    }
                }   // end if(ischaracter)
            }// end else

            //>> Begin: fix jira dma-474
            if(ignoreLastColAnalyse && (ct == colct-1)){
                break;  
            }
            //<< End: fix jira dma-474
        }// end while(*ps)

        if(skipline) {continue;}

        if(!isfixed[colspos[colct-1]]) ct++;
        // POSITION;PRINTF("ct:%d; colct:%d\n", ct, colct);

        std::string msg;
        bool bcheck = check_column_count(ct,colct,_st_external_obj,_st_http_translate_obj,msg);
        if(!bcheck) {            
            if(recordfmtcheck!=0) {
                ThrowWith(msg.c_str());
            }

            if(!skip_printf_log){
                lgprintf(msg.c_str());
            }
            
            errinfile++;
            WriteErrorData(linebak);
            continue;
        }

        //>> begin: fix dma-941
        // 没有load_time和load_filename需要处理的时候，不需要调整，
        // 但是有load_time和load_filename需要处理的时候，就要调整该顺序，
        // 因为格式化表中的列数目和实际值的列数目不一样，实际值的列数目需要调整!
        int fix_col_ptr_index = 0; // 实际值的列数
        //<< end: fix dma-941
        
        _int64 _int_tmp = 0;
        for(int i=0;i<colct;i++) {
            nCol = colspos[i];

            //>> begin: dma-794
            if(colspos[i] == _st_external_obj._load_date_col_index){
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_DATE){
                    ThrowWith("Set load date column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }   
                mt.SetDate(colspos[i],rows,now);
                continue;           
            }
            //<< end:dma-794


            //>> begin: dma-941
            if(colspos[i] == _st_external_obj._load_filename_col_index){
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_CHAR){
                    ThrowWith("Set load filename column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }   
                // 这个地方不能试用SetStr
                // mt.SetStr(colspos[i],rows,fix_filename);
                if(pnszVarCol[nCol]){ 
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],fix_filename,strlen(fix_filename));
                }else{ // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),fix_filename);
                }
                
                continue;           
            }   
            //<< end: dma-941
          
            char *ptrf=ptrs[fix_col_ptr_index];  // 获取文件中时间内容列的值
            /* // 定长字段暂时被屏蔽掉了
            if(isfixed[colspos[i]]) {
                //定长字段允许没有分隔符，因此字段的值有可能未截断，需要复制数据
                memcpy(fieldval,ptrs[fix_col_ptr_index],valuelen[colspos[i]]);
                fieldval[valuelen[colspos[i]]]=0;
                ptrf=fieldval;
            }
            */
            char *trimp=ptrf;
            int olen=strlen(trimp)-1;
            while(trimp[olen]==' ') trimp[olen--]=0;
            while(*trimp==' ') trimp++;
            
            //>> begin: dma-1086 , http 列解析关键字和下载内容
            // http 列必须和下载关键字列和下载项列连在一起,并且http列必须在最前面
            if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._http_col){
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_CHAR){
                    ThrowWith("parser http column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }                    

                if(rows > 1){ // 校验http关键字列和下载列是否更新正确
                    if(_st_http_translate_obj._update_check != 0){
                        ThrowWith("parser http column error,解析http列内容,需要设置[http关键字、下载内容列 共%d列,只设置了%d列].",_st_http_translate_obj.get_column_count(),_st_http_translate_obj.get_column_count() - _st_http_translate_obj._update_check);
                        break;
                    }
                }

                _st_http_translate_obj._update_check = _st_http_translate_obj.get_column_count();
                
                if(_st_http_translate_obj._key_col != -1){ // 解析http列获取关键字,存储到_st_http_translate_obj._key_col_value中
                    if(*trimp == 0){ // http 列为空的情况
                        _st_http_translate_obj._key_col_value[0] = 0;
                    }else{
                        std::string _keyword;    
                        _column_parser_obj.get_keyword_from_http(trimp,_keyword);
                        if(_keyword.size()>HTTP_DOWNLOAD_LEN/2){
                            lgprintf("获取得到错误的关键字:[%s]长度[%d].",_keyword.c_str(),_keyword.size());
                        }                        
                        int valid_len = min(_keyword.size(),HTTP_DOWNLOAD_LEN/2);
                        strncpy(_st_http_translate_obj._key_col_value,_keyword.c_str(),valid_len);
                        _st_http_translate_obj._key_col_value[valid_len]=0;
                    }
                }

                if(_st_http_translate_obj._download_col != -1){ // 解析http列获取下载列内容,存储到_st_http_translate_obj._download_col_value中
                    if(*trimp == 0){ // http 列为空的情况,设置下载内容列的值为空
                        _st_http_translate_obj._download_col_value[0]=0;
                    }else{
                        std::string _download;
                        _column_parser_obj.get_download_from_http(trimp,_download);
                        if(_download.size()>HTTP_DOWNLOAD_LEN/2){
                            lgprintf("获取得到错误的下载内容:[%s]长度[%d].",_download.c_str(),_download.size());
                        }                        
                        int valid_len = min(_download.size(),HTTP_DOWNLOAD_LEN/2);
                        strncpy(_st_http_translate_obj._download_col_value,_download.c_str(),valid_len);
                        _st_http_translate_obj._download_col_value[valid_len]=0;
                    }
                }
                
            }else if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._key_col){
                // 设置关键字的值
                if(pnszVarCol[nCol]){ 
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],_st_http_translate_obj._key_col_value,strlen(_st_http_translate_obj._key_col_value));
                }else{ // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),_st_http_translate_obj._key_col_value);
                }
                _st_http_translate_obj._update_check--;
                
                continue;
            }else if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._download_col){
                // 设置下载项的值
                if(pnszVarCol[nCol]){ 
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],_st_http_translate_obj._download_col_value,strlen(_st_http_translate_obj._download_col_value));
                }else{ // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),_st_http_translate_obj._download_col_value);
                }                
                _st_http_translate_obj._update_check--;
                
                continue; 
            }            
            //>> end: dma-1086     

            if(*trimp==0) {
                wociSetNull(mt,colspos[i],rows,true);
                //mt.SetStr(colspos[i],rows,"");
                //continue;
            }else switch(mt.getColType(colspos[i])) {
            case COLUMN_TYPE_CHAR   :
                if(ipcheck) {
                    wociGetColumnName(mt,colspos[i],colname);
                    if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr(ptrf,'.')==NULL) {
                        int ip=atoi(ptrs[fix_col_ptr_index]);
                        unsigned char *bip=(unsigned char*)&ip;
                        //TODO : this only work on little-endian system
                        sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
                        if(ip==0) ipaddr[0]=0;
                        //strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
                        if(pnszVarCol[nCol]){
                            memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],ipaddr,strlen(ipaddr));
                        }else{
                            strcpy(mt.PtrStrNoVar(nCol,rows),ipaddr);
                        }
                    }
                }else{
                    //ascii码转换
                    if(asciicvt[colspos[i]]) {
                        if(strlen(trimp)>3) {
                            if(recordfmtcheck!=0) 
                                ThrowWith("ASCII 转换只支持一位，文件'%s'第%d行第%d字段的值'%s'无法转换",
                                filename.c_str(),curoffset,i+1,trimp);
                            lgprintf  ("ASCII 转换只支持一位，文件'%s'第%d行第%d字段的值'%s'无法转换",
                                filename.c_str(),curoffset,i+1,trimp);
                            errinfile++;
                            WriteErrorData(linebak);skipline=true;
                        }
                        char chr[2];
                        chr[1]=0;
                        chr[0]=atoi(trimp);
                        if(pnszVarCol[nCol]){
                            memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],chr,2);
                        }
                        else{
                            strcpy(mt.PtrStrNoVar(colspos[i],rows),chr);
                        }
                    }else{      
                        int _trimpLen = strlen(trimp);
                        if(colsAdjustWidth[i]){ // 长度超过目标表的列宽度，且需要截断处理的
                            if( _trimpLen > mt.getColLen(colspos[i])-1){                 
                                // strncpy(mt.PtrStr(colspos[i],rows),trimp,(mt.getColLen(colspos[i])-1));
                                //  mt.PtrStr(colspos[i],rows)[mt.getColLen(colspos[i])-1] = 0;
                                if(pnszVarCol[nCol]){
                                     memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,(mt.getColLen(colspos[i])-1));
                                }else{             
                                    strncpy(mt.PtrStrNoVar(colspos[i],rows),trimp,(mt.getColLen(colspos[i])-1));
                                    mt.PtrStrNoVar(colspos[i],rows)[mt.getColLen(colspos[i])-1] = 0;
                                }
                                if(!skip_printf_log){
                                    lgprintf("文件'%s'第%d行第%d字段超过定义的长度(%d,%d),已截取有效长度(%d).",filename.c_str(),
                                        curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1,mt.getColLen(colspos[i])-1);
                                }
                                
                                ignore_rows++;
                            }else{
                                if(pnszVarCol[nCol]){
                                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,strlen(trimp));
                                }else{
                                    strcpy(mt.PtrStrNoVar(colspos[i],rows),trimp);
                                }
                            }
                        }else{// 不需要调整列宽度，且长度超过列宽度的情况
                            //BUG? char(1),getColLen got 2
                            if( _trimpLen >mt.getColLen(colspos[i])-1){
                                if(recordfmtcheck!=0) 
                                    ThrowWith("文件'%s'第%d行第%d字段超过定义的长度(%d,%d)！",filename.c_str(),
                                    curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1);

                                if(!skip_printf_log){
                                    lgprintf("文件'%s'第%d行第%d字段超过定义的长度(%d,%d)！",filename.c_str(),
                                        curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1);
                                }
                                errinfile++;
                                WriteErrorData(linebak);
                                
                                skipline=true;   
                            } else {
                                if(pnszVarCol[nCol]){
                                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,strlen(trimp));
                                }else{
                                    strcpy(mt.PtrStrNoVar(colspos[i],rows),trimp);
                                }
                            }
                        }
                    }
                }
                break;
            case COLUMN_TYPE_FLOAT  :
                *mt.PtrDouble(colspos[i],rows)=*trimp==0?0:atof(trimp);
                break;
            case COLUMN_TYPE_INT    :
                _int_tmp = ((*trimp==0)?0:atol(trimp));
                if(_int_tmp >= PLUS_INF_64 || _int_tmp <= MINUS_INF_64){// fix dma-1164
                    if(recordfmtcheck!=0) {
                        ThrowWith("文件'%s'第%d行第%d字段错误的数值(%s)！",filename.c_str(),curoffset,i+1,trimp);
                    }
                    else{
                        lgprintf("文件'%s'第%d行第%d字段错误的数值(%s)！",filename.c_str(),curoffset,i+1,trimp);
                        errinfile++;
                        WriteErrorData(linebak);skipline=true;
                    }
                }
                *mt.PtrInt(colspos[i],rows)=_int_tmp;
                break;
            case COLUMN_TYPE_BIGINT :
                _int_tmp = ((*trimp==0)?0:atol(trimp));
                if(_int_tmp >= PLUS_INF_64 || _int_tmp <= MINUS_INF_64){ // fix dma-1164
                    if(recordfmtcheck!=0) {
                        ThrowWith("文件'%s'第%d行第%d字段错误的数值(%s)！",filename.c_str(),curoffset,i+1,trimp);
                    }
                    else{
                        lgprintf("文件'%s'第%d行第%d字段错误的数值(%s)！",filename.c_str(),curoffset,i+1,trimp);
                        errinfile++;
                        WriteErrorData(linebak);skipline=true;
                    }
                }
                *mt.PtrLong(colspos[i],rows)=_int_tmp;
                break;
            case COLUMN_TYPE_DATE   : {
                if(*trimp==0) {
                    wociSetDateTime(mt.PtrDate(colspos[i],rows),1900,1,1,0,0,0);
                    break;
                }
                int of=0,y=0,m=0,d=0,hh=0,mm=0,ss=-1;
                char *dptr=trimp;
                while(*dptr==' ') dptr++;
                if(*dptr<'0' || *dptr>'9') {
                    if(recordfmtcheck!=0) 
                        ThrowWith("文件'%s'第%d行第%d字段错误的日期(%s)！",filename.c_str(),
                        curoffset,i+1,dptr);
                    lgprintf("文件'%s'第%d行第%d字段错误的日期(%s)！",filename.c_str(),
                        curoffset,i+1,dptr);
                    errinfile++;
                    WriteErrorData(linebak);skipline=true;
                    break;
                }
                if(dptr[4]>='0' && dptr[4]<='9') {
                    //没有分隔符,例如：20121112121212
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
                    if(addsecond && strlen(dptr) == 12){//201211121212
                        ss = 0 ;                               
                    }else{
                        ss=atoi(dptr);dptr+=2;
                    }
                }else {
                    // 存在分隔符，例如：2012-11-12 12:12:12,2012/12/12 12/12/12
                    y=atoi(dptr+of);of+=5;
                    m=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                    d=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                    hh=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                    mm=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                    if(addsecond && strlen(dptr)==16){ // 2012-11-12 12:12
                        ss = 0;   
                    }else{
                        ss=atoi(dptr+of);
                    }
                }
                if(y<1900 || m<1 || m>12 || d<1 || d>31 ||hh<0 || hh>60 || mm<0 || mm>60 ||ss<0 || ss>60) {
                    //>> begin: fix dma-895
                    if(recordfmtcheck!=0) {
                        ThrowWith("文件'%s'第%d行第%d字段错误的日期(%s)！",filename.c_str(),curoffset,i+1,dptr);
                    }
                    else{
                        lgprintf("文件'%s'第%d行第%d字段错误的日期(%s)！",filename.c_str(),curoffset,i+1,dptr);
                        errinfile++;
                        WriteErrorData(linebak);skipline=true;
                    }
                    //<< end: fix dma-895
                    break;
                }
                wociSetDateTime(mt.PtrDate(colspos[i],rows), y,m,d,hh,mm,ss);
                }
                break;
            case COLUMN_TYPE_NUM    :
            default :
                ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                break;
                
            } // end switch(mt.getColType(colspos[i])) {

            if(skipline){
                _st_http_translate_obj._update_check = 0;
                break;
            }

            fix_col_ptr_index ++;
        }  //end for(int i=0;i<colct;i++) 

        if(!skipline) {
             for(int _i = 0;_i<nulls_colct;_i++){
                mt.SetStr(nulls_colspos[_i],rows,(char*)"");
             }
             filerows++;
             rows++;
             nVarRow++;

             if(nVarRow == nWrNum){
                for(int i=0;i<colct;i++){
                    if(pnszVarCol[colspos[i]]){
                        mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
                        memset(pWrBf[colspos[i]],0,nWrNum*pnszTypeLen[colspos[i]]*sizeof(char));
                    }
                }
                nVarLevelRow = rows;
                nVarRow = 0;
                _wdbiSetMTRows(mt,rows);
             }
        }
    }// end while (Get Next Line)

    for(int i=0;i<colct;i++){
        if(pnszVarCol[colspos[i]]){
            mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
            delete[] pWrBf[colspos[i]];
        }
    }
    _wdbiSetMTRows(mt,rows);
  
    delete[] pnszTypeLen;
    delete[] pnszVarCol;
    delete[] pWrBf;
    
    if(feof(fp)!=0){
        AutoStmt st(dbc);
        /***************文件解析结果记录*****************/
        //获取错误数据存储位置
    
        char errfilename[300];
        strcpy(errfilename,pWrapper->getOption("files:errordatapath","/tmp"));
        
        if(errfilename[0]!=0){
            if(errfilename[strlen(errfilename)-1]!='/') strcat(errfilename,"/");
            strcat(errfilename,basefilename.c_str());
            strcat(errfilename,".err");
        }else{      
            strcpy(errfilename,"NULL");
        }

        // 设置文件为临时处理状态
        st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now(),errrows=%d,errfname='%s',ignorerows=%d where tabid=%d and datapartid=%d and fileseq=%d",filerows,errinfile,errfilename,ignore_rows,tabid,datapartid,fileseq);
        st.Execute(1);
        st.Wait();
        
        lgprintf("文件'%s'处理结束，共%d行记录，文件%d行，错误%d行。",filename.c_str(),filerows,curoffset,errinfile);
        fclose(fp);fp=NULL;
        if(errinfile>0){
            sp.log(tabid,datapartid,106,"文件'%s'处理结束，共%d行记录，文件%d行，错误%d行。日志文件: %s ",filename.c_str(),filerows,curoffset,errinfile,_wdbiGetLogFile());

            // 对错误数据文件进行压缩处理,fix dma-1159   
            CompressErrData(); 
        }
        
        if( fm.SrcFileIsZipped() ||
            (pWrapper->getOption("files:keeppickfile",0)!=1 && pWrapper->getOption("files:skippickup",0)!=1) ||
            fm.GetIsBackupDir()
          ){
            lgprintf("删除文件[%s]",filename.c_str());
            remove(filename.c_str());
        }

        //>> Begin: delete src file ,after parsed src file,DM-216
        fm.DeleteSrcFile();
        //<< End:

        curoffset=0;
    }
    else if(rows==maxrows) {
        return 0; // memtable is full and datafile not eof
    }
    if(rows==maxrows) {
        return 2;// memtable is full and datafile is eof
    }
    return 1;
}
