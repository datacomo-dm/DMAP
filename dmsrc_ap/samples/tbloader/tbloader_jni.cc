#include <string.h>
#include <stdio.h>
#include "tbloader_jni.h"
#include "tbloader_helper.h"


#define MAX_LOADER_NUM   50

// 导出数据对象
pthread_mutex_t     g_loaderlock;         // 锁
bool                g_loader_init_flag = false;
tbloader_helper     g_loader[MAX_LOADER_NUM];  // loader

/*
    函数: onInit
    描述: 初始化库，申请资源(锁控制等)
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         0: 成功
         -1:失败
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeStorer_onInit...\n");
    if(g_loader_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_loaderlock,NULL);
    if(g_loader_init_flag == true){
        pthread_mutex_unlock(&g_loaderlock);
    	return (jint)0;
    }
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       g_loader[i].setId(-1);
       g_loader[i].setRunFlag(false);
       g_loader[i].setStatus(status_unknown);
    }
    g_loader_init_flag = true;
    
    // wdbi 日志，只能初始化1次
    if(!g_logInit)
    {        
        g_logInit = true;
        WOCIInit("tbloader_log/tbloader");
        wociSetOutputToConsole(TRUE);
        wociSetEcho(true);
        g_sysPath[0]= 0;
        LoadSysPath(g_sysPath);
    } 
    pthread_mutex_unlock(&g_loaderlock);
    
    lgprintf("NativeStorer_onInit ok.");
    return (jint)0;
}

/*
    函数: onUninit
    描述: 释放已申请的资源(锁控制等)
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         0: 成功
         -1:失败
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onUninit(JNIEnv *env,jobject obj)
{
    lgprintf("NativeStorer_onUninit...");
    int retcode = 0;
    if(g_loader_init_flag == false){
       return (jint)0;
    }  	
    pthread_mutex_lock(&g_loaderlock);
    if(g_loader_init_flag == false){
       pthread_mutex_unlock(&g_loaderlock);
       return (jint)0;
    } 
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       if(g_loader[i].getRunFlag()){
          g_loader[i].stop();
          g_loader[i].setId(-1);
       }
    }
    pthread_mutex_unlock(&g_loaderlock);
    pthread_mutex_destroy(&g_loaderlock);
    g_loader_init_flag = false;
    lgprintf("NativeStorer_onUninit ok.");
    if(g_logInit){
        g_logInit = false;
        WOCIQuit(); 
    }
    return (jint)retcode;   		
}

/*
    函数: getStorerID
    描述: 获取空闲的存储ID
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         -1: 失败没有空闲可用的ID
         [0-50]: 可用的ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_getStorerID(JNIEnv *env,jobject obj)
{
    int storerid=-1;
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
        if(!g_loader[i].getRunFlag())  
        {
            storerid = i;
            g_loader[i].setRunFlag(true);
            g_loader[i].setStatus(unstart);
            g_loader[i].setId(i);
            g_loader[i].setSysPath(g_sysPath);
            break;
        }
    }
    pthread_mutex_unlock(&g_loaderlock);
    
    lgprintf("id=[%d] getStorerID storerid = %d",g_loader[storerid].getId(),storerid);
    return (jint)storerid;
}

/*
    函数: initStorerForDB
    描述: 初始数据存储功能
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          dsn[in]   : odbc 连接数据库数据源
          user[in]  : odbc 数据源连接数据库的用户名称
          pwd[in]   : odbc 数据源连接数据库的用户密码
          dbname[in]: 数据库实例名称
          tbname[in]: 数据库表名称
          logpth[in]: 日志输出路径
          engine[in]: 数据表引擎<1:MyISAM> <2:brighthouse>
    返回值:
          0: 成功
          -1: 数据库连接失败 
          1: 已经初始化过，不能重复调用
          2: 无效的storerid
    前件条件:
          1. odbc 数据源配置正常
          2. dbname.tbname 要求存在
          3. getStorerID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForDB(JNIEnv *env,jobject obj,jint storerid,
                          jstring dsn,jstring user,jstring pwd,jstring dbname,jstring tbname,
                          jint engine,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("initStorerForDB storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= unstart &&  g_loader[_storerid].getStatus() != hasstoped)
    {
        lgprintf("initStorerForDB id[%d] getStatus() = %d error.",_storerid,g_loader[_storerid].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.dbname,(char *)env->GetStringUTFChars(dbname, 0));
    strcpy(st.tbname,(char *)env->GetStringUTFChars(tbname, 0));
    int _engine = (int)engine;
    st.tbEngine = (_EngineType)_engine;
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);    
    
    if(st.tbEngine != MyISAM && st.tbEngine != Brighthouse)
    {
       lgprintf("table %s.%s engine %d error,engine<1:MyISAM 2:brighthouse>.",st.dbname,st.tbname,st.tbEngine);
       return 5;
    }
	
    lgprintf("id=[%d] initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,tbname = %s,engine = %d,logpth=%s]",
           g_loader[storerid].getId(),storerid,st.dsn,st.user,st.pwd,st.dbname,st.tbname,st.tbEngine,logpath);
    
    int ret = 0;
    pthread_mutex_lock(&g_loaderlock);
    ret = g_loader[_storerid].start(&st,insert_db,logpath);	
    pthread_mutex_unlock(&g_loaderlock);
    return ret;    
}

/*
    函数: initStorerForFile
    描述: 初始数据存储功能
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          logpth[in]: 日志输出路径
          fname[in]: 完整路径文件名称
    返回值:
          0: 成功
          -1: 数据库连接失败 
          1: 已经初始化过，不能重复调用
          2: 文件打开失败
          3: 无效的storerid
    前件条件:
          1. odbc 数据源配置正常
          2. dbname.tbname 要求存在
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForFile(JNIEnv *env,jobject obj,jint storerid,
                           jstring fname,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("initStorerForFile storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= unstart && g_loader[_storerid].getStatus() != hasstoped)
    {
        lgprintf("initStorerForFile getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    
    _InitInfo st;
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars(fname, 0));
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);
    
    lgprintf("id=[%d] initStorerForFile params [storerid = %d,filename = %s,logpth=%s]",
           g_loader[storerid].getId(),storerid,st.fn_ext,logpath);
           
    return g_loader[_storerid].start(&st,write_files,logpath);	
}

/*
    函数: writeHeadInfo
    描述: 写数据头内容
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          headerlen[in]   : 文件列头部数据长度,4字节
          header[in]: 文件列头部数据
    返回值:
          0: 成功
          -1: 文件写入失败
          1: 文件头数据重复调用写入
          2: 数据长度校验失败
          3: 无效的storerid
          4: 未初始化 initStorerForFile 或者 initStorerForDB
		  5: 数据头内容解析错误
		  6: 数据头和目标表结构不一致
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeHeadInfo(JNIEnv *env,jobject obj,jint storerid,
                          jint headerlen,jbyteArray header)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("writeHeadInfo storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()>= parserheader){
        lgprintf("writeHeadInfo getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus()< hasstarted){
        lgprintf("writeHeadInfo getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 4;
    }
    lgprintf("id=[%d] writeHeadInfo storerid = %d,headerlen = %d",g_loader[_storerid].getId(),_storerid,headerlen);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(header, 0);
    jint  oldsize = env->GetArrayLength(header);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _headerlen = (int)headerlen;
    if(len != (int)_headerlen){ 
        env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);
        lgprintf("writeHeadInfo len [ %d != %d] error ",_headerlen,len);
        return 2;
    }
      
    int ret = g_loader[_storerid].parserColumnInfo((const HerderPtr)bytearr,_headerlen);
    env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);    
    return (jint)ret;
}

/*
    函数: writeDataBlock
    描述: 写数据头内容
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          blocklen[in]   : 明文数据长度
          datablock[in]: 明文数据
    返回值:
          0: 成功
          -1: 文件写入失败
          1: 文件头未写入过,tl_writeHeadInfo未调用成功过
          2: 数据长度校验失败
          3: 无效的storerid
          4: 已经关闭状态，不能在写入数据
		  5: 数据包错误
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeDataBlock(JNIEnv *env,jobject obj,jint storerid,
                          jint blocklen,jbyteArray datablock,jint rowNum)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("writeDataBlock storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()< parserheader){
        lgprintf("writeDataBlock getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus() == hasstoped){
        lgprintf("writeDataBlock getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 4;
    }
	if(g_loader[_storerid].getOperType() == write_files && rowNum<=0)
	{
        lgprintf("writeDataBlock getOperType() == write_files && rowNum<=0.");
        return 6;
    }
    Timer t1(_storerid,"NativeStorer_writeDataBlock 完整过程");
    dbg_lgprintf("id=[%d] writeDataBlock storerid = %d,blocklen = %d,rowNum = %d.",g_loader[_storerid].getId(),_storerid,blocklen,rowNum);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(datablock, 0);
    jint  oldsize = env->GetArrayLength(datablock);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _blocklen = (int)blocklen;
    if(len != (int)_blocklen){
        lgprintf("id=[%d] closeStorer block len [%d != %d] error = %d",_storerid,_blocklen,blocklen);         
        env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
        return 2;
    }
    
    int ret = g_loader[_storerid].writePackData((const char*)bytearr,_blocklen,rowNum);
    env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
    
    return (jint)ret;
}

/*
    函数: getDataMD5
    描述: 获取数据的md5校验值
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          hasdata[in]   : 是否写入过数据
    返回值:
          32个字符的数据md5校验值
    说明:
          在调用closeStorer之前调用
*/
JNIEXPORT jstring JNICALL Java_com_datamerger_file_NativeStorer_getDataMD5(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    char md5sum[conMd5StrLen] = {0};
    
    jstring rtn;
    // 无效id，0000000
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        memset(md5sum,0,conMd5StrLen);
        rtn = env->NewStringUTF(md5sum);
        lgprintf("id=[%d] getDataMD5 md5sum = %s",_storerid,md5sum);
        return rtn;
    }

	lgprintf("id=[%d] getDataMD5 begin ...",g_loader[storerid].getId());

	// 还没有执行完成，等待write数据完成后再进行数据dm5值获取
	if(g_loader[_storerid].getStatus() < hasstoped){
		g_loader[_storerid].stop();
		lgprintf("id=[%d] stop finish ...",g_loader[storerid].getId());	
	}
       
    g_loader[_storerid].getMd5Sum(md5sum);
    lgprintf("id=[%d] getDataMD5 md5sum = %s",g_loader[storerid].getId(),md5sum);
    rtn = env->NewStringUTF(md5sum);
    return rtn;
}

/*
    函数: closeStorer
    描述: 释放tbloader接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          hasdata[in]   : 是否写入过数据
    返回值:
          code_md5sum<32字节，所有DataBlock的md5连续校验值(writeDataBlock参数)>
          code值:
          0: 成功     
          1: 数据库文件写入失败
          2: 导出文件写入失败
          3: 未调用initStorerForFile或者initStorerForDB
          4: storerid错误
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_closeStorer(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("closeStorer storerid = %d ",_storerid);
        return 4;
    }
   
    lgprintf("id=[%d] closeStorer storerid = %d,runflag = %d ",g_loader[_storerid].getId(),_storerid,g_loader[_storerid].getRunFlag()?1:0);
	// 有可能getDataMD5中已经调用了
    if(g_loader[_storerid].getRunFlag() == true){
        retcode = g_loader[_storerid].stop();
    }       
    g_loader[_storerid].setId(-1); 

    lgprintf("after close: id=[%d] closeStorer storerid = %d,runflag = %d ",g_loader[_storerid].getId(),_storerid,g_loader[_storerid].getRunFlag()?1:0);

    return (jint)retcode;
}
