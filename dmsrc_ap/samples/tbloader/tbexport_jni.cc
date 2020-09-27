#include <string.h>
#include <stdio.h>
#include "tbexport_jni.h"
#include "tbexport_helper.h"

#define MAX_EXPORTER_NUM   50
#define EXPORTER_BASE      200

//--------------------------------------------------------------------------------------------------------
pthread_mutex_t     g_exporterlock;         // 锁
bool                g_exporter_init_flag = false;
tbexport            g_exporter[MAX_EXPORTER_NUM];  // exporter

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeCSV_onInit...\n");
    if(g_exporter_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_exporterlock,NULL);
    pthread_mutex_lock(&g_exporterlock);
    if(g_exporter_init_flag == true){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
       g_exporter[i].setId(-1);
       g_exporter[i].setRunFlag(false);
       g_exporter[i].setStatus(exp_unstart);
    }
    
    // wdbi 日志，只能初始化1次
    if(!g_logInit){
        g_logInit = true;
        WOCIInit("tbloader_log/tbloader");
        wociSetOutputToConsole(TRUE);
        wociSetEcho(true);
        g_sysPath[0]= 0;
        LoadSysPath(g_sysPath);
    }
    
    pthread_mutex_unlock(&g_exporterlock);
    g_exporter_init_flag = true;    
    lgprintf("NativeCSV_onInit ok.\n");
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj)
{
    lgprintf("NativeCSV_onUninit...\n");
	if(g_exporter_init_flag == false){
    	return (jint)0;
    }
    pthread_mutex_lock(&g_exporterlock);
	if(g_exporter_init_flag == false){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    int retcode = 0;
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
       if(g_exporter[i].getRunFlag()){
          g_exporter[i].stop();
          g_exporter[i].setStatus(exp_unstart);
       }
    }       
    pthread_mutex_unlock(&g_exporterlock);
    pthread_mutex_destroy(&g_exporterlock);
    g_exporter_init_flag = false;
    lgprintf("NativeCSV_onUninit ok.\n");
    
    if(g_logInit){
        g_logInit = false;
        WOCIQuit(); 
    }
    return (jint)retcode;   
}

/*
    函数: getCsvID
    描述: 获取空闲的CSV导出ID
    参数: 
          evn[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         -1: 失败没有空闲可用的ID
         [0-50]: 可用的ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj)
{    
    int storerid ;
    pthread_mutex_lock(&g_exporterlock);
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
        if(!g_exporter[i].getRunFlag())  
        {
            storerid = i+EXPORTER_BASE;
            g_exporter[i].setRunFlag(true);
            g_exporter[i].setId(storerid);
            g_exporter[i].setStatus(exp_unstart);
            break;
        }
    }
    pthread_mutex_unlock(&g_exporterlock);
    lgprintf("getCsvID storerid = %d\n",storerid);
    return (jint)storerid;

}

/*
    函数: initCsv
    描述: 初始导出csv文件数据接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          csvID[in] : csvID
          filepath[in]:csv 文件名称
          dsn[in]   : odbc 连接数据库数据源
          user[in]  : odbc 数据源连接数据库的用户名称
          pwd[in]   : odbc 数据源连接数据库的用户密码
          SQL[in]   : sql语句
          logpth[in]: 日志输出路径
    返回值:
          0: 成功
          -1: 数据库连接失败 
          1: 已经初始化过，不能重复调用
          2: 无效ID
    前件条件:
          1. odbc 数据源配置正常
          2. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_initCsv(JNIEnv *env,jobject obj,jint csvID,
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){  	  
        lgprintf("initCsv csvID = %d error.\n",_csvID);
        return 2;
    }
    
    if (g_exporter[_csvID-EXPORTER_BASE].getStatus()!= exp_unstart && g_exporter[_csvID-EXPORTER_BASE].getStatus() != exp_hasstoped)
    {
        lgprintf("initCsv getStatus() = %d\n",g_exporter[_csvID-EXPORTER_BASE].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars(filepath, 0));
    const char* logpath = (char *)env->GetStringUTFChars(filepath, 0);
    const char* strsql = (char*)env->GetStringUTFChars(SQL, 0);
    
    lgprintf("initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,logpth=%s]\n",
           _csvID,st.dsn,st.user,st.pwd,logpath);
           
    pthread_mutex_lock(&g_exporterlock);
    ret = g_exporter[_csvID-EXPORTER_BASE].start(&st,strsql,logpath);
    pthread_mutex_unlock(&g_exporterlock);
    
    return (jint)ret;
}

/*
    函数: writeCSV
    描述: 写csv文件接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          csvID[in] : csvID
    返回值:
          true : 成功
          false: 失败
    前件条件:
          1. initCsv
    说明:该表过程是异步执行的，内部独立线程执行
*/
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){	  
        lgprintf("initCsv csvID = %d error.\n",_csvID);
        return false;
    }
    lgprintf("writeCSV csvid = %d.",_csvID);  
    ret = g_exporter[_csvID-EXPORTER_BASE].doStart();
    return (jboolean)(ret == 0 ? true:false);  
}

/*
    函数: stateCSV
    描述: 写csv文件接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          csvID[in] : csvID
    返回值: 返回码_当前写入记录数_导出总记录数
        返回码取值:
        0: 成功  <0_当前写入记录数_导出总记录数>
        1: 失败，写入数据失败 <1_当前写入记录数_导出总记录数>              
    前件条件:
          1. initCsv
    说明:该表过程是异步执行的，内部独立线程执行
*/
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID)
{
    char rtnValue[128] = {0};
    jstring rtn;
    
    int _csvID = (int)csvID;
    
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){	  
        lgprintf("stateCSV csvID = %d error.\n",_csvID);
        strcpy(rtnValue,"1_0_0");        
        rtn = env->NewStringUTF(rtnValue);
        return rtn;
    }    
    sprintf(rtnValue,"0_%d_%d",g_exporter[_csvID-EXPORTER_BASE].getCurrentRows(),g_exporter[_csvID-EXPORTER_BASE].getRowSum());
    lgprintf("id=[%d] stateCSV return : %s.\n",_csvID,rtnValue);
    rtn = env->NewStringUTF(rtnValue);
    return rtn;
}

/*
    函数: closeCsv
    描述: 释放csv接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          csvID[in] : csvID
    返回值:
          0: 成功  
          1: 无效的ID
    前件条件:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 1;
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){
        lgprintf("closeCsv csvID = %d error.\n",_csvID);
        return (jint)ret;
    }
    ret = g_exporter[_csvID-EXPORTER_BASE].stop();
    g_exporter[_csvID-EXPORTER_BASE].setId(-1);
    g_exporter[_csvID-EXPORTER_BASE].setStatus(exp_hasstoped);
    lgprintf("closeCsv csvid = %d.\n",_csvID);
   
    return (jint)0;
}
