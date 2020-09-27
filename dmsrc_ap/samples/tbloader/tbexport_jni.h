#ifndef TBLOADER_JIN_DEF_H
#define TBLOADER_JIN_DEF_H

/********************************************************************
  file : tbexport_jni.h
  desc : define the tbexport java call interface
  author: liujianshun,201304
  note: support multiple thread use
  libname:libtbexporter.so
********************************************************************/
#include <jni.h>

#ifdef __cplusplus  
extern "C" {  
#endif 
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj);

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj);

/*
    函数: getCsvID
    描述: 获取空闲的CSV导出ID
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         -1: 失败没有空闲可用的ID
         成功：[0-50]: 可用的ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj);

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
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth); 

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
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID);

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
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID);

/*
    函数: closeCsv
    描述: 释放csv接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          csvID[in] : csvID
    返回值:
          0: 成功  
          -1: 无效的ID
    前件条件:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID);


#ifdef __cplusplus  
}
#endif 

#endif
