#ifndef TBLOADER_JIN_DEF_H
#define TBLOADER_JIN_DEF_H

/********************************************************************
  file : tbloader_jni.h
  desc : define the tbloader java call interface
  author: liujianshun,201304
  note: support multiple thread use
  libname:libtbloader.so
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onInit(JNIEnv *env,jobject obj);

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onUninit(JNIEnv *env,jobject obj);


/*
    函数: getStorerID
    描述: 获取空闲的存储ID
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
    返回值:
         -1: 失败没有空闲可用的ID
         成功：[0-50]: 可用的ID
    前件条件:onInit调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_getStorerID(JNIEnv *env,jobject obj);

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
    前件条件:
          1. odbc 数据源配置正常
          2. dbname.tbname 要求存在
          3. getStorerID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForDB(JNIEnv *env,jobject obj,jint storerid,
                          jstring dsn,jstring user,jstring pwd,jstring dbname,jstring tbname,
                          jint engine,jstring logpth); 

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
          1: 已经初始化过，不能重复调用
          2: 文件打开失败
    前件条件:
          1. odbc 数据源配置正常
          2. dbname.tbname 要求存在
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForFile(JNIEnv *env,jobject obj,jint storerid,
                           jstring fname,jstring logpth); 

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
          3: 未初始化 initStorerForFile 或者 initStorerForDB
		  4: 数据头内容解析错误
		  5: 数据头和目标表结构不一致
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeHeadInfo(JNIEnv *env,jobject obj,jint storerid,
                          jint headerlen,jbyteArray header);


/*
    函数: writeDataBlock
    描述: 写数据头内容
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
          blocklen[in]   : 明文数据长度
          datablock[in]: 明文数据
          rowNum[in]: 数据包中记录行数
    返回值:
          0: 成功
          -1: 文件写入失败
          1: 文件头未写入过,writeHeadInfo未调用成功过
          2: 数据长度校验失败
          3: 未初始化 initStorerForFile 或者 initStorerForDB
		  4: 数据包错误
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeDataBlock(JNIEnv *env,jobject obj,jint storerid,
                          jint blocklen,jbyteArray datablock,jint rowNum);

/*
    函数: getDataMD5
    描述: 获取数据的md5校验值
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
    返回值:
          32个字符的数据md5校验值
    说明:
          在调用closeStorer之前调用
*/
JNIEXPORT jstring JNICALL Java_com_datamerger_file_NativeStorer_getDataMD5(JNIEnv *env,jobject obj,jint storerid);


/*
    函数: closeStorer
    描述: 释放tbloader接口
    参数: 
          env[in]   : jni 环境接口指针
          obj[in]   : jni 对象参数，必须要有的
          storerid[in] : storerID
    返回值:
          0: 成功     
          1: 数据库文件写入失败
          2: 导出文件写入失败
          3: 未调用initStorerForFile或者initStorerForDB
    前件条件:
          1. initStorerForFile 调用成功 或者 initStorerForDB 调用成功
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_closeStorer(JNIEnv *env,jobject obj,jint storerid);


#ifdef __cplusplus  
}
#endif 

#endif
