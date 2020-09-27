#ifndef DTIO_COMMON_H
#define DTIO_COMMON_H
#include <stdlib.h>
#include <stdio.h>
#include <dt_common.h>
#ifdef WIN32
#include <windows.h>
typedef __int64 uint8B;
typedef __int64 int8B;
//#define getpass(a) 
char *dirname(char *dirc) ;
char *basename(char *dirc);
#else
#include <libgen.h>
#include <unistd.h>
typedef unsigned long long uint8B;
typedef long long int8B;
#endif

typedef unsigned int uint4;
typedef int int4;
#ifdef MYSQL_VER_51
#define DTIO_OLDVERSION 0x1010
#define DTIO_OLDVERSION3 0x1020
#define DTIO_VERSION 0x1030
#else
#define DTIO_OLDVERSION 0x1010
#define DTIO_VERSION 0x1020
#endif
#define MAXCONTFILE 20
#define DTIO_STREAMFLAG 0xa511
#define PATH_LEN 250 //120
/*
#ifndef DTIOExport
#ifdef WIN32
#ifdef  WDBI_EXPORTS
#define DTIOExport   __declspec( DTIOExport ) 
#else
#define DTIOExport   __declspec( dllimport ) 
#endif
#else
#define DTIOExport
#endif
#endif
*/
//CRC相关字义
typedef unsigned int uLongCRC ;
typedef unsigned int uLongCRCf;
typedef unsigned int uInt;
typedef unsigned char Bytef;
#ifndef __linux
/* Centos 5.1已经有/usr/include/zlib.h中定义crc32了*/
#ifdef __cplusplus
extern "C"
{
	//uLongCRC crc32(uLongCRC crc,const Bytef *buf,uInt len);
}
#else
 uLongCRC crc32(uLongCRC crc,const Bytef *buf,uInt len);
#endif
#endif
#define DTIO_UNITTYPE_MEMTAB		2020
#define DTIO_UNITTYPE_MFRMFILE		2021
#define DTIO_UNITTYPE_MMYIFILE		2022
#define DTIO_UNITTYPE_MMYDFILE		2023
#define DTIO_UNITTYPE_SYSDBFILE		2024
#define DTIO_UNITTYPE_DTDBFILE		2025
#define DTIO_UNITTYPE_DESTDBFILE	2026
#define DTIO_UNITTYPE_IDXDBFILE		2027
#define DTIO_UNITTYPE_DATFILE		2028
#define DTIO_UNITTYPE_IDXFILE		2029
#define DTIO_UNITTYPE_BLOCKHDR		2030

#define DTIO_UNITTYPE_NORMDTTAB		2031
#define DTIO_UNITTYPE_DTPARAM		2032
#define DTIO_UNITTYPE_MMRGFILE		2033

#define DPIO_VERSION			" 数据备份/恢复工具 "
#define GetDFMName(dtio) (dtio->GetVersion()==DTIO_OLDVERSION?"DP_DATAFILEMAP":"DP_DATAFILEMAP")
#define GetIFMName(dtio) (dtio->GetVersion()==DTIO_OLDVERSION?"DP_DATAFILEMAP":"DP_DATAFILEMAP")
#define GetDPTable(dtio) (dtio->GetVersion()==DTIO_OLDVERSION?"DP_TABLE":"DP_TABLE")
#define GetDPIndex(dtio) (dtio->GetVersion()==DTIO_OLDVERSION?"DP_INDEX":"DP_INDEX")
#define GetDPIndexAll(dtio) (dtio->GetVersion()==DTIO_OLDVERSION?"DP_INDEX_ALL":"DP_INDEX_ALL")
//#define DEFAULT_SPLITLEN		((long long)0x800000000000)
//#define DEFAULT_SPLITLEN		((long long)650000000)
DTIOExport extern int8B DEFAULT_SPLITLEN;
DTIOExport void ThrowWith(const char *format,...) ;
#endif
