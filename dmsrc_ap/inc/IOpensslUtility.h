/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : IOPENSSLUTILITY.H      
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2012/9/6 10:48
  Description : OpensslUtility 对外工具,支持windows/linux，主要实现功能如下
				DES 加密解密
				RSA 加密解密
				BASE64 转换
				IToolUtility 常用简单函数
**************************************************************************************************/
#ifndef OPENSSL_UTILITY_DEF_H
#define	OPENSSL_UTILITY_DEF_H

#include <cstdio>
#include <iostream>
#include <cassert>						// assert
#include <cstring>
#include <cstdlib>
#include <cstdarg>						// va_start,va_end,va_list
#include "openssl/rsa.h"				// rsa
#include "openssl/x509.h"				// d2i,i2d,bn
#include "openssl/des.h"				// des
#include "openssl/md5.h"				// md5
#include "openssl/evp.h"                // base64

using	namespace	std;

namespace	IOpensslUtility
{
//#pragma pack(4)							// 4字节对齐 
#define			ERROR			-1			// 失败
#define			OK				0			// 成功

#define		ASSERT(exp)		assert((exp))		// assert
#define		CON_EXP(exp)	#exp
#define		FREE_MEM(mem) do {if(NULL != (mem)){free((mem));(mem) = NULL;}} while (0); // 是否malloc的内存
typedef		char					CHAR;
typedef		unsigned	char		UCHAR;
typedef		int						INT;
typedef		unsigned	int			UINT;
typedef		short					SSHORT;
typedef		unsigned	short		USHORT;
typedef		long					LONG;
typedef		unsigned	long		ULONG;	
typedef		unsigned	short		WORD;
typedef		unsigned	int			DWORD;
#ifdef _WIN32
#define		VOID		void
#else
typedef		void		VOID;			// activex conflict
#endif
#define		CONST		const

// RSA 加密解密,加密位数
CONST	USHORT		conCryptoBit_1024		=	1024;
CONST	USHORT		conCryptoBit_512		=	512;
CONST	USHORT		conCryptoBit_256		=	256;
CONST	USHORT		conCryptoBit_128		=	128;
CONST	USHORT		conCryptoBit_64			=	64;


CONST	USHORT		conMd5SumLen            =   16;     // MD5校验字符
CONST	USHORT      conDesKeysFileLen       =   8*1024;	// 存储1024个
enum EnumKeySourceType    // 密钥类型，对应dp.dp_keysource的keytype
{
	KeySourceType_Des_Random_key = 1,                   // Des随机密钥文件类型
};

CONST	USHORT		conKeyPosLen			=	2;		// 密钥数据长度

#define	DATA_LEN_4_BYTES								// 数据长度定义，目前是支持4字节的数据，去除该宏后，长度为2
#ifdef DATA_LEN_4_BYTES
CONST	USHORT		conDataLenLen			=	4;		
typedef UINT   DATA_LEN_LEN;							// 4字节
#else
typedef USHORT DATA_LEN_LEN;							// 2字节 0xFFFF,最大64KB
CONST	USHORT		conDataLenLen			=	2;		
#endif 


const	INT		conEcbKeyLen = 8;			// ecb模式key长度 8 
const	INT		conEcb3KeyLen = 16;			// ecb3模式key长度 16
const	INT		conEcbEncryDataLen = 8;		// ecb模式下，每次只能加密8字节

// 描述加密锁DES,RSA的状态是否好
typedef	enum	CryptoStatus
{
	CRYPTO_UNREADY = 0,				// 初始状态，0
	CRYPTO_READY = 7,			    // Ready状态，可以进行加密,DES/RSA共用，加密/解密111--->7
	CRYPTO_RSA_ENCRYPT = 3,			// RSA用，只能加密011--->3
	CRYPTO_RSA_DECRYPT = 5,			// RSA用，只能解密101--->5
}CryptoStatus;


	/**************************************************************************************************
	  Object      : IToolUtility 工具函数
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 10:50
	  Description : 主要提供一些常用的测试函数
					LogMsg该函数 需要注意
					1）给服务端(控制台程序,win/linux)用需要重新写（结合服务端调用输出函数）
					2）给客户端调用，例如Activex的日志函数，如果需要弹出MessageBox那就需要重定向
	**************************************************************************************************/
	namespace	IToolUtility
	{
		#if 0
		// 调试信息输出到屏幕，内部已经包含了具体的文件名称与行数
		VOID	LogMsg(CONST CHAR*	pszFormat,...); 
		#else
		// 后面用统一的函数定义,注意：activex中使用的应该是MessageBox输出，控制台程序可以使用printf输出
		#define		LogMsg	printf
		#endif
		// ☆ 后续增加从文件的指定位置读取

		// 从文件中读取指定字节的数据,二进制读取文件,返回读取成功的字节数
		LONG	LoadDataFromFile(CONST CHAR* pfilename,UCHAR *pbuff,CONST LONG len,LONG *retLen);		
		// 将数据写入文件,返回写入字节数，二进制写入文件，返回写入成功的字节数
		LONG	SaveDataToFile(CONST CHAR* pfilename,CONST UCHAR *pdata,CONST LONG saveLen);

		// 输出字符串
		VOID	PrintData(CONST UCHAR *pdata,CONST UINT len,CONST CHAR *pcaption);
		// 将byte转换成16进制字符串
		INT		ChangeBytesToHexstr(CONST UCHAR *pSource,CONST INT sourceLen,CHAR *pDest,CONST INT destLen,INT *retDestLen);

		/************************************************************************/
		/* 下面的函数用宏替代                                                                     */
		/************************************************************************/
		// 16字节的MD5校验值
		UCHAR*	MD5Sum(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *md);
		// 生成keyIndex
		USHORT	GetKeyIndexFromPlainText(CONST UCHAR *pSource,CONST UINT sourceLen);
		// 从[KeyIndex+DataLen+Cipher]中获取KeyIndex ,DataLen
		void	GetKeyIndexAndDataLenFromCombination(CONST UCHAR *pCombinationBuff,USHORT *KeyIndex,DATA_LEN_LEN *DataLen);
	}

	/**************************************************************************************************
	  Object      : RsaCrypto
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:15
	  Description : RSA加密解密(公钥加密，私钥解密)，公钥的导入导出，私钥的导入导出等功能
	**************************************************************************************************/
	class RsaCrypto
	{
	public:
		RsaCrypto();
		virtual ~RsaCrypto();
	public:
		/************************************************************************/
		/*  注意：openssl 算法对接的密钥的导入，
		/*  导出公钥私钥，加密与解密必须用openssl库
		/************************************************************************/
		INT			LoadRsaPubKeyFromFile(CONST	CHAR	*pKeyFile);
		INT			SaveRsaPubkeyToFile(CONST	CHAR	*pKeyFile);
		INT			LoadRsaPriKeyFromFile(CONST	CHAR	*pKeyFile);
		INT			SaveRsaPrikeyToFile(CONST	CHAR	*pKeyFile);

		/************************************************************************/
		/*  生成ras密钥（公钥私钥）                                                                    
		/************************************************************************/
		INT			RenewGenerateKey(CONST INT	bits = conCryptoBit_1024,CONST ULONG pubExponnent = RSA_3);

	public:
		/************************************************************************/
		/* 注意：从openssl算法库导出公钥模数（私钥模数），公钥指数，私钥指数
		/* 给其他对接（加密，解密）的算法使用
		/* 公钥模数 + 公钥指数 ----> 公钥
		/* 公钥模数 + 公钥指数 + 私钥指数----> 私钥
		/************************************************************************/
		// 导出模数(公钥私钥一样的)，pHexModule 在外部用OPENSSL_free释放
		INT			ExportModule(CHAR **pHexModule,INT *moduleLen);
		// 导出公钥指数, 在外部用OPENSSL_free释放
		INT			ExportPubExponent(CHAR **pHexPubExponent,INT *pubExponentLen);
		// 导出私钥指数, 在外部用OPENSSL_free释放
		INT			ExportPriExponnent(CHAR **pHexPriExponent,INT *priExponentLen);

	public:
		/************************************************************************/
		/* 从标准的算法导入密钥                                                                      
		/* 公钥模数 + 公钥指数 ----> 公钥
		/* 公钥模数 + 公钥指数 + 私钥指数----> 私钥
		/************************************************************************/
		// 导入私钥
		INT			ImportPrivateKey(CONST CHAR* pubHexExponent,CONST CHAR *hexModule,CONST CHAR *priHexExponent);
		// 导入公钥
		INT			ImportPublicKey(CONST CHAR* pubHexExponent,CONST CHAR *hexModule);

	public:
		// 通过明文长度获取加密后的数据存储长度
		UINT		GetEncryptedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// 通过秘闻长度获取解密后的数据长度
		UINT		GetDecryptedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// 获取加密锁的状态
		inline	INT	GetLockStatus(){ return m_cryptoState;};
		// 获取rsa加密，解密位数
		inline	USHORT		GetRstEncryptoBits(){return m_bits;};

	public:
		// 加密解密,要支持拆分功能
		LONG		PubEncrypt(CONST UCHAR *pSource,CONST ULONG sourceLen,UCHAR *pDest,CONST ULONG destLen,ULONG* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// 每次解密后的字符串要进行将后面的空格截断处理
		LONG		PriDecrypt(CONST UCHAR *pSource,CONST ULONG sourceLen,UCHAR *pDest,CONST ULONG destLen,ULONG* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		
	private:
		// 验证加密解密数据包长度是否满足要求,在PubEncrypt，PriDecrypt内部调用
		LONG		CheckEncryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST UINT bits = conCryptoBit_1024,CONST ULONG padding = RSA_PKCS1_PADDING);
		LONG		CheckDecryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST UINT bits = conCryptoBit_1024,CONST ULONG padding = RSA_PKCS1_PADDING);

	private:
		USHORT		m_bits;						// 多少位加密默认为1024
		USHORT		m_fLen;						// rsa对象长度,每次加密的时候带入的，加密解密的时候与pading方式有关
		RSA*		m_rsaObj;					// 加密对象		
		USHORT		m_rsaLen;					// ras长度，RSA_Size()
		BIGNUM*		m_bne;						// pub Exponent
		CryptoStatus m_cryptoState;				// 锁的状态
	};

	/**************************************************************************************************
	  Object      : DesPlainDataAdapter
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/24 18:26
	  Description : Des加密，openssl 要能够与EliteIV进行对接，必须做到下面2点
					1. 明文长度必须为8字节的整数倍
					2. 如果明文数据不够8字节，那么需要进行填充，从指定的数据中填充，填充数据为固定的值
					注意：为保证通用性，DES相关加密解密类，下面类需要从该类派生
					IOpensslUtility::DesCryptoHelper
					IOpensslUtility::DesCryptoRandomKeyHelper
					SenseEliteIVUtility::EliteIVHelper
	**************************************************************************************************/
	class DesPlainDataAdapter
	{
	public:
		DesPlainDataAdapter();
		virtual ~DesPlainDataAdapter();

	protected:
		// 根据原数据长度获取要存储需要的空间
		DATA_LEN_LEN	GetCryptoSourceLen(CONST DATA_LEN_LEN inSourceLen);
		// 将长度不是8字节整数倍的数据包进行填充
		INT				PackCryptoSource(CONST UCHAR *inSource,CONST DATA_LEN_LEN inSourceLen,UCHAR *outSource,CONST DATA_LEN_LEN cryptoLen);
	};

	/**************************************************************************************************
	  Object      : DesCrypto
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:16
	  Description : DES 加密解密功能，目前只是暂时支持ecb模式，后续添加ecb3和其他的
	**************************************************************************************************/
	class DesCrypto
	{
	public:
		DesCrypto();
		virtual	~DesCrypto();
	public:
		INT		SetKey(CONST UCHAR *pkey,CONST INT keyLen = 8);
		inline	INT	GetLockStatus(){ return m_cryptoState;};
		// 加密解密
		LONG	EcbEncrypt(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		LONG	EcbDecrypt(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// 获取加密后的长度和解密后的长度
		LONG	GetEncryptLenFromSourceLen(CONST UINT sourceLen,CONST INT type = ECB);
		LONG	GetDecryptLenFromSourceLen(CONST UINT sourceLen,CONST INT type = ECB);

	private:
		DES_key_schedule	m_schedule;
		DES_cblock			m_cblock;					
		CryptoStatus		m_cryptoState;				// 锁的状态
		// 加密解密类型
		enum DesCryptType
		{
			ECB,
			ECB3,
		};
		// 验证加密解密数据包长度是否满足要求
		LONG	CheckEncryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST INT type = ECB);
		LONG	CheckDecryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST INT type = ECB);
	};

	/**************************************************************************************************
	  Object      : Base64
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 10:40
	  Description : base64转换类，将二进制转换成base64编码字符串，将base64编码字符串转化成2进制
					根据base64字符串获取存储转换后二进制的字符串长度，根据二进制长度获取存储转换后
					base64的位数
	**************************************************************************************************/
	class Base64
	{
	public:
		Base64(void);
		virtual ~Base64(void);
	private:
		// typedef struct evp_Encode_Ctx_st
		EVP_ENCODE_CTX	m_encCtx,m_decCtx;				
		// bytes ---> base64 字符长度校验长度是否合理,存在3种情况
		INT	CheckB64BuffLen(CONST UINT	sourceLen,CONST UINT destLen);
		// base64 ---> bytes 字符长度校验是否合理
		INT	CheckBytesBuffLen(CONST UINT	sourceLen,CONST UINT destLen);

	public:
		// 通过base数，获取需要存储的bytes的最小字节数
		UINT GetBytesNumFromB64(CONST UINT B64Len);
		// 通过bytes字节数，获取需要存储的base64的最小字节数
		UINT GetB64NumFromBytes(CONST UINT BytLen);
		// 将base64转换成bytes
		INT	B64ToBytes(CONST CHAR*	pSource,CONST UINT sourceLen,UCHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// 将bytes转换成base64
		INT	BytesToB64(CONST UCHAR* pSource,CONST UINT sourceLen,CHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
	};

	/**************************************************************************************************
	  Object      : RsaCryptoHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:18
	  Description : Rsa加密解密辅助类
					RSA加密(公钥加密，私钥解密)，解密的对外一个加密解密辅助类
					加密过程：明文--->长度 + 二进制密文--->BASE64编码
					解密过程：BASE--->长度 + 二进制密文--->二进制明文
					==☆需要增加数据长度在密钥头部
	**************************************************************************************************/
	class RsaCryptoHelper
	{
	public:
		RsaCryptoHelper();
		virtual ~RsaCryptoHelper();

	public: // 加密，解密过程
		// 加密过程：明文--->明文长度+二进制密文--->BASE64编码
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// 解密过程：BASE--->明文长度+二进制密文--->二进制明文
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);

		// 通过明文长度获取加密后的数据存储长度
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// 通过密文长度获取解密后的数据长度
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);

	public:
		// 返回外部调用接口,并可以导入密钥，或者导出密钥等，也可以直接调用该接口加密，解密
		inline	RsaCrypto*	GetRsaCrypto(){return &m_oRsaCrypto;}

	private:
		RsaCrypto	m_oRsaCrypto;			// rsa object
		Base64		m_oBase64;				// base64 object
	};

	/**************************************************************************************************
	  Object      : DesCryptoHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:19
	  Description : DES加密解密辅助类
					DES加密ecb，解密的对外一个加密解密辅助类
					加密过程：明文--->二进制密文--->BASE64编码
					解密过程：BASE--->二进制密文--->二进制明文
					==☆需要增加数据长度在密钥头部
	**************************************************************************************************/
	class DesCryptoHelper:public DesPlainDataAdapter
	{
	public:
		DesCryptoHelper();
		virtual ~DesCryptoHelper();

	public: // 加密，解密过程
		// 加密过程：明文--->长度+二进制密文--->BASE64编码
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// 解密过程：BASE--->长度+二进制密文--->二进制明文
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);

		// 通过明文长度获取加密后的数据存储长度
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen);
		// 通过秘闻长度获取解密后的数据长度
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen);

	public:
		// 返回外部调用接口,并可以导入密钥，或者导出密钥等，也可以直接调用该接口加密，解密
		inline	DesCrypto*	GetDesCrypto(){return &m_oDesCrypto;}

	private:
		DesCrypto	m_oDesCrypto;				// des object 
		Base64		m_oBase64;					// base64 object
	};

	/**************************************************************************************************
	  Object      : DesCryptoRandomKeyHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/20 9:10
	  Description :  DES加密解密辅助类(支持动态密钥的产生，根据数据源，动态的生成密钥的位置，
					从8K密钥中导出密钥进行加密解密)，DES加密ecb，解密的对外一个加密解密辅助类
	  加密过程：明文--->8K密钥文件获取密钥--->二进制密文--->密钥索引+明文长度+二进制密文--->BASE64编码
	  解密过程：BASE64编码--->密钥索引+明文长度+二进制密文--->8K密钥文件获取密钥 + 源数据长度 ---> 明文
	**************************************************************************************************/
	class DesCryptoRandomKeyHelper:public DesPlainDataAdapter
	{
	public:
		DesCryptoRandomKeyHelper();
		virtual ~DesCryptoRandomKeyHelper();

	public: 
		// 加密过程：明文--->8K密钥文件获取密钥--->二进制密文--->密钥索引+明文长度+二进制密文--->BASE64编码
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// 解密过程：BASE64编码--->密钥索引+明文长度+二进制密文--->8K密钥文件获取密钥 + 源数据长度 ---> 明文
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);

		// 通过明文长度获取加密后的数据存储长度
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen);
		// 通过秘闻长度获取解密后的数据长度
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen);

	public:
		// 返回外部调用接口,并可以导入密钥，或者导出密钥等，也可以直接调用该接口加密，解密
		inline	DesCrypto*	GetDesCrypto(){return &m_oDesCrypto;}

	public:
		// dump keys source file data to m_pKeySourceFile,dump keys size 1024 ,file size 8192
		INT		DumpKeysSourceFile(CONST	UCHAR *pKeySource,CONST UINT keySourceLen);
		// find key from the keys source file with the source and source len,encrypt using
		INT		FindKeyFromKeysSourceFile(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *pKey);
		// find key from the keys source file with keyIndex,decrypt using
		INT		FindKeyFromKeysSourceFile(CONST USHORT	keyIndex,UCHAR *pKey);

	private:
		// key index get from data source and len 
		USHORT	GetKeyIndexFromPlainText(CONST UCHAR *pSource,CONST UINT sourceLen);

	private:
		DesCrypto	m_oDesCrypto;				// des object 
		Base64		m_oBase64;					// base64 object
		UCHAR		*m_pKeySourceFile;			// keys source file ,size 8KB

	private:
		UCHAR		m_mdHash[conMd5SumLen];		// md5 hash len for each encrypt

	};
	//#pragma pack()
}

#endif
