#ifndef UTILITY_DEF_H
#define UTILITY_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : UTILITY.H    
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/14 22:39	
  Description : 工具头文件
**************************************************************************************************/
#include "dt_common.h"
#include "AutoHandle.h"
#include <map>
#include <string>

//---- socket ---------
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/resource.h>
//#include <linux/tcp.h>
#include <linux/socket.h>
#include <arpa/inet.h>		// inet_addr 
//---- socket ---------

/**************************************************************************************************
  Object      : ConfigFile
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:06
  Description : 控制文件类
**************************************************************************************************/
class ConfigFile
{
protected:
	typedef std::map<std::string,std::string>   STRING_MAP;
	STRING_MAP       m_options;
	int split(char *text,char **tokens,char delimter);
public :
	virtual ~ConfigFile() ;
	ConfigFile(const char *ctlfile);

public:
	const char*      getOption(const char *keyval,const char *defaults) ;
	const char       getOption(const char *keyval,const char defaults) ;
	int              getOption(const char *keyval,int defaults) ;
	double           getOption(const char *keyval,double defaults);
};

/**************************************************************************************************
  Object      : SockTcpClient
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:07
  Description : tcp socket  客户端类
                同步发送数据，异步接受数据回调上层通知
**************************************************************************************************/
class ThreadList;            // common thread
#define RECV_BUFF_LEN        16*1024     
#define MAX_EVENTS 200		// epoll_create(size) 的size 与 event对象数

// 接收数据的回调函数
enum NetEvent
{
    Server_Disconnect,             // 服务端断开连接
    Server_DataArrived,            // 接收服务端数据 
};
typedef void(*pCallback_ProcNetEvent)(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam);
class SockTcpClient
{
protected:
    // 接受服务端消息用的，接受消息及网络事件后回调通知上层
    pthread_t          m_RecvMsgThread;
    pthread_t          m_ReconnectThread;
	pCallback_ProcNetEvent  m_cbProcNetEvent;                  // 接收消息回调
    void*              m_pUserParam;                           // 上层应用自带参数

	int	               m_localSock;			                   // socket
	struct sockaddr_in m_remote;                               // server remote
	bool               m_isConnected;                          // 是否连接
    bool               m_isAutoReconnect;                      // 是否自动重连
    char               m_serverIP[16];                         // Server IP
    unsigned short     m_serverPort;                           // Server Port
    bool               m_isRuning;                             // 正在运行

public:
	inline int&       GetLocalSock(){return m_localSock;};
	inline bool       IsAutoConnect(){return m_isAutoReconnect;};
    int               DoConnect();
    void              CloseConnect();
	inline pCallback_ProcNetEvent GetCbFun(){return m_cbProcNetEvent;};

public:
    // 发送消息是同步处理
    int              Start();
    inline  void     Stop(){m_isRuning = false;m_isConnected = false;};
    int              SendMsg(const unsigned char* buff,const unsigned long buffLen);

protected:
    static void*     OnRecvMsgProc(void* param);     // 接收数据线程处理函数
    static void*     OnReconnect(void *param);       // 自动重连线程

public:
    SockTcpClient(void(*pCallback_ProcNetEvent)(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam),
		const char* remoteServerIp,const unsigned short remoteServerPort,void* pUserParam=NULL,const bool isAutoReconnect = true);

    virtual ~SockTcpClient();
};
#endif
