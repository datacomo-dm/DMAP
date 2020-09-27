#include "Utility.h"

//================================================================================
// Config File 类
ConfigFile::~ConfigFile() {
   m_options.clear();
}

#define PARAM_MAXLEN 8000
ConfigFile::ConfigFile(const char *ctlfile) 
{
	char cfilename[300];
	char line[PARAM_MAXLEN];
	int len=0;
	char *words[3];
	char *ptext;
	char section[300],key[300],value[3000];
	strcpy(cfilename,getenv("DATAMERGER_HOME"));
	strcat(cfilename,"/ctl/");
	strcat(cfilename,ctlfile);
	FILE *fp=fopen(cfilename,"rt");
	if(fp==NULL) ThrowWith("Open control file '%s' error!",cfilename);
	section[0]=0;
	int linect=0;
	while(fgets(line,PARAM_MAXLEN,fp)!=NULL) {
		linect++;
		//remove all blank
		len=strlen(line);
		bool inner=false;
		for(int i=0;i<len;i++) {
			if(line[i]=='\n' || line[i]=='\r' ) {line[i]=0;len=i;break;}
			if((line[i]=='\'' || line[i]=='\"') && line[i-1]=='\\') {
				memmove(line+i-1,line+i,len-i+1);len--;
			}
			if((line[i]=='\'' || line[i]=='\"') && line[i-1]!='\\') {
				memmove(line+i,line+i+1,len-i+1);len--;i--;
				inner=!inner;
			}
			if((line[i]==' '|| line[i]=='\t')&& !inner) {
				memmove(line+i,line+i+1,len-i+1);len--;i--;
			}

			// Begin: add 20121114 DM-182 移动位置后，在判断末尾是否存在\n,\r
			if(line[i]=='\n' || line[i]=='\r' ) {line[i]=0;len=i;break;}
			// End: add 20121114 DM-182 移动位置后，在判断末尾是否存在\n,\r

		}
		//empty line
		len=strlen(line);
		if(len<1) continue;
		if(*line=='#') continue;
		// get a section line
		if(*line=='[') {
			char *ptrim=line+len-1;
			ptext=line+1;
			while(ptrim>ptext && *ptrim!=']') ptrim--;
			if(ptrim==ptext) ThrowWith("Get a invalid section on line %d of control file '%s'",
				linect,cfilename); 
			*ptrim=0;
			ptrim=ptext;
			while(*ptrim) {*ptrim++=tolower(*ptrim);}
			strcpy(section,ptext);
			continue;
		}
		if(split(line,words,'=')==-1)
			ThrowWith("Get a invalid item on line %d of control file '%s'",
			linect,cfilename);
		char *trans=words[0];
		while(*trans) {*trans++=tolower(*trans);}
		std::string key(section),value(words[1]);
		key+=":";
		key+=words[0];
		m_options.insert(std::map<std::string,std::string>::value_type(key,value));
	}
}

const char *ConfigFile::getOption(const char *keyval,const char *defaults) {
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= m_options.find(key); 
	if(it == m_options.end()) {
		return defaults;
	}
	else {
		return it->second.c_str();
	} 
}

const char ConfigFile::getOption(const char *keyval,const char defaults) {
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= m_options.find(key); 
	if(it == m_options.end()) {
		return defaults;
	}
	else {
		return it->second.c_str()[0];
	} 
}

int ConfigFile::getOption(const char *keyval,int defaults){
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= m_options.find(key); 
	if(it == m_options.end()) {
		return defaults;
	}
	else {
		return atoi(it->second.c_str());
	} 
}

double ConfigFile::getOption(const char *keyval,double defaults){
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= m_options.find(key); 
	if(it == m_options.end()) {
		return defaults;
	}
	else {
		return atof(it->second.c_str());
	} 
}
int ConfigFile::split(char *text,char **tokens,char delimter) {
	int wordct=0;
	char *psep=strchr(text,delimter);
	if(psep==NULL) return -1;
	*psep++=0;
	tokens[0]=text;tokens[1]=psep;
	return 1;
}

//===========================================================================================
// SockTcpClient 类
SockTcpClient::SockTcpClient(void(*pCallback_ProcNetEvent)(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam),
	const char* remoteServerIp,const unsigned short remoteServerPort,void* pUserParam,const bool isAutoReconnect)
{
	m_localSock = 0;
    m_localSock = socket(AF_INET, SOCK_STREAM, 0);
	if( m_localSock < 0) 
	{
		ThrowWith("create sock(AF_INET, SOCK_STREAM, 0) return < = 0");
	}
    
	m_isAutoReconnect = isAutoReconnect; // 自动重连

	// remote info 
	strcpy(m_serverIP,remoteServerIp);
    m_serverPort = remoteServerPort;
	memset(&m_remote,0x0,sizeof(m_remote));
	m_remote.sin_family = AF_INET;
	m_remote.sin_addr.s_addr = inet_addr(m_serverIP);
	m_remote.sin_port = htons(m_serverPort);	
    m_isRuning = false;
	// set call back
	m_cbProcNetEvent = NULL;

	if(NULL != pCallback_ProcNetEvent)
    {
		m_cbProcNetEvent = pCallback_ProcNetEvent;
		m_pUserParam = pUserParam;
	}
}

SockTcpClient::~SockTcpClient()
{
    if (m_localSock > 0)
    {
        close(m_localSock);
	}   
	m_isRuning = false;
}

// 接收服务端消息线程,客户端无需使用epoll即可
#define NO_FLAGS_SET 0
#define	MAX_MESSAGE_LEN  8*1024           // 上送消息最大支持8K
#define SOCKET_ERROR            (-1)
void*  SockTcpClient::OnRecvMsgProc(void* param)
{
    SockTcpClient *pObj = (SockTcpClient*)param;
	if(NULL == pObj)
	{
		lgprintf("OnRecvMsgProc param is NULL .");
		return NULL;
	}
    sleep(1);
	unsigned char buffer[MAX_MESSAGE_LEN] = {0};
	int numrcv;
	while(pObj->m_isRuning)
	{
	    int clientSocket=(int)pObj->m_localSock;
		memset(buffer,0,MAX_MESSAGE_LEN);
		numrcv=recv(clientSocket,buffer, MAX_MESSAGE_LEN, NO_FLAGS_SET);
		if ((numrcv == 0) || (numrcv == SOCKET_ERROR))
		{			
			pObj->m_isConnected = false;
			close(pObj->m_localSock);
			sprintf((char*)buffer,"server[%s:%d] disconnect .",pObj->m_serverIP,pObj->m_serverPort);
			pObj->GetCbFun()(Server_Disconnect,buffer,strlen((char*)buffer),pObj->m_pUserParam);
		}
        else
        {
		    // 收到数据回调通知上层
		    pObj->GetCbFun()(Server_DataArrived,buffer,numrcv,pObj->m_pUserParam);
		}
	}

	if (pObj->m_localSock >0)
	{
		close(pObj->m_localSock);
	}
    return NULL;
}

void* SockTcpClient::OnReconnect(void *param)
{
    SockTcpClient *pObj = (SockTcpClient*)param;
	if(NULL == pObj)
	{
		lgprintf("OnRecvMsgProc param is NULL .");
		return NULL;
	}	
    sleep(1);
	while(pObj->m_isRuning)
	{
		if(!pObj->m_isConnected)
		{
			pObj->DoConnect();
		}
		sleep(10);	
	}
    lgprintf("退出重连线程.");
    return NULL;   	 	
}

int  SockTcpClient::DoConnect()
{
	if (m_isConnected)
	{
        lgprintf("已经连接告警服务器[%s:%d],无需在进行连接.",m_serverIP,m_serverPort);
		return 0;
	}
	
	if(m_localSock >0)
	{
		close(m_localSock);        
    }
    m_localSock = socket(AF_INET, SOCK_STREAM, 0);
	// 连接服务端
	if(connect(m_localSock, (struct sockaddr *)&m_remote, sizeof(m_remote)) < 0) 
	{
		lgprintf("连接告警服务器[%s:%d]失败.",m_serverIP,m_serverPort);
		return -1;
	}
	lgprintf("连接告警服务器[%s:%d]成功.",m_serverIP,m_serverPort);

	m_isConnected = true;
	return 0;
}


void SockTcpClient::CloseConnect()
{
    if(m_localSock>0)
	{
        close(m_localSock);
		m_localSock = 0;
	}
	m_isConnected = false;
}

// 启动tcp客户端，连接上服务端，接收数据线程和并处理网络故障
int SockTcpClient::Start()
{
    if(m_isRuning)
    	return -1;
 
	if(DoConnect()<0)
	    return -1;
    
    if(pthread_create(&m_RecvMsgThread,NULL,OnRecvMsgProc,(void*)this)!=0)
    {
		lgprintf("无法启动数据接收线程，启动失败.\n");
		return -1;    	
    }
    
    if(m_isAutoReconnect)
    {
       if(pthread_create(&m_RecvMsgThread,NULL,OnReconnect,(void*)this)!=0)
       {
	   	   lgprintf("无法启动tcp重新连接线程,启动失败.\n");
	   	   return -1;    	
       }    
    }	
     
    m_isRuning = true;
    return 0;
}

// 发送数据，同步调用
int SockTcpClient::SendMsg(const unsigned char* buff,const unsigned long buffLen)
{
    if (m_localSock>0 && m_isConnected)
    {
        // 发送失败
        if(send(m_localSock,buff,buffLen,0) < buffLen)
		{ 
			lgprintf("向告警服务器[%s:%d]发送数据失败.",m_serverIP,m_serverPort);
		    return -1;
        }
    }
	else
	{
        lgprintf("服务端未连接发送数据失败.");
		return -1;
	}

	return 0;
}
