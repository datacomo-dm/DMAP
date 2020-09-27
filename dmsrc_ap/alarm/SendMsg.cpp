#include "SendMsg.h"

//===============================================================================
// SendMessageTcp 类的实现文件
SendMessageTcp::SendMessageTcp()
{
	m_ProcessRecvMsg = NULL;
	m_sockTcpClient = NULL;
}

SendMessageTcp::~SendMessageTcp()
{
	if(NULL != m_sockTcpClient)
	{
		m_sockTcpClient->Stop();
		delete m_sockTcpClient;
		m_sockTcpClient = NULL;
	}
}

// SockTcpClient 类的处理回调函数
void  SendMessageTcp::Callback_ProcNetEvent(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam)
{
	SendMessageTcp* pObj = NULL;
	pObj = (SendMessageTcp*)pUserParam;
	if (NULL == pObj)
	{
		lgprintf("SendMessageTcp::Callback_ProcNetEvent error.");
	}

	switch(event)
	{
	case Server_Disconnect:                 // 服务端断开连接
		{
			lgprintf("检测到服务端断开连接，进行重新连接.");
			if (pObj->m_sockTcpClient->IsAutoConnect())
			{
				pObj->m_sockTcpClient->DoConnect();
			}

			// 上层业务处理
			if (pObj->m_ProcessRecvMsg != NULL)
			{
				pObj->m_ProcessRecvMsg->ProcRecvMsg(rcvMsg,rcvMsgLen);
			}
		}
		break;
	case Server_DataArrived:               // 接收服务端数据    
		{
			// 上层业务处理
			if (pObj->m_ProcessRecvMsg != NULL)
			{
				pObj->m_ProcessRecvMsg->ProcRecvMsg(rcvMsg,rcvMsgLen);
			}
		}
		break;
	}

}

int  SendMessageTcp::Init(void* param)
{
	if(m_hasInited)
	{
		lgprintf("SendMessageTcp::Init 已经初始化过,不能重复执行.");
		return 0;
	}
	TcpServerItem* pObj = (TcpServerItem*)param;
	if(pObj == NULL)
	{
		return -1;
	}

	if (NULL == m_sockTcpClient)
	{
		m_sockTcpClient = new SockTcpClient(Callback_ProcNetEvent,pObj->_ip,pObj->_port,(void*)this,true);
	}

	if(NULL != m_sockTcpClient)
	{
		if(m_sockTcpClient->Start() < 0)
		{
			delete m_sockTcpClient;
			m_sockTcpClient = NULL;
			return -1;
		}
	}

	m_hasInited = true;
}
int  SendMessageTcp::SendMsg(AlarmMsgInfo* alarmMsgObj)
{
	if(NULL == alarmMsgObj || m_sockTcpClient == NULL || !m_hasInited )
	{
		lgprintf("对象参数不完整，无法发送告警信息.");
		return -1;
	}
    return m_sockTcpClient->SendMsg(alarmMsgObj->GetMsgInfo().msg_payload,alarmMsgObj->GetMsgInfo().msg_payload_len);
}

int  SendMessageTcp::UnInit()
{
	if(m_hasInited)    
	{
		if (NULL != m_sockTcpClient)
		{
			m_sockTcpClient->Stop();    
		}
	}
	return 0;
}


//===========================================================================
// SendMessageDB 类实现
SendMessageDB::SendMessageDB()
{

}
SendMessageDB::~SendMessageDB()
{

}

int     SendMessageDB::Init(void* param)
{
    lgprintf("SendMessageDB 暂时未实现.");
	return -1;
}
int     SendMessageDB::SendMsg(AlarmMsgInfo* alarmMsgObj)
{
    lgprintf("SendMessageDB 暂时未实现.");
    return -1;
}
int     SendMessageDB::UnInit()
{
    lgprintf("SendMessageDB 暂时未实现.");
    return -1;
}
