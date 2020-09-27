#include "AlarmMsg.h"

const char* FunGetAlarmLevel(AlarmLevel& al)
{
    switch(al)
    {
    case  DUMP_LOG:
    	return "导出正常处理日志";
	case  MLOAD_LOG:
		return "整理正常处理日志";
	case  LOAD_LOG:
		return "装入正常处理日志";
	case  DUMP_WARNING:
		return "导出警告";
	case  MLOAD_WARNING:
		return "整理警告";
	case  LOAD_WARNING:
		return "装入警告";
	case  DUMP_ERROR:
		return "导出错误";
	case  MLOAD_ERROR:
		return "整理错误";
	case  LOAD_ERROR:
		return "装入错误";
    default:
        break;  	
    }
    return NULL;
}


const  char* FunGetAlarmStatus(AlarmStatus& as)
{
	switch(as)
	{
	case AlarmStatus_CLEAR: 
		return "清除告警";
    case AlarmStatus_OCCUR: 
    	return "产生告警";
	case AlarmStatus_HEARTBEAT: 
		return "HEARTBEAT";
	default:
		break;    
	}
	return NULL;
};
/**************************************************************************************************
  Function    : DoPackMessage()
  DateTime    : 2013/1/14 15:49
  Description : 文本告警协议的打包
  Param       : null
  Return      : success : 0
                error : -1
  Note        : 湖北移动告警格式
  example:
  ------------------------------------------------------------------------------------------
  告警信息:                                           恢复信息:                                
  <BeginAlarm>                                        <BeginAlarm>                             
  告警流水号:1699759                                  告警流水号:1699759                       
  网元名称:XN785_狼荷M-GSM900M基站                    网元名称:XN785_狼荷M-GSM900M基站         
  定位信息:                                           定位信息:                                
  地区:咸宁                                           地区:咸宁                                
  网元类型:机房                                       网元类型:机房                            
  告警标题:门禁                                       告警标题:门禁                            
  可能原因:某个门碰探头探测到基站门非法打开           可能原因:某个门碰探头探测到基站门非法打开
  告警状态:活动告警                                   告警状态:清除告警                        
  告警类别:动力门禁                                   告警类别:动力门禁                        
  告警级别:二级告警                                   网管告警级别:二级告警                    
  告警发生时间:2013-01-08 15:04:23                    告警发生时间:2013-01-08 15:04:23         
  <EndAlarm>                                          告警恢复时间:2013-01-08 15:04:22         
                                                      <EndAlarm>                      
  -----------------------------------------------------------------------------------------
**************************************************************************************************/
#define  alarm_occur_format "%s\n\
告警流水号:%d\n\
网元名称:\n\
定位信息:\n\
地区:\n\
网元类型:\n\
告警标题:%s\n\
可能原因:%s\n\
告警状态:%s\n\
告警类别:%d\n\
告警级别:%s\n\
告警发生时间:%s\n\
%s"    

#define  alarm_clear_format "%s\n\
告警流水号:%d\n\
网元名称:\n\
定位信息:\n\
地区:\n\
网元类型:\n\
告警标题:%s\n\
可能原因:%s\n\
告警状态:%s\n\
告警类别:%d\n\
告警级别:%s\n\
告警发生时间:%s\n\
告警恢复时间:%s\n\
%s"

#define  alarm_heartbeat_format "%s\n%s %s\n%s"

int TextAlarmInfo::DoPackMessage()
{
    memset(&m_stmsgInfo,0,sizeof(StruMsgInfo));
    if (AlarmStatus_OCCUR == m_alarmStatus)
    {
        sprintf((char*)m_stmsgInfo.msg_payload,alarm_occur_format,
            m_PackHeader,
            m_alarmId,
            m_alarmTitle,
            m_alarmReason,
            FunGetAlarmStatus(m_alarmStatus),
            m_alarmType,
            FunGetAlarmLevel(m_alarmLevel),
            m_alarmOccurTime,
            m_PackTail);
    } 
    else if(AlarmStatus_CLEAR == m_alarmStatus)
    {
		sprintf((char*)m_stmsgInfo.msg_payload,alarm_clear_format,
            m_PackHeader,
            m_alarmId,
            m_alarmTitle,
            m_alarmReason,
            FunGetAlarmStatus(m_alarmStatus),
            m_alarmType,
            FunGetAlarmLevel(m_alarmLevel),
            m_alarmOccurTime,
			m_alarmClearTime,
            m_PackTail);
	}
    else if(AlarmStatus_HEARTBEAT == m_alarmStatus)
	{
		char current_dt[DATETIME_LEN] = {0};
        GetCurrentTime(current_dt);

        sprintf((char*)m_stmsgInfo.msg_payload,alarm_heartbeat_format,
			m_PackHeader,
			FunGetAlarmStatus(m_alarmStatus),
			current_dt,
			m_PackTail);
	}
	else
	{
		return -1;
	}	
	m_stmsgInfo.msg_payload_len = strlen((char*)m_stmsgInfo.msg_payload);
	return 0;
}


// 文本协议，湖北移动[亿阳信通]定义的协议
int TextAlarmInfo::DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen)
{ 
	// 暂时没有返回的协议
    lgprintf("TextAlarmInfo::DoUnPackMessage 暂未支持解析该协议.数据[%s],长度[%d].",(char*)rcvMsg,rcvMsgLen);
    return -1;
}

//=======================================================================================
// AlarmMsgQueue
AlarmMsgQueue::AlarmMsgQueue()
{
	pthread_mutex_init(&m_lock,NULL);
	m_msgVector.clear();
    m_currentIndex = -1;
}

AlarmMsgQueue::~AlarmMsgQueue()
{
    pthread_mutex_lock(&m_lock);
    for (int i=0;i<m_msgVector.size();i++)
    {
        AlarmMsgInfo *pObj = NULL;
        pObj = m_msgVector[i];
        if(NULL != pObj)
		{
            delete pObj;
            pObj = NULL;
        }
    }
	m_msgVector.clear();
	pthread_mutex_unlock(&m_lock);
    pthread_mutex_destroy(&m_lock);
    m_currentIndex = -1;
}

// 添加告警前先判断告警记录是否存在内存中
int AlarmMsgQueue::AddAlarmMsg(AlarmMsgInfo* msg)
{
    if(NULL != msg)
	{
        AutoLock alk(&m_lock);
        m_msgVector.push_back(msg);
        return 0;
	}
	return -1;
}

// 判断告警存储的条件：告警id相同，告警状态相同，因为:同一条告警存在2个状态[告警产生，告警恢复]
bool AlarmMsgQueue::FindAlarmMsg(const unsigned long alarmId,const AlarmStatus as)
{
    AutoLock alk(&m_lock);
    for (int i=0;i<m_msgVector.size();i++)
    {
        if ((m_msgVector[i] != NULL ) && (m_msgVector[i]->GetAlarmID() == alarmId && m_msgVector[i]->GetAlarmStatus() == as))
        {
			return true;
        }
    }

	return false;
}

/**************************************************************************************************
  Function    : GetAlarmMsg(),RetAlarmMsg(AlarmMsgInfo* msg)
  DateTime    : 2013/1/15 10:07
  Description : 这两个函数必须成对出现使用
**************************************************************************************************/
AlarmMsgInfo*  AlarmMsgQueue::GetAlarmMsg()
{
    AutoLock alk(&m_lock);
    if(m_msgVector.size() >0 && (m_currentIndex > m_msgVector.size() -1))
	{
        m_currentIndex = 0;
	}
    AlarmMsgInfo* pObj = NULL;
    int   indexAdd = 0;
	int   i = 0;

    // 从当前位置往后找	
    for (i=m_currentIndex;i<m_msgVector.size();i++)
    {
        indexAdd++;
        if(m_msgVector[i] != NULL && m_msgVector[i]->GetRef() ==0 && m_msgVector[i]->GetAlarmStatus() != AlarmStatus_Delete)
		{
             pObj = (AlarmMsgInfo*)m_msgVector[i];
			 break;
        }
    }
	m_currentIndex+= indexAdd;
	if(m_currentIndex > m_msgVector.size() -1)
	{
		m_currentIndex = 0;
	}
	if (NULL != pObj)
	{
		pObj->AddRef();
		return pObj;
	}

	// 从头部开始往后找
    indexAdd=0;
	for (i=0;i<=m_currentIndex;i++)
	{
        indexAdd++;
		if(m_msgVector[i] != NULL && m_msgVector[i]->GetRef() ==0 && m_msgVector[i]->GetAlarmStatus() != AlarmStatus_Delete)
		{
			pObj = (AlarmMsgInfo*)m_msgVector[i];
	        break;
		}
	}
	m_currentIndex+= indexAdd;
	if(m_currentIndex > m_msgVector.size() -1)
	{
		m_currentIndex = 0;
	}
	if (NULL != pObj)
	{
		pObj->AddRef();
		return pObj;
	}

	// 没有找到满足条件的返回NULL
	return NULL;
}
int AlarmMsgQueue::RetAlarmMsg(AlarmMsgInfo* msg)       // 告警信息使用完成后将其返回队列中 
{
    if(NULL == msg)
	{
        return -1;
	}
	msg->SubRef();
    return 0;
}

// 删除空的对象
int AlarmMsgQueue::DeleteNullAlarmMsg()
{
	AutoLock alk(&m_lock);
	AlarmMsgVector::iterator iter;
	int deleteNum = 0;
retry:
	for (iter = m_msgVector.begin();iter!=m_msgVector.end();iter++)
	{
		AlarmMsgInfo* obj = *iter;
		if ( obj != NULL && obj->GetAlarmStatus() == AlarmStatus_Delete)
		{
			m_msgVector.erase(iter);
			delete obj;
			obj = NULL;
			deleteNum++;
			goto retry;
		}		
	}	
	return deleteNum;
}

int  AlarmMsgQueue::DeleteAlarmMsg(AlarmMsgInfo* msg)
{	
	AutoLock alk(&m_lock);
	AlarmMsgVector::iterator iter;
	for (iter = m_msgVector.begin();iter!=m_msgVector.end();iter++)
	{
		AlarmMsgInfo* obj = *iter;
		if((obj == NULL) && (obj->GetAlarmID() == msg->GetAlarmID() && obj->GetAlarmStatus() == msg->GetAlarmStatus()))
		{
			m_msgVector.erase(iter);
			delete obj;
			obj = NULL;
		}
	}	
}

unsigned long AlarmMsgQueue::Size()
{
    AutoLock alk(&m_lock);
    return m_msgVector.size();
}
