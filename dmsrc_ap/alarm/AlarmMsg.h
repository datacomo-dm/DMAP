#ifndef ALARM_MSG_DEF_H
#define	ALARM_MSG_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO(http://www.datacomo.com/)
  File name   : ALARMMSG.H      
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/13 9:45
  Description : define alarm message info class
**************************************************************************************************/
#include "dt_common.h"
#include "dt_svrlib.h"
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <pthread.h>

#define SEND_MAX_TIMES        5                // 最大上送次数
#define SEND_DEFAULT_INTERVAL 5*60             // 上送时间间隔  
#define SEND_MIN_INTERVAL     10               // 最小上送时间间隔
#define MAX_MESSAGE_LEN       8*1024           // 上送消息最大支持8K
#define DATETIME_LEN          20               // yyyy-mm-dd hh:mm:ss


enum AlarmStatus
{
    AlarmStatus_CLEAR,         // 清除告警
    AlarmStatus_OCCUR,         // 告警开始
    AlarmStatus_HEARTBEAT,     // 心跳命令，不存储数据库，发送记录存储日志表
    AlarmStatus_Delete,        // 内存中应该被删除的告警信息
};
typedef AlarmEventType AlarmType;             // dt_svrlib.h 中定义告警类型
typedef AlarmEventLevel AlarmLevel;           // dt_svrlib.h 中定义告警级别

enum  ProcStatus  // 告警处理状态，对应dp_alarm.procstatus
{
    ProcStatus_Alarm_Wait=0,            // 0:等待处理产生告警
    ProcStatus_Alarm_Doing,             // 1:产生告警处理中
    ProcStatus_Alarm_Done,              // 2:产生告警处理完成
	ProcStatus_Alarm_Error,             // 3:产生告警处理出错
    ProcStatus_Clear_Alarm_Wait=10,     // 10:等待处理恢复告警,
	ProcStatus_Clear_Alarm_Doing,       // 11:产生告警处理完成
	ProcStatus_Clear_Alarm_Done,        // 12:产生告警处理出错
    ProcStatus_Clear_Alarm_Error,       // 13:恢复告警处理出错
};

enum  SendType // code_type: 25 code_value : 告警信息发送接口类型
{
   SEND_SOCKET_TCP = 100,   // 100-socket tcp(亿阳信通-湖北移动) 
   SEND_SMS = 101,          // 101-sms 
   SEND_DB = 102,           // 102-database table 
   SEND_EMAIL = 103,        // 103-email 
   SEND_MMS = 104,          // 104-mms 
   SEND_WEB_SERVICE = 105,  // 105-web service
};

// 返回 : yyyy-mm-dd hh:mm:ss
#define GetCurrentTime(datetime)\
{\
	char now[10];\
	wociGetCurDateTime(now);\
	_wdbiDateTimeToStr(now,datetime);\
}

// 告警状态
extern const char* FunGetAlarmStatus(AlarmStatus& as);
// 告警级别
extern const char* FunGetAlarmLevel(AlarmLevel& al);


/**************************************************************************************************
  Object      : my_timer
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/25 18:07
  Description : 定时器的简单实现
**************************************************************************************************/
class my_timer {
	struct timeval st;
public:
	my_timer() {
	    memset(&st,0,sizeof(timeval));
	}
	void Clear() {
		memset(&st,0,sizeof(timeval));
	}
	void Start() {
		gettimeofday(&st,NULL);
	}
	void Restart() {
		Clear();Start();
	}
	double GetTime() {
        struct timeval ct;
        gettimeofday(&ct,NULL);
		return (double)(ct.tv_sec-st.tv_sec) + (double)(ct.tv_usec/1000000);
	}
};

/**************************************************************************************************
  Object      : AlarmMsgInfo
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:03
  Description : 告警信息类
**************************************************************************************************/
typedef struct StruMsgInfo
{
	unsigned int    msg_payload_len;                    // msg 有效长度
	unsigned char   msg_payload[MAX_MESSAGE_LEN];       // 消息，消息
}StruMsgInfo,*StruMsgInfoPtr;
class AlarmMsgInfo          // 消息内容类
{
protected:
	unsigned long        m_alarmId;                         // 告警流水号:1699759
    char                 m_alarmTitle[128];                 // 告警标题:门禁
    char                 m_alarmReason[1024];               // 可能原因:某个门碰探头探测到基站门非法打开
	AlarmStatus          m_alarmStatus;                     // 告警状态:清除告警
	AlarmType            m_alarmType;                       // 告警类别:动力门禁
	AlarmLevel           m_alarmLevel;                      // 网管告警级别:二级告警
    char                 m_alarmOccurTime[DATETIME_LEN];    // 告警发生时间:2013-01-08 15:04:23
	char                 m_alarmClearTime[DATETIME_LEN];    // 告警恢复时间:2013-01-08 15:04:22

protected:
	unsigned    char     m_PackHeader[32];                  // 包头
	unsigned    char     m_PackTail[32];                    // 包尾

protected:
	StruMsgInfo          m_stmsgInfo;                       // 发送消息信息    
                         
protected:	// 管理使用
	unsigned short       m_sendTimes;                       // 告警上送次数
	unsigned short       m_sendInterval;                    // 告警上送错误处理时间间隔,单位为s
	my_timer             m_alarmTimer;                      // 告警计时器
	char                 m_lastSendTime[DATETIME_LEN];      // 最近一次发送时间      
    ProcStatus           m_procStatus;                      // 告警处理状态机
    int                  m_ref;                             // 对象引用计数，队列中获取对象，引用计数+1，使用完成后引用计数-1
    SendType             m_sendInterface;                   // 消息发送接口类型  
	unsigned int         m_eventid;                         // 告警事件id，对应dp_log主键

public:
	AlarmMsgInfo()
	{
        m_stmsgInfo.msg_payload[0] = 0;
		m_stmsgInfo.msg_payload_len = 0;
		memset(m_alarmOccurTime,0,DATETIME_LEN);
		memset(m_alarmClearTime,0,DATETIME_LEN);
		memset(m_lastSendTime,0,DATETIME_LEN);
		memset(m_alarmTitle,0,128);
		memset(m_alarmReason,0,1024);
		m_alarmTimer.Start();
		
		m_alarmId = -1;
        m_ref = 0;
	}
	virtual ~AlarmMsgInfo()
	{
        m_alarmTimer.Clear();
	}

public:
	// 消息封装类，不同消息类型，派生类中实现
	virtual int             DoPackMessage(){ThrowWith("Call DoPackMessage Error");return -1;};
	// 消息解析
	virtual int             DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen){ThrowWith("call DoUnPackMessage Error");return -1;};

public:
	inline  unsigned  short GetSendTimes(){return m_sendTimes;};
	inline  void            AddSendTimes(){m_sendTimes++;};
	inline  void            SetSendTimes(const int sendTimes){m_sendTimes = sendTimes;};

	inline  unsigned  short GetSendInterval(){return m_sendInterval;};
	inline  void            SetSendInterval(const int sendInterval){m_sendInterval = sendInterval;};
	
	inline  char*           GetLastSendTime(){return m_lastSendTime;};
	inline  void            SetLastSendTime(const char* tm){strcpy(m_lastSendTime,tm);};
	inline  void            SetLastSendTime(){char tm[DATETIME_LEN] = {0};GetCurrentTime(tm);strcpy(m_lastSendTime,tm);};
	inline  StruMsgInfo&    GetMsgInfo(){return m_stmsgInfo;};

	inline  ProcStatus      GetProcStatus(){return m_procStatus;};
	inline  void            SetProcStatus(ProcStatus procStatus){m_procStatus = procStatus;};

	inline  void            SetSendInterface(SendType sendType){m_sendInterface = sendType;};
	inline  SendType        GetSendInterface(){return m_sendInterface;};

    inline  void            AddRef(){m_ref++;};
    inline  void            SubRef(){if(m_ref>0) m_ref--;};
    inline  int             GetRef(){return m_ref;};
	
	inline  unsigned  int   GetEventID(){return m_eventid;};
	inline  void            SetEventID(const int eventid){m_eventid = eventid;};

	inline  my_timer&        GetAlarmTimer(){return m_alarmTimer;};

public:
    inline  unsigned long   GetAlarmID(){return m_alarmId;};               // 告警流水号:1699759
    inline  void            SetAlarmID(const unsigned long alarmId){m_alarmId = alarmId;};

	inline  char*           GetAlarmTitle(){return m_alarmTitle;};         // 告警标题:门禁
    inline  void            SetAlarmTitle(const char* alarmTitle){strcpy(m_alarmTitle,alarmTitle);m_alarmTitle[strlen(alarmTitle)] = 0;};

	inline  char*           GetAlarmReason(){return m_alarmReason;};       // 可能原因:某个门碰探头探测到基站门非法打开
	inline  void            SetAlarmReason(const char* alarmReason){ strcpy(m_alarmReason,alarmReason);m_alarmReason[strlen(alarmReason)] = 0;};

	inline  AlarmStatus     GetAlarmStatus(){return m_alarmStatus;};       // 告警状态:清除告警,产生告警
	inline  void            SetAlarmStatus(const AlarmStatus alarmStatus){m_alarmStatus = alarmStatus;};

	inline  AlarmType       GetAlarmType(){return m_alarmType;};           // 告警类别:动力门禁
	inline  void            SetAlarmType(const AlarmType alarmType){m_alarmType = alarmType;};
	
	inline  AlarmLevel      GetAlarmLevel(){return m_alarmLevel; };        // 网管告警级别:二级告警
	inline  void            SetAlarmLevel(const AlarmLevel alarmLevel){m_alarmLevel = alarmLevel;};

	inline  char*           GetAlarmOccurTime(){return m_alarmOccurTime;}; // 告警发生时间:2013-01-08 15:04:23
	inline  void            SetAlarmOccurTime(const char* aot){strcpy(m_alarmOccurTime,aot);m_alarmOccurTime[strlen(aot)]=0;};
	inline  char*           GetAlarmClearTime(){return m_alarmClearTime;}; // 告警恢复时间:2013-01-08 15:04:22
	inline  void            SetAlarmClearTime(const char* act){strcpy(m_alarmClearTime,act);m_alarmClearTime[strlen(act)]=0;};
};


/**************************************************************************************************
  Object      : TextAlarmInfo
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:02
  Description : 湖北移动移动文本告警类
**************************************************************************************************/
class TextAlarmInfo:public AlarmMsgInfo
{
public:
	TextAlarmInfo():AlarmMsgInfo()
	{
        memset(m_PackHeader,0,32);
        strcpy((char*)m_PackHeader,"<+++>");
		memset(m_PackTail,0,32);
		strcpy((char*)m_PackTail,"<--->");
	}
	virtual ~TextAlarmInfo(){};

public:
	// 告警信息打包
	virtual int    DoPackMessage();
	virtual int    DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen);

};

enum AlarmProtocolType            // 协议类型
{
    Protocol_Text = 0,            // 湖北文本协议
    Protocol_Bin,                 // 二进制协议
};

// 发送消息简单工程类
class AlarmMessageFactory
{
public:
	static   AlarmMsgInfo* CreateAlarmMessageObj(const AlarmProtocolType alarmProtocolType)
	{
		// 目前做文本的协议
		if (Protocol_Text == alarmProtocolType)
		{
			return new TextAlarmInfo();
		} 
		else
		{
			// 其他类型协议
			return NULL;
		}
	}
};

//================================================================================================
typedef pthread_mutex_t Lock;
class AutoLock 
{
private:
	Lock* m_lock;
public:
    AutoLock(Lock* lock):m_lock(lock)
	{
		pthread_mutex_lock(m_lock);
	}
	virtual ~AutoLock()
	{
		pthread_mutex_unlock(m_lock);
	}
};

/**************************************************************************************************
  Object      : AlarmMsgQueue
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:00
  Description : 告警消息队列
**************************************************************************************************/
class AlarmMsgQueue 
{
protected:
	typedef	std::vector<AlarmMsgInfo*>  AlarmMsgVector;
	AlarmMsgVector           m_msgVector;
	Lock                     m_lock;       
    unsigned long            m_currentIndex; // 当前获取的对象索引，GetAlarmMsg时候用

public:
    AlarmMsgQueue();
    virtual~ AlarmMsgQueue();

public:
    // 判断告警存储的条件：告警id相同，告警状态相同
	bool                    FindAlarmMsg(const unsigned long alarmId,const AlarmStatus as);
	int                     AddAlarmMsg(AlarmMsgInfo* msg);
	int                     DeleteAlarmMsg(AlarmMsgInfo* msg);
	int                     DeleteNullAlarmMsg();
	unsigned long           Size();

public:
    // 下列函数必须成对使用
	AlarmMsgInfo*           GetAlarmMsg();                         // 从队列中获取一个不再使用的告警信息                  
	int                     RetAlarmMsg(AlarmMsgInfo* msg);        // 告警信息使用完成后将其返回队列中 
};
#endif 
