#ifndef  PROCESS_MSG_DEF_H
#define  PROCESS_MSG_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : PROCESSALARMMSG.H    
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/14 22:50	
  Description : 告警处理相关业务实现
**************************************************************************************************/
#include "AlarmMsg.h"
#include "SendMsg.h"

// 处理告警消息类,不删除告警，删除需要手动删除
class ProcessMsg:public ProcessRecvMsg 
{
protected:
    AlarmMsgQueue             m_AlarmMsgQueue;         // 内存告警队列
    int                       m_SendMsgNum;            // 发送对象个数
	SendMessage**             m_SendMsgAry;            // 发送消息接口类指针 new SendMessage*[x]
    bool                      m_HasStarted;            // 线程是否已经启动
	ConfigFile*               m_configFile;            // 控制文件
	bool                      m_HasInited;             // 是否已经初始化

protected:
    pthread_t                 m_AlarmMsgProcThread;    // 告警消息处理线程
    bool                      m_alarmProcRun;          // 处理线程运行标志
	
	// 处理告警的相关数据库连接
	AutoHandle                m_InsertConn,m_LoadConn,m_UpdateConn;
	my_timer                  m_HeartBeatTimer;        // 心跳命令定时器
	enum
    { 
        LogSendMsg_No = 0,      // 不输出发送日志到文件
		LogSendMsg_Yes = 1,     // 输出发送日志到文件
    };
	enum
	{
        SEND_ALARM_YES = 1,     // 上送告警
        SEND_ALARM_NO = 0,      // 不上送告警
	};

protected:
	// 从dp_log表中获取告警信息插入到dp_alarm
	int                       InsertDbAlarmInfo();
	// 根据任务完成状态更新dp_alarm表中的告警信息，即将告警修改为等待清除告警
	int                       InsertDbClearAlarmInfo();
	// 加载告警信息到内存
    int                       LoadDbSrcAlarmInfo(const bool bFirstLoad = false);
	// 更新告警状态到db数据库
    int                       UpdateDbAlarmInfo(AlarmMsgInfo* alarminfo);    
	// 插入告警信息到告警日志表
	int                       InsertAlarmInfo2AlarmLog(AlarmMsgInfo* alarminfo);
    // 心跳命令的产生
    int                       GenerateHeartBeat();
	// 处理告警业务
	int                       ProcAlarmInfo();
    // 根据数据库中的消息发送接口类型，创建消息发送接口对象
    int                       CreateSendMsgArray();

public:
	ProcessMsg();
	virtual ~ProcessMsg();

public:	// 启动，停止告警处理
	int                       OnInit(const char* ctlConfigFile);
    int                       OnStart();
    int                       OnStop();
	// 接收来说对方接口的消息
	virtual        int	      ProcRecvMsg(const unsigned char *rcvMsg,const unsigned int rcvMsgLen);
	

protected: // 告警处理相关线程
	static        void*       OnAlarmMsgProc(void *params);      // 处理告警信息线程
};

#endif
