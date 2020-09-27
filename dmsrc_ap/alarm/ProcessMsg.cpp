#include "ProcessMsg.h"

ProcessMsg::ProcessMsg()
{
	m_SendMsgAry = NULL;
	m_configFile = NULL;
	m_alarmProcRun = false;
	m_HasInited = false;
}

ProcessMsg::~ProcessMsg()
{
	if (m_SendMsgAry != NULL)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if(NULL != m_SendMsgAry[i])
			{
				m_SendMsgAry[i]->UnInit();
				delete m_SendMsgAry[i];
				m_SendMsgAry[i] = NULL;
			}
		}

		delete [] m_SendMsgAry;
		m_SendMsgAry = NULL;

		m_SendMsgNum = 0;
	}

	m_alarmProcRun = false;

	if (m_configFile != NULL)
	{
		delete m_configFile;
		m_configFile = NULL;
	}
}


// 从dp_log表中获取告警信息插入到dp_alarm中，并更新dp_log表中通知记录
int ProcessMsg::InsertDbAlarmInfo()
{
	if(!m_HasStarted)
	{
		return -1;
	}

	// 取出告警事件，插入dp_alarm中
	AutoMt  mt(m_InsertConn,1000);
	mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,c.sendinterface "
		" from dp.dp_log a, dp.dp_eventtype b, dp.dp_eventsendctl c where a.evt_tp = b.eventtypeid and  b.eventtypeid = c.eventtype and "
		" b.eventlevel = c.eventlevel and a.notifystatus = %d",WAIT_TO_NOTIFY);

	int InsertNum = mt.Wait();
	if(InsertNum>0)
	{
		for (int i=0;i<InsertNum;i++)
		{
			AutoStmt mt2(m_InsertConn);  
			// 告警插入dp_alarm中
			mt2.Prepare("INSERT INTO `dp`.`dp_alarm`(`eventid`,`procstatus`,`sendtimes`,`sendinterface`)  values (%d,%d,%d,%d)",
				mt.GetInt("eventid",i),ProcStatus_Alarm_Wait,0,mt.GetInt("sendinterface",i));
			mt2.Execute(1);
			mt2.Wait();

			// 更新dp_log中的记录
			mt2.Prepare("update dp.dp_log set notifystatus = %d where eventid = %d",HAS_NOTIFYED,mt.GetInt("eventid",i));
			mt2.Execute(1);
			mt2.Wait();
		}		 
	}
	return InsertNum;
}

// 根据任务完成状态更新dp_alarm表中的告警信息，即将告警修改为等待清除告警	
// 告警恢复条件：
// 1>. 采集过程：采集过程中的产生的告警信息，等待该表的对应分区数据采集完成后，就恢复告警，对应dp_datapart.status = 2
// 2>. 整理和装入过程：整理和装入过程中产生的告警信息，等待该表的对应分区数据装入完成后恢复告警，对应dp_datapart.status = 5
int  ProcessMsg::InsertDbClearAlarmInfo()
{
	if(!m_HasStarted)
	{
		return -1;
	}
	AutoStmt mt(m_UpdateConn);  

	/*
	-- 导出过程中的恢复告警设置
	update dp.dp_alarm set procstatus = 10 ,sendtimes = 0 where (procstatus = 2 or (procstatus = 3 and sendtimes >= 5)) and eventid in (
	select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c 
	where a.tabid = b.tabid and a.datapartid = b.datapartid and 
	a.evt_tp = c.eventtypeid and c.eventtypeid < 250 and b.status = 2
	*/
	mt.Prepare("update dp.dp_alarm set procstatus = %d ,sendtimes = 0 where (procstatus = %d or (procstatus = %d and sendtimes >= %d)) and eventid in ( "
				" select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c "
				" where a.tabid = b.tabid and a.datapartid = b.datapartid and "
				" a.evt_tp = c.eventtypeid and c.eventtypeid < %d and b.status = 2 )",
				ProcStatus_Clear_Alarm_Wait,
				ProcStatus_Alarm_Done,
                ProcStatus_Alarm_Error,
				m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
				DUMP_BEGIN_DUMPING_NOTIFY);

	mt.Execute(1);
	mt.Wait();

	/*
	-- 整理转入过程中的清除告警
	update dp.dp_alarm set procstatus = 10 ,sendtimes = 0 where (procstatus = 2 or (procstatus = 3 and sendtimes >= 5)) and eventid in (
	select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c 
	where a.tabid = b.tabid and a.datapartid = b.datapartid and 
	a.evt_tp = c.eventtypeid and c.eventtypeid > 300 and b.status = 5
	*/
	mt.Prepare("update dp.dp_alarm set procstatus = %d ,sendtimes = 0 where (procstatus = %d or (procstatus = %d and sendtimes >= %d)) and eventid in ( "
		" select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c "
		" where a.tabid = b.tabid and a.datapartid = b.datapartid and "
		" a.evt_tp = c.eventtypeid and c.eventtypeid > %d and b.status = 5 )",
		ProcStatus_Clear_Alarm_Wait,
		ProcStatus_Alarm_Done,
		ProcStatus_Alarm_Error,
		m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
		MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR);

	mt.Execute(1);
	mt.Wait();

	return 0;
}

// 加载告警信息到内存(程序目前只支持一个进程运行)
int ProcessMsg::LoadDbSrcAlarmInfo(const bool bFirstLoad)
{
    if(!m_HasStarted)
	{
		return -1;
	}

	// 取出告警事件，插入dp_alarm中
    AutoMt  mt(m_LoadConn,1000);
	if (bFirstLoad)
	{
		// 第一次加载，只加载处理过程中和处理失败且发送次数小于最大发送次数的告警
		mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,"
			"c.alarmid,c.procstatus,c.sendtimes,c.sendinterface,c.lastproctime "
			"from dp.dp_log a, dp.dp_eventtype b, dp.dp_alarm c where a.evt_tp = b.eventtypeid "
			" and c.eventid = a.eventid and ( sendtimes < %d and c.procstatus in(%d,%d,%d,%d))",
			m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
			ProcStatus_Alarm_Doing,ProcStatus_Alarm_Error,ProcStatus_Clear_Alarm_Doing,ProcStatus_Clear_Alarm_Error);
	}
	else
	{
		// 不是第一次加载，只加载等待处理的告警，和恢复处理的告警
		mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,"
			"c.alarmid,c.procstatus,c.sendtimes,c.sendinterface,c.lastproctime "
			"from dp.dp_log a, dp.dp_eventtype b, dp.dp_alarm c where a.evt_tp = b.eventtypeid "
			"and c.eventid = a.eventid and (c.procstatus = %d or c.procstatus = %d)",
			ProcStatus_Alarm_Wait,ProcStatus_Clear_Alarm_Wait);
	}
	int InsertNum = mt.Wait();
	char dt1[DATETIME_LEN],dt2[10];
    if(InsertNum>0)
	{
		for (int i=0;i<InsertNum;i++)
		{
			// 告警插入内存队列
            AlarmMsgInfo* pAlarmMsg = NULL;
			pAlarmMsg = AlarmMessageFactory::CreateAlarmMessageObj(Protocol_Text);
			if (pAlarmMsg == NULL)
			{
				lgprintf("暂时不支持该类型%d的告警协议",Protocol_Text);
				continue;
			}

			// 设置告警属性
			pAlarmMsg->SetAlarmID(mt.GetInt("alarmid",i));
			pAlarmMsg->SetAlarmTitle(mt.PtrStr("eventtype",i));
			pAlarmMsg->SetSendInterface((SendType)mt.GetInt("sendinterface",i));
			pAlarmMsg->SetAlarmType((AlarmType)mt.GetInt("eventtypeid",i));
			pAlarmMsg->SetAlarmLevel((AlarmLevel)mt.GetInt("eventlevel",i));
			pAlarmMsg->SetAlarmReason(mt.PtrStr("event",i));
			if (mt.GetInt("procstatus",i) < ProcStatus_Clear_Alarm_Wait)
			{
                pAlarmMsg->SetAlarmStatus(AlarmStatus_OCCUR);
				pAlarmMsg->SetProcStatus(ProcStatus_Alarm_Doing);
			}
			else
			{
				pAlarmMsg->SetProcStatus(ProcStatus_Clear_Alarm_Doing);
				pAlarmMsg->SetAlarmStatus(AlarmStatus_CLEAR);
				char tm[DATETIME_LEN] = {0};
				GetCurrentTime(tm);
				pAlarmMsg->SetAlarmClearTime(tm);
			}
			
			// 告警发送时间
			memset(dt2,0,10);
			memset(dt1,0,DATETIME_LEN);
			mt.GetDate("evt_tm",i,dt2);
			_wdbiDateTimeToStr(dt2,dt1);
			pAlarmMsg->SetAlarmOccurTime(dt1);
			
			int sendInterval = m_configFile->getOption("send_msg_ctl:send_interval",SEND_DEFAULT_INTERVAL);
			if (sendInterval>SEND_MIN_INTERVAL)
			{
				sendInterval = SEND_MIN_INTERVAL;
				pAlarmMsg->SetSendInterval(sendInterval);
			}
			pAlarmMsg->SetSendTimes(mt.GetInt("sendtimes",i));
			pAlarmMsg->SetEventID(mt.GetInt("eventid",i));       // 用于插入dp_alarmlog的时候查询用
			
			// 最后更新时间
			memset(dt2,0,10);
			memset(dt1,0,DATETIME_LEN);
			mt.GetDate("lastproctime",i,dt2);
			_wdbiDateTimeToStr(dt2,dt1);
			pAlarmMsg->SetLastSendTime(dt1);
			
			// 启动定时器
			pAlarmMsg->GetAlarmTimer().Restart();
			
			// 加入队列
			m_AlarmMsgQueue.AddAlarmMsg(pAlarmMsg);

			// 更新数据库中告警状态
			UpdateDbAlarmInfo(pAlarmMsg);
		}		 
	}
	return InsertNum;
}

// 更新告警状态到db数据库
int ProcessMsg::UpdateDbAlarmInfo(AlarmMsgInfo* alarminfo)
{
	if (NULL == alarminfo)
	{
		return -1;
	}
	if (!m_HasStarted)
	{
		return -1;
	}
	// 心跳告警不插入数据库中
	if (alarminfo->GetAlarmStatus() == AlarmStatus_HEARTBEAT)
	{
		return 0;
	}

	AutoStmt mt2(m_UpdateConn);  
	// 更新dp_log中的记录
	mt2.Prepare("UPDATE `dp`.`dp_alarm` SET `procstatus` = %d ,`sendtimes` = %d,`lastproctime` = '%s'  WHERE alarmid = %d",
		alarminfo->GetProcStatus(),alarminfo->GetSendTimes(),alarminfo->GetLastSendTime(),
		alarminfo->GetAlarmID());
	mt2.Execute(1);
	mt2.Wait();

	return 0;
}
// 插入告警信息到告警日志表
int ProcessMsg::InsertAlarmInfo2AlarmLog(AlarmMsgInfo* alarminfo)
{
	if (alarminfo == NULL)
	{
		return -1;
	}

	// 心跳告警不插入数据库中
	if (alarminfo->GetAlarmStatus() == AlarmStatus_HEARTBEAT)
	{
		return 0;
	}

	// 告警插入dp_alarmlog中
	AutoStmt mt2(m_InsertConn);  
	// 告警插入dp_alarm中
	mt2.Prepare("INSERT INTO `dp`.`dp_alarmlog`(`alarmid`,`tabid`,`datapartid`,`alarmtype`,`alarmlevel`,`alarmtime`,`alarmtitle`,`alarmreason`,`alarmstatus`,"
		" `procstatus`,`sendtimes`,`alarmprocesstime`,`sendinterface`) "
		" select '%d',tabid,datapartid,'%d','%d','%s','%s','%s','%d','%d','%d','%s','%d' from dp.dp_log where eventid = %d",
		alarminfo->GetAlarmID(),
		alarminfo->GetAlarmType(),
		alarminfo->GetAlarmLevel(),
		// 根据告警类型填写告警时间和清除告警时间
		alarminfo->GetAlarmStatus() == AlarmStatus_OCCUR ? alarminfo->GetAlarmOccurTime():alarminfo->GetAlarmClearTime(),
		alarminfo->GetAlarmTitle(),
		alarminfo->GetAlarmReason(),
		alarminfo->GetAlarmStatus(),
		alarminfo->GetProcStatus(),
		alarminfo->GetSendTimes(),
		alarminfo->GetLastSendTime(),
		alarminfo->GetSendInterface(),
		alarminfo->GetEventID());

	mt2.Execute(1);
	mt2.Wait();
    
	return 0;
}

// 心跳命令的产生
int ProcessMsg::GenerateHeartBeat()
{
	if (m_SendMsgAry == NULL)
	{
		return -1;
	}
	int send_interval = m_configFile->getOption("heart_beat:send_interval",5);
	if (m_HeartBeatTimer.GetTime()>send_interval)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if (m_SendMsgAry[i] == NULL)
			{
				continue;
			}

			AlarmMsgInfo* pAlarmMsgObj = NULL;
			switch(m_SendMsgAry[i]->GetSendInterface())
			{
			case SEND_SOCKET_TCP:
                pAlarmMsgObj = AlarmMessageFactory::CreateAlarmMessageObj(Protocol_Text);
				break;

			default:
				// 心跳命令暂时只做tcp的
				break;
			}

			if (NULL != pAlarmMsgObj)
			{
				// 心跳命令所有接口都发送，满足时间就发送
				pAlarmMsgObj->SetAlarmStatus(AlarmStatus_HEARTBEAT);
				// 添加队列
				m_AlarmMsgQueue.AddAlarmMsg(pAlarmMsgObj);
			}
		}

		m_HeartBeatTimer.Restart();
		return 0;
	}
	return -1;
}
// 根据数据库中的消息发送接口类型，创建消息发送接口对象
int ProcessMsg::CreateSendMsgArray()
{
	if(m_SendMsgAry != NULL)
	{
		lgprintf("告警接口对象已经初始化，无法创建发送对象.");
		return -1;
	}
	AutoMt  mt(m_LoadConn,1000);
	mt.FetchFirst("select distinct codevalue from dp.dp_code where codetype = 25");
	int SendMsgTypeNum = mt.Wait();
	m_SendMsgNum = SendMsgTypeNum;
	if(SendMsgTypeNum>0)
	{
		m_SendMsgAry = new SendMessage*[SendMsgTypeNum];
		for (int i=0;i<SendMsgTypeNum;i++)
		{		    
			m_SendMsgAry[i] = NULL;	
		}
		// 创建对象
		for (int i=0;i<SendMsgTypeNum;i++)
		{
			// 告警插入内存队列
			SendType send_type;
			try
			{
				send_type = (SendType)atoi(mt.PtrStr("codevalue",i));
			}
			catch(...)
			{
			   lgprintf("数据库中存储的codevalue类型值错误.");
			   return -1;	
			}
			m_SendMsgAry[i] = SendMessageFactory::CreateSendMessageObj(send_type);
			if (m_SendMsgAry[i] == NULL)
			{
				lgprintf("暂时不支持该类型%d的发送消息接口.",send_type);
				continue;
			}
			m_SendMsgAry[i]->SetParentProcessRecvMsg(this);
            m_SendMsgAry[i]->SetSendInterface(send_type); // 设置发送命令类型
			
			int initFlag = -1;
			TcpServerItem st_tcp;
			DbServerItem st_db;
			switch(send_type)
			{
			case SEND_SOCKET_TCP:
				strcpy(st_tcp._ip,m_configFile->getOption("send_interface_tcp:serverip",""));
				st_tcp._port = m_configFile->getOption("send_interface_tcp:serverport",6666);
		        initFlag = m_SendMsgAry[i]->Init((void*)&st_tcp);
				break;
			case SEND_DB:
				// 初始化
				//initFlag = m_SendMsgAry[i]->Init((void*)&st_db);
				initFlag =0;  // 以后修改实现
				break;
			default:
				// 不支持
				initFlag =0;
				break;
			}

			if (initFlag < 0)
			{
				lgprintf("消息发送接口%d初始化失败.",send_type);
				return -1;
			}

		}// end for

		return 0;

	}// end if
    
	lgprintf("没有告警接口可以初始化.");
	return -1;
}

int  ProcessMsg::OnInit(const char* ctlConfigFile)
{
    if(m_HasInited)
    {
        lgprintf("ProcessMsg 已经初始化过，不能重复调用.");
        return 0;	
    }
    if (NULL == m_configFile)
    {
		m_configFile = new ConfigFile(ctlConfigFile);
    }

	// 创建连接,控制文件读取即可
	const char* dns = m_configFile->getOption("dp_server:dsn","dp");
	const char* user = m_configFile->getOption("dp_server:user","root");
	const char* password = m_configFile->getOption("dp_server:password","dbplus03");
	m_LoadConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));
	m_UpdateConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));
	m_InsertConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));

	// 创建发送对象
	if(CreateSendMsgArray() != 0)
	{
		return -1;
	}

	m_HeartBeatTimer.Clear();
	m_HeartBeatTimer.Start();

    m_HasInited = true;
    return 0;
}
int  ProcessMsg::OnStart()
{
    if(!m_HasInited)
    {
        if (-1 == OnInit("SendAlarm.ctl"))
        {
        	lgprintf(" OnInit(SendAlarm.ctl) 失败，无法处理告警信息.");
        	return -1;
        }
    }      
    
	// 启动告警处理线程
	if(pthread_create(&m_AlarmMsgProcThread,NULL,OnAlarmMsgProc,(void*)this)!=0)
    {
		lgprintf("can't create thread.\n");
		return -1;    	
    }
	m_alarmProcRun = true;
	m_HasStarted = true;
    m_HeartBeatTimer.Start();
    
	return 0;
}

int  ProcessMsg::OnStop()
{
	m_HasStarted = false;
	m_alarmProcRun = false;
	sleep(1);
	
	// 停止发送接口
	if (m_SendMsgAry != NULL)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if(NULL != m_SendMsgAry[i])
			{
				m_SendMsgAry[i]->UnInit();
				delete m_SendMsgAry[i];
				m_SendMsgAry[i] = NULL;
			}
		}

		delete [] m_SendMsgAry;
		m_SendMsgAry = NULL;

		m_SendMsgNum = 0;
	}
	sleep(1);

	// 内存中的告警状态无需更新，下次启动会加载出来	

	return 0;
}


// 处理告警业务
int ProcessMsg::ProcAlarmInfo()
{
    unsigned long alarmSize = m_AlarmMsgQueue.Size();
	// 每次处理1半的告警
    for (int i=0;i<alarmSize;i++)
    {
		// 获取告警信息
		AlarmMsgInfo* pAlarmObj = m_AlarmMsgQueue.GetAlarmMsg();
		if (pAlarmObj == NULL)
		{  
		    lgprintf("没有找到满足条件的告警记录，");
			continue;
		}

		// 判断时间是否有效,心跳命令就直接处理
		if(pAlarmObj->GetSendTimes()>0 && pAlarmObj->GetAlarmStatus() != AlarmStatus_HEARTBEAT)
		{
			if(pAlarmObj->GetAlarmTimer().GetTime() < pAlarmObj->GetSendInterval())
			{
				// 不满足发送条件
				m_AlarmMsgQueue.RetAlarmMsg(pAlarmObj);
				continue;
			}
		}

		// 直接可以发送的
		int retFlag = -1;
		for (int indexInterface=0;indexInterface<m_SendMsgNum;indexInterface++)
		{
			if (m_SendMsgAry[indexInterface] != NULL && 
				(m_SendMsgAry[indexInterface]->GetSendInterface() == pAlarmObj->GetSendInterface()|| 
				pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT))
			{
				// 发送消息
				pAlarmObj->DoPackMessage();

				if( pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT )
				{   // 心跳命令，直接发送告警
				    retFlag = m_SendMsgAry[indexInterface]->SendMsg(pAlarmObj);
				}
				else
				{   // 是否屏蔽告警，屏蔽就不发送
				    if(SEND_ALARM_YES == m_configFile->getOption("send_msg_ctl:send_alarm_switch",SEND_ALARM_YES)) 
				    {
    				    retFlag = m_SendMsgAry[indexInterface]->SendMsg(pAlarmObj);               
				    }
					else
					{
					    lgprintf("上送告警开关send_msg_ctl:send_alarm_switch = 0,未上送告警(只上送了心跳).");
                        retFlag = 1;
					}
				}
				pAlarmObj->GetAlarmTimer().Restart();
				if(LogSendMsg_Yes == m_configFile->getOption("send_msg_ctl:log_send_msg",LogSendMsg_Yes))
				{
					lgprintf("发送消息:\n%s\n %s.",pAlarmObj->GetMsgInfo().msg_payload,retFlag < 0 ? "失败":"成功");
                }
				// 如果是心跳命令将不进行数据库相关处理
				if (pAlarmObj->GetAlarmStatus()== AlarmStatus_HEARTBEAT)
				{
					lgprintf("发送心跳命令:%s , 发送%s.",pAlarmObj->GetMsgInfo().msg_payload,retFlag < 0 ? "失败":"成功");
					break;
				}

				// 更改处理状态
				if (pAlarmObj->GetProcStatus()<ProcStatus_Clear_Alarm_Wait)
				{
					pAlarmObj->SetProcStatus(retFlag < 0 ? ProcStatus_Alarm_Error : ProcStatus_Alarm_Done);
				}
				else
				{
					pAlarmObj->SetProcStatus(retFlag < 0 ? ProcStatus_Clear_Alarm_Error : ProcStatus_Clear_Alarm_Done);
				}
				pAlarmObj->SetLastSendTime();
				pAlarmObj->AddSendTimes();

				// 更新告警状态信息进入dp_alarm表中
				UpdateDbAlarmInfo(pAlarmObj);
				
				// 保存告警日志信息到dp_alarmlog表中
				InsertAlarmInfo2AlarmLog(pAlarmObj);
				
				break;
			} // end if 
		}// end for
        m_AlarmMsgQueue.RetAlarmMsg(pAlarmObj);	

		// 删除内存中的告警
		// 1. 心跳
		// 2. 已经处理完成的告警信息
		// 3. 发送次数大于最大发送次数的告警
		if (pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT || 
			(pAlarmObj->GetProcStatus() == ProcStatus_Alarm_Done) || 
			(pAlarmObj->GetProcStatus() == ProcStatus_Clear_Alarm_Done)|| 
			(pAlarmObj->GetSendTimes() > m_configFile->getOption("send_msg_ctl::send_times",SEND_MAX_TIMES)) )
		{
			// 设置告警为删除状态,在DeleteNullAlarmMsg中删除
			pAlarmObj->SetAlarmStatus(AlarmStatus_Delete);
		}

    } // for (int i=0;i<alarmSize;i++) 

	// 清空告警列表中为NULL的告警信息
	return m_AlarmMsgQueue.DeleteNullAlarmMsg();
}

// 告警处理线程
void* ProcessMsg::OnAlarmMsgProc(void *params)
{
    ProcessMsg* _this = NULL;
	_this = (ProcessMsg*)params;
	if (_this == NULL)
	{
		lgprintf("线程参数不正确，ProcessMsg::OnAlarmMsgProc线程启动失败.");
		return NULL;
	}

	//  第一次从数据库中加载上一次没有处理完成的告警
	_this->LoadDbSrcAlarmInfo(true);

    while (_this->m_alarmProcRun)
    {
		// 加载告警信息到内存中
		_this->LoadDbSrcAlarmInfo(false);
		sleep(1);

		// 添加心跳信息告警信息到内存
		if(-1 == _this->GenerateHeartBeat())
		{			
			sleep(4);          // 没有心跳
		}

		// 将dp_log中的告警信息插入表dp_alarm中
		_this->InsertDbAlarmInfo();
		
		// 处理告警业务信息
		int processNum = _this->ProcAlarmInfo();
		int sleepRate = processNum / 50;
		if (sleepRate == 0)
		{
			sleep(5);
		}
		else if (sleepRate >=1 && sleepRate<= 5)
		{
			sleep(3);
		}
		else
		{
			sleep(1);
		}
			
		// 恢复告警的状态设置
		_this->InsertDbClearAlarmInfo();
    }
    
    return NULL;
}



// 接收来说对方接口的消息
int	ProcessMsg::ProcRecvMsg(const unsigned char *rcvMsg,const unsigned int rcvMsgLen)
{
	// 以后处理来自对方的消息
	lgprintf("recv msg : len = %d,msg = %s.",rcvMsgLen,(char*)rcvMsg);
	return 0;
}
