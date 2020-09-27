# 以#号开始表示这一行是注释
[heart_beat] 
# 心跳命令时间间隔，单位为秒
send_interval=30

#发送控制
[send_msg_ctl]
# 同一告警的连续2次发送时间间隔，单位秒钟，默认值300秒，最小值为10秒
send_interval = 30
# 发送失败重复次数，默认值为5次，最小值为1次
send_times = 6
# 是否输出发送日志,1:日志文件中输出发送日志信息，0:日志文件中不输出发送日志信息,默认值为:1
log_send_msg = 1


#告警数据库连接信息,连接dp的数据库信息
[dp_server]
# dsn 数据源
dsn = dp
# 用户名
user = root
# 密码
password = dbplus03

#告警上送接口类型参数
[send_interface_tcp]
#服务端ip
serverip=192.168.1.107
#服务端端口
serverport=6800

[send_interface_db]
#数据库接口配置
