# 以#号开始表示这一行是注释
#[FTP]
#host= 192.192.192.2
#Username=dm4321
#Password=Admin@#@!
##如果是Binary模式传输文件，Textmode设置为0
#Textmode=0
#path=/

#本地路径，FTP/LOCAL只能有一个，不允许两个同时配置
[PARSER]
PARSERMODULE=/app/dm6/plugin/libcommontxt_parser.so
#指定文件分解模块在一个时间段内不工作，单位是时（0-23），Start必须小于或等于END，START=END=0，表示分拣模块全天工作。分拣模块空闲的两个考虑，一是避免和数据整理上线抢占系统资源，二是在查询高峰期避免响应速度变慢。
PARSERIDLESTART=4
PARSERIDLEEND=10


[LOCAL]
PATH=/app/dm6/dataData
#PATH=/data/dt2/xaGeneData/data
#File name pattern section :
[FILES]
Localpath=/data/dt1/dma/tstfiles
#Localpath=/data/dt4/zkb_dm6_test/temp_test
#参考dma-59
year=-8t
month=-4t
day=-2t
separator=;
hour=p
sequencecheck =0
#参考DMA-64
recordnumcheck =0
recordfmtcheck =0
#结束数据导出的控制，1表示立即结束（ENDCHECKDELAY、ENDDELAY参数无效），0表示按照ENDCHECKDELAY、ENDDELAY参数来控制
forceenddump=1
#文件名中可以选择的动态参数：
# $year $month　$day $hour: 时间参数
# $table :目标表名
# $db : 目标数据库名
#文件名中间不能有空格
FILENAME="$db-$table_$year-$month-$day_$hour-*.txt"
#备份文件的路径。文件分解模块可以建子目录,备份文件必须使用共享目录，
# 多进程并发处理时，不同的主机向同一个共享目录备份
# 本地保存的路径可以不是共享路径
BACKUPPATH=/data/dt1/dma/backup
#BACKUPPATH=/data/dt4/zkb_dm6_test/dest_test
#备份文件保留天数，超过这个周期，会被删除
FILEKEEPDAYS=3

#采集后的文件是否保留，默认删除
#keeppickfile=1
#本地是否直接分拣（跳过采集操作），默认需要采集
#skippickup
#本地采集不备份文件，默认备份
#skipbackup

#文件检查循环周期，秒
SEEKINTERVAL=60
#文件截止时间，延后分钟数。这个是按照目标表对应日期的延后分钟数。
ENDDELAY=3
#文件处理结束后，截止时间检查，延后分钟数。这个是相对与最后一文件的本地处理时间延迟
ENDCHECKDELAY=1
HEADER=*
enclosedby="\""


