#cdrp /backupdata/gsm/pickbackdata/200611
#cdrp /backupdata/gsm/pickbackdata/200612
#cdrp /ttfile/pickbackdata/200612
#cdrp /ttfile/pickbackdata/200701
#2006年12月开始,备份的目录变更为 /cdrdata/gsm/pickbackdata/yyyymmdd
# 目录名称到日. 最新文件的目录位置无变化,单只有最近一到两天的文件.
#cdrp /cdrdata/gsm/pickbackdata/$(date +\%Y\%M)
#cdrp /cdrdata/gsm/pickbackdata/20061230
#cdrp /cdrdata/gsm/pickbackdata/20061231
cdrp /cdrdata/gsm/pickbackdata/20070120
cdrp /cdrdata/gsm/pickbackdata/20070121
cdrp /cdrdata/gsm/pickbackdata/20070122
cdrp /cdrdata/gsm/pickbackdata/20070123
