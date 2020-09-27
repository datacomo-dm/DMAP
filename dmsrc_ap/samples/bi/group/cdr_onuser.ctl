# 参考: http://130.86.2.146/mwiki/index.php/%E5%8F%82%E6%95%B0%E6%96%87%E4%BB%B6%E6%A0%B7%E6%9C%AC

[基本选项]

错误时继续循环=0
引用数据=0
短信通知=0
日志文件=bigroup/cdr_subscrb/200607_10.log
日志回显=1
统计标题=7月后GC话单统计(到用户)
详细输出=1

[源数据]
记录数=50000
变量='src_tablename part_id1'
连接参数='root cdr0930 dtagt80 odbc'
SQL='select part_id1,subscrbid,calltype,roamtype,islocal,tolltype,term_type,termphonetype, 
 (prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)>0.001 flag, 
 prcpln_basfee/1000 prcpln_basfee,prcpln_LOCADDFEE/1000 prcpln_LOCADDFEE,prcpln_TOLLFEE/1000 prcpln_TOLLFEE, 
 prcpln_TOLLADDFEE/1000 prcpln_TOLLADDFEE ,DISCNTbasfee/1000 DISCNTbasfee,DISCNTTOLLFEE/1000 DISCNTTOLLFEE, 
 DISCNTADDFEE/1000 DISCNTADDFEE,prcpln_INFOFEE/1000 prcpln_INFOFEE,DISCNTINFOFEE/1000 DISCNTINFOFEE , 
 (prcpln_basfee+prcpln_LOCADDFEE+prcpln_TOLLFEE+prcpln_TOLLADDFEE+DISCNTbasfee+DISCNTTOLLFEE+DISCNTADDFEE+prcpln_INFOFEE+DISCNTINFOFEE)/1000 sumfee, 
 ctimes,ltimes,calltimelen  
 from dest.%s where part_id1=\'%s\' '
分组字段='part_id1,subscrbid,calltype,roamtype,islocal,tolltype,term_type,termphonetype,flag'
汇总字段='prcpln_basfee,prcpln_LOCADDFEE,prcpln_TOLLFEE,prcpln_TOLLADDFEE,DISCNTbasfee,DISCNTTOLLFEE,DISCNTADDFEE,prcpln_INFOFEE,DISCNTINFOFEE,sumfee,ctimes,ltimes,calltimelen'

[结果数据]
记录数=12000000
变量='res_GORC res_MONTH'
结果表名='tab_%svoicdr_%s_st'
连接参数='ods_temp ods_temp //130.86.12.30:1521/ubisp.ynunicom.com oracle'
结果集存储=2

[变量]
part_id1='730 731 732 733 734 735 736 860 861 862 863 864 865 866 867 869'
src_tablename='tab_gsmvoicdr4 tab_cdmavoicdr4'
res_GORC='conn:src_tablename gsm cdma '
res_MONTH='conn:src_tablename 200610 200610'
