#统计一个地市的会员明细
# 结果写到表 tab_memberlist_XXX
load ./myhello.so
dbSetEcho true
dbSetOutputToConsole true
#set obsbc [dbCon wanggsh wanggsh //130.86.1.2:1526/obs9i ]
set yncrm [dbCon wanggsh wanggsh //130.86.4.3/yncrm ]
# set obs   [dbCon wanggsh wanggsh //130.86.12.18:1522/obs9i ]
#set casbc   [dbCon wanggsh wanggsh //130.86.1.2:1526/cas9i ]
set usrtab [dbMt [$yncrm id]]
$usrtab SetMaxRows  240000
#附加字段，用于存放积分兑换项目
dbAddColumn [$usrtab id] removedate date 7 0
dbAddColumn [$usrtab id] joindate date 7 0
#保留附加字段，在执行SQL的时候不清除已有的字段
$usrtab SetAutoClear false
$usrtab FetchSQL "select companyid,usrid,mphonecode,usrname,0 haveemp,decode(cardtypeid,'03',1,'02',2,3) grade	,\
   0 intuse,substr(feesetcode,4,5) prcplnid,0.0 m3sumfee,to_char(opendate,'yyyy') openyear,to_char(opendate,'mm') openmonth \
  from yncrmnew.tab_usrresult where netcode='G' and removetag='0' and /*mphonecode like '%6' and */cardtypeid in ('01','02','03')"
 
$usrtab Wait
$usrtab Sort usrid

#取积分兑换记录
set midmt [dbMt [$casbc id]]
$midmt SetMaxRows 150000
$midmt FetchSQL "select lpad(to_char(a.subscrbid),16,'0') usrid,c.integraldef,1 as havegift,TO_CHAR(a.paydate,'yyyymmdd') \
 from yncas.tab_integraldtal a ,obs.tab_integralcashdtal@dblnk_yn18 b,obs.tab_integralcash@dblnk_yn18 c \
where a.recordtype='60' and a.svcid='10' and a.integralsn=b.cashsn and b.sn=c.sn and c.state='1' \
order by a.paydate "
$midmt Wait
$midmt Sort usrid
dbValuesSet [$usrtab id] "usrid;intuse" [$midmt id] "usrid;havegift" true set 
dbValuesSet [$usrtab id] "usrid;integraldef" [$midmt id] "usrid;integraldef" true set 

#取品牌
set midmt [dbMt [$obsbc id]]
$midmt SetMaxRows 50000
$midmt FetchSQL "select to_char(prcplnid) prcplnid,brand from obs.tab_prcpln_brand"
$midmt Wait
$midmt Sort prcplnid
dbValuesSet [$usrtab id] "prcplnid;brand" [$midmt id] "prcplnid;brand" false set

#取客户经理
set midmt [dbMt [$yncrm id]]
$midmt SetMaxRows 50000
$midmt FetchFirstSQL "select usrid,decode(allotflag,'2',1,0) haveemp from yncrmnew.tab_usrfix"
set rn [$midmt Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "usrid;haveemp" [$midmt id] "usrid;haveemp" true set
 $midmt FetchNext
 set rn [$midmt Wait]
}
#取上月费用
$midmt SetMaxRows 50000
$midmt FetchFirstSQL "select usrid,m3sumfee from yncrmnew.tab_usrmonthfee where dealdate=200903 and m3sumfee>0"
set rn [$midmt Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "usrid;m3sumfee" [$midmt id] "usrid;m3sumfee" true set
 $midmt FetchNext
 set rn [$midmt Wait]
}


dbSetMTName [$usrtab id] 品牌会员数据
dbMTToTextFile [$usrtab id] "memberbrand.txt" 0 ""
$usrtab CreateAndAppend tab_member_brand090415 0 true true
#输出数据
#select '云南',b.companyname,mphonecode,usrname,'世界风',decode(haveemp,1,1,null),grade,decode(intuse,0,null,intuse),
#integraldef,openyear,openmonth 
#from wanggsh.tab_member_brand090415 a ,tab_company b 
#where brand='SJF' and mphonecode like '%6_' and m3sumfee>0
# and a.companyid=b.companyid
# order by b.companyname