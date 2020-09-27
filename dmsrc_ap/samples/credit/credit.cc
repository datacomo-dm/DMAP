#include "dt_common.h"
#include "AutoHandle.h"

//#define TESTCOND " and rownum<10000"
#define TESTCOND 
int Start(void *ptr);
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("stat/credit");
    if(argc!=2) {
    	printf("Usage: credit <year_month>\nFor examples: credit 200612 .\n");
    	return -1;
    }
    nRetCode=wociMainEntrance(Start,false,argv[1],2);
    WOCIQuit(); 
    return nRetCode;
}

void setMTSTR(AutoMt &mt,const char *col,const char *val)
{
	char *ptr;
	int len;
	mt.GetStrAddr(col,0,&ptr,&len);
	if(strlen(val)>len) ThrowWith("Set value '%' to column '%s' out of length.",val,col);
	int rn=mt.GetRows();
	while(rn--) {
		strcpy(ptr,val);ptr+=len;
	}
}
	
int Start(void *ptr) { 
    const char *month=(const char *)ptr;
    int rn;
    wociSetOutputToConsole(TRUE);
    wociSetEcho(TRUE);
    printf("连接到数据库...\n");
    AutoHandle obs,cas;
    obs.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.1.2:1526/obs9i",DTDBTYPE_ORACLE));
    cas.SetHandle(wociCreateSession("wanggsh","wanggsh","//130.86.1.2:1526/cas9i",DTDBTYPE_ORACLE));
    
    //计算统计时间段
    char hfymonth[10];
    {
    	AutoMt tmp(obs,10);
    	tmp.FetchAll("select to_char(add_months(to_date('%s','yyyymm'),-6),'yyyymm') from dual",month);
    	tmp.Wait();
    	strcpy(hfymonth,tmp.PtrStr(0,0));
    }
    printf("半年的统计时间: %s--%s.\n",hfymonth,month);

    //取用户档案
    lgprintf("取基本用户资料..");
    AutoMt subscrb(obs,5000000);
    wociAddColumn(subscrb,"stype",NULL,COLUMN_TYPE_CHAR,2,0);
    wociAddColumn(subscrb,"acctid",NULL,COLUMN_TYPE_INT,9,0);
    wociAddColumn(subscrb,"stoptimes",NULL,COLUMN_TYPE_INT,4,0);
    wociAddColumn(subscrb,"exptimes",NULL,COLUMN_TYPE_INT,4,0); //未及时缴费次数
    wociAddColumn(subscrb,"sumfee",NULL,COLUMN_TYPE_NUM,10,2);
    wociAddColumn(subscrb,"credit",NULL,COLUMN_TYPE_NUM,10,2);
    wociAddColumn(subscrb,"cgrade",NULL,COLUMN_TYPE_CHAR,2,0);

    subscrb.SetAutoClear(false);
    subscrb.FetchAll("select areaid,ceil(months_between(to_date('%s','yyyymm'),opendate)) innetmths,subscrbid,svcid "
                      " from obs.tab_subscrb where ceil(months_between(to_date('%s','yyyymm'),opendate))>=6 "
                      " and nvl(closedate,sysdate+300)>to_date('%s','yyyymm') and svcstat not in ('9','B') and svcid in ('10','20')" TESTCOND
                      ,month,month,month);
    subscrb.Wait();
    lgprintf("排序..");
    wociSetSortColumn(subscrb,"subscrbid");
    wociSort(subscrb);
    setMTSTR(subscrb,"stype","B");
    
    lgprintf("取帐户对照表...");
    AutoMt srctab(obs,50000);
    srctab.FetchFirst("select acctid,subscrbid from obs.tab_acctrelation where effdate<to_date('%s','yyyymm') and "
                " nvl(expdate,sysdate)>to_date('%s','yyyymm') " TESTCOND ,month,month);
    rn=srctab.Wait();
    while(rn>0) {
    	wociValuesSet(subscrb,srctab,"subscrbid;acctid","subscrbid;acctid",true,VALUESET_SET);
    	srctab.FetchNext();
    	rn=srctab.Wait();
    }
     
    lgprintf("取半年内消费总额...");
    wociSetDefaultPrec(9,0);
    srctab.FetchFirst("select fee,to_number(subscrbid) subscrbid from obs.rep_hftj where month between %s and %s" TESTCOND,
       hfymonth,month);
    rn=srctab.Wait();
    while(rn>0) {
    	wociValuesSet(subscrb,srctab,"subscrbid;sumfee","subscrbid;fee",true,VALUESET_ADD);
    	srctab.FetchNext();
    	rn=srctab.Wait();
    }

    lgprintf("取半年内停机次数...");
    srctab.FetchFirst("select subscrbid,1 st from obs.TAB_STATUS_HISTORY where "
    	" NEWSTATUS ='4' and STATCHGDATE between to_date('%s','yyyymm') and to_date('%s','yyyymm') " TESTCOND ,
    	   hfymonth,month);
    rn=srctab.Wait();
    while(rn>0) {
    	wociValuesSet(subscrb,srctab,"subscrbid;stoptimes","subscrbid;st",true,VALUESET_ADD);
    	srctab.FetchNext();
    	rn=srctab.Wait();
    }
    
    lgprintf("取非正常缴费...");
    srctab.SetDBC(cas);
    char *pareaid[]={"086730","086731","086732","086733","086734","086735","086736","086860","086861","086862","086863",
		"086864","086865","086866","086867","086869",NULL     };

    char **ptmp=pareaid;
    while(*ptmp!=NULL) {
     srctab.FetchFirst("select subscrbid,1 st from yncas.tab_billdetail_%s where "                                                     
    	" months_between(nvl(recstatchgdate,sysdate+360),to_date(billingcyclid,'yyyymm'))>1.0 "                                 
    	" and billingcyclid>='%s' and recstat not in('15','20')" TESTCOND,*ptmp,month);                                                           
    	ptmp++;
     rn=srctab.Wait();                                                                                                              
     while(rn>0) {                                                                                                                  
    	wociValuesSet(subscrb,srctab,"subscrbid;exptimes","subscrbid;st",true,VALUESET_ADD);                                       
    	srctab.FetchNext();                                                                                                        
    	rn=srctab.Wait();                                                                                                          
     }                                                                                                                              
    }                                                                                                           
    lgprintf("取缴费类型：是否托收...");//(A 类缴费用户的exptimes需要在后续处理中忽略)                                             
    lgprintf("排序..");
    srctab.SetDBC(obs);
    wociSetSortColumn(subscrb,"acctid");                                                                                           
    wociSort(subscrb);                                                                                                             
    srctab.FetchFirst("select acctid,cast(decode(paytype,'15','A','B') as varchar(2)) stype from obs.tab_acct where stat=1 " TESTCOND );
    rn=srctab.Wait();
    while(rn>0) {
    	wociValuesSet(subscrb,srctab,"acctid;stype","acctid;stype",true,VALUESET_SET);
    	srctab.FetchNext();
    	rn=srctab.Wait();
    }
    
    /*
    //未见地市分公司配置有区别,简化起见,暂时直接计算,抛开参数表!
    //两类用户的在网时间，消费额度分值评估中占的权重,PAYW means what ?
    AutoMt weight(cas,1000);
    weight.FetchAll("select * from yncas.TAB_CREDIT_WEIGHT");
    weight.Wait();
    //分段划分评分
    AutoMt rule(cas,1000);
    rule.FetchAll("select * from yncas.TAB_CREDIT_EVALUATE_RULE");
    rule.Wait();
    wociSetSortColumn(rule,"areaid,svcid,ev_item");
    wociSort(rule);
    //信用等级划分方法
    AutoMt grade(cas,1000);
    grade.FetchAll("select * from yncas.TAB_CREDIT_GRADE_RULE");
    grade.Wait();
    */
    
    lgprintf("计算：信用分值(规则和权重按地市分公司配置)，等级...");
    
    rn=subscrb.GetRows();
    double value=0,credit=0;
    char *grade;
    for(int i=0;i<rn;i++) {
	credit=0;
    	bool isgsm=false;
    	if(subscrb.PtrStr("svcid",i)[0]=='1') isgsm=true;
    	if(subscrb.PtrStr("stype",i)[0]=='A') {
    		value=6-subscrb.GetInt("exptimes",i);//及时缴费月份
    		value=value<3?0:(value<5?3:(value<6?8:10));
    		credit+=value*(isgsm?6:7);
    	}
        else {    		
    		value=subscrb.GetInt("stoptimes",i); //停机次数
    		value=value<1?10:(value<3?8:(value<5?3:0));
    		credit+=value*(isgsm?6:7);
    	}
    	value=subscrb.GetInt("innetmths",i); //在网月数
    	value=value<7?0:(value<10?2:(value<13?4:(value<19?6:(value<24?8:10))));
    	credit+=value*(isgsm?3:2);
    	value=subscrb.GetDouble("sumfee",i)/6; //半年的月均消费
    	value=value<50?0:(value<100?2:(value<200?4:(value<300?6:(value<500?8:10))));
    	credit+=value;
    	grade=subscrb.PtrStr("cgrade",i);
    	strcpy(grade,credit<31?"E":(value<45?"D":(value<70?"C":(value<85?"B":"A"))));
    	value=subscrb.GetDouble("sumfee",i)/6; //半年的月均消费
    	credit=value* (credit<31?0:(credit<45?0.5:(credit<70?0.8:(credit<85?1.0:1.5))));
    	credit=(int(credit/10))*10;//按10向下取整
        *subscrb.PtrDouble("credit",i)=credit;
    }
    lgprintf("保存数据...");
    AutoHandle bi;
    bi.SetHandle(wociCreateSession("ods_temp","ods_temp","//130.86.12.30:1521/ubisp.ynunicom.com",DTDBTYPE_ORACLE));
    char tab[100];
    sprintf(tab,"tab_subscrb_credit_%s",month);
    subscrb.CreateAndAppend(tab,bi);
    lgprintf("正常结束");
    return 1;
}
