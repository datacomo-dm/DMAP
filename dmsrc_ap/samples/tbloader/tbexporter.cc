#include "tbexport_helper.h"

/*
   ./tbexporter csvfile,dsn,user,pwd,sql
*/
const char* msg = "tbexporter:调用参数输入错误.\n"
    "         [导出文件名称][ODBC数据源][数据库用户名][数据库密码][\"导出sql语句\"]\n"
    "         example : ./tbexporter csvfile dsn user pwd \"sql\"\n\n";
int main(int argc,char *argv[])  
{                   
    if(argc != 6){
        printf(msg);
        return 0;	
    }

    WOCIInit("tbexporter");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);	
    
	int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.fn_ext,argv[index++]);
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
	char sql[50000];
	strcpy(sql,argv[index++]);
	
	tbexport tbobj;
    tbobj.setStatus(exp_unstart);
    tbobj.setId(1);
	tbobj.start(&stInitInfo,sql,"tbexporter");
	printf("exporting csv file: %s , current rows[%d],sum rows[%d]\n",stInitInfo.fn_ext,tbobj.getCurrentRows(),tbobj.getRowSum());
	tbobj.doStart();
    while(1){
	   if(tbobj.getCurrentRows() == tbobj.getRowSum()){
	   	break;
	   }
	   // printf("exporting csv file: %s , current rows[%d],sum rows[%d]\n",stInitInfo.fn_ext,tbobj.getCurrentRows(),tbobj.getRowSum());
	   sleep(1);
    }
	tbobj.stop();

    WOCIQuit(); 
	return 0;
}

