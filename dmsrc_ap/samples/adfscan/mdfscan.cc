#include <stdio.h>
#include <stdlib.h>
#include "dt_svrlib.h"


const char* pmsg = "mdfdump: 1).将导出完成的数据文件进行读取校验\n"
                   "mdfdump [index file(in)] [data file(in)] [dpn(in,default:dp)] [user(in,default:root)] [passwd(in,default:dbplus03)]\n"
                   "example : mdfdump /app/dma/temp/mdidx_290478.dat /app/dma/temp/mddt_290478.dat \n\n";

typedef struct stru_param{
    char  idx_file[256];
    char  dat_file[256];
    char  dns_name[256];
    char  user_name[256];
    char  pwd_name[256];    
}stru_param,*stru_param_ptr;

int main(int argc,char **argv)
{
    if(argc != 3 && argc !=6) {
        printf(pmsg);
        return 1;
    }

    stru_param st_param;
    if(argc == 3){
        strcpy(st_param.idx_file,argv[1]);
        strcpy(st_param.dat_file,argv[2]);
        strcpy(st_param.dns_name,"dp");
        strcpy(st_param.user_name,"root");
        strcpy(st_param.pwd_name,"dbplus03");
    }else{
        strcpy(st_param.idx_file,argv[1]);
        strcpy(st_param.dat_file,argv[2]);
        strcpy(st_param.dns_name,argv[3]);
        strcpy(st_param.user_name,argv[4]);
        strcpy(st_param.pwd_name,argv[5]);
    }
    AutoHandle dts;
    dts.SetHandle(wociCreateSession(st_param.user_name,st_param.pwd_name,st_param.dns_name,DTDBTYPE_ODBC));

    int rownum = 0;
    dt_file df_idx;
    df_idx.Open(st_param.idx_file,0);
    rownum = df.GetRowNum();
    


    return 0;
}


