#include "dt_common.h"
#include "dt_lib.h"
#include "dt_cmd_base.h"
#include <string.h>
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
int Start(void *ptr);
void ShowDoubleByte() ;
//#define dtdbg(a) lgprintf(a)
#define dtdbg(a)
#ifdef __unix
pthread_t hd_pthread_loop;
#endif

const char *helpmsg=
    "参数 : \n"
//  "-f {check,dump,mload,dload,dc,all} \n"
    "-f {dump,mload,dload,dc,all} \n"
    "    功能选择,缺省为dump以外所有功能 \n"
    "    可以选择其中的一个: \n"
    "    dump: 数据抽取,可以用-ds选项控制源系统类型 \n"
    "    mload: 数据预处理 \n"
    "    dload: 数据装入 \n"
    "    dc: 二次压缩数据 \n"
    "    all: (缺省),mload+dload+dc \n"
    " -ds '1,2,3' \n"
    "   只从dp_code表的code_type=9所列出的类型抽取数据\n"
    "-rm [数据库名] [表名] [分区名称]\n"
    "    删除" DBPLUS_STR "表分区\n"
    "-rmi [数据库名] [表名] [分区ID]\n"
    "    删除" DBPLUS_STR "表分区[通过分区ID删除，不进行交互性提示]\n"
    "-rma [数据库名] [表名] \n"
    "    删除" DBPLUS_STR "整表\n"
    "-rma_force [数据库名] [表名] \n"
    "    强制删除" DBPLUS_STR "整表\n"
    "-bk [数据库名] [表名] [备份路径]\n"
    "    备份数据库表.\n"
    "-bka [数据库名] [表名] [备份路径]\n"
    "    备份数据库表(备份数据库表结构).\n"
    "-rs [数据库名] [表名] [还原数据表路径]\n"
    "    还原数据库表.\n"
    "-rsa [数据库名] [表名] [还原数据表路径]\n"
    "    还原数据库表(还原数据库表结构).\n"
    "-mv [源数据库] [源表] [目标数据库] [目标表]\n"
    "    表移动(改名),只有对该表不在查询的时候才能够进行mv操作.\n"
//  "-mb [database name] [table name] \n"
//  "    转化" DBPLUS_STR "表到 MyISAM 块格式\n"
//  "-hmr [数据库名] [表名] \n"
//  "    同构(不需要数据重组)索引重建.\n"
    "-ck [数据库名] [表名]\n"
    "    检查表结构和数据.\n"
    "-si [数据库名] [表名]\n"
    "    查看数据库表所占存储空间.\n"
    "-cl [参考库名] [参考表名] [新源数据库名] [新源表名] [新目标数据库名] [新目标表名] [任务开始时间]\n"
    "    按参考表类似创建源表/分区/目标表/索引/任务信息.\n"
    "    开始时间可以输入now表示现在的系统时间，\n"
    "       或者如\"2000-01-01 12:00:00\"格式(注意需要双引号).\n"
    "-clf [参考库名] [参考表名] [新源数据库名] [新源表名] [新目标数据库名] [新目标表名] [任务开始时间]\n"
    "    按参考表类似创建源表/分区/目标表/索引/任务信息,保留格式表.\n"
    "    开始时间可以输入now表示现在的系统时间，\n"
    "       或者如\"2000-01-01 12:00:00\"格式(注意需要双引号).\n"

    "-csit [源数据库服务名] [源数据库用户名] [源数据库用户密码] [源数据库名] [源表名] [目标数据库名] [目标表名] \n"
    " [任务开始时间,可以用now() 或者如 \"2013-01-01 12:00:01 \"] [临时路径，不能存在空格] [数据路径(整理后的数据存放路径)，不能存在空格] \n"
    " [索引名称(使用\",\"进行分割)，默认为\"\"，如果索引名称为空，则会到索引文件中获取独立索引] [索引列表文件名] \n"
    " [抽取语句,默认值用\"default\"]\n"
    " 例如: -csit //192.168.1.10:1523/data_source srcUserName srcUserPasswd srcDbName srcTabName dstDbName dstTabName \"now()\"  /app/datamerger/temp /app/datamerger/data  \"\"  /app/datamerger/ctl/solid_index_file.ctl  \"default\" \n"

    "-enc (text)\n"
    "    密码加密 \n"
    "-mh  (dsn)\n"
    "    到本地" DBPLUS_STR " Server 的ODBC 连接名 \n"
    "-mun (username) \n"
    "    到本地" DBPLUS_STR " Server 的用户名\n"
    "-mpwd [password] \n"
    "    到本地" DBPLUS_STR " Server 的连接密码\n"
//  "-osn (oracle service name) \n"
// "    for Oracle AgentServer connection\n"
//  "-oun (oracle username) \n"
//  "    for Oracle AgentServer connection \n"
//  "-opwd [Oracle password] \n"
//  "    for Oracle AgentServer connection \n"
    "-lp 	\n"
    "   循环执行模式\n"
//  "-fr    \n"
//  "    calculate freeze time. \n"
    "-v	\n"
    "    详细输出模式.\n"
    "-nv	\n"
    "    取消详细输出模式.\n"
    "-e	\n"
    "   允许控制台输出执行信息.\n"
    "-ne	\n"
    "   取消允许控制台输出执行信息.\n"
    "-wt ( seconds) \n"
    "   循环执行模式的睡眠时间(秒)\n"
    "-thn (threadnum) \n "
    "   二次数据压缩线程数(1-16)\n"
    "-h \n"
    "    帮助信息 \n"
    "\n---------------------------------------------------------\n"
    "使用以下环境变量:\n"
//  "DP_INDEXNUM \n"
//  "   数据装入时的块大小:50-300(MB)\n"
    "DP_SERVERIP \n"
    "   服务器的IP地址 \n"
    "DP_MTLIMIT \n"
    "   数据抽取时的块大小:50-20000(MB)\n"
//  "DP_BLOCKSIZE \n"
//  "   " DBPLUS_STR "数据块大小:100-512 (KB)\n"
    "DP_LOADTIDXSIZE \n"
    "   中间索引内存限制:50-20000 (KB)\n"
    "DP_WAITTIME \n"
    "   循环执行模式的睡眠时间(秒)\n"
    "DP_ECHO \n"
    "   控制台输出执行信息.1:打开,0:关闭\n"
    "DP_VERBOSE \n"
    "    详细输出模式.1:打开,0:关闭\n"
    "DP_LOOP \n"
    "   循环执行模式\n"
    "DP_THREADNUM \n"
    "   二次数据压缩线程数(1-16)\n"
    "DP_FUNCODE {DUMP MLOAD DLOAD DC ALL} \n"
    "    功能选择,缺省为DUMP以外所有功能 \n"
    "    可以选择其中的一个: \n"
    "    DUMP: 数据抽取 \n"
    "    MLOAD: 数据预处理 \n"
    "    DLOAD: 数据装入 \n"
    "    DC: 二次压缩数据 \n"
    "    ALL: (缺省),MLOAD+DLOAD+DC \n"
    "DP_MPASSWORD  \n"
    "    到本地" DBPLUS_STR " Server 的连接密码\n"
    "    要设置为加密后的密码\n"
    "    使用参数 -enc <密码明文> 加密\n"
    "DP_MUSERNAME \n"
    "    连接" DBPLUS_STR " Server 的用户名\n"
    "DP_DSN \n"
    "    连接" DBPLUS_STR " Server 的ODBC 连接名 \n"
    "DP_LOAD_DATA_FILE_SIZE \n"
    "    数据装入过程，一次装入多少M的文件,默认值2048\n"
    "DP_LOAD_AS_LAST_FILE \n"
    "    数据装入过程，创建包索引<packindex>的方式，默认为1\n"
//  "DP_ORAPASSWORD \n"
//  "   password  for Oracle AgentServer connection\n"
//  "DP_ORAUSERNAME \n"
//  "   username for Oracle AgentServer connection\n"
//  "DP_ORASVCNAME \n"
//  "   service name for Oracle AgentServer connection\n"
    ;

#ifndef DPADMIN_VERSION
#define DPADMIN_VERSION    "dpadmin 1.6.0"
#endif
void DpadminPrintfVersion()
{
    lgprintf("DPADMIN-[%s]",DPADMIN_VERSION);
}

//>> Begin:DM-228 创建数据任务表辅助参数结构
typedef struct CrtTaskTableAssistParam {
    char solid_index_list[256];                // 独立索引名称，逗号分隔
    char tmp_path[PARAMLEN];                   // 临时路径
    char back_path[PARAMLEN];                  // 备份路径
    char solid_index_list_file[PARAMLEN];      // 独立索引列表文件名称
    char ext_sql[4000];                        // 导出sql语句
    CrtTaskTableAssistParam()
    {
        tmp_path[0] = 0;
        back_path[0] = 0;
        solid_index_list_file[0] = 0;
        ext_sql[0] = 0;
        solid_index_list[0] = 0;
    };
} CrtTaskTableAssistParam,*CrtTaskTableAssistParamPtr;
void Str2Lower(char *str)
{
    while(*str!=0) {
        *str=tolower(*str);
        str++;
    }
}
//<< End:DM-228

struct   cmdparam:public cmd_base {

    //>> Begin:DM-228
    char orasvcname[PARAMLEN];
    char orausrname[PARAMLEN];
    char orapswd[PARAMLEN];
    CrtTaskTableAssistParam   objCrtTskTabAssistParam;
    //<< End: DM-228

    char srcdbn[PARAMLEN];
    char srctbn[PARAMLEN];
    char srcpartname[PARAMLEN];
    char dstdbn[PARAMLEN];
    char dsttbn[PARAMLEN];
    char srcowner[PARAMLEN];
    char srctable[PARAMLEN];
    char taskdate[41];
    char dumpstrains[100];
    //char lgfname[PARAMLEN];
    enum {
        DP_NULL=0,DP_CHECK,DP_DUMP,DP_MDLDR,DP_DSTLDR,DP_DEEPCMP,DP_ALL,DP_MOVE,DP_BACKUP,DP_RESTORE,DP_BACKUP_ALL,DP_RESTORE_ALL,DP_REMOVE,DP_REMOVE_ALL,DP_REMOVE_ALL_FORCE,DP_GET_SIZE,DP_REMOVE_BY_ID,DP_MYSQLBLOCK,DP_ENCODE,DP_HOMO_REINDEX,DP_CREATELIKE,DP_CREATELIKE_PRESERVE_FORMAT,DP_CREAT_SOLID_INDEX_TABLE,DP_LAST
    } funcid;

    int load_tidxmem;
    int indexnum;
    int mtlimit;
    int threadnum;
    int blocksize;
    bool directIOSkip,useOldBlock;
    cmdparam():cmd_base()
    {
        threadnum=1;
        freezemode=false;
        blocksize=512;
    }

    void GetEnv()
    {
        cmd_base::GetEnv();
        const char *fncode=getenv("DP_FUNCODE");
        dumpstrains[0]=0;
        //set DP_FUNCODE or -f(-f check,-f dump,-f mload,-f dload,-f dc,-f all) parameter.
        if(fncode==NULL)
            funcid=DP_ALL;
        //else if(strcmp(fncode,"CHECK")==0) funcid=DP_CHECK;
        else if(strcmp(fncode,"DUMP")==0) funcid=DP_DUMP;
        else if(strcmp(fncode,"MLOAD")==0) funcid=DP_MDLDR;
        else if(strcmp(fncode,"DLOAD")==0) funcid=DP_DSTLDR;
        else if(strcmp(fncode,"DC")==0) funcid=DP_DEEPCMP;
        else if(strcmp(fncode,"ALL")==0) funcid=DP_ALL;
        else funcid=DP_ALL;
        SetValue(threadnum,getenv("DP_THREADNUM"));
        SetValue(mtlimit,getenv("DP_MTLIMIT"));
//      SetValue(indexnum,getenv("DP_INDEXNUM"));
        SetValue(blocksize,getenv("DP_BLOCKSIZE"));
        if(blocksize<1) blocksize=512; // default block size is 512 KB
        SetValue(load_tidxmem,getenv("DP_LOADTIDXSIZE"));
        SetValue(directIOSkip,getenv("DP_SKIP_DIRECT_IO"));
        SetValue(useOldBlock,getenv("DP_USE_OLD_BLOCK"));
    }
    bool checkvalid()
    {
        bool ret=cmd_base::checkvalid();
        if(funcid==DP_NULL || funcid>=DP_LAST) {
            printf("Invalid function selection code ,set DP_FUNCODE or -f (dump,mload,dload,dc,all) option..\n");
            ret=false;
        }
        if(load_tidxmem<50) {
            printf("中间索引记录内存使用%dMB ,修改为 50MB\n",load_tidxmem);
            load_tidxmem=50;
        }
        if(load_tidxmem>MAX_LOADIDXMEM) {
            printf("中间索引记录条数使用%dMB ,修改为 %dMB\n",load_tidxmem,MAX_LOADIDXMEM);
            load_tidxmem=MAX_LOADIDXMEM;
        }
        //indexnum固定使用10MB
        indexnum=10;
        /*if(indexnum<50) {
            printf("索引内存使用%d ,修改为 50MB\n",indexnum);
            indexnum=50;
        }
        if(indexnum>300) {
            printf("索引内存使用%d ,修改为 300MB\n",indexnum);
            indexnum=300;
        }
        */
        if(mtlimit<50) {
            printf("数据抽取内存%d, 修改为 50MB\n",mtlimit);
            mtlimit=50;
        }
        if(mtlimit>MAX_LOADIDXMEM) {
            printf("数据抽取内存%d, 修改为 %dGB\n",mtlimit,MAX_LOADIDXMEM/1000);
            mtlimit=MAX_LOADIDXMEM;
        }
        if(blocksize<100) {
            printf("数据块大小%d, 修改为 100KB\n",mtlimit);
            blocksize=100;
        }
        if(blocksize>512) {
            printf("数据块大小%d, 修改为 512KB\n",mtlimit);
            blocksize=512;
        }
        //mtlimit=650000000;
        if(threadnum<1 || threadnum>16) {
            printf("并发数据压缩线程数只能是1-16,修改为1.\n");
            threadnum=1;
        }
        if(strlen(serverip)<1) {
            printf("未指定DP_SERVERIP参数,使用默认值localhost.\n");
            strcpy(serverip,"localhost");
        }

        return ret;
    }

    bool ExtractParam(int &p,int n,char **argv)
    {
        if(cmd_base::ExtractParam(p,n,argv)) return true;
        if(strcmp(argv[p],"-f")==0) {
//        if(p+1>=n) ThrowWith("Need function code,use one of 'check,dump,mload,dload,dc,all'");
            if(p+1>=n) ThrowWith("缺少功能代码,使用'dump,mload,dload,dc,all'之一");
            p++;
            if(strcmp(argv[p],"dump")==0) funcid=DP_DUMP;
            //else if(strcmp(argv[p],"check")==0) funcid=DP_CHECK;
            else if(strcmp(argv[p],"mload")==0) funcid=DP_MDLDR;
            else if(strcmp(argv[p],"dload")==0) funcid=DP_DSTLDR;
            else if(strcmp(argv[p],"dc")==0) funcid=DP_DEEPCMP;
            else if(strcmp(argv[p],"all")==0) funcid=DP_ALL;
//        else ThrowWith("Invalid function code '%s',use one of 'check,dump,mload,dload,dc,all'",argv[p]);
            else ThrowWith("错误的功能代码 '%s',使用'dump,mload,dload,dc,all'之一",argv[p]);
        } else if(strcmp(argv[p],"-thn")==0) {
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-thn 选项需要指定线程数.");
            threadnum=atoi(argv[++p]);
        } else if(strcmp(argv[p],"-ds")==0) {
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-ds 选项需要指定限制类型.");
            strcpy(dumpstrains,argv[++p]);
        } else if(strcmp(argv[p],"-hmr")==0) {
            funcid=DP_HOMO_REINDEX;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("同构索引重建(-hmr)数据需要指定库名和表名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("同构索引重建(-hmr)数据需要指定库名和表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-ck")==0) {
            funcid=DP_CHECK;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("数据检查(-ck)需要指定库名和表名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("数据检查(-ck)需要指定库名和表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-si")==0) {
            funcid=DP_GET_SIZE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("表空间获取(-si)需要指定库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("表空间获取(-si)需要指定表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-mv")==0) {
            funcid=DP_MOVE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("表移动(-mv)需要指定 来源/目标 库名和表名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("表移动(-mv)需要指定 来源/目标 库名和表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("表移动(-mv)需要指定 来源/目标 库名和表名.");
            if(p+4>=n || argv[p+4][0]=='-') ThrowWith("表移动(-mv)需要指定 来源/目标 库名和表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(dstdbn,argv[++p]);
            strcpy(dsttbn,argv[++p]);
        } else if(strcmp(argv[p],"-cl")==0 || strcmp(argv[p],"-clf")==0) {
            if(strcmp(argv[p],"-cl")==0)
                funcid=DP_CREATELIKE;
            else funcid=DP_CREATELIKE_PRESERVE_FORMAT;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("类似创建需要指定参考数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("类似创建需要指定参考表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("类似创建需要指定新源数据库名.");
            if(p+4>=n || argv[p+4][0]=='-') ThrowWith("类似创建需要指定新源表名.");
            if(p+5>=n || argv[p+5][0]=='-') ThrowWith("类似创建需要指定新数据库名.");
            if(p+6>=n || argv[p+6][0]=='-') ThrowWith("类似创建需要指定新表名.");
            if(p+7>=n || argv[p+7][0]=='-') ThrowWith("类似创建需要指定任务开始时间.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcowner,argv[++p]);
            strcpy(srctable,argv[++p]);
            strcpy(dstdbn,argv[++p]);
            strcpy(dsttbn,argv[++p]);
            strcpy(taskdate,argv[++p]);
        }
        //>>Begin:DM-228
        else if(strcmp(argv[p],"-csit") == 0) {
            funcid = DP_CREAT_SOLID_INDEX_TABLE;
            /*
            example:
            dpadmin -csit localhost:1522 dbuser dbpasswd srcdb srctable1 dstdb dsttable1 "now()"
              /tmp/temp_path /tmp/back_path 5 createtable.lst "select * from srcdb.srctable1"
            */
            // 共14个参数
            if(p+14 != n) {
                ThrowWith("创建独立索引任务表参数错误. 请参考:dpadmin --h");
            }

            // ora svc ,user,name
            strcpy(orasvcname,argv[++p]);
            Trim(orasvcname);
            strcpy(orausrname,argv[++p]);
            Trim(orausrname);
            strcpy(orapswd,argv[++p]);
            Trim(orapswd);

            // src db & table
            strcpy(srcdbn,argv[++p]);
            Trim(srcdbn);
            strcpy(srctbn,argv[++p]);
            Trim(srctbn);

            // dst db & table
            strcpy(dstdbn,argv[++p]);
            Trim(dstdbn);
            Str2Lower(dstdbn);
            strcpy(dsttbn,argv[++p]);
            Trim(dsttbn);
            Str2Lower(dsttbn);

            // tack date
            strcpy(taskdate,argv[++p]);

            // temp path,dest path,
            strcpy(objCrtTskTabAssistParam.tmp_path,argv[++p]);
            strcpy(objCrtTskTabAssistParam.back_path,argv[++p]);

            // compress type,solid index file name
            strcpy(objCrtTskTabAssistParam.solid_index_list,argv[++p]);
            strcpy(objCrtTskTabAssistParam.solid_index_list_file,argv[++p]);

            // get the extsql
            if(strcasestr(argv[p+1],"default") != NULL) {
                sprintf(objCrtTskTabAssistParam.ext_sql,"select * from %s.%s",srcdbn,srctbn);
            } else {
                strcpy(objCrtTskTabAssistParam.ext_sql,argv[p+1]);
            }
            p++;
        } // end  else if(strcmp(argv[p],"-csit")
        //<<End:Dm-228
        else if(strcmp(argv[p],"-enc")==0) {
            funcid=DP_ENCODE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("No text to encode!.");
            strcpy(srcdbn,argv[++p]);
        } else if(strcmp(argv[p],"-rm")==0) {
            funcid=DP_REMOVE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("删除表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("删除表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("删除表需要指定分区名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else if(strcmp(argv[p],"-rma")==0) {
            funcid=DP_REMOVE_ALL;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("删除表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("删除表需要指定表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-rma_force")==0) {
            funcid=DP_REMOVE_ALL_FORCE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("删除表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("删除表需要指定表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-rmi")==0) {
            funcid=DP_REMOVE_BY_ID;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("删除表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("删除表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("删除表需要指定分区ID.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else if(strcmp(argv[p],"-mb")==0) {
            funcid=DP_MYSQLBLOCK;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("块格式转换需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("块格式转换需要指定表名.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        } else if(strcmp(argv[p],"-bk")==0) {
            funcid=DP_BACKUP;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("备份表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("备份表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("备份表需要指定备份文件保存路径.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else if(strcmp(argv[p],"-rs")==0) {
            funcid=DP_RESTORE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("还原表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("还原表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("还原表需要指定还原文件完整路径.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else if(strcmp(argv[p],"-bka")==0) {
            funcid=DP_BACKUP_ALL;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("备份表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("备份表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("备份表需要指定备份文件保存路径.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else if(strcmp(argv[p],"-rsa")==0) {
            funcid=DP_RESTORE_ALL;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("还原表需要指定数据库名.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("还原表需要指定表名.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("还原表需要指定还原文件完整路径.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcpartname,argv[++p]);
        } else ThrowWith("错误的命令参数 :'%s'",argv[p]);

        Str2Lower(srcdbn);
        Str2Lower(srctbn);
        Str2Lower(dstdbn);
        Str2Lower(dsttbn);

        return true;
    }

};
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;

cmdparam cp;

const char *GetDPLibVersion();
#define CONNECT_SYNC_FETCH  "DP_CONNECT_SYNC_FETCH"
int main(int argc,char *argv[])
{
    int nRetCode = 0;
    if(argc==2 && (strcmp(argv[1],"-h")==0 || strcmp(argv[1],"-help")==0 || strcmp(argv[1],"--h")==0 ||
                   strcmp(argv[1],"--help")==0 || strcmp(argv[1],"help")==0
                  ) ) {
        printf("帮助信息. \n\n %s",helpmsg);
        return 0;
    }
    if(argc==3 && (strcmp(argv[1],"-3396709")==0 ) ) {
        char str[100];
        strcpy(str,argv[2]);
        decode(str);
        printf("帮助信息. \n\n %s",str);
        return 0;
    }
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    int failretry=0;
    do {
        WOCIInit("dpadmin");
        char* pconn_syn_fetch = getenv(CONNECT_SYNC_FETCH);
        if(NULL != pconn_syn_fetch && strcasecmp(pconn_syn_fetch,"syn_dbplus") == 0) {
            nRetCode=wociMainEntrance(Start,false,NULL,corelev);
        } else { // 异步方式
            nRetCode=wociMainEntrance(Start,true,NULL,corelev);
        }
        WOCIQuit();
        if(cp.loopmode) {
            lgprintf("发现异常错误,请检查!");
            lgprintf("进程在5分钟后继续.");
            mySleep(5*60);
            failretry++;
        } else break;
    } while(true);
    return nRetCode;
}
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif

/*
thread_rt LoopMonite(void *ptr) {
    bool *loopend=(bool *)ptr;
    printf("\n输入 'quit' 退出循环执行模式\n.");
    while(1)
    {
        char bf[1000];
        gets(bf);
        if(strcmp(bf,"quit")==0) {
            *loopend=true;
            printf("\n循环执行模式已退出，程序将在结束当前任务后终止.\n");
            break;
        }
        else if(strlen(bf)>0) {
            printf("\n命令'%s'不可识别，只能输入'quit'终止循环执行模式.\n",bf);
        }
    }
    thread_end;
}
*/

int Start(void *ptr)
{
    wociSetOutputToConsole(TRUE);
    DbplusCert::initInstance();
    DbplusCert::getInstance()->printlogo();
    printf(DBPLUS_STR " 基础库版本 :%s \n",GetDPLibVersion());
    int rp=0;
    cp.GetEnv();
    cp.GetParam();
    //corelev=0;
    if(!cp.checkvalid()) return -1;
    wociSetEcho(cp.verbosemode);
    wociSetOutputToConsole(cp.echomode);
    if(cp.funcid==cmdparam::DP_ENCODE) {
        encode(cp.srcdbn);
        return 1;
    }
    printf("连接到" DBPLUS_STR "...\n");
    char date[20];
    wociGetCurDateTime(date);
    int y,m,d;
    DbplusCert::getInstance()->GetExpiredDate(y,m,d);
    if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m)
        ThrowWith("您用的" DBPLUS_STR "版本太老，请更新后使用(Your " DBPLUS_STR " is too old,please update it)!");
    if(cp.freezemode) {
        char dtnow[19],dtstart[19];
        int y=0,m=0,d=0,h=0,mi=0,s=0;
        wociGetCurDateTime(dtnow);
        printf("Input start year(%d) :",wociGetYear(dtnow));
        scanf("%d",&y);
        if(y==0) y=wociGetYear(dtnow);
        if(y<2000 || y>2100) {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start month(%d) :",wociGetMonth(dtnow));
        if(m==0) m=wociGetMonth(dtnow);
        scanf("%d",&m);
        if(m<1 || m>12) {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start day(%d) :",wociGetDay(dtnow));
        scanf("%d",&d);
        if(d==0) d=wociGetDay(dtnow);
        if(d<1 || d>31) {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start hour :");
        scanf("%d",&h);
        if(h<0 || h>53) {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start minute :");
        scanf("%d",&mi);
        if(mi<0 || mi>59) {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start sec(0) :");
        scanf("%d",&s);
        if(s<0 || s>59) {
            printf("Invalid input.\n");
            return -1;
        }
        wociGetCurDateTime(dtnow);
        wociSetDateTime(dtstart,y,m,d,h,mi,s);
        int sl=wociDateDiff(dtstart,dtnow);
        if(sl<1) {
            printf("等待 %d 秒,(Y/N)?",sl);
            int ans=getch();
            if(ans!='Y' && ans!='y') return -1;
        } else  {
            printf("等待 %d 秒...",sl);
            mySleep(sl);
            printf("开始检查!\n");
        }
    }

    DpadminPrintfVersion();
    try {
        wociPrintfVersion();
    } catch(...) {
        lgprintf("使用的wdbi库过老，缺乏版本对应信息,wociPrintfVersion error !");
        return -1;
    }
    try {
        DpioshPrintfVersion();
    } catch(...) {
        lgprintf("使用的dpiosh库过老，缺乏版本对应信息,DpioshPrintfVersion error !");
        return -1;
    }

    bool normal=true;
    //building wdbi connect to dbplus server
    printf("取运行参数...\n");
    {
        //this code block use a separate database connection,release at end of code block
        AutoHandle dts;
        dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
        {
            {
                AutoStmt st(dts);
                st.DirectExecute("update dp.dp_seq set id=id+1");
                st.DirectExecute("lock tables dp.dp_seq write");
            }
            AutoMt seq(dts,10);
            seq.FetchAll("select id as fid from dp.dp_seq");
            seq.Wait();
            {
                AutoStmt st(dts);
                st.DirectExecute("unlock tables");
            }
        }
        //building direct api connect to dbplus server
        printf("连接到:%s\n",cp.serverip);
        SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd,NULL,cp.port);
        sa.Reload();
        SysAdmin *psa=&sa;
        /****Not loopable********************/
        if(cp.funcid==cmdparam::DP_CREATELIKE ) {
            MiddleDataLoader dl(psa);
            printf("DP类似创建：'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.srctable,cp.dsttbn,cp.taskdate);
            dl.CreateLike(cp.srcdbn,cp.srctbn,cp.srcowner,cp.srctable,cp.dstdbn,cp.dsttbn,cp.taskdate,false);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_CREATELIKE_PRESERVE_FORMAT ) {
            MiddleDataLoader dl(psa);
            printf("DP类似创建：'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.srctable,cp.dsttbn,cp.taskdate);
            dl.CreateLike(cp.srcdbn,cp.srctbn,cp.srcowner,cp.srctable,cp.dstdbn,cp.dsttbn,cp.taskdate,true);
            return 0;
        }
        //>> Begin: DM-228
        if(cp.funcid == cmdparam::DP_CREAT_SOLID_INDEX_TABLE) {
            MiddleDataLoader dl(psa);
            printf("DP独立创建：'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn,cp.taskdate);
            dl.CreateSolidIndexTable(cp.orasvcname,cp.orausrname,cp.orapswd,
                                     cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn,
                                     cp.objCrtTskTabAssistParam.solid_index_list,
                                     cp.objCrtTskTabAssistParam.tmp_path,
                                     cp.objCrtTskTabAssistParam.back_path,
                                     cp.taskdate,
                                     cp.objCrtTskTabAssistParam.solid_index_list_file,
                                     cp.objCrtTskTabAssistParam.ext_sql);
            return 0;
        }
        //<< End:Dm-228
        if(cp.funcid==cmdparam::DP_REMOVE) {
            DestLoader dstl(&sa);
            lgprintf("表分区删除...");
            dstl.RemoveTable(cp.srcdbn,cp.srctbn,cp.srcpartname);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_GET_SIZE) {
            DestLoader dstl(&sa);
            lgprintf("获取数据库表 %s.%s 存储空间...",cp.srcdbn,cp.srctbn);
            LONG64 totalsize =  dstl.GetTableSize(cp.srcdbn,cp.srctbn);
            if(totalsize == -1) {
                totalsize = dstl.GetTableSize(cp.srcdbn,cp.srctbn);
            }
            if((double)totalsize/(1024*1024*1024) > 1) {
                lgprintf("表 %s.%s 占用: %0.4f GB",cp.srcdbn,cp.srctbn,(double)totalsize/(1024*1024*1024));
            } else if((double)totalsize/(1024*1024) > 1) {
                lgprintf("表 %s.%s 占用: %0.4f MB",cp.srcdbn,cp.srctbn,(double)totalsize/(1024*1024));
            } else if((double)totalsize/(1024) > 1) {
                lgprintf("表 %s.%s 占用: %0.4f KB",cp.srcdbn,cp.srctbn,(double)totalsize/1024);
            } else {
                lgprintf("表 %s.%s 占用: %d bytes",cp.srcdbn,cp.srctbn,totalsize);
            }
            return 0;
        }
        if(cp.funcid==cmdparam::DP_REMOVE_BY_ID) {
            DestLoader dstl(&sa);
            lgprintf("表分区删除...");
            int datapartid = 0;
            datapartid = atoi(cp.srcpartname);
            dstl.RemoveTable(cp.srcdbn,cp.srctbn,datapartid);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_REMOVE_ALL) {
            DestLoader dstl(&sa);
            lgprintf("整表数据删除...");
            dstl.RemoveTable(cp.srcdbn,cp.srctbn);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_REMOVE_ALL_FORCE) {
            DestLoader dstl(&sa);
            lgprintf("强制执行整表数据删除...");
            dstl.RemoveTable(cp.srcdbn,cp.srctbn,false);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_MYSQLBLOCK) {
            DestLoader dstl(&sa);
            lgprintf("格式转换...");
            dstl.ToMySQLBlock(cp.srcdbn,cp.srctbn);
            return 0;
        }
        if(cp.funcid == cmdparam::DP_MOVE) {
            BackupTables bkt(&sa);
            lgprintf("表重命名...\n");
            bkt.RenameTable(cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn);
            return 0;
        }
        if(cp.funcid == cmdparam::DP_BACKUP) {
            BackupTables bkt(&sa);
            lgprintf("表数据备份...\n");
            bkt.BackupTable(cp.srcdbn,cp.srctbn,cp.srcpartname,false);
            return 0;
        }
        if(cp.funcid == cmdparam::DP_RESTORE) {
            BackupTables rst(&sa);
            lgprintf("表数据还原...\n");
            rst.RestoreTable(cp.srcdbn,cp.srctbn,cp.srcpartname,false);
            return 0;
        }
        if(cp.funcid == cmdparam::DP_BACKUP_ALL) {
            BackupTables bkt(&sa);
            lgprintf("表数据备份...\n");
            bkt.BackupTable(cp.srcdbn,cp.srctbn,cp.srcpartname,true);
            return 0;
        }
        if(cp.funcid == cmdparam::DP_RESTORE_ALL) {
            BackupTables rst(&sa);
            lgprintf("表数据还原...\n");
            rst.RestoreTable(cp.srcdbn,cp.srctbn,cp.srcpartname,true);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_HOMO_REINDEX) {
            printf("错误--同构索引重建不再支持：'%s.%s' ...\n",cp.srcdbn,cp.srctbn);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_CHECK ) {
            MiddleDataLoader dl(psa);
            printf("DP文件检查：'%s.%s' ...\n",cp.srcdbn,cp.srctbn);
            dl.dtfile_chk(cp.srcdbn,cp.srctbn);
            return 0;
        }
    } // block end,the connection release. below will build a new connection.
    bool loopend=false;
    int isbusy;
    while(true) {
        isbusy=0;
        {
            AutoHandle dts;
            dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
            SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd,NULL,cp.port);
            SysAdmin *psa=&sa;

            // 插件中独立使用
            AutoHandle plugin_dts;
            plugin_dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
            SysAdmin plugin_sa (plugin_dts,cp.serverip,cp.musrname,cp.mpswd,NULL,cp.port);
            SysAdmin *plugin_psa=&plugin_sa;

            sa.Reload();
            sa.SetNormalTask(normal);
            printf("检查目标表结构...\n");
            try {
                int tabid;
                while(sa.GetBlankTable(tabid) && sa.CreateDT(tabid)) {
                    isbusy++;
                }
                if(isbusy>0) sa.Reload();
            } catch(...) {
            };
            if(cp.funcid==cmdparam::DP_DUMP) { // || cp.funcid==cmdparam::DP_ALL) {
                if(cp.blocksize>512) cp.blocksize=512;
                if(cp.blocksize<100) cp.blocksize=100;
                DataDump dd(psa->GetDTS(),cp.mtlimit,cp.blocksize);
                printf("检查数据导出任务...\n");
                isbusy+=dd.DoDump(*psa,*plugin_psa,cp.dumpstrains[0]==0?NULL:cp.dumpstrains);
                printf("检查数据追加任务...\n");
                if(dd.CheckAddFiles(*psa)==1) isbusy+=1;
                MiddleDataLoader dl(psa);
                dl.AutoAddPartition(*psa);
            }
            if(cp.funcid==cmdparam::DP_MDLDR || cp.funcid==cmdparam::DP_ALL) {
                MiddleDataLoader dl(psa);
                printf("检查数据整理任务...\n");
                isbusy+=dl.Load(cp.indexnum,cp.load_tidxmem,cp.useOldBlock);
            }
            if(cp.funcid==cmdparam::DP_DSTLDR || cp.funcid==cmdparam::DP_ALL) {
                DestLoader dstl(&sa);
                printf("检查数据导入任务...\n");
                while(dstl.Load(cp.directIOSkip));
                isbusy+=dstl.RecreateIndex(&sa);
            }
            /***********************/
            if(cp.funcid==cmdparam::DP_DEEPCMP || (cp.funcid==cmdparam::DP_ALL && isbusy==0) ) {
                DestLoader dstl(&sa);
                printf("检查数据二次压缩任务...\n");
                isbusy+=dstl.ReCompress(cp.threadnum);
                isbusy+=dstl.ReLoad();
                isbusy+=dstl.RecreateIndex(&sa);
            }
            //如果处理过程出现异常，则总是到不了这里
            //如果是要求强制退出：
            if(loopend) break;
            //如果处理了有效任务，直接继续 ：
            if(isbusy>0) continue;
            //未发现有效任务，如果未处理异常任务，尝试处理
            if(normal) {
                normal=false;
                printf("尝试恢复异常任务执行...\n");
                continue;
            }
            //连异常任务也处理完
            normal=true;
            if( !cp.loopmode) break;
        }// connection release here
        mySleep(cp.waittime)
    }
    //wait LoopMonite thread end.
    mySleep(1)
    SvrAdmin::ReleaseInstance();
    SvrAdmin::ReleaseDTS();
    /**********************/
    printf("正常结束。\n");
    DbplusCert::ReleaseInstance();
    return 0;
}


