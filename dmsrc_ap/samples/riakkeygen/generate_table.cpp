#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<unistd.h>
#include<vector>
#include<algorithm>
#include<string>
#include "riakcluster.h"
#include "msgIBRiak.pb.h"
#include "IBCompress.h"
#include <pthread.h>
#include <glob.h>
#include "generate_part.h"
#include "generate_table.h"

using namespace std;

RiakCluster *_riak_cluster = NULL;
pthread_once_t _riak_cluster_init_once = PTHREAD_ONCE_INIT;
void*    pAsyncRiakHandle = NULL;
int _riak_nodes = 0;
int _riak_connect_number = 0;
int _riak_max_thread = 0;

/*
    功能: 将某一列 的pack包信息，转换后存入到riak中
    参数: 
            pold_basepth[in]:源数据库$DATAMERGER_HOME/var 目录
            pold_dbname[in]:源数据库名称
            pold_tbname[in]:源数据库表名称
            col_num[in]:需要转换的列编号，从0开始
            col_type[in]:列类型
            pack_num[in]:该列所具有 的pack包数目
            pnew_basepth[in]:新数据库$DATAMERGER_HOME/var 目录
            pnew_dbname[in]:新数据库名称
            pnew_tbname[in]:新数据库表名称
            st_partition_info[in/out]:分区信息,内部存储所有存在riak中的keyspace,用于所有列的统一提交和回滚
    说明:
        1> 打开源SWATCH_AB文件,判断dpn的最后一个包是如何读取的
        2> 打开源dpn文件，逐个读取包的信息
        3> 根据源dpn中读取到的数据包，判断是否存在pack文件中
        4> 对已经存在源pack文件的数据包进行读取，对读取的数据包内容保存到riak中
        5> 生成信息的dpn文件数据，将读取的dpn包写入到新的文件中
        6> 生成信息的分区文件
        7> 不同的分区需要进行更新异步保存包大小的提交
*/
int generate_riak_column(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const int col_num,const AttributeType col_type,const int pack_num,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,
                        stru_partinfo& st_partition_info);

ValueType GetValueType(const _uint64 maxv)
{
    if(maxv<= 0xff)
        return UCHAR;
    else if(maxv<=0xffff)
        return USHORT;
    else if(maxv<=0xffffffff)
        return UINT;
    else 
        return UINT64;
}

void _riak_cluster_init()
{
    char *riak_hostname;
    char *riak_port_str;
    int fret;

    riak_hostname = getenv(RIAK_HOSTNAME_ENV_NAME);
    riak_port_str = getenv(RIAK_PORT_ENV_NAME);

    if (!riak_hostname) {
        ThrowWith("Must set $%s and $%s\n", RIAK_HOSTNAME_ENV_NAME);
    }

    // 每一个riak节点的连接线程池数目(riak 驱动部分的连接池数)
    const char* priak_max_connect_per_node=getenv(RIAK_MAX_CONNECT_PER_NODE_ENV_NAME);

    if(NULL == priak_max_connect_per_node)
    {
        _riak_cluster = new RiakCluster(180);
    }
    else
    {
        int riak_max_connect_per_node = atoi(priak_max_connect_per_node);
        if(riak_max_connect_per_node >0 ){
            _riak_cluster = new RiakCluster(180,riak_max_connect_per_node);
        }
        else{
            _riak_cluster = new RiakCluster(180);
        }
    }
    fret = _riak_cluster->Rebuild(riak_hostname, riak_port_str);

        if (fret > 0) {
        DPRINTF("Riak cluster: found and success connect to %d nodes.\n", fret);
        } else if (fret == 0) {
        DPRINTF("Riak cluster: connect to first node, but cannot connect to any cluster node, maybe try again later.\n");
        } else {
            int ferr = (0 - fret);
            DPRINTF("Riak cluster: connect to first node failed, ferr=%d, msg=%s\n", ferr, strerror(ferr));
            throw ferr;
        }

    // 连接riak数目
    const char* priak_connect_number = getenv(RIAK_CONNECT_THREAD_NUMBER_ENV_NAME);
    if(NULL != priak_connect_number)
        _riak_connect_number = atoi(priak_connect_number);

    // 启动的线程数的设置
    int riak_connect_thread_times = 0;
    const char *priak_connect_thread_times = getenv(RIAK_CONNTCT_THREAD_TIMES_ENV_NAME);
    if(NULL == priak_connect_thread_times){
        riak_connect_thread_times = RIAK_CONNTCT_THREAD_TIMES_MIN*2;
    }
    else{
        riak_connect_thread_times = (atoi(priak_connect_thread_times)>RIAK_CONNTCT_THREAD_TIMES_MIN) ?
            atoi(priak_connect_thread_times) : RIAK_CONNTCT_THREAD_TIMES_MIN;
    }
    _riak_max_thread = fret * riak_connect_thread_times +1;

    _riak_nodes = fret;
}

// 获取表id和列数目
int generate_get_table_info(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabid,int& colnum)
{
    char pth[300];
    // ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht/Table.ctb
    sprintf(pth,"%s/%s/%s.bht/Table.ctb",pbasepth,pdbname,ptbname);
    
    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile){
        ThrowWith("文件%s打开失败!\n",pth);    
    }
    // get colno
    char buf[500] = {0};
    int fsize = fread(buf,1,33,pFile);
    if (fsize < 33 || strncmp(buf, "RScT", 4) != 0 || buf[4] != '2'){
        ThrowWith("Bad format of table definition in %s\n",pth);
    }
    colnum=*((int*)(buf+25));
    
    // get tabno
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    
    return 0;
}


// 获取包数目和列类型
int generate_get_col_info(const char* pbasepth,const char* pdbname,const char* ptbname,const int col_number,int& pack_no, AttributeType& col_type)
{
    char path[256];
    sprintf(path,"%s/%s/%s.bht",pbasepth,pdbname,ptbname);
    std::string attr_file = AttrFileName(path,col_number);
    
    FILE  *pFile  = fopen(attr_file.c_str(),"rb");
    if(!pFile){
        lgprintf("文件%s打开失败!\n",attr_file.c_str());    
        return -__LINE__;
    }

    const int CLMD_HEADER_LEN = 46;
    char read_buf[CLMD_HEADER_LEN+1];
    int ret = fread(read_buf,CLMD_HEADER_LEN,1,pFile);
    if(ret != 1){
        lgprintf("文件%s读取失败!\n",attr_file.c_str());    
        return -__LINE__;
    }

    if ((uchar)read_buf[8] < 128) {
        col_type = (AttributeType)read_buf[8];
    } else {
        col_type = (AttributeType)((uchar)read_buf[8]-128);
    }

    pack_no = *((int*)(read_buf + 30));

    fclose(pFile);
    pFile = NULL;
    
    return 0;
}


// 修改数据库表的id,从brighthouse.seq中获取id,并更新id
int  _getSeqId(const char* pfile,int & tabid)
{
    // 打开文件列表，看文件列表是否和tar包中的一致
    FILE* pFile = fopen(pfile,"rt");
    if(NULL == pFile){
        lgprintf("Open file %s error \n",pfile);
        return -__LINE__;
    }
    char lines[300];
    fgets(lines,300,pFile);
    tabid = atoi(lines);
    fclose(pFile);
    return 0;
}

int _updateSeqId(const char* pfile,const int tabid)
{
    FILE* pFile = fopen(pfile,"wt");
    if(NULL == pFile){
        lgprintf("Open file %s error \n",pfile);
        return -__LINE__;
    }
    fprintf(pFile,"%d",tabid);
    fclose(pFile);    
    return 0;
}

int _updateTableId(const char* pth,const int tabid)
{
    FILE  *pFile  = fopen(pth,"r+b");
    if(!pFile){
        lgprintf("文件%s打开失败!",pth);
        return -__LINE__;
    }
    fseek(pFile,-4,SEEK_END);
    fwrite(&tabid,4,1,pFile);;
    fclose(pFile); 
    pFile = NULL;
    return 0;
}

int _getTableId(const char* pth,int & tabid)
{
    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile){
        lgprintf("文件%s打开失败!",pth);
        return -__LINE__;
    }
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    return tabid;
}

// 更新指定目录下的*.rsi文件的表id
// ????.%d.*.rsi 文件替换中通的 %d 的表id
#define RSI_FILE_HEAD_LEN  4   // "CMAP." or  "HIST."
#define RSI_POS            '.'  
bool _updateRsiID(const char* pth,const int tabid,const char* pattern)
{
    char rsi_parttern[300] = {0};
    // tabid已经变了，不用这个新的id
    //sprintf(rsi_parttern,"%s/????.%d.*.rsi",pth,tabid);
    if(pattern == NULL){
        sprintf(rsi_parttern,"%s/*",pth);
    }
    else{
        sprintf(rsi_parttern,"%s/%s",pth,pattern);
    }
    
    char path_head[300];
    sprintf(path_head,"%s",pth);
    int path_head_len = strlen(path_head)+1;  // 包括‘/’的长度

    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(rsi_parttern, GLOB_DOOFFS, NULL, &globbuf)) {
        for(int i=0;i<globbuf.gl_pathc;i++) {
            char *poldname = globbuf.gl_pathv[i];

            std::string strname(poldname);
            int str_len = strname.size();
            // 截取CMAP 或者HIST
            std::string strhead = strname.substr(path_head_len,RSI_FILE_HEAD_LEN);
            
            // %d.*.rsi 的截取    
            int  _tmp1 = path_head_len+RSI_FILE_HEAD_LEN + 1;  // '.' 一个字符
            int  _tmp2 = str_len-_tmp1;    //     %d.*.rsi 的长度
            
            std::string strtmp = strname.substr(_tmp1,_tmp2);
                
            // *.rsi的截取
            int pos = strtmp.find(RSI_POS);
            str_len = strtmp.size();
            strtmp = strtmp.substr(pos+1);  // get 'colid.rsi'

            // get new filename by new tabid
            char pnewname[300];
            // %s/BH_RSI_Repository/????.%d.*.rsi
            sprintf(pnewname,"%s/%s.%d.%s",pth,strhead.c_str(),tabid,strtmp.c_str());

            // 文件重命名操作  poldname ---> pnewname
            char cmd[300];
            sprintf(cmd,"mv %s %s",poldname,pnewname);
            int ret = system(cmd);
            assert(ret != -1);            
        }    
    }
    globfree(&globbuf);
    return true;
}

// 更新tabid,return tabid
int  generate_change_table_id(const char* pbasepth,const char* dbname,const char* tbname)
{
    int tabid=0;
    int ret = -1;
    lgprintf("开始修改表[%s/%s/%s]表ID.",pbasepth,dbname,tbname);

    int seqid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    ret = _getSeqId(seq_file,seqid);
    if(ret != 0){
        lgprintf("获取brighthouse.seq表id失败.");
        return -__LINE__;
    }

    //  直接使用一个新的id，seqid + 1
    seqid = seqid +1;
    tabid= seqid;
    ret = _updateSeqId(seq_file,seqid);
    if(ret != 0){
        lgprintf("更新brighthouse.seq表id失败.");
        return -__LINE__;
    }

    // 更新tabid
    char _table_tcb_pth[300];
    sprintf(_table_tcb_pth,"%s/%s/%s.bht/Table.ctb",pbasepth,dbname,tbname);
    
    ret = _updateTableId(_table_tcb_pth,tabid);
    if(ret != 0){
        lgprintf("更新表[%s/%s/%s]的ID失败.",pbasepth,dbname,tbname);
        return -__LINE__;
    }
    
    lgprintf("修改表[%s/%s/%s]表ID成功.",pbasepth,dbname,tbname);
    return tabid;
}


// solid 分区对应的riak keyspace
int generate_riak_solid_keyspace(const stru_partinfo st_partinfo)
{
    if(_riak_cluster != NULL){
        int tab_number = st_partinfo.tabid;
        int attr_number = st_partinfo.attrid;
        for(int part_num=0;part_num<st_partinfo.st_part_list.size();part_num++){
            for(int riak_pnode=0;riak_pnode<st_partinfo.st_part_list[part_num].riak_pnode_list.size();riak_pnode++){
                _riak_cluster->KeySpaceSolidify(RIAK_PAT_NAME,tab_number,attr_number,st_partinfo.st_part_list[part_num].riak_pnode_list[riak_pnode]);     
            }
        }
    }
    return 0;
}

// solid 分区对应的riak keyspace
int generate_riak_free_keyspace(const stru_partinfo st_partinfo)
{
    if(_riak_cluster != NULL){
        int tab_number = st_partinfo.tabid;
        int attr_number = st_partinfo.attrid;
        for(int part_num=0;part_num<st_partinfo.st_part_list.size();part_num++){
            for(int riak_pnode=0;riak_pnode<st_partinfo.st_part_list[part_num].riak_pnode_list.size();riak_pnode++){
                _riak_cluster->KeySpaceFree(RIAK_PAT_NAME,tab_number,attr_number,st_partinfo.st_part_list[part_num].riak_pnode_list[riak_pnode]);   
            }
        }
    }
    return 0;
}

// 拷贝rsi索引文件
int generate_copy_rsi(const char* pold_basepth,const char* pnew_basepth,const int old_tabid,const int new_tabid)
{
    char old_table_rsi_pattern[300];
    char new_table_rsi_pattern[300];

    char cmd[500];
    sprintf(old_table_rsi_pattern,"%s/BH_RSI_Repository/????.%d.*.rsi",pold_basepth,old_tabid);
    

    sprintf(cmd,"cp %s/BH_RSI_Repository/????.%d.*.rsi %s/BH_RSI_Repository",pold_basepth,old_tabid,pnew_basepth);
    system(cmd);

    // 更新rsi的tabid
    sprintf(new_table_rsi_pattern,"????.%d.*.rsi",old_tabid);
    char BH_RSI_dir[300];
    sprintf(BH_RSI_dir,"%s/BH_RSI_Repository",pnew_basepth);
    
    bool ret = _updateRsiID(BH_RSI_dir,new_tabid,new_table_rsi_pattern);
    if(!ret){
        lgprintf("_updateRsiID error");
        return -__LINE__;
    }
    return 0;
}

// 如果转换失败进行回滚
int generate_table_rallback(const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,const int pnew_tbid)    
{
    char cmd[1000];
    char path[256];
    sprintf(path,"%s/%s/%s.*",pnew_basepth,pnew_dbname,pnew_tbname);
    sprintf(cmd,"rm %s -rf",path);
    lgprintf("运行命令:%s",cmd);
    system(cmd);    

    if(pnew_tbid>0){
        sprintf(path,"%s/BH_RSI_Repository/????.%d.*.rsi -rf",pnew_basepth,pnew_tbid);
        lgprintf("运行命令:%s",cmd);
        system(cmd);
    }
    return 0;
}

// 判断数据库表是否存在
bool generate_exist_table(const char* pbasepth,const char* pdbname,const char* ptbname)
{
    char path_frm[256];
    sprintf(path_frm,"%s/%s/%s.frm",pbasepth,pdbname,ptbname);

    struct stat stat_info;
    return (0 == stat(path_frm, &stat_info)) ;
}

// 将老的数据库表,转换成新的数据库表
int  generate_riak_table(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname)
{
    // 返回值
    int ret = 0;
    bool generate_success = true; // 操作是否成功
    int column_num = 0; 
    int old_tabid = 0;
    int new_tabid = 0;
    
    // 存储各个分区的riak信息
    std::vector<stru_partinfo> stru_partinfo_vector;

    AttributeType  col_type;
    int pack_no = 0;
    
    // 1> 判断目标表是否存在
    if(generate_exist_table(pnew_basepth,pnew_dbname,pnew_tbname)){
        lgprintf("目标数据库表[%s.%s]已经存在,不能进行转换表.",pnew_dbname,pnew_tbname);
        ret = -__LINE__;
        return ret;
    }

    // 2> 获取源表id和存在多少列
    ret = generate_get_table_info(pold_basepth,pold_dbname,pold_tbname,old_tabid,column_num);
    if(0 != ret){
        lgprintf("获取源表[%s.%s]结构信息失败.",pold_dbname,pold_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }

    // 3> 拷贝表结构
    ret = generate_copy_frm_bht(pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname,column_num);
    if(ret != 0){
        lgprintf("拷贝表[%s.%s]结构-->[%s.%s]失败.",pold_dbname,pold_tbname,pnew_dbname, pnew_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }

    // 4> 修改表id和表名称
    ret = generate_change_table_id(pnew_basepth,pnew_dbname, pnew_tbname);
    if(ret < 0){
        lgprintf("更新表[%s.%s]ID失败.",pold_dbname,pold_tbname,pnew_dbname, pnew_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }
    new_tabid = ret;

    // 4> 复制RSI信息
    lgprintf("开始拷贝表[%s/%s/%s]的RSI索引文件信息.",pold_basepth,pold_dbname,pold_tbname);
    ret = generate_copy_rsi(pold_basepth,pnew_basepth,old_tabid,new_tabid);
    if(ret != 0){   
        lgprintf("拷贝表[%s/%s/%s]的RSI索引文件信息失败.",pold_basepth,pold_dbname,pold_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;       
    }
    lgprintf("拷贝表[%s/%s/%s]的RSI索引文件信息成功.",pold_basepth,pold_dbname,pold_tbname);

    // 5> 逐列数据进行装换
    lgprintf("开始将表[%s/%s/%s]的逐个列进行转换到表[%s/%s/%s].",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);

    // 5.1>存储各个分区的riak信息,逐列的转换处理
    for(int i=0;i<column_num;i++){
        int _column_num_index = i;
        lgprintf("开始将表[%s/%s/%s]的第[%d]列进行转换到表[%s/%s/%s]的第[%d]列.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);
        stru_partinfo st_partition_info;
        st_partition_info.tabid = new_tabid;
        st_partition_info.attrid = _column_num_index;
        stru_partinfo_vector.push_back(st_partition_info);
        try{
            if(0 != generate_get_col_info(pold_basepth,pold_dbname,pold_tbname,_column_num_index, pack_no , col_type)){
                generate_success = false;
                ThrowWith("将表[%s/%s/%s]的第[%d]列进行转换到表[%s/%s/%s]的第[%d]列,获取列信息失败.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);
            }
            
            
            ret = generate_riak_column(pold_basepth,pold_dbname,pold_tbname,_column_num_index, col_type,pack_no, pnew_basepth, pnew_dbname, pnew_tbname,stru_partinfo_vector[_column_num_index]);
            if(ret != 0){
                lgprintf("将表[%s/%s/%s]的第[%d]列进行转换到表[%s/%s/%s]的第[%d]列,装换失败.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);   
                generate_success = false;
                break;
            }else{
                lgprintf("将表[%s/%s/%s]的第[%d]列进行转换到表[%s/%s/%s]的第[%d]列,装换成功.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);   
            }
            
        }catch(int){
            generate_success = false;
            break;
        }catch(...){
            generate_success = false;
            break;
        }            
    }

    if(generate_success){// 提交keyspace
        lgprintf("已经将表[%s/%s/%s]的逐个列进行转换到表[%s/%s/%s]完成,准备提交RIAK的KeySpace.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
        for(int j=0;j<stru_partinfo_vector.size();j++){
            ret = generate_riak_solid_keyspace(stru_partinfo_vector[j]);
            if(ret != 0){
                generate_success = false;                
                break;
            }
        }
    }

    if(generate_success){
        lgprintf("表[%s/%s/%s]的转换已经完成,提交RIAK的KeySpace成功.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
    }
 
    if(!generate_success){// 失败就回滚
        lgprintf("表[%s/%s/%s]的转换失败,回滚RIAK的KeySpace.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);       
        generate_table_rallback(pnew_basepth, pnew_dbname, pnew_tbname,new_tabid);
        for(int j=0;j<stru_partinfo_vector.size();j++){
            generate_riak_free_keyspace(stru_partinfo_vector[j]);
        }
    }

finish:
    if(!generate_success){// 清除目录
        lgprintf("表[%s/%s/%s]的转换失败,回滚清除数据.",pnew_basepth, pnew_dbname, pnew_tbname);
        generate_table_rallback(pnew_basepth, pnew_dbname, pnew_tbname,new_tabid);
        lgprintf("表[%s/%s/%s]的转换失败,回滚清除数据完成.",pnew_basepth, pnew_dbname, pnew_tbname);
        
        return ret;
    }
    
    lgprintf("表[%s/%s/%s]的逐个列进行转换到表[%s/%s/%s]成功.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
    
    return ret;
}

/*
    功能: 将某一列 的pack包信息，转换后存入到riak中
    参数: 
            pold_basepth[in]:源数据库$DATAMERGER_HOME/var 目录
            pold_dbname[in]:源数据库名称
            pold_tbname[in]:源数据库表名称
            col_num[in]:需要转换的列编号，从0开始
            col_type[in]:列类型
            pack_num[in]:该列所具有 的pack包数目
            pnew_basepth[in]:新数据库$DATAMERGER_HOME/var 目录
            pnew_dbname[in]:新数据库名称
            pnew_tbname[in]:新数据库表名称
            st_partition_info[in/out]:分区信息,内部存储所有存在riak中的keyspace,用于所有列的统一提交和回滚
    说明:
        1> 打开源SWATCH_AB文件,判断dpn的最后一个包是如何读取的
        2> 打开源dpn文件，逐个读取包的信息
        3> 根据源dpn中读取到的数据包，判断是否存在pack文件中
        4> 对已经存在源pack文件的数据包进行读取，对读取的数据包内容保存到riak中
        5> 生成信息的dpn文件数据，将读取的dpn包写入到新的文件中
        6> 生成信息的分区文件
        7> 不同的分区需要进行更新异步保存包大小的提交
*/
int generate_riak_column(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const int col_num,const AttributeType col_type,const int pack_num,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,
                        stru_partinfo& st_partition_info)
{
    // ------------------------------------------------------------------------
    // 1>  源表:dpn,part,pack 文件名称
    stru_src_table_assist st_src_table_assist;    
    stru_dst_table_assist st_dst_table_assist;    
    bool has_catch_throw = false;
    std::string src_tmp = "";
    short len_mode = sizeof(short);

    try{
        // 1>  源表:dpn,part,pack 文件名称
        sprintf(st_src_table_assist.table_path,"%s/%s/%s.bht",pold_basepth,pold_dbname,pold_tbname);
        
        src_tmp = DpnFileName(st_src_table_assist.table_path,col_num);
        strcpy(st_src_table_assist._s_d_p_item.fdpn_name,src_tmp.c_str());
        
        src_tmp = PartFileName(st_src_table_assist.table_path,col_num);
        strcpy(st_src_table_assist._s_d_p_item.fpart_name,src_tmp.c_str());

        
        // 1.1>  打开源表dpn,part
        st_src_table_assist._s_d_p_item.fdpn_handle = fopen(st_src_table_assist._s_d_p_item.fdpn_name,"rb");
        if(!st_src_table_assist._s_d_p_item.fdpn_handle){
            ThrowWith("文件%s打开失败!\n",st_src_table_assist._s_d_p_item.fdpn_name);    
        }
        st_src_table_assist._s_d_p_item.fpart_handle= fopen(st_src_table_assist._s_d_p_item.fpart_name,"rb");
        if(!st_src_table_assist._s_d_p_item.fpart_handle){
            ThrowWith("文件%s打开失败!\n",st_src_table_assist._s_d_p_item.fpart_name);    
        }

        // 1.3> 加载源表part到内存
        st_partition_info.LoadApPartInfo(st_src_table_assist._s_d_p_item.fpart_name,st_src_table_assist._s_d_p_item.fpart_handle);
        st_dst_table_assist.table_number = st_partition_info.tabid;
        st_dst_table_assist.attr_number = st_partition_info.attrid;
        
        // ------------------------------------------------------------------------
        // 2>  新表:dpn,part,pack 文件名称
        sprintf(st_dst_table_assist.table_path,"%s/%s/%s.bht",pnew_basepth,pnew_dbname,pnew_tbname);
        
        src_tmp = DpnFileName(st_dst_table_assist.table_path,col_num);
        strcpy(st_dst_table_assist._s_d_p_item.fdpn_name,src_tmp.c_str());
        
        src_tmp = PartFileName(st_dst_table_assist.table_path,col_num);
        strcpy(st_dst_table_assist._s_d_p_item.fpart_name,src_tmp.c_str());
            
        // 2.1> 打开目标表的dpn,part
        st_dst_table_assist._s_d_p_item.fdpn_handle = fopen(st_dst_table_assist._s_d_p_item.fdpn_name,"wb");
        if(!st_dst_table_assist._s_d_p_item.fdpn_handle){
            ThrowWith("文件%s打开失败!\n",st_dst_table_assist._s_d_p_item.fdpn_name);    
        }
        st_dst_table_assist._s_d_p_item.fpart_handle= fopen(st_dst_table_assist._s_d_p_item.fpart_name,"wb");
        if(!st_dst_table_assist._s_d_p_item.fpart_handle){
            ThrowWith("文件%s打开失败!\n",st_dst_table_assist._s_d_p_item.fpart_name);    
        }

        // ------------------------------------------------------------------------
        // 3> 根据列类型获取数据包类型
        AttrPackType packtype;
        if (IsNumericType(col_type) ||IsDateTimeType(col_type)){
            packtype = PackN;
            len_mode = sizeof(short);
        }else{
            packtype = PackS;
            if(col_type == RC_BIN){
                len_mode = sizeof(uint);
            }
        }
        
        const int conDPNSize = 37;
        // ------------------------------------------------------------------------
        // 4> 根据AB_SWITCH文件获取ABS_STATE,并读取文件信息
        
        // 打开dpn文件，从dpn中读取数据
        // 最后一个包读取
        // 0:37 bytes :  ABS_A_STATE
        // 37:37 bytes :  ABS_B_STATE
        
        // ABS_A_STATE ---> A
        // ABS_B_STATE ---> B    
        ABSwitcher absw;
        ABSwitch cur_ab_switch_state = absw.GetState(st_src_table_assist.table_path);    
        int _pack_no = 0; // 当前读取到的pack_no
        char dpn_buf[38];
        int ret = 0;

        // 4.1> 大于两个包的情况下，先将头2个包的位置预留 出来
        if(pack_num >= 2)    {
            fseek(st_src_table_assist._s_d_p_item.fdpn_handle,2*conDPNSize,SEEK_SET);
            memset(dpn_buf,0,38);
            fwrite(dpn_buf,1,conDPNSize,st_dst_table_assist._s_d_p_item.fdpn_handle);
            fwrite(dpn_buf,1,conDPNSize,st_dst_table_assist._s_d_p_item.fdpn_handle);
        }

        // 4.2> 从dpn中逐个读取每一个数据包
        while(_pack_no < pack_num){
            if(_pack_no == pack_num - 1){// 是否是最后一个包
                // 没有switch_ab文件的时候，最后一个包读取17-34字节
                if(cur_ab_switch_state == ABS_A_STATE){
                    fseek(st_src_table_assist._s_d_p_item.fdpn_handle,conDPNSize,SEEK_SET);
                    fseek(st_dst_table_assist._s_d_p_item.fdpn_handle,conDPNSize,SEEK_SET);
                }else{
                    fseek(st_src_table_assist._s_d_p_item.fdpn_handle,0,SEEK_SET);
                    fseek(st_dst_table_assist._s_d_p_item.fdpn_handle,0,SEEK_SET);
                }
            }

            // 读取37个字节(一个完整的dpn信息)
            ret =fread(dpn_buf,conDPNSize,1,st_src_table_assist._s_d_p_item.fdpn_handle);
            bool is_stored = true;
            
            if(ret == 1){// 读取成功，判断是否是有效包
                char* buf=dpn_buf;
                int pack_file = *((int*) buf);
                int pack_addr = *((uint*) (buf + 4));
                _uint64 local_min = *((_uint64*) (buf + 8));
                _uint64 local_max = *((_uint64*) (buf + 16));
                _uint64 sum_size = 0;
                if(packtype == PackN){
                    sum_size = *((_uint64*) (buf + 24));
                }else{
                    sum_size = ushort(*((_uint64*) (buf + 24)));
                }
                ushort no_objs = *((ushort*) (buf + 32));
                ushort no_nulls = *((ushort*) (buf + 34));
                if(pack_file == PF_NULLS_ONLY){
                    no_nulls = no_objs + 1;
                }
                if(pack_file == PF_NULLS_ONLY || (packtype == PackN && local_min == local_max && no_nulls == 0)) {
                    is_stored = false;
                } else if(pack_file == PF_NO_OBJ) {
                    is_stored = false;
                } else {
                    is_stored = true;
                }

                // 4.3  需要读取pack 数据包内容，将数据包内容存入riak中
                if(is_stored){
                    src_tmp = AttrPackFileNameDirect(col_num,pack_file,st_src_table_assist.table_path);
                    // 相等说明不需要用新的文件
                    if(strlen(st_src_table_assist.fpack_name) == 0 || strcmp(st_src_table_assist.fpack_name,src_tmp.c_str()) != 0){ 
                        strcpy(st_src_table_assist.fpack_name,src_tmp.c_str());
                        

                        // 打开pack文件
                        if(st_src_table_assist.fpack_handle != NULL){
                            fclose(st_src_table_assist.fpack_handle);
                            st_src_table_assist.fpack_handle = NULL;
                        }
                            
                        st_src_table_assist.fpack_handle = fopen(st_src_table_assist.fpack_name,"rb");
                        if(st_src_table_assist.fpack_handle == NULL){
                            ThrowWith("打开文件%s失败!\n",st_src_table_assist.fpack_name);
                        }                            
                    }

                    
                    // 4.3.1> 设置当前包号正对应的分区名称
                    bool new_partition_pack = false; // 该包号对应一个新的分区
                    char* partition_name = st_partition_info.GetPartName(_pack_no);
                    if(strlen(st_dst_table_assist._s_d_p_item.fcurrent_part_name) == 0 ){ // 第一个分区的开始转换
                        st_src_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                        st_dst_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                      
                        pAsyncRiakHandle = _riak_cluster->AsyncInit(_riak_max_thread,1024,10);
                        
                        new_partition_pack = true;
                    }else if(strcmp(partition_name,st_dst_table_assist._s_d_p_item.fcurrent_part_name) != 0 ){ // 上一个分区转换完成

                        // 提交上一个分区的装入文件
                        {
                            std::string err_msg;
                            long ret = 0;
                            ret = _riak_cluster->AsyncWait(pAsyncRiakHandle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
                            if(ret < 0){
                                ThrowWith(" Save pack error:%s ,AsyncWait return :%d",err_msg.c_str(),ret);
                            }
                            
                            // 提交riak中保存文件大小
                            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,st_dst_table_assist._s_d_p_item.GetCurrentPartName(),ret);
                        }
                        st_src_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                        st_dst_table_assist._s_d_p_item.SetCurrentPartName(partition_name);                        

                        pAsyncRiakHandle = _riak_cluster->AsyncInit(_riak_max_thread,1024,10);
                        new_partition_pack = true;
                    }

                    
                    // 4.3.2> 设置pack_file 和 pack_addr
                    if( new_partition_pack || st_dst_table_assist.ShouldAllocPNode()) { // 同一个分区,已经连续处理4096个包
                        st_dst_table_assist.RiakAllocPNode();
                        // 设置分区信息,将riak信息写入分区中
                        st_partition_info.AddRaikPnode(_pack_no,st_dst_table_assist.GetRiakPNode());
                    }else{
                    	// 已经分配了keyspace的，直接使用
                    	st_dst_table_assist.AddRiakPNode();
                    }

                    //  4.3.3 > 写入dpn 信息
                    {                        
                        *((int*) buf) = st_dst_table_assist.pack_file;
                        *((uint*) (buf + 4)) = st_dst_table_assist.pack_addr;
                        
                        ret = fwrite(dpn_buf,conDPNSize,1,st_dst_table_assist._s_d_p_item.fdpn_handle);
                        if(ret != 1){
                            ThrowWith("写入文件%s失败!\n",st_dst_table_assist._s_d_p_item.fdpn_name);    
                        }
                    }

                    // 4.4> 读取pack包数据,构建riak对象,向riak中写入对象
                    {    
                        // 移动到数据的准确位置
                        fseek(st_src_table_assist.fpack_handle,pack_addr,SEEK_SET);

                        // 加载 packn/packs
                        {
                            uint comp_buf_size,comp_null_buf_size,comp_len_buf_size;
                            comp_buf_size =comp_null_buf_size =comp_len_buf_size = 0;                        
                            unsigned char *compressed_buf = NULL;
                            uint buf_used;
                            uint* nulls = NULL;
                            uint data_full_byte_size = 0;
                            uint total_size = 0;
                            uchar optimal_mode = 0;
                            uint no_obj_in_pack=0;
                            uint no_nulls = 0;
                            
                            // 构造riak中使用的keyspace
                            std::string str_riak_key_value = st_dst_table_assist.Get_riak_key();
                            
                            if(packtype == PackN) {// read PackN and send to riak
                                void* data_full = NULL;
                                uchar head[17];
                                buf_used = fread(head,1,17,st_src_table_assist.fpack_handle);
                                if(buf_used < 17) {
                                    ThrowWith("Error: Cannot load or wrong header of data pack (AttrPackN).\n");
                                }
                                total_size = *((uint*) head);
                                optimal_mode = head[4];
                                no_obj_in_pack = *((ushort*) (head + 5));
                                ++no_obj_in_pack;
                                no_nulls = *((ushort*) (head + 7));
                                _uint64 max_val = *((_uint64*) (head + 9));
                                ValueType value_type = GetValueType(max_val);

                                if(ib_packn_compressmode_assist::IsModeNoCompression(optimal_mode)) { // 不需要进行压缩的数据包
                                    if(no_nulls) {
                                        nulls = (uint*)malloc(2048 * sizeof(uint));
                                        if(no_obj_in_pack < 65536){
                                            memset(nulls, 0, 8192);
                                        }
                                        fread((char*)nulls,1,((no_obj_in_pack+7)/8),st_src_table_assist.fpack_handle);
                                    }
                                    if(value_type * no_obj_in_pack) {
                                        data_full = malloc(value_type * no_obj_in_pack);
                                        fread((char*)data_full,1,value_type * no_obj_in_pack,st_src_table_assist.fpack_handle);
                                    }
                                    
                                    // 设置成riak接口接口向riak中发送
                                    AttrPackN_Save(no_nulls,nulls,data_full,no_obj_in_pack,optimal_mode,local_min,max_val,str_riak_key_value);
                                    
                                } else {
                                    if(total_size < 17) {
                                        ThrowWith("Wrong header of data pack in file %s \n",st_src_table_assist.fpack_name);
                                    }

                                    comp_buf_size = total_size - 17;
                                    compressed_buf = (uchar*) malloc((comp_buf_size + 1) * sizeof(uchar));
  
                                    if(!compressed_buf) {
                                        ThrowWith("Error: out of memory (%d bytes failed). (28)\n",comp_buf_size);
                                    }
                                    buf_used = fread((char*) compressed_buf,1,comp_buf_size,st_src_table_assist.fpack_handle);
                                    if(buf_used > comp_buf_size) {
                                        ThrowWith("Error: read compressed buf error");
                                    }

                                    // 需要对压缩数据包进行解压缩
                                    data_full_byte_size = value_type * no_obj_in_pack;
                                    data_full = malloc(data_full_byte_size);
                                    int _no_nulls = no_nulls;
                                    
                                   	if(nulls == NULL && _no_nulls>0){
                                    	nulls = (uint*)malloc(2048 * sizeof(uint));
	                                    if(no_obj_in_pack < 65536){
	                                        memset(nulls, 0, 8192);
	                                    }
                                    }	
                                    
                                    ret = ib_packn_decompress(compressed_buf,comp_buf_size,optimal_mode,nulls,_no_nulls,data_full,(int&)no_obj_in_pack);
                                    if(ret != 0){
                                        ThrowWith("ib_packn_decompress error return %d. ",ret);
                                    }                

                                    // 设置成riak接口接口向riak中发送
                                    AttrPackN_Save(_no_nulls,nulls,data_full,no_obj_in_pack,optimal_mode,local_min,max_val,str_riak_key_value);
                                    
                                }
                                
                                if(data_full != NULL){
                                    free(data_full);
                                    data_full = NULL;
                                }
                            }else { // PackS
                                uchar *data = NULL;    
                                void *lens = NULL;
                                uint decomposer_id = 0;
                                uchar no_groups = 0;

                                no_nulls = 0;
                                optimal_mode = 0;
                                uchar prehead[9];
                                buf_used = fread(prehead,1,9,st_src_table_assist.fpack_handle);                            
                                uint total_size = *((uint *)prehead);
                                uchar tmp_mode = prehead[4];
                                no_obj_in_pack= *((ushort*)(prehead + 5));
                                ++no_obj_in_pack;
                                no_nulls = *((ushort*)(prehead + 7));
                                int header_size;
                                uchar ver;
                                
                                if(tmp_mode >= 8 && tmp_mode <=253) {
                                    ver = tmp_mode;
                                    uchar head[10];
                                    buf_used = fread(head,1,10,st_src_table_assist.fpack_handle);
                                    optimal_mode = head[0];
                                    
                                    decomposer_id = *((uint*)(head + 1));
                                    no_groups = head[5];
                                    data_full_byte_size = *((uint*)(head + 6));
                                    header_size = 19;
                                } else {
                                    ver = 0;
                                    optimal_mode = tmp_mode;
                                    uchar head[4];
                                    buf_used = fread(head,1,4,st_src_table_assist.fpack_handle);
                                    data_full_byte_size = *((uint*)(head));
                                    header_size = 13;
                                }
                                if(!ib_packs_compressmode_assist::IsModeValid(optimal_mode)) {
                                    ThrowWith("Error: Unexpected byte in data pack (AttrPackS).\n");
                                } else {
                                    int _no_nulls = no_nulls;
                                    if(data == NULL) {
	                                   if(data_full_byte_size) {
                                          data = (uchar*)malloc(data_full_byte_size);
                                          if(NULL == data){
                                             ThrowWith("Error: out of memory ( %d bytes failed). (40).\n",data_full_byte_size);
                                          }
                                       }
                                    }
                                    lens = malloc(len_mode * no_obj_in_pack * sizeof(char));
                                    if(no_nulls > 0) {
                                        if(!nulls){
                                            nulls = (uint*) malloc(2048 * sizeof(uint));
                                        }
                                        if(no_obj_in_pack < 65536){
                                            memset(nulls, 0, 8192);
                                        }
                                    }

                                    if(ib_packs_compressmode_assist::IsModeNoCompression(optimal_mode)){ // 没有压缩的,直接读取即可
                                        if(data){
                                            fread((char*) *data,1,data_full_byte_size,st_src_table_assist.fpack_handle);
                                        }
                                        if(nulls){
                                            fread((char*) nulls,1,(no_obj_in_pack + 7) / 8,st_src_table_assist.fpack_handle);
                                        }
                                        if(lens){
                                            fread((char*) lens,1,len_mode * no_obj_in_pack,st_src_table_assist.fpack_handle);
                                        }
                                        
                                        // 设置成riak接口接口向riak中发送
                                        AttrPackS_Save(no_nulls,nulls,data,data_full_byte_size,len_mode,lens,decomposer_id,no_groups,no_obj_in_pack,optimal_mode,ver,str_riak_key_value);
                                    }else{
                                        // normal compression
                                        if(ib_packs_compressmode_assist::IsModeCompressionApplied(optimal_mode)) // 0,1    = Huffmann 16-bit compression of remainings with tree (0 - flat null encoding, 1 - compressed nulls)
                                        {
                                            comp_buf_size = total_size - header_size;
                                            compressed_buf = (uchar*)malloc((comp_buf_size + 1) * sizeof(uchar));

                                            if(!compressed_buf) {
                                                ThrowWith("Error: out of memory (%d bytes failed). (32)\n",comp_buf_size+1);
                                            }
                                            
                                            buf_used = fread((char*)compressed_buf,1,comp_buf_size,st_src_table_assist.fpack_handle);

                                            // 对数据包进行解压操作                                     
                                            ret = ib_packs_decompress(compressed_buf,comp_buf_size,no_obj_in_pack,optimal_mode,data,data_full_byte_size,len_mode,lens,nulls,_no_nulls);
                                            if(ret != 0){
                                                ThrowWith("Error:ib_packs_decompress error.");
                                            }

                                            // 设置成riak接口接口向riak中发送
                                            AttrPackS_Save(_no_nulls,nulls,data,data_full_byte_size,len_mode,lens,decomposer_id,no_groups,no_obj_in_pack,optimal_mode,ver,str_riak_key_value);
            
                                        } 
                                        else {
                                            ThrowWith("The process reached to an invalid code path! (LoadData)\n");
                                        }
                                    }

                                    if(lens != NULL){
                                        free(lens);
                                        lens = NULL;
                                    }
                                    if(data != NULL){
                                        free(data);
                                        data = NULL;
                                    }                    
                                }
                            } // end if packs

                            if(compressed_buf != NULL){
                                free(compressed_buf);
                                compressed_buf = NULL;
                            }
                            if(nulls != NULL){
                                free(nulls);
                                nulls = NULL;
                            }
                        }
                    }
                    
                }
                else
                {
                    // 直接写入dpn文件就可以
                    ret = fwrite(dpn_buf,conDPNSize,1,st_dst_table_assist._s_d_p_item.fdpn_handle);
                    if(ret != 1){
                        ThrowWith("写入文件%s失败!\n",st_dst_table_assist._s_d_p_item.fdpn_name);
                    }
                }
            }    else    {
                ThrowWith("读取文件%s失败\n",st_src_table_assist._s_d_p_item.fdpn_name);
            }
            
            _pack_no ++;
        }

        // 保存分区信息,生成riak版本中使用的partition信息
        st_partition_info.SaveRiakPartInfo(st_dst_table_assist._s_d_p_item.fpart_name,st_dst_table_assist._s_d_p_item.fpart_handle);


        // 提交上一个分区转换后的存入riak中的大小
        long _current_save_size = 0;
        if((pack_num >0) && (strlen(st_dst_table_assist._s_d_p_item.GetCurrentPartName())>0))
        {
            std::string err_msg;
            _current_save_size = _riak_cluster->AsyncWait(pAsyncRiakHandle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
            if(_current_save_size < 0){
                ThrowWith(" Save pack error:%s ,AsyncWait return :%d",err_msg.c_str(),_current_save_size);
            }    
            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,st_dst_table_assist._s_d_p_item.GetCurrentPartName(),_current_save_size);
        }else if((pack_num >0) && (strlen(st_dst_table_assist._s_d_p_item.GetCurrentPartName()) == 0)){
            // 没有向riak中保存过数据包的(只在DPN中存在内容),记录riak中的包大小为0
            _current_save_size = 0;
            char* partition_name = st_partition_info.GetPartName((_pack_no-1));
            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,partition_name,_current_save_size);
        }     
              
    }catch(...){
        has_catch_throw = true;
        goto finish;   
    }

finish:
    if(st_src_table_assist.fpack_handle != NULL){
        fclose(st_src_table_assist.fpack_handle);
        st_src_table_assist.fpack_handle = NULL;
    }
    if(st_src_table_assist._s_d_p_item.fdpn_handle != NULL){
        fclose(st_src_table_assist._s_d_p_item.fdpn_handle);
        st_src_table_assist._s_d_p_item.fdpn_handle = NULL;
    }
    if(st_src_table_assist._s_d_p_item.fpart_handle!= NULL){
        fclose(st_src_table_assist._s_d_p_item.fpart_handle);
        st_src_table_assist._s_d_p_item.fpart_handle = NULL;
    }

    if(st_dst_table_assist._s_d_p_item.fdpn_handle != NULL){
        fclose(st_dst_table_assist._s_d_p_item.fdpn_handle);
        st_dst_table_assist._s_d_p_item.fdpn_handle = NULL;
    }
    if(st_dst_table_assist._s_d_p_item.fpart_handle!= NULL){
        fclose(st_dst_table_assist._s_d_p_item.fpart_handle);
        st_dst_table_assist._s_d_p_item.fpart_handle = NULL;
    }

    if(has_catch_throw){
        ThrowWith("generate_riak_column throw");
    }
    return 0;
}

void DecodePacksTest(const char *datafile,int seek_pos)
{ // PackS
    uchar *data = NULL;    
    void *lens = NULL;
    uint decomposer_id = 0;
    uchar no_groups = 0;

    int no_nulls = 0;
    uchar optimal_mode = 0;
    uchar prehead[9];
    FILE *fp=fopen(datafile,"rb");
    assert(fp);
    fseek(fp,seek_pos,SEEK_SET);
    int buf_used = fread(prehead,1,9,fp); 
    assert(buf_used==9);
    uint total_size = *((uint *)prehead);
    uchar tmp_mode = prehead[4];
    int no_obj_in_pack= *((ushort*)(prehead + 5));
    ++no_obj_in_pack;
    no_nulls = *((ushort*)(prehead + 7));
    int header_size;
    uchar ver;
    int data_full_byte_size=0;
    if(tmp_mode >= 8 && tmp_mode <=253) {
        ver = tmp_mode;
        uchar head[10];
        buf_used = fread(head,1,10,fp);
        optimal_mode = head[0];
        
        decomposer_id = *((uint*)(head + 1));
        no_groups = head[5];
        data_full_byte_size = *((uint*)(head + 6));
        header_size = 19;
    } else {
        ver = 0;
        optimal_mode = tmp_mode;
        uchar head[4];
        buf_used = fread(head,1,4,fp);
        data_full_byte_size = *((uint*)(head));
        header_size = 13;
    }
    int len_mode=2;
    uint *nulls=NULL;
    if(!ib_packs_compressmode_assist::IsModeValid(optimal_mode)) {
        ThrowWith("Error: Unexpected byte in data pack (AttrPackS).\n");
    } else {
        int _no_nulls = no_nulls;
        if(data == NULL) {
           if(data_full_byte_size) {
              data = (uchar*)malloc(data_full_byte_size);
              if(NULL == data){
                 ThrowWith("Error: out of memory ( %d bytes failed). (40).\n",data_full_byte_size);
              }
           }
        }
        lens = malloc(len_mode * no_obj_in_pack * sizeof(char));
        if(no_nulls > 0) {
            if(!nulls){
                nulls = (uint*) malloc(2048 * sizeof(uint));
            }
            if(no_obj_in_pack < 65536){
                memset(nulls, 0, 8192);
            }
        }
        int comp_buf_size=0;
        uchar *compressed_buf=NULL;
        if(ib_packs_compressmode_assist::IsModeNoCompression(optimal_mode)){ // 没有压缩的,直接读取即可
            if(data){
                fread((char*) *data,1,data_full_byte_size,fp);
            }
            if(nulls){
                fread((char*) nulls,1,(no_obj_in_pack + 7) / 8,fp);
            }
            if(lens){
                fread((char*) lens,1,len_mode * no_obj_in_pack,fp);
            }
        }else
        {
            // normal compression
            if(ib_packs_compressmode_assist::IsModeCompressionApplied(optimal_mode)) // 0,1    = Huffmann 16-bit compression of remainings with tree (0 - flat null encoding, 1 - compressed nulls)
            {
                comp_buf_size = total_size - header_size;
                compressed_buf = (uchar*)malloc((comp_buf_size + 1) * sizeof(uchar));

                if(!compressed_buf) {
                    ThrowWith("Error: out of memory (%d bytes failed). (32)\n",comp_buf_size+1);
                }
                
                buf_used = fread((char*)compressed_buf,1,comp_buf_size,fp);

                // 对数据包进行解压操作                                     
                int ret = ib_packs_decompress(compressed_buf,comp_buf_size,no_obj_in_pack,optimal_mode,data,data_full_byte_size,len_mode,lens,nulls,_no_nulls);
                if(ret != 0){
                    ThrowWith("Error:ib_packs_decompress error.");
                }

            } 
            else {
                ThrowWith("The process reached to an invalid code path! (LoadData)\n");
            }
        }

        if(lens != NULL){
            free(lens);
            lens = NULL;
        }
        if(data != NULL){
            free(data);
            data = NULL;
        }   
        if(nulls != NULL){
            free(nulls);
            nulls = NULL;
        }
        if(compressed_buf != NULL){
            free(compressed_buf);
            compressed_buf = NULL;
        }
    }
}


std::string GetRiakKey(const int64 packno)
{
    char riak_key_buf[32]; 
    std::string ret; 
    snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", packno>>12,(int)(packno&0x00000fff));
    ret = riak_key_buf; 
    return ret;
}

int AttrPackN_Save(const int no_nulls,         /*:    null 个数             */
                    const uint *nulls,            /*:    nulls 存储缓存         */    
                    const void* data_full,        /*:    完整数据            */
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const _uint64 minv,            /*: 数据中的最小值        */
                    const _uint64 maxv,            /*: 数据中的最大值        */
                    const std::string& str_riak_key_node)        /*: riak 中存储的key*/
{
    uint total_size = 1;
    msgRCAttr_packN msg_pack;
    std::string riak_value;
    int ret = 0;

    msg_pack.set_no_obj(no_obj - 1);
    msg_pack.set_no_nulls(no_nulls);
    msg_pack.set_optimal_mode(optimal_mode);
    msg_pack.set_min_val(minv);
    msg_pack.set_max_val(maxv);
    msg_pack.set_total_size(total_size);
    
    ValueType value_type = GetValueType(maxv);

    if(no_nulls)
        msg_pack.set_nulls(nulls, (no_obj + 7) / 8);

    if(data_full)
        msg_pack.set_data(data_full, value_type * no_obj);

    std::string riak_params = "";
    msg_pack.SerializeToString(&riak_value);

#ifdef PRINT_LOG_INFO
	lgprintf("AttrPackN_Save, riakkey = %s , value_size = %d\n", str_riak_key_node.c_str(), (int)(riak_value.size()));
#endif

    if(pAsyncRiakHandle == NULL){
        ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }else{
        ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }
    if (ret != 0) {
        std::string errmsg;
        char errmsg_buf[128];
        snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackN [%s] save failed, ret [%d], msg [%s]",
            str_riak_key_node.c_str(), ret, strerror(ret));
        errmsg = errmsg_buf;
        ThrowWith("%s\n",errmsg.c_str());
    }

    return ret;
}

// 保存packn包
int AttrPackN_Save_2(const int no_nulls,         /*:    null 个数             */    
                    const char* compressed_buf,   /*:    压缩数据包           */
                    const uint compressed_buf_len,/*:    压缩后数据长度 	*/
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const _uint64 minv,            /*: 数据中的最小值        */
                    const _uint64 maxv,            /*: 数据中的最大值        */
                    const std::string& str_riak_key_node,        /*: riak 中存储的key*/
                    const std::string& riak_params) /*: 压缩类型参数 */
{
    ThrowWith("generate_table::AttrPackN_Save_2 调用错误.");
    return 0;
}



int AttrPackS_Save(const int no_nulls,         /*:    null 个数             */
                    const uint *nulls,            /*:    nulls 存储缓存         */    
                    const uchar* data,            /*:    完整数据            */
                    const uint data_full_byte_size,/*: 完整数据长度*/
                    const short len_mode,
                    const void* lens,  
                    const uint decomposer_id,
                    const uchar no_groups,
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const uchar ver,        /*: 数据中的最大值        */
                    const std::string& str_riak_key_node)        /*: riak 中存储的key*/
{
    msgRCAttr_packS msg_pack;
    std::string riak_value;
    msg_pack.set_no_obj(no_obj - 1);
    msg_pack.set_no_nulls(no_nulls);
    msg_pack.set_optimal_mode(optimal_mode);
    msg_pack.set_decomposer_id(decomposer_id);
    msg_pack.set_no_groups(no_groups);
    msg_pack.set_data_full_byte_size(data_full_byte_size);
    msg_pack.set_ver(ver);
    std::string riak_params = " ";
		
    if(data) {
		std::string tmp_data;    	
    	tmp_data.append((char*)data,(int)data_full_byte_size);
        msg_pack.set_data(tmp_data);
    }

    if(nulls) {
        msg_pack.set_nulls(nulls, (no_obj + 7) / 8);
    }

    if(lens) {
        msg_pack.set_lens(lens, len_mode * no_obj);
    }

    if (!msg_pack.SerializeToString(&riak_value)) {
        printf("Serialize error at: %s\n", str_riak_key_node.c_str());
    }

#ifdef PRINT_LOG_INFO
	lgprintf("AttrPackS_Save, riakkey = %s , value_size = %d\n", str_riak_key_node.c_str(), (int)(riak_value.size()));
#endif

    int ret = 0;
    if(pAsyncRiakHandle == NULL){
        ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }else{
        ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }
    if (ret != 0) {
        std::string errmsg;
        char errmsg_buf[128];
        snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackS [%s] save failed, ret [%d], msg [%s]",str_riak_key_node.c_str(), ret, strerror(ret));
        errmsg = errmsg_buf;
        ThrowWith("%s\n",errmsg.c_str());
    }

    return 0;
}


int AttrPackS_Save_2(const int no_nulls,         /*:    null 个数             */
                    const char* compressed_buf,   /*:    压缩数据包           */
                    const uint compressed_buf_len,/*:    压缩后数据长度 	*/
                    const uint data_full_byte_size,/*: 完整数据长度*/
                    const short len_mode,
                    const uint decomposer_id,
                    const uchar no_groups,
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const uchar ver,        
                    const std::string& str_riak_key_node,        /*: riak 中存储的key*/
                    const std::string& riak_params) /*: 压缩类型参数 */
{
    ThrowWith("generate_table::AttrPackS_Save_2 调用错误.");
    return 0;
}
                    


typedef struct stru_table_path
{
    char _pbasepth[300];
    char _pdbname[300];
    char _ptbname[300];
    stru_table_path(const char* pbasepth,const char* pdbname,const char* ptbname){
        strcpy(_pbasepth, pbasepth);      
        strcpy(_pdbname, pdbname);
        strcpy(_ptbname, ptbname);
    }
}stru_table_path,*stru_table_path_ptr;
void DecodePacksTest(const char *datafile,int seek_pos);

int main(int argc,char *argv[]) {
    WOCIInit("generate_table/generate_table_");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false); 
    if(argc == 3) {
        DecodePacksTest(argv[1],atoi(argv[2]));           
        WOCIQuit(); 
        return 0;        
    }
    else if(argc != 6){
        lgprintf("将DMA单机版本表移动转换到分布式版本,输入参数错误!\n");
        lgprintf("参数说明: ");
        lgprintf("        ./generate_table 源数据库数据路径  源数据库名称 源数据库表名称 目标数据库数据路径 目标数据库名称 ");
        lgprintf("        src_path:源数据库数据路径,通常为$DATAMERGER_HOME/var");
        lgprintf("        src_dbname:源数据库名称");
        lgprintf("        src_tbname:源数据库表名称");
        lgprintf("        dst_path:目标数据库数据路径,通常为$DATAMERGER_HOME/var");
        lgprintf("        dst_dbname:目标数据库名称");
        lgprintf("样例:");
        lgprintf("        ./generate_table /home/dma_new/dma/var dbtest dp_datapart /home/dma_new/dma_riak_test/var dbtest");
        WOCIQuit(); 
        return -__LINE__;
    }

    // 源表和目标表结构
    stru_table_path st_src_table(argv[1],argv[2],argv[3]);
    stru_table_path st_dst_table(argv[4],argv[5],argv[3]);
    
  
    long ret = 0;
    pthread_once(&_riak_cluster_init_once, _riak_cluster_init);    
    if(_riak_cluster == NULL){
        ThrowWith("目标表创建连接对象失败,程序退出.");
    }

    // 异步的线程数目设置,异步save的时候，线程数不能超过riak节点数的4倍
    int cfree = 0;
    getfreecpu(cfree);
    int maxthread= cfree;
    if(_riak_connect_number>0){
        maxthread = (_riak_connect_number > cfree) ? (_riak_connect_number):(cfree);
    }else{ 
        maxthread = (_riak_nodes > cfree) ? (_riak_nodes):(cfree);
    }
    _riak_max_thread = maxthread;
    
    // 开始对dma单机版本的表转换成分布式版本的表
    ret = generate_riak_table(st_src_table._pbasepth,st_src_table._pdbname,st_src_table._ptbname,
                              st_dst_table._pbasepth,st_dst_table._pdbname,st_dst_table._ptbname);
finish:
    if(_riak_cluster!= NULL){
        delete _riak_cluster;
        _riak_cluster = NULL;
    }

    WOCIQuit(); 
    return 1;
}




