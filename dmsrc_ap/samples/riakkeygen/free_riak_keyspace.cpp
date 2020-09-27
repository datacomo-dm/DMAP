#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include <unistd.h>
#include <vector>
#include <string>
#include<algorithm>
#include <assert.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <glob.h>

#include "riakcluster.h"
#include "wdbi_inc.h"
#include "dt_common.h"

using namespace std;

#ifndef uint_
typedef unsigned int uint;
#define uint_
#endif

#ifndef uchar_
typedef unsigned char uchar;
#define uchar_
#endif

#ifndef ushort_
typedef unsigned short ushort;
#define ushort_
#endif


#ifndef _uint64
#ifdef __GNUC__
typedef unsigned long long _uint64;
#else
typedef unsigned __int64 _uint64;
#endif
#endif

typedef  _uint64 int64;

#define ENV_KEY_SPACE_LOCK_DIR "RIAK_KEY_SPACE_DIR"
#define RIAK_KS_LOCK_FILE "riak_ks_lock"            // 不同进程建锁文件
#define RIAK_KS_ALLOC_STEMP 4096                    // 分配步长
#define RIAK_KS_ALLOC_FILE  "riak_ks_info.bin"      // 已经回收空闲的key
#define RIAK_CLUSTER_DP_BUCKET_NAME "IB_DATAPACKS"
#define RIAK_PAT_NAME  "RIAK_PACKET_PAT"
#define RIAK_HOSTNAME_ENV_NAME    "RIAK_HOSTNAME"
#define RIAK_PORT_ENV_NAME    "RIAK_PORT"
#define RIAK_CONNTCT_THREAD_TIMES_ENV_NAME "RIAK_CONNTCT_THREAD_TIMES"   // 查询操作: riak节点倍数，用于设置查询线程数目
#define RIAK_CONNECT_THREAD_NUMBER_ENV_NAME "RIAK_CONNECT_NUMBER"        // 装入和查询操作: 连接riak的线程数目，设置了该选项:RIAK_CONNTCT_THREAD_TIMES 无效
#define RIAK_MAX_CONNECT_PER_NODE_ENV_NAME "RIAK_MAX_CONNECT_PER_NODE"   // riak驱动层，每一个riak node节点连接池的最大连接数
#define RIAK_CLUSTER_DP_BUCKET_NAME    "IB_DATAPACKS"
#define RIAK_CONNTCT_THREAD_TIMES_MIN    4
#define RIAK_ASYNC_WAIT_TIMEOUT_US    1000000*1800       //  异步save包，退出wait超时时间30分钟
#define RIAK_ASYNC_PUT_TIMEOUT_US    1000000*300         //  异步save包，阻塞等待超时时间5分钟


RiakCluster *_riak_cluster = NULL;
pthread_once_t _riak_cluster_init_once = PTHREAD_ONCE_INIT;
void*    pAsyncRiakHandle = NULL;
int _riak_nodes = 0;
int _riak_connect_number = 0;
int _riak_max_thread = 0;

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


/////////////////////////////////////////////////////////////////////////////////////
// 生成riak-key
std::string _get_riak_key(const int64 packno)
{
    char riak_key_buf[32]; 
    std::string ret; 
    snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", packno>>12,(int)(packno&0xfff));
    ret = riak_key_buf; 
    return ret;
}

// return $ENV_KEY_SPACE_LOCK_DIR/riak_ks_lock
std::string _get_riak_ks_lock_file(){
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir){
        lgprintf("warning: getenv(ENV_KEY_SPACE_LOCK_DIR) return NULL \n");
        return std::string("");
    }
    
    std::string str_path(p_riak_space_dir);
    str_path += "/";
    str_path += RIAK_KS_LOCK_FILE;
    return str_path;
}

// return $ENV_KEY_SPACE_LOCK_DIR/riak_ks_info.bin
std::string _get_riak_ks_alloc_file(){
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir){
        lgprintf("warning: getenv(ENV_KEY_SPACE_LOCK_DIR) return NULL \n");
        return std::string("");
    }
    
    std::string str_path(p_riak_space_dir);
    str_path += "/";
    str_path += RIAK_KS_ALLOC_FILE;
    return str_path;
}

// dpn 的简化版
typedef struct stru_pack_file_addr
{
    stru_pack_file_addr(){
        pack_file = -1;
        pack_addr = -1;
        origin_pack_file = -1;
        origin_pack_addr = -1;
    }
    void _set_riak_key_space(int64_t _riak_key_space){
        pack_file = (int)(_riak_key_space>>32);
        origin_pack_file = pack_file;
        pack_addr = (int)(_riak_key_space&0xffffffff);
        origin_pack_addr = pack_addr;
    }
    void _add_riak_key_space(){
        pack_addr++;
    }
    int64 _get_riak_key() {
        assert(pack_file>=0);
        return (((int64)pack_file)<<32)+pack_addr;
    }

    bool _riak_ks_valid(){
        if(origin_pack_addr!=-1){
            return (pack_addr-origin_pack_addr)<RIAK_KS_ALLOC_STEMP;
        }else{
            return false;
        }
    }

    int origin_pack_file;
    int origin_pack_addr;
    
    int pack_file;
    int pack_addr;
}stru_pack_file_addr,*stru_pack_file_addr_ptr;

//  free the space on the disk according to the keyspace
// Usage: riak-toolbox kvtest_sc <PUT | GET | DEL> <bucket> <key> [param] [value | value_filename]
int _riak_free_keyspace_on_disk(stru_pack_file_addr_ptr _st_pack_file_addr)
{
    char cmd[1024];
    int64 pack_no = 0;
    std::string riak_key_str = "";

    pAsyncRiakHandle = _riak_cluster->AsyncInit(_riak_max_thread,1024,5);
    int ret = 0;
    while(_st_pack_file_addr->_riak_ks_valid()){
        riak_key_str = _get_riak_key(_st_pack_file_addr->_get_riak_key());
        ret = _riak_cluster->AsyncDel(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME,riak_key_str,NULL);        
        if(ret != 0){
            lgprintf("ERROR : _riak_cluster->AsyncDel(pAsyncRiakHandle,%s,%s,NULL) return = %d .",RIAK_CLUSTER_DP_BUCKET_NAME,riak_key_str.c_str(),ret);
            ThrowWith("ERROR : _riak_cluster->AsyncDel(pAsyncRiakHandle,%s,%s,NULL) return = %d .",RIAK_CLUSTER_DP_BUCKET_NAME,riak_key_str.c_str(),ret);           
        }
        _st_pack_file_addr->_add_riak_key_space();
    }
    std::string err_msg;
    ret = _riak_cluster->AsyncWait(pAsyncRiakHandle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
    if(ret < 0){
        ThrowWith(" Save pack error:%s ,AsyncWait return :%d",err_msg.c_str(),ret);
    }

    return 0;
}

int main(int argc,char *argv[])
{	     
    if(argc !=2){
        printf("free_riak_keyspace input param error\n");
        printf("example : free_riak_keyspace start_keyspace \n");
        printf("example : free_riak_keyspace 0 \n");
        return -1;
    }
    
    WOCIInit("free_riak_keyspace/free_riak_keyspace_");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false); 
      
    long ret = 0;
    pthread_once(&_riak_cluster_init_once, _riak_cluster_init);    
    if(_riak_cluster == NULL){
        ThrowWith("目标表创建连接对象失败,程序退出.");
    }

    int cfree = 24;
    int maxthread= cfree;
    if(_riak_connect_number>0){
        maxthread = (_riak_connect_number > cfree) ? (_riak_connect_number):(cfree);
    }else{ 
        maxthread = (_riak_nodes > cfree) ? (_riak_nodes):(cfree);
    }
    _riak_max_thread = maxthread;

    int64 start_key_space = atol(argv[1]);
    
    FILE* pf_lock = NULL;
    FILE* pf_ks = NULL;
    std::string _riak_lock_file = _get_riak_ks_lock_file();
    assert(_riak_lock_file.size()>0);
    std::string _riak_ks_file = _get_riak_ks_alloc_file();
    assert(_riak_ks_file.size()>0);
	ret = 0;
    while(1) // 打开文件
    {
        pf_lock = fopen(_riak_lock_file.c_str(),"wb"); 
        if(pf_lock == NULL){
            lgprintf("File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }
        int64_t ks_free_number = 0;
        int64_t ks_max = 0; 
        struct stat stat_info;  
        if(0 != stat(_riak_ks_file.c_str(), &stat_info)) // do not exist : RIAK_KS_ALLOC_FILE
        {
            lgprintf("File do not exist : Open file %s return NULL\n",_riak_lock_file.c_str());
            assert(0);
        }
        else // exist : RIAK_KS_ALLOC_FILE
        { 
            pf_ks = fopen(_riak_ks_file.c_str(),"rb");  // read/write
            if(pf_ks == NULL){
                lgprintf("File exist : Open file %s return NULL\n",_riak_ks_file.c_str());
                assert(0);
            }
            
            // update max ks
            ret = fread(&ks_max,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);
        
            // update free num
            ret = fread(&ks_free_number,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);

            // get the free riak key space
            int64_t _free_keyspace = 0;
            stru_pack_file_addr _st_pack_file_addr;
            bool start_list = false;
            int64_t ks_free_number_tatal =  ks_free_number;
            while(ks_free_number>0){                
                ret = fread(&_free_keyspace,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);

                if( start_key_space != 0 && !start_list ){
                    if( start_key_space == _free_keyspace){
                        start_list = true;
                    }
                    else{
                        ks_free_number--;  
                        continue; // 跳过前面部分keyspace
                    }
                }

                _st_pack_file_addr._set_riak_key_space(_free_keyspace);
                
                lgprintf("Begin to free keyspace %ld \n",_free_keyspace);
                
                _riak_free_keyspace_on_disk(&_st_pack_file_addr);

                ks_free_number--;               

                lgprintf("Free keyspace %ld , total keyspace %ld.",ks_free_number,ks_free_number_tatal);
            }           
        }   

        if(pf_ks){
            fclose(pf_ks);
            pf_ks = NULL;
        }

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;

        break;
    }   

finish:
    if(_riak_cluster!= NULL){
        delete _riak_cluster;
        _riak_cluster = NULL;
    }

    WOCIQuit(); 
    
    return 0;
}

