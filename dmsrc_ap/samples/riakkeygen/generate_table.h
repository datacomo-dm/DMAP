#ifndef GENERATE_TABLE_DEF_H
#define GENERATE_TABLE_DEF_H

#include<stdlib.h>
#include<string.h>
#include<vector>
#include "IBCompress.h"
#include "wdbi_inc.h"
#include "dt_common.h"
#include<string>
using namespace std;

#ifndef int64_def
#define int64_def
typedef _uint64 int64;
#endif

extern "C" RiakCluster *_riak_cluster;
extern "C" void*    pAsyncRiakHandle;


#define RIAK_PAT_NAME  "RIAK_PACKET_PAT"
#define RIAK_HOSTNAME_ENV_NAME    "RIAK_HOSTNAME"
#define RIAK_PORT_ENV_NAME    "RIAK_PORT"
#define    RIAK_CONNTCT_THREAD_TIMES_ENV_NAME "RIAK_CONNTCT_THREAD_TIMES"   // 查询操作: riak节点倍数，用于设置查询线程数目
#define RIAK_CONNECT_THREAD_NUMBER_ENV_NAME "RIAK_CONNECT_NUMBER"        // 装入和查询操作: 连接riak的线程数目，设置了该选项:RIAK_CONNTCT_THREAD_TIMES 无效
#define    RIAK_MAX_CONNECT_PER_NODE_ENV_NAME "RIAK_MAX_CONNECT_PER_NODE"   // riak驱动层，每一个riak node节点连接池的最大连接数
#define RIAK_CLUSTER_DP_BUCKET_NAME    "IB_DATAPACKS"
#define    RIAK_CONNTCT_THREAD_TIMES_MIN    4
#define RIAK_ASYNC_WAIT_TIMEOUT_US    1000000*1800       //  异步save包，退出wait超时时间30分钟
#define RIAK_ASYNC_PUT_TIMEOUT_US    1000000*300         //  异步save包，阻塞等待超时时间5分钟


bool _doesFileExist(std::string const& file)
{
    struct stat stat_info;
    return (0 == stat(file.c_str(), &stat_info));
}


void _removeFile(std::string const& path)
{
    assert(path.length());
    if (remove(path.c_str())) {
        if (_doesFileExist(path)) {
            remove(path.c_str());
        }
    }
}

void _renameFile(string const& OldPath, string const& NewPath)
{
    assert(OldPath.length() && NewPath.length());
    if (rename(OldPath.c_str(), NewPath.c_str())) {
        rename(OldPath.c_str(), NewPath.c_str());
    }
}

// 将老的数据库表,dpn,part和转换成新的riak保存的数据
typedef struct stru_dpn_part_assist
{
    char fdpn_name[256];        // dpn
    FILE* fdpn_handle;            
    char fpart_name[256];        // part
    FILE* fpart_handle;
    char fcurrent_part_name[300]; // 正在处理的分区名称
    stru_dpn_part_assist()
    {
        fdpn_name[0]=0;
        fpart_name[0]=0;
        fcurrent_part_name[0] = 0;
        fdpn_handle = NULL;
        fpart_handle = NULL;
    }    
    ~stru_dpn_part_assist(){
        fdpn_name[0]=0;
        fpart_name[0]=0;    
        fcurrent_part_name[0] = 0;
        if(fdpn_handle){
            fclose(fdpn_handle);
            fdpn_handle = NULL;
        }
        if(fpart_handle){
            fclose(fdpn_handle);
            fpart_handle = NULL;
        }
    }
    void SetCurrentPartName(const char* pcur_part_name){
        strcpy(fcurrent_part_name,pcur_part_name);
    }
    char * GetCurrentPartName(){
        return fcurrent_part_name;
    }
}stru_dpn_part_assist,*stru_dpn_part_assist_ptr;

typedef struct stru_src_table_assist
{
    char table_path[256];           // table.bht
    stru_dpn_part_assist _s_d_p_item;
    char fpack_name[256];         // current pack file name
    FILE* fpack_handle;
    int pack_addr;            // current packaddr 
    stru_src_table_assist()
    {
        table_path[0] = 0;
        fpack_name[0] = 0;
        fpack_handle =  NULL;
        pack_addr= 0;
    }
    ~stru_src_table_assist()
    {
        table_path[0] = 0;
        fpack_name[0] = 0;
        if(fpack_handle){
            fclose(fpack_handle);
            fpack_handle =  NULL;
        }
        pack_addr= 0;
    }
}stru_src_table_assist,*stru_src_table_assist_ptr;

#define MAX_PACK_IN_RIAKPNODE 0x1000
typedef struct stru_dst_table_assist
{
    char table_path[256];
    stru_dpn_part_assist _s_d_p_item;
    int64 riak_pnode; // riak 获取的
    int pack_file;     
    int pack_addr;
    int table_number;
    int attr_number;
    stru_dst_table_assist()
    {
        table_path[0] = 0;
        riak_pnode = 0;
        pack_file = -1;
        pack_addr = 0;
        table_number = 0;
        attr_number = 0;
    }
    void ResetRiakPNode()
    {
        pack_file = -1;
        pack_addr = 0;
        table_number = 0;
        attr_number = 0;
    }
    long GetRiakPNode(){
        return riak_pnode;
    }
    
    bool RiakAllocPNode()
    {
        long newriaknode= _riak_cluster->KeySpaceAlloc(RIAK_PAT_NAME,table_number,attr_number);
        if(newriaknode<0) {
            lgprintf("Internal error: allocate packet id failed. get pnode %ld,table_number %d,attr_number %d.",newriaknode,table_number,attr_number);
            throw -1;    
        }
        else {
            lgprintf("RiakAllocPNode::Got pnode %ld ,tabid:%d ,attr : %d",newriaknode,table_number,attr_number);
        }
        if(newriaknode%MAX_PACK_IN_RIAKPNODE!=0){
            lgprintf("Internal error: allocate packet id failed. get pnode %ld,table_number %d,attr_number %d.",newriaknode,table_number,attr_number);
            throw -1;
        }
        
        riak_pnode =newriaknode;
        pack_file = (int)(riak_pnode>>32);
        pack_addr = (int)(riak_pnode&0xffffffff);
    }

    bool ShouldAllocPNode()
    {
        // 注意： == 优先级高于& 
        return ((pack_addr&0xfff) == 0xfff || (pack_file == -1));
    }
    
    bool AddRiakPNode()
    {
        pack_addr += 1;
    }
    
    std::string Get_riak_key()
    {
        char riak_key_buf[32]; 
        std::string ret; 
        int64 pack_no = (((int64)pack_file)<<32)+pack_addr;
		snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", pack_no>>12,(int)(pack_no&0xfff));
        ret = riak_key_buf; 
        return ret;
    }

}stru_dst_table_assist,*stru_dst_table_assist_ptr;


//----------------------------------------------------------------------------------------

enum AttrPackType {PackUnknown = 0, PackN, PackT, PackB, PackS};
const static int PF_NULLS_ONLY    = -1; // Special value pack_file=-1 indicates "nulls only".
const static int PF_NO_OBJ        = -2; // Special value pack_file=-2 indicates "no objects".
const static int PF_NOT_KNOWN    = -3; // Special value pack_file=-3 indicates "number not determined yet".
const static int PF_END            = -4; // Special value pack_file=-4 indicates the end of numbers list.
const static int PF_DELETED        = -5; // Special value pack_file=-5 indicates that the datapack has been deleted

enum AttributeType    {    RC_STRING,                // string treated either as dictionary value or "free" text
                        RC_VARCHAR,                // as above (discerned for compatibility with SQL)
                        RC_INT,                    // integer 32-bit
                        RC_NUM,                    // numerical: decimal, up to DEC(18,18)
                        RC_DATE,                // numerical (treated as integer in YYYYMMDD format)
                        RC_TIME,                // numerical (treated as integer in HHMMSS format)
                        RC_BYTEINT,                // integer 8-bit
                        RC_SMALLINT,            // integer 16-bit
                        RC_BIN,                    // free binary (BLOB), no encoding
                        RC_BYTE,                // free binary, fixed size, no encoding
                        RC_VARBYTE,                // free binary, variable size, no encoding
                        RC_REAL,                // double (stored as non-interpreted _int64, null value is NULL_VALUE_64)
                        RC_DATETIME,
                        RC_TIMESTAMP,
                        RC_DATETIME_N,
                        RC_TIMESTAMP_N,
                        RC_TIME_N,
                        RC_FLOAT,
                        RC_YEAR,
                        RC_MEDIUMINT,
                        RC_BIGINT,
                        RC_UNKNOWN = 255
                    };

bool IsInteger32Type(AttributeType attr_type)
{
    return attr_type == RC_INT     || attr_type == RC_BYTEINT || attr_type == RC_SMALLINT || attr_type == RC_MEDIUMINT;
}
bool IsNumericType(AttributeType attr_type)
{
    return IsInteger32Type(attr_type) || attr_type == RC_BIGINT || attr_type == RC_NUM || attr_type == RC_FLOAT || attr_type == RC_REAL;
}
bool IsDateTimeType(AttributeType attr_type)
{
    return attr_type == RC_DATE || attr_type == RC_TIME || attr_type == RC_YEAR || attr_type == RC_DATETIME
            || attr_type == RC_TIMESTAMP;
}

//----------------------------------------------------------------------------------
int  CalculateBinSize(unsigned int n)            // 000 -> 0, 001 -> 1, 100 -> 3 etc.
{
    int s=0;
    while(n>0)
    {
        n=n>>1;
        s++;
    }
    return s;
}

ValueType ChooseValueType(int bit_rate)
{
    if(bit_rate <= 8)
        return UCHAR;
    else if(bit_rate <= 16)
        return USHORT;
    else if(bit_rate <= 32)
        return UINT;
    else
        return UINT64;
}

// ftype {0:read,1:write}
// path: $DATAMERGER_HOME/var/dbname/tbname.bht
// return: T{A/B}xxxxx.ctb文件
std::string AttrFileName(const std::string path,const int attr_number) 
{
    char fnm[] = { "TA00000.ctb" };

    bool oppositeName =false;
    int ftype = 0;
    
    if(ftype == 1) {
        fnm[1] = 'S'; // save file:   TS000...
    } else {
        ABSwitcher absw;
        ABSwitch cur_ab_switch_state = absw.GetState(path);

        if(oppositeName) {
            if(cur_ab_switch_state == ABS_A_STATE)
                fnm[1] = 'B';
        } else {
            if(cur_ab_switch_state == ABS_B_STATE)
                fnm[1] = 'B';
        }
    }

    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename +="/";
    filename += fnm;
    return filename;
}


// path: $DATAMERGER_HOME/var/dbname/tbname.bht
std::string DpnFileName(const std::string path,const int attr_number) 
{
    char fnm[] = { "TA00000DPN.ctb" };
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += "/";
    filename +=fnm;
    return filename;
}

std::string AttrPackFileNameDirect(int attr_number, int n_file, const std::string& path)
{
    char fnm[] = {"TA00000000000000.ctb"};
    fnm[15]=(char)('0'+n_file%10);
    fnm[14]=(char)('0'+(n_file/10)%10);
    fnm[13]=(char)('0'+(n_file/100)%10);
    fnm[12]=(char)('0'+(n_file/1000)%10);
    fnm[11]=(char)('0'+(n_file/10000)%10);
    fnm[10]=(char)('0'+(n_file/100000)%10);
    fnm[9]=(char)('0'+(n_file/1000000)%10);
    fnm[8]=(char)('0'+(n_file/10000000)%10);
    fnm[7]=(char)('0'+(n_file/100000000)%10);
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += "/";
    filename += fnm;
    return filename;
}


// 返回CPU数量和 空闲CPU个数 
// 返回CPU数量和 空闲CPU个数 
double getfreecpu(int &cpuct) {
    FILE *fp=fopen("/proc/stat","rt");
    char *line=NULL;
    size_t len=0;
    long user=1,nice=0,system=0,idle=0,iowait=0,irq=0,softirq=0;
    cpuct=0;
    static int sys_cpuct=-1;
    while(getline(&line,&len,fp)!=-1) {
        if(strncmp(line,"cpu ",4)==0) {
            sscanf(line+5,"%ld %ld %ld %ld %ld %ld %ld",&user,&nice,&system,&idle,&iowait,&irq,&softirq);
            if(sys_cpuct!=-1) break;
        }
        else if(strncmp(line,"cpu",3)==0) sys_cpuct++;
        else break;
    }
    cpuct=sys_cpuct;
    if(cpuct==0) cpuct==1;
    if(line) free(line);
    fclose(fp);
    return (double)cpuct*idle/((double)user+nice+system+idle+iowait+irq+softirq);
}


// 保存packn包
int AttrPackN_Save( const int no_nulls,         /*:    null 个数             */
                    const uint *nulls,            /*:    nulls 存储缓存         */    
                    const void* data_full,        /*:    完整数据            */
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const _uint64 minv,            /*: 数据中的最小值        */
                    const _uint64 maxv,            /*: 数据中的最大值        */
                    const std::string& str_riak_key_node);        /*: riak 中存储的key*/

// 保存packn包
int AttrPackN_Save_2(const int no_nulls,         /*:    null 个数             */    
                    const char* compressed_buf,   /*:    压缩数据包           */
                    const uint compressed_buf_len,/*:    压缩后数据长度 	*/
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const _uint64 minv,            /*: 数据中的最小值        */
                    const _uint64 maxv,            /*: 数据中的最大值        */
                    const std::string& str_riak_key_node,        /*: riak 中存储的key*/
                    const std::string& riak_params); /*: 压缩类型参数 */


// 保存packs包
int AttrPackS_Save(const int no_nulls,         /*:    null 个数             */
                    const uint *nulls,            /*:    nulls 存储缓存         */    
                    const uchar* data,        /*:    完整数据            */
                    const uint data_full_byte_size,/*: 完整数据长度*/
                    const short len_mode,
                    const void* lens,  
                    const uint decomposer_id,
                    const uchar no_groups,
                    const int no_obj,            /*: 数据包中对象数        */
                    const uchar optimal_mode,        /*: 压缩状态选项        */ 
                    const uchar ver,        
                    const std::string& str_riak_key_node);        /*: riak 中存储的key*/


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
                    const std::string& riak_params); /*: 压缩类型参数 */




//////////////////////////////////////////////////////////////////////////////////////////////////

// 
// 1. 正在装入的单次装入文件大小的文件
// PackSize_tabid.loading.ctl ,文件格式如下:
/*
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
*/

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
更新文件的时候，将PackSize_tabid.ctl 内容读取出来后，写入
PackSize_tabid.commit.ctl
写入完成后，将PackSize_tabid.commit.ctl --> PackSize_tabid.ctl
*/
class PackSizeInfo
{
public:
    PackSizeInfo(std::string path,int tabid);
    virtual ~ PackSizeInfo();

protected:
    std::string _strpath;
    int _tabid;

    // 分区包字节信息结构
    typedef struct StruPartPackSize
    {
        short part_name_len;
        char part_name[256];
        unsigned long packsize;
        StruPartPackSize(){
            part_name_len = 0;
            part_name[0] = 0;
            packsize = 0;
        }
    }StruPartPackSize;

public:
    std::string GetPackSizeFileName();    // PackSize_tabid.ctl 
    std::string GetCommitPackSizeFileName(); // PackSize_tabid.ctl.tmp 
    std::string GetLoadingPackSizeFileName();// PackSize_tabid.loading.ctl    
    bool SaveTmpPackSize(const std::string strPartName,const unsigned long packsize); //  PackSize_tabid.loading.ctl    
    bool GetTmpPackSize(std::string& strPartName,unsigned long& packsize);
    bool UpdatePartPackSize(const std::string strPartName,const unsigned long packsize); //  PackSize_tabid.ctl
    bool DeletePartPackSize(const std::string strPartName)    ;//   PackSize_tabid.ctl
    bool GetTableSize(); // PackSize_tabid.ctl 中所有分区大小
    bool CommitPackSize();
    bool RollBackPackSize();
};
//----------------------------------------------------------------------------------------------

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
PackSizeInfo::PackSizeInfo(std::string path,int tabid)
{
    this->_strpath = path;
    this->_tabid = tabid;
}

PackSizeInfo::~PackSizeInfo()
{
    this->_strpath = "";
    this->_tabid = 0;
}

// PackSize_tabid.ctl 
std::string PackSizeInfo::GetPackSizeFileName()    
{
    char tmpfn[256];
    snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.ctl",this->_strpath.c_str(),this->_tabid);
    std::string fn(tmpfn);
    return fn;
}

std::string PackSizeInfo::GetCommitPackSizeFileName() // PackSize_tabid.ctl.tmp
{
    char tmpfn[256];
    snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.ctl.tmp",this->_strpath.c_str(),this->_tabid);
    std::string fn(tmpfn);
    return fn;
}

 // PackSize_tabid.loading.ctl    
std::string PackSizeInfo::GetLoadingPackSizeFileName()  
{
    char tmpfn[256];
    snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.loading.ctl",this->_strpath.c_str(),this->_tabid);
    std::string fn(tmpfn);
    return fn;
}

//  PackSize_tabid.loading.ctl    
// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
bool    PackSizeInfo::SaveTmpPackSize(const std::string strPartName,const unsigned long packsize) 
{
    std::string tmpPackFile = GetLoadingPackSizeFileName();
    
    FILE* pFile = NULL;
    if(_doesFileExist(tmpPackFile)){
        _removeFile(tmpPackFile);
    }
    pFile = fopen(tmpPackFile.c_str(),"wb");
    assert(pFile);

    // [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
    // read
    short part_name_len = strPartName.size();
    if(fwrite(&part_name_len,sizeof(part_name_len),1,pFile) != 1){
        lgprintf("Error: SaveTmpPackSize file:%s part_name_len error. ",tmpPackFile.c_str());
        fclose(pFile);
        pFile = NULL;            
        return false;
    }

    const char* p=strPartName.c_str();
    if(fwrite(p,part_name_len,1,pFile) != 1){
        lgprintf("Error: SaveTmpPackSize file: %s strPartName error. ",tmpPackFile.c_str());
        fclose(pFile);
        pFile = NULL;            
        return false;
    }
    
    if(fwrite(&packsize,8,1,pFile) != 1){
        lgprintf( "Error: SaveTmpPackSize file:%s packsize error. ",tmpPackFile.c_str());
        fclose(pFile);
        pFile = NULL;            
        return false;
    }
    fclose(pFile);
    pFile = NULL;
    lgprintf("Info: SaveTmpPackSize file:%s do not exist, create it. sise = %ld bytes.",tmpPackFile.c_str(),packsize);

    return true;
}

//  PackSize_tabid.loading.ctl    
// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
bool PackSizeInfo::GetTmpPackSize(std::string& strPartName,unsigned long& packsize)
{
    std::string tmpPackFile = GetLoadingPackSizeFileName();
    
    FILE* pFile = NULL;
    if(!_doesFileExist(tmpPackFile)){
        lgprintf("Warning: GetTmpPackSize file: %s do not exist , pack size is 0 ." ,tmpPackFile.c_str());
        return false;
    }
    else{
        pFile = fopen(tmpPackFile.c_str(),"r+b"); // read ,modify
        assert(pFile);

        // read partname len
        short part_name_len = 0;
        if(fread(&part_name_len,sizeof(part_name_len),1,pFile) != 1){
            lgprintf("Error: GetTmpPackSize file: %s read part_name_len error. " ,tmpPackFile.c_str());

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }

        // read part_name
        char part_name[256];
        if(fread(part_name,part_name_len,1,pFile) != 1){
            lgprintf("Error: GetTmpPackSize file:%s read part_name error. " ,tmpPackFile.c_str());

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }        
        part_name[part_name_len] = 0;
        strPartName = std::string(part_name);

        // read pack size
        packsize = 0;
        if(fread(&packsize,8,1,pFile) != 1){
            lgprintf("Error: GetTmpPackSize file:%s read pack size error. ",tmpPackFile.c_str());;

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }        
    }
    
    fclose(pFile);
    pFile = NULL;

    return true;

}

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
*/
bool     PackSizeInfo::UpdatePartPackSize(const std::string strPartName,const unsigned long packsize) //  PackSize_tabid.ctl
{
    std::string PackFile = GetPackSizeFileName();
    unsigned long spsize_total = 0;

    lgprintf("Info: UpdatePartPackSize to file %s part name %s packsize %ld",PackFile.c_str(),strPartName.c_str(),packsize);

    FILE* pFile = NULL;
    if(!_doesFileExist(PackFile)){
        lgprintf("Info: SavePackSize file:%s  do not exist,create pack file .",PackFile.c_str());
        
        pFile = fopen(PackFile.c_str(),"wb"); 
        assert(pFile != NULL);
        
        int ret = 0;
        
        short part_num = 1;
        ret = fwrite(&part_num,sizeof(short),1,pFile);
        assert(ret == 1);
        
        short partname_len = strPartName.size();
        ret = fwrite(&partname_len,sizeof(short),1,pFile);
        assert(ret == 1);
        
        ret = fwrite(strPartName.c_str(),partname_len,1,pFile);
        assert(ret == 1);

        spsize_total = packsize;
        ret = fwrite(&spsize_total,sizeof(long),1,pFile);
        assert(ret ==1);

        fclose(pFile);
        pFile = NULL;
        
        return true;
    }
    else{
        pFile = fopen(PackFile.c_str(),"rb"); 
        assert(pFile);

        //------------------------------------------------------
        // 1. 读取到内存
        short part_num = 0;
        if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
            lgprintf("Error: UpdatePartPackSize file: %s read part_num error.",PackFile.c_str());

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }

        std::vector<StruPartPackSize> part_pack_size_vector;
        int _part_index=0;
        while(_part_index<part_num)
        {
            StruPartPackSize _st_item;
            // read partname len
            if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file: %s read part_name_len error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }            

            // read part_name
            if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file: %s read part_name error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }        
            _st_item.part_name[_st_item.part_name_len]= 0;
            
            // read pack size
            if(fread(&_st_item.packsize,8,1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file: %s read pack size error. ",PackFile.c_str());
                fclose(pFile);
                pFile = NULL;
                
                return false;
            }    
            
            part_pack_size_vector.push_back(_st_item);            
            
            _part_index++;
        }

        fclose(pFile);
        pFile = NULL;

        //-------------------------------------------
        //2.更新分区的字节大小到内存
        bool exist_part = false;
        for(int i = 0;i<part_pack_size_vector.size();i++){
            if(strncmp(part_pack_size_vector[i].part_name,strPartName.c_str(),part_pack_size_vector[i].part_name_len) == 0){
                part_pack_size_vector[i].packsize += packsize;
                exist_part = true;
                break;
            }
        }

        if(!exist_part){
            StruPartPackSize _st_item;
            _st_item.part_name_len = strPartName.size();
            strcpy(_st_item.part_name, strPartName.c_str());
            _st_item.packsize = packsize;

            part_pack_size_vector.push_back(_st_item);
        }

        // -----------------------------------------
        // 3. 更新后的写入临时文件
        std::string CommitPackFile = GetCommitPackSizeFileName();
        
        pFile = fopen(CommitPackFile.c_str(),"wb"); //create file
        assert(pFile);

        part_num = (short)part_pack_size_vector.size();
        if(fwrite(&part_num,sizeof(part_num),1,pFile) != 1){
            lgprintf("Error: UpdatePartPackSize file: %s update size %ld error. ",PackFile.c_str(),packsize);
            fclose(pFile);
            pFile = NULL;            
            return false;
        }

        for(int i=0;i<part_pack_size_vector.size();i++)
        {
            // write partname len
            if(fwrite(&part_pack_size_vector[i].part_name_len,sizeof(part_pack_size_vector[i].part_name_len),1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file: %s read part_name_len error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                _removeFile(CommitPackFile);                
                return false;
            }

            // write part_name
            if(fwrite(part_pack_size_vector[i].part_name,part_pack_size_vector[i].part_name_len,1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file: %s read part_name error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                _removeFile(CommitPackFile);    
                
                return false;
            }        
            
            // write pack size
            if(fwrite(&part_pack_size_vector[i].packsize,8,1,pFile) != 1){
                lgprintf("Error: GetTmpPackSize file:%s read pack size error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                _removeFile(CommitPackFile);    
                
                return false;
            }    
        }

        //-------------------------------------------
        //4. 将新写入的临时文件替换成原来的文件
        _renameFile(CommitPackFile,PackFile);
    }
    
    fclose(pFile);
    pFile = NULL;

    return true;

}

bool PackSizeInfo::GetTableSize()// PackSize_tabid.ctl 中所有分区大小
{
    std::string PackFile = GetPackSizeFileName();
    unsigned long spsize_total = 0;

    FILE* pFile = NULL;
    if(!_doesFileExist(PackFile)){
        lgprintf( "Error: GetTableSize file: %s  do not exist, do not get pack size .",PackFile.c_str());
        return false;
    }
    else{
        pFile = fopen(PackFile.c_str(),"rb"); // read ,modify
        assert(pFile);

        short part_num = 0;
        if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
            lgprintf("Error: GetTableSize file:%s read part_num error. ",PackFile.c_str());

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }
    
        int _part_index=0;
        while(_part_index<part_num)
        {
            StruPartPackSize _st_item;
            // read partname len
            if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
                lgprintf("Error: GetTableSize file:%s read part_name_len error.  ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }

            // read part_name
            if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
                lgprintf("Error: GetTableSize file:%s  read part_name error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }        
            
            // read pack size
            if(fread(&_st_item.packsize,8,1,pFile) != 1){
                lgprintf("Error: GetTableSize file:%s read pack size error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }    
            spsize_total+= _st_item.packsize;    
            
            _part_index++;
        }
    }
}
    

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
*/
bool PackSizeInfo::DeletePartPackSize(const std::string strPartName)//   PackSize_tabid.ctl
{
    std::string PackFile = GetPackSizeFileName();
    unsigned long spsize_total = 0;

    lgprintf(" Info: DeletePartPackSize to file %s part name %s ", PackFile.c_str(),strPartName.c_str());

    FILE* pFile = NULL;
    if(!_doesFileExist(PackFile)){
        lgprintf("Error: DeletePartPackSize file: %s do not exist, do not delete pack size ." ,PackFile.c_str());
        return false;
    }
    else{
        pFile = fopen(PackFile.c_str(),"r+b"); // read ,modify
        assert(pFile);

        short part_num = 0;
        if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
            lgprintf("Error: DeletePartPackSize file:%s  read part_num error. " ,PackFile.c_str());

            fclose(pFile);
            pFile = NULL;
            
            return false;
        }

        std::vector<StruPartPackSize> part_pack_size_vector;
        int _part_index=0;
        while(_part_index<part_num)
        {
            StruPartPackSize _st_item;
            // read partname len
            if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
                lgprintf("Error: DeletePartPackSize file: %s read part_name_len error. " ,PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }

            // read part_name
            if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
                lgprintf("Error: DeletePartPackSize file: %s  read part_name error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }        
            
            // read pack size
            if(fread(&_st_item.packsize,8,1,pFile) != 1){
                lgprintf("Error: DeletePartPackSize file: %s read pack size error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }    
            
            part_pack_size_vector.push_back(_st_item);            
            
            _part_index++;
        }

        fclose(pFile);
        pFile=NULL;
        
        // 更新分区 的字节大小
        bool exist_part = false;
        std::vector<StruPartPackSize>::iterator iter;
        for(iter = part_pack_size_vector.begin();iter != part_pack_size_vector.end();iter++)
        {
            if(strcmp(iter->part_name,strPartName.c_str()) == 0)
            {    
                part_pack_size_vector.erase(iter);
                exist_part =true;
                break;         
            }
        }
        
        if(!exist_part){
            assert(0);
        }

        // 重新写入
        pFile = fopen(PackFile.c_str(),"wb"); 
        assert(pFile);

        part_num = (short)part_pack_size_vector.size();
        if(fwrite(&part_num,sizeof(part_num),1,pFile) != 1){
            lgprintf("Error: DeletePartPackSize file: %s error. ",PackFile.c_str());

            fclose(pFile);
            pFile = NULL;            
            return false;
        }

        for(int i=0;i<part_pack_size_vector.size();i++)
        {
            // write partname len
            if(fwrite(&part_pack_size_vector[i].part_name_len,sizeof(part_pack_size_vector[i].part_name_len),1,pFile) != 1){
                lgprintf( "Error: DeletePartPackSize file: %s  read part_name_len error. ",PackFile.c_str());

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }

            // write part_name
            if(fwrite(part_pack_size_vector[i].part_name,part_pack_size_vector[i].part_name_len,1,pFile) != 1){
                lgprintf( "Error: DeletePartPackSize file: %s  read part_name error. " ,PackFile.c_str());
                

                fclose(pFile);
                pFile = NULL;
                
                return false;
            }        
            
            // write pack size
            if(fwrite(&part_pack_size_vector[i].packsize,8,1,pFile) != 1){
                lgprintf("Error: DeletePartPackSize file: %s  read pack size error. ",PackFile.c_str());
                
                fclose(pFile);
                pFile = NULL;
                
                return false;
            }    
        }

    }
    
    fclose(pFile);
    pFile = NULL;

    return true;

}

bool PackSizeInfo::CommitPackSize()
{
    std::string  psi_loading_file = GetLoadingPackSizeFileName();
    if(_doesFileExist(psi_loading_file))
    {
        std::string _part_name ("");
        unsigned long _part_size = 0;
        if(GetTmpPackSize(_part_name,_part_size)){
            UpdatePartPackSize(_part_name,_part_size);
        }
        else{
            assert(0);
        }
        
        _removeFile(psi_loading_file);
    }
    // else: create table?
}
bool PackSizeInfo::RollBackPackSize()
{
    std::string  psi_loading_file = GetLoadingPackSizeFileName();
    if(_doesFileExist(psi_loading_file))
    {
        _removeFile(psi_loading_file);
    }
}
//----------------------------------------------------------------------------------


void SaveRiakCommitSize(const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,const int tab_number,const char* currentPartName,const long current_save_size){
     // [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
     char _path_bht[300];
     sprintf(_path_bht,"%s/%s/%s.bht",pnew_basepth,pnew_dbname,pnew_tbname);
     int _table_number = tab_number;
     
     PackSizeInfo psi(_path_bht,_table_number);
     std::string strLoadingFile = psi.GetLoadingPackSizeFileName();
     if(_doesFileExist(strLoadingFile)){
         _removeFile(strLoadingFile);
     }
     std::string strPartName(currentPartName);
     psi.SaveTmpPackSize(strPartName,current_save_size);   
     
     // 提交文件大小
     psi.CommitPackSize();  
}

int generate_copy_frm_bht(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                                const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,
                                const int attr_numbers)    
{
    lgprintf("开始拷贝表结构 %s/%s/%s --> %s/%s/%s.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);    

    // cp frm
    char run_cmd[300];
    sprintf(run_cmd,"cp %s/%s/%s.frm %s/%s/%s.frm",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);
    int ret = 0;
    lgprintf("拷贝表结构,运行命令: %s",run_cmd);
    ret = system(run_cmd);
    if(ret != 0){
        lgprintf("run cmd: %s error.",run_cmd);
        return -__LINE__;
    }

    // mkdir bht
    sprintf(run_cmd,"mkdir %s/%s/%s.bht",pnew_basepth,pnew_dbname,pnew_tbname);   
    lgprintf("拷贝表结构,运行命令: %s",run_cmd);
    ret = system(run_cmd);
    if(ret != 0){
        lgprintf("run cmd: %s error.",run_cmd);
        return -__LINE__;
    }

    // cp table.bht/ab_switch 
    char ab_switch_name[300];
    sprintf(ab_switch_name,"%s/%s/%s.bht/ab_switch",pold_basepth,pold_dbname,pold_tbname);
    if(_doesFileExist(ab_switch_name)){
        sprintf(run_cmd,"cp %s/%s/%s.bht/ab_switch %s/%s/%s.bht -rf",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);   
        lgprintf("拷贝表结构,运行命令: %s",run_cmd);
        ret = system(run_cmd);
        if(ret != 0){
            lgprintf("run cmd: %s error.",run_cmd);
            return -__LINE__;
        }
    }

    // cp table.bht/Table.ctb     
    sprintf(run_cmd,"cp %s/%s/%s.bht/Table.ctb %s/%s/%s.bht -rf",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);   
    lgprintf("拷贝表结构,运行命令: %s",run_cmd);
    ret = system(run_cmd);
    if(ret != 0){
        lgprintf("run cmd: %s error.",run_cmd);
        return -__LINE__;
    }
    
    // cp table.bht/T??????.ctb
    sprintf(run_cmd,"cp %s/%s/%s.bht/T??????.ctb %s/%s/%s.bht -rf",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);   
    lgprintf("拷贝表结构,运行命令: %s",run_cmd);
    ret = system(run_cmd);
    if(ret != 0){
        lgprintf("run cmd: %s error.",run_cmd);
        return -__LINE__;
    }

    // cp apindex 
    bool exist_packindex = false;
    char pack_index_file_name[300];
    for(int j=0;j<attr_numbers;j++){
        sprintf(pack_index_file_name,"%s/%s/%s.bht/attrindex_%d.ctb",pold_basepth,pold_dbname,pold_tbname,j);
        if(_doesFileExist(pack_index_file_name)){
            exist_packindex = true;
        }
    }
    
    if(exist_packindex)
    {
        // 1> cp table.bht/attrindex_*.ctb
        sprintf(run_cmd,"cp %s/%s/%s.bht/attrindex_*.ctb %s/%s/%s.bht -rf",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);   
        lgprintf("拷贝表结构,运行命令: %s",run_cmd);
        ret = system(run_cmd);
        if(ret != 0){
            lgprintf("run cmd: %s error.",run_cmd);
            return -__LINE__;
        }

        // 2> 获取生成packindex的列的编号
        // [dma_new@cloud01 test_20131125001_partition.bht]$ ls attrindex_*.ctb -1
        // attrindex_16.ctb
        // attrindex_19.ctb
        
        char lst_file[300];
        sprintf(lst_file,"%s/%s/%s.bht/apindex_%d.lst",pnew_basepth,pnew_dbname,pnew_tbname,getpid());
        unlink(lst_file);

        sprintf(run_cmd,"ls %s/%s/%s.bht/attrindex_*.ctb -1 > %s",pnew_basepth,pnew_dbname,pnew_tbname,lst_file);
        lgprintf("拷贝表结构,运行命令: %s",run_cmd);
        ret = system(run_cmd);
        if(ret != 0){
            lgprintf("run cmd: %s error.",run_cmd);
            return -__LINE__;
        }        

        // 获取packindex的列编号
        // /home/dma_new/dma_riak_test/var/dbtest/test_20131125001_partition.bht/attrindex_16.ctb -->16
        // /home/dma_new/dma_riak_test/var/dbtest/test_20131125001_partition.bht/attrindex_19.ctb -->19

        std::vector<int> attr_vector;
        {
            FILE *pf = fopen(lst_file,"rt");
            if(pf==NULL) ThrowWith("文件打开文件 %s 失败.",lst_file);
            char lines[300]={0};
            char str_attr_number[100]={0};
            bool start_copy = false;
            int attr_number  = 0;
            int lens = 0;
            while(fgets(lines,300,pf)!=NULL){
                lens = strlen(lines);
                const char* p = lines + (lens - 1);

                // /home/dma_new/dma_riak_test/var/dbtest/test_20131125001_partition.bht/attrindex_16.ctb  
                // -----> attrindex_16.ctb
                while((*p) != '/') p--;
                            
                char *q = str_attr_number;
                start_copy = false;
                while( (*p != '.') && (*p !='\t') && (*p!='\n') ){
                    if(*p == '_'){
                       start_copy = true;
                    }
                    if(start_copy){
                        if(*(p+1) != '.'){
                            *q = *(p+1);
                            q++;
                        }
                    }
                    p++;                    
                }
                *q = 0;
                attr_number = atoi(str_attr_number);
                attr_vector.push_back(attr_number);
            }
            fclose(pf);pf=NULL;
        }

        // 3> 逐个拷贝packindex索引
        // 16_p01
        // 16_p02
        // 19_p01
        // 19_p02
        for(int i=0;i<attr_vector.size();i++){
            sprintf(run_cmd,"cp %s/%s/%s.bht/%d_* %s/%s/%s.bht -rf",pold_basepth,pold_dbname,pold_tbname,attr_vector[i],pnew_basepth,pnew_dbname,pnew_tbname);   
            lgprintf("拷贝表结构,运行命令: %s",run_cmd);
            ret = system(run_cmd);
            if(ret != 0){
                lgprintf("run cmd: %s error.",run_cmd);
                return -__LINE__;
            }
        }
        unlink(lst_file);
    }
    
    lgprintf("开始拷贝表结构 %s/%s/%s --> %s/%s/%s 执行成功.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth,pnew_dbname,pnew_tbname);    
    
    return 0;
}

#endif


