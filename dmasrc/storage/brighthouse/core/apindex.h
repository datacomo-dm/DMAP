#ifndef AP_INDEX_HEADER
#define AP_INDEX_HEADER

#include <cr_class/cr_class.h>
#include <assert.h>
#include <string.h>
#include <leveldb/db.h>
#include <util/hash.h>
#include <leveldb/write_batch.h>
#include <leveldb/slice.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <vector>
#include <iostream>
#include <set>
#include <map>
#include <string>
#include <unordered_map>
/* "readdir" etc. are defined here. */
#include <dirent.h>
/* limits.h defines "PATH_MAX". */
#include <limits.h>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#define ATTRINDEX_VERSION_1 0xa0
#define ATTRINDEX_VERSION_2 0xb0

// very slow for sparse_hash_map<string,string>: 4's--21s
//#include <sparsehash/sparse_hash_map>
//#include <sparsehash/dense_hash_map>

//#include <mm/cache_map.hpp>

#include "DMThreadGroup.h"
//尽可能一次装入过程不要拆分，一次Write
//521K
//#define MAX_JOBS (2<<20)
#define MAX_JOBS (1<<24)
#define MAX_WRITE_BATCH_MEM_SIZE    (1024*1024*1024)    //1G
const _int64 SWITCH_MAX_SUFFIX_MEM_SIZE = 1024*1024*1024;  //1G,key 后缀的切换大小
//#define MAX_JOBS 4096*16
#define MAX_PART_NAME_LEN 100
//using google::sparse_hash_map;
//using google::dense_hash_map;
// leveldb is thread safe,but not multi-process ready(can not open more than one times paralle)
class apindex;
struct DBEntry {
    leveldb::DB* db;
    leveldb::Options *options;
    // set bfmode before create option
    DBEntry()
    {
        db=NULL;
        options=NULL;
    }
    void CreateOption()
    {
        options=new leveldb::Options();
        if(bfmode)
            options->filter_policy = leveldb::NewBloomFilterPolicy(10);
        options->create_if_missing = true;
        // 32MB write buffer ( up to two write buffer)
        options->block_cache=leveldb::NewLRUCache(32 << 20) ;
        options->write_buffer_size = 32<<20;
        options->max_open_files=32768;
    }

    void Release()
    {
        if(db) delete db;
        if(options) {
            if(options->filter_policy)
                delete options->filter_policy;
            if(options->block_cache)
                delete options->block_cache;
            delete options;
            options = NULL;
        }
        db=NULL;
        options=NULL;
    }
    int refct;
    bool bfmode;
    std::vector<apindex *> _refs;
    ~DBEntry()
    {
        //if(bfmode)
        // delete options.filter_policy;
        // can not delete here as copy safe not imple.
        //delete db;
    }
};

class AutoLock
{
    DMMutex *plock;
public:
    AutoLock(DMMutex *p):plock(p)
    {
        plock->Lock();
    }
    ~AutoLock()
    {
        plock->Unlock();
    }
};


#define STRING_KEY

#ifndef LEVELDB_SLICE_HASH
#define LEVELDB_SLICE_HASH
namespace std
{
    template<>
    struct hash<leveldb::Slice>: public std::unary_function<leveldb::Slice, size_t> {
        size_t operator()(leveldb::Slice key) const
        {
            size_t hash = 5381;
            int c;
            const char *data=key.data();
            while(c = *data++)
                hash = ((hash << 5) + hash) + c; // hash * 33 + c
            return hash;
        }
    };
};
#endif

class apindex
{
    std::string dbname;
    std::string path;
    int attr_num;
    char part_name[MAX_PART_NAME_LEN];
    leveldb::DB* db;
    static DMMutex  fork_lock;

    leveldb::Status status;
    leveldb::WriteBatch *pbatch;
    long pbatch_mem_size;//内存大小

#ifdef  STRING_KEY
//typedef mm::cache_map <std::string,std::string> BATCH_MAP;
//typedef dense_hash_map <std::string,std::string> BATCH_MAP;

    typedef std::unordered_map<std::string,std::string> BATCH_MAP;
#else
// BUG:lost key differ
    typedef std::map<leveldb::Slice,std::string> BATCH_MAP;
//typedef std::unordered_map<leveldb::Slice,std::string> BATCH_MAP;
#endif
    BATCH_MAP *p_batch_map;

    int batch_jobct;
    int dupwt;
    int savedpack;
    class DBPool
    {
        std::map<std::string,DBEntry > pool;
        DMMutex  pool_lock;
        leveldb::Status status;
    public :
        DBPool()
        {
        }
        ~DBPool()
        {
            AutoLock lock(&pool_lock);
            std::map<std::string,DBEntry >::iterator iter=pool.begin();
            while(iter!=pool.end()) {
                iter->second.Release();
                pool.erase(iter++);
            }
        }

        leveldb::Options *GetOptions(std::string &dbname)
        {
            AutoLock lock(&pool_lock);
            std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
            if(iter==pool.end()) return NULL;
            return iter->second.options;
        }

        void Retrieve(std::string &dbname,apindex *mdb,bool bfmode)
        {
            std::map<std::string,DBEntry >::iterator iter;
            {
                AutoLock lock(&pool_lock);
                iter=pool.find(dbname);
            }
            if(iter==pool.end() || iter->second.db==NULL) {
                DBEntry dbe;

                dbe.bfmode=bfmode;
                dbe.CreateOption();
                // leveldb 不能打开2次(不同进程也不可以)
                status = leveldb::DB::Open(*dbe.options,dbname, &dbe.db);
                if(!status.ok()) {
                    throw "Open leveldb failed.";
                }
                dbe.refct=1;
                dbe._refs.push_back(mdb);
                AutoLock lock(&pool_lock);
                if(iter==pool.end())
                    pool.insert(std::pair<std::string,DBEntry>(dbname,dbe));
                else
                    iter->second=dbe;
                mdb->SetDB(dbe.db);
            } else {
                AutoLock lock(&pool_lock);
                iter->second._refs.push_back(mdb);
                iter->second.refct++;
                mdb->SetDB(iter->second.db);
            }
        }

        void Release(std::string &dbname,apindex *mdb)
        {
            AutoLock lock(&pool_lock);
            std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
            //assert(iter!=pool.end());
            // RCTableImpl::LoadDirtyData release RCAttr cause this
            if(iter==pool.end()) return;
            // remove reference list element
            for(int i=0; i<iter->second._refs.size(); i++) {
                if(iter->second._refs[i]==mdb) {
                    mdb->SetDB(NULL);
                    iter->second._refs.erase(iter->second._refs.begin()+i);
                }
            }
            iter->second.refct--;
            if(iter->second.refct==0) {
                iter->second.Release();
                pool.erase(iter);
            }
        }

        //force remove from pool
        int RemoveDB(std::string &dbname,std::vector<apindex *> &refs)
        {
            AutoLock lock(&pool_lock);
            std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);

            if(iter==pool.end()) {
                throw "leveldb pool find failed.";
            }

            int oldrefct=iter->second.refct;
            refs=iter->second._refs;
            //delete db maybe waiting on write data complete,store to a temporary var ,release after delete db
            iter->second.Release();
            for(int i=0; i<iter->second._refs.size(); i++) {
                iter->second._refs[i]->SetDB(NULL);
            }
            pool.erase(iter);
            return oldrefct;
        }

        void SetRef(int refct,std::string &dbname,std::vector<apindex *> &refs)
        {
            AutoLock lock(&pool_lock);
            std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
            if(iter==pool.end()) {
                throw "leveldb pool find failed.";
            }
            iter->second.refct=refct;
            iter->second._refs=refs;
            //update refs
            for(int i=0; i<iter->second._refs.size(); i++) {
                iter->second._refs[i]->SetDB(iter->second.db);
            }
        }

    };

    static apindex::DBPool ap_pool;
// adding k/v only store new value of the key!
//追加模式  仅仅记录新增的packs ,做Add操作时不包含原有的packs
// 要求同一个pack不能重复做add！
    bool appendmode;
    bool writemode;
    bool bfmode; //blooming filter mode
// set containers are generally slower than unordered_set containers to access individual elements by their key, but they allow the direct iteration on subsets based on their order.
//Sets are typically implemented as binary search trees.
//std::map<int,unsigned int> keyinapack;


protected:
    unsigned char file_ver;        //ATTRINDEX_VERSION_1,ATTRINDEX_VERSION_2

    //>> begin: fix dma-1319: key使用后缀合成
    bool  is_suffix_key;     //表示是key是否含有后缀[_1,_2,_3,_4]
    short suffix_max_val;    //表示是key的后缀的最大值是多少,例如：suffix_max_val  = 10 ,表示：key进入的查询范围是[key_1,key_2,...,key_9,key_10],每一个分区都是从1开始
    _int64 mem_size;          //表示包索引当前大小,每超过4G大小就切换key_n中对应的n,每一个分区都是独立的计算的
    //<< end: fix dma-1319

public :
    inline char* get_part_name() { return part_name;}

    inline void SetFileVer(unsigned char _fv)
    {
        file_ver =  _fv;
    }
    inline void SetIsSuffixKey(const bool _v)
    {
        is_suffix_key = _v;
    }
    inline void SetSuffixMaxVal(const short _v)
    {
        suffix_max_val = _v;
    }
    inline void SetMemSize(const _int64 _v)
    {
        mem_size = _v;
    }

    inline short GetSuffixMaxVal()
    {
        return suffix_max_val;
    }
    inline _int64 GetMemSize()
    {
        return mem_size ;
    }
    inline bool GetIsSuffixKey()
    {
        return is_suffix_key;
    }

public:

    void SetDB(leveldb::DB *pdb)
    {
        db=pdb;
    }

    apindex()
    {
        pbatch=NULL;
        p_batch_map = NULL;
        pbatch_mem_size = 0;
        Init();
        bfmode=true;
        file_ver=ATTRINDEX_VERSION_2;
        is_suffix_key = true;
        suffix_max_val = 0;
        mem_size = 0;
    }

    apindex(std::string _dbname,std::string _basepath,bool _bfmode=true)
    {
        pbatch=NULL;
        p_batch_map = NULL;
        pbatch_mem_size = 0;
        Init();
        bfmode=_bfmode;
        dbname=_dbname;
        file_ver=ATTRINDEX_VERSION_2;
        is_suffix_key = true;
        suffix_max_val = 0;
        mem_size = 0;
        if(_basepath.length()==0)
            path=_dbname;
        else path=_basepath+"/"+_dbname;
        Open();
    }

    bool isempty()
    {
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        bool retval=it->Valid();
        delete it;
        return !retval;
    }

    apindex(const apindex &ap)
    {
        pbatch=NULL;
        p_batch_map = NULL;
        pbatch_mem_size = 0;
        Init();
        bfmode=ap.bfmode;
        attr_num=ap.attr_num;
        writemode=ap.writemode;
        strcpy(part_name,ap.part_name);
        dbname=ap.dbname;//.assign(fn,strlen(fn));
        path=ap.path;
        file_ver = ap.file_ver;
        is_suffix_key = ap.is_suffix_key;
        suffix_max_val = ap.suffix_max_val;
        mem_size = ap.mem_size;

        Open();
    }

    apindex(int attrnum,int partnum,std::string _basepath,bool _writemode,bool _bfmode=true)
    {
        pbatch=NULL;
        p_batch_map = NULL;
        pbatch_mem_size = 0;
        Init();
        bfmode=_bfmode;
        attr_num=attrnum;
        file_ver=ATTRINDEX_VERSION_2;
        writemode=_writemode;
        sprintf(part_name,"%05d",partnum);
        char fn[300];
        sprintf(fn,"%d_%s%s",attrnum,part_name,writemode?"_w":"");
        dbname=fn;//.assign(fn,strlen(fn));
        path=_basepath+dbname;
        Open();
    }

    static std::string GetDBPath(int attrnum,const char * partname,std::string _basepath,bool _writemode)
    {
        std::string dbpath;
        char fn[300];
        sprintf(fn,"%d_%s%s",attrnum,partname,_writemode?"_w":"");
        dbpath=_basepath+fn;
        return dbpath;
    }

    std::string GetPath()
    {
        return path;
    }

    apindex(int attrnum,const char * partname,std::string _basepath,bool _writemode,bool _bfmode=true)
    {
        pbatch=NULL;
        p_batch_map = NULL;
        pbatch_mem_size = 0;
        Init();
        bfmode=_bfmode;
        attr_num=attrnum;
        strcpy(part_name,partname);
        writemode=_writemode;
        file_ver=ATTRINDEX_VERSION_2;
        char fn[300];
        sprintf(fn,"%d_%s%s",attrnum,part_name,writemode?"_w":"");
        dbname=fn;//.assign(fn,strlen(fn));
        path=_basepath+dbname;
        Open();
    }
    void Init()
    {
        if(pbatch) delete pbatch;
        pbatch=new leveldb::WriteBatch();

        if(p_batch_map) delete p_batch_map;
        p_batch_map = new BATCH_MAP;
        pbatch_mem_size = 0;
        dupwt=0;
        attr_num=-1;
        savedpack=-1;
        //default false for append mode
        appendmode=false;
        p_batch_map->clear();
    }
    bool SetSavePack(int n)
    {
        if(savedpack>=n) return false;
        savedpack=n;
        return true;
    }

    void SetAppendMode(bool _v)
    {
        appendmode=_v;
    }

    void RemovePath(const std::string path)
    {
        std::string cmd="/bin/sh -c -- 'rm -rf ";
        cmd+=path;
        cmd+="'";
        system(cmd.c_str());
    }

    void ClearDB(bool reopen=true)
    {
        std::vector<apindex *> refs;
        int refct=ap_pool.RemoveDB( path,refs);
        RemovePath(path);
        if(reopen) {
            Open();
            ap_pool.SetRef(refct,  path,refs);
        } else db=NULL;
    }

    static int BLCopyFile(const char* source, const char* destination)
    {
        //Here we use kernel-space copying for performance reasons

        int input, output;
        struct stat filestat;
        stat(source, &filestat);
        if( (input = open(source, O_RDONLY)) == -1)
            return 0;

        if( (output = open(destination, O_WRONLY | O_CREAT,S_IRUSR|S_IWUSR|S_IRGRP)) == -1) {
            close(input);
            return 0;
        }

        off_t bytesCopied=0;

        int result = sendfile(output, input, &bytesCopied, filestat.st_size) == -1;

        close(input);
        close(output);

        return result;
    }

// remove directory
    static void RemoveDir(const char *srcpath)
    {
        DIR * d;
        d = opendir (srcpath);
        if(!d) return;
        while(1) {
            struct dirent * entry;
            const char * d_name;
            /* "Readdir" gets subsequent entries from "d". */
            entry = readdir (d);
            if (! entry) {
                /* There are no more entries in this directory, so break
                    out of the while loop. */
                break;
            }
            d_name = entry->d_name;
            int path_length;
            char path[PATH_MAX];
            path_length = snprintf (path, PATH_MAX,
                                    "%s/%s", srcpath, d_name);
            if (entry->d_type & DT_DIR) {
                /* Check that the directory is not "d" or d's parent. */
                if (strcmp (d_name, "..") != 0 &&
                    strcmp (d_name, ".") != 0) {
                    /* Recursively call "list_dir" with the new path. */
                    RemoveDir (path);
                }
            } else
                unlink(path);
        }
        rmdir(srcpath);
    }

// link sst && copy all others files
    static void CopyDBFiles(const char *srcpath,const char *dstpath)
    {
        DIR * d;
        d = opendir (dstpath);
        if(!d)
            mkdir(dstpath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        else closedir(d);
        d = opendir (srcpath);
        //srcpath maybe a empty db(no directory)
        if(!d) return;
        while(1) {
            struct dirent * entry;
            const char * d_name;
            /* "Readdir" gets subsequent entries from "d". */
            entry = readdir (d);
            if (! entry) {
                /* There are no more entries in this directory, so break
                    out of the while loop. */
                break;
            }
            d_name = entry->d_name;
            int path_length;
            char path[PATH_MAX];
            path_length = snprintf (path, PATH_MAX,
                                    "%s/%s", srcpath, d_name);
            char newpath[PATH_MAX];
            snprintf(newpath,PATH_MAX,"%s/%s",dstpath,d_name);
            if (entry->d_type & DT_DIR) {
                /* Check that the directory is not "d" or d's parent. */
                if (strcmp (d_name, "..") != 0 &&
                    strcmp (d_name, ".") != 0) {
                    /* Recursively call "list_dir" with the new path. */
                    CopyDBFiles (path,newpath);
                }
            } else {
                // leveldb 1.10 版本的数据文件*.sst
                // leveldb 1.15 版本的数据文件*.ldb
                if(strlen(d_name)>4 &&
                   (strcmp(d_name+strlen(d_name)-4,".sst")==0 ||
                    strcmp(d_name+strlen(d_name)-4,".ldb")==0 )) {
                    // create hard link
                    link(path,newpath);
                } else {
                    BLCopyFile(path,newpath);
                }
            }
        }
        closedir(d);
    }

// Fork a new db with common data (sst file)
    static void ForkDB(const char *db,const char *newdb)
    {
        //Reload to merge log file
        //ReloadDB();
        // close db to prepare fork
        //std::vector<apindex *> refs;
        //int refct=ap_pool.RemoveDB( path,refs);
        AutoLock al(&fork_lock);
        RemoveDir(newdb);
        CopyDBFiles(db,newdb);
        //Open();
        //ap_pool.SetRef(refct,  path,refs);
    }

    void ReplaceDB(const char *replacebypath)
    {
        std::vector<apindex *> refs;
        int refct=ap_pool.RemoveDB( path,refs);
        RemovePath(path);
        rename(replacebypath,path.c_str());
        Open();
        ap_pool.SetRef(refct,  path,refs);
    }

    void UpdateBWmemSize(const leveldb::Slice& key,const leveldb::Slice& value)
    {
        pbatch_mem_size += key.size();
        pbatch_mem_size += value.size();
    }

    void UpdatePackIndexSize(const int key_size,const int value_size,const bool bltsession = false) // fix dma-1319
    {
        // fix dma-1319
        if(is_suffix_key) {
            mem_size += key_size;
            mem_size += value_size;

            if(!bltsession) { // 分布式排序入库短会话,及原AP装入流程 的时候才起作用

                double _switch_item = (double)mem_size/SWITCH_MAX_SUFFIX_MEM_SIZE;
                
                if( _switch_item > suffix_max_val ) { // 切换后缀值
                    suffix_max_val += 1;
                }
            }

            // 分布式排序入库长会话只合成生成一个key及:key---> key_1
        }
    }

    bool ShouldWriteBW()
    {
        return pbatch_mem_size>=MAX_WRITE_BATCH_MEM_SIZE;
    }

    long GetMapSize()
    {
        return p_batch_map==NULL?0:p_batch_map->size();
    }

    void ReloadDB()
    {
        std::vector<apindex *> refs;
        int refct=ap_pool.RemoveDB( path,refs);
        Open();
        ap_pool.SetRef(refct,  path,refs);
    }

    void Open()
    {
        ap_pool.Retrieve( path ,this,bfmode);
        BatchBegin();
    }

    virtual ~apindex()
    {
        BatchWrite();
        ap_pool.Release(path,this);
        delete pbatch;
        delete p_batch_map;
    }
// deprecated
    void PackBegin()
    {
        //keyinapack.clear();
    }

    void ScanDB(int &rownum,int &itemsnum)
    {
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        rownum=0;
        itemsnum=0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const int *pval=(const int *)((const char *)it->value().data());
            // adjust packs offset to absolute packs offset
            int size=it->value().size()/sizeof(int);
            rownum++;
            itemsnum+=size;
        }
        delete it;
    }
// Dump db to a text file ,a line for a record(k/v)
// type: 1,int; 2,long ;3,double ;4,varchar
    void DumpDB(const char *filename,int keytype)
    {
        FILE *fp=fopen(filename,"w+t");
        assert(fp);
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const int *pval=(const int *)((const char *)it->value().data());
            // adjust packs offset to absolute packs offset
            int size=it->value().size()/sizeof(int);
            //const char *key=it->key().data();
            std::string key(it->key().data(),it->key().size());
            const int *pvalend=pval+size;
            char kstring[1000];
            if(keytype==1) {
                _int64 int_key = CR_Class_NS::str2i64(key);
                sprintf(kstring,"%d",int_key);
            } else if(keytype==2) {
                _int64 long_key = CR_Class_NS::str2i64(key);
                sprintf(kstring,"%ld",long_key);
            } else if(keytype==3) {
                // TODO
                sprintf(kstring,"%f",*((const double *)key.data()));
            } else if(keytype==4) {
                memcpy(kstring,key.data(),it->key().size());
                kstring[it->key().size()]=0;
            } else assert(0);
            fprintf(fp,"key %s ,value ",kstring);
            while(pval<pvalend) {
                int pks=*pval;
                fprintf(fp," %d",pks);
                pval++;
            }
            fputs("\n",fp);
        }
        fclose(fp);
        delete it;
    }

//merge apindex,keep exists items!
//  在原有数据已经有KEY时:
// appendmode: 原有记录的packs与新增记录的packs作排重
//  否则，新增记录直接追加到原有的packs
// 排重(appendmode==false):假定需要合并的数据和HashMap在相同的key上仅有最多最后一个包重复

    void Import(apindex &apidx)
    {
        leveldb::Iterator* it = apidx.db->NewIterator(leveldb::ReadOptions());
        BatchBegin();
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            if(!appendmode) {
                // for a specified key ,adding new value
                std::string value;
                // find old packs for this key
                status = db->Get(leveldb::ReadOptions(), it->key(), &value);
                if(status.ok()) {
                    const int *pv=(const int *)it->value().data();
                    int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
                    if(*pv<lastkey) {
                        throw "new pack value number less than last loaded pack.";
                    }
                    //first of iter->second equals last of value ?
                    bool rep=(*pv==lastkey);
                    value.append((const char *)(rep?(pv+1):pv),it->value().size()-(rep?sizeof(int):0));
                    UpdateBWmemSize(it->key(),leveldb::Slice(value));
                    pbatch->Put(it->key(),leveldb::Slice(value));
                } else {
                    UpdateBWmemSize(it->key(),it->value());
                    pbatch->Put(it->key(),it->value());     // add to batch writer
                }
            } else {
                // appendmode :直接把内容追加到原有的记录上
                UpdateBWmemSize(it->key(),it->value());
                pbatch->Put(it->key(),it->value());
            }
            batch_jobct++;

            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
                batch_jobct=0;
            }

        }
        assert(it->status().ok());  // Check for any errors found during the scan
        delete it;
        BatchWrite();
    }

// call MergeSrcDbtoMap to prepare merged hashmap before this one
//   separator merge and write and reduce lock time
//  batch_map will be cleared after import to db
    void ImportHash(apindex &apidx)
    {
        BatchBegin();
        std::string value;
        BATCH_MAP::iterator iter;
        for(iter = apidx.p_batch_map->begin(); iter!=apidx.p_batch_map->end(); iter++) {
            leveldb::Slice keyslice(iter->first);
            pbatch->Put(keyslice, leveldb::Slice(iter->second));
            UpdateBWmemSize(keyslice, leveldb::Slice(iter->second));
            batch_jobct++;

            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
            }
        }
        BatchWrite();
        apidx.p_batch_map->clear();
        delete apidx.p_batch_map;
        apidx.p_batch_map = new BATCH_MAP;
    }
    /* deprecated!
     // key type must same in src/new
     // merge srcdb and newdb to this object
     //  overlaped values on duplicated keys will be checked ,but only remove at most one elements
     // TODO: packs duplicate on all element,change to check only edge one
     void Merge(apindex &srcdb,apindex &newdb) {
        ClearDB();
        leveldb::Iterator* it = srcdb.db->NewIterator(leveldb::ReadOptions());
        leveldb::Iterator* itnew = newdb.db->NewIterator(leveldb::ReadOptions());
        BatchBegin();
        std::string newvalue;
        std::string value;
        itnew->SeekToFirst();
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            // for a specified key ,adding new value
            // find key in newdb
            while(itnew->Valid() && itnew->key().compare(it->key())<0) {
                pbatch->Put(itnew->key(),itnew->value());
                itnew->Next();
            }

            if(itnew->Valid() && itnew->key().compare(it->key())==0) {
                // same key in both db,merge them
                std::set<int> packs;
                value.assign(it->value().data(),it->value().size());
                const int *pval=(const int *)((const char *)value.data());
                int size=value.size()/sizeof(int);
                const int *pvalend=pval+size;
                while(pval<pvalend) {
                    packs.insert(*pval++);
                }
                // check exists of new packs and update pack set
                //pval=(const int *)newvalue.data();
                pval=(const int *)itnew->value().data();
                //size=newvalue.size()/sizeof(int);
                size=itnew->value().size()/sizeof(int);
                pvalend=pval+size;
                while(pval<pvalend) {
                   if(packs.count(*pval)==0) {
                    int pks=*pval;
                    packs.insert(pks);
                    value.append((const char *)&pks,sizeof(int));
                   }
                   pval++;
                }
                pbatch->Put(it->key(),leveldb::Slice(value.data(),value.size()));
                itnew->Next();
            }
            else
                pbatch->Put(it->key(),it->value());
            batch_jobct++;
            if(batch_jobct>=MAX_JOBS) {
                BatchWrite();
                batch_jobct=0;
            }
        }
        for (;itnew->Valid(); itnew->Next()) {
            pbatch->Put(itnew->key(),itnew->value());
            batch_jobct++;
            if(batch_jobct>=MAX_JOBS) {
               BatchWrite();
               batch_jobct=0;
            }
        }
        assert(it->status().ok());  // Check for any errors found during the scan
        delete it;
        delete itnew;
        BatchWrite();
     }
    */
// key type must same in src/new
// merge srcdb and newdb to this object
//  overlaped values on duplicated keys will be checked ,but only remove at most one elements
// clear newdb after merge.
    void MergeFromHashMap(apindex &srcdb,apindex &newdb)
    {
        ClearDB();
        leveldb::ReadOptions opts;
        opts.fill_cache=false;
        leveldb::Iterator* it = srcdb.db->NewIterator(opts);
        BATCH_MAP::iterator itnew;
        // newdb is static during this oper
        BatchBegin();
        std::string newkey;
        std::string newvalue;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            // for a specified key ,adding new value
            // find key in newdb
            newkey.assign(it->key().data(),it->key().size());
            itnew=newdb.p_batch_map->find(newkey);
            if(itnew!=newdb.p_batch_map->end()) {
                const int *pv=(const int *)itnew->second.data();
                int lastkey=*(const int *)(it->value().data()+it->value().size()-sizeof(int));
                if(*pv<lastkey) {
                    throw "new pack value number less than last loaded pack.";
                }
                //first of iter->second equals last of value ?
                bool rep=*pv==lastkey;
                newvalue.assign(it->value().data(),it->value().size());
                newvalue.append((const char *)(rep?(pv+1):pv),itnew->second.size()-(rep?sizeof(int):0));
                pbatch->Put(it->key(), leveldb::Slice(newvalue));
                UpdateBWmemSize(it->key(), leveldb::Slice(newvalue));
                //nvalue=leveldb::Slice(itnew->second);
                //pbatch->Put(it->key(),nvalue);
                newdb.p_batch_map->erase(itnew);
            } else {
                UpdateBWmemSize(it->key(),it->value());
                pbatch->Put(it->key(),it->value());
            }

            batch_jobct++;
            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
                batch_jobct=0;
            }
        }
        for (itnew=newdb.p_batch_map->begin(); itnew!=newdb.p_batch_map->end(); itnew++) {
            pbatch->Put(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
            UpdateBWmemSize(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
            batch_jobct++;
            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
                batch_jobct=0;
            }
        }

        assert(it->status().ok());  // Check for any errors found during the scan
        delete it;
        BatchWrite();
        newdb.p_batch_map->clear();
    }

	void ReplaceFromHashMap(apindex &newdb)
    {
        ClearDB();
        BATCH_MAP::iterator itnew;
        // newdb is static during this oper
        BatchBegin(); 
        for (itnew=newdb.p_batch_map->begin(); itnew!=newdb.p_batch_map->end(); itnew++) {
            pbatch->Put(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
            UpdateBWmemSize(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
            batch_jobct++;
            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
                batch_jobct=0;
            }
        }
        BatchWrite();
        newdb.p_batch_map->clear();
    }

    const char *MergeIndexPath(char *name)
    {
        return GetMergeIndexPath(path.c_str(),name);
    }

    static const char *GetMergeIndexPath(const char *spath,char *name)
    {
        sprintf(name,"%s_mrg",spath);
        return name;
    }
    static const char *GetWriteIndexPath(const char *spath,char *name)
    {
        sprintf(name,"%s_w",spath);
        return name;
    }

    void RebuildByTruncate(int *packvars,int packvsize,apindex &srcdb)
    {
        ClearDB();
        leveldb::ReadOptions opts;
        opts.fill_cache=false;
        leveldb::Iterator* it = srcdb.db->NewIterator(opts);
        BatchBegin();
        std::string _value;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            _value.assign(it->value().data(),it->value().size());
            //int *pv=(int *)(it->value().data());
            int *pv=(int*)(_value.c_str());
            // int psize=it->value().size()/sizeof(int);
            int psize=_value.size()/sizeof(int);
            int *pvend=pv+psize;
            while(pv<pvend) {
                // try to fix dma-1223,modify by liujs
                // packvars 的顺序是逆序的,大的在前,小的在后
                // e.g: {1836, 153, 918, 153, 153, 153}
                // 如果pv为919则需要减去153,在减去153,只能从大的先减去
                int *packvars_ref = packvars;
                int *pvarlast=packvars_ref+packvsize*2-2;

                while(packvars_ref<=pvarlast) {
                    if(*pv>=*packvars_ref) {
                        *pv-=packvars_ref[1];
                    }
                    packvars_ref+=2;
                }
                pv++;
            }
            //pbatch->Put(it->key(),it->value());
            pbatch->Put(it->key(),leveldb::Slice(_value.data(),_value.size()));
            UpdateBWmemSize(it->key(),leveldb::Slice(_value.data(),_value.size()));
            batch_jobct++;
            if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
                BatchWrite();
                batch_jobct=0;
            }
        }
        assert(it->status().ok());  // Check for any errors found during the scan
        delete it;
        BatchWrite();
    }
// merge srcdb to batch_map ,将原数据库的值合并到内存中来
// 排重:假定需要合并的数据和HashMap在相同的key上仅有最多最后一个包重复
    void MergeSrcDbtoMap(apindex &srcdb)
    {
        std::string value;
        BATCH_MAP::iterator iter;
        leveldb::ReadOptions opts;
        opts.fill_cache=false;
        for(iter = p_batch_map->begin(); iter!=p_batch_map->end(); iter++) {
#ifdef STRING_KEY
            leveldb::Slice keyslice(iter->first);
#else
            leveldb::Slice keyslice(iter->frist);
#endif
            // 到数据库中进行查找，找到就进行合并操作，将数据库中合并入内存
            status = srcdb.db->Get(opts, keyslice, &value);
            std::string  &update_value = iter->second;

            if(status.ok()) { // 原库中值的获取key对应的pack
                const int *pv=(const int *)iter->second.data();
                int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
                if(*pv<lastkey) {
                    throw "new pack value number less than last loaded pack.";
                }
                //first of iter->second equals last of value ?
                bool rep=*pv==lastkey;
                value.append((const char *)(rep?(pv+1):pv),iter->second.size()-(rep?sizeof(int):0));
                (*p_batch_map)[iter->first]=value;
            } // end if(status.ok())

        } // end for(iter = p_batch_map->begin();iter!=p_batch_map->end();iter++)
    }


    void BatchBegin()
    {
        //pbatch->Clear();
        delete pbatch;
        pbatch=new leveldb::WriteBatch();
        batch_jobct=0;
        pbatch_mem_size = 0;
    }

    void ClearMap()
    {
        //p_batch_map->clear();
        delete p_batch_map;
        p_batch_map = new BATCH_MAP;
    }

    leveldb::WriteBatch &GetBatch()
    {
        return *pbatch;
    }

// Dot forget to clear batch_jobct in owner of _batch !
    void BatchWrite(leveldb::WriteBatch &_batch)
    {
        status = db->Write(leveldb::WriteOptions(), &_batch);
        assert(status.ok());
        _batch.Clear();
        pbatch_mem_size = 0;
    }

    void BatchWrite()
    {
        //>> begin:fix dma-705
        if(batch_jobct<1 ) {
            BatchBegin();
            ClearMap();
            return;
        }
        BATCH_MAP::iterator iter;
        for(iter = p_batch_map->begin(); iter != p_batch_map->end(); iter++) {
            pbatch->Put(iter->first,
                        leveldb::Slice(iter->second));
        }
        ClearMap();
        //p_batch_map->clear();
        //<< end: fix dma-705

        //skip write to watch performance
        status = db->Write(leveldb::WriteOptions(), pbatch);
        pbatch_mem_size = 0;
        assert(status.ok());
        BatchBegin();
    }

// file name for hash map dump
    std::string GetHashFile()
    {
        std::string hashfile=path+"/"+"hashmap.bin";
        return hashfile;
    }

// write p_batch_map to a binary file.
// if p_batch_map is empty,remove binary file
    void WriteMap()
    {
#ifdef PITEST_USE   // 测试工具pitest
        FILE *fp=fopen(GetHashFile().c_str(),"w+b");
        assert(fp!=NULL);
        if(batch_jobct>0) {
            int elements=p_batch_map->size();
            fwrite(&elements,sizeof(int),1,fp);
            BATCH_MAP::iterator iter;
            for(iter = p_batch_map->begin(); iter != p_batch_map->end(); iter++) {
                int keylen = iter->first.size();
                int valuelen = iter->second.size();
                fwrite(&keylen,4,1,fp);
                fwrite(iter->first.data(),keylen,1,fp);
                fwrite(&valuelen,4,1,fp);
                fwrite(iter->second.data(),valuelen,1,fp);
            }
        }
        p_batch_map->clear();
        delete p_batch_map;
        p_batch_map = new BATCH_MAP;
        fclose(fp);
#else
        IBFile fp;
        long _write_byte = 0;
        fp.OpenCreateEmpty(GetHashFile());
        if(batch_jobct>0) {
            int elements=p_batch_map->size();
            fp.WriteExact(&elements,sizeof(int));
            _write_byte+=sizeof(int);

            BATCH_MAP::iterator iter;
            for(iter = p_batch_map->begin(); iter != p_batch_map->end(); iter++) {
                int keylen = iter->first.size();
                int valuelen = iter->second.size();

                fp.WriteExact(&keylen,sizeof(keylen));
                _write_byte+=sizeof(int);

                fp.WriteExact(iter->first.data(),keylen);
                _write_byte+=keylen;

                fp.WriteExact(&valuelen,sizeof(valuelen));
                _write_byte+=sizeof(int);

                fp.WriteExact(iter->second.data(),valuelen);
                _write_byte+=valuelen;
            }
        }
        rclog << lock << "Debug info : Apindex WriteMap :"<<GetHashFile()<<" , write file size ["<< _write_byte << "] bytes" <<unlock;

        p_batch_map->clear();
        delete p_batch_map;
        p_batch_map = new BATCH_MAP;

        fp.Close();

#endif
    }

// load from map file -> mergesrc ->write db
    int CommitMap(apindex &srcdb)
    {
#ifdef PITEST_USE  // 测试工具pitest用
        srcdb.BatchBegin();
        FILE *fp=fopen(GetHashFile().c_str(),"rb");
        assert(fp!=NULL);
        int elements=0;
        fread(&elements,sizeof(int),1,fp);
        int mxkeylen=300;
        int mxvaluelen=500;
        char *keybuf=(char *)malloc(mxkeylen);
        leveldb::ReadOptions opts;
        opts.fill_cache=false;
        char *valuebuf=(char *)malloc(mxvaluelen);
        std::string value;
        for(int i=0; i<elements; i++) {
            int keylen=0,valuelen=0;
            fread(&keylen,sizeof(int),1,fp);
            if(keylen>mxkeylen) {
                mxkeylen=(keylen*1.1);
                keybuf=(char *)realloc(keybuf,mxkeylen);
            }
            fread(keybuf,keylen,1,fp);
            fread(&valuelen,sizeof(int),1,fp);
            if(valuelen>mxvaluelen) {
                mxvaluelen=(valuelen*1.1);
                valuebuf=(char *)realloc(valuebuf,mxvaluelen);
            }
            fread(valuebuf,valuelen,1,fp);
            //std::string value(valuebuf,valuelen);
            leveldb::Slice keyslice(keybuf,keylen);
            // 到数据库中进行查找，找到就进行合并操作，将数据库中合并入内存
            status = srcdb.db->Get(opts, keyslice, &value);
            //std::string  &update_value = iter->second;

            if(status.ok()) { // 原库中值的获取key对应的pack
                const int *pv=(const int *)valuebuf;
                int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
                if(*pv<lastkey) {
                    throw "new pack value number less than last loaded pack.";
                }
                //first of iter->second equals last of value ?
                bool rep=*pv==lastkey;
                value.append((const char *)(rep?(pv+1):pv),valuelen-(rep?sizeof(int):0));
                srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(value));
                srcdb.pbatch->Put(keyslice, leveldb::Slice(value));
            } else {
                srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(valuebuf,valuelen));
                srcdb.pbatch->Put(keyslice, leveldb::Slice(valuebuf,valuelen));
            }
            srcdb.batch_jobct++;
            if(srcdb.batch_jobct>=MAX_JOBS || srcdb.ShouldWriteBW()) {
                srcdb.BatchWrite();
            }
        }
        srcdb.BatchWrite();
        free(keybuf);
        free(valuebuf);
        fclose(fp);

#else
        srcdb.BatchBegin();
        IBFile fp;
        fp.OpenReadOnly(GetHashFile());
        long _read_size = 0;
        int elements=0;

        fp.ReadExact(&elements,sizeof(int));
        _read_size += sizeof(int);

        int mxkeylen=300;
        int mxvaluelen=500;
        char *keybuf=(char *)malloc(mxkeylen);
        leveldb::ReadOptions opts;
        opts.fill_cache=false;
        char *valuebuf=(char *)malloc(mxvaluelen);
        std::string value;
        for(int i=0; i<elements; i++) {
            int keylen=0,valuelen=0;

            fp.ReadExact(&keylen,sizeof(int));
            _read_size+=sizeof(int);

            if(keylen>mxkeylen) {
                mxkeylen=(keylen*1.1);
                keybuf=(char *)realloc(keybuf,mxkeylen);
            }

            fp.ReadExact(keybuf,keylen);
            _read_size+=keylen;

            fp.ReadExact(&valuelen,sizeof(int));
            _read_size+=sizeof(int);

            if(valuelen>mxvaluelen) {
                mxvaluelen=(valuelen*1.1);
                valuebuf=(char *)realloc(valuebuf,mxvaluelen);
            }

            fp.ReadExact(valuebuf,valuelen);
            _read_size+=valuelen;

            //std::string value(valuebuf,valuelen);
            leveldb::Slice keyslice(keybuf,keylen);
            // 到数据库中进行查找，找到就进行合并操作，将数据库中合并入内存
            status = srcdb.db->Get(opts, keyslice, &value);
            //std::string  &update_value = iter->second;

            if(status.ok()) { // 原库中值的获取key对应的pack
                const int *pv=(const int *)valuebuf;
                int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
                if(*pv<lastkey) {
                    throw "new pack value number less than last loaded pack.";
                }
                //first of iter->second equals last of value ?
                bool rep=*pv==lastkey;
                value.append((const char *)(rep?(pv+1):pv),valuelen-(rep?sizeof(int):0));
                srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(value));
                srcdb.pbatch->Put(keyslice, leveldb::Slice(value));
            } else {
                srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(valuebuf,valuelen));
                srcdb.pbatch->Put(keyslice, leveldb::Slice(valuebuf,valuelen));
            }
            srcdb.batch_jobct++;
            if(srcdb.batch_jobct>=MAX_JOBS || srcdb.ShouldWriteBW()) {
                srcdb.BatchWrite();
            }
        }
        rclog << lock << "Debug info : Apindex CommitMap :"<<GetHashFile()<<" , read file size ["<< _read_size << "] bytes" <<unlock;

        srcdb.BatchWrite();
        free(keybuf);
        free(valuebuf);

        fp.Close();

#endif
    }

    int LoadMap()
    {

#ifdef PITEST_USE  // 测试工具pitest用
        FILE *fp=fopen(GetHashFile().c_str(),"rb");
        assert(fp!=NULL);
        int elements=0;
        fread(&elements,sizeof(int),1,fp);
        int mxkeylen=300;
        int mxvaluelen=500;
        char *keybuf=(char *)malloc(mxkeylen);
        char *valuebuf=(char *)malloc(mxvaluelen);
        p_batch_map->clear();
        for(int i=0; i<elements; i++) {
            int keylen=0,valuelen=0;
            fread(&keylen,sizeof(int),1,fp);
            if(keylen>mxkeylen) {
                mxkeylen=(keylen*1.1);
                keybuf=(char *)realloc(keybuf,mxkeylen);
            }
            fread(keybuf,keylen,1,fp);
            fread(&valuelen,sizeof(int),1,fp);
            if(valuelen>mxvaluelen) {
                mxvaluelen=(valuelen*1.1);
                valuebuf=(char *)realloc(valuebuf,mxvaluelen);
            }
            fread(valuebuf,valuelen,1,fp);
            std::string key(keybuf,keylen);
            std::string value(valuebuf,valuelen);
            (*p_batch_map)[key]=value;
        }
        free(keybuf);
        free(valuebuf);
        fclose(fp);

#else

        IBFile fp;
        fp.OpenReadOnly(GetHashFile());
        long _read_size = 0;
        int elements=0;

        fp.ReadExact(&elements,sizeof(int));
        _read_size += sizeof(int);

        int mxkeylen=300;
        int mxvaluelen=500;
        char *keybuf=(char *)malloc(mxkeylen);
        char *valuebuf=(char *)malloc(mxvaluelen);
        p_batch_map->clear();
        for(int i=0; i<elements; i++) {
            int keylen=0,valuelen=0;

            fp.ReadExact(&keylen,sizeof(int));
            _read_size += sizeof(int);

            if(keylen>mxkeylen) {
                mxkeylen=(keylen*1.1);
                keybuf=(char *)realloc(keybuf,mxkeylen);
            }
            fp.ReadExact(keybuf,keylen);
            _read_size += keylen;

            fp.ReadExact(&valuelen,sizeof(int));
            _read_size += sizeof(int);

            if(valuelen>mxvaluelen) {
                mxvaluelen=(valuelen*1.1);
                valuebuf=(char *)realloc(valuebuf,mxvaluelen);
            }

            fp.ReadExact(valuebuf,valuelen);
            _read_size += valuelen;

            std::string key(keybuf,keylen);
            std::string value(valuebuf,valuelen);
            (*p_batch_map)[key]=value;
        }
        free(keybuf);
        free(valuebuf);

        rclog << lock << "Debug info : Apindex LoadMap :"<<GetHashFile()<<" , read file size ["<< _read_size << "] bytes" <<unlock;

        fp.Close();

#endif
    }

    bool Get(int key,std::vector<int>  &packs)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);

            if(is_suffix_key) { // fix dma-1319
                bool exist = false;
                for(int i=1; i<=suffix_max_val; i++) {
                    char suffix[10];
                    sprintf(suffix,"_%d",i);

                    std::string fix_key =  _key + std::string(suffix);

                    leveldb::Slice keyslice(fix_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }
                return exist;
            } else {
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);
            }

        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(int));
            return Get(keyslice,packs);
        }
    }

    bool Get(long key,std::vector<int>  &packs)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);

            if(is_suffix_key) { // fix dma-1319
                bool exist = false;
                for(int i=1; i<=suffix_max_val; i++) {
                    char suffix[10];
                    sprintf(suffix,"_%d",i);

                    std::string fix_key =  _key + std::string(suffix);

                    leveldb::Slice keyslice(fix_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }

                return exist;
            } else {
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);
            }
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(long));
            return Get(keyslice,packs);
        }
    }

    void Compact()
    {
        db->CompactRange(NULL, NULL);
    }

    bool Get(_int64 key,std::vector<int>  &packs)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);

            if(is_suffix_key) { // fix dma-1319
                bool exist = false;
                for(int i=1; i<=suffix_max_val; i++) {
                    char suffix[10];
                    sprintf(suffix,"_%d",i);

                    std::string fix_key =  _key + std::string(suffix);

                    leveldb::Slice keyslice(fix_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }
                return exist;
            } else {
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);
            }
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(_int64));
            return Get(keyslice,packs);
        }
    }

    bool Get(const char *key,const uint key_len,std::vector<int>  &packs)
    {
        if(is_suffix_key) { // fix dma-1319
            bool exist = false;
            for(int i=1; i<=suffix_max_val; i++) {
                char suffix[10];
                sprintf(suffix,"_%d",i);

                std::string fix_key =  std::string(key,key_len) + std::string(suffix);

                leveldb::Slice keyslice(fix_key);
                if(Get(keyslice,packs)) {
                    exist = true;
                }
            }

            return exist;
        } else {
            leveldb::Slice keyslice(key,strlen(key));
            return Get(keyslice,packs);
        }
    }

    bool Get(leveldb::Slice &key,std::vector<int> &packs)
    {
        std::string value;
#ifdef STRING_KEY
        std::string keys(key.data(),key.size());
#endif
        bool exist = false;
        //>> begin: fix dma-705
        BATCH_MAP::iterator iter;
#ifdef STRING_KEY
        iter = p_batch_map->find(keys);
#else
        iter = p_batch_map->find(key);
#endif
        if(iter != p_batch_map->end()) {
#ifdef STRING_KEY
            value = (*p_batch_map)[keys];
#else
            value = (*p_batch_map)[key];
#endif
            const int *pval=(const int *)((const char *)value.data());
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                packs.push_back(*pval++);
            }
            exist = true;
        }
        //<< end: fix dma-705

        //packs.clear();
        status = db->Get(leveldb::ReadOptions(), key, &value);
        if(status.ok()) {
            const int *pval=(const int *)((const char *)value.data());
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                // already exists
                packs.push_back(*pval++);
            }
            exist = true;
        }
        return exist;
    }

// range query including key & limit
    bool GetRange(int key,int limit,std::vector<int>  &packs)
    {
        bool exist = false;
        if(file_ver == ATTRINDEX_VERSION_2) {
            for (; key<=limit; key++) {
                std::string _key = CR_Class_NS::i642str(key);

                if(is_suffix_key) { // fix dma-1319

                    for(int i=1; i<=suffix_max_val; i++) {
                        char suffix[10];
                        sprintf(suffix,"_%d",i);

                        std::string fix_key =  _key + std::string(suffix);

                        leveldb::Slice keyslice(fix_key);
                        if(Get(keyslice,packs)) {
                            exist = true;
                        }
                    }

                } else {
                    leveldb::Slice keyslice(_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }
            }
        } else {
            for (; key<=limit; key++) {
                leveldb::Slice keyslice((const char *)&key,sizeof(int));
                if(Get(keyslice, packs)) {
                    exist = true;
                }
            }
        }
        return exist;
    }

    bool GetRange(long key,long limit,std::vector<int>  &packs)
    {
        bool exist = false;
        if(file_ver == ATTRINDEX_VERSION_2) {
            for (; key<=limit; key++) {
                std::string _key = CR_Class_NS::i642str(key);

                if(is_suffix_key) { // fix dma-1319

                    for(int i=1; i<=suffix_max_val; i++) {
                        char suffix[10];
                        sprintf(suffix,"_%d",i);

                        std::string fix_key =  _key + std::string(suffix);

                        leveldb::Slice keyslice(fix_key);
                        if(Get(keyslice,packs)) {
                            exist = true;
                        }
                    }

                } else {
                    leveldb::Slice keyslice(_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }
            }
        } else {
            for (; key<=limit; key++) {
                leveldb::Slice keyslice((const char *)&key,sizeof(long));
                if(Get(keyslice, packs)) {
                    exist = true;
                }
            }
        }
        return exist;
    }

    bool GetRange(_int64 key,_int64 limit,std::vector<int>  &packs)
    {
        bool exist = false;

        if(file_ver == ATTRINDEX_VERSION_2) {
            for (; key<=limit; key++) {
                std::string _key = CR_Class_NS::i642str(key);

                if(is_suffix_key) { // fix dma-1319

                    for(int i=1; i<=suffix_max_val; i++) {
                        char suffix[10];
                        sprintf(suffix,"_%d",i);

                        std::string fix_key =  _key + std::string(suffix);

                        leveldb::Slice keyslice(fix_key);
                        if(Get(keyslice,packs)) {
                            exist = true;
                        }
                    }

                } else {
                    leveldb::Slice keyslice(_key);
                    if(Get(keyslice,packs)) {
                        exist = true;
                    }
                }
            }
        } else {
            for (; key<=limit; key++) {
                leveldb::Slice keyslice((const char *)&key,sizeof(_int64));
                if(Get(keyslice, packs)) {
                    exist = true;
                }
            }
        }

        return true;
    }

#ifdef PITEST_USE  // pitest 工具使用的    
    bool Get(int64_t key,int packn)
    {
        std::string _key = CR_Class_NS::i642str(key);
        leveldb::Slice keyslice(_key);
        return Get(keyslice,packn);
    }

    bool Get(const char *key,int packn)
    {
        leveldb::Slice keyslice(key,strlen(key));
        return Get(keyslice,packn);
    }

    bool Get(leveldb::Slice &key,int packn)
    {
        std::string value;

        //>> begin: fix dma-704
        BATCH_MAP::iterator iter;
#ifdef STRING_KEY
        std::string keys(key.data(),key.size());
        iter = p_batch_map->find(keys);
#else
        iter = p_batch_map->find(key);
#endif
        if(iter != p_batch_map->end()) {
#ifdef STRING_KEY
            value = (*p_batch_map)[keys];
#else
            value = (*p_batch_map)[key];
#endif
            const int *pval=(const int *)((const char *)value.data());
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                if(*pval++==packn) {
                    return true;
                }
            }
        }
        //<< end: fix dma-704

        status = db->Get(leveldb::ReadOptions(), key, &value);
        if(status.ok()) {
            const int *pval=(const int *)((const char *)value.data());
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                // already exists
                if(*pval++==packn) {
                    return true;
                }
            }
        }
        return false;
    }
#endif

    bool GetRange(const char *key,const uint key_len,const char *limit,const uint limit_len,std::vector<int>  &packs)
    {
        if(is_suffix_key) { // fix dma-1319

            char suffix[10];

            // 合成start key
            sprintf(suffix,"_%d",1);
            std::string fix_key(key,key_len);
            fix_key += std::string(suffix);

            // 合成end key
            sprintf(suffix,"_%d",suffix_max_val);
            std::string fix_limit(limit,limit_len);
            fix_limit += std::string(suffix);

            // 查询
            leveldb::Slice keyslice(fix_key),keylimit(fix_limit);
            return GetRange(keyslice,keylimit,packs);

        } else {
            leveldb::Slice keyslice(key,strlen(key)),keylimit(limit,strlen(limit));
            return GetRange(keyslice,keylimit,packs);
        }
    }

    bool GetRange(leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs)
    {
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(key); it->Valid() && it->key().compare(limit)<=0; it->Next()) {
            leveldb::Slice value=it->value();
            const int * pval=(const int *)value.data();
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                // already exists
                packs.push_back(*pval++);
            }
        }
        assert(it->status().ok());  // Check for any errors found during the scan
        delete it;
        return true;
    }

    void Put(int key,int packn,const bool bltsession = false)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);
            if(is_suffix_key) { // fix dma-1319
                char suffix[10];
                sprintf(suffix,"_%d",suffix_max_val);
                _key = _key + std::string(suffix);
            }
            leveldb::Slice keyslice(_key);
            Put(keyslice,packn,bltsession);
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(int));
            Put(keyslice,packn,bltsession);
        }
    }

    void Put(long key,int packn,const bool bltsession = false)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);
            if(is_suffix_key) { // fix dma-1319
                char suffix[10];
                sprintf(suffix,"_%d",suffix_max_val);
                _key = _key + std::string(suffix);
            }
            leveldb::Slice keyslice(_key);
            Put(keyslice,packn,bltsession);
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(long));
            Put(keyslice,packn,bltsession);
        }
    }

    void Put(_int64 key,int packn,const bool bltsession = false)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);
            if(is_suffix_key) { // fix dma-1319
                char suffix[10];
                sprintf(suffix,"_%d",suffix_max_val);
                _key = _key + std::string(suffix);
            }
            leveldb::Slice keyslice(_key);
            Put(keyslice,packn,bltsession);
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(_int64));
            Put(keyslice,packn,bltsession);
        }
    }

    void Put(const char *key,int packn,const bool bltsession = false)
    {
        std::string _key = std::string(key);
        if(is_suffix_key) { // fix dma-1319
            char suffix[10];
            sprintf(suffix,"_%d",suffix_max_val);
            _key = _key + std::string(suffix);
        }
        leveldb::Slice keyslice(_key);
        Put(keyslice,packn,bltsession);
    }

    unsigned int BPHash(const char* str, unsigned int len)
    {
        unsigned int hash = 0;
        unsigned int i    = 0;
        for(i = 0; i < len; str++, i++) {
            hash = hash << 7 ^ (*str);
        }

        return hash;
    }

    void Put(leveldb::Slice &key,int packn,const bool bltsession = false)
    {
#ifdef STRING_KEY
        std::string keys(key.data(),key.size());
#endif
        /*
            int hash=   leveldb::Hash(key.data(), key.size(), 0);
            unsigned int v=BPHash(key.data(), key.size());
            if(keyinapack.count(hash)!=0) {
                if(keyinapack[hash]==v){
                    return;  // 2次hash结果一致，可以认为该值已经存在
                }
            }
            else{
                keyinapack[hash]=v;
            }
        */
        //>> fix dma-705 先到p_batch_map中查找,找到就不在leveldb中进行查找，起到缓存的作用,这个地方不能使用leveldb::writebatch
        std::string newvalue((const char*)&packn,sizeof(int));
        BATCH_MAP::iterator iter;

#ifdef STRING_KEY
        iter = p_batch_map->find(keys);
#else
        iter = p_batch_map->find(key);
#endif

        if(iter != p_batch_map->end()) { // 先到内存map中进行查找
            std::string &value = iter->second;
            const int *pval=(const int *)((const char *)value.data());
            int size=value.size()/sizeof(int);
            const int *pvalend=pval+size;
            while(pval<pvalend) {
                if(*pval++==packn) {// already exists
                    dupwt++;
                    return;
                }
            }
            value.append((const char*)&packn,sizeof(int));

            UpdatePackIndexSize(0,sizeof(int),bltsession); // fix dma-1319

            return;  //  std::string &value 已经修改值，可以返回
        }
        /*
        else{ // 内存中没有找到，到数据库中找
            //std::string value;
            status = db->Get(leveldb::ReadOptions(), key, &newvalue);
            if(status.ok()) {
                // append value to key item
                const int *pval=(const int *)((const char *)newvalue.c_str());
                int size=newvalue.size()/sizeof(int);
                const int *pvalend=pval+size;
                while(pval<pvalend) {
                    if(*pval++==packn) {
                        dupwt++;
                        return; // already exists
                    }
                }
                newvalue.append((const char*)&packn,sizeof(int));
            }
            else
                assert(newvalue.size()==sizeof(int));
        }
        */
#ifdef STRING_KEY
        (*p_batch_map)[keys] = newvalue;
#else
        (*p_batch_map)[key] = newvalue;
#endif

        UpdatePackIndexSize(key.size(),sizeof(int),bltsession); // fix dma-1319

        batch_jobct++;

        /* // fix dma-707
        if(batch_jobct >= MAX_JOBS){
            BatchWrite();
        }
        */
    }

    // FIX DMA-1319 : 数值类型的包索引设置: strpackn : 为pack的组合[pack-i][pack-j]....[pack-m][packk]
    void Put(const _int64 key,const std::string  strpackn,const bool bltsession = false)
    {
        if(file_ver == ATTRINDEX_VERSION_2) {
            std::string _key = CR_Class_NS::i642str(key);
            if(is_suffix_key) {
                char suffix[10];
                sprintf(suffix,"_%d",suffix_max_val);
                _key = _key + std::string(suffix);
            }
            leveldb::Slice keyslice(_key);
            Put(keyslice,strpackn,bltsession);
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(_int64));
            Put(keyslice,strpackn,bltsession);
        }
    }

    // FIX DMA-1319 : 字符串类型的包索引设置: strpackn : 为pack的组合[pack-i][pack-j]....[pack-m][packk]
    void Put(const char *key,const std::string  strpackn,const bool bltsession = false)
    {
        std::string _key(key);
        if(is_suffix_key) {
            char suffix[10];
            sprintf(suffix,"_%d",suffix_max_val);
            _key = _key + std::string(suffix);
        }
        leveldb::Slice keyslice(_key);
        Put(keyslice,strpackn,bltsession);
    }

    // FIX DMA-1319 : 包索引数据的合并生成: strpackn : 为pack的组合[pack-i][pack-j]....[pack-m][packk]
    void Put(leveldb::Slice &key,const std::string  strpackn,const bool bltsession = false)
    {
#ifdef STRING_KEY
        std::string keys(key.data(),key.size());
#endif

        BATCH_MAP::iterator iter;
#ifdef STRING_KEY
        iter = p_batch_map->find(keys);
#else
        iter = p_batch_map->find(key);
#endif

        if(iter != p_batch_map->end()) { // 需要合并数据包的PACK编号,短会话的包索引数据会生成
            // 已经存储的
            std::string &old_value = iter->second;
            const int *pold_val=(const int *)((const char *)old_value.data());
            const int old_size=old_value.size()/sizeof(int);

            // strpackn 的包号是排序的
            const int *pnew_val=(const int *)((const char*)strpackn.data());
            int new_size = strpackn.size()/sizeof(int);

            assert(pold_val[old_size-1] <= pnew_val[0]); // 上一次的最后一个包号必须<=下一次的第一个包号


            if(pold_val[old_size-1] == pnew_val[0]) {
                old_value.append((const char*)(pnew_val+1),(new_size-1)*sizeof(int));

                UpdatePackIndexSize(0,(new_size-1)*sizeof(int),bltsession); // fix dma-1319
            } else {
                old_value.append((const char*)pnew_val,new_size*sizeof(int));

                UpdatePackIndexSize(0,new_size*sizeof(int),bltsession); // fix dma-1319
            }

            return ;

        } else {

#ifdef STRING_KEY
            (*p_batch_map)[keys] = strpackn;
#else
            (*p_batch_map)[key] = strpackn;
#endif
            batch_jobct++;

            UpdatePackIndexSize(key.size(),strpackn.size(),bltsession); // fix dma-1319

        }

        /*
        if(batch_jobct >= MAX_JOBS*10){
            BatchWrite();
        }
        */
    }

};

#endif

