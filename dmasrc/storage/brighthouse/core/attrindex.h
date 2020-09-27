#ifndef ATTRINDEX_HEADER
#define ATTRINDEX_HEADER
#include <system/IBFile.h>
#define INDEX_READY  0
#define INDEX_BUILD  1
#define INDEX_DELETE 2
#include "apindex.h"
#include "partinfo.h"
#include "DMThreadGroup.h"

class AttrIndex
{
protected:
    DMMutex mutex;
    int attrnum;
    bool ismerged;//query on a list of apindex or a merged apindex
    bool isvalid;
    bool islistvalid;
    bool is_suffix_key;// fix dma-1319,是否支持key的后缀[key-->key_1,key_2,key_3,key_4]
    //std::vector<apindex> index_list;
    // merged index:
    apindex *index_attr;
    std::string basepath;
    std::string headfile;
    std::string ltsession_headfile_sufixx()
    {
    	return truncate_headfile_sufixx();
        return std::string(".ltsession");
    }
    std::string truncate_headfile_sufixx()
    {
        return std::string(".ntrunc");
    }
    short max_partseq;
    unsigned char file_ver;
    struct apindexinfo {
        char partname[MAX_PART_NAME_LEN];
        short action;// 0,none; 1,wait commit 2,wait delete
        int sessionid;
        unsigned char file_ver;

        //>> begin: fix dma-1319: key使用后缀合成
        bool  is_suffix_key;     //表示是key是否含有后缀[_1,_2,_3,_4]
        short suffix_max_val;    //表示是key的后缀的最大值是多少,例如：suffix_max_val  = 10 ,表示：key进入的查询范围是[key_1,key_2,...,key_9,key_10],每一个分区都是从1开始
        _int64 mem_size;         //表示包索引当前大小,每超过4G大小就切换key_n中对应的n,每一个分区都是独立的计算的
        apindex *index;

        void update_fuffixInfo_to_apindex()
        {
            if(this->index != NULL) {
                if(this->is_suffix_key) {
                    this->index->SetIsSuffixKey(is_suffix_key);
                    this->index->SetSuffixMaxVal(suffix_max_val);
                    this->index->SetMemSize(mem_size);
                }
            }
        }
        void update_fuffixInfo_to_apindex(apindex *_index)
        {
            if(_index != NULL) {
                if(this->is_suffix_key) {
                    _index->SetIsSuffixKey(is_suffix_key);
                    _index->SetSuffixMaxVal(suffix_max_val);
                    _index->SetMemSize(mem_size);
                }
            }
        }
        void update_fuffixInfo_from_apindex()
        {
            if(index != NULL) {
                if(index->GetIsSuffixKey() && index->GetSuffixMaxVal() >=1) {
                    is_suffix_key = index->GetIsSuffixKey();
                    suffix_max_val = index->GetSuffixMaxVal();
                    mem_size = index->GetMemSize();
                }
            }
        }
        //<< end: fix dma-1319

        apindexinfo():action(0),sessionid(0),file_ver(ATTRINDEX_VERSION_2),
            is_suffix_key(true),suffix_max_val(1),mem_size(0),index(NULL)
        {
            partname[0]=0;
        }
        apindexinfo(short pnum,short _action,int sid,unsigned char fv,
                    const bool isk,const short smv,const _int64 ms):
            action(_action),sessionid(sid),file_ver(fv),
            is_suffix_key(isk),suffix_max_val(smv),mem_size(ms),index(NULL)
        {
            sprintf(partname,"%05d",pnum);
        }
        apindexinfo(const char * pname,short _action,int sid,unsigned char fv,
                    const bool isk,const short smv,const _int64 ms):
            action(_action),sessionid(sid),file_ver(fv),
            is_suffix_key(isk),suffix_max_val(smv),mem_size(ms),index(NULL)
        {
            strcpy(partname,pname);
        }
        apindexinfo(const apindexinfo &ap)
        {
            action=ap.action;
            strcpy(partname,ap.partname);
            sessionid=ap.sessionid;
            file_ver = ap.file_ver;
            is_suffix_key = ap.is_suffix_key;
            mem_size = ap.mem_size;
            suffix_max_val= ap.suffix_max_val;
            if(ap.index==NULL) {
                index=NULL;
            } else {
                index=new apindex(*ap.index);
                index->SetFileVer(file_ver);
            }
        }

        apindexinfo& operator = ( const apindexinfo& ap )
        {
            action=ap.action;
            strcpy(partname,ap.partname);
            sessionid=ap.sessionid;
            file_ver = ap.file_ver;
            is_suffix_key = ap.is_suffix_key;
            mem_size = ap.mem_size;
            suffix_max_val= ap.suffix_max_val;
            if(ap.index==NULL) {
                index=NULL;
            } else {
                //fix dma-691 temp db (write) not released!
                index=new apindex(*ap.index);
                index->SetFileVer(file_ver);
            }
            return *this;
        }

        virtual ~apindexinfo()
        {
            if(index!=NULL) {
                delete index;
                index=NULL;
            }
        }
    };
    // global index info for temporary index data
    // key: tabid+columnid+sessionid
    static std::map<std::string,apindexinfo *> g_index;// static global index info
    std::vector<apindexinfo *> index_info;
public:
    // mysqld 退出的时候,释放leveldb对象,RCEngine::~RCEngine() 函数中调用
    static void release_leveldb_index()
    {
        std::map<std::string,apindexinfo *>::iterator iter;
        for(iter = g_index.begin(); iter!=g_index.end(); iter++) {
            apindexinfo* pidx = (apindexinfo*)iter->second;
            if(pidx != NULL) {
                if(pidx->index != NULL) {
                    delete pidx->index;
                    pidx->index = NULL;
                }
                delete pidx;
                pidx = NULL;
            }
        }
        g_index.clear();
    }
    //Get global index info key
    std::string GetGKey(int tabid,int sid,const char *partname)
    {
        char ckey[300]; // tabid_colunum_sid_partname
        sprintf(ckey,"%d_%d_%d_%s",tabid,attrnum,sid,partname);
        return std::string(ckey);
    }

    //Move temporary apindex(build data in hashmap) to global map
    void MoveToGlobal(int tabid,int sid,const char *partname)
    {
        AutoLock lock(&mutex);
        for(int i=0; i<index_info.size(); i++) {
            if(strcmp(index_info[i]->partname,partname)==0 && index_info[i]->sessionid==sid) {
                g_index[GetGKey(tabid,sid,partname)] = index_info[i];
                index_info.erase(index_info.begin()+i);
            }
        }
    }

    //clear global index info item
    void RemoveGlobal(int tabid,int sid,const char *partname)
    {
        AutoLock lock(&mutex);
        if(g_index[GetGKey(tabid,sid,partname)] != NULL) {
            if(g_index[GetGKey(tabid,sid,partname)]-> index != NULL) {
                delete g_index[GetGKey(tabid,sid,partname)]->index;
                g_index[GetGKey(tabid,sid,partname)]->index = NULL;
            }
            delete g_index[GetGKey(tabid,sid,partname)];
            g_index[GetGKey(tabid,sid,partname)] = NULL;
        }
        g_index.erase(GetGKey(tabid,sid,partname));
    }

    apindex *GetGlobal(int tabid,int sid,const char *partname)
    {
        return g_index[GetGKey(tabid,sid,partname)]->index;
    }

    AttrIndex(int _attrnum,const char *_attrpath):index_info()
    {
        basepath=_attrpath;
        attrnum=_attrnum;
        char fn[300];
        sprintf(fn,"/attrindex_%d.ctb",attrnum);
        headfile=_attrpath;
        headfile+=fn;
        init();
        index_attr=NULL;
    }

    virtual ~AttrIndex()
    {
        init();
        if(index_attr!=NULL) {
            delete index_attr;
            index_attr= NULL;
        }

        for(int i=0; i<index_info.size(); i++) {
            if(index_info[i]!=NULL) {
                if(index_info[i]->index !=NULL) {
                    delete index_info[i]->index ;
                    index_info[i]->index = NULL;
                }
                delete index_info[i];
                index_info[i] = NULL;
            }
        }
        index_info.clear();
    }

    void init (const char *except_part = NULL)
    {
        max_partseq=0;
        ismerged=false;
        isvalid=false;
        islistvalid=false;
        file_ver=ATTRINDEX_VERSION_2;
        is_suffix_key = true;  // fix dma-1319
        for(int i=0; i<index_info.size(); i++) {

            if(index_info[i]!=NULL ) {

                // 防止长会话的_w库被清理了
                if( except_part== NULL || (except_part != NULL && strcasecmp(index_info[i]->partname,except_part) != 0)) {

                    if(index_info[i]->index !=NULL) {
                        delete index_info[i]->index ;
                        index_info[i]->index = NULL;
                    }
                    delete index_info[i];
                    index_info[i] = NULL;
                }
            }
        }

        // index_info.clear();
        // 防止长会话的_w库被清理了
        for(std::vector<apindexinfo *>::iterator iter=index_info.begin(); iter != index_info.end();) {
            if(*iter == NULL) {
                iter = index_info.erase(iter);
            } else {
                ++iter;
            }
        }
    }

    bool Valid()
    {
        return isvalid;
    }

    bool SetValid(bool v)
    {
        isvalid=v;
    }

    bool LoadHeader(int from_truncate_part = 0,const char *except_part=NULL)
    {
        // load header info :
        init(except_part);
        if(!DoesFileExist(headfile+(from_truncate_part?truncate_headfile_sufixx():""))) {
            return false;
        }
        IBFile idxhfile;
        idxhfile.OpenReadOnly(headfile+(from_truncate_part?truncate_headfile_sufixx():""));
        idxhfile.ReadExact(&file_ver,1);
        //assert(file_ver==ATTRINDEX_VERSION_1);

        unsigned char flags;
        idxhfile.ReadExact(&flags,1);
        isvalid=flags&0x01;
        islistvalid=flags&0x02;
        ismerged=flags&0x04;
        is_suffix_key = flags&0x08;

        int datalen=0;
        max_partseq=0;
        idxhfile.ReadExact(&datalen,sizeof(int));

        // at least 2 values
        if(datalen<8) { // real length [4bytes: sizeof(int)][4bytes:length]
            return false;
        }
        char *tmp=new char[datalen];
        assert(tmp);
        idxhfile.ReadExact(tmp,datalen);
        char *plist=tmp;
        char *pend=tmp+datalen;
        char partname[300];

        // elements:
        // <short>partnamelen <string>PartName <short>action(status) <int>sessionid
        // key 支持后缀的情况下,文件结构如下
        // is_suffix_key : <short>partnamelen <string>PartName <short>action(status) <int>sessionid  <short>suffix_max_val  <_int64> mem_size
        while(plist<pend) {
            int pnamelen=*(short *)(plist);
            max_partseq++;
            plist+=sizeof(short);
            memcpy(partname,plist,pnamelen);
            partname[pnamelen]=0;
            plist+=pnamelen;

            apindexinfo* pobj = NULL;
            if(is_suffix_key) { // fix dma-1319


                // 长会话_w库没有必要被覆盖
                if(except_part != NULL && strcasecmp(partname,except_part) == 0) {
                    plist += sizeof(short)+sizeof(int)+sizeof(short)+sizeof(_int64);
                    continue;
                }

                // <short>action(status) <int>sessionid  <short>suffix_max_val  <_int64> mem_size
                pobj = new apindexinfo(partname,
                                       *(short *)(plist),                                          // <short>action(status)
                                       *(int *)(plist+sizeof(short)),                              // <int>sessionid
                                       file_ver,
                                       is_suffix_key,
                                       *(short *)(plist+sizeof(short)+sizeof(int)),                // <short>suffix_max_val
                                       *(_int64 *)(plist+sizeof(short)+sizeof(int)+sizeof(short))); // <int64> mem_size

                // <short>action(status) <int>sessionid  <short>suffix_max_val  <int64> mem_size
                plist += sizeof(short)+sizeof(int)+sizeof(short)+sizeof(_int64);

            } else {

                // 长会话_w库没有必要被覆盖
                if(except_part != NULL && strcasecmp(partname,except_part) == 0) {

                    plist+=sizeof(short)+sizeof(int);
                    continue;
                }


                // <short>action(status) <int>sessionid
                pobj = new apindexinfo(partname,
                                       *(short *)(plist),                  // <short>action(status)
                                       *(int *)(plist+sizeof(short)),      // <int>sessionid
                                       file_ver,is_suffix_key,1,0);

                plist+=sizeof(short)+sizeof(int);
            }
            pobj->update_fuffixInfo_to_apindex();

            index_info.push_back(pobj);
        }
        delete []tmp;
        return true;
    }
    // delete a partition index :
    //  1 . load header info
    //  2 . lock table for write
    //  3 . delete index table
    //  4 . save header info
    //  5 . unlock table
    //  6 . reload header
    // add a partition index :
    //  1. load header info
    //  2. create new partition with sessionid(transaction id)-- set has not be commited flag
    //  3. create index table
    //  4. save uncommit header info (waitting for server process to commit)
    //  5. commit by server
    //     5.1 lock table for write
    //     5.2 release index online
    //     5.3 reload header info
    //     5.4 find partnum to be committed
    //     5.5 change flags to commited
    //     5.6 save header info
    //     5.7 unlock table
    //     5.8 reload header

    //  make sure this object has correct value to save ,as the header file maybe deleted or rebuild in this method.
    // if truncate_tab ,then save file to xxx.ntrunc
    bool SaveHeader(bool truncate_tab=false,const char *truncpart=NULL)
    {
        if(index_info.size()<1) {
            RemoveFile(headfile);
            return false;
        }
        IBFile idxhfile;
        long _write_size = 0;
        idxhfile.OpenCreateEmpty(headfile+(truncate_tab?truncate_headfile_sufixx():""));
        idxhfile.WriteExact(&file_ver,1);

        _write_size+=1;

        unsigned char flags=0;
        flags|=isvalid?0x01:0;
        flags|=islistvalid?0x02:0;
        flags|=ismerged?0x04:0;
        flags|=is_suffix_key?0x08:0;

        idxhfile.WriteExact(&flags,1);
        _write_size+=1;

        // elements:
        // is_suffix_key == false :<short>partnamelen <string>PartName <short>action(status) <int>sessionid
        // is_suffix_key == true  : <short>partnamelen <string>PartName <short>action(status) <int>sessionid  <short>suffix_max_val  <int64> mem_size

        int datalen=0;
        if(!is_suffix_key) {
            int _tmp = sizeof(short) + sizeof(short)+sizeof(int);
            datalen = index_info.size()*(_tmp+MAX_PART_NAME_LEN);
        } else {
            int _tmp = sizeof(short) + sizeof(short)+sizeof(int) + sizeof(short) + sizeof(_int64);
            datalen = index_info.size()*(_tmp+MAX_PART_NAME_LEN);
        }

        char *tmp=new char[datalen];
        // at least 2 values
        //assert(datalen>=8 && datalen % 8==0); // 2 short ,2 int

        char *plist=tmp;
        for(int i=0; i<index_info.size(); i++) {

            if(truncate_tab && strcmp(index_info[i]->partname,truncpart)==0)
                continue;

            // elements:
            // is_suffix_key == false :<short>partnamelen <string>PartName <short>action(status) <int>sessionid
            // is_suffix_key == true  : <short>partnamelen <string>PartName <short>action(status) <int>sessionid  <short>suffix_max_val  <int64> mem_size

            *(short *)(plist)=strlen(index_info[i]->partname);
            plist+=sizeof(short);

            memcpy(plist,index_info[i]->partname,strlen(index_info[i]->partname));
            plist+=strlen(index_info[i]->partname);

            *(short *)(plist)=index_info[i]->action;
            plist+=sizeof(short);

            *(int *)(plist)=index_info[i]->sessionid;
            plist+=sizeof(int);

            if(is_suffix_key) {

                index_info[i]->update_fuffixInfo_from_apindex();

                *(short *)(plist)=index_info[i]->suffix_max_val;
                plist+=sizeof(short);

                *(_int64 *)(plist)=index_info[i]->mem_size;
                plist+=sizeof(_int64);
            }

            // if(truncate_tab) continue;
            // db data updated at maximum batch jobs or here :
            //if(index_info[i]->action==INDEX_BUILD && index_info[i]->index!=NULL)
            //  index_info[i]->index->BatchWrite();
        }

        // real length
        datalen=plist-tmp;
        idxhfile.WriteExact(&datalen,sizeof(int));
        _write_size+=sizeof(int);

        idxhfile.WriteExact(tmp,datalen);
        _write_size+=datalen;

        delete []tmp;
        tmp = NULL;

        std::string _log_fn = headfile+(truncate_tab?truncate_headfile_sufixx():"");
        rclog << lock << "Debug info : Apindex SaveHeader :"<<_log_fn<<" , write file size ["<< _write_size << "] bytes" <<unlock;

        return true;
    }

    bool SaveHeader_ltsession()   // 长会话上保存包索引的时候用
    {
        if(index_info.size()<1) {
            RemoveFile(headfile+ltsession_headfile_sufixx());
            return false;
        }
        IBFile idxhfile;
        long _write_size = 0;
        idxhfile.OpenCreateEmpty(headfile+ltsession_headfile_sufixx());
        idxhfile.WriteExact(&file_ver,1);

        _write_size+=1;

        unsigned char flags=0;
        flags|=isvalid?0x01:0;
        flags|=islistvalid?0x02:0;
        flags|=ismerged?0x04:0;
        flags|=is_suffix_key?0x08:0;

        idxhfile.WriteExact(&flags,1);
        _write_size+=1;

        // elements:
        // is_suffix_key == false :<short>partnamelen <string>PartName <short>action(status) <int>sessionid
        // is_suffix_key == true  : <short>partnamelen <string>PartName <short>action(status) <int>sessionid  <short>suffix_max_val  <int64> mem_size

        int datalen=0;
        if(!is_suffix_key) {
            int _tmp = sizeof(short) + sizeof(short)+sizeof(int);
            datalen = index_info.size()*(_tmp+MAX_PART_NAME_LEN);
        } else {
            int _tmp = sizeof(short) + sizeof(short)+sizeof(int) + sizeof(short) + sizeof(_int64);
            datalen = index_info.size()*(_tmp+MAX_PART_NAME_LEN);
        }

        char *tmp=new char[datalen];
        // at least 2 values
        //assert(datalen>=8 && datalen % 8==0); // 2 short ,2 int

        char *plist=tmp;
        for(int i=0; i<index_info.size(); i++) {

            // elements:
            // is_suffix_key == false :<short>partnamelen <string>PartName <short>action(status) <int>sessionid
            // is_suffix_key == true  : <short>partnamelen <string>PartName <short>action(status) <int>sessionid  <short>suffix_max_val  <int64> mem_size

            *(short *)(plist)=strlen(index_info[i]->partname);
            plist+=sizeof(short);

            memcpy(plist,index_info[i]->partname,strlen(index_info[i]->partname));
            plist+=strlen(index_info[i]->partname);

            *(short *)(plist)=index_info[i]->action;
            plist+=sizeof(short);

            *(int *)(plist)=index_info[i]->sessionid;
            plist+=sizeof(int);

            if(is_suffix_key) {

                index_info[i]->update_fuffixInfo_from_apindex();

                *(short *)(plist)=index_info[i]->suffix_max_val;
                plist+=sizeof(short);

                *(_int64 *)(plist)=index_info[i]->mem_size;
                plist+=sizeof(_int64);
            }
        }

        // real length
        datalen=plist-tmp;
        idxhfile.WriteExact(&datalen,sizeof(int));
        _write_size+=sizeof(int);

        idxhfile.WriteExact(tmp,datalen);
        _write_size+=datalen;

        delete []tmp;
        tmp = NULL;

        std::string _log_fn = headfile+ltsession_headfile_sufixx();
        rclog << lock << "Debug info : Apindex SaveHeader_ltsession :"<<_log_fn<<" , write file size ["<< _write_size << "] bytes" <<unlock;

        return true;
    }


    bool RollbackTruncate(const char *partname)
    {
        if(!DoesFileExist(headfile+truncate_headfile_sufixx())) return false;
        RemoveFile( headfile+truncate_headfile_sufixx());
        //LoadHeader();
        //LoadForRead();
        return true;
    }

    bool RebuildByTruncate(int *packvars,int packvsize,std::set<std::string> rebuildparts,int sid)
    {
        std::set<std::string>::iterator iter=rebuildparts.begin();
        while(iter!=rebuildparts.end()) {
            apindex *db=GetReadIndex(iter->c_str());
            char mergedbname[300];
            apindex mergeidx(db->MergeIndexPath(mergedbname),"");
            assert(db);
            mergeidx.RebuildByTruncate(packvars,packvsize,*db);
            iter++;
        }
    }

    bool CommitTruncate(const char *partname)
    {
        if(!DoesFileExist(headfile+truncate_headfile_sufixx())) return false;
        std::string idxpath=GetReadIndex(partname)->GetPath();
        RemovePart(partname);
        DeleteDirectory( idxpath);
        DeleteDirectory( idxpath+"_w");
        RemoveFile( headfile);
        RenameFile(headfile+truncate_headfile_sufixx(), headfile);
        LoadHeader();
        LoadForRead();
        for(int i=0; i<index_info.size(); i++) {
            if(index_info[i]->action==INDEX_READY) {
                char mergename[300];
                if(DoesFileExist(index_info[i]->index->MergeIndexPath(mergename))) {
                    index_info[i]->index->ReplaceDB(mergename);
                    index_info[i]->index->ReloadDB();
                }
            }
        }
        return true;
    }

    bool CommitTruncate_ltsession(const char *partname)
    {
        if(!DoesFileExist(headfile+ltsession_headfile_sufixx())) return false;

        RemoveFile(headfile);
        RenameFile(headfile+ltsession_headfile_sufixx(), headfile);

        int from_truncate_part = 0;
        const char* pExceptPart = partname;
        LoadHeader(from_truncate_part,pExceptPart);

        LoadForRead(pExceptPart);

        for(int i=0; i<index_info.size(); i++) {

            if(strcasecmp(partname,index_info[i]->partname) !=0) {
                // 其他分区,直接重命名: db_mrg--->db
                char mergename[300];
                if(DoesFileExist(index_info[i]->index->MergeIndexPath(mergename))) {
                    index_info[i]->index->ReplaceDB(mergename);
                    index_info[i]->index->ReloadDB();
                }
            }
        }

        return true;
    }

    //call LoadHeader before this
    void LoadForRead(const char* except_part=NULL)
    {
        if(!isvalid || index_info.size()<1)
            return;

        for(int i=0; i<index_info.size(); i++) {
            // open committed partition only. skip  crashed none commit partitions
            /* a partition maybe exists both uncommited and commited data */
            if(index_info[i]->action==INDEX_READY && index_info[i]->index==NULL) {

                // 长会话分区的_w库跳过,add by liujs
                if(except_part != NULL && strcasecmp(index_info[i]->partname,except_part) == 0) {
                    continue;
                }

                t_init_index_info(i,false);

                if(index_info[i]->index->isempty()) {
                    delete index_info[i]->index;
                    index_info[i]->index=NULL;
                } else isvalid=true;
            }
        }

    }

    apindex *   GetMergedIndex()
    {
        assert(index_attr!=NULL);
        return index_attr;
    }

    apindex *GetIndex(short partnum,int sessionid)
    {
        char pname[MAX_PART_NAME_LEN];
        sprintf(pname,"%05d",partnum);
        return GetIndex(pname,sessionid);
    }

    apindex *GetIndex(const char *pname,int sessionid)
    {
        for(int i=0; i<index_info.size(); i++) {
            // GetIndex By both readonly Table RCAttr(PackIndexMerge) and RCAttrLoad(Write temp index table)
            if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
                return index_info[i]->index;
            }
        }
        throw "GetIndex got null";
    }

    // 如果没有对于分区(会话)的索引，则新建
    //
    apindexinfo *LoadForUpdate(short partnum,int sessionid)
    {
        char pname[MAX_PART_NAME_LEN];
        sprintf(pname,"%05d",partnum);
        return LoadForUpdate(pname,sessionid);
    }

    apindex *GetReadIndex(const char *pname)
    {
        for(int i=0; i<index_info.size(); i++) {
            if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_READY) {
                if(index_info[i]->index==NULL) {
                    index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
                    index_info[i]->index->SetFileVer(file_ver);
                    index_info[i]->update_fuffixInfo_to_apindex();
                }
                return index_info[i]->index;
            }
        }
        index_info.push_back(new apindexinfo(pname,INDEX_READY,-1,ATTRINDEX_VERSION_2,is_suffix_key,1,0));
        apindexinfo *newindex=index_info.back();
        newindex->index=new apindex(attrnum,pname,basepath,false);
        newindex->index->SetFileVer(file_ver);
        newindex->update_fuffixInfo_to_apindex();
        return newindex->index;
    }

    void RemovePart(const char *pname)
    {
        for(int i=0; i<index_info.size(); i++) {
            if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_READY) {
                if(index_info[i]->index != NULL) {
                    delete index_info[i]->index;
                    index_info[i]->index =NULL;
                }
                delete index_info[i];
                index_info[i] = NULL;
                index_info.erase(index_info.begin()+i);
                i--;
            }
        }
    }

    // index list中对一个分区可能有两个items,一个是ready for read ,另一个是index_build
    apindexinfo *LoadForUpdate(const char *pname,int sessionid,bool IsMerge = false)
    {
        for(int i=0; i<index_info.size(); i++) {
            if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
                // increment build is not supported!
                //index_info[i]->sessionid=sessionid;
                assert(index_info[i]->action==INDEX_BUILD);
                if(index_info[i]->index==NULL) {
                    t_init_index_info(i,true);
                }
                if(!IsMerge)
                    index_info[i]->index->ClearDB();
                return index_info[i];
            }
            //remove old sessionid load
            else if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_BUILD) {
                if(index_info[i]->index != NULL) {
                    delete index_info[i]->index;
                    index_info[i]->index = NULL;
                }
                delete index_info[i];
                index_info[i] = NULL;
                index_info.erase(index_info.begin()+i);
                i--;
            }
        }
        index_info.push_back(new apindexinfo(pname,INDEX_BUILD,sessionid,file_ver,is_suffix_key,1,0));
        apindexinfo *newindex=index_info.back();
        newindex->index=new apindex(attrnum,pname,basepath,true);
        newindex->index->SetFileVer(file_ver);
        newindex->update_fuffixInfo_to_apindex();
        if(!IsMerge)
            newindex->index->ClearDB();
        return newindex;
    }

    void Rollback(int tabid,short partnum,int sessionid)
    {
        char pname[MAX_PART_NAME_LEN];
        sprintf(pname,"%05d",partnum);
        Rollback(tabid,pname,sessionid);
    }

    void Rollback(int tabid,const char *pname,int sessionid)
    {
        /*std::vector<apindexinfo>::iterator iter=index_info.begin();
        for(;iter!=index_info.end();++iter) {
            if(strcmp(pname,iter->partname)==0 && iter->sessionid==sessionid) {
                // increment build is not supported!
                assert(iter->action==INDEX_BUILD);
                if(iter->index!=NULL)
                    delete iter->index;
                iter->index=new apindex(attrnum,iter->partname,basepath,true);
                iter->index->ClearDB();
                index_info.erase(iter);
                break;
            }
        }*/
        RemoveGlobal(tabid,sessionid,pname);
        ClearTempDB(pname,sessionid);
        SaveHeader();
    }

    void ClearTempDB(const char *pname,int sessionid)
    {
        std::vector<apindexinfo *>::iterator iter=index_info.begin();
        for(; iter!=index_info.end(); ++iter) {
            // ClearTempDB By a readonly Table (RCAttr instead of RCAttrLoad)
            if(strcmp(pname,(*iter)->partname)==0 && (*iter)->sessionid==sessionid) {
                // increment build is not supported!
                assert((*iter)->action==INDEX_BUILD);
                if((*iter)->index==NULL) {
                    (*iter)->index=new apindex(attrnum,(*iter)->partname,basepath,true);
                    (*iter)->index->SetFileVer(file_ver);
                    (*iter)->update_fuffixInfo_to_apindex();
                }
                std::string idxpath=(*iter)->index->GetPath();
                (*iter)->index->ClearDB();
                (*iter)->index->ClearMap();

                if((*iter)->index != NULL) {
                    delete (*iter)->index;
                    (*iter)->index = NULL;
                }
                delete *iter;
                *iter = NULL;
                index_info.erase(iter);
                DeleteDirectory( idxpath);
                break;
            }
        }
    }

	// 默认就所有分区都删除,否则就删除指定的分区
    void ClearMergeDB(const std::string del_dbname = "")
    {
        std::vector<apindexinfo *>::iterator iter=index_info.begin();
        for(; iter!=index_info.end(); ++iter) {
            if((*iter)->action==INDEX_READY) {
                std::string dbname=apindex::GetDBPath(attrnum,(*iter)->partname,basepath,false);
                char mergepath[300];
				if(del_dbname.size() == 0){ // 默认所有都清除
	                if(DoesFileExist( apindex::GetMergeIndexPath(dbname.c_str(),mergepath))){
	                    DeleteDirectory(mergepath);
	                }
 		        }else{
					if(del_dbname == dbname){ // 清除单个
		                if(DoesFileExist( apindex::GetMergeIndexPath(dbname.c_str(),mergepath))){
		                    DeleteDirectory(mergepath);
		                }
				    }
 		        }
            }
        }
    }

    //lock table for write before commit
    //  and reload table after commit

    void Commit(int tabid,short partnum,int sessionid)
    {
        char pname[MAX_PART_NAME_LEN];
        sprintf(pname,"%05d",partnum);
        Commit(tabid,pname,sessionid);
    }

    void Commit(int tabid,const char *pname,int sessionid)
    {
        for(int i=0; i<index_info.size(); i++) {
            if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
                // increment build is not supported!
                assert(index_info[i]->action==INDEX_BUILD);
                isvalid=true;

                // delete and reopen to close index to commit;
                // Not load index at commit !
                //if(index_info[i]->index!=NULL)
                // delete index_info[i]->index;
                //index_info[i]->index=NULL;

                //get apindex or create if not exists.
                apindex *pindex=GetReadIndex(pname);

                // fix dma-1327
                index_info[i]->update_fuffixInfo_to_apindex(pindex);

                char mergename[300];
                if(DoesFileExist(pindex->MergeIndexPath(mergename))) {
                    pindex->ReplaceDB(mergename);
                    pindex->ReloadDB();

                    if(index_info[i]->index==NULL) {
                        index_info[i]->index=new apindex(attrnum,pname,basepath,true);
                        index_info[i]->index->SetFileVer(file_ver);
                        index_info[i]->update_fuffixInfo_to_apindex();
                    }
                    std::string idxpath=index_info[i]->index->GetPath();
                    if(index_info[i]!=NULL) {
                        if(index_info[i]->index!=NULL) {
                            delete index_info[i]->index;
                            index_info[i]->index = NULL;
                        }
                        delete index_info[i];
                        index_info[i]= NULL;
                    }
                    index_info.erase(index_info.begin()+i);
                    DeleteDirectory(idxpath );
                } else {
                    //reuse temp index
                    //use hash map prepared in loading data.
                    if(index_info[i]->index==NULL) {
                        index_info[i]->index=new apindex(attrnum,pname,basepath,true);
                        index_info[i]->index->SetFileVer(file_ver);
                        index_info[i]->update_fuffixInfo_to_apindex();
                    }
                    apindex *tomerge=index_info[i]->index;

                    //apindex *tomerge=GetGlobal(tabid,sessionid,pname);
                    //backup for delete later
                    std::string idxpath=tomerge->GetPath();
                    //pindex->ImportHash(*tomerge);
                    //tomerge->ClearDB();// 内部删除*.bin文件
                    // release hashmap
                    //tomerge->ClearMap();
                    //release write db, must be last instance

                    //RemoveGlobal(tabid,sessionid,pname);
                    if(index_info[i]!=NULL) {
                        if(index_info[i]->index != NULL) {
                            delete index_info[i]->index;
                            index_info[i]->index= NULL;
                        }
                        delete index_info[i];
                        index_info[i] = NULL;
                    }
                    index_info.erase(index_info.begin()+i);
                    DeleteDirectory(idxpath );

                    // reloaddb to write LOG to SST
                    // keep pindex open(no reopen) ?
                    pindex->ReloadDB();
                    // reloaddb again to release log/sst merge resource
                    // pindex->ReloadDB();
                }
                break;
            }
            // clear other sessions in building
            else if(index_info[i]->action==INDEX_BUILD) {
                // clear all others session (hanged session will be remove)
                t_init_index_info(i,true);

                index_info[i]->index->ClearDB();
                if(index_info[i] != NULL) {
                    if(index_info[i]->index!=NULL) {
                        delete index_info[i]->index;
                        index_info[i]->index=NULL;
                    }
                    delete index_info[i];
                    index_info[i]=NULL;
                }
                index_info.erase(index_info.begin()+i);
                // current item(i) became the next one and need be proceed again,minus 1 to cancel out i++ in loop
                --i;
            }
        }

        SaveHeader();
    }

    bool Get(int key,std::vector<int>  &packs)
    {
        if(!isvalid) return false;

        if(file_ver == ATTRINDEX_VERSION_2) {
            if(!is_suffix_key) {

                std::string _key = CR_Class_NS::i642str(key);
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);

            } else { // fix dma-1319

                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->Get(key,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack_int,this,i,boost::ref(key),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }

                return packs.size() > 0 ;
            }

        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(int));
            return Get(keyslice,packs);
        }
    }

    bool Get(long key,std::vector<int>  &packs)
    {
        if(!isvalid) return false;

        if(file_ver == ATTRINDEX_VERSION_2) {
            if(!is_suffix_key) {

                std::string _key = CR_Class_NS::i642str(key);
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);
            } else { // fix dma-1319

                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->Get(key,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack_long,this,i,boost::ref(key),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }

                return packs.size() > 0 ;
            }

        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(long));
            return Get(keyslice,packs);
        }
    }

    bool Get(_int64 key,std::vector<int>  &packs)
    {
        if(!isvalid) return false;

        if(file_ver == ATTRINDEX_VERSION_2) {
            if(!is_suffix_key) {

                std::string _key = CR_Class_NS::i642str(key);
                leveldb::Slice keyslice(_key);
                return Get(keyslice,packs);

            } else { // fix dma-1319

                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->Get(key,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack_int64,this,i,boost::ref(key),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }

                return packs.size() > 0 ;
            }
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(_int64));
            return Get(keyslice,packs);
        }
    }

    bool Get(const char *key,const uint key_len,std::vector<int>  &packs)
    {
        if(!isvalid) return false;
        if(!is_suffix_key) {

            leveldb::Slice keyslice(key,key_len);
            return Get(keyslice,packs);

        } else { // fix dma-1319

            if(index_info.size()<3) {
                for(int i=0; i<index_info.size(); i++) {
                    if(index_info[i]->action==INDEX_READY) {
                        t_init_index_info(i,false);
                        index_info[i]->index->Get(key,key_len,packs);
                    }
                }
            } else {
                DMThreadGroup ptlist;
                std::vector< std::vector<int> > _packs_vector(index_info.size());

                // get packs
                for(int i=0; i<index_info.size(); i++) {
                    if(index_info[i]->action==INDEX_READY) {
                        ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack_char,this,i,key,key_len,boost::ref(_packs_vector[i])));
                    }
                }
                ptlist.WaitAllEnd();

                // merge packs
                for(int i=0; i<_packs_vector.size(); i++)    {
                    packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                }
            }

            return packs.size() > 0 ;
        }
    }


    // 初始化构建对象
    void t_init_index_info(int i,bool bwritemode)
    {
        if(index_info[i]->index==NULL) { // TODO : how to decide it's write or read?
            //index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
            index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,bwritemode);
            index_info[i]->index->SetFileVer(file_ver);
            index_info[i]->update_fuffixInfo_to_apindex();
        }
    }
    // 获取单个leveldb 的key对应的pack到vector中
    int t_GetPack_Slice(int i,leveldb::Slice &key,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->Get(key,packs)? 0:-1;
    }
    int t_GetPack_int(int i,int &key,std::vector<int> &packs)   // int
    {
        t_init_index_info(i,false);
        return index_info[i]->index->Get(key,packs)? 0:-1;
    }
    int t_GetPack_long(int i,long &key,std::vector<int> &packs)   // long
    {
        t_init_index_info(i,false);
        return index_info[i]->index->Get(key,packs)? 0:-1;
    }
    int t_GetPack_int64(int i,_int64 &key,std::vector<int> &packs)  
    {
        t_init_index_info(i,false);
        return index_info[i]->index->Get(key,packs)? 0:-1;
    }
    int t_GetPack_char(int i,const char* key,const uint key_len,std::vector<int> &packs)   
    {
        t_init_index_info(i,false);
        return index_info[i]->index->Get(key,key_len,packs)? 0:-1;
    }


    bool GetRange(int key,int limit,std::vector<int>  &packs)
    {
        if(!isvalid) return false;
        if(file_ver == ATTRINDEX_VERSION_2) {

            if(!is_suffix_key) {
                std::string _key = CR_Class_NS::i642str(key);
                std::string _limit =  CR_Class_NS::i642str(limit);
                leveldb::Slice keyslice(_key);
                leveldb::Slice keylimit(_limit);
                return GetRange(keyslice,keylimit,packs);

            } else {
                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->GetRange(key,limit,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack_int,this,i,boost::ref(key),boost::ref(limit),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }
                return packs.size() > 0;
            }
        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(int)),keylimit((const char *)&limit,sizeof(int));
            return GetRange(keyslice,keylimit,packs);
        }
    }

    bool GetRange(long key,long limit,std::vector<int>  &packs)
    {
        if(!isvalid) return false;
        if(file_ver == ATTRINDEX_VERSION_2) {
            if(!is_suffix_key) {

                std::string _key = CR_Class_NS::i642str(key);
                std::string _limit =  CR_Class_NS::i642str(limit);
                leveldb::Slice keyslice(_key);
                leveldb::Slice keylimit(_limit);
                return GetRange(keyslice,keylimit,packs);

            } else {

                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->GetRange(key,limit,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack_long,this,i,boost::ref(key),boost::ref(limit),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }

                return packs.size()>0;

            }

        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(long)),keylimit((const char *)&limit,sizeof(long));
            return GetRange(keyslice,keylimit,packs);
        }
    }

    bool GetRange(_int64 key,_int64 limit,std::vector<int>  &packs)
    {
        if(!isvalid) return false;
        if(file_ver == ATTRINDEX_VERSION_2) {
            if(!is_suffix_key) {

                std::string _key = CR_Class_NS::i642str(key);
                std::string _limit =  CR_Class_NS::i642str(limit);
                leveldb::Slice keyslice(_key);
                leveldb::Slice keylimit(_limit);
                return GetRange(keyslice,keylimit,packs);

            } else {
                if(index_info.size()<3) {
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            t_init_index_info(i,false);
                            index_info[i]->index->GetRange(key,limit,packs);
                        }
                    }
                } else {
                    DMThreadGroup ptlist;
                    std::vector< std::vector<int> > _packs_vector(index_info.size());

                    // get packs
                    for(int i=0; i<index_info.size(); i++) {
                        if(index_info[i]->action==INDEX_READY) {
                            ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack_int64,this,i,boost::ref(key),boost::ref(limit),boost::ref(_packs_vector[i])));
                        }
                    }
                    ptlist.WaitAllEnd();

                    // merge packs
                    for(int i=0; i<_packs_vector.size(); i++)    {
                        packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                    }
                }
                return packs.size() > 0;
            }

        } else {
            leveldb::Slice keyslice((const char *)&key,sizeof(_int64)),keylimit((const char *)&limit,sizeof(_int64));
            return GetRange(keyslice,keylimit,packs);
        }
    }

    bool GetRange(const char *key,const uint key_len,const char *limit,const uint limit_len,std::vector<int>  &packs)
    {
        if(!isvalid) return false;
        if(!is_suffix_key) {

            leveldb::Slice keyslice(key,key_len),keylimit(limit,limit_len);
            return GetRange(keyslice,keylimit,packs);

        } else {
            if(index_info.size()<3) {
                for(int i=0; i<index_info.size(); i++) {
                    if(index_info[i]->action==INDEX_READY) {
                        t_init_index_info(i,false);
                        index_info[i]->index->GetRange(key,key_len,limit,limit_len,packs);
                    }
                }
            } else {
                DMThreadGroup ptlist;
                std::vector< std::vector<int> > _packs_vector(index_info.size());

                // get packs
                for(int i=0; i<index_info.size(); i++) {
                    if(index_info[i]->action==INDEX_READY) {
                        ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack_char,this,i,
                            boost::ref(key),key_len,boost::ref(limit),limit_len,boost::ref(_packs_vector[i])));
                    }
                }
                ptlist.WaitAllEnd();

                // merge packs
                for(int i=0; i<_packs_vector.size(); i++)    {
                    packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
                }
            }
            return packs.size() > 0;
        }
    }

    // 获取单个leveldb 的key对应的pack到vector中
    int t_GetRangePack_Slice(int i,leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->GetRange(key,limit,packs)? 0:-1;
    }
    int t_GetRangePack_int(int i,int& key,int& limit,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->GetRange(key,limit,packs)? 0:-1;
    }

    int t_GetRangePack_long(int i,long& key,long& limit,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->GetRange(key,limit,packs)? 0:-1;
    }

    int t_GetRangePack_int64(int i,_int64& key,_int64& limit,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->GetRange(key,limit,packs)? 0:-1;
    }
    int t_GetRangePack_char(int i,const char *key,const uint key_len,const char *limit,const uint limit_len,std::vector<int> &packs)
    {
        t_init_index_info(i,false);
        return index_info[i]->index->GetRange(key,key_len,limit,limit_len,packs)? 0:-1;
    }

protected:

    // return false : can not use index,maybe not a indexed column or index in building
    // return true : return found packs
    bool Get(leveldb::Slice &key,std::vector<int> &packs)
    {
        if(!isvalid) return false;

        if(index_info.size()<3) {
            for(int i=0; i<index_info.size(); i++) {
                if(index_info[i]->action==INDEX_READY) {
                    t_init_index_info(i,false);
                    index_info[i]->index->Get(key,packs);
                }
            }
        } else {
            DMThreadGroup ptlist;
            std::vector< std::vector<int> > _packs_vector(index_info.size());

            // get packs
            for(int i=0; i<index_info.size(); i++) {
                if(index_info[i]->action==INDEX_READY) {
                    ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack_Slice,this,i,boost::ref(key),boost::ref(_packs_vector[i])));
                }
            }
            ptlist.WaitAllEnd();

            // merge packs
            for(int i=0; i<_packs_vector.size(); i++)    {
                packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
            }
        }
        return true;
    }


    // return false : can not use index,maybe not a indexed column or index in building
    // return true : return found packs
    bool GetRange(leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs)
    {
        if(!isvalid) return false;

        if(index_info.size()<3) {
            for(int i=0; i<index_info.size(); i++) {
                if(index_info[i]->action==INDEX_READY) {
                    t_init_index_info(i,false);
                    index_info[i]->index->GetRange(key,limit,packs);
                }
            }
        } else {
            DMThreadGroup ptlist;
            std::vector< std::vector<int> > _packs_vector(index_info.size());

            // get packs
            for(int i=0; i<index_info.size(); i++) {
                if(index_info[i]->action==INDEX_READY) {
                    ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack_Slice,this,i,boost::ref(key),boost::ref(limit),boost::ref(_packs_vector[i])));
                }
            }
            ptlist.WaitAllEnd();

            // merge packs
            for(int i=0; i<_packs_vector.size(); i++)    {
                packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
            }
        }
        return packs.size()>0;
    }

};

#endif

