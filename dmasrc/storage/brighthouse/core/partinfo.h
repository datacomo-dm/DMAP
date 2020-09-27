
#ifndef ATTR_PARTITION_INFO
#define ATTR_PARTITION_INFO
#include "apindex.h"
#include "system/IBFile.h"
#include "system/RCException.h"
#include <map>
// 分区文件不连续版本，不能多个分区同时装入？
// pack number 有可能是不连续的,比如在分区的多次交叉装入过程就会导致数据包编号不一致
//  比如；
//    分区1 packs 1,2,3,4,5,        10,12         ,16,17,18,19,20
//    分区2 packs           6,7,8,9      ,13,14,15
//     文件号     1,1,1,1,1,2,2,2,2,1,  1,2  ,2,2, 3 ,3 ,3 ,3 ,3,
// 在一个分区中，文件也有可能不连续，比如上面的例子，分区1种的文件 1，3 分区2的文件 2
//
//
//分区文件连续版本，文件名中加入分区名称，每个分区单独从1编号
//
//
//分区不连续实现较容易，首先选择不连续的方案
typedef std::map<int,int> SectionMap;
class attr_partinfo
{
    int attrnum;
    char partname[MAX_PART_NAME_LEN];
    char basepath[300];
    uint firstpack,lastpack;
    uint lastpackobjs;
    uint lastsavepack;
    uint lastsavepackobjs;
    uint savesessionid;
    uint total_packs;
    uint lastfileid;
    int savepos;
    int lastsavepos;
    std::vector<uint> files;
    struct partsection {
        int packstart,packend,lastobjs;
        partsection()
        {
            packstart=-1;
            packend=-1;
            lastobjs=0;
        }
        partsection(int st,int ed,int objs)
        {
            packstart=st;
            packend=ed;
            lastobjs=objs;
        }
        partsection(const partsection &cp)
        {
            packstart=cp.packstart;
            packend=cp.packend;
            lastobjs=cp.lastobjs;
        }
        void setobjs(int objs)
        {
            lastobjs=objs;
        }

    };
    typedef std::vector<partsection> PartSectionList;
    PartSectionList partlist;

public :

    bool HasFile(int fileid)
    {
        int vectsize=files.size();
        for(int i=0; i<vectsize; i++) {
            if(files[i]==fileid) return true;
        }
        return false;
    }

    void AppendSection(SectionMap &pmap)
    {
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            pmap[partlist[i].packstart]=partlist[i].packend;
        }
    }
    // for truncate table partition oper
    PartSectionList getpartlist()
    {
        return partlist;
    }
    const char *name()
    {
        return partname;
    }
    uint getsessionid()
    {
        return savesessionid;
    }
    void setsessionid(uint sid)
    {
        savesessionid=sid;
    }
    uint getpacks()
    {
        return total_packs;
    }
    bool ispart(const char *name)
    {
        return strcmp(partname,name)==0;
    }

    bool ispart(int partid)
    {
        char partstr[300];
        sprintf(partstr,"%05d",partid);
        return strcmp(partname,partstr)==0;
    }

    //create new part with partname
    attr_partinfo(int _attrnum,const char *_basepath,const char *_partname)
    {
        init();
        attrnum=_attrnum;
        if(_partname!=NULL) {
            assert(strlen(_partname)<MAX_PART_NAME_LEN);
            strcpy(partname,_partname);
        } else {
            partname[0]=0;
        }

        if(_partname!=NULL)
            strcpy(basepath,_basepath);
        else
            basepath[0]=0;
        //loadhead();
    }
    // create new part with partid
    attr_partinfo(int _attrnum,const char *_basepath,int partid)
    {
        char partstr[300];
        sprintf(partstr,"%05d",partid);
        attr_partinfo(_attrnum,_basepath,partstr);
    }
    // load part from control file
    attr_partinfo()
    {
        init();
    }
    // partname version
    // 数据文件的名称中包含分区信息的版本
    // 分区不连续的版本，不需要使用这个方法！
    std::string getpackfile(int file)
    {
        std::string filename;
        char fn[300];
        if(partname[0]!=0)
            sprintf(fn,"%s/TA_%s_%05d%09d.ctb",basepath,partname,attrnum,file);
        else
            sprintf(fn,"%s/TA%05d%09d.ctb",basepath,attrnum,file);
        filename=fn;
        return filename;
    }

    void init()
    {
        firstpack=lastpack=0;
        lastpackobjs=0;
        total_packs=0;
        lastsavepack=0;
        lastsavepackobjs=0;
        savesessionid=0;
        attrnum=-1;
        savepos=-1;
        lastsavepos=-1;
        lastfileid=0;
        files.clear();
        partlist.clear();
    }

    void load(IBFile &partf,bool oldversion)
    {
        short len=0;
        total_packs=0;
        char buf[8];
        partf.Read(buf,8);
        if(memcmp(buf,"PARTPARA",8)!=0) {
            throw DatabaseRCException("Wrong partition info file.");
        }
        partf.Read(&len,sizeof(short));
        partf.Read(partname,len);
        partname[len]=0;
        partf.Read(&firstpack,sizeof(uint));
        partf.Read(&lastpack,sizeof(uint));
        partf.Read(&lastpackobjs,sizeof(uint));
        partf.Read(&lastsavepack,sizeof(uint));
        partf.Read(&lastsavepackobjs,sizeof(uint));
        partf.Read(&savesessionid,sizeof(uint));
        partf.Read(&attrnum,sizeof(uint));
        // last commited file (uncommited file in files)
        if(!oldversion) {
            partf.Read(&lastfileid,sizeof(uint));
            partf.Read(&savepos,sizeof(int));
            partf.Read(&lastsavepos,sizeof(int));
        }
        // files vector number
        int vectsize;
        partf.Read(&vectsize,sizeof(int));
        files.clear();
        for(int i=0; i<vectsize; i++) {
            int file=0;
            partf.Read(&file,sizeof(int));
            files.push_back(file);
        }
        int packsize;
        partf.Read(&packsize,sizeof(int));
        partlist.clear();
        for(int i=0; i<packsize; i++) {
            int packstart=0,packend=0,lastobjs=0;
            partf.Read(&packstart,sizeof(int));
            partf.Read(&packend,sizeof(int));
            partf.Read(&lastobjs,sizeof(int));
            uint sectionpacks=0;
            if(lastpack>=packstart) // otherwise it's a new section which has not commited,ommited
                sectionpacks=(packend>lastpack?(lastpack==0?-1:lastpack):packend)-packstart+1;
            total_packs+=sectionpacks;
            partlist.push_back(partsection(packstart,packend,lastobjs));
        }
        if(oldversion) {
            // no idea for how to get actually last commited fileid on oldversion
            if(files.size()>0)
                lastfileid=lastfile();
            lastsavepos=-1;
            savepos=-1;
        }
    }

    int getfiles()
    {
        return files.size();
    }
    //设置当前loading进程的文件位置
    void setsavefilepos(int v)
    {
        lastsavepos=v;
    }

    //if not in append load mode,lastsavepos has reset to savepos in preload
    int getlastsavepos()
    {
        return lastsavepos;
    }

    //得到已提交的数据写入位置
    int getsavefilepos()
    {
        return savepos;
    }
    //保存两份，以防丢失
    void save(IBFile &partf,long& _write_size)
    {
        short len=0;
        char buf[8];
        if(partlist.size()>0)
            firstpack=partlist[0].packstart;
        partf.Write("PARTPARA",8);
        _write_size += 8;

        len=strlen(partname);
        partf.Write(&len,sizeof(short));
        _write_size += sizeof(short);

        partf.Write(partname,len);
        _write_size += len;

        partf.Write(&firstpack,sizeof(uint));
        partf.Write(&lastpack,sizeof(uint));
        partf.Write(&lastpackobjs,sizeof(uint));
        partf.Write(&lastsavepack,sizeof(uint));
        partf.Write(&lastsavepackobjs,sizeof(uint));
        partf.Write(&savesessionid,sizeof(uint));
        partf.Write(&attrnum,sizeof(uint));
        partf.Write(&lastfileid,sizeof(uint));
        partf.Write(&savepos,sizeof(int));
        partf.Write(&lastsavepos,sizeof(int));
        // files vector number
        int vectsize=files.size();
        partf.Write(&vectsize,sizeof(int));

        _write_size += (11*sizeof(int));

        for(int i=0; i<vectsize; i++) {
            int file=files[i];
            partf.Write(&file,sizeof(int));
        }
        _write_size += (vectsize*sizeof(int));

        int packsize=partlist.size();
        partf.Write(&packsize,sizeof(int));
        _write_size += sizeof(int);

        for(int i=0; i<packsize; i++) {
            int packstart=partlist[i].packstart,packend=partlist[i].packend,lastobjs=partlist[i].lastobjs;
            partf.Write(&packstart,sizeof(int));
            partf.Write(&packend,sizeof(int));
            partf.Write(&lastobjs,sizeof(int));
        }
        _write_size += (packsize*12);

    }

    // return -1 means at end of partition files vector
    bool isempty()
    {
        // total_packs means packs be committed.
        //return total_packs==0;
        //return files.size()==0;
        return lastsavepackobjs==0 && lastsavepack==0;
    }

    int firstfile()
    {
        if(files.size()>0)
            return files[0];
        return -1;
    }

    int nextfile(int file)
    {
        int vectsize=files.size();
        for(int i=0; i<vectsize; i++) {
            if(files[i]==file) {
                if(i<vectsize-1) return files[i+1];
                return -1;
            }
        }
        //not found
        //assert(0);
        return -1;
    }

    int lastsavefile()
    {
        return lastfileid;
    }

    // lastfile in vector incluing uncommitted
    int lastfile()
    {
        if(files.size()>0) return files.back();
        return -1;
    }

    //恢复操作,恢复到新的表
    void SetID(int _attrnum)
    {
        attrnum=_attrnum;
    }

    // get next package in this parition
    // -1 means end of partition
    int nextpack(int pack)
    {
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            if(pack>=partlist[i].packstart) {
                if(pack<partlist[i].packend) return ++pack;
                if(pack==partlist[i].packend) {
                    if(i==mysize-1) return -1;
                    return partlist[i+1].packstart;
                }
            }
        }
        // not found
        return -1;
    }

    // 在 rmvp被删除(删除分区)时,调整全部分区的列表数据
    //rmv: another partition part packs list
    void removeby(attr_partinfo &rmvp,int *packvars,std::set<std::string> &rebuildparts)
    {
        PartSectionList &rmv=rmvp.partlist;
        int mysize=partlist.size();
        int rmsize=rmv.size();
        int srmpacks=0;
        for(int i=0; i<rmsize; i++) {
            int rmpackstart=rmv[rmsize-i-1].packstart;
            int rmpacks=rmv[rmsize-i-1].packend-rmv[rmsize-i-1].packstart+1;
            packvars[i*2]=rmpackstart+rmpacks;
            srmpacks+=rmpacks;
            //packvars[i*2+1]=srmpacks;   // to fix dma-1232 delete by liujs
            packvars[i*2+1]=rmpacks;
        }
        for(int j=0; j<mysize; j++) {
            //for(int i=rmsize-1;i>=0;i--) { // to fix dma-1232 delete by liujs
            for(int i=0; i<=rmsize-1; i++) {
                if(partlist[j].packstart>=packvars[i*2]) {
                    partlist[j].packstart-=packvars[i*2+1];
                    partlist[j].packend-=packvars[i*2+1];
                    if(rebuildparts.count(std::string(partname))==0)
                        rebuildparts.insert(std::string(partname));
                }
            }
        }
        if(mysize>0) {
            lastsavepack=lastpack=partlist.back().packend;
            firstpack=partlist[0].packstart;
        }
    }

    //备份操作:抽取出当前分区,需要调整列表数据
    // compack pactlist
    void ExtractMe()
    {
        int cur_packs=0;
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            int ppacks=partlist[i].packend+1-partlist[i].packstart;
            partlist[i].packstart=cur_packs;
            partlist[i].packend=cur_packs+ppacks-1;
            cur_packs+=ppacks;
        }
    }

    //恢复操作:当前分区调整列表数据,准备加入到分区列表中
    void AppendFrom(int lastpack)
    {
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            partlist[i].packstart+=lastpack;
            partlist[i].packend+=lastpack;
        }
    }

    // return:
    // 1: 包是连续的,直接设置最后一个包的编号即可
    // 2: 包已经设置过,不用处理
    // 3: 包是不连续的,被其他分区隔断了的
    int newpack(int pack)
    {
        int mysize=partlist.size();
        if(mysize==0) firstpack=pack;
        for(int i=0; i<mysize; i++) {
            if(
                (pack==partlist[i].packend+1) &&
                (partlist[i].lastobjs == MAX_PACK_ROW_SIZE) // 解决分布式排序入库第一个包就不满的情况[N][F]...[F][N]
            ) {
                ++partlist[i].packend;
                lastsavepack=partlist[i].packend;
                lastsavepackobjs=0;
                total_packs++;
                return 1;
            } else if(pack>=partlist[i].packstart && pack<=partlist[i].packend)
                return 2;
        }
        // add a new pack pair
        partlist.push_back(partsection(pack,pack,0));
        lastsavepack=pack;
        lastsavepackobjs=0;
        total_packs++;
        return 3;
    }

    void setsaveobjs(int objs)
    {
        lastsavepackobjs=objs;
        partlist[partlist.size()-1].lastobjs=objs;
    }
    /*
    bool isempty() {
      return lastsavepackobjs==0 && lastsavepack==0;
    }
    */
    int getsaveobjs()
    {
        return lastsavepackobjs;
    }

    ushort getlastpackobjs()
    {
        return (ushort)lastpackobjs;
    }

    // the pack shoud be equal to last element of partlist(packend)
    void setsavepos(int pack,int objs)
    {
        //
        assert(lastsavepack==pack||lastsavepack==pack-1);
        lastsavepack=pack;
        lastsavepackobjs=objs;
        partlist[partlist.size()-1].lastobjs=objs;
    }

    int size()
    {
        return partlist.size();
    }
    uint getsavepack()
    {
        return lastsavepack;
    }

    uint getlastpack()
    {
        return lastpack;
    }

    void getsavepos(int &pack,int &objs)
    {
        pack=lastsavepack;
        objs=lastsavepackobjs;
    }

    // search file by pack ? no need to save file info
    void append(int file)
    {
        files.push_back(file);
    }

    // just adjust variables, has not been saved to control file!!
    void commit()
    {
        savesessionid=0;
        lastpackobjs=lastsavepackobjs;
        lastpack=lastsavepack;
        savepos=lastsavepos;
        lastfileid=lastfile();
    }

    // force reset to last committed position.(in case of repeat loading on error)
    // return false for reseted last load
    bool preload(uint sessid,std::vector<int> &rmfiles)
    {
        //rollback but keep value of savesessionid
        if(sessid==savesessionid)
            return true;
        //uint oldsession=savesessionid=0;
        rollback(rmfiles);
        //savesessionid=oldsession;
        savesessionid=sessid;
        lastsavepack=lastpack;
        lastsavepackobjs=lastpackobjs;
        lastfileid=lastfile();
        return false;
    }

    void AppendNFBlocks(std::map<int,int> &nfblocks)
    {
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            if(partlist[i].packstart<=lastpack) {
                if(partlist[i].packend>=lastpack)
                    // using committed one
                    nfblocks[lastpack]=lastpackobjs;
                else
                    nfblocks[partlist[i].packend]=partlist[i].lastobjs;
            }
        }
    }

    void rollback(std::vector<int> &rmfiles)
    {
        savesessionid=0;
        lastsavepackobjs=lastpackobjs;
        lastsavepack=lastpack;
        lastsavepos=savepos;
        int mysize=partlist.size();
        for(int i=0; i<mysize; i++) {
            if( lastpack>=partlist[i].packstart && lastpack<=partlist[i].packend) {
                partlist[i].packend=lastpack;
                partlist[i].lastobjs=lastpackobjs;
            }
            if( partlist[i].packstart>lastpack) {
                //should be last item
                assert(i+1==mysize);
                partlist.erase(partlist.begin()+i);
                if(partlist.size()==0) {
                    // empty partition,should be erase
                    init();
                }
                return;
            }
        }
        //roll back files
        std::vector<uint>::iterator iter=files.begin();
        while(iter!=files.end()) {
            if(*iter>lastfileid) {
                iter=files.erase(iter);
                rmfiles.push_back(*iter);
            } else ++iter;
        }
    }


};

class attr_partitions
{
    //客户端在发起装载数据前，需要检查是否有正在处理的任务
    // part will be saving ,fill by admin program(seperate from server process)
    char savepartname[MAX_PART_NAME_LEN];
    char truncpartname[MAX_PART_NAME_LEN];
    // the client which apply for loading ,connect_id (session_id?)
    int conn_id,trunc_sid;
    short lastload;
    // sess_id not save to file,but updated by active loader process
    uint sess_id;
    char basepath[300];
    int attrnum;
    uint total_packs;
    std::vector<attr_partinfo> parts;
    const std::string getfile()
    {
        char filename[300];
        std::string fn;
        sprintf(filename,"%s/PART_%05d.ctl",basepath,attrnum);
        fn=filename;
        return fn;
    }
    const std::string getglobalfile()
    {
        char filename[300];
        std::string fn;
        sprintf(filename,"%s/loadinfo.ctl",basepath);
        fn=filename;
        return fn;
    }
public:
    void Set_TruncatePartName(const char* partname)
    {
        strcpy(truncpartname,partname);
    }
    char* Get_TruncatePartName()
    {
        return truncpartname;
    }

public :
    bool IsLastLoad()
    {
        return lastload==1;
    }
    attr_partitions(int _attrid,const char *_basepath)
    {
        attrnum=_attrid;
        conn_id=-1;
        trunc_sid=-1;
        lastload=0;
        strcpy(basepath,_basepath);
        savepartname[0]=0;
        parts.clear();
        total_packs=0;
        sess_id=-1;
        load();
    }
    virtual ~attr_partitions()
    {
        attrnum=-1;
        conn_id=-1;
        trunc_sid=-1;
        lastload=0;
        basepath[0]=0;
        savepartname[0]=0;
        parts.clear();
        total_packs=0;
        sess_id=-1;
    }
    // fill part section list(merge all partition except 'exclude_part')
    //  (map is orderded default)
    // fill lastpack object number
    int GetPartSection(SectionMap &pmap,const char *exclude_part="")
    {
        pmap.clear();
        int partsize=parts.size();
        int lastobjs=0;
        for(int i=0; i<partsize; i++) {
            if(strcmp(parts[i].name(),exclude_part)!=0 ) {
                lastobjs=parts[i].getlastpackobjs();
                parts[i].AppendSection(pmap);
            }
        }
        return lastobjs;
    }

    int GetLastFile()
    {
        int lfile=0;
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            int fid=parts[i].firstfile();
            while(fid!=-1) {
                if(fid>lfile) lfile=fid;
                fid=parts[i].nextfile(fid);
            }
        }
        return lfile;
    }
    // TotalPacks?
    uint GetLastPack()
    {
        uint lastpack=0;
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            uint pack=parts[i].getlastpack();
            if(lastpack<pack) lastpack=pack;
        }
        return lastpack;
    }

    int GetFileList(std::vector<int> &filelist,const char *exceptpart=NULL)
    {
        int files=0;
        filelist.clear();
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            if(exceptpart && parts[i].ispart(exceptpart) ) continue;
            int fid=parts[i].firstfile();
            while(fid!=-1) {
                filelist.push_back(fid);
                fid=parts[i].nextfile(fid);
                ++files;
            }
        }
        return files;
    }

    int GetPartFileList(std::vector<int> &filelist,const char *part=NULL)
    {
        int files=0;
        filelist.clear();
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            if(part && parts[i].ispart(part) ) {
                int fid=parts[i].firstfile();
                while(fid!=-1) {
                    filelist.push_back(fid);
                    fid=parts[i].nextfile(fid);
                    ++files;
                }
            }
        }
        return files;
    }
    /* make sure control file is exists?
    attr_partitions()
    {
        tabnum=-1;
        attrnum=-1;
        conn_id=-1;
        basepath[0]=0;
        savepartname[0]=0;
        parts.clear();
    }*/

    std::string GetTruncateControlFile()
    {
        std::string fn=basepath;
        fn+="/truncinfo.ctl";
        return fn;
    }
    //truncate a partition by a control file and truncate table statement
    // control file use only once
    bool GetTruncatePartInfo(int &sid)
    {
        std::string filename=GetTruncateControlFile();
        if(!DoesFileExist(filename)) return false;
        else {
            IBFile truncinfo;
            truncinfo.OpenReadOnly(filename);
            char buf[8];
            truncinfo.Read(buf,8);
            if(memcmp(buf,"TRUNCTRL",8)!=0) return false;
            truncinfo.Read(&trunc_sid,sizeof(int));
            short len=0;
            truncinfo.Read(&len,sizeof(short));
            truncinfo.Read(&truncpartname,len);
            truncpartname[len]=0;
        }
        sid=trunc_sid;
        //remove file ?
        return true;
    }

    void Commit(uint sessionid)
    {
        attr_partinfo *attrpart=getsavepartptr(sessionid);
        attr_partinfo &savepart=getsavepartinfo(NULL);
        if(attrpart==NULL) { // && connid==conn_id)
            attrpart=&savepart;
        }
        if(attrpart==NULL && total_packs > 0) {
            rclog << lock << "Error: Commit got empty attr_partinfo<"<<basepath<<","<<attrnum<<">,"<<"<sessionid:"<<sessionid<<" conn_id:"<<conn_id<<">"<<unlock;
            throw DatabaseRCException("attr_partitions::Commit Error in Commit");
        } else if(attrpart==NULL) {
            // empty table:create table
            rclog << lock << "Warning: Commit got empty attr_partinfo<"<<basepath<<","<<attrnum<<">,"<<"<sessionid:"<<sessionid<<" conn_id:"<<conn_id<<">"<<unlock;
            return ;
        }

        if(attrpart->getsessionid()!=sessionid) {
            rclog << lock << "ERROR: Commit check sessionid error <sessionid:"<<sessionid<<" session_id:"<<attrpart->getsessionid()<<">"<<unlock;
        }

        //if(attrpart->getsessionid()==connid) // try to fix dma-1173
        {
            attrpart->commit();
            attrpart->setsessionid(0);
            conn_id=-1;
            truncpartname[0]=0;
            trunc_sid=-1;
            savepartname[0]=0;
            save();
            // dma-614: truncate partition failed after loading data
            //refresh data(esp. total_packs)
            load();
        }
    }

    void CleanCtrFile(const long connid = 0)
    {
        std::string  _global_file = getglobalfile();
        if(!DoesFileExist(getglobalfile()) || connid <=0) {
            return;
        } else {
            if((int)connid == conn_id) {
                unlink(_global_file.c_str());
            }
        }
    }

    void Rollback(uint connid,std::vector<int> &rmfiles)
    {
        attr_partinfo *attrpart=getsavepartptr(connid);
        if(attrpart==NULL && total_packs >0 ) {
            rclog << lock << "Error: Rollback got empty attr_partinfo<"<<basepath<<","<<attrnum<<">,"<<"<connid:"<<connid<<">"<<unlock;
            throw DatabaseRCException("attr_partitions::Rollback Error in rollback");
        } else if(attrpart==NULL) {
            rclog << lock << "Warning: Rollback got empty attr_partinfo<"<<basepath<<","<<attrnum<<">,"<<"<connid:"<<connid<<" conn_id:"<<conn_id<<">"<<unlock;
            return ;
        }
        if(attrpart->getsessionid()==connid) {
            attrpart->rollback(rmfiles);
            attrpart->setsessionid(0);
            conn_id=-1;
            savepartname[0]=0;
            truncpartname[0]=0;
            trunc_sid=-1;
            save();
        } else {
            rclog << lock << "ERROR: rollback check sessionid error <connid:"<<connid<<" session_id:"<<attrpart->getsessionid()<<">"<<unlock;
            throw DatabaseRCException("attr_partitions::Rollback Error in rollback");
        }
    }

    bool checksession(int connid)
    {
        if(conn_id!=connid) return false;
        //getsavepartinfo(NULL).preload();
        return true;
    }
    int GetSaveID()
    {
        return conn_id;
    }
    uint GetSessionID()
    {
        return sess_id;
    }
    void SetSessionID(uint v)
    {
        sess_id=v;
    }
    // if partname is null ,set current savepartname as connid
    // as loading data ,partname should be filled to control file by client program(dpadmiin)
    // NOT USED anywhere
    void SetSavePartition(uint connid,const char *partname)
    {
        if(partname!=NULL)
            strcpy(savepartname,partname);
        //create if none exists partition
        //getsavepartinfo(NULL).setsessionid(sess_id);
        conn_id=connid;
    }

    //Get or Create partition info,if partname is null ,get last save parition
    attr_partinfo *getsavepartptr(uint sessionid)
    {
        int partsize=parts.size();

        for(int i=0; i<partsize; i++) {
            if(parts[i].getsessionid()==sessionid ) {
                return &parts[i];
            }
        }
        return NULL;
    }

    attr_partinfo *gettruncpartptr(uint connid)
    {
        if(connid!=trunc_sid) return NULL;
        int partsize=parts.size();
        return getpartptr(truncpartname);
    }

    attr_partinfo *getpartptr(const char *pname)
    {
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            if(strcmp(parts[i].name(),pname)==0) {
                return &parts[i];
            }
        }
        return NULL;
    }

    // 获取对应分区的最后一次装入数据的最后一个包的编号和装入的记录数
    int getlastloadpackinfo(const char *partname,int& lastpackno,int& lastpackobj)
    {
        int partsize=parts.size();
        if(partname == NULL) {
            assert(0);
            return -1;
        }

        for(int i=0; i<partsize; i++) {
            if(strcmp(parts[i].name(),partname) == 0) { // 找到对应分区
                lastpackno = parts[i].getlastpack();
                lastpackobj = parts[i].getlastpackobjs();
                return 0;
            }
        }
        return -1;
    }

    //Get or Create partition info,if partname is null ,get last save parition
    attr_partinfo &getsavepartinfo(const char *partname)
    {
        int partsize=parts.size();
        if(partname==NULL && savepartname[0]==0) { // not loading by standard client(dpadmin)
            // save to a idle partition(last partition)
            // dma-607
            if(partsize>0) {
                strcpy(savepartname,parts[partsize-1].name());
                if(parts[partsize-1].getsessionid()==0)
                    parts[partsize-1].setsessionid(sess_id);
                return parts.back();
            }
            /*for(int i=partsize-1;i>=0;i--) {
                if(parts[i].getsessionid()!=-1) {
                    conn_id=parts[i].getsessionid();
                    strcpy(savepartname,parts[i].name());
                    return parts[i];
                }
            }
            */
            //if not empty parts,but all busy,giveup
            assert(partsize==0);
            //default partition
            parts.push_back(attr_partinfo(attrnum,basepath,"MAINPART"));
            strcpy(savepartname,"MAINPART");
            if(parts[0].getsessionid()==0)
                parts[0].setsessionid(sess_id);
            return parts.back();
        }
        for(int i=0; i<partsize; i++) {
            if(parts[i].ispart(partname==NULL?savepartname:partname)) {
                //conn_id=parts[i].getsessionid();
                if(parts[i].getsessionid()==0)
                    parts[i].setsessionid(sess_id);
                if(partname!=NULL)
                    strcpy(savepartname,partname);
                return parts[i];
            }
        }
        parts.push_back(attr_partinfo(attrnum,basepath,partname==NULL?savepartname:partname));
        if(partname!=NULL)
            strcpy(savepartname,partname);
        if(parts.back().getsessionid()==0)
            parts.back().setsessionid(sess_id);
        return parts.back();
    }

    bool IsValidPartLoad()
    {
        //load partition name set by client
        return savepartname[0]!=0 && conn_id!=-1;
    }
    // get table packs(all partition) for reading
    uint getpacks()
    {
        return total_packs;
    }

    // 数据包编号变更记录:packvars
    //       2个值一组: 1,end_packs,2,delete packs
    bool Truncate(int sid,int *packvars,std::set<std::string> &rebuildparts,const bool for_mysqld_merge = false)
    {
        if(!for_mysqld_merge) { // fix dma-1365 , add by liujs
            if(sid!=trunc_sid) return false;
        }
        return RemovePartition(truncpartname,packvars,rebuildparts);
    }

    bool RemovePartition(const char *partname,int *packvars,std::set<std::string> &rebuildparts)
    {
        int partsize=parts.size();
        for(int i=0; i<partsize; i++) {
            if(strcmp(parts[i].name(),partname)==0) {
                for(int j=0; j<partsize; j++) {
                    if(j!=i) parts[j].removeby(parts[i],packvars,rebuildparts);
                }
                parts.erase(parts.begin()+i);
                return true;
            }
        }
        // invalid partname or empty parts;
        return false;
    }

    void LoadNFBlocks(std::map<int,int> &nfblocks)
    {
        nfblocks.clear();
        int partsize=parts.size();

        for(int i=0; i<partsize; i++) {
            //why lastpackobjs equal actual obj+1?
            // this not take effect:
            // RCAttrLoadBase::LoadData
            // partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);
            // where dpn.no_objs is actual obj-1 becuase:
            //  dpn.no_objs = ushort(cur_obj + load_obj - 1);
            parts[i].AppendNFBlocks(nfblocks);
            //nfblocks[parts[i].getlastpack()]=parts[i].getlastpackobjs()-1;
        }
    }

    void load(const bool from_truncate_partition = false )
    {
        conn_id=-1;
        trunc_sid=-1;
        savepartname[0]=0;
        total_packs=0;
        struct stat stat_info;
        if(0 == stat(getglobalfile().c_str(), &stat_info)) {
            if(stat_info.st_size > 0) { // fix DMA-1152
                IBFile loadinfo;
                loadinfo.OpenReadOnly(getglobalfile());
                char buf[8];
                loadinfo.Read(buf,8);
                if(memcmp(buf,"LOADCTRL",8)!=0) {
                    throw DatabaseRCException("Wrong load ctrl file.");
                }
                loadinfo.Read(&conn_id,sizeof(int));
                loadinfo.Read(&lastload,sizeof(short));
                short len=0;
                loadinfo.Read(&len,sizeof(short));
                if(len>sizeof(savepartname))
                    throw DatabaseRCException("Invalid load control file.");
                loadinfo.ReadExact(&savepartname,len);
                savepartname[len]=0;
                //TODO:  add return code and message
            } else { // fix DMA-1152
                rclog << lock << "warning: ["<<getglobalfile()<<"] is an empty file !!!  remove it."<< unlock;
                CleanCtrFile();
            }
        }
        std::string fn = getfile();
        if(from_truncate_partition) {
            fn += ".ntrunc";
        }
        if(!DoesFileExist(fn)) return;
        IBFile partf;
        partf.OpenReadOnly(fn);
        char buf[8];
        partf.Read(buf,8);
        bool oldversion=false;
        if(memcmp(buf,"PARTINFO",8)==0) oldversion=true;
        else if(memcmp(buf,"PARTIF15",8)!=0) {
            throw DatabaseRCException("Wrong partinfo file version.");
        }
        int _attrnum=-1;
        partf.Read(&_attrnum,sizeof(int));
        if(_attrnum>=0) attrnum=_attrnum;
        int len=0;

        partf.Read(&len,sizeof(short));
        parts.clear();
        for(int i=0; i<len; i++) {
            parts.push_back(attr_partinfo());
            parts.back().load(partf,oldversion);
            total_packs+=parts.back().getpacks();
        }

    }

    void save(const bool from_truncate_partition = false)
    {

        IBFile partf;
        long _write_size = 0;
        std::string fn = getfile();

        if(from_truncate_partition) {
            fn += std::string(".ntrunc");
        }

        partf.OpenCreateEmpty(fn);
        char buf[8];
        partf.Write("PARTIF15",8);
        _write_size+=8;

        partf.Write(&attrnum,sizeof(int));
        _write_size+=sizeof(int);

        short len=parts.size();
        partf.Write(&len,sizeof(short));
        _write_size+=sizeof(short);

        for(int i=0; i<len; i++) {
            parts[i].save(partf,_write_size);
        }

        rclog << lock << "Debug info : Partinfo save :"<<getfile()<<" , write file size ["<< _write_size << "] bytes" <<unlock;

    }

    // load truncate info before call this method.
    void CreateTruncateFile()
    {

        IBFile partf;

        long _write_size = 0;

        std::string newfile=getfile()+".ntrunc";
        partf.OpenCreateEmpty(newfile);
        char buf[8];
        partf.Write("PARTIF15",8);

        _write_size+=8;

        partf.Write(&attrnum,sizeof(int));
        _write_size+=sizeof(int);

        int len=parts.size();
        
        int len_fix = len;
        for(int i=0; i<len; i++){
            if(parts[i].ispart(truncpartname) ) {
                len_fix -=1;
            }
        }
        partf.Write(&len_fix,sizeof(short));
        _write_size+=sizeof(short);

        for(int i=0; i<len; i++) {
            if(parts[i].ispart(truncpartname) ) continue;
            parts[i].save(partf,_write_size);
        }
        rclog << lock << "Debug info : Partinfo save :"<<getfile()<<" , write file size ["<< _write_size << "] bytes" <<unlock;
    }

    bool CommitTruncate()
    {
        std::string newfile=getfile()+".ntrunc";
        if(DoesFileExist( newfile) && DoesFileExist(getfile())) {
            RemoveFile( getfile());
            RenameFile(getfile()+".ntrunc", getfile());
        }
        load();
        //RemoveFile(GetTruncateControlFile());
        return true;
    }

    bool RollbackTruncate()
    {
        std::string newfile=getfile()+".ntrunc";
        if(DoesFileExist( newfile))  {
            RemoveFile(newfile);
        }
        //RemoveFile(GetTruncateControlFile());
        return true;
    }

};

#endif
