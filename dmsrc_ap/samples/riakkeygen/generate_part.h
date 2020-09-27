#ifndef GENERATE_PART_DEF_H
#define GENERATE_PART_DEF_H

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

std::string PartFileName(const std::string& path,const int& attr_num) {
    char filename[300];
    std::string fn;
    sprintf(filename,"%s/PART_%05d.ctl",path.c_str(),attr_num);
    fn=filename;
    return fn;
}

//-----------------------------------------------------------------------------
// PART_xxxxx.ctl 文件结构
typedef struct stru_partinfo{
    bool version15;                    // 版本
    int attrid;                        // 列id
    int parts;                        // 分区数
    int tabid;

    typedef struct stru_part
    {
        char partname[300];
        int  namelen;

        int firstpack;                
        int lastpack;    
        int lastpackobj;

        int lastsavepack;
        int lastsavepackobj;
        int savesessionid;
        
        long lastfileid;
        int savepos;
        int lastsavepos; 

        typedef struct stru_part_section     // 每一个分区存储的包的信息
        {
            int packstart;                
            int packend;    
            int lastobjs;
            stru_part_section(){
                packstart = packend = lastobjs=0;
            }        
        }stru_part_section,*stru_part_section_ptr;

        // 分区内部的部分包的位置
        std::vector<stru_part_section>    part_section_list;    

        // ap版本的分区的文件列表
        std::vector<int> part_file_list;

        // 存入riak中的pnode列表
        std::vector<_uint64> riak_pnode_list;

        stru_part(){
            partname[0] = 0;
            namelen = 0;
            lastfileid=0;
            savepos = lastsavepos=-1;
            lastsavepack = lastsavepackobj = savesessionid=0;
            firstpack = lastpack = lastpackobj = 0;
            part_section_list.clear();
            part_file_list.clear();
            riak_pnode_list.clear();            
        }
            
    }stru_part,*stru_part_ptr;

    // 分区的信息
    std::vector<stru_part>  st_part_list;

    stru_partinfo(){
        version15 = true;
        attrid = 0;
        parts = 0;
        tabid = 0;
        st_part_list.clear();
    }

    // 加载AP版本的PART_xxxxx.ctl信息到内存
    int LoadApPartInfo(const char *filename,FILE* fp){
        version15=true;
        char buf[20];
        fread(buf,1,8,fp);
        if(memcmp(buf,"PARTIF15",8)!=0) {
           if(memcmp(buf,"PARTINFO",8)!=0) {
               ThrowWith("文件格式错误,'%s'.",filename);
           }
           version15=false;
        }
            
        fread(&attrid,sizeof(int),1,fp);
        fread(&parts,sizeof(short),1,fp);
        long total_objs=0;

        // 逐个分区读取到st_part_list
        st_part_list.clear();
         for(int i=0;i<parts;i++) {
             stru_part _st_part;
             fread(buf,1,8,fp);
             if(memcmp(buf,"PARTPARA",8)!=0) {
                 lgprintf("%d分区格式错误'%s'.",i+1,filename);
                 throw -1;
             }
             fread(&_st_part.namelen,sizeof(short),1,fp);
             fread(_st_part.partname,_st_part.namelen,1,fp);
             _st_part.partname[_st_part.namelen]=0;
             fread(&_st_part.firstpack,sizeof(uint),1,fp);
             fread(&_st_part.lastpack,sizeof(uint),1,fp);
             fread(&_st_part.lastpackobj,sizeof(uint),1,fp);
             fread(&_st_part.lastsavepack,sizeof(uint),1,fp);
             fread(&_st_part.lastsavepackobj,sizeof(uint),1,fp);
             fread(&_st_part.savesessionid,sizeof(uint),1,fp);
             fread(&attrid,sizeof(uint),1,fp);
             if(version15) {
                 fread(&_st_part.lastfileid,sizeof(uint),1,fp);
                 fread(&_st_part.savepos,sizeof(int),1,fp);
                  fread(&_st_part.lastsavepos,sizeof(int),1,fp);
             }

             // ap版本的分区的文件列表
             int vectsize;
             fread(&vectsize,sizeof(uint),1,fp);
             for(int j=0;j<vectsize;j++) {
                 int fileid=0;
                 fread(&fileid,sizeof(uint),1,fp);
                _st_part.part_file_list.push_back(fileid);
             }// for(int j=0;j<vectsize;j++) {

             // part section 信息读取到内存
             int packsize;
             fread(&packsize,sizeof(uint),1,fp);
             for(int j=0;j<packsize;j++) {
                stru_part::stru_part_section st_part_section;
                fread(&st_part_section.packstart,sizeof(uint),1,fp);
                fread(&st_part_section.packend,sizeof(uint),1,fp);
                fread(&st_part_section.lastobjs,sizeof(uint),1,fp);

                _st_part.part_section_list.push_back(st_part_section);
             } //  for(int j=0;j<packsize;j++) {

             st_part_list.push_back(_st_part);        
             
        } // end for(int i=0;i<parts;i++) { 
        
    }// end LoadApPartInfo

    // 通过包号获取分区名称
    char * GetPartName(const int pack_no) {
        assert(parts >0 && parts == st_part_list.size());
        for(int i=0;i<st_part_list.size();i++){
            for(int j=0;j<st_part_list[i].part_section_list.size();j++){
                if(pack_no>=st_part_list[i].part_section_list[j].packstart && pack_no<=st_part_list[i].part_section_list[j].packend ){
                    return st_part_list[i].partname;
                }            
            }
        }
        assert(0);
    }

    // 将priaknode添加到pack_no对应的分区内
    bool AddRaikPnode(const int pack_no,int64 priaknode)
    {
        assert(parts >0 && parts == st_part_list.size());
        for(int i=0;i<st_part_list.size();i++){
            for(int j=0;j<st_part_list[i].part_section_list.size();j++){
                if(pack_no>=st_part_list[i].part_section_list[j].packstart && pack_no<=st_part_list[i].part_section_list[j].packend ){
                    int exist_num = 0;
                    exist_num = std::count(st_part_list[i].riak_pnode_list.begin(),st_part_list[i].riak_pnode_list.end(),priaknode);
                    if(exist_num<=0){
                        st_part_list[i].riak_pnode_list.push_back(priaknode);
                        st_part_list[i].lastfileid = priaknode;
                    }
                }            
            }
        }
    }

    // 保存PART_xxxxx.ctl信息到riak版本中
    int SaveRiakPartInfo(const char* filename,FILE* fp){
        char buf[20];
        strcpy(buf,"PARTIF15");
        fwrite(buf,1,8,fp);
                  
        fwrite(&attrid,sizeof(int),1,fp);
        fwrite(&parts,sizeof(short),1,fp);
        
        // 逐个分区从st_part_list中写道文件中
         for(int i=0;i<parts;i++) {
             strcpy(buf,"PARTPARA");
             fwrite(buf,1,8,fp);

             fwrite(&st_part_list[i].namelen,sizeof(short),1,fp);
             fwrite(st_part_list[i].partname,st_part_list[i].namelen,1,fp);
             fwrite(&st_part_list[i].firstpack,sizeof(uint),1,fp);
             fwrite(&st_part_list[i].lastpack,sizeof(uint),1,fp);
             fwrite(&st_part_list[i].lastpackobj,sizeof(uint),1,fp);
             fwrite(&st_part_list[i].lastsavepack,sizeof(uint),1,fp);
             fwrite(&st_part_list[i].lastsavepackobj,sizeof(uint),1,fp);
             fwrite(&st_part_list[i].savesessionid,sizeof(uint),1,fp);
             fwrite(&attrid,sizeof(uint),1,fp);
             if(version15) {
                 fwrite(&st_part_list[i].lastfileid,sizeof(long),1,fp);
                 fwrite(&st_part_list[i].savepos,sizeof(int),1,fp);
                 fwrite(&st_part_list[i].lastsavepos,sizeof(int),1,fp);
             }

             // ap版本的分区的文件列表
             int vectsize = st_part_list[i].riak_pnode_list.size();
             fwrite(&vectsize,sizeof(uint),1,fp);
             for(int j=0;j<vectsize;j++) {
                 _uint64 riak_pnode=st_part_list[i].riak_pnode_list[j];
                 fwrite(&riak_pnode,sizeof(long),1,fp);
             }// for(int j=0;j<vectsize;j++) {

             // part section 保存
             int packsize = st_part_list[i].part_section_list.size();
             fwrite(&packsize,sizeof(uint),1,fp);
             for(int j=0;j<packsize;j++) {
                 fwrite(&st_part_list[i].part_section_list[j].packstart,sizeof(uint),1,fp);
                 fwrite(&st_part_list[i].part_section_list[j].packend,sizeof(uint),1,fp);
                 fwrite(&st_part_list[i].part_section_list[j].lastobjs,sizeof(uint),1,fp);
             } //  for(int j=0;j<packsize;j++) {
             
        } // end for(int i=0;i<parts;i++) { 
        
    }// end SaveRiakPartInfo(const char filename,FILE* fp)
    
}stru_partinfo,*stru_partinfo_ptr;


//------------------------------------------------------------------------------------
typedef enum ABSwtich_tag { ABS_A_STATE, ABS_B_STATE }  ABSwitch;

#define INVALID_TRANSACTION_ID   0xFFFFFFFF

#define AB_SWITCH_FILE_NAME   "ab_switch"

class ABSwitcher
{
public:
    ABSwitch GetState(std::string const& path);

    static const char* SwitchName(ABSwitch value );

private:
    void GenerateName(std::string& name, std::string const& path);

};


ABSwitch ABSwitcher::GetState(std::string const&  path)
{
    std::string name;
    GenerateName(name, path);
    struct stat st;
    if(stat(name.c_str(),&st)){ // 成功返回0
        return ABS_A_STATE;
    }    
    return ABS_B_STATE;
}

void ABSwitcher::GenerateName(std::string& name, std::string const& path)
{
    name = path;
    name += "/";
    name += AB_SWITCH_FILE_NAME;
}

const char* ABSwitcher::SwitchName( ABSwitch value )
{
    if (value == ABS_A_STATE) return "ABS_A_STATE";
    return "ABS_B_STATE";
}


#endif
