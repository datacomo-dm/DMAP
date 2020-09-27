/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include <boost/bind.hpp>

#include "system/BHToolkit.h"
#include "edition/local.h"
#include "edition/loader/RCAttr_load.h"
#include "DataLoader.h"
#include "RCTable_load.h"
#include "system/Buffer.h"
#include "core/tools.h"

using namespace std;

RCTableLoad::RCTableLoad(string const& a_path, int current_state, vector<DTCollation> charsets) throw(DatabaseRCException)
    :   RCTableImpl(a_path, charsets, current_state, RCTableImpl::OpenMode::FOR_LOADER), no_loaded_rows(0)
{
    if(no_attr > 0) {
        try {
            LoadAttribute(0,current_state == 1 ? false:true);
        } catch(DatabaseRCException&) {
            throw;
        }

        m_update_time = a[0]->UpdateTime();
    } else
        m_update_time = m_create_time;
}

void RCTableLoad::WaitForSaveThreads()
{
    for(int at = 0; at < no_attr; at++) {
        if(a[at])
            ((RCAttrLoad*)a[at])->WaitForSaveThreads();
    }
}
void RCTableLoad::PrepareLoadData(uint connid)
{
    LockPackInfoForUse();
    for(uint i = 0; i < NoAttrs(); i++) {
        LoadAttribute(i);
        a[i]->SetPackInfoCollapsed(true);
        // true: load by insert sql
        ((RCAttrLoad*)a[i])->LoadPartionForLoader(connid,session,true);
        //((RCAttrLoad*)a[i])->SetLargeBuffer(&rclb);
        ((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
        // prevent clear index data previous load: IsMerge TRUE
        ((RCAttrLoad*)a[i])->LoadPackIndexForLoader(((RCAttrLoad*)a[i])->GetLoadPartName(),session,true);
        //a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
    }
}

void RCTableLoad::LoadData(uint connid,NewValuesSetBase **nvs)
{
    vector<RCAttrLoad*> attrs;
    LockPackInfoForUse();
    for(uint i = 0; i < NoAttrs(); i++)
        attrs.push_back((RCAttrLoad*)a[i]);
    try {
        LargeBuffer bf;
        IOParameters iop;
        auto_ptr<DataLoader> loader = DataLoaderImpl::CreateDataLoader(attrs, bf, iop);
        no_loaded_rows = loader->Proceed(nvs);
    } catch(...) {
        WaitForSaveThreads();
        UnlockPackInfoFromUse();
        for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
        throw;
    }
    WaitForSaveThreads();
    UnlockPackInfoFromUse();
}



void RCTableLoad::WritePackIndex(const bool ltsession)   // �ֲ�ʽֱ�Ӻϲ��������ļ�����,���������ڴ�����д��leveldb��
{

    rclog << lock << "Note : write packindex begin"<<unlock;
    try {
        //mulit-thread merge index
        mytimer _tm_write_packindex;
        _tm_write_packindex.Restart();
        rclog << lock << "RCTableLoad::WritePackIndex()  write packindex begin ..."<<unlock;

        DMThreadGroup ptlist;
        for(int i = 0; i < NoAttrs(); i++) {
            if((RCAttrLoad*)a[i] && (RCAttrLoad*)a[i]->Type().IsPackIndex()) {
                a[i]->Set_LoadDataFromTruncatePartition(ltsession);
                ptlist.LockOrCreate()->Start(boost::bind(&RCAttrLoadBase::SavePartitionPackIndex,(RCAttrLoadBase*)a[i],_1));
            }
        }
        ptlist.WaitAllEnd();

        _tm_write_packindex.Stop();
        rclog << lock << "RCTableLoad::WritePackIndex()  write packindex end ,use ["<<_tm_write_packindex.GetTime()<<"] (S)"<<unlock;

    }
    catch(char* e){
            rclog << lock << "ERROR: Write temporary packindex,throw char* msg["<<e<<"]."<<unlock; 
    
            char err_msg[1024];
            sprintf(err_msg,"ERROR: Write temporary packindex,throw char* msg[%s]",e);
            throw InternalRCException(std::string(err_msg));
    
        }catch(int e){
            rclog << lock << "ERROR: Write temporary packindex,throw int msg["<<e<<"]."<<unlock; 
    
            char err_msg[1024];
            sprintf(err_msg,"ERROR: Write temporary packindex,throw char* msg[%d]",e);
            throw InternalRCException(std::string(err_msg));
    
        }catch(...) {
            rclog << lock << "ERROR: Write temporary packindex "<<unlock;
    
            char err_msg[1024];
            sprintf(err_msg,"ERROR: Write temporary packindex,throw unknow error");
            throw InternalRCException(std::string(err_msg));
        }
    

    rclog << lock << "Note : write packindex end"<<unlock;

}


// ��ȡ�ύ���ݵĻỰID�ͺϲ����ݵ�·��
int RCTableLoad::get_merge_session_info(std::string& sessionid,
                                        std::string& mergepath,
                                        std::string& partname,
                                        _int64& rownums)
{

    char load_filename[300];
    sprintf(load_filename,"%s/loadinfo.ctl",path.c_str());

    int _conn_id=0;
    short _lastload = 0;
    char _tmp[256];
    char _session[32];
    struct stat stat_info;
    if(0 == stat(load_filename, &stat_info)) {
        if(stat_info.st_size > 0) { // fix DMA-1152
            IBFile loadinfo;
            loadinfo.OpenReadOnly(load_filename);
            char buf[8];
            loadinfo.Read(buf,8);
            if(memcmp(buf,"LOADCTRL",8)!=0) {
                throw DatabaseRCException("Wrong load ctrl file.");
            }
            loadinfo.Read(&_conn_id,sizeof(int));
            loadinfo.Read(&_lastload,sizeof(short));
            short len=0;

            // ��������
            loadinfo.Read(&len,sizeof(short));
            if(len>sizeof(_tmp))
                throw DatabaseRCException("Invalid load control file.");
            loadinfo.ReadExact(&_tmp,len);
            _tmp[len] = 0;
            partname = std::string(_tmp);

            // �ỰID
            loadinfo.Read(&len,sizeof(short));
            if(len>sizeof(_session))
                throw DatabaseRCException("Invalid load control file.");
            loadinfo.ReadExact(&_session,len);
            _session[len] = 0;
            sessionid = std::string(_session);

            // �ϲ�����·��
            loadinfo.Read(&len,sizeof(short));
            if(len>sizeof(_tmp))
                throw DatabaseRCException("Invalid load control file.");
            loadinfo.ReadExact(&_tmp,len);
            _tmp[len] = 0;
            mergepath = std::string(_tmp);

            // ��¼��
            rownums = 0;
            loadinfo.Read(&rownums,sizeof(rownums));

            return 0;
        } else { // fix DMA-1152
            rclog << lock << "warning: ["<<load_filename<<"] is an empty file !!!  remove it."<< unlock;
            return -1;
        }
    }
    return -1;
}


// �ֲ�ʽ�����������ݽ��кϲ�װ�����
int RCTableLoad::merge_table_from_sorted_data(bool st_session)
{

    std::string sessionid,mergepath,partname;
    _int64 rownums = 0;
    int ret = get_merge_session_info(sessionid,mergepath,partname,rownums);
    if(ret != 0){
        char load_filename[300];
        sprintf(load_filename,"%s/loadinfo.ctl",path.c_str());
        rclog << lock << "Error: get merge session info from file ["<<load_filename<<"] error."<< unlock;
        return -1;
    }

    std::vector<RCAttrLoad*> attrs;
    for(uint i = 0; i < NoAttrs(); i++) {
        LoadAttribute(i);
        ((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
        // a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
        attrs.push_back((RCAttrLoad*)a[i]);
    }
    LockPackInfoForUse();

    try
    {

        // ���н������ݺϲ�,TODO: ���߳��޸�
        rclog << lock << "Info:[merge session data start]:  merge sorted data to partition "
              << GetPath() << "-->"
              << partname <<" from " <<mergepath <<" . "<< unlock;
        
        mytimer _tm_merge_data;
        _tm_merge_data.Restart();

        const char* p_use_single_column_merge = getenv("DM_USE_SINGLE_COLUMN_MERGE");
        if(p_use_single_column_merge != NULL) {
            // ���Ժϲ�����
            if(st_session) { // �̻Ự���ݺϲ�����
                for(uint i = 0; i < NoAttrs(); i++) {
                    ((RCAttrLoad*)a[i])->merge_short_session_data(mergepath,sessionid,partname,rownums);
                }
            } else {
                for(uint i = 0; i < NoAttrs(); i++) {
                    ((RCAttrLoad*)a[i])->merge_long_session_data(mergepath,sessionid,partname,rownums);
                }
            }
        } else {
            // ���̲߳������кϲ�����
            DMThreadGroup ptlist;

            //>> begin: fix dma-1382: ֧�ֶ��̲߳������кϲ�����
            ptlist.SetThreadKey(ConnectionInfoOnTLS.GetKey());
            ptlist.SetThreadSpecific(&ConnectionInfoOnTLS.Get());
            //<< end 
            
            if(st_session) { // �̻Ự���ݺϲ�����
                for(uint i = 0; i < NoAttrs(); i++) {
                    ptlist.LockOrCreate()->StartInt(boost::bind(&RCAttrLoadBase::merge_short_session_data,
                                                    (RCAttrLoad*)a[i],mergepath,
                                                    sessionid,partname,rownums));
                }
            } else {

                for(uint i = 0; i < NoAttrs(); i++) {
                    ptlist.LockOrCreate()->StartInt(boost::bind(&RCAttrLoadBase::merge_long_session_data,
                                                    (RCAttrLoad*)a[i],mergepath,
                                                    sessionid,partname,rownums));
                }
            }
            ptlist.WaitAllEnd();
        }
        _tm_merge_data.Stop();
        rclog << lock << "Info,Merge sorted data use ["<<_tm_merge_data.GetTime()<<"] second."<<unlock; 
        
    }
    catch(InternalRCException& e){
        rclog << lock << "ERROR,Merge sorted data throw InternalRCException."<<unlock; 
        throw e;
    }
    catch(DatabaseRCException& e){
        rclog << lock << "ERROR,Merge sorted data throw DatabaseRCException."<<unlock; 
        throw e;
    }
    catch(...){        
        rclog << lock << "ERROR,Merge sorted data throw ..."<<unlock; 
        throw ;
    }
    
    // �᲻�����PART,DPN������ɹ��ˣ�packindex����ʧ�ܣ��������յ����ݴ����޷�����
    bool lt_session = !st_session;
    WritePackIndex(lt_session);

    // ����PART,DPN,ATTR�ļ�
    
    for(uint i = 0; i < NoAttrs(); i++) {
        a[i]->Set_LoadDataFromTruncatePartition(lt_session?true:false);
        a[i]->save_merge_table_header();
        a[i]->Set_LoadDataFromTruncatePartition(lt_session?false:false);
    }

    rclog << lock << "Info:[merge session data finish]:  merge sorted data to partition "
          << GetPath() << "-->"
          << partname <<" from " <<mergepath <<" . "<< unlock;
    
    return 0;
}


void RCTableLoad::LoadDataEnd()
{
    vector<RCAttrLoad*> attrs;
    LockPackInfoForUse();
    for(uint i = 0; i < NoAttrs(); i++)
        attrs.push_back((RCAttrLoad*)a[i]);
    try {
        LargeBuffer bf;
        IOParameters iop;
        auto_ptr<DataLoader> loader = DataLoaderImpl::CreateDataLoader(attrs, bf, iop);
        no_loaded_rows = loader->ProceedEnd();
    } catch(...) {
        WaitForSaveThreads();
        UnlockPackInfoFromUse();
        for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
        throw;
    }
    WaitForSaveThreads();
    UnlockPackInfoFromUse();
}

void RCTableLoad::LoadData(IOParameters& iop,uint connid)
{
    LargeBuffer rclb;

#ifndef __BH_COMMUNITY__ /* DATA PROCESSOR - <michal> to refactor !!!*/
    if(iop.GetEDF() == INFOBRIGHT_DF) {

        vector<RCAttrLoad*> attrs;
        for(uint i = 0; i < NoAttrs(); i++) {
            LoadAttribute(i);
            ((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
            a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
            attrs.push_back((RCAttrLoad*)a[i]);
        }
        LockPackInfoForUse();

        try {
            auto_ptr<DataLoader> loader = DataLoaderEnt::CreateDataLoader(attrs, rclb, iop);
            no_loaded_rows = loader->Proceed();
            //cerr << "loader->GetPackrowSize() = " << loader->GetPackrowSize() << endl;
            iop.SetPackrowSize(loader->GetPackrowSize());
            for(uint i = 0; i < NoAttrs(); i++)
                iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());

        } catch(...) {
            WaitForSaveThreads();
            UnlockPackInfoFromUse();
            for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
            throw;
        }

        WaitForSaveThreads();
        UnlockPackInfoFromUse();
        return;
    }
#endif

    if (!rclb.IsAllocated())
        throw OutOfMemoryRCException("Unable to create largebuffer due to insufficient memory.");

    vector<RCAttrLoad*> attrs;
    for(uint i = 0; i < NoAttrs(); i++) {
        LoadAttribute(i);
        a[i]->SetPackInfoCollapsed(true);

        ((RCAttrLoad*)a[i])->SetLargeBuffer(&rclb);
        ((RCAttrLoad*)a[i])->LoadPartionForLoader(connid,session);
        ((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
        ((RCAttrLoad*)a[i])->LoadPackIndexForLoader(((RCAttrLoad*)a[i])->GetLoadPartName(),session);
        a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
        attrs.push_back((RCAttrLoad*)a[i]);
    }
    LockPackInfoForUse();
    try {
        rclb.BufOpen(iop, READ);
        if((rclb.BufStatus() != 1 && rclb.BufStatus() != 4 && rclb.BufStatus() != 5))
            throw FileRCException("Unable to open " + (IsPipe(iop.Path()) ? string("pipe ") : string("file ")) + string(iop.Path()) + string("."));
        if(rclb.BufSize()) {
            auto_ptr<DataLoader> loader = DataLoaderImpl::CreateDataLoader(attrs, rclb, iop);
            no_loaded_rows = loader->Proceed();
        }
        //if(a[0]->IsLastLoad())
        //  MergePackIndex(connid);
        //else
        //  for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::ClearMergeDB, _1));
        rclb.BufClose();
        for(uint i = 0; i < NoAttrs(); i++)
            iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());
    } catch(...) {
        WaitForSaveThreads();
        rclb.BufClose();
        UnlockPackInfoFromUse();
        for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
        throw;
    }
    WaitForSaveThreads();
    UnlockPackInfoFromUse();
}

void RCTableLoad::LoadData(IOParameters& iop, Buffer& buffer)
{
    vector<RCAttrLoad*> attrs;
    for(uint i = 0; i < NoAttrs(); i++) {
        LoadAttribute(i);
        a[i]->SetPackInfoCollapsed(true);
        //((RCAttrLoad*)a[i])->LoadPartionForLoader(connid,session,true);
        //((RCAttrLoad*)a[i])->SetLargeBuffer(&rclb);
        ((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
        //((RCAttrLoad*)a[i])->LoadPackIndexForLoader(((RCAttrLoad*)a[i])->GetLoadPartName(),session);
        a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
        attrs.push_back((RCAttrLoad*)a[i]);
    }
    LockPackInfoForUse();
    try {
        auto_ptr<DataLoader> loader = DataLoader::CreateDataLoader(attrs, buffer, iop);
        loader->Proceed();
        for(uint i = 0; i < NoAttrs(); i++)
            iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());
    } catch(...) {
        WaitForSaveThreads();
        UnlockPackInfoFromUse();
        for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
        throw;
    }
    WaitForSaveThreads();
    UnlockPackInfoFromUse();
}

RCTableLoad::~RCTableLoad()
{
    // Note:: there was not critical section destroy function in the windows version of this destructor. ???
    // we added the destroy in the linux port.
    //pthread_mutex_destroy(&synchr);
    WaitForSaveThreads();
}

inline void RCTableLoad::LoadAttribute(int attr_no,bool loadapindex)
{
    if(!a[attr_no]) {
        if(!a[attr_no]) {
            try {
                a[attr_no] = new RCAttrLoad(attr_no, tab_num, path, conn_mode, session,loadapindex, charsets[attr_no]);

                if (a[attr_no]->OldFormat()) {
                    a[attr_no]->UpgradeFormat();
                    delete a[attr_no];
                    a[attr_no] = new RCAttrLoad(attr_no, tab_num, path, conn_mode, session,loadapindex, charsets[attr_no]);
                }
            } catch(DatabaseRCException&) {
                throw;
            }
        }
    }
}

_int64 RCTableLoad::NoRecordsLoaded()
{
    return no_loaded_rows;
}
