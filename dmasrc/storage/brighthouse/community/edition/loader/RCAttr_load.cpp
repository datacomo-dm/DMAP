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

#include "common/CommonDefinitions.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "core/WinTools.h"
#include "RCAttr_load.h"
#include "loader/NewValueSet.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
using namespace std;

RCAttrLoad::RCAttrLoad(int a_num,int t_num, string const& a_path,int conn_mode,unsigned int s_id, bool loadapindex,DTCollation collation) throw(DatabaseRCException)
    : RCAttrLoadBase(a_num, t_num, a_path, conn_mode, s_id,loadapindex, collation )
{
}

RCAttrLoad::~RCAttrLoad()
{
    LogWarnigs();
}



int RCAttrLoad::Save(bool for_insert)
{
    if(current_state!=1) {
        rclog << lock << "Error: cannot save. It is read only session." << unlock;
        throw;
    }
    LoadPackInfo();

    if(!for_insert) { // 不是insert的时候,必须执行,insert的时候在RCAttrLoadBase::LoadData函数中的pack包的force_saveing_pack已经保存操作。
        int npack=NoPack();

        attr_partinfo &curpart=partitioninfo->getsavepartinfo(NULL);
        //uint lastpack=partitioninfo->GetLastPack()-packs_omitted;
        uint lastpack=curpart.getsavepack()-packs_omitted;
        if(npack-packs_omitted>lastpack+1) lastpack++;
        for(uint i=lastpack; i<npack-packs_omitted; i++) {
            SavePack(i);
        }
    }

    if(rsi_hist_update || rsi_cmap_update)
        SaveRSI();
    BHASSERT(FileFormat()==10, "should be 'file_format==10'");
    SaveDPN();
    SaveHeader();
    return 0;
}


void RCAttrLoad::SavePackIndex(DMThreadData *pl,int n)
{
    RCAttrLoad* rcattr = this;
    _uint64 obj_start = (_uint64(n) << 16);
    _uint64 obj_stop = obj_start + rcattr->dpns[n].GetNoObj();
    _uint64 obj = /*new_prefix ? 0 :*/obj_start;
    bool ispacks=PackType() == PackS;

    apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId());
    if(pindex->SetSavePack(n)) {
        for(; obj < obj_stop; obj++)
            if(!IsNull(obj)) {
                if(ispacks) {
                    RCBString str=GetNotNullValueString(obj);
                    const char *putvalue=str.Value();
                    int len=str.size();
                    // fix dma-1384
                    #if 0
                    leveldb::Slice keyslice(putvalue,len);
                    pindex->Put(keyslice,n+packs_omitted);
                    #else
                    std::string strv(putvalue,len);
                    pindex->Put(strv.c_str(),n+packs_omitted);
                    #endif
                } else
                    pindex->Put(GetNotNullValueInt64(obj),n+packs_omitted);
            }
    }
}

int RCAttrLoad::DoSavePack(int n,bool reset_pack)       // WARNING: assuming that all packs with lower numbers are already saved!
{
    if(current_state != 1) {
        rclog << lock << "Error: cannot save. Read only session." << unlock;
        throw;
    }
    LoadPackInfo();

    #if 1 // 校验dosavepack的文件的偏移量
    const char* pcheck_file_addr = getenv("DM_CHECK_PACK_ADDR");
    if(pcheck_file_addr != NULL && strlen(pcheck_file_addr) == 1){
        if(n>1){
            if(dpns[n].pack_file>=1 && (dpns[n].pack_file == dpns[n-1].pack_file) && 
                dpns[n].is_stored && dpns[n-1].is_stored){
                if(dpns[n].pack_addr < dpns[n-1].pack_addr ){
                    rclog << lock << "Error:RCAttrLoad::DoSavePack check pack_addr error ." << unlock;
                    assert(0);
                }                
            }
        }
    }    
    #endif

    RCAttrLoad* rcattr = this;
    if(dpns[n].pack && dpns[n].pack->UpToDate())
        return 2;
    // process packindex before empty_pack check in case of all value in pack identity to a same value(equals to min/max)
    // Update pack index
    // check dpns[n].pack->UpToDate() to bypass?
    // parallel process packindex fill data
    if(!skip_DoSavePack_packidx) {
        if(ct.IsPackIndex() && dpns[n].pack_mode!=PACK_MODE_EMPTY && dpns[n].pack_file != PF_NULLS_ONLY) {
            pi_thread.LockOrCreate()->Start(boost::bind(&RCAttrLoad::SavePackIndex,this,_1,n)); // fix dma-715 //SavePackIndex(NULL,n);
        }
    }

    if(!skip_DoSavePack_packidx) {
        if(!dpns[n].pack || dpns[n].pack->IsEmpty() || dpns[n].pack_mode == PACK_MODE_TRIVIAL || dpns[n].pack_mode == PACK_MODE_EMPTY) {
            if(PackType() == PackS && GetDomainInjectionManager().HasCurrent()) {
                AddOutliers(0);
            }
            pi_thread.WaitAllEnd();
            return 0;
        }
    }


    CompressionStatistics stats = rcattr->dpns[n].pack->Compress(rcattr->GetDomainInjectionManager());

    if(rcattr->PackType() == PackS && rcattr->GetDomainInjectionManager().HasCurrent())
        rcattr->AddOutliers(stats.new_no_outliers - stats.previous_no_outliers);

    //////////////// Update RSI ////////////////

    rcattr->UpdateRSI_Hist(n, rcattr->dpns[n].GetNoObj());      // updates pack n in RSIndices (NOTE: it may be executed in a random order of packs),
    rcattr->UpdateRSI_CMap(n, rcattr->dpns[n].GetNoObj());      // but does not save it to disk. Use SaveRSI to save changes (after the whole load)



    bool last_pack = false;
    if (rcattr->dpns[n].pack_file != PF_NOT_KNOWN && rcattr->dpns[n].pack_file != PF_NULLS_ONLY && rcattr->dpns[n].pack_file != PF_NO_OBJ
        && rcattr->dpns[n].pack_file>=0) {
        //int pack_loc = rcattr->dpns[n].pack_file % 2;
        //DMA-614 abs switch error ,no recover
        // 文件顺序编号(改造前不同会话按奇偶编号)
        if (rcattr->dpns[n].pack_file==GetSaveFileLoc(GetCurSaveLocation())
            || (GetSavePosLoc(GetCurSaveLocation())==0 && rcattr->dpns[n].pack_file==GetSaveFileLoc(GetCurSaveLocation())-1)) {
            // check file size to see if writing the last datapack (overwrite it)
            // or not (leave it and append a new version, so the following datapacks are not overwritten)
            try {
                IBFile fattr;
                fattr.OpenCreate(AttrPackFileName(n));
                fattr.Seek(0, SEEK_END);
                uint fsize = fattr.Tell();
                if (fsize == rcattr->dpns[n].pack_addr + rcattr->dpns[n].pack->PreviousSaveSize())
                    last_pack = true;
            } catch(DatabaseRCException& e) {
                rclog << lock << "Error: cannot get filesize " << AttrPackFileName(n) << ". " << GetErrorMessage(errno) << unlock;
                throw;
            }
        }
    }
    if (last_pack) {
        //DMA-614 abs switch error ,no recover
        // 文件顺序编号(改造前不同会话按奇偶编号)
        if (rcattr->dpns[n].pack_file != GetSaveFileLoc(GetCurSaveLocation()) ) {
            // last pack, but from the wrong file (switching session)
            SetSaveFileLoc(1 - GetCurSaveLocation(), rcattr->dpns[n].pack_file);
            SetSavePosLoc(1 - GetCurSaveLocation(), rcattr->dpns[n].pack_addr);
            rcattr->dpns[n].pack_file = GetSaveFileLoc(GetCurSaveLocation());
            rcattr->dpns[n].pack_addr = GetSavePosLoc(GetCurSaveLocation());
        }
    } else {
        // new package
        rcattr->dpns[n].pack_file=GetSaveFileLoc(GetCurSaveLocation());
        rcattr->dpns[n].pack_addr=GetSavePosLoc(GetCurSaveLocation());
    }
    ////////////////////////////////////////////////////////
    // Now the package has its final location.
    if(rcattr->dpns[n].pack_file >= rcattr->GetTotalPackFile()) // find the largest used file number
        rcattr->SetTotalPackFile(rcattr->dpns[n].pack_file + 1);
    BHASSERT(rcattr->dpns[n].pack_file >= 0, "should be 'rcattr->dpns[n].pack_file >= 0'");
    uint pack_des = rcattr->dpns[n].pack_addr;
    if(rcattr->dpns[n].pack && !rcattr->dpns[n].pack->IsEmpty())
        rcattr->SetSavePosLoc(rcattr->GetCurSaveLocation(), rcattr->dpns[n].pack_addr
                              + rcattr->dpns[n].pack->TotalSaveSize());
    if(rcattr->GetSavePosLoc(rcattr->GetCurSaveLocation()) > rcattr->file_size_limit) { // more than 1.5 GB - start the next package from the new file
        int cur_save_loc = rcattr->GetCurSaveLocation();
        rcattr->SetSavePosLoc(cur_save_loc, 0);
        int save_location = rcattr->GetSaveFileLoc(cur_save_loc)+1;
        /////TODO: 测试正常后,去掉这段代码
        if(DoesFileExist( rcattr->AttrPackFileNameDirect(rcattr->attr_number, save_location, rcattr->path))) {
            // save isolated file
            //
            string packfilename=rcattr->AttrPackFileNameDirect(rcattr->attr_number, save_location, rcattr->path);
            RenameFileF(packfilename,packfilename+".isolate");
            rclog << lock << "RCAttrLoad::DoSavePack Find isolated file,need to clear manually:" << packfilename<<".isolate"<<unlock;
        }
        /////
        rcattr->SetSaveFileLoc(cur_save_loc, save_location );

        rclog << lock << "Data file full,create new:" << save_location<<"attr_number:"<<rcattr->attr_number<<unlock;
    }
    // update save parition
    if(partitioninfo) {
        attr_partinfo &curpart=partitioninfo->getsavepartinfo(NULL);
        int cur_file=rcattr->dpns[n].pack_file;
        // has appended at CreateNewPacket
        //curpart.newpack(n+packs_omitted);
        curpart.setsavepos(n+packs_omitted,rcattr->dpns[n].GetNoObj());
        if(cur_file>=0 && cur_file>curpart.lastfile())
            curpart.append(cur_file);
        int cur_save_loc = rcattr->GetCurSaveLocation();
        curpart.setsavefilepos(rcattr->GetSavePosLoc(rcattr->GetCurSaveLocation()));
    }
    ////////////////////////////////////////////////////////
    string file_name;

    try {
        IBFile fattr;
        fattr.OpenCreate(file_name = rcattr->AttrPackFileName(n));
        fattr.Seek(pack_des, SEEK_SET);
        rcattr->dpns[n].pack->Save(&fattr, rcattr->GetDomainInjectionManager());
        fattr.Close();
    } catch(DatabaseRCException& e) {
        rclog << lock << "Error: can not save data pack to " << file_name << ". " << GetErrorMessage(errno) << unlock;
        throw;
    }
    
    if(!skip_DoSavePack_packidx) {
        pi_thread.WaitAllEnd();
    }

    // 只有从bhloader进入和insert进入的时候,再能在保存完成数据包后将其释放
    // 多节点排序过程的数据包不能进行释放,因为该包如果是非满的还需要进行合并用
    // ADD BY LIUJS
    if(reset_pack) {
        rcattr->UnlockPackFromUse(n);
    } else {
        if(rcattr->dpns[n].GetNoObj() == 0xffff+1) {
            rcattr->UnlockPackFromUse(n);
        }
    }
    return 0;
}

