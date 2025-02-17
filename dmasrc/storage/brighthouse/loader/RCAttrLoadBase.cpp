/* Copyright (C) 2005-2008 Infobright Inc. */

#include <sstream>
#include <boost/shared_ptr.hpp>

#include "common/CommonDefinitions.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "core/WinTools.h"
#include "RCAttrLoadBase.h"
#include "loader/NewValueSet.h"
#include "loader/BHLoader.h"
#include "core/DPN.h"
#include "core/DataPackImpl.h"
#include "types/RCDataTypes.h"
#include "edition/core/GlobalDataCache.h"
#include "edition/core/Transaction.h"
#include "loader/NewValuesSetBase.h"
#include <cr_class/cr_externalsort.h>

#include <glob.h>
#include <fcntl.h>
#include <algorithm>

using namespace std;
using namespace boost;

#define PACK_EXTERNAL_SIZE 1024*1024

RCAttrLoadBase::RCAttrLoadBase(int a_num,int t_num,string const& a_path,int conn_mode,unsigned int s_id,bool loadapindex, DTCollation collation) throw(DatabaseRCException)
    : RCAttr(a_num, t_num, a_path, conn_mode, s_id,loadapindex, collation), illegal_nulls(false), packrow_size(MAX_PACK_ROW_SIZE), no_outliers(TransactionBase::NO_DECOMPOSITION)
{
}

RCAttrLoadBase::RCAttrLoadBase(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, string const& path_ )
    :   RCAttr(a_num, a_type, a_field_size, a_dec_places, param, collation, compress_lookups, path_), illegal_nulls(false), packrow_size(MAX_PACK_ROW_SIZE), no_outliers(TransactionBase::NO_DECOMPOSITION)
{
}

RCAttrLoadBase::~RCAttrLoadBase()
{
    LogWarnigs();
}
void RCAttrLoadBase::LoadPackIndexForLoader(const char *partname,int sessionid,bool ismerge,int from_truncate_part/* 是否从删除的分区上进行加载 */)
{
    if(ct.IsPackIndex()) {
        if(ldb_index!=NULL) delete ldb_index;
        ldb_index=new AttrIndex(attr_number,path.c_str());
        ldb_index->LoadHeader(from_truncate_part);
        ldb_index->LoadForUpdate(partname,sessionid,ismerge);
    }
}

// appendmode : 不需要参数控制文件,在最后一个分区上装入(insert sql mode)
void RCAttrLoadBase::LoadPartionForLoader(uint connid,int sessionid,bool appendmode)
{
    //if(ct.IsPackIndex()) {
    if(partitioninfo!=NULL) delete partitioninfo;
    if(appendmode) { // bulid a virtual control file
        string ctlfile=path+"/loadinfo.ctl";
        if (DoesFileExist(ctlfile))
            RemoveFile( ctlfile);
        IBFile loadinfo;
        loadinfo.OpenCreate(ctlfile);
        loadinfo.WriteExact("LOADCTRL",8);
        loadinfo.WriteExact(&connid,sizeof(int));
        short len=0;
        loadinfo.WriteExact(&len,sizeof(short));
    }
    partitioninfo=new attr_partitions(attr_number,path.c_str());
    partitioninfo->SetSessionID(GetSessionId());
    if(!partitioninfo->checksession(connid)) {
        rclog << lock << "Internal error: load connection id not match: server got :" << connid << ",client request: " << partitioninfo->GetSaveID()<< unlock;
        throw DatabaseRCException("load connection id not match.");
    }
    //}
}

void RCAttrLoadBase::LoadPackInfoForLoader()
{
    if(!GetPackInfoCollapsed()) {
        return;
    }
    WaitForSaveThreads();
    BHASSERT(FileFormat()==10, "should be 'file_format==10'");
    string fn;
    if(GetDictOffset() != 0) {
        IBFile fattr;
        try {
            fn = AttrFileName(file);
            fattr.OpenReadOnly(fn);
            int fin_size = int(fattr.Seek(0, SEEK_END));
            char* buf_ptr = new char[(fin_size - GetPackOffset()) + 1];
            fattr.Seek(GetPackOffset(), SEEK_SET);
            fattr.ReadExact(buf_ptr, fin_size - GetPackOffset());

            if(!Type().IsLookup()) {
                if(GetDictOffset() != 0 && (fin_size - GetDictOffset()))
                    LoadDictionaries(buf_ptr);
            } else {
                if(process_type == ProcessType::BHLOADER)
                    dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree> (FTreeCoordinate(table_number,
                            attr_number), bind(&RCAttrLoadBase::LoadLookupDictFromFile, this));
                else
                    dic = ConnectionInfoOnTLS->GetTransaction()->GetFTreeForUpdate(FTreeCoordinate(table_number,
                            attr_number));

                if(!dic) {
                    dic = boost::shared_ptr<FTree> (new FTree());
                    LoadLookupDictFromFile();
                    ConnectionInfoOnTLS->GetTransaction()->PutObject(FTreeCoordinate(table_number, attr_number), dic);
                }
            }

            delete[] buf_ptr;
        } catch (DatabaseRCException&) {
            rclog << lock << "Internal error: unable to load column dictionary from " << fn << ". " << GetErrorMessage(
                      errno) << unlock;
            throw;
        }
    }

    if(NoPack() > 0) {
        int const ss(SplicedVector<DPN>::SPLICE_SIZE);
        packs_omitted = ((NoPack() - 1) / ss) * ss;
        // load start at last save pack:(align to ss)
        // dma-521 retore packs_ommitted
        //packs_omitted=(partitioninfo->getsavepartinfo(NULL).getsavepack()/ss)*ss;
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! ( packs_omitted % ss ) );
    } else
        packs_omitted = 0;
    if(NoPack()-packs_omitted > GetNoPackPtr()) {
        uint n_pack_ptrs = NoPack() - packs_omitted;
        if(n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE) {
            n_pack_ptrs += SplicedVector<DPN>::SPLICE_SIZE;
            n_pack_ptrs -= (n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE);
        }
        SetNoPackPtr(n_pack_ptrs);
        dpns.resize(GetNoPackPtr());
    }
    //设置装入位置
    int cur_save_loc = GetCurSaveLocation();
    partitioninfo->SetSessionID(GetSaveSessionId());
    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    std::vector<int> rmfiles;
    //partitioninfo->Rollback(connid,rmfiles);
    //restore uncommitted data
    // rollback last uncommited transaction
    //如果是同一个会话,则不回退,否则回退上次状态并update sessionid
    bool appendmode=savepart.preload(GetSaveSessionId(),rmfiles);
    for(int i=0; i<rmfiles.size(); i++) {
        //clear packfiles
        //TODO: 测试正常后,改为删除
        //RemoveFile(AttrPackFileNameDirect(attr_number,rmfiles[i],path));
        string packfilename=AttrPackFileNameDirect(attr_number, rmfiles[i], path);
        RenameFileF(packfilename,packfilename+".isolate");
        rclog << lock << "RCAttrLoadBase::LoadPackInfoForLoader()1.Find isolated file,need to clear manually:" << packfilename<<".isolate"<<unlock;
    }
    int newfileid=partitioninfo->GetLastFile()+1;
    if(savepart.isempty()||NoPack()<1 || savepart.getfiles()==0) {
        //新分区
        SetSaveFileLoc(cur_save_loc,newfileid);
        /////TODO: 测试正常后,去掉这段代码
        if(DoesFileExist( AttrPackFileNameDirect(attr_number, newfileid, path))) {
            // save isolated file
            //
            string packfilename=AttrPackFileNameDirect(attr_number, newfileid, path);
            RenameFileF(packfilename,packfilename+".isolate");
            rclog << lock << "RCAttrLoadBase::LoadPackInfoForLoader()2.Find isolated file,need to clear manually:" << packfilename<<".isolate"<<unlock;
        }
        /////

        SetSavePosLoc(cur_save_loc,0);
    } else  {
        //不连续
        if(savepart.getsavepack()!=NoPack()-1) {
            //TODO: 检查是否需要截断文件,删除上次未提交的数据
            SetSaveFileLoc(cur_save_loc,newfileid);
            /////TODO: 测试正常后,去掉这段代码
            if(DoesFileExist( AttrPackFileNameDirect(attr_number, newfileid, path))) {
                // save isolated file
                //
                string packfilename=AttrPackFileNameDirect(attr_number, newfileid, path);
                RenameFileF(packfilename,packfilename+".isolate");
                rclog << lock << "RCAttrLoadBase::LoadPackInfoForLoader()3.Find isolated file,need to clear manually:" << packfilename<<".isolate"<<unlock;
            }
            /////

            //truncate file
            SetSavePosLoc(cur_save_loc,0);
        } else {
            //新装入紧接上次最后装入的分区,如何连续装入,
            // 找到未提交的起始点!
            //为什么不用ABS机制保存的位置�?因为每个分区有自己的存储位置
            // last pack there is not real data
            _int64 fsize=0;
            GetFileSize(AttrPackFileNameDirect(attr_number,savepart.lastfile(),path),fsize);

            //如果已提交的最后一个包有pack_file(并且未满),则直接使用(未提交部分不会覆盖这个包的DPN数据(ABS)
            DPN &dpn(dpns[savepart.getlastpack()- packs_omitted]);
            if(dpn.pack_file>=0 && dpn.no_objs!=65535) {
                SetSaveFileLoc(cur_save_loc,dpn.pack_file);
                SetSavePosLoc(cur_save_loc,dpn.pack_addr);
            } else {
                SetSaveFileLoc(cur_save_loc,savepart.lastfile());
                //getsavefilepos() return -1,means a invalid pos
                SetSavePosLoc(cur_save_loc,savepart.getlastsavepos()<0?fsize:savepart.getlastsavepos());
            }
        }
    }

    //ExpandDPNArrays();
    SetPackInfoCollapsed(false);
}

void RCAttrLoadBase::ExpandDPNArrays()
{
    if((NoPack() - packs_omitted + 1) > GetNoPackPtr()) {
        // Create new package table (enlarged by one) and copy old values
        uint old_no_pack_ptr = GetNoPackPtr();
        uint n_pack_ptrs = (NoPack() - packs_omitted + 1);
        if(n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE) {
            n_pack_ptrs += SplicedVector<DPN>::SPLICE_SIZE;
            n_pack_ptrs -= (n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE);
        }
        SetNoPackPtr(n_pack_ptrs);
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !( ((NoPack() - packs_omitted + 1) > GetNoPackPtr()) || ( n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE ) ) );
        WaitForSaveThreads();

        dpns.resize(GetNoPackPtr());

        if(dpns.empty()) {
            rclog << lock << "Error: out of memory (" << GetNoPackPtr() - old_no_pack_ptr << " new packs failed). (25)"
                  << unlock;
            return;
        }
    }
}


DPN RCAttrLoadBase::LoadDataPackN(const DPN& source_dpn, NewValuesSetBase* nvs, _int64& load_min, _int64& load_max,
                                  int& load_nulls)
{
    DPN dpn(source_dpn);
    double& real_sum = *(double*) &dpn.sum_size;
    uint cur_nulls = dpn.no_nulls;
    uint cur_obj = dpn.GetNoObj();
    bool nulls_conferted = false;
    _int64 null_value = -1;
    int nonv = nvs->NoValues();
    bool is_real_type = ATI::IsRealType(TypeName());
    int load_obj = nvs->NoValues();

    if(!ATI::IsRealType(TypeName())) {
        load_min = PLUS_INF_64;
        load_max = MINUS_INF_64;
    } else {
        load_min = *(_int64*) &PLUS_INF_DBL;
        load_max = *(_int64*) &MINUS_INF_DBL;
    }

    for(int i = 0; i < nonv; i++) {
        if(nvs->IsNull(i)) {
            if(Type().GetNullsMode() == NO_NULLS) {
                nulls_conferted = true;
                illegal_nulls = true;
                _int64 v = 0;
                if(ATI::IsStringType(TypeName())) {
                    if(null_value == -1)
                        null_value = v = EncodeValue_T(ZERO_LENGTH_STRING, 1);
                    else
                        v = null_value;
                } else {
                    null_value = v = 0;
                }
            } else
                load_nulls++;
        }
    }

    if(!is_real_type) {
        _int64 tmp_sum;
        nvs->GetIntStats(load_min, load_max, tmp_sum);
        dpn.sum_size += tmp_sum;
        if(nulls_conferted) {
            if(load_min > null_value)
                load_min = null_value;
            if(load_max < null_value)
                load_max = null_value;
        }
    } else {
        double tmp_sum;
        nvs->GetRealStats(*(double*) &load_min, *(double*) &load_max, tmp_sum);
        real_sum += tmp_sum;
        if(nulls_conferted) {
            if(*(double*) &load_min > *(double*) &null_value)
                *(double*) &load_min = *(double*) &null_value;
            if(*(double*) &load_max < *(double*) &null_value)
                *(double*) &load_max = *(double*) &null_value;
        }
    }

    if((cur_nulls + load_nulls) == 0 && load_min == load_max && (cur_obj == 0 || (dpn.local_min == load_min
            && dpn.local_max == load_max))) {
        // no need to store any values - uniform package
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NOT_KNOWN; // will not be used
        dpn.local_min = load_min;
        dpn.local_max = load_max;
        // sum is already calculated
    } else if(load_nulls == load_obj && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
        // no need to store any values - uniform package (nulls only)
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NULLS_ONLY;
        dpn.local_min = PLUS_INF_64;
        dpn.local_max = MINUS_INF_64;
        dpn.sum_size = 0;
        // uniqueness status not changed
    } else {
        _uint64 new_max_val = 0;
        if(cur_obj == 0 || dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_mode == 3) {
            // new package (also in case of expanding so-far-uniform package)
            _uint64 uniform_so_far = 0;
            if(dpn.pack_file == PF_NULLS_ONLY) {
                uniform_so_far = (_uint64) NULL_VALUE_64;
                dpn.local_min = load_min;
                dpn.local_max = load_max;
            } else {
                if(ATI::IsRealType(TypeName())) {
                    uniform_so_far = dpn.local_min; // fill with uniform-so-far
                } else if(dpn.local_min > load_min)
                    uniform_so_far = _uint64(dpn.local_min - load_min);

                if(!ATI::IsRealType(TypeName())) {
                    if(cur_obj == 0 || dpn.local_min > load_min)
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || dpn.local_max < load_max)
                        dpn.local_max = load_max;
                } else {
                    if(cur_obj == 0 || *(double*) &(dpn.local_min) > *(double*) &(load_min))
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || *(double*) &(dpn.local_max) < *(double*) &(load_max))
                        dpn.local_max = load_max;
                }
            }

            BHASSERT(dpn.pack, "'dpn.pack' should not be null");
            if(ATI::IsRealType(TypeName())) {
                // full 64-bit scope
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL));
            } else {
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, new_max_val);
            }
            dpn.pack_file = PF_NOT_KNOWN;
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
            // Now fill the beginning of the table by so far uniform values (if there is any beginning)
            if(cur_obj > 0) {
                if(uniform_so_far != NULL_VALUE_64) {
                    if((dpn.local_min != dpn.local_max) || (load_nulls > 0))
                        for(int i = 0; i < (int) cur_obj; i++)
                            ((AttrPackN*) dpn.pack.get())->SetVal64(i, uniform_so_far);
                } else {
                    for(int i = 0; i < (int) cur_obj; i++) {
                        dpn.pack->SetNull(i);
                    }
                }
            }
        } else {
            // expand existing package
            //fix JIRA DMAPP-1100,packn rollbacked rows not reset while re-insert.
            ((AttrPackN*) dpn.pack.get())->TruncateObj(cur_obj);
            if(ATI::IsRealType(TypeName())) {
                if(*(double*) &dpn.local_min > *(double*) &load_min)
                    *(double*) &dpn.local_min = *(double*) &load_min;
                if(*(double*) &dpn.local_max < *(double*) &load_max)
                    *(double*) &dpn.local_max = *(double*) &load_max;
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL), 0);
            } else {
                _int64 offset = 0;
                if(dpn.local_min > load_min) {
                    offset = dpn.local_min;
                    offset -= load_min;
                    dpn.local_min = load_min;
                }
                if(dpn.local_max < load_max)
                    dpn.local_max = load_max;
                BHASSERT(dpn.pack, "'dpn.pack' should not be null");
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, new_max_val, offset);
            }
        }

        _uint64 v = 0;
        int obj = 0;
        bool isnull;
        for(int i = 0; i < load_obj; i++) {
            isnull = nvs->IsNull(i);
            if(isnull && Type().GetNullsMode() == NO_NULLS) {
                isnull = false;
                v = null_value;
            } else
                v = *(_uint64*) nvs->GetDataBytesPointer(i);
            obj = i + cur_obj;
            if(dpn.local_min == dpn.local_max && !ATI::IsRealType(TypeName())) {// special case: no data stored except nulls
                if(isnull)
                    dpn.pack->SetNull(obj);
            } else {
                if(!ATI::IsRealType(TypeName()))
                    v -= dpn.local_min;
                if(isnull) {
                    //((AttrPackN*)dpns[NoPack()-packs_omitted-1].pack)->SetVal64(obj, 0);
                    ((AttrPackN*) dpn.pack.get())->SetNull(obj);
                } else
                    ((AttrPackN*) dpn.pack.get())->SetVal64(obj, v);
            }
        }
    }
    return dpn;
}

bool RCAttrLoadBase::UpdateGlobalMinAndMaxForPackN(const DPN& dpn)
{
    return UpdateGlobalMinAndMaxForPackN(dpn.GetNoObj(), dpn.local_min, dpn.local_max, dpn.no_nulls);
}

bool RCAttrLoadBase::UpdateGlobalMinAndMaxForPackN(int no_obj, _int64 load_min, _int64 load_max, int load_nulls)
{
    bool is_exclusive = false;
    if(load_nulls != no_obj) {
        if(NoObj() == 0) {
            SetMinInt64(load_min);
            SetMaxInt64(load_max);
            is_exclusive = true;
        } else {
            if(!ATI::IsRealType(TypeName())) {
                is_exclusive = (GetMinInt64() > load_max || GetMaxInt64() < load_min);
                if(GetMinInt64() > load_min)
                    SetMinInt64(load_min);
                if(GetMaxInt64() < load_max)
                    SetMaxInt64(load_max);
            } else {
                _int64 a_min = GetMinInt64();
                _int64 a_max = GetMaxInt64();
                is_exclusive = (*(double*) (&a_min) > *(double*) (&load_max) || *(double*) (&a_max)
                                < *(double*) (&load_min));
                if(*(double*) (&a_min) > *(double*) (&load_min))
                    SetMinInt64(load_min);
                if(*(double*) (&a_max) < *(double*) (&load_max))
                    SetMaxInt64(load_max); // 1-level statistics
            }
        }
    }
    return is_exclusive;
}

bool RCAttrLoadBase::HasRepetitions(DPN & new_dpn, const DPN & old_dpn, int load_obj, int load_nulls,
                                    NewValuesSetBase *nvs)
{
    bool has_repetition = false;
    std::map<int,int> nfblocks;
    Filter f_val(new_dpn.local_max - new_dpn.local_min + 1,nfblocks);
    f_val.Reset();
    if(new_dpn.local_min == new_dpn.local_max) {
        if(old_dpn.GetNoObj() + load_obj - new_dpn.no_nulls - load_nulls > 1) // more than one non-null uniform object?
            new_dpn.repetition_found = true;
    } else {
        _uint64 v = 0;
        for(int i = 0; i < load_obj; i++) {
            if(nvs->IsNull(i) == false) {
                BHASSERT_WITH_NO_PERFORMANCE_IMPACT(*(_int64*)nvs->GetDataBytesPointer(i) <= new_dpn.local_max);
                BHASSERT_WITH_NO_PERFORMANCE_IMPACT(*(_int64*)nvs->GetDataBytesPointer(i) >= new_dpn.local_min);
                v = *(_int64*) nvs->GetDataBytesPointer(i) - new_dpn.local_min;
                if(f_val.Get(v)) {
                    has_repetition = true;
                    break;
                }
                f_val.Set(v);
            }
        }
    }
    return has_repetition;
}

void RCAttrLoadBase::UpdateUniqueness(const DPN& old_dpn, DPN& new_dpn, _int64 load_min, _int64 load_max,
                                      int load_nulls, bool is_exclusive, NewValuesSetBase* nvs)
{
    int load_obj = nvs->NoValues();

    if((old_dpn.no_nulls + load_nulls) == 0 && load_min == load_max && (old_dpn.GetNoObj() == 0 || (old_dpn.local_min
            == load_min && old_dpn.local_max == load_max))) {
        if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
            if(load_obj - load_nulls == 1)
                SetUnique(true); // only one new value, different than the previous
            else
                SetUnique(false); // at least two identical values found
        } else
            SetUniqueUpdated(false); // may be unique or not
    }

    if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
       && !ATI::IsRealType(TypeName()) // temporary limitations
       && _int64(new_dpn.local_max - new_dpn.local_min) > 0 && _int64(new_dpn.local_max - new_dpn.local_min)
       < 8000000) {
        new_dpn.repetition_found = HasRepetitions(new_dpn, old_dpn, load_obj, load_nulls, nvs);
        if(new_dpn.repetition_found)
            SetUnique(false); // else remains true
    } else
        SetUniqueUpdated(false);
}

void RCAttrLoadBase::UpdateUniqueness(DPN& dpn, bool is_exclusive)
{

    if(dpn.no_nulls == 0 && dpn.local_min == dpn.local_max) {
        if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
            if(dpn.GetNoObj() == 1)
                SetUnique(true); // only one new value, different than the previous
            else
                SetUnique(false); // at least two identical values found
        } else
            SetUniqueUpdated(false); // may be unique or not
    }

    if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
       && !ATI::IsRealType(TypeName()) // temporary limitations
       && _int64(dpn.local_max - dpn.local_min) > 0 && _int64(dpn.local_max - dpn.local_min) < 8000000) {
        if(dpn.repetition_found)
            SetUnique(false); // else remains true
    } else
        SetUniqueUpdated(false);
}

void RCAttrLoadBase::InitKNsForUpdate()
{
    if(!NeedToCreateRSIIndex()) {
        return;
    }
    if(process_type == ProcessType::DATAPROCESSOR) {
        if(PackType() == PackN) {
            if(rsi_hist_update == NULL) {
                rsi_hist_update = new RSIndex_Hist();
                rsi_hist_update->SetID(RSIndexID(RSI_HIST, table_number, attr_number));
            }
        } else if(rsi_cmap_update == NULL && !RequiresUTFConversions(ct.GetCollation())) {
            rsi_cmap_update = new RSIndex_CMap();
            rsi_cmap_update->SetID(RSIndexID(RSI_CMAP, table_number, attr_number));
        }
    } else {
        if(PackType() == PackN) {
            if(rsi_manager && rsi_hist_update == NULL)
                rsi_hist_update = (RSIndex_Hist*) rsi_manager->GetIndexForUpdate(RSIndexID(RSI_HIST, table_number,
                                  attr_number), GetCurReadLocation(),load_data_from_truncate_partition);
        } else {
            if(rsi_manager && rsi_cmap_update == NULL && !RequiresUTFConversions(ct.GetCollation()))
                rsi_cmap_update = (RSIndex_CMap*) rsi_manager->GetIndexForUpdate(RSIndexID(RSI_CMAP, table_number,
                                  attr_number), GetCurReadLocation(),load_data_from_truncate_partition);
        }
    }
}

bool RCAttrLoadBase::PreparePackForLoad()
{
    bool new_pack_created = false;
    /* adjust partition loading */
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);
    int current_pack = savepart.getsavepack();
    //强制新建 pack
    //只有最后一个分区的增量装入使用继续填充数据块的策略
    bool force_create=(current_pack !=NoPack() - 1) || savepart.isempty();
    //if(NoPack() == 0 || dpns[NoPack() - packs_omitted - 1].GetNoObj() == GetPackrowSize()) {
    // check new partion
    if(NoPack() == 0 || force_create || dpns [current_pack - packs_omitted].GetNoObj() == GetPackrowSize() ) {
        CreateNewPackage();
        new_pack_created = true;
        if(force_create)
            rccontrol << lock << "Partition splitted while loading,partname :" << partitioninfo->getsavepartinfo(NULL).name()<<" packs:"<<NoPack()<< unlock;
    } else
        LockPackForUse(NoPack() - packs_omitted - 1);
    //LockPackForUse(current_pack-packs_omitted);
    return new_pack_created;
}
int RCAttrLoadBase::LoadData(NewValuesSetBase* nvs, bool copy_forced, bool force_saveing_pack, bool pack_already_prepared)
{
    MEASURE_FET(string("a[") + boost::lexical_cast<string>(attr_number) + "].LoadData(...)");

    InitKNsForUpdate();

    if(nvs->NoValues() == 0)
        return 0;
    LoadPackInfoForLoader();
    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);
    // called at RCTableLoad::LoadData ,not here!
    // LoadPackIndexForLoader((short)GetSaveSessionId(),GetSaveSessionId());
    if(NoPack()>0 &&  (savepart.getsavepack()!=NoPack()-1  || savepart.isempty())) {
        //
        //new partition on a none empty table,create new file!
        int cur_save_loc = GetCurSaveLocation();
        SetSavePosLoc(cur_save_loc, 0);
        //int save_location = GetSaveFileLoc(cur_save_loc);
        //SetSaveFileLoc(cur_save_loc, save_location + 1);// why not +1??
        //必须是新文件!
        int lastf=GetTotalPackFile();
        while(DoesFileExist( AttrPackFileNameDirect(attr_number, lastf, path))) {
            if(partitioninfo->GetLastFile()<lastf)
                //could be truncate safety
                break;
            // half committed???
            lastf++;
        }
        SetTotalPackFile(lastf);
        SetSaveFileLoc(cur_save_loc, GetTotalPackFile());
        //没有看到这个提示�!
        // TODO : dma-712:
        // 2013-06-28 16:23:25 New partition loading ,partname:20130110 save loc: 2,other save loc: 1,p:807NoPack:792

        rclog << lock << "New partition loading ,partname:" << partitioninfo->getsavepartinfo(NULL).name()
              <<" save loc: "<< GetSaveFileLoc(cur_save_loc)<< ",other save loc: "<<GetSaveFileLoc(1-cur_save_loc)<< ",p:"<<savepart.getsavepack()<<"NoPack:"<<NoPack()<<unlock;
        if(NoPack()<savepart.getsavepack()) {
            throw DatabaseRCException("nopack < part save pack,data partition maybe need to reload.");
        }
    }
    bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();



    //add partition support here:
    // find last loading pack,fill the pack until full
    //  create new pack ordered continue to total_pack of table
    int load_nulls = 0;
    int load_obj = nvs->NoValues();
    /* adjust partition loading */

    int current_pack = NoPack() - packs_omitted - 1;

    // dma-521 retore origin code
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    uint cur_obj = (uint) dpns[current_pack].GetNoObj();

    if(this->PackType() == PackN) {
        _int64 load_min, load_max;
        DPN new_dpn = LoadDataPackN(dpns[current_pack], nvs, load_min, load_max, load_nulls);
        SetNaturalSizeSaved(GetNaturalSizeSaved() + nvs->SumarizedSize()); //this is needed for LOOKUP column
        bool is_exclusive = UpdateGlobalMinAndMaxForPackN(nvs->NoValues(), load_min, load_max, load_nulls);
        UpdateUniqueness(dpns[current_pack], new_dpn, load_min, load_max, load_nulls, is_exclusive, nvs);
        dpns[current_pack] = new_dpn;
    } else if(PackType() == PackS) {
        DPN& dpn(dpns[current_pack]);
        for(int i = 0; i < nvs->NoValues(); i++) {
            if(nvs->IsNull(i)) {
                if(Type().GetNullsMode() == NO_NULLS) {
                    illegal_nulls = true;
                } else
                    load_nulls++;
            }
        }
        dpn.natural_save_size = uint(nvs->SumarizedSize());
        SetNaturalSizeSaved(GetNaturalSizeSaved() + nvs->SumarizedSize());
        AttrPackS*& packS = ((AttrPackS*&) dpn.pack);

        if(load_nulls == nvs->NoValues() && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
            // no need to store any values - uniform package
            dpn.pack_mode = PACK_MODE_TRIVIAL;
            dpn.is_stored = false;
            dpn.pack_file = PF_NULLS_ONLY;
            dpn.local_min = GetDomainInjectionManager().GetCurrentId();
            dpn.local_max = -1;
            dpn.sum_size = 0;
        } else {
            if(dpn.pack_file == PF_NULLS_ONLY)
                dpn.local_min = 0;
            BHASSERT(packS, "'packS' should not be null"); //pack object must exist
            SetUnique(false); // we will not check uniqueness for strings now
            SetUniqueUpdated(false);
            if(cur_obj == 0 || dpn.pack_mode == 0 || dpn.pack_mode == 3) { // new package (also in case of expanding so-far-null package)
                packS->Prepare(dpn.no_nulls); // data size: one byte for each null, plus more...
                dpn.pack_mode = PACK_MODE_IN_MEMORY;
                dpn.is_stored = true;
                dpn.pack_file = PF_NOT_KNOWN;
            }
            if(!new_pack_created && current_pack == 0 && dpn.local_min == 0 && dpn.local_max == -1
               && GetPackOntologicalStatus(current_pack) != NULLS_ONLY) {
                RCDataTypePtr min_ptr;
                RCDataTypePtr max_ptr;
                GetMinMaxValuesPtrs(current_pack, min_ptr, max_ptr);
                if(!min_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_min), *(RCBString*) (min_ptr.get()), sizeof(_int64));
                if(!max_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_max), *(RCBString*) (min_ptr.get()),
                            (uint)min( (*(RCBString*)(min_ptr.get())).size(), sizeof(_int64)) );
            }

            RCBString min_s;
            RCBString max_s;
            ushort max_size = 0;
            nvs->GetStrStats(min_s, max_s, max_size);
            // JIRA: DMA-670 last pack of last loading not committed.
            if(packS->NoObjs()+nvs->NoValues()>MAX_NO_OBJ)
                packS->TruncateObj(MAX_NO_OBJ-nvs->NoValues());
            packS->Expand(nvs->NoValues());
            bool isnull = false;
            char const* v = 0;
            uint size = 0;
            RCBString zls(ZERO_LENGTH_STRING, 0);
            for(int i = 0; i < nvs->NoValues(); i++) {
                isnull = nvs->IsNull(i);
                if(isnull && Type().GetNullsMode() == NO_NULLS) {
                    isnull = false;
                    v = ZERO_LENGTH_STRING;
                    size = 0;
                    if(min_s.IsNull() || min_s > zls)
                        min_s = zls;
                    if(max_s.IsNull() || max_s < zls)
                        max_s = zls;
                } else {
                    v = nvs->GetDataBytesPointer(i);
                    size = nvs->Size(i);
                }
                packS->BindValue(isnull, (uchar*) v, size);
            }

            if(new_pack_created || GetPackOntologicalStatus(current_pack) == NULLS_ONLY) {
                if(!min_s.IsNull())
                    SetPackMin(current_pack, min_s);
                if(!max_s.IsNull())
                    SetPackMax(current_pack, max_s);
                dpn.sum_size = max_size;
            } else {
                if(!min_s.IsNull() && !min_s.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation(), true))
                    SetPackMin(current_pack, min_s);

                if(!max_s.IsNull() && !max_s.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation(), true))
                    SetPackMax(current_pack, max_s);

                if(dpn.sum_size < max_size)
                    dpn.sum_size = max_size;
            }
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
        }
    }

    DPN& dpn(dpns[current_pack]);
    dpn.no_nulls += load_nulls;
    dpn.no_objs = ushort(cur_obj + load_obj - 1);
    // in case of un full pack loading as this buffer fetched end
    partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);
    //SetNoObj(NoObj() + load_obj);
    //FIX DMA-495 DMA-494
    //SetNoObj(  ((_int64)current_pack<<16)+load_obj);
    //FIX DMA-507
    //SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
    // FIX DMA-607: inserted record lost(one row once)
    SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);

    SetNoNulls(NoNulls() + load_nulls);

    if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
        SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
    }

    if(PackType() == PackS && (copy_forced || dpn.GetNoObj() != GetPackrowSize()))
        ((AttrPackS&) *dpn.pack).CopyBinded();

    if(dpn.GetNoObj() == GetPackrowSize() || force_saveing_pack)
        SavePack(current_pack);
    return 0;
}

void RCAttrLoadBase::SavePartitionPackIndex(DMThreadData *tl)
{
    //Write all index data.
    if(ct.IsPackIndex()) {
        if(!Get_LoadDataFromTruncatePartition()) { // 长会话上过来的数据,不能清除_mrg,mrg可能是其他分区修正过的包索引数据
            ldb_index->ClearMergeDB();
        }
        if(!partitioninfo->IsLastLoad()) {
            apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId()); // _w db
            rclog << lock << "LOG: Write packindex begin "<<path<<" colid "<< attr_number <<unlock;
            pindex->WriteMap();
            rclog << lock << "LOG: Write packindex end "<<path<<" colid "<< attr_number <<unlock;

        } else {

            if(!Get_LoadDataFromTruncatePartition()) { // 不是长会话上过了的提交保存

                // packindex的合并过程:
                // 1. fork 原来的数据库
                // 2. merge 原来的数据库和当前正在写的数据库(_w)到新的数据库(_mrg)中
                // ---> 3. 提交过程在mysqld中，将_mrg重命名操作
                rclog << lock << "LOG: Generate new db begin "<<path<<" colid "<< attr_number <<unlock;

                mytimer _tm_save_packindex;             // 记录时间用
                _tm_save_packindex.Restart();

                // fork packindex
                std::string dbname=apindex::GetDBPath(attr_number,GetLoadPartName(),path,false);
                std::string tmpreaddb=dbname+"_fork";
                apindex::ForkDB(dbname.c_str(),tmpreaddb.c_str());

                _tm_save_packindex.Stop();
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
                rclog << lock << "RCAttrLoadBase::SavePartitonPacksIndex fork "<<dbname <<" to "<<tmpreaddb << ", use time ["<<_tm_save_packindex.GetTime()<<"] (S) , "<<path<<" colid "<< attr_number <<unlock;
#endif

                apindex sindex(tmpreaddb,"");
                sindex.ReloadDB();
                char mergedbname[300];
                apindex::GetMergeIndexPath(dbname.c_str(),mergedbname);
                apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId()); // _w db
                rclog << lock << "colid :"<< attr_number <<"  MapSize:"<<pindex->GetMapSize()<<unlock;
                apindex mergeidx(mergedbname,"");

                _tm_save_packindex.Restart();
                mergeidx.MergeFromHashMap(sindex,*pindex);


                _tm_save_packindex.Stop();
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
                rclog << lock << "RCAttrLoadBase::SavePartitonPacksIndex merge "<<tmpreaddb <<" & "<< pindex->GetPath()<< " to "<< mergedbname <<" , use time ["<<_tm_save_packindex.GetTime()<<"] (S) , "<<path<<" colid "<< attr_number <<unlock;
#endif

                mergeidx.ReloadDB();
                ldb_index->ClearTempDB(GetLoadPartName(),GetSaveSessionId());
                sindex.ClearDB(false);
                rclog << lock << "LOG: Generate new db end "<<path<<" colid "<< attr_number <<unlock;

            } else {
                // 长会话上过来的直接将ldb_w转换成ldb_mrg
                // 空的与ldb_w ----> ldb_mrg
                rclog << lock << "LOG: Generate new db begin "<<path<<" colid "<< attr_number <<unlock;
                std::string dbname=apindex::GetDBPath(attr_number,GetLoadPartName(),path,false);

                ldb_index->ClearMergeDB(dbname); // 清除该库的ldb_mrg
                apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId()); // _w db

                char mergedbname[300];
                apindex::GetMergeIndexPath(dbname.c_str(),mergedbname);
                rclog << lock << "colid :"<< attr_number <<"  MapSize:"<<pindex->GetMapSize()<<unlock;
                apindex mergeidx(mergedbname,"");

#if 1
                // 该过程是可以正常运行的,就是合并的时候会生成非常多的小文件
                // try to fix dma-1653
                mergeidx.ReplaceFromHashMap(*pindex);
#else
                char writedbname[300];
                pindex->BatchWrite(); // 将内存中的合并到文件中db_w

                apindex::GetWriteIndexPath(dbname.c_str(),writedbname);
                mergeidx.ReplaceDB(writedbname);
#endif

                mergeidx.ReloadDB();
                rclog << lock << "LOG: Generate new db end "<<path<<" colid "<< attr_number <<unlock;

                Set_LoadDataFromTruncatePartition(false);
            }
        }
    }
}

void RCAttrLoadBase::UnlockPackFromUse(unsigned pack_no)
{
    //TODO: [michal] Refactoring
    DPN& dpn(dpns[pack_no]);
    if(process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR) {
        //for(int i = 0; i < dpn.no_pack_locks; i++)    don't unlock to avoid a race with memory manager
        //  dpn.pack->Unlock();                         the pack ought to be deleted on following reset so can be locked
        dpn.no_pack_locks = 0;
        dpn.pack.reset();
        if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
            dpn.pack_mode = PACK_MODE_UNLOADED;
    } else if(dpn.pack) {
        PackCoordinate pack_coord(table_number, attr_number, pack_no + packs_omitted,0,0);
        for(int i = 0; i < dpn.no_pack_locks - 1; i++)
            dpn.pack->Unlock();
        TrackableObject::UnlockAndResetPack(dpn.pack);
        dpn.no_pack_locks = 0;
        if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
            dpn.pack_mode = PACK_MODE_UNLOADED;
        else if(!ShouldExist(pack_no))
            ConnectionInfoOnTLS->GetTransaction()->DropLocalObject(pack_coord);
    }
}

void RCAttrLoadBase::LockPackForUse(unsigned pack_no)
{
    //TODO: [michal] Refactoring
    assert((int)pack_no < NoPack());
    PackCoordinate pack_coord(table_number, attr_number, pack_no + packs_omitted,0,0);
    DPN& dpn(dpns[pack_no]);
    if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
        if(!!dpn.pack) {
            dpn.pack->Lock();
            dpn.no_pack_locks++;
        } else {
            if(PackType() == PackS) {
                if(Type().GetCompressType() == Compress_DEFAULT) {
                    dpn.pack = AttrPackPtr(new AttrPackS(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                    dpn.pack = AttrPackPtr(new AttrPackS_Zip(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else if(Type().GetCompressType() == Compress_Snappy) {
                    dpn.pack = AttrPackPtr(new AttrPackS_Snappy(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else {
                    dpn.pack = AttrPackPtr(new AttrPackS_lz4(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                }
            } else {
                if(Type().GetCompressType()== Compress_DEFAULT) {
                    dpn.pack = AttrPackPtr(new AttrPackN(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                    dpn.pack = AttrPackPtr(new AttrPackN_Zip(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else if(Type().GetCompressType() == Compress_Snappy) {
                    dpn.pack = AttrPackPtr(new AttrPackN_Snappy(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else {
                    dpn.pack = AttrPackPtr(new AttrPackN_lz4(pack_coord, TypeName(), GetInsertingMode(), 0));
                }
            }
            dpn.no_pack_locks = 1;
            LoadPack(pack_no);
        }
    } else {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ConnectionInfoOnTLS.IsValid());
        PackAllocator pa(NULL, PackN); /* FIXME: Amok, this is only a dummy object */
        if(!dpn.pack) {
            dpn.pack = ConnectionInfoOnTLS->GetTransaction()->GetAttrPackForUpdate(pack_coord);
            if(!!dpn.pack && dpn.pack_mode == PACK_MODE_UNLOADED)
                dpn.pack_mode = PACK_MODE_IN_MEMORY;
        } else
            dpn.pack->Lock();

        if(!!dpn.pack) {
            dpn.no_pack_locks++;
        } else {
            if(PackType() == PackS) {
                if(Type().GetCompressType() == Compress_DEFAULT) {
                    dpn.pack = AttrPackPtr(new AttrPackS(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                    dpn.pack = AttrPackPtr(new AttrPackS_Zip(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else if(Type().GetCompressType() == Compress_Snappy) {
                    dpn.pack = AttrPackPtr(new AttrPackS_Snappy(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                } else {
                    dpn.pack = AttrPackPtr(new AttrPackS_lz4(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
                }
            } else {
                if(Type().GetCompressType() == Compress_DEFAULT) {
                    dpn.pack = AttrPackPtr(new AttrPackN(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                    dpn.pack = AttrPackPtr(new AttrPackN_Zip(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else if(Type().GetCompressType() == Compress_Snappy) {
                    dpn.pack = AttrPackPtr(new AttrPackN_Snappy(pack_coord, TypeName(), GetInsertingMode(), 0));
                } else {
                    dpn.pack = AttrPackPtr(new AttrPackN_lz4(pack_coord, TypeName(), GetInsertingMode(), 0));
                }
            }
            dpn.no_pack_locks = 1;
            ConnectionInfoOnTLS->GetTransaction()->PutObject(pack_coord, dpn.pack);
            LoadPack(pack_no);
        }
    }
}

DPN& RCAttrLoadBase::CreateNewPackage(bool for_mysqld_insert,int for_mysqld_merge)
{
    //TODO: [Michal] Refactoring
    MEASURE_FET("RCAttrLoadBase::CreateNewPackage()");
    ExpandDPNArrays();
    int pack(NoPack() - packs_omitted);
    DPN& dpn(dpns[pack]);
    if(PackType() == PackN) {
        dpn.local_min = PLUS_INF_64;
        dpn.local_max = MINUS_INF_64;

        BHASSERT(!dpn.pack, "'dpns[no_pack-packs_omitted].pack' should be null");
        if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
            PackCoordinate pc(table_number, attr_number, pack,0,0);
            if(Type().GetCompressType() == Compress_DEFAULT) {
                dpn.pack = AttrPackPtr(new AttrPackN(pc, TypeName(), GetInsertingMode(), 0));
            } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                dpn.pack = AttrPackPtr(new AttrPackN_Zip(pc, TypeName(), GetInsertingMode(), 0));
            } else if(Type().GetCompressType() == Compress_Snappy) {
                dpn.pack = AttrPackPtr(new AttrPackN_Snappy(pc, TypeName(), GetInsertingMode(), 0));
            } else {
                dpn.pack = AttrPackPtr(new AttrPackN_lz4(pc, TypeName(), GetInsertingMode(), 0));
            }
            dpn.no_pack_locks = 1;
        } else {
            PackCoordinate pc(table_number, attr_number, NoPack(),0,0);
            if(Type().GetCompressType() == Compress_DEFAULT) {
                dpn.pack = AttrPackPtr(new AttrPackN(pc, TypeName(), GetInsertingMode(), 0));
            } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                dpn.pack = AttrPackPtr(new AttrPackN_Zip(pc, TypeName(), GetInsertingMode(), 0));
            } else if(Type().GetCompressType() == Compress_Snappy) {
                dpn.pack = AttrPackPtr(new AttrPackN_Snappy(pc, TypeName(), GetInsertingMode(), 0));
            } else {
                dpn.pack = AttrPackPtr(new AttrPackN_lz4(pc, TypeName(), GetInsertingMode(), 0));
            }
            dpn.no_pack_locks = 1;

            if(for_mysqld_insert) { // fix dma-1361:分布式排序后的数据合并,不能往cache中添加一个空包,否则会导致第一次查询返回NULL
                ConnectionInfoOnTLS->GetTransaction()->PutObject(pc, dpn.pack);
            }
        }
    } else {
        dpn.local_min = 0;
        dpn.local_max = -1;

        if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
            PackCoordinate pc(table_number, attr_number, pack,0,0);
            if(Type().GetCompressType()== Compress_DEFAULT) {
                dpn.pack = AttrPackPtr(new AttrPackS(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                dpn.pack = AttrPackPtr(new AttrPackS_Zip(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else if(Type().GetCompressType() == Compress_Snappy) {
                dpn.pack = AttrPackPtr(new AttrPackS_Snappy(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else {
                dpn.pack = AttrPackPtr(new AttrPackS_lz4(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            }
            dpn.no_pack_locks = 1;
        } else {
            PackCoordinate pc(table_number, attr_number, NoPack(),0,0);
            if(Type().GetCompressType()== Compress_DEFAULT) {
                dpn.pack = AttrPackPtr(new AttrPackS(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else if(Type().GetCompressType() == Compress_Soft_Zip) {
                dpn.pack = AttrPackPtr(new AttrPackS_Zip(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else if(Type().GetCompressType() == Compress_Snappy) {
                dpn.pack = AttrPackPtr(new AttrPackS_Snappy(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            } else {
                dpn.pack = AttrPackPtr(new AttrPackS_lz4(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
            }
            dpn.no_pack_locks = 1;

            if(for_mysqld_insert) { // fix dma-1361:分布式排序后的数据合并,不能往cache中添加一个空包,否则会导致第一次查询返回NULL
                ConnectionInfoOnTLS->GetTransaction()->PutObject(pc, dpn.pack);
            }
        }
    }

    if(for_mysqld_merge != 1) { // add by liujs , 分布式装入过程不用

        partitioninfo->getsavepartinfo(NULL).newpack(NoPack());

        SetNoPack(NoPack() + 1);
    }
    return dpn;
}

void RCAttrLoadBase::LoadPack(uint n)
{
    BHASSERT(n < NoPack()-packs_omitted, "should be 'n < no_pack-packs_omitted'");
    if(dpns[n].pack_mode != PACK_MODE_UNLOADED || !ShouldExist(n))
        return;
    WaitForSaveThreads();
    LoadPackInherited(n);
}

void RCAttrLoadBase::LoadPackInherited(int n)
{
    DPN& dpn(dpns[n]);
    if(dpn.pack_file < 0)
        rclog << lock << "INTERNAL ERROR: attempting to open wrong file (LoadPack), dpns[" << n << "].pack_file="
              << dpn.pack_file << unlock;
    {
        if(dpn.pack_mode != PACK_MODE_UNLOADED)
            return;

        BHASSERT(dpn.pack_mode == PACK_MODE_UNLOADED, "Invalid pack_mode!");
        IBFile fattr;

        try {
            fattr.OpenReadOnly(AttrPackFileName(n));
            fattr.Seek(dpn.pack_addr, SEEK_SET);
            dpn.pack->LoadData(&fattr);
            fattr.Close();
        } catch (DatabaseRCException&) {
            rclog << lock << "Error: corrupted " << AttrPackFileName(n) << unlock;
            throw;
        }
        dpn.pack->Uncompress(dom_inj_mngr);
        dpn.pack_mode = PACK_MODE_IN_MEMORY;
        dpn.is_stored = true;
    }
}

/* // 派生类中已经实现,delete by liujs
int RCAttrLoadBase::Save()
{
    MEASURE_FET("RCAttrLoadBase::Save()");
    if(current_state != 1) {
        rclog << lock << "Error: cannot save. It is read only session." << unlock;
        throw;
    }
    LoadPackInfo();
    WaitForSaveThreads();
    int npack = NoPack();
    for(uint i = 0; i < npack - packs_omitted; i++)
        SavePack(i);
    WaitForSaveThreads();
    if(rsi_hist_update || rsi_cmap_update)
        SaveRSI();
    BHASSERT(FileFormat()==10, "should be 'file_format==10'");
    SaveDPN();
    SaveHeader();
    return 0;
}
*/

void RCAttrLoadBase::CompareAndSetCurrentMin(RCBString tstmp, RCBString & min, bool set)
{
    bool res;
    if(RequiresUTFConversions(Type().GetCollation())) {
        res = CollationStrCmp(Type().GetCollation(), tstmp, min) < 0;
    } else
        res = strcmp(tstmp, min) < 0;

    if(!set || res) {
        min = tstmp;
        min.MakePersistent();
        set = true;
    }
}

// 通过dpn文件获取生成数据包数
int RCAttrLoadBase::get_sorted_packno(const std::string& sorted_dpn_file)
{
    struct stat _stat;
    if(stat(sorted_dpn_file.c_str(), &_stat)!=0) {

        char _log_msg[1024];
        sprintf(_log_msg, "Error: Merge sorted data to table[%s] , attr[%d],can't open file [%s]. ",
                path.c_str(),attr_number,sorted_dpn_file.c_str());

        rclog << lock << std::string(_log_msg) <<unlock;

        throw DatabaseRCException(std::string(_log_msg));
    }
    //assert(_stat.st_size>0);
    // return _stat.st_size/37 - 1;
    return _stat.st_size/37; // [pack1][pack2][pack3]......[packn]
}


// 获取所有节点dpn的pack数目
int RCAttrLoadBase::get_pack_num_from_file_info(std::vector<node_sorted_data_file>& sorted_data_file_lst)
{
    int _pack_no = 0;
    for(int i=0; i<sorted_data_file_lst.size(); i++) {
        sorted_data_file_lst[i].pack_no = get_sorted_packno(sorted_data_file_lst[i].dpn_name);
        _pack_no += sorted_data_file_lst[i].pack_no;
    }
    return _pack_no;
}

// 获取准备装入的数据文件列表信息
bool comp_node_sorted_data_file(const RCAttrLoadBase::node_sorted_data_file n1,
                                const RCAttrLoadBase::node_sorted_data_file n2)
{
    return n1.dpn_name < n2.dpn_name;
}

int RCAttrLoadBase::get_sorted_data_file_info(const int attr_num,
        const std::string sessionid,
        const std::string sorted_data_path,
        std::vector<node_sorted_data_file>& sorted_data_file_lst)
{

    char name_pattern[128];
    sorted_data_file_lst.clear();

    // 1. dpn file get
    sprintf(name_pattern,"%s/S%s-N*-A%05d.dpn",sorted_data_path.c_str(),sessionid.c_str(),attr_num);
    {
        glob_t globbuf;
        memset(&globbuf,0,sizeof(globbuf));
        globbuf.gl_offs = 0;
        //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        if(!glob(name_pattern, GLOB_DOOFFS, NULL, &globbuf)) {
            for(int i=0; i<globbuf.gl_pathc; i++) {
                node_sorted_data_file _sorted_data_file_obj;
                _sorted_data_file_obj.dpn_name = std::string(globbuf.gl_pathv[i]);
                sorted_data_file_lst.push_back(_sorted_data_file_obj);
            }
        }
        globfree(&globbuf);
    }
    std::sort(sorted_data_file_lst.begin(),sorted_data_file_lst.end(),comp_node_sorted_data_file);

    // 2. rsi file get
    for(int dpn_idx=0; dpn_idx<sorted_data_file_lst.size(); dpn_idx++) {
        std::string dpn_file_name = sorted_data_file_lst[dpn_idx].dpn_name;
        char rsi_file_name[256];
        strcpy(rsi_file_name,dpn_file_name.c_str());
        rsi_file_name[strlen(rsi_file_name)-4] = 0;
        strcat(rsi_file_name,".rsi");

        glob_t globbuf;
        memset(&globbuf,0,sizeof(globbuf));
        globbuf.gl_offs = 0;
        //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        if(!glob(rsi_file_name, GLOB_DOOFFS, NULL, &globbuf)) {
            for(int i=0; i<globbuf.gl_pathc; i++) {
                sorted_data_file_lst[dpn_idx].rsi_name= std::string(globbuf.gl_pathv[i]);
            }
        }
        globfree(&globbuf);
    }

    // 3. pack file get
    for(int dpn_idx=0; dpn_idx<sorted_data_file_lst.size(); dpn_idx++) {
        std::string dpn_file_name = sorted_data_file_lst[dpn_idx].dpn_name;
        char pack_file_name[256];
        strcpy(pack_file_name,dpn_file_name.c_str());
        pack_file_name[strlen(pack_file_name)-4] = 0;
        strcat(pack_file_name,"-B*.pck");

        glob_t globbuf;
        memset(&globbuf,0,sizeof(globbuf));
        globbuf.gl_offs = 0;
        //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        if(!glob(pack_file_name, GLOB_DOOFFS, NULL, &globbuf)) {
            for(int i=0; i<globbuf.gl_pathc; i++) {
                sorted_data_file_lst[dpn_idx].pack_name_lst.push_back(std::string(globbuf.gl_pathv[i]));
            }
        }
        globfree(&globbuf);
        std::sort(sorted_data_file_lst[dpn_idx].pack_name_lst.begin(),sorted_data_file_lst[dpn_idx].pack_name_lst.end());
    }

    return sorted_data_file_lst.size();
}


// 获取包索引的文件列表(节点生成后的排序的包索引,DMA-1319)
int RCAttrLoadBase::get_sorted_ldb_file_info(const int attr_num,
        const std::string sessionid,
        const std::string sorted_data_path,
        std::vector<std::string>& ldb_file_lst)
{

    char name_pattern[128];
    ldb_file_lst.clear();

    // 1. dpn file get
    // S%s-N%05u-A%05u.pki
    sprintf(name_pattern,"%s/S%s-N*-A%05d.pki",sorted_data_path.c_str(),sessionid.c_str(),attr_num);
    {
        glob_t globbuf;
        memset(&globbuf,0,sizeof(globbuf));
        globbuf.gl_offs = 0;
        //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        if(!glob(name_pattern, GLOB_DOOFFS, NULL, &globbuf)) {
            for(int i=0; i<globbuf.gl_pathc; i++) {
                ldb_file_lst.push_back(std::string(globbuf.gl_pathv[i]));
            }
        }
        globfree(&globbuf);
    }
    std::sort(ldb_file_lst.begin(),ldb_file_lst.end());

    return ldb_file_lst.size();

}


int RCAttrLoadBase::read_one_packn(IBFile & pack_handle,char* p_pack_buff,uint& pack_buff_size,int& read_pack_size)
{
    enum ValueType { UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8};
    try {
        // read header of the pack.     WARNING: if the first 4 bytes are NOT going to indicate file size, change CheckPackFileSize in RCAttr !
        char  head[20];
        read_pack_size = 0;

        int _buf_used = pack_handle.ReadExact(head, 17);
        if(_buf_used < 17) {
            rclog << lock << "Error: pack_handle.ReadExact(head, 17) return error, attr ["<< attr_number << "]."<< unlock;
            return -1;
        }

        uint _total_size = *((uint*) head);

        if(_total_size > 512*1024*1024) { // 单个数据包最大不应超过512M
            rclog << lock << "Error: pack_handle.ReadExact(head, 17) _total_size > 512*1024*1024 , attr ["<< attr_number << "]."<< unlock;
            return -1;
        } else if(_total_size > pack_buff_size) {
            pack_buff_size = _total_size +10;
            p_pack_buff=(char *)realloc(p_pack_buff,pack_buff_size);
        }

        memcpy(p_pack_buff,head,17);
        read_pack_size+=17;

        unsigned char _optimal_mode = head[4];
        int _pack_no_obj = *((ushort*) (head + 5));
        uint _no_nulls = *((ushort*) (head + 7));

        int bit_rate = CalculateBinSize(*((_uint64*) (head + 9)));
        ValueType value_type;
        if(bit_rate <= 8)
            value_type = UCHAR;
        else if(bit_rate <= 16)
            value_type = USHORT;
        else if(bit_rate <= 32)
            value_type= UINT;
        else
            value_type = UINT64;

        if(_optimal_mode & 0x40) {
            if(_no_nulls) {
                pack_handle.ReadExact((char*)p_pack_buff+read_pack_size, (_pack_no_obj+7)/8);
                read_pack_size += (_pack_no_obj+7)/8;
            }
            if(bit_rate > 0 && value_type * _pack_no_obj) {
                pack_handle.ReadExact((char*)p_pack_buff+read_pack_size, value_type * _pack_no_obj);
            }
            read_pack_size += value_type * _pack_no_obj;
        } else {
            uint _comp_buf_size = _total_size - 17;

            _buf_used = pack_handle.ReadExact((char*)p_pack_buff+read_pack_size,_comp_buf_size);
            if(_buf_used > _comp_buf_size) {
                return -1;
            }
            read_pack_size+=_comp_buf_size;
        }
    } catch (RCException&) {

        free(p_pack_buff);
        pack_buff_size = 0;

        throw;
    }

    return read_pack_size;
}

int RCAttrLoadBase::read_one_packs(IBFile & pack_handle,char* p_pack_buff,uint& pack_buff_size,int& read_pack_size)
{
    enum ValueType { UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8};
    read_pack_size = 0;
    try {
        unsigned char _optimal_mode = 0;
        char prehead[20] ;
        int _buf_used = pack_handle.ReadExact(prehead, 9);
        if(_buf_used < 9) {
            rclog << lock << "Error: pack_handle.ReadExact(prehead, 9) return error, attr ["<< attr_number << "]."<< unlock;
            return -1;
        }


        // WARNING: if the first 4 bytes are NOT going to indicate file size, change CheckPackFileSize in RCAttr !
        uint _total_size = *((uint *)prehead);

        if(_total_size > 512*1024*1024) { // 单个数据包最大不应超过512M
            rclog << lock << "Error: pack_handle.ReadExact(head, 17) _total_size > 512*1024*1024 , attr ["<< attr_number << "]."<< unlock;
            return -1;
        } else if(_total_size > pack_buff_size) {
            pack_buff_size = _total_size +10;
            p_pack_buff=(char *)realloc(p_pack_buff,pack_buff_size);
        }

        memcpy(p_pack_buff,prehead,9);
        read_pack_size+=9;

        uchar _tmp_mode = prehead[4];
        uint _pack_no_obj = *((ushort*)(prehead + 5));
        int _header_size;
        uint _data_full_byte_size = 0;

        uint _len_mode = 0;
        if(this->Type()== RC_BIN) {
            _len_mode = sizeof(uint);
        } else {
            _len_mode = sizeof(ushort);
        }

        if(_tmp_mode >= 8 && _tmp_mode <=253) {
            char* _p = p_pack_buff+read_pack_size;
            _buf_used = pack_handle.ReadExact(_p, 10);
            _optimal_mode = _p[0];
            _data_full_byte_size = *((uint*)(_p + 6));
            _header_size = 19;
            read_pack_size += 10;
        } else {
            char* _p = p_pack_buff+read_pack_size;
            _optimal_mode = _tmp_mode;
            _buf_used = pack_handle.ReadExact(_p, 4);
            _data_full_byte_size = *((uint*)(_p));
            _header_size = 13;
            read_pack_size += 4;
        }
        if(!(_optimal_mode & 0xFC) == 0 || _optimal_mode&0x4) {
            return -1;
        } else {
            if(_optimal_mode & 0x4) {

                pack_handle.ReadExact((char*) p_pack_buff + read_pack_size, _data_full_byte_size);
                read_pack_size += _data_full_byte_size;

                pack_handle.ReadExact((char*) p_pack_buff + read_pack_size, (_pack_no_obj + 7) / 8);
                read_pack_size += (_pack_no_obj + 7) / 8;

                pack_handle.ReadExact((char*) p_pack_buff + read_pack_size, _len_mode * _pack_no_obj);
                read_pack_size += _len_mode * _pack_no_obj;

            } else {
                if(_optimal_mode & 0x2 || _optimal_mode & 0x1) {
                    uint _comp_buf_size = _total_size - _header_size;
                    pack_handle.ReadExact((char*)p_pack_buff + read_pack_size, _comp_buf_size);
                    read_pack_size+= _comp_buf_size;
                } else {
                    return -1;
                }
            }
        }

    } catch (DatabaseRCException&) {

        free(p_pack_buff);
        pack_buff_size = 0;

        throw;
    }

    return read_pack_size;
}

// 创建特殊dpn(全null,最大最小相同的)
DPN RCAttrLoadBase::load_data_spec_packn(const DPN& source_dpn,
        const int load_no_nulls,
        const int load_no_objs,
        const _int64 load_min,
        const _int64 load_max)
{
    DPN dpn(source_dpn);
    double& real_sum = *(double*) &dpn.sum_size;
    uint cur_nulls = dpn.no_nulls;
    uint cur_obj = dpn.GetNoObj();
    int load_obj = load_no_objs;

    bool is_real_type = ATI::IsRealType(TypeName());


    // 单个数据包的合并流程,必须满足合并的数据包记录数不能超过65536
    assert(cur_obj + load_obj <= 65536);

    if(load_no_nulls == load_no_objs) {
        dpn.sum_size += 0;
    } else {
        if(!is_real_type) {
            dpn.sum_size += load_min*load_no_objs;
        } else {
            real_sum += (double)load_min*load_no_objs;
        }
    }

    if((cur_nulls + load_no_nulls) == 0 && load_min == load_max && (cur_obj == 0 || (dpn.local_min == load_min
            && dpn.local_max == load_max))) {
        // no need to store any values - uniform package
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NOT_KNOWN; // will not be used
        dpn.local_min = load_min;
        dpn.local_max = load_max;
        // sum is already calculated
    } else if(load_no_nulls == load_obj && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
        // no need to store any values - uniform package (nulls only)
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NULLS_ONLY;
        dpn.local_min = PLUS_INF_64;
        dpn.local_max = MINUS_INF_64;
        dpn.sum_size = 0;
        // uniqueness status not changed
    } else {
        _uint64 new_max_val = 0;
        if(cur_obj == 0 || dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_mode == 3) {
            // new package (also in case of expanding so-far-uniform package)
            _uint64 uniform_so_far = 0;
            if(dpn.pack_file == PF_NULLS_ONLY) {
                uniform_so_far = (_uint64) NULL_VALUE_64;
                dpn.local_min = load_min;
                dpn.local_max = load_max;
            } else {
                if(ATI::IsRealType(TypeName())) {
                    uniform_so_far = dpn.local_min; // fill with uniform-so-far
                } else if(dpn.local_min > load_min)
                    uniform_so_far = _uint64(dpn.local_min - load_min);

                if(!ATI::IsRealType(TypeName())) {
                    if(cur_obj == 0 || dpn.local_min > load_min)
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || dpn.local_max < load_max)
                        dpn.local_max = load_max;
                } else {
                    if(cur_obj == 0 || *(double*) &(dpn.local_min) > *(double*) &(load_min))
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || *(double*) &(dpn.local_max) < *(double*) &(load_max))
                        dpn.local_max = load_max;
                }
            }

            BHASSERT(dpn.pack, "'dpn.pack' should not be null");
            if(ATI::IsRealType(TypeName())) {
                // full 64-bit scope
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL));
            } else {
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, new_max_val);
            }
            dpn.pack_file = PF_NOT_KNOWN;
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
            // Now fill the beginning of the table by so far uniform values (if there is any beginning)
            if(cur_obj > 0) {
                if(uniform_so_far != NULL_VALUE_64) {
                    if((dpn.local_min != dpn.local_max) || (load_no_nulls > 0))
                        for(int i = 0; i < (int) cur_obj; i++)
                            ((AttrPackN*) dpn.pack.get())->SetVal64(i, uniform_so_far);
                } else {
                    for(int i = 0; i < (int) cur_obj; i++) {
                        dpn.pack->SetNull(i);
                    }
                }
            }
        } else {
            // expand existing package
            //fix JIRA DMAPP-1100,packn rollbacked rows not reset while re-insert.
            ((AttrPackN*) dpn.pack.get())->TruncateObj(cur_obj);
            if(ATI::IsRealType(TypeName())) {
                if(*(double*) &dpn.local_min > *(double*) &load_min)
                    *(double*) &dpn.local_min = *(double*) &load_min;
                if(*(double*) &dpn.local_max < *(double*) &load_max)
                    *(double*) &dpn.local_max = *(double*) &load_max;
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL), 0);
            } else {
                _int64 offset = 0;
                if(dpn.local_min > load_min) {
                    offset = dpn.local_min;
                    offset -= load_min;
                    dpn.local_min = load_min;
                }
                if(dpn.local_max < load_max)
                    dpn.local_max = load_max;
                BHASSERT(dpn.pack, "'dpn.pack' should not be null");
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                // 修正调整包内已经存在的值的大小
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, new_max_val, offset);
            }
        }

        _uint64 v = 0;
        int obj = 0;
        bool isnull;
        for(int i = 0; i < load_obj; i++) {
            isnull = (load_no_nulls == load_no_objs);
            if(isnull && Type().GetNullsMode() == NO_NULLS) {
                isnull = false;
                v = -1;
            } else {
                v = load_min; // 特殊包最大值,最小值相同
            }
            obj = i + cur_obj;
            if(dpn.local_min == dpn.local_max && !ATI::IsRealType(TypeName())) {// special case: no data stored except nulls
                if(isnull)
                    dpn.pack->SetNull(obj);
            } else {
                if(!ATI::IsRealType(TypeName()))
                    v -= dpn.local_min;
                if(isnull) {
                    //((AttrPackN*)dpns[NoPack()-packs_omitted-1].pack)->SetVal64(obj, 0);
                    ((AttrPackN*) dpn.pack.get())->SetNull(obj);
                } else
                    ((AttrPackN*) dpn.pack.get())->SetVal64(obj, v);
            }
        }
    }
    return dpn;

}


// 构建一个dpn
DPN RCAttrLoadBase::load_data_packn(const DPN& source_dpn,
                                    AttrPack* one_pack,
                                    const _int64 local_min,
                                    const _int64 local_max,
                                    _int64& load_min,
                                    _int64& load_max,
                                    int& load_nulls)
{
    DPN dpn(source_dpn);
    double& real_sum = *(double*) &dpn.sum_size;
    uint cur_nulls = dpn.no_nulls;
    uint cur_obj = dpn.GetNoObj();
    bool nulls_conferted = false;
    _int64 null_value = -1;
    int nonv = one_pack->NoObjs();
    bool is_real_type = ATI::IsRealType(TypeName());
    int load_obj = one_pack->NoObjs();

    // 单个数据包的合并流程,必须满足合并的数据包记录数不能超过65536
    assert(cur_obj + load_obj <= 65536);
    if(!ATI::IsRealType(TypeName())) {
        load_min = PLUS_INF_64;
        load_max = MINUS_INF_64;
    } else {
        load_min = *(_int64*) &PLUS_INF_DBL;
        load_max = *(_int64*) &MINUS_INF_DBL;
    }

    AttrPackN* one_pack_n = (AttrPackN*)one_pack;

    for(int i = 0; i < nonv; i++) {
        if(one_pack_n->IsNull(i)) {
            if(Type().GetNullsMode() == NO_NULLS) {
                nulls_conferted = true;
                illegal_nulls = true;
                _int64 v = 0;
                if(ATI::IsStringType(TypeName())) {
                    if(null_value == -1)
                        null_value = v = EncodeValue_T(ZERO_LENGTH_STRING, 1);
                    else
                        v = null_value;
                } else {
                    null_value = v = 0;
                }
            } else
                load_nulls++;
        }
    }

    if(!is_real_type) {
        _int64 tmp_sum=0;
        for(int i=0; i<one_pack_n->NoObjs(); i++) {
            if(!one_pack_n->IsNull(i)) {
                _int64 _tmp = one_pack_n->GetVal64(i);
                _tmp += local_min;
                tmp_sum += _tmp;
                if(load_min>_tmp) {
                    load_min=_tmp;
                }
                if(load_max<_tmp) {
                    load_max=_tmp;
                }
            }
        }
        dpn.sum_size += tmp_sum;
        if(nulls_conferted) {
            if(load_min > null_value)
                load_min = null_value;
            if(load_max < null_value)
                load_max = null_value;
        }
    } else {
        double tmp_sum = 0;
        for(int i=0; i<one_pack_n->NoObjs(); i++) {
            if(!one_pack_n->IsNull(i)) {
                _int64 _tmpx = one_pack_n->GetVal64(i);
                double _tmp =*(double *)&_tmpx;
                tmp_sum += _tmp;
                if(*(double*) &load_min>_tmp) {
                    *(double*) &load_min=_tmp;
                }
                if(*(double*) &load_max<_tmp) {
                    *(double*) &load_max=_tmp;
                }
            }
        }
        real_sum += tmp_sum;
        if(nulls_conferted) {
            if(*(double*) &load_min > *(double*) &null_value)
                *(double*) &load_min = *(double*) &null_value;
            if(*(double*) &load_max < *(double*) &null_value)
                *(double*) &load_max = *(double*) &null_value;
        }
    }

    if((cur_nulls + load_nulls) == 0 && load_min == load_max && (cur_obj == 0 || (dpn.local_min == load_min
            && dpn.local_max == load_max))) {
        // no need to store any values - uniform package
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NOT_KNOWN; // will not be used
        dpn.local_min = load_min;
        dpn.local_max = load_max;
        // sum is already calculated
    } else if(load_nulls == load_obj && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
        // no need to store any values - uniform package (nulls only)
        dpn.pack_mode = PACK_MODE_TRIVIAL;
        dpn.is_stored = false;
        dpn.pack_file = PF_NULLS_ONLY;
        dpn.local_min = PLUS_INF_64;
        dpn.local_max = MINUS_INF_64;
        dpn.sum_size = 0;
        // uniqueness status not changed
    } else {
        _uint64 new_max_val = 0;
        if(cur_obj == 0 || dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_mode == 3) {
            // new package (also in case of expanding so-far-uniform package)
            _uint64 uniform_so_far = 0;
            if(dpn.pack_file == PF_NULLS_ONLY) {
                uniform_so_far = (_uint64) NULL_VALUE_64;
                dpn.local_min = load_min;
                dpn.local_max = load_max;
            } else {
                if(ATI::IsRealType(TypeName())) {
                    uniform_so_far = dpn.local_min; // fill with uniform-so-far
                } else if(dpn.local_min > load_min)
                    uniform_so_far = _uint64(dpn.local_min - load_min);

                if(!ATI::IsRealType(TypeName())) {
                    if(cur_obj == 0 || dpn.local_min > load_min)
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || dpn.local_max < load_max)
                        dpn.local_max = load_max;
                } else {
                    if(cur_obj == 0 || *(double*) &(dpn.local_min) > *(double*) &(load_min))
                        dpn.local_min = load_min;
                    if(cur_obj == 0 || *(double*) &(dpn.local_max) < *(double*) &(load_max))
                        dpn.local_max = load_max;
                }
            }

            BHASSERT(dpn.pack, "'dpn.pack' should not be null");
            if(ATI::IsRealType(TypeName())) {
                // full 64-bit scope
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL));
            } else {
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                ((AttrPackN*) dpn.pack.get())->Prepare(cur_obj + load_obj, new_max_val);
            }
            dpn.pack_file = PF_NOT_KNOWN;
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
            // Now fill the beginning of the table by so far uniform values (if there is any beginning)
            if(cur_obj > 0) {
                if(uniform_so_far != NULL_VALUE_64) {
                    if((dpn.local_min != dpn.local_max) || (load_nulls > 0))
                        for(int i = 0; i < (int) cur_obj; i++)
                            ((AttrPackN*) dpn.pack.get())->SetVal64(i, uniform_so_far);
                } else {
                    for(int i = 0; i < (int) cur_obj; i++) {
                        dpn.pack->SetNull(i);
                    }
                }
            }
        } else {
            // expand existing package
            //fix JIRA DMAPP-1100,packn rollbacked rows not reset while re-insert.
            ((AttrPackN*) dpn.pack.get())->TruncateObj(cur_obj);
            if(ATI::IsRealType(TypeName())) {
                if(*(double*) &dpn.local_min > *(double*) &load_min)
                    *(double*) &dpn.local_min = *(double*) &load_min;
                if(*(double*) &dpn.local_max < *(double*) &load_max)
                    *(double*) &dpn.local_max = *(double*) &load_max;
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL), 0);
            } else {
                _int64 offset = 0;
                if(dpn.local_min > load_min) {
                    offset = dpn.local_min;
                    offset -= load_min;
                    dpn.local_min = load_min;
                }
                if(dpn.local_max < load_max)
                    dpn.local_max = load_max;
                BHASSERT(dpn.pack, "'dpn.pack' should not be null");
                new_max_val = _uint64(dpn.local_max - dpn.local_min);
                // 修正调整包内已经存在的值的大小
                ((AttrPackN*) dpn.pack.get())->Expand(cur_obj + load_obj, new_max_val, offset);
            }
        }

        _uint64 v = 0;
        int obj = 0;
        bool isnull;
        for(int i = 0; i < load_obj; i++) {
            isnull = one_pack_n->IsNull(i);
            if(isnull && Type().GetNullsMode() == NO_NULLS) {
                isnull = false;
                v = null_value;
            } else {
                if(!ATI::IsRealType(TypeName())) {
                    v = one_pack_n->GetVal64(i)+local_min;
                } else {
                    v = one_pack_n->GetVal64(i);
                }
            }
            obj = i + cur_obj;
            if(dpn.local_min == dpn.local_max && !ATI::IsRealType(TypeName())) {// special case: no data stored except nulls
                if(isnull)
                    dpn.pack->SetNull(obj);
            } else {
                if(!ATI::IsRealType(TypeName()))
                    v -= dpn.local_min;
                if(isnull) {
                    //((AttrPackN*)dpns[NoPack()-packs_omitted-1].pack)->SetVal64(obj, 0);
                    ((AttrPackN*) dpn.pack.get())->SetNull(obj);
                } else
                    ((AttrPackN*) dpn.pack.get())->SetVal64(obj, v);
            }
        }
    }
    return dpn;
}


// 加载dpn中的pack对象到内存
int RCAttrLoadBase::load_current_pack(const int current_pack)
{
    // fix dma-1426:dpn内部pack不为空时候,需要重新加载pack对象
    DPN& dpn(dpns[current_pack]);
    if(dpn.is_stored && dpn.pack.get() == NULL) {

        dpn.pack = GetAllocator()->lockedAlloc(PackCoordinate(table_number, attr_number, 0,0,0));

        IBFile pack_fn_handle;
        std::string pack_file = RCAttr::AttrPackFileNameDirect(attr_number,dpn.pack_file,path);
        pack_fn_handle.OpenReadOnly(pack_file);
        pack_fn_handle.Seek(dpn.pack_addr,SEEK_SET);
        dpn.pack.get()->LoadData((IBStream*)&pack_fn_handle);
        dpn.pack.get()->Uncompress(GetAllocator()->GetDomainInjectionManager());

        pack_fn_handle.Close();
    }

    // dpn 中的obj和pack必须完全一致
    //if(dpn.is_stored && dpn.pack.get() != NULL) {
    //    assert(dpn.GetNoObj() == dpn.pack.get()->NoObjs());
    //}
}


// 合并一个packn数据包(全null,所有值相同的)
int RCAttrLoadBase::load_one_spec_packn(const int load_no_nulls,
                                        const int load_no_objs,
                                        const _int64 load_min,
                                        const _int64 load_max,
                                        const bool first_merge,
                                        bool pack_already_prepared)
{
    InitKNsForUpdate();

    assert(load_no_objs > 0);

    if(first_merge) { // fix dma-1359
        // LoadPackInfoForLoader();
        LockPackForUse(NoPack() - packs_omitted - 1);
    }

    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();

    int load_nulls = load_no_nulls;
    int load_obj = load_no_objs;
    /* adjust partition loading */

    int current_pack = NoPack() - packs_omitted - 1;

    // dma-521 retore origin code
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    uint cur_obj = (uint) dpns[current_pack].GetNoObj();

    if(this->PackType() == PackN) {
        DPN new_dpn = load_data_spec_packn(dpns[current_pack],load_nulls,load_obj,load_min, load_max);

        _int64 SumarizedSize = 0;
        if(load_nulls == load_obj) { // 全null
            SumarizedSize = 0;
        } else {
            SumarizedSize += load_min * load_obj;
        }
        SetNaturalSizeSaved(GetNaturalSizeSaved() + SumarizedSize); //this is needed for LOOKUP column
        bool is_exclusive = UpdateGlobalMinAndMaxForPackN(load_obj, load_min, load_max, load_nulls);

        //UpdateUniqueness(dpns[current_pack], new_dpn, load_min, load_max, load_nulls, is_exclusive, nvs);
        {
            DPN& old_dpn = dpns[current_pack];

            if((old_dpn.no_nulls + load_nulls) == 0 && load_min == load_max && (old_dpn.GetNoObj() == 0 || (old_dpn.local_min
                    == load_min && old_dpn.local_max == load_max))) {
                if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
                    if(load_obj - load_nulls == 1)
                        SetUnique(true); // only one new value, different than the previous
                    else
                        SetUnique(false); // at least two identical values found
                } else
                    SetUniqueUpdated(false); // may be unique or not
            }

            if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
               && !ATI::IsRealType(TypeName()) // temporary limitations
               && _int64(new_dpn.local_max - new_dpn.local_min) > 0 && _int64(new_dpn.local_max - new_dpn.local_min)
               < 8000000) {

                //new_dpn.repetition_found = HasRepetitions(new_dpn, old_dpn, load_obj, load_nulls, nvs);
                {
                    bool has_repetition = false;
                    std::map<int,int> nfblocks;
                    Filter f_val(new_dpn.local_max - new_dpn.local_min + 1,nfblocks);
                    f_val.Reset();
                    if(new_dpn.local_min == new_dpn.local_max) {
                        if(old_dpn.GetNoObj() + load_obj - new_dpn.no_nulls - load_nulls > 1) // more than one non-null uniform object?
                            new_dpn.repetition_found = true;
                    } else {
                        _uint64 v = 0;
                        if(load_nulls != load_obj) { // 只有一个值的情况
                            for(int i = 0; i < load_obj; i++) {
                                v = load_min;
                                if(f_val.Get(v)) {
                                    has_repetition = true;
                                    break;
                                }
                                f_val.Set(v);
                            }
                        }
                    }
                    new_dpn.repetition_found  =  has_repetition;
                }

                if(new_dpn.repetition_found)
                    SetUnique(false); // else remains true
            } else
                SetUniqueUpdated(false);
        }

        dpns[current_pack] = new_dpn;
    }

    DPN& dpn(dpns[current_pack]);
    dpn.no_nulls += load_nulls;
    dpn.no_objs = ushort(cur_obj + load_obj - 1);
    // in case of un full pack loading as this buffer fetched end
    partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);

    //SetNoObj(NoObj() + load_obj);
    //FIX DMA-495 DMA-494
    //SetNoObj(  ((_int64)current_pack<<16)+load_obj);
    //FIX DMA-507
    //SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
    // FIX DMA-607: inserted record lost(one row once)
    SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);

    SetNoNulls(NoNulls() + load_nulls);

    if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
        SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
    }


    // 特殊的数据包,不用调用DoSavePack
    bool reset_pack_obj = false;
    SavePack(current_pack,reset_pack_obj);


    if(dpn.GetNoObj() == MAX_NO_OBJ) { // 合并满了,外面没有设置过数据包
        return 1; // 满包,外面调整current_pack用
    }

    return 0;


}


// 单个数值类型数据包的合并装入过程
int RCAttrLoadBase::load_one_packn(AttrPack* one_pack,
                                   const _int64 local_min,
                                   const _int64 local_max,
                                   const bool first_merge,
                                   bool pack_already_prepared)
{

    InitKNsForUpdate();

    assert(one_pack->NoObjs() > 0);

    if(first_merge) { // fix dma-1359
        // LoadPackInfoForLoader();
        LockPackForUse(NoPack() - packs_omitted - 1);
    }

    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();

    int load_nulls = 0;
    int load_obj = one_pack->NoObjs();
    /* adjust partition loading */

    int current_pack = NoPack() - packs_omitted - 1;

    // fix dma-1426:如果包为空就加载到内存
    load_current_pack(current_pack);

    // dma-521 retore origin code
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    uint cur_obj = (uint) dpns[current_pack].GetNoObj();

    if(this->PackType() == PackN) {
        _int64 load_min, load_max;
        DPN new_dpn = load_data_packn(dpns[current_pack],one_pack, local_min,local_max,load_min, load_max, load_nulls);

        _int64 SumarizedSize = 0;
        AttrPackN* one_pack_n = (AttrPackN*)one_pack;
        for(int i=0; i<one_pack_n->NoObjs(); i++) {
            if(!one_pack_n->IsNull(i)) {
                SumarizedSize += local_min;
                SumarizedSize += one_pack_n->GetVal64(i);
            }
        }
        SetNaturalSizeSaved(GetNaturalSizeSaved() + SumarizedSize); //this is needed for LOOKUP column
        bool is_exclusive = UpdateGlobalMinAndMaxForPackN(one_pack->NoObjs(), load_min, load_max, load_nulls);

        //UpdateUniqueness(dpns[current_pack], new_dpn, load_min, load_max, load_nulls, is_exclusive, nvs);
        {
            DPN& old_dpn = dpns[current_pack];

            if((old_dpn.no_nulls + load_nulls) == 0 && load_min == load_max && (old_dpn.GetNoObj() == 0 || (old_dpn.local_min
                    == load_min && old_dpn.local_max == load_max))) {
                if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
                    if(load_obj - load_nulls == 1)
                        SetUnique(true); // only one new value, different than the previous
                    else
                        SetUnique(false); // at least two identical values found
                } else
                    SetUniqueUpdated(false); // may be unique or not
            }

            if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
               && !ATI::IsRealType(TypeName()) // temporary limitations
               && _int64(new_dpn.local_max - new_dpn.local_min) > 0 && _int64(new_dpn.local_max - new_dpn.local_min)
               < 8000000) {

                //new_dpn.repetition_found = HasRepetitions(new_dpn, old_dpn, load_obj, load_nulls, nvs);
                {
                    bool has_repetition = false;
                    std::map<int,int> nfblocks;
                    Filter f_val(new_dpn.local_max - new_dpn.local_min + 1,nfblocks);
                    f_val.Reset();
                    if(new_dpn.local_min == new_dpn.local_max) {
                        if(old_dpn.GetNoObj() + load_obj - new_dpn.no_nulls - load_nulls > 1) // more than one non-null uniform object?
                            new_dpn.repetition_found = true;
                    } else {
                        _uint64 v = 0;
                        for(int i = 0; i < load_obj; i++) {
                            if(one_pack_n->IsNull(i) == false) {
                                BHASSERT_WITH_NO_PERFORMANCE_IMPACT((one_pack_n->GetVal64(i)+local_min)<= new_dpn.local_max);
                                BHASSERT_WITH_NO_PERFORMANCE_IMPACT((one_pack_n->GetVal64(i)+local_min) >= new_dpn.local_min);
                                v = one_pack_n->GetVal64(i);
                                if(f_val.Get(v)) {
                                    has_repetition = true;
                                    break;
                                }
                                f_val.Set(v);
                            }
                        }
                    }
                    new_dpn.repetition_found  =  has_repetition;
                }

                if(new_dpn.repetition_found)
                    SetUnique(false); // else remains true
            } else
                SetUniqueUpdated(false);
        }

        dpns[current_pack] = new_dpn;
    }

    DPN& dpn(dpns[current_pack]);
    dpn.no_nulls += load_nulls;
    dpn.no_objs = ushort(cur_obj + load_obj - 1);
    // in case of un full pack loading as this buffer fetched end
    partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);

    //SetNoObj(NoObj() + load_obj);
    //FIX DMA-495 DMA-494
    //SetNoObj(  ((_int64)current_pack<<16)+load_obj);
    //FIX DMA-507
    //SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
    // FIX DMA-607: inserted record lost(one row once)
    SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);

    SetNoNulls(NoNulls() + load_nulls);

    if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
        SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
    }

    // pack不能够释放,后面可能会继续合并包用,多节点分布式排序装入用
    bool reset_pack_obj = false;
    SavePack(current_pack,reset_pack_obj);

    if(dpn.GetNoObj() == MAX_NO_OBJ) { // 合并满了,外面没有设置过数据包
        return 1; // 满包,外面调整current_pack用
    }

    return 0;
}


int RCAttrLoadBase::load_one_spec_packs(const int no_nulls,const bool first_merge,bool pack_already_prepared )
{
    InitKNsForUpdate();

    assert(no_nulls> 0);

    if(first_merge) { // fix dma-1359
        // LoadPackInfoForLoader();
        LockPackForUse(NoPack() - packs_omitted - 1);
    }

    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();

    int load_nulls = no_nulls;
    int load_obj = no_nulls;
    /* adjust partition loading */

    int current_pack = NoPack() - packs_omitted - 1;

    // dma-521 retore origin code
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    uint cur_obj = (uint) dpns[current_pack].GetNoObj();

    // 单个数据包的合并流程,必须满足合并的数据包记录数不能超过65536
    assert(cur_obj + load_obj <= 65536);

    if(PackType() == PackS) {

        DPN& dpn(dpns[current_pack]);

        if(Type().GetNullsMode() == NO_NULLS) {
            illegal_nulls = true;
        }

        if(dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY) {
            // no need to store any values - uniform package
            dpn.pack_mode = PACK_MODE_TRIVIAL;
            dpn.is_stored = false;
            dpn.pack_file = PF_NULLS_ONLY;
            dpn.local_min = GetDomainInjectionManager().GetCurrentId();
            dpn.local_max = -1;
            dpn.sum_size = 0;
        } else {

            AttrPackS*& packS = ((AttrPackS*&) dpn.pack);

            if(dpn.pack_file == PF_NULLS_ONLY)
                dpn.local_min = 0;
            BHASSERT(packS, "'packS' should not be null"); //pack object must exist
            SetUnique(false); // we will not check uniqueness for strings now
            SetUniqueUpdated(false);
            if(cur_obj == 0 || dpn.pack_mode == 0 || dpn.pack_mode == 3) { // new package (also in case of expanding so-far-null package)
                packS->Prepare(dpn.no_nulls); // data size: one byte for each null, plus more...
                dpn.pack_mode = PACK_MODE_IN_MEMORY;
                dpn.is_stored = true;
                dpn.pack_file = PF_NOT_KNOWN;
            }
            if(!new_pack_created && current_pack == 0 && dpn.local_min == 0 && dpn.local_max == -1
               && GetPackOntologicalStatus(current_pack) != NULLS_ONLY) {
                RCDataTypePtr min_ptr;
                RCDataTypePtr max_ptr;
                GetMinMaxValuesPtrs(current_pack, min_ptr, max_ptr);
                if(!min_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_min), *(RCBString*) (min_ptr.get()), sizeof(_int64));
                if(!max_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_max), *(RCBString*) (min_ptr.get()),
                            (uint)min( (*(RCBString*)(min_ptr.get())).size(), sizeof(_int64)) );
            }

            RCBString min_s("");
            RCBString max_s("");

            ushort max_size = 0;
            for(int i=0; i<packS->NoObjs(); i++) {
                if(!packS->IsNull(i)) {
                    if(max_size < packS->GetSize(i)) {
                        max_size = packS->GetSize(i);
                    }
                }
            }

            // JIRA: DMA-670 last pack of last loading not committed.
            if(packS->NoObjs()+load_obj>MAX_NO_OBJ)
                packS->TruncateObj(MAX_NO_OBJ-load_obj);
            packS->Expand(load_obj);
            bool isnull = false;
            char const* v = 0;
            uint size = 0;
            RCBString zls(ZERO_LENGTH_STRING, 0);
            for(int i = 0; i < load_obj; i++) {
                isnull = true;
                if(isnull && Type().GetNullsMode() == NO_NULLS) {
                    isnull = false;
                    v = ZERO_LENGTH_STRING;
                    size = 0;
                    if(min_s.IsNull() || min_s > zls)
                        min_s = zls;
                    if(max_s.IsNull() || max_s < zls)
                        max_s = zls;
                }
                packS->BindValue(isnull, (uchar*) v, size);
            }

            if(new_pack_created || GetPackOntologicalStatus(current_pack) == NULLS_ONLY) {
                if(!min_s.IsNull())
                    SetPackMin(current_pack, min_s);
                if(!max_s.IsNull())
                    SetPackMax(current_pack, max_s);
                dpn.sum_size = max_size;
            } else {
                if(!min_s.IsNull() && !min_s.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation(), true))
                    SetPackMin(current_pack, min_s);

                if(!max_s.IsNull() && !max_s.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation(), true))
                    SetPackMax(current_pack, max_s);

                if(dpn.sum_size < max_size)
                    dpn.sum_size = max_size;
            }
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
        }
    }

    DPN& dpn(dpns[current_pack]);
    dpn.no_nulls += load_nulls;
    dpn.no_objs = ushort(cur_obj + load_obj - 1);
    // in case of un full pack loading as this buffer fetched end
    partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);
    //SetNoObj(NoObj() + load_obj);
    //FIX DMA-495 DMA-494
    //SetNoObj(  ((_int64)current_pack<<16)+load_obj);
    //FIX DMA-507
    //SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
    // FIX DMA-607: inserted record lost(one row once)
    SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);

    SetNoNulls(NoNulls() + load_nulls);

    if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
        SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
    }


    // 特殊包无需进行调用DoSavePack
    if(dpn.GetNoObj() != GetPackrowSize())
        ((AttrPackS&) *dpn.pack).CopyBinded();

    // pack不能够释放,后面可能会继续合并包用,多节点分布式排序装入用
    bool reset_pack_obj = false;
    SavePack(current_pack,reset_pack_obj);


    if(dpn.GetNoObj() == MAX_NO_OBJ) { // 合并满了,外面没有设置过数据包
        return 1; // 满包了,外面调整current_pack用
    }

    return 0;
}



// 单个字符串类型数据包的合并装入过程
int RCAttrLoadBase::load_one_packs(AttrPack* one_pack,const bool first_merge,bool pack_already_prepared)
{

    InitKNsForUpdate();

    assert(one_pack->NoObjs() > 0);

    if(first_merge) { // fix dma-1359
        // LoadPackInfoForLoader();
        LockPackForUse(NoPack() - packs_omitted - 1);
    }

    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();

    int load_nulls = 0;
    AttrPackS * one_packs = (AttrPackS *)one_pack;
    int load_obj = one_packs->NoObjs();
    /* adjust partition loading */

    int current_pack = NoPack() - packs_omitted - 1;

    // fix dma-1426:如果包为空就加载到内存
    load_current_pack(current_pack);

    // dma-521 retore origin code
    //int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
    uint cur_obj = (uint) dpns[current_pack].GetNoObj();

    // 单个数据包的合并流程,必须满足合并的数据包记录数不能超过65536
    assert(cur_obj + load_obj <= 65536);

    if(PackType() == PackS) {

        DPN& dpn(dpns[current_pack]);
        for(int i = 0; i < one_packs->NoObjs(); i++) {
            if(one_pack->IsNull(i)) {
                if(Type().GetNullsMode() == NO_NULLS) {
                    illegal_nulls = true;
                } else
                    load_nulls++;
            }
        }
        //dpn.natural_save_size = uint(nvs->SumarizedSize());
        //SetNaturalSizeSaved(GetNaturalSizeSaved() + nvs->SumarizedSize());
        AttrPackS*& packS = ((AttrPackS*&) dpn.pack);

        if(load_nulls == one_pack->NoObjs() && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
            // no need to store any values - uniform package
            dpn.pack_mode = PACK_MODE_TRIVIAL;
            dpn.is_stored = false;
            dpn.pack_file = PF_NULLS_ONLY;
            dpn.local_min = GetDomainInjectionManager().GetCurrentId();
            dpn.local_max = -1;
            dpn.sum_size = 0;
        } else {
            if(dpn.pack_file == PF_NULLS_ONLY)
                dpn.local_min = 0;
            BHASSERT(packS, "'packS' should not be null"); //pack object must exist
            SetUnique(false); // we will not check uniqueness for strings now
            SetUniqueUpdated(false);
            if(cur_obj == 0 || dpn.pack_mode == 0 || dpn.pack_mode == 3) { // new package (also in case of expanding so-far-null package)
                packS->Prepare(dpn.no_nulls); // data size: one byte for each null, plus more...
                dpn.pack_mode = PACK_MODE_IN_MEMORY;
                dpn.is_stored = true;
                dpn.pack_file = PF_NOT_KNOWN;
            }
            if(!new_pack_created && current_pack == 0 && dpn.local_min == 0 && dpn.local_max == -1
               && GetPackOntologicalStatus(current_pack) != NULLS_ONLY) {
                RCDataTypePtr min_ptr;
                RCDataTypePtr max_ptr;
                GetMinMaxValuesPtrs(current_pack, min_ptr, max_ptr);
                if(!min_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_min), *(RCBString*) (min_ptr.get()), sizeof(_int64));
                if(!max_ptr->IsNull())
                    strncpy((uchar*) (&dpn.local_max), *(RCBString*) (min_ptr.get()),
                            (uint)min( (*(RCBString*)(min_ptr.get())).size(), sizeof(_int64)) );
            }

            RCBString min_s("");
            RCBString max_s("");
            ushort max_size = 0;

            for(int i=0; i<one_packs->NoObjs(); i++) {
                if(!one_packs->IsNull(i)) {
                    RCBString str(one_packs->GetVal(i),one_packs->GetSize(i));
                    if(min_s > str) {
                        min_s = str;
                    }
                    if(max_s < str) {
                        max_s = str;
                    }

                    if(max_size < one_packs->GetSize(i)) {
                        max_size = one_packs->GetSize(i);
                    }
                }
            }
            // nvs->GetStrStats(min_s, max_s, max_size);


            // JIRA: DMA-670 last pack of last loading not committed.
            if(packS->NoObjs()+one_packs->NoObjs()>MAX_NO_OBJ)
                packS->TruncateObj(MAX_NO_OBJ-one_packs->NoObjs());
            packS->Expand(one_packs->NoObjs());
            bool isnull = false;
            char const* v = 0;
            uint size = 0;
            RCBString zls(ZERO_LENGTH_STRING, 0);
            for(int i = 0; i < one_packs->NoObjs(); i++) {
                isnull = one_packs->IsNull(i);
                if(isnull && Type().GetNullsMode() == NO_NULLS) {
                    isnull = false;
                    v = ZERO_LENGTH_STRING;
                    size = 0;
                    if(min_s.IsNull() || min_s > zls)
                        min_s = zls;
                    if(max_s.IsNull() || max_s < zls)
                        max_s = zls;
                } else {
                    v = one_packs->GetVal(i);
                    size = one_packs->GetSize(i);
                }
                packS->BindValue(isnull, (uchar*) v, size);
            }

            if(new_pack_created || GetPackOntologicalStatus(current_pack) == NULLS_ONLY) {
                if(!min_s.IsNull())
                    SetPackMin(current_pack, min_s);
                if(!max_s.IsNull())
                    SetPackMax(current_pack, max_s);

                if(dpn.sum_size < max_size) // fix dma-1647
                    dpn.sum_size = max_size;

            } else {
                if(!min_s.IsNull() && !min_s.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation(), true))
                    SetPackMin(current_pack, min_s);

                if(!max_s.IsNull() && !max_s.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation(), true))
                    SetPackMax(current_pack, max_s);

                if(dpn.sum_size < max_size)
                    dpn.sum_size = max_size;
            }
            dpn.pack_mode = PACK_MODE_IN_MEMORY;
            dpn.is_stored = true;
        }
    }

    DPN& dpn(dpns[current_pack]);
    dpn.no_nulls += load_nulls;
    dpn.no_objs = ushort(cur_obj + load_obj - 1);
    // in case of un full pack loading as this buffer fetched end
    partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);
    //SetNoObj(NoObj() + load_obj);
    //FIX DMA-495 DMA-494
    //SetNoObj(  ((_int64)current_pack<<16)+load_obj);
    //FIX DMA-507
    //SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
    // FIX DMA-607: inserted record lost(one row once)
    SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);

    SetNoNulls(NoNulls() + load_nulls);

    if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
        SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
    }

    if(dpn.GetNoObj() != GetPackrowSize())
        ((AttrPackS&) *dpn.pack).CopyBinded();

    // pack不能够释放,后面可能会继续合并包用,多节点分布式排序装入用
    bool reset_pack_obj = false;
    SavePack(current_pack,reset_pack_obj);

    if(dpn.GetNoObj() == MAX_NO_OBJ) { // 合并满了,外面没有设置过数据包
        return 1; // 满包了,外面调整current_pack用
    }

    return 0;
}


// 分布式排序过后的数据进行合并装入表中
//
// sorter cluster 生成的dpn,rsi文件格式调整
// 1. sorter cluster 生成的DPN调整为[pack1][pack2][pack3]......[packn]
// 2. sorter cluster 生成的RSI调整为[header][pack1][pack2][pack3]......[packn]
// 3. 数据的合并过程思路
//   a>. 需要合并数据包的过程,走bhloader原流程进行合并
//   b>. 不需要合并数据包的过程,直接拼接dpn,rsi,pack文件即可。
// 4. 数据包生成,每一个装入节点生成的数据包格式如下:

// ------------------------------------------------------------------------------
// 分布式排序的每一个节点生成的数据包
// [Start_pack][full_pack][full_pack]......[full_pack][full_pack][End_pack]
//
// 其中：
// Start_pack：每个节点生成的第一个包,可能是满的，也可以是非满的，不是满的情况下，需要走BHloader的单个数据包的合并流程
// full_pack：满的数据包，该部分也可能不存在，如果存在就走之间数据包合并过程
// End_pack：每个节点生成的最后一个包,可能是满的，也可以是非满的，也可能不存在，如果是非满的需要走BHloader的单个数据包的合并流程，满的就走直接文件合并流程

int RCAttrLoadBase::merge_table_from_sorted_data(const std::string& sorted_data_path,
        const std::string& sorted_sessionid,
        const std::string& datapart_name,
        const _int64& rownums)
{
    LoadPackInfoForLoader();
    InitKNsForUpdate();

    // 加载包索引,合并包索引用
    if(load_data_from_truncate_partition) {
        LoadPackIndexForLoader(datapart_name.c_str(),GetSaveSessionId(),false,1);
    } else {
        LoadPackIndexForLoader(datapart_name.c_str(),GetSaveSessionId());
    }

    // dpn 大小
    const int conDPNSize = 37;

    // rsi 大小
    int conRSISize = 0;  // rsi 包的大小
    int conRSIHeaderSize = 0;
    bool conCMAPtype = false;
    if(this->PackType() == PackN) {
        conRSISize = 32*sizeof(int);
        conRSIHeaderSize = 15;
        conCMAPtype = false;
    } else {
        // fix dma-1383 : conRSISize = 64*32;
        conRSISize = 32 * (Type().GetPrecision() > 64 ? 64 : Type().GetPrecision());
        conRSIHeaderSize = 14;
        conCMAPtype = true;
    }

    // 待合并的pack文件句柄
    IBFile save_pack_fn_handle;
    bool   save_pack_fn_handle_open = false;

    // 更新获取分区信息
    if(load_data_from_truncate_partition) {
        partitioninfo->Set_TruncatePartName(datapart_name.c_str());
        partitioninfo->load(load_data_from_truncate_partition);
    }

    // 读取pack文件包的缓存,PACKN/PACKS共用,如果长度不够:read_one_packn/read_one_packs中自动扩展
    uint _load_pack_buff_size = 0;
    char* _load_pack_buff = NULL;
    _load_pack_buff_size = 65536 * Type().GetPrecision() + PACK_EXTERNAL_SIZE;
    _load_pack_buff = (char*)malloc(_load_pack_buff_size) ;

    // 1. 获取DPN,RSI,PACK文件列表
    std::vector<node_sorted_data_file> _sorted_data_file_info_lst;

    // 获取DPN,RSI,PACK文件列表信息和数据包数
    int ret = get_sorted_data_file_info(this->attr_number,
                                        sorted_sessionid,
                                        sorted_data_path,
                                        _sorted_data_file_info_lst);

    if(rownums == 0) {

        std::string err_msg;
        err_msg  = "Info : merge sorted data to partition ";
        err_msg += path;
        err_msg += "-->";
        err_msg += datapart_name;
        err_msg += ",merge 0 rows.";

        rclog << lock << err_msg << unlock;

        if(_load_pack_buff != NULL) {
            free(_load_pack_buff);
            _load_pack_buff = NULL;
            _load_pack_buff_size = 0;
        }

        return 0;

    } else {
        if(ret == 0) { // 如果没有获取到排序文件,直接返回错误

            std::string err_msg;
            err_msg  = "ERROR : merge sorted data to partition ";
            err_msg += path;
            err_msg += "-->";
            err_msg += datapart_name;
            err_msg += ",can not find sorted data fils[*.dpn,*.rsi,*.pck].";

            rclog << lock << err_msg << unlock;

            if(_load_pack_buff != NULL) {
                free(_load_pack_buff);
                _load_pack_buff = NULL;
                _load_pack_buff_size = 0;
            }

            throw DatabaseRCException(err_msg);
        }
    }

    _int64 laod_sorted_data_check_rows = 0; // 校验dpadmin提交的记录数与后台处理的记录数是否一致

    // 获取总的包数目和更新单个节点的包的数目
    int   all_pack_number = get_pack_num_from_file_info(_sorted_data_file_info_lst); // 单次会话提交的总的数据包数目

    bool  need_to_merge_pack = false;     // 需要合并pack数据包
    bool  need_to_skip_pack = false;      // 是否跳过已经合并过数据包

    bool  need_to_create_new_pack_file = false; // 是否需要创建一个新的pack文件
    attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

    if( total_p_f == 0) { // 更新第一个文件编号
        total_p_f++;
    }

    // 记录当前包的起始位置
    int _save_pack_fn_idx = 0 ;                 // 记录当前数据包保存的文件编号

    /*
    if(!load_data_from_truncate_partition) {
         int  current_pack_addr = 0;                // 记录当前数据包的存储位置
         current_pack_addr = GetSavePosLoc(1-GetCurSaveLocation());
         _save_pack_fn_idx = GetSaveFileLoc(1-GetCurSaveLocation());

         // 调整写入位置,以便合并写入使用
         SetSavePosLoc(GetCurSaveLocation(),current_pack_addr);
         SetSaveFileLoc(GetCurSaveLocation(),_save_pack_fn_idx);

     } else {
         _save_pack_fn_idx = GetSaveFileLoc(GetCurSaveLocation());
     }
     */
    _save_pack_fn_idx = GetSaveFileLoc(GetCurSaveLocation());
    merge_packidx_packno_init = 0; // 合并包索引需要跳过的包号

    // 确定新合并装入分区数据的编号
    if(NoPack()>0 &&  (savepart.getsavepack()!=NoPack()-1|| savepart.isempty())) { // 新生成分区文件
        int   lastf = GetTotalPackFile();  // pack 文件编号
        lastf=GetTotalPackFile();
        while(DoesFileExist( AttrPackFileNameDirect(attr_number, lastf, path))) {
            if(partitioninfo->GetLastFile()<lastf) {
                break;
            }
            lastf++;
        }
        SetTotalPackFile(lastf);
        _save_pack_fn_idx = lastf;

        // 调整文件编号和文件位置,从读取的位置上接着写入,有可能是从长会话上进来的数据合并
        SetSaveFileLoc(GetCurSaveLocation(), GetTotalPackFile());
        SetSavePosLoc(GetCurSaveLocation(), 0);

        need_to_create_new_pack_file = true;

        // 合并包索引的初始化跳过包号,新分区要跳过的包号
        merge_packidx_packno_init = NoPack();

    } else if(NoPack() == 0) { // 空表

        // 调整文件编号和文件位置
        if(!load_data_from_truncate_partition) {
            SetTotalPackFile(1);
        }
        SetSavePosLoc(GetCurSaveLocation(), 0);
        SetSaveFileLoc(GetCurSaveLocation(), GetTotalPackFile());

        need_to_create_new_pack_file = true;

        // 合并包索引的初始化跳过包号,空表为0
        merge_packidx_packno_init = 0;

    } else if(NoPack()>0) {

        if(savepart.getlastpackobjs()%MAX_NO_OBJ !=0 ) {

            need_to_merge_pack = true; // 需要合并单个包的流程
            // 该处无需调整文件编号和文件位置,need_to_merge_pack流程中会调整

            // 合并包索引的初始化跳过包号,最后一个包上会继续合并数据
            merge_packidx_packno_init = NoPack()-1;

        } else {
            need_to_merge_pack = false;

            // 合并包索引的初始化跳过包号,最后一个包刚好满了
            merge_packidx_packno_init = NoPack();
        }
    }

    // 所有节点的数据同时合并
    int node_number = _sorted_data_file_info_lst.size();
    for(int node_idx = 0 ; node_idx<node_number; node_idx++) {

        // 单个节点排序后生成的数据包的数目
        int _sorted_pack_no = _sorted_data_file_info_lst[node_idx].pack_no;

        if(_sorted_pack_no == 0) { // fix dma-1371: 分布式装入时，若部分节点实际无数据输出，则合并时可能会出错
            char _log_msg[1024];

            sprintf(_log_msg, "Info : Merge sorted node[%d]'s data to table[%s] ,partition[%s], attr[%d] from dpn files [%s], merge empty file .",
                    node_idx,path.c_str(),datapart_name.c_str(),attr_number,
                    _sorted_data_file_info_lst[node_idx].dpn_name.c_str());

            rclog << lock << std::string(_log_msg) <<unlock;

            continue;
        }

        // 需要读取的pack文件句柄及名称
        IBFile _sorted_pack_fn_handle;
        bool _switch_sorted_pack_fn = false;
        bool _sorted_pack_fn_is_open = false;
        int _sorted_pack_fn_index = 0;          // 排序后的文件下标

        char _dpn_buf[38];                        // 读取DPN用的buff
        int _cur_sorted_pack_index = 0;         // 单个节点的pack文件下标
        bool _single_dpn_last_pack = false;     // 单个节点的最后一个数据包
        int _cur_pack_file_index = -1;          // 用于切换是否切换排序后的pack文件
        int _last_pack_size = 0;

        // 排序生成的dpn文件
        IBFile _sorted_dpn_fn_handle;           // 存储排序过后生成的DPN文件句柄
        _sorted_dpn_fn_handle.OpenReadOnly(_sorted_data_file_info_lst[node_idx].dpn_name);

        // 排序生成的rsi文件
        IBFile _sorted_rsi_fn_handle;           // 存储排序过后生成的RSI文件句柄
        char _rsi_pack_buf[2049]; // conRSISize+1
        char _rsi_header[16]; // hist[15],cmap[14]
        _sorted_rsi_fn_handle.OpenReadOnly(_sorted_data_file_info_lst[node_idx].rsi_name);
        _sorted_rsi_fn_handle.Read(_rsi_header,conRSIHeaderSize);


        if(need_to_merge_pack) { // 如果第一个包不满的情况下,需要合并第一个包的流程,走原来dataload流程

            // 排序生成的dpn
            char _sorted_pack_dpn[100];
            _sorted_dpn_fn_handle.ReadExact(_sorted_pack_dpn,conDPNSize);
            char* _sorted_buf = _sorted_pack_dpn;  // 第一个dpn

            DPN _sorted_dpn;
            RCAttr::RestoreDPN(_sorted_buf,_sorted_dpn);

            // 记录单个包中的记录数
            laod_sorted_data_check_rows +=_sorted_dpn.no_objs;
            laod_sorted_data_check_rows +=1;

            // 合并包的过程,数据包已经创建好了,无需再进行创建,最后一个参数为true
            bool first_merge = (node_idx==0)?true:false;

            if(_sorted_dpn.is_stored) { // 一般的数据包(不是全null及所有值相同的数据包)

                // 获取pack数据,准备合并
                AttrPackPtr one_pack = this->GetAllocator()->lockedAlloc(PackCoordinate(table_number, attr_number, 0,0,0));

                // 打开读取第一个包解压放入内存
                _sorted_pack_fn_handle.OpenReadOnly(_sorted_data_file_info_lst[node_idx].pack_name_lst[_sorted_pack_fn_index]);
                _sorted_pack_fn_handle.Seek(_sorted_dpn.pack_addr,SEEK_SET);
                one_pack.get()->LoadData((IBStream*)&_sorted_pack_fn_handle);

                _sorted_pack_fn_is_open = true; //_sorted_pack_fn_handle.Close();

                one_pack->Uncompress(this->GetAllocator()->GetDomainInjectionManager());

                if(PackType() == PackN) {
                    ret = load_one_packn(one_pack.get(),_sorted_dpn.local_min,_sorted_dpn.local_max,first_merge,true);
                } else {
                    ret = load_one_packs(one_pack.get(),first_merge,true);
                }

            } else { // 特殊数据包,全null或者最大最小值相同的
                if(PackType() == PackN) {
                    ret = load_one_spec_packn(_sorted_dpn.no_nulls,_sorted_dpn.no_objs+1,_sorted_dpn.local_min,_sorted_dpn.local_max,first_merge,true);
                } else {
                    // fix dma-1422
                    assert(_sorted_dpn.no_nulls == _sorted_dpn.no_objs+1);
                    ret = load_one_spec_packs(_sorted_dpn.no_nulls,first_merge,true);
                }
            }


            //-----------------------------------
            // 可能存在多个节点的包都不是满的情况下
            int current_pack = 0;
            if(NoPack()>0) {
                current_pack = NoPack() - packs_omitted - 1;
            } else {
                current_pack = 0;
            }
            DPN& cur_dpn(dpns[current_pack]);

            if( !cur_dpn.is_stored && !_sorted_dpn.is_stored) { // try to fix dma-1516
                if(cur_dpn.GetNoObj() == cur_dpn.no_nulls) {
                    cur_dpn.pack_file = PF_NULLS_ONLY;
                }
            }

            if(cur_dpn.GetNoObj() != MAX_NO_OBJ) { // try to fix dma-1322
                need_to_merge_pack = true;
            } else {
                need_to_merge_pack = false;
            }

            need_to_skip_pack = true; // 合并过包了,需要跳过

            // 校验数据包的起始位置,新合并生成的文件起始位置不能比之前的小: try to fix dma-1581
            if(current_pack>0) {
                DPN& lst_dpn(dpns[current_pack-1]); // 上一个dpn
                if(_save_pack_fn_idx >0 &&
                   cur_dpn.pack_file == GetSaveFileLoc(GetCurSaveLocation()) &&
                   GetSavePosLoc(GetCurSaveLocation()) > cur_dpn.pack_addr &&
                   cur_dpn.is_stored &&
                   cur_dpn.pack_addr == 0 &&
                   cur_dpn.GetNoObj()== MAX_NO_OBJ &&
                   NoPack() > 1 &&
                   lst_dpn.is_stored &&
                   lst_dpn.pack_file == cur_dpn.pack_file) {

                    rclog << lock << "cur_dpn.pack_file == _save_pack_fn_idx && current_pack_addr > cur_dpn.pack_addr check error" << unlock;
                    assert(0);
                }
            }

            // 重新调整包编号
            if(cur_dpn.pack_file>=_save_pack_fn_idx) {
                // 最后一个数据包,有可能是特殊数据包,pack_file可能是{-1,-2,-3}
                // 也可能是邋DoSavePack内部进行的文件切换了
                _save_pack_fn_idx = cur_dpn.pack_file;
            }

        }

        // 需要保存的pack文件句柄及名称
        if(_save_pack_fn_idx <= 0) {
            _save_pack_fn_idx = total_p_f;
        }
        std::string _save_pack_fn = RCAttr::AttrPackFileNameDirect(this->attr_number,
                                    _save_pack_fn_idx,
                                    this->path);

        if(!save_pack_fn_handle_open) {
            if(need_to_create_new_pack_file) {
                save_pack_fn_handle.OpenCreateEmpty(_save_pack_fn);
            } else {
                if(DoesFileExist(_save_pack_fn)) {
                    save_pack_fn_handle.OpenReadWrite(_save_pack_fn);
                } else {
                    save_pack_fn_handle.OpenCreateEmpty(_save_pack_fn);
                }
            }
            save_pack_fn_handle_open = true;
        }

        // 当前的数据包编号
        int current_pack = 0;
        if(NoPack()>0) {

            if(need_to_merge_pack) {
                current_pack = NoPack() - packs_omitted - 1;

                // 下一个包要走单个包的合并流程,不用调整位置

            } else {
                current_pack = NoPack() - packs_omitted;

                // 不是新生成的文件,跳转到上一个包的写入的最后的位置,进行合并写入数据
                int  fix_pack_no = current_pack -1;

                DPN& last_dpn(dpns[fix_pack_no]);

                if(!need_to_create_new_pack_file ) {// 如果是新的pack文件则不需要跳转到文件位置
                    // 上一次保存后的文件写入位置,直接拼接即可
                    save_pack_fn_handle.Seek(GetSavePosLoc(GetCurSaveLocation()),SEEK_SET);

                } else {
                    need_to_create_new_pack_file = false;
                }
            }
        } else {
            current_pack = 0;
            need_to_create_new_pack_file = false;
        }

        // -------------------------------------------------------------
        // 不需要合并数据包,只需要读取拼接文件
        // -------------------------------------------------------------
        {
            char _log_msg[1024];
            sprintf(_log_msg, "Info : merge sorted data to table[%s],partition[%s],attr[%d] from dpn files[%s].",
                    path.c_str(),datapart_name.c_str(),attr_number,_sorted_data_file_info_lst[node_idx].dpn_name.c_str());

            rclog << lock << std::string(_log_msg) << unlock;

            while(_cur_sorted_pack_index < _sorted_pack_no) { // 开始遍历单个dpn文件

                if(_cur_sorted_pack_index == _sorted_pack_no - 1) { // 单个节点的最后一个数据包
                    _single_dpn_last_pack = true;
                }

                if(need_to_skip_pack && !_single_dpn_last_pack) { // 第一个包已经合并过了,需要跳过该包不进行合并处理

                    //_sorted_dpn_fn_handle.Seek(conDPNSize,SEEK_CUR); // fix dma-1341,已经读取过一个了,在前面就
                    _sorted_rsi_fn_handle.Seek(conRSISize,SEEK_CUR);
                    _cur_sorted_pack_index++;
                    need_to_skip_pack = false;

                    continue;

                }

                if(need_to_skip_pack && _single_dpn_last_pack) { // 只有一个包的情况,已经在if(need_to_merge_pack) 流程中处理
                    _cur_sorted_pack_index++;
                    need_to_skip_pack = false;
                    break;
                }

                ret =_sorted_dpn_fn_handle.Read(_dpn_buf,conDPNSize);
                if(ret == conDPNSize) { // 读取成功，判断是否是有效包

                    bool for_mysqld_insert =false;
                    int  for_mysqld_merge = 1;
                    CreateNewPackage(for_mysqld_insert,for_mysqld_merge);    // 扩展DPN,创建包对象

                    DPN& _sorted_dpn(dpns[current_pack]);

                    RCAttr::RestoreDPN(_dpn_buf,_sorted_dpn,false);

                    // 记录单个包中的记录数
                    laod_sorted_data_check_rows +=_sorted_dpn.no_objs;
                    laod_sorted_data_check_rows +=1;

                    if(!_single_dpn_last_pack && _cur_sorted_pack_index >0 ) {
                        assert(_sorted_dpn.no_objs == 0xffff);  // 不是第一个包和最后一个包,必须要保证所有包都是满的
                    }

                    // pack 文件的更新
                    if(_sorted_dpn.is_stored == true) {

                        if(_cur_pack_file_index == -1) {
                            _cur_pack_file_index = _sorted_dpn.pack_file;
                        }
                        if(_cur_pack_file_index != _sorted_dpn.pack_file) { // 切换下一个文件
                            _switch_sorted_pack_fn = true;
                            _cur_pack_file_index = _sorted_dpn.pack_file;
                        }

                        assert(_sorted_dpn.no_objs >=_sorted_dpn.no_nulls);

                        // 切换save的pack文件
                        if( ( _save_pack_fn_idx >= GetSaveFileLoc(GetCurSaveLocation()) &&
                              GetSavePosLoc(GetCurSaveLocation()) > file_size_limit) ||
                            _save_pack_fn_idx < GetSaveFileLoc(GetCurSaveLocation()) ) { // DoSavePack 中到达切换文件大小,这里也是要更新的

                            _save_pack_fn_idx++;


                            _save_pack_fn = RCAttr::AttrPackFileNameDirect(this->attr_number,
                                            _save_pack_fn_idx,
                                            this->path);

                            rclog << lock <<"switch pack file : ["<< _save_pack_fn << "]" << unlock;

                            total_p_f++; // 文件数增加

                            save_pack_fn_handle.Close();
                            save_pack_fn_handle_open = false;

                            save_pack_fn_handle.OpenCreateEmpty(_save_pack_fn);
                            save_pack_fn_handle_open = true;

                            SetSaveFileLoc(GetCurSaveLocation(),_save_pack_fn_idx);
                            SetSavePosLoc(GetCurSaveLocation(),0);
                        }

                        if(!_sorted_pack_fn_is_open) { // 第一次打开
                            assert(_sorted_pack_fn_index<_sorted_data_file_info_lst[node_idx].pack_name_lst.size());
                            _sorted_pack_fn_is_open = true;
                            _sorted_pack_fn_handle.OpenReadOnly(_sorted_data_file_info_lst[node_idx].pack_name_lst[_sorted_pack_fn_index]);
                        }
                        if(_switch_sorted_pack_fn) { // 切换文件
                            _sorted_pack_fn_index++;
                            _sorted_pack_fn_handle.Close();
                            std::string _f = _sorted_data_file_info_lst[node_idx].pack_name_lst[_sorted_pack_fn_index];
                            if(DoesFileExist(_f)) {
                                _sorted_pack_fn_handle.OpenReadOnly(_f);
                            } else {
                                sprintf(_log_msg,"Error : PackFile [%s] is not exist , can not merge pack file .",_f.c_str());
                                rclog << lock << std::string(_log_msg) << unlock;
                                throw InternalRCException(_log_msg);
                            }

                            _switch_sorted_pack_fn = false;
                        }

                        {
                            // 读取文件,直接拼接文件

                            // 不是所有包的都存储pack文件的,fix dma-1417
                            _sorted_pack_fn_handle.Seek(_sorted_dpn.pack_addr,SEEK_SET);

                            int read_pack_size = 0;
                            if(PackType() == PackN) {
                                ret = this->read_one_packn(_sorted_pack_fn_handle,_load_pack_buff,_load_pack_buff_size,read_pack_size);
                            } else {
                                ret = this->read_one_packs(_sorted_pack_fn_handle,_load_pack_buff,_load_pack_buff_size,read_pack_size);
                            }

                            if(ret != read_pack_size) {
                                std::string err_msg;
                                err_msg  = "ERROR : merge sorted data to partition ";
                                err_msg += path;
                                err_msg += "-->";
                                err_msg += datapart_name;
                                err_msg += ",read file [";
                                err_msg += _sorted_data_file_info_lst[node_idx].pack_name_lst[_sorted_pack_fn_index];
                                err_msg += "] occur error.";
                                rclog << lock << err_msg << unlock;

                                if(_load_pack_buff != NULL) {
                                    free(_load_pack_buff);
                                    _load_pack_buff = NULL;
                                    _load_pack_buff_size = 0;
                                }

                                throw DatabaseRCException(err_msg);
                            }

                            // 合并读入的数据
                            save_pack_fn_handle.Seek(GetSavePosLoc(GetCurSaveLocation()),SEEK_SET);
                            save_pack_fn_handle.Write(_load_pack_buff,read_pack_size);

                            // 保存DPN中的信息
                            _sorted_dpn.pack_file = GetSaveFileLoc(GetCurSaveLocation());
                            _sorted_dpn.pack_addr = GetSavePosLoc(GetCurSaveLocation());

                            // 下一个包的起始位置
                            SetSaveFileLoc(GetCurSaveLocation(),GetSaveFileLoc(GetCurSaveLocation()));

                            int next_save_pos = GetSavePosLoc(GetCurSaveLocation())+read_pack_size;
                            SetSavePosLoc(GetCurSaveLocation(),next_save_pos);

                        }
                    } else {
                        _sorted_dpn.pack_mode =  PACK_MODE_TRIVIAL;
                        if(_sorted_dpn.local_max == _sorted_dpn.local_min) {
                            assert(_sorted_dpn.no_nulls ==0 );
                            //_sorted_dpn.pack_file = PF_NOT_KNOWN;
                        } else {
                            assert(_sorted_dpn.no_nulls ==_sorted_dpn.no_objs+1);
                            //_sorted_dpn.pack_file = PF_NULLS_ONLY;
                        }

                        // 记录上一个正确的pack的位置,以便下次合并数据包的时候有正确的位置
                        // 如果不设置,从null上合并非空就会重写覆盖dpn,dma-1581
                        _sorted_dpn.pack_addr = GetSavePosLoc(GetCurSaveLocation());

                        // fix dma-1601
                        if(_sorted_dpn.pack_file>=0) {
                            _sorted_dpn.pack_file = GetSaveFileLoc(GetCurSaveLocation());
                        }
                    }

                    // 更新分区信息
                    int not_full_pack = 0;
                    if(this->partitioninfo) {
                        attr_partinfo &curpart=this->partitioninfo->getsavepartinfo(datapart_name.c_str());

                        not_full_pack = curpart.newpack(NoPack());

                        // FIX DMA-1334
                        // 错误原因：RCAttrLoadBase::load_data_packn/RCAttrLoadBase::load_data_packs 中调用
                        // RCAttrLoad::DoSavePack的时候的pack编号是调整过的：NoPack()-packs_omitted，取值范围是[0-127]
                        // 但是，RCAttrLoadBase::merge_table_from_sorted_data 中的n是没有调整过的，因此此处不能直接使用：packs_omitted 进行修正编号。
                        curpart.setsavepos(this->no_pack /*+packs_omitted*/ ,_sorted_dpn.no_objs+1);

                        if(_sorted_dpn.is_stored && _sorted_dpn.pack_file>0 && _sorted_dpn.pack_file>curpart.lastfile())
                            curpart.append(_sorted_dpn.pack_file);

                        curpart.setsavefilepos(_sorted_dpn.pack_addr);
                    }

                    // 更新列信息
                    {
                        SetNoPack(NoPack()+1);
                        SetNoObj((this->no_pack-1)*MAX_PACK_ROW_SIZE +_sorted_dpn.GetNoObj());
                        SetNoNulls(NoNulls()+_sorted_dpn.no_nulls);
                    }

                    // 合并dpn
                    // dpns[current_pack] = _sorted_dpn;
                    // replace by following code :
                    // DPN& _sorted_dpn(dpns[current_pack]);

                    // 读取rsi包数据
                    ret = _sorted_rsi_fn_handle.Read(_rsi_pack_buf,conRSISize);
                    assert(ret == conRSISize);

                    // 合并rsi
                    CopyRSI_Hist(current_pack,_rsi_pack_buf,conRSISize);
                    CopyRSI_CMap(current_pack,_rsi_pack_buf,conRSISize);

                    if(_single_dpn_last_pack) {
                        // 关闭dpn文件和rsi文件pack文件,新的下一个数据包要在走单个数据包的合并流程
                        if(_sorted_dpn.GetNoObj() != MAX_NO_OBJ) { // try to fix dma-1322
                            need_to_merge_pack = true;
                            need_to_skip_pack = false;

                            // 下一个节点的第一个数据包需要合并,这时候需要将当前的包再次重新加载到内存中
                            // 如果该包是通过直接拷贝数据包的流程,那么dpns中的数据包对象是没有有效加载到内存的
                            // 下一个包走合并的流程的时候,就没有对应的数据记录

                            if(_sorted_dpn.is_stored && node_idx != (node_number -1)) { // 最后一个阶段的最后一个包不需要加载

                                AttrPack*& packX = (AttrPack*&)_sorted_dpn.pack;
                                assert(packX != NULL);

                                save_pack_fn_handle.Seek(_sorted_dpn.pack_addr,SEEK_SET);
                                packX->LoadData((IBStream*)&save_pack_fn_handle);
                                packX->Uncompress(this->GetAllocator()->GetDomainInjectionManager());

                                assert(_sorted_dpn.GetNoObj() == packX->NoObjs());

                            }

                            // 需要关闭pack文件
                            save_pack_fn_handle.Close();
                            save_pack_fn_handle_open = false;
                        }

                        current_pack ++; // 当前包号也要改变,fix dma-1361

                    } else {
                        // 用完的数据包要释放掉,否则会泄露的,参考:DoSavePack流程
                        if(_sorted_dpn.GetNoObj() == MAX_NO_OBJ) {
                            UnlockPackFromUse(current_pack);

                            current_pack ++; // 当前包号也要改变,fix dma-1361
                        } else {
                            // 如果第一个包就被其他分区隔离开的情况,也需要调整current_pack值
                            if(not_full_pack == 3) {

                                UnlockPackFromUse(current_pack);  // 该包也不会合并使用了

                                current_pack ++;
                            }
                        }
                    }

                } else { // dpn 读取到错误的部分

                    char _log_msg[1024];

                    sprintf(_log_msg, "ERROR : merge sorted data to table[%s] ,partition[%s], attr[%d] from dpn files [%s],read [%d/%d] pack of dpns .",
                            path.c_str(),datapart_name.c_str(),attr_number,
                            _sorted_data_file_info_lst[node_idx].dpn_name.c_str(),
                            _cur_sorted_pack_index,_sorted_pack_no);

                    rclog << lock << std::string(_log_msg) <<unlock;


                    if(_load_pack_buff != NULL) {
                        free(_load_pack_buff);
                        _load_pack_buff = NULL;
                        _load_pack_buff_size = 0;
                    }


                    throw DatabaseRCException(std::string(_log_msg));
                }

                _cur_sorted_pack_index++;

            }// end while(_cur_sorted_pack_index < _sorted_pack_no)

            _sorted_dpn_fn_handle.Close();
            _sorted_rsi_fn_handle.Close();

        }// end of else of if(need_to_merge_pack)

    } // end of for(int node_idx = 0 ;i< node_number ;node_idx++)
    if(save_pack_fn_handle_open) {
        save_pack_fn_handle.Close();
        save_pack_fn_handle_open = false;
    }

    if(_load_pack_buff != NULL) {
        free(_load_pack_buff);
        _load_pack_buff = NULL;
        _load_pack_buff_size = 0;
    }

    if(rownums != laod_sorted_data_check_rows) { // 校验dpadmin 与 排序端的记录记录数

        char _log_msg[1024];

        sprintf(_log_msg, "ERROR : table[%s] ,partition[%s], attr[%d], mysqld merge [%ld] not equal dpadmin [%ld] rows.",
                path.c_str(),datapart_name.c_str(),attr_number,laod_sorted_data_check_rows,rownums);

        rclog << lock << std::string(_log_msg) <<unlock;

        throw DatabaseRCException(std::string(_log_msg));
    }

    // 释放最后一个未满的包,原流程DoSavePack中是存在的
    int _current_pack = NoPack() - packs_omitted - 1;
    DPN& dpn(dpns[_current_pack]);
    if(dpn.GetNoObj() != MAX_PACK_ROW_SIZE) {
        UnlockPackFromUse(_current_pack);
    }

    // 保存分区对象,列对象文件
    // 该函数必须在所有列都合并完成后再保存,否则会出现部分列保存完成,部分列没有保存完成的状况,数据无法回退
    // save_merge_table_header();

    return 0;
}



bool RCAttrLoadBase::truncate_partition_from_mysql(const std::string& datapart_name,std::vector<int> & del_pack_file_list)
{
//  check operation conditions
    int sid=-1;
    uint connid=0;
    if(no_pack<1){
        rclog << lock << "Error: table [ " << path << " ] is empty,can not support merge long session data."<< unlock;
        return false;
    }
//  prepare for trunc
    LoadPackInfo();
    attr_partinfo* apartinfo=&(partitioninfo->getsavepartinfo(datapart_name.c_str()));
    if(apartinfo==NULL){
        rclog << lock << "Error: table [ " << path << " ] get empty attr_partinfo , merge long session data error."<< unlock;
        return false;
    }
    SectionMap pmap;
    int lastobjs=partitioninfo->GetPartSection(pmap,apartinfo->name());
    bool trunc_lastp=apartinfo->getlastpack()==no_pack-1;
// 1. Create New RSI Data file
    std::string knpath=rsi_manager->GetKNFolderPath();
    std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
    std::string newrsifn=rsifn+".ntrunc";
    int new_totalpack=partitioninfo->getpacks()-apartinfo->getpacks();
    _int64 new_totalobjs=(_int64(new_totalpack-1)<<16)+lastobjs;

    if(new_totalobjs <=0 ) { //
        new_totalobjs = 0;
        rclog << lock << "warning: table [ " << path << " ] just has only one pack"<< unlock;
    }

    if(pmap.size() == 0) {
        // 只有一个分区的情况,直接将分区记录数清除即可:
        // 修改记录数之后之后,直接保存
        no_obj=0;
        no_nulls=0;
        i_min=0;
        i_max=0;
        compressed_size=0;
        natural_size_saved=0;
        no_pack=0;
        // total_p_f=0; // 不能将文件编号设置为0,否则会导致文件被覆盖
        savefile_loc[RCAttr::GetCurSaveLocation()]=total_p_f;
        savepos_loc[RCAttr::GetCurSaveLocation()]=0;

        // 生成临时文件
        RCAttr::Save(AttrFileName(file)+".ntrunc", dic.get(), 0);
        dom_inj_mngr.Save();

        // 获取删除文件列表,提交完成后,删除pack文件
        partitioninfo->Set_TruncatePartName(datapart_name.c_str());
        del_pack_file_list.clear();
        partitioninfo->GetPartFileList(del_pack_file_list,apartinfo->name());

        // 调整删除单个短话后,单个长会话的pack编号
        SetNoPackPtr(0);  // fix dma-1370,删除分区后,所有的dpn要重新加载了
        dpns.clear();
        load_data_from_truncate_partition_packs = no_pack;

        // 生成rsi.ntrunc空的文件
        // 该步骤可以省略,没有rsi.ntrunc文件就从0开始

        // 生成dpn.ntrunc空的文件
        // 该步骤可以省略,没有dpn.ntrunc文件就从0开始

        // 生成leveldb.ntrunc空的文件
        // 该步骤可以省略,没有leveldb.ntrunc不影响的

        // 生成partinfo.ntrunc空的文件
        partitioninfo->CreateTruncateFile();

        return true;

    } else { // 该表存在多个分区的情况
        int novalidpack=0;
        //last pack always save ahead in both rsi&dpn
        (*pmap.rbegin()).second--;
        int lastend=(*pmap.rbegin()).second;
        if( NeedToCreateRSIIndex() && DoesFileExist(rsifn)) { // fix dma-1462
            RemoveFile(newrsifn);
            _int64 fsize=0;
            GetFileSize(rsifn,fsize);
            IBFile oldfile;
            IBFile newfile;
            oldfile.OpenReadOnly( rsifn);
            char header[40];
            int headlen=PackType() == PackN?15:14;

            if(fsize==headlen) { // 只有1个header
                oldfile.ReadExact(header,headlen);
                memcpy(header+headlen,header,headlen);
            } else if(fsize>=headlen*2) { // 存在2个header
                oldfile.ReadExact(header,headlen*2);
            }

            _int64 rsiobjs=*(_int64 *)(header + RCAttr::GetCurReadLocation()*headlen+1);
            // last pack at header,so minus 1
            int rsipacks=*(int * )(header + RCAttr::GetCurReadLocation()*headlen+9)-1;
            bool rsihasempty=rsipacks!=NoPack();
            int rsi_packlen=PackType() == PackN?128:(32*(unsigned char )header[13]);
            newfile.OpenCreate( newrsifn);

            // backup current header
            // dma-614:
            // 一个PACK 在数据为空或者数据值相同时,不产生RSI;
            // 因此rsi数据有可能和表中的no_pack,no_obj不一致
            // 但是如果遇到一个需要RSI的pack,则之前空的RSI会补齐.
            //  需要:
            // 1. 调整header中的no_pack,no_obj
            // 2. 正确处理数据量不一致的情况
            //   2.1 tranc empty packs,判断剩余部分是否还有empty
            //   2.2 remain empty packs
            memcpy(header+(1-RCAttr::GetCurReadLocation())*headlen,header+current_read_loc*headlen,headlen);
            int rsileftpacks=0;
            SectionMap::iterator iter;
            for(iter=pmap.begin(); iter!=pmap.end(); iter++) {
                if(iter->first+1>=rsipacks) continue;
                int sectend=min(rsipacks+1,iter->second+1);
                if(sectend==lastend+1) sectend++;
                rsileftpacks+=sectend-iter->first;
            }
            *(_int64 *)(header+RCAttr::GetCurReadLocation()*headlen+1)=rsiobjs-((_int64)(rsipacks-rsileftpacks)<<16);//new_totalobjs;
            *(int *)(header+current_read_loc*headlen+9)=rsileftpacks;//new_totalpack;
            if(fsize>2*headlen) {
                // maximum pack len : 64*32=2048 bytes(CMAP)
                char lastpacks[2048*2];
                oldfile.ReadExact(lastpacks,rsi_packlen*2);
                int first_packstart=headlen*2+rsi_packlen*2;
                // backup current last pack
                if(trunc_lastp) {
                    if(pmap.rbegin()->second+1<=rsipacks) {
                        memcpy(lastpacks+(1-RCAttr::GetCurReadLocation())*rsi_packlen,lastpacks+current_read_loc*rsi_packlen,rsi_packlen);
                        oldfile.Seek((pmap.rbegin()->second)*rsi_packlen,SEEK_CUR);
                        //oldfile.Seek(-rsi_packlen,SEEK_END);
                        oldfile.ReadExact(lastpacks+RCAttr::GetCurReadLocation()*rsi_packlen,rsi_packlen);
                    }
                    // else last pack's rsi is nulls(no rsi)?
                    else memset(lastpacks+RCAttr::GetCurReadLocation()*rsi_packlen,0,rsi_packlen);
                }
                newfile.WriteExact(header, headlen*2);
                newfile.WriteExact(lastpacks, rsi_packlen*2);

                for(iter=pmap.begin(); iter!=pmap.end(); iter++) {
                    if(iter->first+1>=rsipacks) continue;
                    oldfile.Seek(first_packstart+iter->first*rsi_packlen,SEEK_SET);
                    int rsicopypacks=min(rsipacks+1,iter->second+1)-iter->first;
                    //CopyStream(newfile,oldfile,rsi_packlen,iter->second-iter->first+1);
                    CopyStream(newfile,oldfile,rsi_packlen,rsicopypacks);
                }
            } else
                newfile.WriteExact(header, headlen*2);
        }
        //   2. 删除LevelDB索引
        //         <Option> rename directory <Tabnum><AttrNum><PartName>(/_w) --> xxx.trunc;
        //          new header file attrindex_<TABNUM>_<ATTRNUM>.ctb.ntrunc
        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        //ldb_index already has droped contents.
        //keep current state,not remove part but just save a new header
        if(ldb_index) {
            //ldb_index->RemovePart(apartinfo->name());
            ldb_index->SaveHeader(true,apartinfo->name());
        }
        //   3. 删除数据文件 --> xxx.trunc  变更:调整为提交时直接删除
        int fileid=apartinfo->firstfile();
        while(fileid!=-1 && !novalidpack) {
            std::string dtfilename=AttrPackFileNameDirect(attr_number,fileid,path);
            // keep current state,no rename file,but just check
            if(!DoesFileExist(dtfilename)) {
                rclog << lock << "ERROR: Pack data file lost : " << dtfilename << unlock;
                return false;
            }
            //RenameFile(dtfilename, dtfilename+"trunc");
            fileid=apartinfo->nextfile(fileid);
        }
        //   4. 删除DPN --> xxx.ntrunc
        //   TODO: 是否需要重新调整文件编号? 变为连续编号 调整文件名和DPN中的文件号
        std::string dpnfn=DPNFileName();
        std::string newdpnfn=dpnfn+".ntrunc";
        _int64 newmax=i_max,newmin=i_min;
        _int64 newnullsobj=0;
        {
            RemoveFile(newdpnfn);
            IBFile oldfile;
            IBFile newfile;
            oldfile.OpenReadOnly( dpnfn);
            newfile.OpenCreate( newdpnfn);
            char lastpacks[100];
            oldfile.ReadExact(lastpacks,37*2);
            int first_packstart=37*2;
            // backup current last pack
            if(trunc_lastp) {
                memcpy(lastpacks+(1-RCAttr::GetCurReadLocation())*37,lastpacks+current_read_loc*37,37);
                oldfile.Seek((pmap.rbegin()->second)*37,SEEK_CUR);
                oldfile.ReadExact(lastpacks+RCAttr::GetCurReadLocation()*37,37);
            }
            if(lastobjs!=*(unsigned short *)(lastpacks+RCAttr::GetCurReadLocation()*37+32)+1) {
                //FIXME: objs in partition info wrong?
                //reset objs:
                lastobjs=*(unsigned short *)(lastpacks+RCAttr::GetCurReadLocation()*37+32)+1;
                new_totalobjs=(_int64(new_totalpack-1)<<16)+lastobjs;
            }
            newfile.WriteExact(lastpacks, 37*2);
            // initial range value
            newmax=*(_int64 *)(lastpacks+RCAttr::GetCurReadLocation()*37+16);
            newmin=*(_int64 *)(lastpacks+RCAttr::GetCurReadLocation()*37+8);
            SectionMap::iterator iter;
            for(iter=pmap.begin(); iter!=pmap.end(); iter++) {
                char dpnblock[37];
                oldfile.Seek(first_packstart+iter->first*37,SEEK_SET);
                // new nulls objs require decode dpn block
                //CopyStream(newfile,oldfile,37,iter->second-iter->firset+1);
                for(int i=0; i<iter->second-iter->first+1; i++) {
                    oldfile.ReadExact(dpnblock, 37);
                    newfile.WriteExact(dpnblock, 37);
                    if(PF_NULLS_ONLY==*(int *)dpnblock ) {
                        newnullsobj+=*(unsigned short *)(dpnblock+32)+1;
                        continue;
                    } else
                        newnullsobj+=*(unsigned short *)(dpnblock+34);
                    if(!ATI::IsRealType(TypeName())) {
                        if(newmin>*(_int64 *)(dpnblock+8))
                            newmin=*(_int64 *)(dpnblock+8);
                        if(newmax<*(_int64 *)(dpnblock+16))
                            newmax=*(_int64 *)(dpnblock+16);
                    } else {
                        if(*(double*) (&newmin) > *(double*) (dpnblock+8))
                            newmin=*(_int64 *)(dpnblock+8);
                        if(*(double*) (&newmax) < *(double*) (dpnblock+16))
                            newmax=*(_int64 *)(dpnblock+16);
                    }
                }
            }
        }
        //   5. 删除partinfo --> xxx.ntrunc.

        int part_section_list_size = apartinfo->size();
        int *packvars=new int[ /*apartinfo->size()*/part_section_list_size*2];
        std::set<std::string> rebuildparts;
        bool for_mysqld_merge = true; // fix dma-1365,合并长会话生成数据的时候,需要删除分区,此时没有创建删除分区的文件,因此会话id不对,不进行校验会话id
        partitioninfo->Set_TruncatePartName(datapart_name.c_str());

        // 获取删除文件列表,提交完成后,删除pack文件
        del_pack_file_list.clear();
        partitioninfo->GetPartFileList(del_pack_file_list,apartinfo->name());

        partitioninfo->Truncate(connid,packvars,rebuildparts,for_mysqld_merge);
        if(ldb_index) {
            ldb_index->ClearMergeDB();
            ldb_index->RebuildByTruncate(packvars, /*apartinfo->size()*/part_section_list_size,rebuildparts,sid);
        }
        // total_p_f means maximum file id+1 ,not number of files.
        //int new_pack_files=total_p_f-apartinfo->getfiles();
        delete []packvars;
        int new_pack_files=partitioninfo->GetLastFile()+1;

        partitioninfo->CreateTruncateFile();
        partitioninfo->load(); // retore origin part info,
        // 后面再合并文件的时候,merge_table_from_sorted_data函数中会重新加载分区数据的
        //   6. TA<attrnum>.ctl文件 --> xxx.ntrunc
        //    ref : jira dma-516

        // 计算文件存储量
        // 参考:
        //_int64 css = ComputeCompressedSize();
        _int64 tsize = 0;
        _int64 size = 0;

        if(GetFileSize(DPNFileName(), size))
            tsize = size;
        // for all pack file
        // adjust to process partition files

        std::vector<int> filelist;

        int files=partitioninfo->GetFileList( filelist,apartinfo->name());
        int newlastfid=files==0?-3:filelist.back();
        for(int p_f=0; p_f<files; p_f++) {
            if(GetFileSize(RCAttr::AttrPackFileNameDirect(attr_number, filelist[p_f], path), size))
                tsize += size;
        }

        //   dynamic values using:
        // no_obj(64)/no_nulls(64)/no_pack(32)/total_p_f(32)/i_min(64)/i_max(64)/
        // compressed_size(calc by comprs_size 64)/savefile_loc[2]/savepos_loc[2]
        // natural_size_saved: calc by nvs->SumarizedSize()
        // backup attributes for save new header file
        _int64 old_no_obj=no_obj,old_no_nulls=no_nulls,old_i_min=i_min,old_i_max=i_max;
        _uint64 old_compressed_size=compressed_size,old_natural_size_saved=natural_size_saved;
        int old_no_pack=no_pack,old_total_p_f=total_p_f,
            old_savefile_loc=savefile_loc[RCAttr::GetCurSaveLocation()];
        unsigned int old_savepos_loc=savepos_loc[RCAttr::GetCurSaveLocation()];
        // estimate natural size:
        natural_size_saved=natural_size_saved*(double)new_totalobjs/no_obj;
        no_obj=new_totalobjs;
        no_nulls=newnullsobj;
        i_min=newmin;
        i_max=newmax;
        no_pack=new_totalpack;

        // skip A/B/S switch
        //if (current_state == SESSION_READ)
        file = FILE_READ_SESSION;
        //if(current_state == SESSION_WRITE)
        //  file = FILE_SAVE_SESSION;
        BHASSERT(FileFormat() == CLMD_FORMAT_RSC10_ID, "Invalid Attribute data file format!");

        RCAttr::Save(AttrFileName(file)+".ntrunc", dic.get(), tsize);
        dom_inj_mngr.Save();

        SetNoPackPtr(0);  // fix dma-1370,删除分区后,所有的dpn要重新加载了
        dpns.clear();
        load_data_from_truncate_partition_packs = no_pack;

        /*
        //since trans has not committed,restore variabls:
        no_obj=old_no_obj;
        no_nulls=old_no_nulls;
        i_min=old_i_min;
        i_max=old_i_max;
        compressed_size=old_compressed_size;
        natural_size_saved=old_natural_size_saved;
        no_pack=old_no_pack;
        total_p_f=old_total_p_f;
        savefile_loc[RCAttr::GetCurSaveLocation()]=old_savefile_loc;
        savepos_loc[RCAttr::GetCurSaveLocation()]=old_savepos_loc;
        */
        //   7. Table.ctb --> xxx.ntrunc :在上一级调用过程中处理
        //   no_obj in table.ctb always 0(dma-526)
        //   暂忽略这个过程，table.ctb无须变更
        return true;
    }

    return false;
}
// 合并短会话数据
int RCAttrLoadBase::merge_short_session_data(const std::string& sorted_data_path,
        const std::string& sorted_sessionid,
        const std::string& datapart_name,
        const _int64& rownums)
{

    // 短会话数据直接合并数据就可以
    char _log_msg[1024];

    sprintf(_log_msg, "Info : merge st_session sorted data to table[%s],partition[%s],attr[%d],begin to merge partition.",
            path.c_str(),datapart_name.c_str(),attr_number);

    rclog << lock << std::string(_log_msg) << unlock;

    skip_DoSavePack_packidx = true;  // DoSavePack()是否保存包索引

    load_data_from_truncate_partition = false;
    int ret = merge_table_from_sorted_data(sorted_data_path,sorted_sessionid,datapart_name,rownums);

    if(ret != 0) {
        char err_msg[1024];
        sprintf(err_msg,"ERROR: merge_table_from_sorted_data attr_number [%d].",attr_number);

        rclog << lock << err_msg << unlock;
        throw InternalRCException(std::string(err_msg));
    }
    skip_DoSavePack_packidx = false; // DoSavePack()是否保存包索引

    // 3>. 合并生成包索引数据
    if(ct.IsPackIndex() && ldb_index==NULL) {
        ldb_index=new AttrIndex(attr_number,path.c_str());
        ldb_index->LoadHeader();
        ldb_index->LoadForUpdate(datapart_name.c_str(),GetSaveSessionId());
    }
    if(ct.IsPackIndex() && ldb_index != NULL && rownums > 0) {
        ret = merge_packindex_data(sorted_data_path,sorted_sessionid,false);
    }

    return ret;

}

// 合并长会话数据
int RCAttrLoadBase::merge_long_session_data(const std::string& sorted_data_path,
        const std::string& sorted_sessionid,
        const std::string& datapart_name,
        const _int64& rownums)
{
    // 长回话数据合并过程:
    int ret = 0;

    char _log_msg[1024];


    // 1>. 删除原来分区
    sprintf(_log_msg, "Info : merge lt_session sorted data to table[%s],partition[%s],attr[%d],begin to merge partition.",
            path.c_str(),datapart_name.c_str(),attr_number);

    rclog << lock << std::string(_log_msg) << unlock;

    std::vector<int> del_pack_file_list;   // 删除分区的原来的文件列表
    bool truncate_partition = truncate_partition_from_mysql(datapart_name,del_pack_file_list);
    if(!truncate_partition) {
        char err_msg[1024];
        sprintf(err_msg,"ERROR: merge_long_session_data  merge_long_session_data attr_number [%d].",attr_number);
        rclog << lock << err_msg << unlock;
        throw InternalRCException(std::string(err_msg));
    }

    // 2>. 在删除的分区上合并数据
    sprintf(_log_msg, "Info : merge lt_session sorted data to table[%s],partition[%s],attr[%d],begin to merge data.",
            path.c_str(),datapart_name.c_str(),attr_number);

    rclog << lock << std::string(_log_msg) << unlock;

    // 保存数据提交文件,RCTableImpl::CommitSaveSession中提交数据的时候用到
    SaveLongSessionComitFile(datapart_name.c_str(),del_pack_file_list);

    skip_DoSavePack_packidx = true; // DoSavePack()是否保存包索引


    // 设置从已经删除分区的dpn,rsi文件中进行加载数据
    load_data_from_truncate_partition = true;
    SetPackInfoCollapsed(true);
    ret = merge_table_from_sorted_data(sorted_data_path,sorted_sessionid,datapart_name,rownums);
    if(ret != 0) {
        load_data_from_truncate_partition = false;
        char err_msg[1024];

        sprintf(err_msg,"ERROR: merge_table_from_sorted_data from truncate partition error attr_number [%d].",attr_number);
        rclog << lock << err_msg << unlock;
        throw InternalRCException(std::string(err_msg));
    }
    skip_DoSavePack_packidx = false;  // DoSavePack()是否保存包索引

    // 3>. 合并生成包索引数据
    if(ct.IsPackIndex() && ldb_index==NULL) {
        ldb_index=new AttrIndex(attr_number,path.c_str());
        ldb_index->LoadHeader(1,datapart_name.c_str());
        ldb_index->LoadForRead(datapart_name.c_str());
    }
    if(ct.IsPackIndex() && ldb_index != NULL ) {
        merge_packindex_data(sorted_data_path,sorted_sessionid,true);
    }

    load_data_from_truncate_partition = false;

    // 4>. 将删除分区上合并的数据DPN.ntrunc,RSI.ntrunc,T[A/B]xxxxx.ntrunc 提交重命名
    // sprintf(_log_msg, "Info : merge lt_session sorted data to table[%s],partition[%s],attr[%d],internal commit partition.",
    //        path.c_str(),datapart_name.c_str(),attr_number);

    // rclog << lock << std::string(_log_msg) << unlock;

    // 下面步骤放入:RCTableImpl::CommitSaveSession中调用,锁表后提交
    // commit_long_session_merge_data(datapart_name,del_pack_file_list);

    return 0;
}


// 生成包索引数据
/*
    [短会话合并包索引流程]

        1> . LoadPackIndexForLoader 先加载包索引,打开_w库

        2> . merge_packindex_data 合并包索引,向_w库中写入

        3> . RCTableLoad::WritePackIndex() 将原来的数据库+_w库进行合并成_mrg库
             a>. fork 原来的数据库
             b>. merge 原来的数据库和当前正在写的数据库(_w)到新的数据库(_mrg)中

        4> . RCAttr::CommitSaveSession提交数据库,将_mrg重名称成可以使用的数据库



    [长会话合并包索引流程]

        1> . LoadPackIndexForLoader 先加载包索引

        2> . truncate_partition_from_mysql 删除分区
             a>. attrindex_0.ctb ---> attrindex_0.ctb.ltsession ,内部不含有删除的分区
             b>. RebuildByTruncate 调整未删除其他分区的pack编号,并将数据库修改成_mrg库

        3> . LoadPackIndexForLoader,打开attrindex_0.ctb.ltsession,打开分区对应_w库

        4> . merge_packindex_data 合并包索引,向_w库中写入

        5> . 新生成attrindex_0.ctb.ltsession文件

        6> . commit_long_session_merge_data 将其他分区的_mrg库重名,并将attrindex_0.ctb.ltsession-->attrindex_0.ctb

        5> . RCTableLoad::WritePackIndex() 将原来的数据库+_w库进行合并成_mrg库
             a>. fork 原来的数据库
             b>. merge 原来的数据库和当前正在写的数据库(_w)到新的数据库(_mrg)中

        6> . RCAttr::CommitSaveSession提交数据库,将_mrg重名称成可以使用的数据库

*/
int  RCAttrLoadBase::merge_packindex_data(const std::string& sorted_data_path,
        const std::string& sorted_sessionid,const bool bltsession)
{
    std::vector<std::string> ldb_file_lst;

    // 获取leveldb文件列表
    int ret = get_sorted_ldb_file_info(attr_number,sorted_sessionid,sorted_data_path,ldb_file_lst);

    if(ret >0) { // 合并leveldb

        bool ispacks=PackType() == PackS;

        apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId());

        CR_ExternalSort::PopOnly pop_only;

        pop_only.LoadFiles(ldb_file_lst);

        int64_t _pack_no = 0;

        std::string _str_key = "";    // key 值
        int64_t _num_key = 0;

        std::string _str_value = "" ; // pack 的value值
        bool _str_value_tmp = false;

        std::string pki_last_key = CR_Class_NS::randstr(16); // fix DMA-1501 by hwei
        int last_key_first_packno = -1;  // 记录上一个key的第一个包号
        
        char err_msg[1024];
        size_t valid_key_cnt = 0;

        while (1) {
            ret = pop_only.PopOne(_pack_no, _str_key, _str_value, _str_value_tmp);

            // 返回的包号应该要一致才对
            //assert(_pack_no == _str_value.size()/sizeof(int)); // disabled by hwei, for DMA-1389

            if (ret == EAGAIN) {// done;
                sprintf(err_msg,"Info: CR_ExternalSort::PopOnly::PopOne return EAGAIN ! attr_number [%d] , key_count[%lu].",attr_number,(long unsigned)valid_key_cnt);
                rclog << lock << err_msg << unlock;
                break;
            } else if (ret) { 
                sprintf(err_msg,"ERROR: merge_packindex_data failed! Database may be corrupted! attr_number [%d].",attr_number);
                rclog << lock << err_msg << unlock;
                throw InternalRCException(std::string(err_msg));
            }
            // int pack_no_array[_str_value.size()/sizeof(int)];

            // fix DMA-1501 : we MUST reset last_key_first_packno after _str_key changed. by hwei
            if (pki_last_key != _str_key) {
                pki_last_key = _str_key;
                last_key_first_packno = -1;
                valid_key_cnt++;
            }

            // memcpy后,在修改
            if(merge_packidx_packno_init >0 ) { // 需要调整包号
                int* _pack_no_start = (int*)(_str_value.c_str());
                int  _pack_size = _str_value.size()/sizeof(int); // by hwei, for DMA-1389
                int* _pack_no_end = _pack_no_start + _pack_size;
                while(_pack_no_start<_pack_no_end) {
                    *_pack_no_start += merge_packidx_packno_init;
                    _pack_no_start++;
                }
            }

            const int* p_current_key_packno = (int*)(_str_value.c_str());
            if(_str_value.size() == sizeof(int) &&  last_key_first_packno == *p_current_key_packno) {
                continue;// fix dma-1419:相同的数据包跳过
            }

            if(ispacks) {
                pindex->Put(_str_key.c_str(),_str_value,bltsession);
            } else {
                _num_key = CR_Class_NS::str2i64(_str_key,0);
                pindex->Put(_num_key,_str_value,bltsession);
            }

            last_key_first_packno = *p_current_key_packno;
        }       
        
        rclog << lock << "RCAttrLoadBase::merge_packindex_data colid "<< attr_number <<" MapSize:" << pindex->GetMapSize() << unlock;
        
    }else{
        char err_msg[1024];
        sprintf(err_msg,"Warning:  RCAttrLoadBase::get_sorted_ldb_file_info get empty *.pki list. attr_number [%d].",attr_number);
        rclog << lock << err_msg << unlock;
        throw InternalRCException(std::string(err_msg));
    }

    //--------------------------------------------------
    // 包索引的头部的保存
    if(ldb_index!=NULL) {
        if(load_data_from_truncate_partition) { // save header.ltsession
            ldb_index->SaveHeader_ltsession();

        } else { // save header
            ldb_index->SaveHeader();
        }
    }

    return 0;
}

// 长回话上临时提交数据
bool RCAttrLoadBase::commit_long_session_merge_data(const std::string & datapart_name,std::vector<int> & del_pack_file_list)
{
    try {

        attr_partinfo *apartinfo=partitioninfo->getpartptr(datapart_name.c_str());
        assert(apartinfo!=NULL);

        //   1. move RSI xxx.ntrunc->xxx
        if(NeedToCreateRSIIndex()) { // fix dma-1462
            std::string knpath=rsi_manager->GetKNFolderPath();
            std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
            std::string newrsifn=rsifn+".ntrunc";
            _int64 fsize=0;
            GetFileSize(newrsifn,fsize);
            int headlen=PackType() == PackN?15:14;
            bool novalidpack=false;
            if(headlen==fsize) novalidpack=true;
            assert(DoesFileExist(newrsifn)) ;
            RemoveFile( rsifn);
            RenameFile(newrsifn, rsifn);
        }

        //   2. LevelDB  rmdir xxx.trunc ;
        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            int from_truncate_part = 1;
            ldb_index->LoadHeader(from_truncate_part);
            ldb_index->LoadForUpdate(datapart_name.c_str(),GetSaveSessionId());
        }
        if(ldb_index) {
            ldb_index->CommitTruncate_ltsession(apartinfo->name());
        }

        //   3. PackFile rm xxx.trunc
        for( int i=0; i<del_pack_file_list.size(); i++) {
            std::string dtfilename=AttrPackFileNameDirect(attr_number,del_pack_file_list[i],path);
            rclog << lock << Name() << ": RemoveFile "<< dtfilename << " . " << unlock;
            RemoveFile(dtfilename);
        }

        //   4. DPN   move xxx.ntrunc -> xxx
        std::string dpnfn=DPNFileName();
        std::string newdpnfn=dpnfn+".ntrunc";
        if(!DoesFileExist(newdpnfn))
            throw "no file to be committed on dpn.";
        RemoveFile(dpnfn);
        RenameFile(newdpnfn, dpnfn);

        //   5. partinfo:move xxx.ntrunc -> xxx
        partitioninfo->CommitTruncate();

        //   6. replace TA<ATTRNUM>.ctb.ntrunc ->xxx
        RemoveFile(AttrFileName(file));
        RenameFile(AttrFileName(file)+".ntrunc",AttrFileName(file));
    } catch(...) {
        rclog << lock << Name() << ": CommitTruncatePartiton failed! Database may be corrupted!" << unlock;
        throw;
    }
    return true;

}



void RCAttrLoadBase::CompareAndSetCurrentMax(RCBString tstmp, RCBString & max)
{
    bool res;
    if(RequiresUTFConversions(Type().GetCollation())) {
        res = CollationStrCmp(Type().GetCollation(), tstmp, max) > 0;
    } else
        res = strcmp(tstmp, max) > 0;

    if(res) {
        max = tstmp;
        max.MakePersistent();
    }
}

uint RCAttrLoadBase::RoundUpTo8Bytes(RCBString& s)
{
#ifndef PURE_LIBRARY
    uint useful_len = 0;
    if(Type().GetCollation().collation->mbmaxlen > 1) {
        int next_char_len;
        while(true) {
            if(useful_len >= s.len)
                break;
            next_char_len = Type().GetCollation().collation->cset->mbcharlen(Type().GetCollation().collation,
                            (uchar) s.val[useful_len + s.pos]);
            assert("wide character unrecognized" && next_char_len > 0);
            if(useful_len + next_char_len > 8)
                break;
            useful_len += next_char_len;
        }
    } else
        useful_len = s.len > 8 ? 8 : s.len;
    return useful_len;
#else
    BHERROR("NOT IMPLEMENTED");
    return 0;
#endif
}

void RCAttrLoadBase::SetPackMax(uint pack, RCBString& max_s)
{
    DPN const& dpn(dpns[pack]);
    if(RequiresUTFConversions(Type().GetCollation())) {
        int useful_len = RoundUpTo8Bytes(max_s);

        //deal with ...ae -> ...

        strncpy((uchar*) (&dpn.local_max), max_s, useful_len);
        if(useful_len < 8)
            ((uchar*) &dpn.local_max)[useful_len] = 0;
    } else
        strncpy((uchar*) (&dpn.local_max), max_s, (uint)min(max_s.size(), sizeof(_int64)));
}

void RCAttrLoadBase::SetPackMin(uint pack, RCBString& min_s)
{
    DPN const& dpn(dpns[pack]);
    if(RequiresUTFConversions(Type().GetCollation())) {
        int useful_len = RoundUpTo8Bytes(min_s);

        //deal with ...ae -> ...

        strncpy((uchar*) (&dpn.local_min), min_s, useful_len);
        if(useful_len < 8)
            ((uchar*) &dpn.local_min)[useful_len] = 0;
    } else
        strncpy((uchar*) (&dpn.local_min), min_s, sizeof(_int64));
}

RCBString RCAttrLoadBase::MinS(Filter* f)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
    FETOperator feto("RCAttr::MinS(...)");
#endif
    if(f->IsEmpty() || !ATI::IsStringType(TypeName()) || NoObj() == 0 || NoObj() == NoNulls())
        return RCBString();
    RCBString min;
    bool set = false;
    if(f->NoBlocks() + packs_omitted != NoPack())
        throw DatabaseRCException("Data integrity error, query cannot be evaluated (MinS).");
    else {
        RCBString tstmp;
        LoadPackInfo();
        FilterOnesIterator it(f);
        while(it.IsValid()) {
            uint b = it.GetCurrPack();
            if(b >= NoPack() - packs_omitted)
                continue;
            DPN const& dpn( dpns[b] );
            if(PackType() == PackN &&
               (GetPackOntologicalStatus(b) == UNIFORM ||
                (GetPackOntologicalStatus(b) == UNIFORM_AND_NULLS && f->IsFull(b)))
              ) {
                tstmp = DecodeValue_S(dpn.local_min);
                CompareAndSetCurrentMin(tstmp, min, set);
                it.NextPack();
            } else if(!(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ)) {
                while(it.IsValid() && b == it.GetCurrPack()) {
                    int n = it.GetCurrInPack();
                    if(PackType() == PackS && dpn.pack->IsNull(n) == 0) {
                        int len = ((AttrPackS*)dpn.pack.get())->GetSize(n);
                        tstmp = len ? RCBString(((AttrPackS*)dpn.pack.get())->GetVal(n), len) : RCBString("");
                        CompareAndSetCurrentMin(tstmp, min, set);
                    }
                    ++it;
                }
            }
        }
    }
    return min;
}

RCBString RCAttrLoadBase::MaxS(Filter* f)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
    FETOperator feto("RCAttr::MaxS(...)");
#endif
    if(f->IsEmpty() || !ATI::IsStringType(TypeName()) || NoObj() == 0 || NoObj() == NoNulls())
        return RCBString();

    RCBString max;
    if(f->NoBlocks() + packs_omitted != NoPack())
        throw DatabaseRCException("Data integrity error, query cannot be evaluated (MaxS).");
    else {
        RCBString tstmp;
        LoadPackInfo();
        FilterOnesIterator it(f);
        while(it.IsValid()) {
            uint b = it.GetCurrPack();
            if(b >= NoPack() - packs_omitted)
                continue;
            DPN const& dpn( dpns[b] );
            if(PackType() == PackN &&
               (GetPackOntologicalStatus(b) == UNIFORM ||
                (GetPackOntologicalStatus(b) == UNIFORM_AND_NULLS && f->IsFull(b)))
              ) {
                tstmp = DecodeValue_S(dpn.local_min);
                CompareAndSetCurrentMax(tstmp, max);
            } else if(!(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ)) {
                while(it.IsValid() && b == it.GetCurrPack()) {
                    int n = it.GetCurrInPack();
                    if(PackType() == PackS && dpn.pack->IsNull(n) == 0) {
                        int len = ((AttrPackS*)dpn.pack.get())->GetSize(n);
                        tstmp = len ? RCBString(((AttrPackS*)dpn.pack.get())->GetVal(n), len) : RCBString("");
                        CompareAndSetCurrentMax(tstmp, max);
                    } else if(PackType() == PackN && !dpn.pack->IsNull(n)) {
                        tstmp = RCBString(DecodeValue_S(((AttrPackN*) dpn.pack.get())->GetVal64(n) + dpn.local_min));
                        CompareAndSetCurrentMax(tstmp, max);
                    }
                    ++it;
                }
            }
        }
    }
    return max;
}

RCDataTypePtr RCAttrLoadBase::GetMinValuePtr(int pack) // needed for loader?
{
    AttributeType a_type = TypeName();
    RCDataType* ret = 0;
    if(this->NoNulls() != NoObj() && NoObj() != 0) {
        LoadPackInfo();
        if(ATI::IsIntegerType(a_type))
            ret = new RCNum(dpns[pack].local_min);
        else if(ATI::IsDateTimeType(a_type))
            ret = new RCDateTime(dpns[pack].local_min, a_type);
        else if(ATI::IsRealType(a_type))
            ret = new RCNum(dpns[pack].local_min, 0, true);
        else if(a_type == RC_NUM)
            ret = new RCNum(dpns[pack].local_min, Type().GetScale());
        else {
            std::map<int,int> nfblocks;
            Filter f(NoObj() - (static_cast<_uint64> (packs_omitted) << 16),nfblocks);
            f.SetBlock(pack);
            ret = new RCBString(MinS(&f));
        }
    }
    return RCDataTypePtr(ret);
}

RCDataTypePtr RCAttrLoadBase::GetMaxValuePtr(int pack) // needed for loader?
{
    AttributeType a_type = TypeName();
    RCDataType* ret = 0;
    if(this->NoNulls() != NoObj() && NoObj() != 0) {
        LoadPackInfo();
        if(ATI::IsIntegerType(a_type))
            ret = new RCNum(dpns[pack].local_max);
        else if(ATI::IsDateTimeType(a_type))
            ret = new RCDateTime(dpns[pack].local_max, a_type);
        else if(ATI::IsRealType(a_type))
            ret = new RCNum(dpns[pack].local_max, 0, true);
        else if(a_type == RC_NUM)
            ret = new RCNum(dpns[pack].local_max, Type().GetScale());
        else {
            std::map<int,int> nfblocks;
            Filter f(NoObj() - (static_cast<_uint64> (packs_omitted) << 16),nfblocks);
            f.SetBlock(pack);
            ret = new RCBString(MaxS(&f));
        }
    }
    return RCDataTypePtr(ret);
}

void RCAttrLoadBase::GetMinMaxValuesPtrs(int pack, RCDataTypePtr& out_min, RCDataTypePtr& out_max) // needed for loader?
{
    out_min = GetMinValuePtr(pack);
    out_max = GetMaxValuePtr(pack);
}

void RCAttrLoadBase::LogWarnigs()
{
    if(illegal_nulls) {

        std::pair<std::string, std::string> db_and_table_names = RCEngine::GetDatabaseAndTableNamesFromFullPath(path);

        stringstream ss;
        ss << "WARNING: There was an attempt to insert NULL to " << db_and_table_names.first << "." << db_and_table_names.second << "." << this->Name()
           << " column that is defined as NOT NULL.";
        rclog << lock << ss.str() << unlock;

        ss.str("");
        ss << "         NULLs were changed to ";

        if(ATI::IsStringType(TypeName()))
            ss << "empty string.";
        else if(ATI::IsNumericType(TypeName()))
            ss << RCNum(0, Type().GetScale(), ATI::IsRealType(TypeName()), TypeName()).ToRCString() << ".";
        else
            ss << RCDateTime(0, TypeName()).ToRCString() << ".";

        rclog << lock << ss.str() << unlock;
    }
}

void RCAttrLoadBase::UpdateRSI_Hist(int pack, int no_objs) // make the index up to date for the selected pack
{
    if(!NeedToCreateRSIIndex()) {
        return;
    }
    MEASURE_FET("RCAttrLoadBase::UpdateRSI_Hist(...)");
    if(PackType() != PackN || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0)
        return;
    // Note that GetIndxForUpdate will create a new index, if it does not exist.
    // Assuming that if UpdateRSI_Hist() is executed, then we really should have an index for this attr.
    // Allows updating the histogram in the random order.

    if(rsi_hist_update == NULL)
        rsi_hist_update = (RSIndex_Hist*) rsi_manager->GetIndexForUpdate(
                              RSIndexID(RSI_HIST, table_number, attr_number), GetCurReadLocation(),load_data_from_truncate_partition);

    if(rsi_hist_update == NULL) {
        rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
        return;
    }
    LoadPackInfo();
    if(rsi_hist_update->NoObj() == 0)
        rsi_hist_update->Create(NoObj(), (ATI::IsRealType(TypeName()) ? false : true)); // the new index
    else
        rsi_hist_update->Update(NoObj()); // safe also if no_obj is the same as a while ago

    rsi_hist_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack
    _int64 pmin = dpns[pack].local_min;
    _int64 pmax = dpns[pack].local_max;

    if(!dpns[pack].is_stored && dpns[pack].pack_file == PF_NULLS_ONLY) { // fix dma-1548,null 包不用再更新rsi
        return;
    }

    BHASSERT(pmin != PLUS_INF_64, "should be 'pmin != PLUS_INF_64'");
    if(pmin == pmax || GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return;

    //LockPackForUse(pack);
    //  if(dpns[pack].pack_mode != PACK_MODE_IN_MEMORY) LoadPack(pack); // do not mark this pack access, as it is optional
    _uint64 obj_start = (_uint64(pack) << 16);
    _uint64 obj_stop = obj_start + no_objs;
    for(_uint64 obj = obj_start; obj < obj_stop; obj++)
        rsi_hist_update->PutValue(GetValueInt64(obj), pack + packs_omitted, pmin, pmax);
    //UnlockPackFromUse(pack);
    // NOTE: we must update and delete index after each load, because someone may use it in a query before commit.
    // Updating should be performed externally (after load) to avoid too frequent disk access.
}

void RCAttrLoadBase::UpdateRSI_CMap(int pack, int no_objs, bool new_prefix) // make the index up to date for the selected pack
{
    if(!NeedToCreateRSIIndex()) {
        return;
    }

    MEASURE_FET("RCAttrLoadBase::UpdateRSI_CMap(...)");
    if(PackType() != PackS || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0
       || RequiresUTFConversions(Type().GetCollation()))
        return;
    if(rsi_cmap_update == NULL)
        rsi_cmap_update = (RSIndex_CMap*) rsi_manager->GetIndexForUpdate(
                              RSIndexID(RSI_CMAP, table_number, attr_number), GetCurReadLocation(),load_data_from_truncate_partition);

    if(rsi_cmap_update == NULL) {
        rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
        return;
    }
    LoadPackInfo();
    if(rsi_cmap_update->NoObj() == 0)
        rsi_cmap_update->Create(NoObj(), (Type().GetPrecision() > 64 ? 64 : Type().GetPrecision())); // the new index
    else
        rsi_cmap_update->Update(NoObj()); // safe also if no_obj is the same as a while ago
    // AS DMA-495 fixed,restore this line;
    //rsi_cmap_update->Update( (((_int64)pack)<<16)+no_objs);
    rsi_cmap_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack

    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return;

    //LockPackForUse(pack);
    _uint64 obj_start = (_uint64(pack) << 16);
    _uint64 obj_stop = obj_start + no_objs;
    _uint64 obj = /*new_prefix ? 0 :*/obj_start;
    int prefix_len = (int) GetPrefix(pack).size();
    for(; obj < obj_stop; obj++)
        if(!IsNull(obj))
            rsi_cmap_update->PutValue(GetValueString(obj) += prefix_len, pack + packs_omitted); // false - because we don't want to see binary as hex
    // NOTE: we must update and delete index after each load, because someone may use it in a query before commit.
    // Updating should be performed externally (after load) to avoid too frequent disk access.
}

// 拷贝rsi,pack = NoPack()-packs_omitted
void RCAttrLoadBase::CopyRSI_Hist(int pack,const char* p_rsi_buff,const int rsi_buff_size)
{
    if(!NeedToCreateRSIIndex()) {
        return;
    }

    MEASURE_FET("RCAttrLoadBase::CopyRSI_Hist(...)");
    if(PackType() != PackN || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0)
        return;
    // Note that GetIndxForUpdate will create a new index, if it does not exist.
    // Assuming that if UpdateRSI_Hist() is executed, then we really should have an index for this attr.
    // Allows updating the histogram in the random order.

    if(rsi_hist_update == NULL)
        rsi_hist_update = (RSIndex_Hist*) rsi_manager->GetIndexForUpdate(
                              RSIndexID(RSI_HIST, table_number, attr_number), GetCurReadLocation(),load_data_from_truncate_partition);

    if(rsi_hist_update == NULL) {
        rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
        return;
    }
    LoadPackInfo();
    if(rsi_hist_update->NoObj() == 0)
        rsi_hist_update->Create(NoObj(), (ATI::IsRealType(TypeName()) ? false : true)); // the new index
    else
        rsi_hist_update->Update(NoObj()); // safe also if no_obj is the same as a while ago

    rsi_hist_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack

    // copy
    rsi_hist_update->CopyPack(pack + packs_omitted,p_rsi_buff,rsi_buff_size);

}
void RCAttrLoadBase::CopyRSI_CMap(int pack, const char* p_rsi_buff,const int rsi_buff_size)
{
    if(!NeedToCreateRSIIndex()) {
        return;
    }

    MEASURE_FET("RCAttrLoadBase::UpdateRSI_CMap(...)");
    if(PackType() != PackS || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0
       || RequiresUTFConversions(Type().GetCollation()))
        return;
    if(rsi_cmap_update == NULL)
        rsi_cmap_update = (RSIndex_CMap*) rsi_manager->GetIndexForUpdate(
                              RSIndexID(RSI_CMAP, table_number, attr_number), GetCurReadLocation(),load_data_from_truncate_partition);

    if(rsi_cmap_update == NULL) {
        rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
        return;
    }
    LoadPackInfo();
    if(rsi_cmap_update->NoObj() == 0)
        rsi_cmap_update->Create(NoObj(), (Type().GetPrecision() > 64 ? 64 : Type().GetPrecision())); // the new index
    else
        rsi_cmap_update->Update(NoObj()); // safe also if no_obj is the same as a while ago
    // AS DMA-495 fixed,restore this line;
    //rsi_cmap_update->Update( (((_int64)pack)<<16)+no_objs);
    rsi_cmap_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack

    // copy
    rsi_cmap_update->CopyPack(pack + packs_omitted,p_rsi_buff,rsi_buff_size);
}


void RCAttrLoadBase::AddOutliers(int64 no_outliers)
{
    IBGuard guard(no_outliers_mutex);
    if(this->no_outliers == TransactionBase::NO_DECOMPOSITION)
        this->no_outliers = 0;
    this->no_outliers += no_outliers;
}

int64 RCAttrLoadBase::GetNoOutliers() const
{
    IBGuard guard(no_outliers_mutex);
    return no_outliers;
}


