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
#include "dma_mtreader.h"
#include "common/CommonDefinitions.h"
#include "RCAttr.h"
#include "RCAttrPack.h"
#include "RCAttrTypeInfo.h"
#include "system/fet.h"
#include "system/ConnectionInfo.h"
#include "types/ValueParserForText.h"
#include "DataPackImpl.h"
#include "DPN.h"
#include "common/DataFormat.h"
#include "edition/core/Transaction.h"
#include "common/ProcessType.h"

#include "util/BHString.h"
#include "system/IBFile.h"
#include "DMThreadGroup.h"
using namespace bh;
using namespace std;
using namespace boost;

enum { VAL_BUF_MIN_COUNT = 24 };
std::map<std::string,AttrIndex::apindexinfo *> AttrIndex::g_index;
IBMutex RCAttr::m_dpns_mutex;
//IBMutex RCAttr::m_preload_mutex;

apindex::DBPool apindex::ap_pool;
DMMutex  apindex::fork_lock;
void RCAttr::CalcPackFileSizeLimit(int a_field_size)
{
    // Determine pack file size limit
    // Smaller file => more system overload and size limitations (file naming!).
    // Larger size  => harder to backup; cannot exceed 2.1 GB!
    if (a_field_size > 16000)
        file_size_limit = 1000000000;
    else
        // Default size: the safe distance from 2 GB
        file_size_limit = 2000000000 - 2*65536*size_t(a_field_size);

    try {
        string conferror;
        size_t new_file_size_limit = 1000000 *
                                     size_t(ConfMan.GetValueInt("brighthouse/ClusterSize", conferror));
        if(new_file_size_limit < file_size_limit)
            file_size_limit = new_file_size_limit;
    } catch(...) {
        // Just use the default values
    }
    if (file_size_limit < 10000000)
        file_size_limit = 10000000;
    return;
}

RCAttr::RCAttr(char *a_name, int a_num, int t_num, AttributeType a_type, int a_field_size, int a_dec_places, std::string const& a_path, uint param, uint s_id, DTCollation collation)
    :   packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( a_path ) ), last_pack_locked(false)
{

    load_data_from_truncate_partition = false;

    skip_DoSavePack_packidx = false;
    merge_packidx_packno_init = 0;

    _logical_coord.ID=bh::COORD_TYPE::RCATTR;
    _logical_coord.co.rcattr[0] = t_num;
    _logical_coord.co.rcattr[1] = a_num;

    SetFileFormat(10);
    CalcPackFileSizeLimit(a_field_size);

    SetNoPack(0);
    SetNoPackPtr(0);
    SetTotalPackFile(1);

    SetSaveFileLoc(0, 0);
    SetSaveFileLoc(1, 1);
    SetSavePosLoc(0, 0);
    SetSavePosLoc(1, 0);
    SetCurReadLocation(0);
    SetCurSaveLocation(0);

    SetSessionId(0);
    SetSaveSessionId(s_id);
    // Address (byte) of the beginning of i-th pack in the file
    rsi_hist_update = NULL;
    rsi_cmap_update = NULL;
    ldb_index = NULL;

    SetLastPackIndex(0);
    SetNoObj(0);

    NullMode nulls;
    bool is_lookup;

    is_lookup = (param & 0x00000002) != 0;
    // automatic LOOKUP for small columns - DISABLED AS TOO RISKY
//    if((a_type == RC_STRING || a_type == RC_VARCHAR) && a_field_size <= 2)
//      is_lookup = true;
    if(a_type != RC_STRING && a_type != RC_VARCHAR)
        is_lookup = false;              // note: is_lookup  =>  RC_STRING || RC_VARCHAR
    if(param & 0x00000001)
        nulls = NO_NULLS;
    else
        nulls = AS_MISSED;

    if(param & 0x00000004)
        inserting_mode = 1;
    else
        inserting_mode = 0;

    int compress_type = 0;
    if(param & 0x00000040)
        compress_type = Compress_Soft_Zip;
    else if(param & 0x00000020)
        compress_type = Compress_DEFAULT;
    else if(param & 0x00000080)
        compress_type = Compress_Snappy;
    else if(param & 0x00000100)
        compress_type = Compress_Lz4;

    no_compression = false;

    if (a_field_size <= 0) {
        unsigned short int fsize;
        // maximum size of double for gcvt(...,15,...)
        if(a_type==RC_REAL) fsize = 23;
        else if(a_type==RC_FLOAT) fsize = 15;
        else if(a_type==RC_BYTEINT) fsize = 4;
        else if(a_type==RC_SMALLINT) fsize = 6;
        else if(a_type==RC_MEDIUMINT) fsize = 8;
        else if(a_type==RC_INT) fsize = 11;
        else if(a_type==RC_BIGINT) fsize = 20;
        else if(a_type==RC_NUM) fsize = 18;
        else if(a_type==RC_DATE) fsize = 10;
        else if(a_type==RC_TIME) fsize = 10;
        else if(a_type==RC_YEAR) fsize = 4;
        else if(a_type == RC_DATETIME || a_type == RC_TIMESTAMP) fsize = 19;
        a_field_size  = fsize;
    }

    ct.Initialize(a_type, nulls, is_lookup, a_field_size, a_dec_places, param&0x08,param&0x10,compress_type,collation);

    if(Type().IsLookup() || ATI::IsNumericType(a_type) || ATI::IsDateTimeType(a_type))
        SetPackType(PackN);
    else
        SetPackType(PackS);

    m_allocator = new PackAllocator(this,PackType());

    int val_buf_size = Type().GetDisplaySize() + 1;
    if (val_buf_size < VAL_BUF_MIN_COUNT)
        val_buf_size=VAL_BUF_MIN_COUNT;     // minimal RC_NUM buffer size

    SetMinInt64(0);
    SetMaxInt64(0);
    current_state = 1;              // read/write session

    SetNoNulls(0);
    SetNaturalSizeSaved(0);
    SetCompressedSize(0);
    rough_selectivity = -1;
    SetUnique(true);                // all non-null values are different (because there are no values at all)
    SetUniqueUpdated(true);     // true if the information in "is_unique" is up to date

    attr_number = a_num;    // column number (for file naming purposes etc.)
    table_number = t_num;
    partitioninfo=NULL;
    partitioninfo=new attr_partitions(attr_number,path.c_str());
    partitioninfo->SetSessionID(GetSaveSessionId());
    partitioninfo->LoadNFBlocks(nfblocks);
    SetName(a_name);
    SetDescription(NULL);


    SetDictOffset(0);
    SetPackOffset(0);

    if(Type().IsLookup()) {
        dic = boost::shared_ptr<FTree>(new FTree());
        dic->Init(Type().GetPrecision());
        GlobalDataCache::GetGlobalDataCache().PutObject(FTreeCoordinate(table_number, attr_number), dic); // we do not have
    }

    SetPackInfoCollapsed((GetDictOffset() || NoPack()));
    file = FILE_READ_SESSION;
    ComputeNaturalSize();

    if(process_type != ProcessType::DATAPROCESSOR)
        dom_inj_mngr.SetPath(DomainInjectionFileName(file));
    cont_read_times=0;
    last_read_packs=0;


}

RCAttr::RCAttr(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, string const& path_)
    :   packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( path_ ) ), last_pack_locked(false)
{
    _logical_coord.ID=bh::COORD_TYPE::RCATTR;
    _logical_coord.co.rcattr[0] = 0;
    _logical_coord.co.rcattr[1] = a_num;

    load_data_from_truncate_partition = false;
    skip_DoSavePack_packidx = false;
    merge_packidx_packno_init = 0;

    SetFileFormat(10);
    file_size_limit = 0;

    SetNoPack(0);
    SetNoPackPtr(0);
    SetTotalPackFile(1);

    SetSaveFileLoc(0, 0);
    SetSaveFileLoc(1, 1);
    SetSavePosLoc(0, 0);
    SetSavePosLoc(1, 0);
    SetCurReadLocation(0);
    SetCurSaveLocation(0);

    SetSessionId(0);
    SetSaveSessionId(INVALID_TRANSACTION_ID);
    // Address (byte) of the beginning of i-th pack in the file
    rsi_hist_update = NULL;
    rsi_cmap_update = NULL;
    ldb_index = NULL;

    SetLastPackIndex(0);
    SetNoObj(0);

    NullMode nulls;
    bool is_lookup;

    is_lookup = (param & 0x00000002) != 0;
    // automatic LOOKUP for small columns - DISABLED AS TOO RISKY
//    if((a_type == RC_STRING || a_type == RC_VARCHAR) && a_field_size <= 2)
//      is_lookup = true;
    if(a_type != RC_STRING && a_type != RC_VARCHAR)
        is_lookup = false;              // note: is_lookup  =>  RC_STRING || RC_VARCHAR
    if(param & 0x00000001)
        nulls = NO_NULLS;
    else
        nulls = AS_MISSED;

    if(param & 0x00000004)
        inserting_mode = 1;
    else
        inserting_mode = 0;

    int compress_type = 0;
    if(param & 0x00000040)
        compress_type = Compress_Soft_Zip;
    else if(param & 0x00000020)
        compress_type = Compress_DEFAULT;
    else if(param & 0x00000080)
        compress_type = Compress_Snappy;
    else if(param & 0x00000100)
        compress_type = Compress_Lz4;

    no_compression = false;
    if (is_lookup) {
        // convert lookup to no compression mode in dataprocessor
        no_compression = !compress_lookups;
        is_lookup = false;
    }

    if (a_field_size <= 0) {
        unsigned short int fsize;
        // maximum size of double for gcvt(...,15,...)
        if(a_type==RC_REAL) fsize = 23;
        else if(a_type==RC_FLOAT) fsize = 15;
        else if(a_type==RC_BYTEINT) fsize = 4;
        else if(a_type==RC_SMALLINT) fsize = 6;
        else if(a_type==RC_MEDIUMINT) fsize = 8;
        else if(a_type==RC_INT) fsize = 11;
        else if(a_type==RC_BIGINT) fsize = 20;
        else if(a_type==RC_NUM) fsize = 18;
        else if(a_type==RC_DATE) fsize = 10;
        else if(a_type==RC_TIME) fsize = 10;
        else if(a_type==RC_YEAR) fsize = 4;
        else if(a_type == RC_DATETIME || a_type == RC_TIMESTAMP) fsize = 19;
        a_field_size  = fsize;
    }

    ct.Initialize(a_type, nulls, is_lookup, a_field_size, a_dec_places, param&0x08,param&0x10,compress_type,collation);

    if(Type().IsLookup() || ATI::IsNumericType(a_type) || ATI::IsDateTimeType(a_type))
        SetPackType(PackN);
    else
        SetPackType(PackS);

    m_allocator = new PackAllocator(this,PackType());

    int val_buf_size = Type().GetDisplaySize() + 1;
    if (val_buf_size < VAL_BUF_MIN_COUNT)
        val_buf_size=VAL_BUF_MIN_COUNT;     // minimal RC_NUM buffer size

    SetMinInt64(0);
    SetMaxInt64(0);
    current_state = 1;              // read/write session

    SetNoNulls(0);
    SetNaturalSizeSaved(0);
    SetCompressedSize(0);
    rough_selectivity = -1;
    SetUnique(true);                // all non-null values are different (because there are no values at all)
    SetUniqueUpdated(true);     // true if the information in "is_unique" is up to date

    attr_number = a_num;    // column number (for file naming purposes etc.)
    table_number = 0;
    partitioninfo=NULL;
    partitioninfo=new attr_partitions(attr_number,path.c_str());
    partitioninfo->SetSessionID(GetSessionId());
    partitioninfo->LoadNFBlocks(nfblocks);
    SetName(NULL);
    SetDescription(NULL);

    SetDictOffset(0);
    SetPackOffset(0);

    if(Type().IsLookup()) {
        dic = boost::shared_ptr<FTree>(new FTree());
        dic->Init(Type().GetPrecision());
        GlobalDataCache::GetGlobalDataCache().PutObject(FTreeCoordinate(table_number, attr_number), dic); // we do not have
    }

    SetPackInfoCollapsed((GetDictOffset() || NoPack()));
    file = FILE_READ_SESSION;
    ComputeNaturalSize();

    // prepare for load
    pack_info_collapsed = false;
    cont_read_times=0;
    last_read_packs=0;
}

bool RCAttr::HasUnCommitedSession()
{
    return (DoesFileExist(AttrFileName(FILE_SAVE_SESSION)));
}

unsigned int RCAttr::ReadUnCommitedSessionID()
{
    IBFile fattr_save;
    unsigned int session_id = INVALID_TRANSACTION_ID;
    unsigned char a_file_format;
    char* tmp_buf = new char [10000];

    string attrf_name(AttrFileName(FILE_SAVE_SESSION));
    fattr_save.OpenReadOnly(attrf_name);
    fattr_save.Read(tmp_buf, CLMD_HEADER_LEN);
    a_file_format = FILEFORMAT(tmp_buf);
    if (a_file_format == CLMD_FORMAT_INVALID_ID) {
        fattr_save.Close();
        delete [] tmp_buf;
        rclog << lock << "Unsupported version (not an attribute), or file does not exist: " << attrf_name << unlock;
        string mess = (string)"Unsupported version (not an attribute), or file does not exist: " + attrf_name;
        throw DatabaseRCException(mess.c_str());
    }

    uint sp_offset = *((uint*)(tmp_buf + 42));
    if (sp_offset > 0) {                                    // Note that it is assumed that the first special block contain session info
        fattr_save.Seek(sp_offset + 22, SEEK_SET);
        fattr_save.Read(tmp_buf, 46);
        session_id = *((uint*)(tmp_buf + 1));
    }
    fattr_save.Close();

    delete [] tmp_buf;
    return session_id;
}

RCAttr::RCAttr(int a_num, int t_num, string const& a_path, int conn_mode, uint s_id,bool loadapindex, DTCollation collation) throw(DatabaseRCException)
    :   packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( a_path ) ), last_pack_locked(false)
{
    ct.Initialize(RC_INT, AS_MISSED, false, 0, 0,false,false,Compress_DEFAULT,collation);
    rsi_hist_update = NULL;
    rsi_cmap_update = NULL;
    ldb_index = NULL;

    load_data_from_truncate_partition = false;

    skip_DoSavePack_packidx = false;
    merge_packidx_packno_init = 0;


    attr_number = a_num;                // index of the column in table (for file naming purposes etc.)
    table_number = t_num;
    partitioninfo=NULL;
    partitioninfo=new attr_partitions(attr_number,path.c_str());
    partitioninfo->SetSessionID(INVALID_TRANSACTION_ID);
    partitioninfo->LoadNFBlocks(nfblocks);
    _logical_coord.ID=bh::COORD_TYPE::RCATTR;
    _logical_coord.co.rcattr[0] = t_num;
    _logical_coord.co.rcattr[1] = a_num;

    current_state = conn_mode;          // conn_mode is used to determine initial state; current_state may change

    // Determine currently opened save session_id and save_loc (if any)
    SetSessionId(0);
    SetSaveSessionId(INVALID_TRANSACTION_ID);       // this value indicates that there is no session opened
    SetCurSaveLocation(0);

    int new_save_session = 0;           // this flag will indicate that this is a new session and we should switch save locations

    //IBFile fattr_read;
    string attrf_name(AttrFileName(FILE_SAVE_SESSION));

    // Normal read session
    int open_fname = FILE_READ_SESSION;

    if(current_state == SESSION_WRITE) {

        if (HasUnCommitedSession()) {
            // There is a load applied but not yet committed
            SetSaveSessionId(ReadUnCommitedSessionID());
            partitioninfo->SetSessionID(ReadUnCommitedSessionID());
            if (GetSaveSessionId() == s_id) {
                // Continue previous save session since this has same session id as current
                open_fname = FILE_SAVE_SESSION;
            } else {
                rclog << lock << "Error - previous session (id=" << GetSaveSessionId() << ") was not properly committed!" << unlock;
                new_save_session = 1;
            }
        } else {
            new_save_session = 1;
        }

        if (new_save_session) {
            SetSaveSessionId(s_id);
            partitioninfo->SetSessionID(s_id);
            // open read file to copy (by saving attr.) to the second file type
            open_fname = FILE_READ_SESSION;
        }
    }

    no_compression = false;

    // packs
    rough_selectivity = -1;

    file = open_fname;

    // Load file with attribute description
    Load(AttrFileName(open_fname),loadapindex);

    SetCurReadLocation(GetCurSaveLocation());

    // 取消切换会话时,在不同数据文件装入的功能
    // 除了这里注释,还需要在 LoadBase::LoadData中把文件编号增量操作,由 +2 改为 +1
    //  文件编号增加只会出现在:
    //   1.文件超过2G
    //   2.在新的分区装入
    //   3.在被分离的分区中装入: 上次装入数据不是当前分区,DPN编号不连续

    // DMA-614: 装入失败不能恢复,a/b/s切换实现
    //  恢复切换机制
    if (new_save_session == 1)
        SetCurSaveLocation(1 - GetCurSaveLocation());
    BHASSERT(GetCurSaveLocation() == 0 || GetCurSaveLocation() == 1, "Invalid current save location!");

    CalcPackFileSizeLimit(Type().GetPrecision());       // Determine pack file size limit
    ComputeNaturalSize();

    // need to add it manually since Load above can reset collation
    ct.SetCollation(collation);
    m_allocator = new PackAllocator(this,PackType());
    if(process_type != ProcessType::DATAPROCESSOR) {
        dom_inj_mngr.Init(DomainInjectionFileName(open_fname));
        if (new_save_session)
            dom_inj_mngr.SetPath(DomainInjectionFileName(1));
    }
    cont_read_times=0;
    last_read_packs=0;
}

RCAttr::~RCAttr()
{
    BHASSERT(rsi_hist_update == NULL && rsi_cmap_update == NULL,
             "Invalid Histogram or CMAP object. Either one was not released/freed properly!");

    DoEditionSpecificCleanUp();
    //UnlockLastPackFromUse();
    if(ldb_index != NULL) {
        delete ldb_index;
        ldb_index=NULL;
    }
    if(partitioninfo != NULL) {
        delete partitioninfo;
        partitioninfo=NULL;
    }
    if(m_allocator != NULL) {
        delete m_allocator;
        m_allocator = NULL;
    }
}

//int RCAttr::ColumnSize() const
//{
//  return ATI::TextSize(TypeName(), Type().GetPrecision(), Precision());
//}

void RCAttr::ComputeNaturalSize()
{
    AttributeType a_type = TypeName();
    _uint64 na_size;

    na_size = (Type().GetNullsMode()  != NO_NULLS ? 1 : 0) * NoObj() / 8;
    if(a_type == RC_STRING || a_type == RC_BYTE || a_type == RC_DATE)
        na_size += Type().GetPrecision() * NoObj();
    else if(a_type == RC_TIME || a_type == RC_YEAR || a_type == RC_DATETIME || a_type == RC_TIMESTAMP)
        na_size += Type().GetDisplaySize() * NoObj();
    else if(a_type == RC_NUM)
        na_size += (Type().GetPrecision() + (Type().GetScale() ? 1 : 0)) * NoObj();
    else if(ATI::IsRealType(a_type))
        na_size += 8 * NoObj();
    else if(a_type == RC_FLOAT)
        na_size += 4 * NoObj();
    else if(a_type == RC_INT)
        na_size += 4 * NoObj();
    else if(a_type == RC_BIGINT)
        na_size += 8 * NoObj();
    else if(a_type == RC_MEDIUMINT)
        na_size += 3 * NoObj();
    else if(a_type == RC_SMALLINT)
        na_size += 2 * NoObj();
    else if(a_type == RC_BYTEINT)
        na_size += 1 * NoObj();
    else if(a_type == RC_VARCHAR)
        na_size += (_int64)GetNaturalSizeSaved();
    else if(a_type == RC_VARBYTE || a_type == RC_BIN)
        na_size += (_int64)GetNaturalSizeSaved();
    SetNaturalSize(na_size);
}

void RCAttr::SaveHeader()
{
    _int64 css = ComputeCompressedSize();

    if (current_state == SESSION_READ)
        file = FILE_READ_SESSION;
    if(current_state == SESSION_WRITE)
        file = FILE_SAVE_SESSION;
    BHASSERT(FileFormat() == CLMD_FORMAT_RSC10_ID, "Invalid Attribute data file format!");
    //Save(AttrFileName(file), &dic, css);
    Save(AttrFileName(file), dic.get(), css);
    if(ldb_index!=NULL) {
        ldb_index->SaveHeader();
    }
    if(partitioninfo!=NULL)
        partitioninfo->save();

    dom_inj_mngr.Save();
}

const char *RCAttr::GetLoadPartName()
{
    return partitioninfo==NULL?"DEFAULTPART":partitioninfo->getsavepartinfo(NULL).name();
}

bool RCAttr::NeedToCreateRSIIndex()        // 是否需要创建RSI索引数据,fix dma-1443,add by liujs
{
    // 1. 包索引字段不需要构建RSI
    if(ct.IsPackIndex()) {
        return false;
    }

    // 2. 字符串类型,长度超过64自己的不用创建RSI
    if(PackType() == PackS && (Type().GetPrecision() >= 64)) {
        return false;
    }

    return true;
}

//GenerateColumnMetaDataFileName()
string RCAttr::AttrFileName(int ftype, bool oppositeName/*=false*/) const
{
    char fnm[] = { "TA00000.ctb" };

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

    BHASSERT(ftype!=2, "Trying to open file using invalid session mode!");
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += fnm;

    if(load_data_from_truncate_partition) { // 如果是从删除分区后的dpn中加载,需要从指定文件加载
        filename += std::string(".ntrunc");
    }

    return filename;
}

string RCAttr::DomainInjectionFileName(int ftype) const
{
    char fnm[] = {"TA00000DI.ctb"};
    ABSwitcher absw;
    ABSwitch cur_ab_switch_state = absw.GetState(path);
    if (ftype==0) {
        if (cur_ab_switch_state==ABS_B_STATE)
            fnm[1] = 'B';
    } else {
        if (cur_ab_switch_state==ABS_A_STATE)
            fnm[1] = 'B';
    }
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += fnm;
    return filename;
}

unsigned int RCAttr::LoadPackSize(int n)
{
    unsigned int res = 0;
    IBFile fattr;
    try {
        fattr.OpenReadOnly(AttrPackFileName(n));
        fattr.Seek(dpns[n].pack_addr,SEEK_SET);
        if(PackType()==PackS) {
            uchar header[13];
            fattr.Read(header,13);
            res = *((uint*) header);
        } else {
            uchar header[17];
            fattr.Read(header,17);
            res = *((uint*) header);
        }
        fattr.Close();
    } catch (...) {
        res = 0;
    }
    return res;
}

_int64 RCAttr::ComputeCompressedSize()
{
    _int64 tsize = 0;
    _int64 size = 0;

    if(GetFileSize(DPNFileName(), size))
        tsize = size;
    // for all pack file
    // adjust to process partition files
    if(partitioninfo && partitioninfo->getpacks()>0) {
        std::vector<int> filelist;
        int files=partitioninfo->GetFileList( filelist);
        for(int p_f=0; p_f<files; p_f++) {
            if(GetFileSize(RCAttr::AttrPackFileNameDirect(attr_number, filelist[p_f], path), size))
                tsize += size;
        }
    } else for(int p_f = 0; p_f < GetTotalPackFile(); p_f++) {
            if(GetFileSize(RCAttr::AttrPackFileNameDirect(attr_number, p_f, path), size))
                tsize += size;
        }
    return tsize;
}

void RCAttr::UpgradeFormat()
{
    if(file!=0) {
        rclog << lock << "Error: cannot upgrade. RCAttr constructor connects to working file." << unlock;
        throw;
    }
    LoadPackInfo();
    if (PackType()==PackS)
        for (int i = 0; i < NoPack(); i++) {
            DPN& dpn( dpns[i] );
            dpn.local_min = 0;
            dpn.local_max = -1;
        }
    SetFileFormat(10);
    CreateDPNFile();
    string fn(AttrFileName(file));
    string backup_name(fn);
    backup_name += "bck";
    try {
        RenameFile(fn,backup_name);
    } catch(...) {
        rclog << lock << "Internal upgrade error: unable to rename " << fn << " to " << backup_name << "." << unlock;
        throw;
    }
    SaveHeader();
    try {
        RemoveFile(backup_name);
    } catch(...) {
        rclog << lock << "Internal upgrade error: unable to remove " << fn << " backup file (change *.bck to *.ctb before next startup)." << unlock;
        throw;
    }
}

void RCAttr::CreateDPNFile()
{
    if(NoPack() == 0)
        return;
    const int buf_size = 10000;
    char buf[buf_size];
    IBFile fdpn;

    fdpn.OpenCreateEmpty(DPNFileName());

    ushort buffer_pos = 0;
    for(uint slot = 0; slot < (uint)NoPack() + 1; slot++) {
        if(buffer_pos + 37 > buf_size) {
            fdpn.Write(buf, buffer_pos);
            buffer_pos = 0;
        }
        if(slot < 2)
            StoreDPN(NoPack() - 1, buf + buffer_pos);       // double slot for last pack info for rollback handling
        else
            StoreDPN(slot - 2, buf + buffer_pos);
        buffer_pos += 37;
    }
    fdpn.Write(buf, buffer_pos);
    fdpn.Close();
}


// 将dpn转换成buff
void RCAttr::StoreDPN(DPN dpn,char *buf)
{
    if(dpn.no_nulls == (uint) (dpn.no_objs) + 1) {
        *((int*) (buf)) = PF_NULLS_ONLY;
        *((ushort*) (buf + 34)) = 0; // no_nulls (0 is special value here)
    } else {
        *((int*) (buf)) = dpn.pack_file;
        *((ushort*) (buf + 34)) = (ushort) dpn.no_nulls;
    }
    *((uint*) (buf + 4)) = dpn.pack_addr;

    *((_uint64*) (buf + 8)) =   dpn.local_min;
    *((_uint64*) (buf + 16)) =  dpn.local_max;

    *((_uint64*) (buf + 24)) =  dpn.sum_size;

    *((ushort*) (buf + 32)) =   dpn.no_objs;
    *((uchar*) (buf + 36)) = 0;
}


void RCAttr::StoreDPN(uint pack_no, char* buf)
{
    DPN const& dpn(dpns[pack_no]);
    if(dpn.no_nulls == (uint) (dpn.no_objs) + 1) {
        *((int*) (buf)) = PF_NULLS_ONLY;
        *((ushort*) (buf + 34)) = 0; // no_nulls (0 is special value here)
    } else {
        *((int*) (buf)) = dpn.pack_file;
        *((ushort*) (buf + 34)) = (ushort) dpn.no_nulls;
    }
    *((uint*) (buf + 4)) = dpn.pack_addr;

    *((_uint64*) (buf + 8)) =   dpn.local_min;
    *((_uint64*) (buf + 16)) =  dpn.local_max;

    *((_uint64*) (buf + 24)) =  dpn.sum_size;

    *((ushort*) (buf + 32)) =   dpn.no_objs;
    *((uchar*) (buf + 36)) = 0;
}

string RCAttr::AttrPackFileName(int n_pack)
{
    LoadPackInfo();
    return AttrPackFileNameDirect(attr_number, dpns[n_pack].pack_file, path);
}

string RCAttr::AttrPackFileNameDirect(int attr_number, int n_file, const std::string& path)
{
    BHASSERT(n_file>=0, "Invalid input pack number!");
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
    filename += fnm;
    return filename;
}

string RCAttr::DPNFileName() const
{
    char fnm[] = {"TA00000DPN.ctb"};
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += fnm;

    if(load_data_from_truncate_partition) { // 如果是从删除分区后的dpn中加载,需要从指定文件加载
        filename += std::string(".ntrunc");
    }

    return filename;
}

std::string RCAttr::LongSessionCommitFile()
{
    char fnm[] = {"TA00000LTS.ctb"};
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += fnm;
    return filename;
}

// return 1:from long session commit
// return 2:from sort session commit or other commit
int RCAttr::LoadLongSessionComitFile(std::string &partname,std::vector<int>& del_pack_file_lst)
{
    std::string lts_name = LongSessionCommitFile();
    if(!DoesFileExist(lts_name)) {
        return 2;
    } else {
        IBFile ltsfile;
        ltsfile.OpenReadOnly(lts_name);
        short _partnamelen = 0;
        char  _partname[256];

        int   _del_file_number = 0;
        int   _del_file_index = 0;

        ltsfile.ReadExact(&_partnamelen,sizeof(short));
        ltsfile.ReadExact(_partname,_partnamelen);
        _partname[_partnamelen]=0;

        partname = std::string(_partname);

        ltsfile.ReadExact(&_del_file_number,sizeof(int));
        del_pack_file_lst.clear();
        for(int i=0; i<_del_file_number; i++) {
            ltsfile.ReadExact(&_del_file_index,sizeof(int));
            del_pack_file_lst.push_back(_del_file_index);
        }
        ltsfile.Close();

    }
    return 1;
}

void RCAttr::ClearLongSessionCommitFile()
{
    std::string f=LongSessionCommitFile();
    if(DoesFileExist(f)) {
        RemoveFile(f);
    }
}


int RCAttr::SaveLongSessionComitFile(const char* partname,const std::vector<int> del_pack_file_lst)
{
    IBFile ltsfile;
    ltsfile.OpenCreateEmpty(LongSessionCommitFile());
    int len=strlen(partname);
    ltsfile.Write(&len,sizeof(short));
    ltsfile.Write(partname,len);

    int vectsize=del_pack_file_lst.size();
    ltsfile.Write(&vectsize,sizeof(int));
    for(int i=0; i<vectsize; i++) {
        int file=del_pack_file_lst[i];
        ltsfile.Write(&file,sizeof(int));
    }
    ltsfile.Close();
    return 0;
}


void RCAttr::MergeIndexHash(DMThreadGroup *ptlist,uint s_id)
{
    if(ct.IsPackIndex()) {
        ptlist->LockOrCreate()->Start(boost::bind(&RCAttr::DoMergeIndexHash,this,_1,s_id));
    }
}

void RCAttr::DoMergeIndexHash(DMThreadData *tl,uint s_id)
{
    if(!ct.IsPackIndex()) return;
    bool ispacks=PackType() == PackS;
    char mergedbname[300];
    ldb_index->LoadHeader();//cont fix dma-691,index_info maybe loaded before data loading,so refresh latest data.
    ldb_index->LoadForUpdate(GetLoadPartName(),s_id,true/*合并分区的时候，不应该删除新生成索引数据*/);
    apindex *pindex=ldb_index->GetReadIndex(GetLoadPartName());
    apindex *ptmpindex=ldb_index->GetIndex(GetLoadPartName(),s_id);//GetSaveSessionId());
    ptmpindex->CommitMap(*pindex);
    //ptmpindex->LoadMap();
    //ptmpindex->MergeSrcDbtoMap(*pindex);
    //ldb_index->MoveToGlobal(table_number,s_id,GetLoadPartName());
}

bool RCAttr::IsPackIndex()
{
    return ct.IsPackIndex();
}

void RCAttr::MergeIndex(DMThreadGroup *ptlist,uint s_id)
{
    if(ct.IsPackIndex()) {
        ptlist->LockOrCreate()->Start(boost::bind(&RCAttr::DoMergeIndex,this,_1,s_id));
    }
}

void RCAttr::DoMergeIndex(DMThreadData *tl,uint s_id)
{
    if(!ct.IsPackIndex()) return;
    bool ispacks=PackType() == PackS;
    char mergedbname[300];
    ldb_index->LoadHeader();//cont fix dma-691,index_info maybe loaded before data loading,so refresh latest data.
    ldb_index->LoadForUpdate(GetLoadPartName(),s_id,true/*合并分区的时候，不应该删除新生成索引数据*/);
    apindex *psrcindex=ldb_index->GetReadIndex(GetLoadPartName());
    apindex *pindex=ldb_index->GetGlobal(table_number,s_id,GetLoadPartName());//GetSaveSessionId());
    apindex mergeidx(psrcindex->MergeIndexPath(mergedbname),"");
    mergeidx.MergeFromHashMap(*psrcindex,*pindex);
    mergeidx.ReloadDB();
    ldb_index->RemoveGlobal(table_number,s_id,GetLoadPartName());
    ldb_index->ClearTempDB(GetLoadPartName(),s_id);//GetSaveSessionId());
}


void RCAttr::ClearUpRsi()
{
    rsi_hist_update = NULL;
    rsi_cmap_update = NULL;
}

time_t RCAttr::UpdateTime() const
{
    string fname = AttrFileName(current_state);
    int result = (int)GetFileTime(fname);
    if (result < 0) {
        rclog << "Error: " << fname << " status can not be obtained" << endl;
        return 0;
    }
    return (time_t) result;
}
void RCAttr::CommitSaveSessionThread(DMThreadData *pl)
{
    CommitSaveSession();
}
void RCAttr::CommitSaveSession(bool same_file)
{
    //WaitForSaveThreads();
    uint tmp_sessid=GetSaveSessionId();
    uint connid=0;
    if(ConnectionInfoOnTLS.IsValid())
        connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;
    if(ct.IsPackIndex())
        rclog << lock << "commit begin :"<<path<<" colid "<< attr_number <<" part: "<<GetLoadPartName()<<unlock;

    // if save file is not accessible for writing (or not exists), skip the attribute
    // WARNING: dangerous code  --> if(access(fname1,W_OK|F_OK)!=0) return;
    try {

        // 先判断是否是长会话数据合并流程来的数据
        if(DoesFileExist(LongSessionCommitFile())) {
            commit_long_session_merge_data();   // 提交长会话上的数据合并
        }

        string fname0( AttrFileName(0, !same_file) ); // AB Switch: generate alternative name
        string fname1( AttrFileName(1) );

        RemoveFile(fname0);
        RenameFile(fname1, fname0);
        if(partitioninfo==NULL) {
            rclog << lock << Name() << ": commit on a empty partition,table closed?" << unlock;
            partitioninfo=new attr_partitions(attr_number,path.c_str());
            partitioninfo->SetSessionID(GetSaveSessionId());
            partitioninfo->LoadNFBlocks(nfblocks);
        }
        // check connection id?
        partitioninfo->Commit(GetSaveSessionId());
        // update not full block list
        partitioninfo->LoadNFBlocks(nfblocks);

        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        if(ldb_index!=NULL) { // 只会提交当前分区的leveldb数据
            // maybe a empty leveldb (no any partition committed)
            ldb_index->Commit(table_number,GetLoadPartName(), tmp_sessid);
            delete ldb_index;
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
    } catch(DatabaseRCException &e) {
        rclog << lock << Name() << ": CommitWork failed! Database may be corrupted! msg:" <<e.what()<< unlock;
        throw;
    } catch(...) {
        rclog << lock << Name() << ": CommitWork failed! Database may be corrupted!" << unlock;
        throw;
    }
    SetSaveSessionId(INVALID_TRANSACTION_ID);
    if(ct.IsPackIndex())
        rclog << lock << "Commit end :"<<path<<" colid "<< attr_number <<" part: "<<GetLoadPartName()<<unlock;
    file = 0;
}

void RCAttr::Rollback(uint s_id)
{
    DeleteRSIs();
    uint connid=0;
    if(ConnectionInfoOnTLS.IsValid())
        connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;

    if(GetSaveSessionId() != INVALID_TRANSACTION_ID) {
        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        if(ldb_index!=NULL) {
            ldb_index->Rollback(table_number,GetLoadPartName(),GetSaveSessionId());
            delete ldb_index;
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }

        if(partitioninfo==NULL) {
            rclog << lock << Name() << ": rollback on a empty partition,table closed?" << unlock;
            partitioninfo=new attr_partitions(attr_number,path.c_str());
            partitioninfo->SetSessionID(GetSaveSessionId());
            partitioninfo->LoadNFBlocks(nfblocks);
        }
        if(partitioninfo!=NULL) {
            std::vector<int> rmfiles;
            if(s_id != GetSaveSessionId()) {
                rclog << lock << "RCAttr::Rollback error <s_id:"<<s_id<<",save_session_id:"<<GetSaveSessionId()<<">."<<unlock;
            }
            partitioninfo->Rollback(GetSaveSessionId(),rmfiles);
            for(int i=0; i<rmfiles.size(); i++) {
                //TODO: 测试正常后,改为删除
                //clear packfiles
                //RemoveFile(AttrPackFileNameDirect(attr_number,rmfiles[i],path));
                string packfilename=AttrPackFileNameDirect(attr_number, rmfiles[i], path);
                string newfilename=packfilename+".isolate";
                if(DoesFileExist(newfilename))
                    RemoveFile(newfilename);
                RenameFile(packfilename,newfilename);
                rclog << lock << "(RCAttr::Rollback)Find isolated file,need to clear manually:" << packfilename<<".isolate"<<unlock;
            }
        }
        if(s_id == 0 || s_id == GetSaveSessionId()) {
            RemoveFile(AttrFileName(1));
            SetSaveSessionId(INVALID_TRANSACTION_ID);
        } else {
            rclog << lock << "Rollback error - invalid save session_id = " << GetSaveSessionId() << unlock;
        }
    }
}

void RCAttr::DeleteRSIs()
{
    if (process_type == ProcessType::DATAPROCESSOR) {
        delete rsi_hist_update;
        delete rsi_cmap_update;
    } else {
        if(rsi_manager && rsi_hist_update) {
            rsi_manager->UpdateIndex(rsi_hist_update, 0, true);
        }
        if(rsi_manager && rsi_cmap_update) {
            rsi_manager->UpdateIndex(rsi_cmap_update, 0, true);
        }
    }
    rsi_hist_update = 0;
    rsi_cmap_update = 0;
}


void RCAttr::LoadPackInfo_physical(Transaction* trans)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
    FETOperator fet("RCAttr::LoadPackInfo()");
#endif
    IBGuard guard(dpns_load_mutex);

    if(!GetPackInfoCollapsed()) {
        //pthread_mutex_unlock(&cs);
        return;
    }
    mytimer attr_loadtm;
    attr_loadtm.Start();
    if((uint)NoPack() > GetNoPackPtr())
        SetNoPackPtr((uint)NoPack());
    // load packs descriptions (file of a pack, localization in file, statistics, ...)
    if (FileFormat() != 9)
        LoadAllDPN(trans);
    if (FileFormat() == 9 || GetDictOffset() != 0) {
        IBFile fattr;
        char* buf_ptr = NULL;
        string fn(AttrFileName(file));
        try {
            fattr.OpenReadOnly(fn);
            int const SIZE_OF_DPN =
                sizeof ( static_cast<DPN*>( NULL )->pack_file )
                + sizeof ( static_cast<DPN*>( NULL )->pack_addr )
                + sizeof ( static_cast<DPN*>( NULL )->local_min )
                + sizeof ( static_cast<DPN*>( NULL )->local_max )
                + sizeof ( static_cast<DPN*>( NULL )->sum_size )
                + sizeof ( static_cast<DPN*>( NULL )->no_objs )
                /* read comments in RCAttr.h for DPN::no_nulls and look at RestoreDPN implementation,
                 * that is why sizeof ( static_cast<DPN*>( NULL )->no_nulls ) cannot be used here,
                 * no_nulls are stored on disk as ushort, but represented as uint at runtime in engine. */
                + sizeof ( ushort );

            /* Ensure that on disk physical DPN layout wont change unexpectedely. */
            BHASSERT_WITH_NO_PERFORMANCE_IMPACT( SIZE_OF_DPN == 36 );

            int fin_size = (int)fattr.Seek(0, SEEK_END);
            buf_ptr = new char [(fin_size - GetPackOffset()) + 1];
            fattr.Seek(GetPackOffset(), SEEK_SET);
            fattr.Read(buf_ptr, fin_size - GetPackOffset());
            if (FileFormat() == 9) {
                dpns.resize(GetNoPackPtr(), trans);
                char* buf = buf_ptr;
                for(int i=0; i<NoPack(); i++) {
                    RestoreDPN(buf, i);
                    buf += SIZE_OF_DPN;
                }
            }
            if (GetDictOffset() != 0 && (fin_size - GetDictOffset())) {
                if(!Type().IsLookup())
                    LoadDictionaries(buf_ptr);
                else
                    //[mk]
                {
                    if(process_type == ProcessType::BHLOADER || process_type == ProcessType::DIM)
                        dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this));
                    else
                        dic = trans->GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this));
                }
                // [emk]

            }

            delete [] buf_ptr;
            fattr.Close();
        } catch (DatabaseRCException&) {
            delete [] buf_ptr;
            SetPackInfoCollapsed(false);
            //pthread_mutex_unlock(&cs);
            rclog << lock << "Internal error: unable to open/read " << fn << " file." << unlock;
            throw;  //newly added
        }

        //try {
        //  if (Type().IsLookup() && GetDictOffset() != 0)
        //  if(process_type != ProcessType::BHLOADER)
        //  dic = ConnectionInfoOnTLS->GetTransaction()->GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this, true));
        //  else
        //  dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this, true));
        //}
        //catch (DatabaseRCException&)
        //{
        //  SetPackInfoCollaped(false);
        //  rclog << lock << "Internal error: unable to open/read " << AttrFileName(file) << " file." << unlock;
        //  throw;
        //}
    }
    if(NoPack()>0 && dpns.empty())
        rclog << lock << "Error: out of memory (" << NoPack() << " bytes failed). (22)" << unlock;

    attr_loadtm.Stop();
    if(rccontrol.isOn() && PRELOAD_OPER_LOG>=1) {
        rclog<<lock<<"load attr  "<<table_number<<"/"<<attr_number<< ":"<< F2MS(attr_loadtm.GetTime())<<"ms."<<unlock;
    }

    SetPackInfoCollapsed(false);

}

void RCAttr::LoadDictionaries(const char* buf_ptr)
{
    AttributeType a_type = TypeName();
    // load dictionary for the whole attribute
    const char* buf = buf_ptr + (GetDictOffset() - GetPackOffset());
    // dictionary for symbolic (text) values - min, max, FTree object
    if(Type().IsLookup()) {
        SetMinInt64(*((_uint64*)(buf)));
        SetMaxInt64(*((_uint64*)(buf+8)));
        buf+=16;
        if(!dic)
            dic = boost::shared_ptr<FTree>(new FTree());
        dic->Init((uchar*&)buf);        // note that dic.Init will shift buf to the end of minicompressor stream
    }
    // dictionary for numerical values - min, max
    else if(
        ATI::IsNumericType(a_type) ||
        ATI::IsDateTimeType(a_type) || ATI::IsDateTimeNType(a_type)) {
        SetMinInt64(*((_uint64*)(buf)));
        SetMaxInt64(*((_uint64*)(buf+8)));
        buf+=16;
    } else {
        // ... other types of dictionary ...
    }
}

boost::shared_ptr<FTree> RCAttr::LoadLookupDictFromFile() // bool include_restore_dpn)
{
    string fn;
    if (GetDictOffset() != 0) {
        IBFile fattr;
        char* buf_ptr = NULL;
        try {
            fattr.OpenReadOnly(fn = AttrFileName(file));
            int fin_size = (int)fattr.Seek(0, SEEK_END);
            buf_ptr = new char [(fin_size - GetPackOffset()) + 1];
            fattr.Seek(GetPackOffset(), SEEK_SET);
            fattr.ReadExact(buf_ptr, fin_size - GetPackOffset());
            if(GetDictOffset() != 0 && (fin_size - GetDictOffset()))
                LoadDictionaries(buf_ptr);
            delete [] buf_ptr;
            fattr.Close();
        } catch (DatabaseRCException&) {
            delete [] buf_ptr;
            //rclog << lock << "Internal error: unable to load column dictionary from " << fn << ". " << GetErrorMessage(errno) << unlock;
            throw;
        }
    }
    return dic;
}

void RCAttr::LoadAllDPN(Transaction* trans)
{
    if(NoPack() == 0)
        return;
    try {
        IBFile fdpn;
        fdpn.OpenReadOnly(DPNFileName());
        fdpn.Close();
        dpns.resize(NoPack(), trans); /* FIXME */
    } catch (DatabaseRCException&) {
        rclog << lock << "Internal error: unable to open " << DPNFileName() << " file." << unlock;
        throw; //newly added
    }
    return;
}

void RCAttr::RestoreDPN(char* buf, DPN& dpn, bool reset_pack)
{
    if(reset_pack) {
        dpn.pack.reset();
    }
    dpn.pack_file = *((int*)buf);
    dpn.pack_addr = *((uint*)(buf+4));
    dpn.local_min = *((_uint64*)(buf+8));
    dpn.local_max = *((_uint64*)(buf+16));
    if(PackType() == PackN)
        dpn.sum_size = *((_uint64*)(buf+24));
    else
        dpn.sum_size = ushort(*((_uint64*)(buf+24)));
    dpn.no_objs = *((ushort*)(buf+32));
    dpn.no_nulls = *((ushort*)(buf+34));
    if(dpn.pack_file == PF_NULLS_ONLY)
        dpn.no_nulls = dpn.no_objs + 1;
    if(dpn.pack_file == PF_NULLS_ONLY ||
       (PackType() == PackN && dpn.local_min == dpn.local_max && dpn.no_nulls==0)) {
        dpn.pack_mode = PACK_MODE_TRIVIAL;                  // trivial data (only nulls or all values are the same), no physical pack
        dpn.is_stored = false;
    } else if(dpn.pack_file == PF_NO_OBJ) {
        dpn.pack_mode = PACK_MODE_EMPTY;                    // empty pack, no objects
        dpn.is_stored = false;
    } else {
        dpn.pack_mode = PACK_MODE_UNLOADED;                 // non trivial pack - data on disk
        dpn.is_stored = true;
    }

    if(FileFormat() > 9) {
        // restore 37-th byte
    }
}

void RCAttr::RestoreDPN(char* buf, uint pack_no)
{
    RestoreDPN( buf, dpns[pack_no] );
}

//void RCAttr::WriteUnique(RSValue v)
//{
//  IBFile fattr;
//
//  if(v!=RS_UNKNOWN)
//  {
//      SetUniqueUpdated(true);
//      if(v==RS_ALL) SetUnique(true);
//      if(v==RS_NONE) SetUnique(false);
//
//      //char flags = char(nulls_mode)+(declared_unique?4:0)+(is_primary?8:0)+(is_unique?16:0)+(is_unique_updated?32:0);
//      char flags = char(Type().GetNullsMode()) +(IsUnique()?16:0)+(IsUniqueUpdated()?32:0);
//      string fname(AttrFileName(0));
//      fattr.OpenReadWriteWithThreadAffinity(fname);
//      fattr.Seek(25,SEEK_SET);
//      fattr.Write(&flags,1);
//      fattr.CloseWithThreadAffinity(fname);
//  }
//}

PackOntologicalStatus RCAttr::GetPackOntologicalStatus(int pack_no)
{
    LoadPackInfo();
    DPN const* dpn( pack_no >= 0 ? &dpns[pack_no] : NULL );
    if(pack_no < 0 || dpn->pack_file == PF_NULLS_ONLY || dpn->no_nulls-1 == dpn->no_objs)
        return NULLS_ONLY;
    if(PackType() == PackN) {
        if(dpn->local_min == dpn->local_max) {
            if(dpn->no_nulls == 0)
                return UNIFORM;
            return UNIFORM_AND_NULLS;
        }
    }
    return NORMAL;
}

bool RCAttr::ShouldExist(int pack_no)
{
    LoadPackInfo();
    DPN const& dpn(dpns[pack_no]);
    if(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ || dpn.no_nulls - 1 == dpn.no_objs)
        return false;
    if(PackType() == PackN) {
        if(dpn.no_nulls == 0 && dpn.local_min == dpn.local_max)
            return false;
    } else
        return dpn.pack_mode == PACK_MODE_UNLOADED;
    return true;
}


//old name GetTable_S
RCBString RCAttr::GetValueString(const _int64 obj)
{
    if(obj == NULL_VALUE_64)
        return RCBString();
    int pack = (int) (obj >> 16);
    if(PackType() == PackS) {
        DPN const& dpn( dpns[pack] );
        if(dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_file == PF_NULLS_ONLY)
            return RCBString();
        assert(dpn.pack->IsLocked());
        AttrPackS *cur_pack = (AttrPackS*)dpn.pack.get();

        if(cur_pack->IsNull((int) (obj & 65535)))
            return RCBString();

        int len = cur_pack->GetSize((int) (obj & 65535));
        if(len)
            return RCBString(cur_pack->GetVal((int) (obj & 65535)), len);
        else
            return ZERO_LENGTH_STRING;
    }
    _int64 v = GetValueInt64(obj);
    return DecodeValue_S(v);
}

RCBString RCAttr::GetNotNullValueString(const _int64 obj)
{
    int pack = (int) (obj >> 16);
    if(PackType() == PackS) {
        BHASSERT(pack <= dpns.size(), "Reading past the end of DPNs");
        DPN const& dpn( dpns[pack] );
        BHASSERT(dpn.pack->IsLocked(), "Access unlocked pack");
        AttrPackS *cur_pack = (AttrPackS*)dpn.pack.get();
        BHASSERT(cur_pack!=NULL, "Pack ptr is null");
        int len = cur_pack->GetSize((int) (obj & 65535));
        if(len)
            return RCBString(cur_pack->GetVal((int) (obj & 65535)), len);
        else
            return ZERO_LENGTH_STRING;
    }
    _int64 v = GetNotNullValueInt64(obj);
    return DecodeValue_S(v);
}

void RCAttr::GetValueBin(_int64 obj, int& size, char* val_buf)      // original 0-level value (text, string, date, time etc.)
{
    if(obj == NULL_VALUE_64)
        return;
    AttributeType a_type = TypeName();
    size = 0;
    BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
    LoadPackInfo();
    int pack = (int)(obj >> 16);    // simplified version: all packs are full
    DPN const& dpn( dpns[pack] );
    if(dpn.pack_file == PF_NULLS_ONLY)
        return;
    if(ATI::IsStringType(a_type)) {
        if(PackType() == PackN) {
            _int64 res = GetValueInt64(obj);
            if(res == NULL_VALUE_64)
                return;
            size = dic->ValueSize((int)res);
            memcpy(val_buf, dic->GetBuffer((int)res), size);
            return;
        } else {            // no dictionary
            if(dpn.pack_mode == PACK_MODE_TRIVIAL)
                return;
            assert(dpn.pack->IsLocked());
            size = ((AttrPackS*)dpn.pack.get())->GetSize((int)(obj & 65535));
            memcpy(val_buf, ((AttrPackS*)dpn.pack.get())->GetVal((int)(obj & 65535)), size);
            return;
        }
    } else if(ATI::IsInteger32Type(a_type)) {
        size = 4;
        _int64 v = GetValueInt64(obj);
        if(v == NULL_VALUE_64)
            return;
        *(int*)val_buf = int(v);
        val_buf[4] = 0;
        return;
    } else if(a_type == RC_NUM || a_type == RC_BIGINT || ATI::IsRealType(a_type) || ATI::IsDateTimeType(a_type)) {
        size = 8;
        _int64 v = GetValueInt64(obj);
        if(v == NULL_VALUE_64)
            return;
        *(_int64*)(val_buf) = v;
        val_buf[8] = 0;
        return;
    }
    return;
}

RCValueObject RCAttr::GetValue(_int64 obj, bool lookup_to_num)
{
    if(obj == NULL_VALUE_64)
        return RCValueObject();
    AttributeType a_type = TypeName();
    BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
    RCValueObject ret;
    if(!IsNull(obj)) {
        if(ATI::IsTxtType(a_type) && !lookup_to_num)
            ret = GetNotNullValueString(obj);
        else if(ATI::IsBinType(a_type)) {
            int tmp_size = GetLength(obj);
            RCBString rcbs(NULL, tmp_size, true);
            GetValueBin(obj, tmp_size, rcbs.val);
            rcbs.null = false;
            ret = rcbs;
        } else if(ATI::IsIntegerType(a_type))
            ret = RCNum(GetNotNullValueInt64(obj), -1, false, a_type);
        else if(a_type == RC_TIMESTAMP) {
#ifdef PURE_LIBRARY
            BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
            // needs to convert UTC/GMT time stored on server to time zone of client
            RCBString s = GetValueString(obj);
            MYSQL_TIME myt;
            int not_used;
            // convert UTC timestamp given in string into TIME structure
            str_to_datetime(s.Value(), s.len, &myt, TIME_DATETIME_ONLY, &not_used);
            if(!IsTimeStampZero(myt)) {
                // compute how many seconds since beg. of EPOCHE it is
                my_time_t secs = sec_since_epoch_TIME(&myt);
                // convert to local time in TIME structure
                ConnectionInfoOnTLS.Get().Thd().variables.time_zone->gmt_sec_to_TIME(&myt, secs);
            }
            return RCDateTime(myt.year, myt.month, myt.day, myt.hour, myt.minute, myt.second, RC_TIMESTAMP);
#endif
        } else if(ATI::IsDateTimeType(a_type))
            ret = RCDateTime(this->GetNotNullValueInt64(obj), a_type);
        else if(ATI::IsRealType(a_type))
            ret = RCNum(this->GetNotNullValueInt64(obj), 0, true, a_type);
        else if(lookup_to_num || a_type == RC_NUM)
            ret = RCNum((_int64)GetNotNullValueInt64(obj), Type().GetScale());
    }
    return ret;
}

RCDataType& RCAttr::GetValue(_int64 obj, RCDataType& value, bool lookup_to_num)
{
    if(obj == NULL_VALUE_64 || IsNull(obj))
        value = ValuePrototype(lookup_to_num);
    else {
        AttributeType a_type = TypeName();
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
        if(ATI::IsTxtType(a_type) && !lookup_to_num)
            ((RCBString&)value) = GetNotNullValueString(obj);
        else if(ATI::IsBinType(a_type)) {
            int tmp_size = GetLength(obj);
            ((RCBString&)value) = RCBString(NULL, tmp_size, true);
            GetValueBin(obj, tmp_size, ((RCBString&)value).val);
            value.null = false;
        } else if(ATI::IsIntegerType(a_type))
            ((RCNum&)value).Assign(GetNotNullValueInt64(obj), -1, false, a_type);
        else if(ATI::IsDateTimeType(a_type)) {
            ((RCDateTime&)value) = RCDateTime(this->GetNotNullValueInt64(obj), a_type);
        } else if(ATI::IsRealType(a_type))
            ((RCNum&)value).Assign(this->GetNotNullValueInt64(obj), 0, true, a_type);
        else
            ((RCNum&)value).Assign(this->GetNotNullValueInt64(obj), Type().GetScale());
    }
    return value;
}

_int64 RCAttr::GetNoNulls(int pack)
{
    LoadPackInfo();
    if(pack == -1)
        return NoNulls();
    return dpns[pack].no_nulls;
}

unsigned int RCAttr::GetNoValues(int pack)
{
    LoadPackInfo();
    return dpns[pack].GetNoObj();
}

ushort RCAttr::GetActualSize(int pack)
{
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return 0;
    ushort ss = (ushort)dpns[pack].sum_size;
    if(Type().IsLookup() || PackType() != PackS || ss == 0)
        return Type().GetPrecision();
    return ss;
}

_int64 RCAttr::GetSum(int pack, bool &nonnegative)
{
    LoadPackInfo();
    DPN const& dpn( dpns[pack] );
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY || dpns.empty() || Type().IsString())
        return NULL_VALUE_64;
    if(!Type().IsFloat() && (dpn.local_min < (MINUS_INF_64 / 65536) || dpn.local_max > (PLUS_INF_64 / 65536)))
        return NULL_VALUE_64;                               // conservative overflow test for int/decimals
    nonnegative = (dpn.local_min >= 0);
    return dpn.sum_size;
}

double RCAttr::GetSumD(int pack)
{
    LoadPackInfo();
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return NULL_VALUE_D;
    return *(double*)(&dpns[pack].sum_size);
}

_int64 RCAttr::GetMinInt64(int pack)
{
    LoadPackInfo();
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return NULL_VALUE_64;
    return dpns[pack].local_min;
}

_int64 RCAttr::GetMaxInt64(int pack)
{
    LoadPackInfo();
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return NULL_VALUE_64;
    return dpns[pack].local_max;
}


RCBString RCAttr::GetMaxString(int pack)
{
    LoadPackInfo();
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return RCBString();
    char* s = (char*)&dpns[pack].local_max;
    int max_len = (int)dpns[pack].sum_size;
    if(max_len > 8)
        max_len = 8;
    int min_len = max_len - 1;
    while(min_len >= 0 && s[min_len] != '\0')
        min_len--;
    return RCBString(s, min_len >= 0 ? min_len : max_len, true);
}

RCBString RCAttr::GetMinString(int pack)
{
    LoadPackInfo();
    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return RCBString();
    char* s = (char*)&dpns[pack].local_min;
    int max_len = (int)dpns[pack].sum_size;
    int min_len = (max_len > 8 ? 8 : max_len);
    while(min_len > 0 && s[min_len - 1] == '\0')
        min_len--;
    return RCBString(s, min_len, true);
}


std::auto_ptr<DPN> RCAttr::GetDPNWithoutDataPack(const DataPackId& dpid)
{
    LoadPackInfo();
    return dpns[(int)dpid].CloneWithoutPack();
}

std::auto_ptr<DataPack> RCAttr::GetDataPack(const DataPackId& dpid, ConnectionInfo& conn)
{
    LoadPackInfo();
    std::auto_ptr<DataPack> dp;
    if(ATI::IsStringType(TypeName()))
        dp = auto_ptr<DataPack>(new DataPackImpl<RCBString>(dpns[(int)dpid].GetNoObj()));
    else if(ATI::IsDateTimeType(TypeName()))
        dp = auto_ptr<DataPack>(new DataPackImpl<RCDateTime>(dpns[(int)dpid].GetNoObj()));
    else
        dp = auto_ptr<DataPack>(new DataPackImpl<RCNum>(dpns[(int)dpid].GetNoObj()));

    boost::shared_ptr<DataPackLock> dp_lock(new DataPackLock(*this, dpid));
    if(ATI::IsStringType(TypeName())) {
        dp->dp_lock = dp_lock;
        if(!Type().IsLookup()) {
            if(ShouldExist((int)dpid)) {
                dp->SetDecomposerID(((AttrPackS&)(*dpns[(int)dpid].pack)).GetDecomposerID());
                dp->outliers = ((AttrPackS&)(*dpns[(int)dpid].pack)).GetNoOutliers();
            } else
                dp->SetDecomposerID((uint)dpns[(int)dpid].local_min);
        }
    }

    _uint64 i = (((_uint64)dpid - packs_omitted) << 16);
    _uint64 last = i + GetNoValues(dpid - packs_omitted);
    ushort id = 0;
    for(; i < last; i++)
        GetValue(i, (*dp)[id++]);

    return dp;
}

int RCAttr::GetLength(_int64 obj)       // size of original 0-level value (text/binary, not null-terminated)
{
    BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
    LoadPackInfo();
    int pack = (int)(obj >> 16);                                    // simplified version: all packs are full
    DPN const& dpn( dpns[pack] );
    if(dpn.pack_file==PF_NULLS_ONLY)    return 0;
    if(PackType() != PackS) return Type().GetDisplaySize();
    return ((AttrPackS*)dpn.pack.get())->GetSize((int)(obj&65535));
}

int RCAttr::DecodeValue_T(_int64 code, char *val_buf)           // original 0-level value for a given 1-level code
{
    AttributeType a_type = TypeName();
    if(code==NULL_VALUE_64) {
        return 0;
    }
    int v=(int)code;
    if(Type().IsLookup()) {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(PackType() == PackN);
        memcpy(val_buf, dic->GetBuffer(v), dic->ValueSize(v));
        return dic->ValueSize(v);
    } else if(ATI::IsIntegerType(a_type)) {
        RCNum rcn(code, -1, false, a_type);
        RCBString local_rcb(val_buf, TEMP_VALUE_BUF_SIZE);
        return (int)strlen(rcn.ToRCString(local_rcb));
    } else if(ATI::IsRealType(a_type)) {
        //_gcvt_s(val_buf, TEMP_VALUE_BUF_SIZE, *((double*)(&code)), type == RC_REAL ? 15 : 6);
        snprintf( val_buf, ((a_type == RC_REAL) ? 15 : 6) + 1, "%.15f", *((double*)(&code)) );
        //gcvt(*((double*)(&code)), a_type == RC_REAL ? 15 : 6, val_buf);
        size_t s = strlen(val_buf);
        if(s && val_buf[s-1] == '.')
            val_buf[s-1] = 0;
        return (int)strlen(val_buf);
    } else if(a_type==RC_NUM) {
        Text(code,val_buf,Type().GetScale());
        return (int)strlen(val_buf);
    } else if(ATI::IsDateTimeType(a_type)) {
        RCDateTime rcdt(code, a_type);
        RCBString local_rcb(val_buf, TEMP_VALUE_BUF_SIZE);
        return (int)strlen(rcdt.ToRCString(local_rcb));
    }
    return 0;
}

RCBString RCAttr::DecodeValue_S(_int64 code)            // original 0-level value for a given 1-level code
{
    if(code == NULL_VALUE_64) {
        return RCBString();
    }
    if(Type().IsLookup()) {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(PackType() == PackN);
        int v = (int)code;
        return dic->GetRealValue(v);
    }
    char buf[TEMP_VALUE_BUF_SIZE];                  // the longest possible number or date
    int len = DecodeValue_T(code, buf);
    return RCBString(buf,len,true);
}

int RCAttr::EncodeValue_T(const RCBString& rcbs, int new_val, BHReturnCode* bhrc)   // 1-level code value for a given 0-level (text) value
// if new_val=1, then add to dictionary if not present
{
    if(bhrc)
        *bhrc = BHRC_SUCCESS;
    if(rcbs.IsNull()) return NULL_VALUE_32;
    if(ATI::IsStringType(TypeName())) {
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(PackType() == PackN);
        LoadPackInfo();
        int vs;
        if(new_val==0)  vs=dic->GetEncodedValue(rcbs);
        else            vs=dic->Add(rcbs);
        if(vs<0) return NULL_VALUE_32;
        return vs;
    }
    char const* val = rcbs.val;
    if(val == 0)
        val = ZERO_LENGTH_STRING;
    if(ATI::IsDateTimeType(TypeName()) || TypeName() == RC_BIGINT) {
        BHASSERT(0, "Wrong data type!");
    } else {
        RCNum rcn;
        BHReturnCode tmp_bhrc = RCNum::Parse(rcbs, rcn, TypeName());
        if(bhrc)
            *bhrc = tmp_bhrc;
        return (int)(_int64)rcn;
    }
    return NULL_VALUE_32;
}

// transform a RCNum value into 1-level code, take into account the precision etc.
// no changes for REAL; rounded=true iff v has greater precision than the column and the returned result is rounded down
_int64 RCAttr::EncodeValue64(RCDataType *v, bool &rounded, BHReturnCode* bhrc)
{
    rounded = false;
    if(bhrc)
        *bhrc = BHRC_SUCCESS;
    if(!v || v->IsNull())
        return NULL_VALUE_64;

    if((Type().IsLookup() && v->Type() != RC_NUM)) {
        return EncodeValue_T(v->ToRCString(), 0, bhrc);
    } else if(ATI::IsDateTimeType(TypeName()) || ATI::IsDateTimeNType(TypeName())) {
        return ((RCDateTime*) v)->GetInt64();
    }
    BHASSERT(PackType() == PackN, "Pack type must be numeric!");

    _int64 vv = ((RCNum*) v)->Value();
    int vp = ((RCNum*) v)->Scale();
    if(ATI::IsRealType(TypeName())) {
        if(((RCNum*)v)->IsReal())
            return vv; // already stored as double
        double res = double(vv);
        res /= Uint64PowOfTen(vp);
        //for(int i=0;i<vp;i++) res*=10;
        return *(_int64*) (&res); // encode
    }
    if(((RCNum*)v)->IsReal()) { // v is double
        double vd = *(double*)(&vv);
        vd *= Uint64PowOfTen(Type().GetScale());                // translate into _int64 of proper precision
        if(vd > PLUS_INF_64)
            return PLUS_INF_64;
        if(vd < MINUS_INF_64)
            return MINUS_INF_64;
        _int64 res = _int64(vd);
        if(fabs(vd - double(res)) > 0.01)
            rounded = true; // ignore errors which are 2 digits less than declared precision
        return res;
    }
    unsigned char dplaces = Type().GetScale();
    while(vp < dplaces) {
        if(vv < MINUS_INF_64 / 10)
            return MINUS_INF_64;
        if(vv > PLUS_INF_64 / 10)
            return PLUS_INF_64;
        vv *= 10;
        vp++;
    }
    while(vp > dplaces) {
        if(vv % 10 != 0)
            rounded = true;
        vv /= 10;
        vp--;
    }
    return vv;
}

_int64 RCAttr::EncodeValue64(const RCValueObject& v, bool& rounded, BHReturnCode* bhrc)
{
    return EncodeValue64(v.Get(), rounded, bhrc);
}

RCBString RCAttr::GetPrefix(int pack)
{
    LoadPackInfo();

    if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
        return 0;

    DPN const& dpn( dpns[pack] );
    char* min_s = (char*)&dpn.local_min;
    char* max_s = (char*)&dpn.local_max;


    int dif_pos = 0;
    for(; ( dif_pos < sizeof(_uint64) )
        && min_s[dif_pos]
        && max_s[dif_pos]
        && ( min_s[dif_pos] == max_s[dif_pos] ); ++ dif_pos )
        ;

    if ( dif_pos == 0 )
        return ZERO_LENGTH_STRING;

    RCBString s(min_s, dif_pos, true);
    return s;
}

uint AttributeTypeInfo::ExternalSize(RCAttr* attr, EDF edf)
{
    AttributeType attrt = attr->TypeName();
    return AttributeTypeInfo::ExternalSize(attrt, attr->Type().GetPrecision(), attr->Type().GetScale(), edf);
}

uint AttributeTypeInfo::ExtrnalSize(EDF edf)
{
    return AttributeTypeInfo::ExternalSize(attrt, precision, scale, edf);
}

uint AttributeTypeInfo::ExternalSize(AttributeType attrt, int precision, int scale, EDF edf)
{
    return DataFormat::GetDataFormat(edf)->ExtrnalSize(attrt, precision, scale);
}

int AttributeTypeInfo::TextSize()
{
    return DataFormat::GetDataFormat(TXT_VARIABLE)->ExtrnalSize(attrt, precision, scale, &collation);
}

bool AttributeTypeInfo::IsFixedSize(AttributeType attr_type, EDF edf)
{
    return !DataFormat::GetDataFormat(edf)->IsVariableSize(attr_type);
}
/* move to apindex
const char *RCAttr::MergeIndexName(char *name)
{
    sprintf(name,"%s_%d_mrg",GetLoadPartName(),attr_number);
    return name;
}
*/
void RCAttr::LockPackForUse(_int64 *pahead,unsigned pack_no, ConnectionInfo& conn)
{
    if(conn.killed())
        throw KilledRCException();
    LockPackForUse(pahead,pack_no, *conn.GetTransaction(),boost::bind(&ConnectionInfo::NotifyPackLoad, &conn));
}

// 删除分区数据:
// 通过控制文件传入分区名字和连接号
// 先检查控制文件和分区信息,可以做删除,则
//   1. 删除RSI CMAP/HIST.<tabid><attrid> --> xxx.ntrunc
//   2. 删除LevelDB索引
//         <Option> rename directory <Tabnum><AttrNum><PartName>(/W) --> xxx.trunc;
//          new header file attrindex_<TABNUM>_<ATTRNUM>.ctb.ntrunc
//   3. 删除数据文件 (Just check file is exists)
//   4. 删除DPN --> xxx.ntrunc
//   5. 删除partinfo --> xxx.ntrunc.
//   6. TA<attrnum>.ctb文件 --> xxx.ntrunc
//   7. Table.ctb --> xxx.ntrunc
//      no_obj in table.ctb always 0(dma-526)
//      暂忽略这个过程(7)，table.ctb无须变更
// 提交过程:
//   1. move RSI xxx.ntrunc->xxx
//   2. LevelDB  rmdir xxx.trunc ;
//   3. PackFile rm xxx
//   4. DPN   move xxx.ntrunc -> xxx
//   5. partinfo:move xxx.ntrunc -> xxx
//   6. replace TA<ATTRNUM>.ctb.ntrunc ->xxx
//   7. Table.ctb 暂忽略这个过程(7)，table.ctb无须变更
//   8. reload & update table
//   9. 删除控制文件(merge to step 5)
//
// 回滚过程:
//   1. RSI delete xxx.ntrunc
//   2. LevelDB rename xxx.trunc ->xxx
//   3. PACKFile : do nothing
//   4. DPN delete xxx.ntrunc
//   5. PARTINFO  delete xxx.ntrunc
//   6. remove TA<ATTRNUM>.ctb.ntrunc
//   7. Table.ctb 暂忽略这个过程(7)，table.ctb无须变更
//   8. 删除控制文件(merge to step 5)
//
//      以上操作只有对Table.ctl的修改在RCAttr之外进行
//
//    删除分区数据操作在表一级锁表
//       a.不能与loading并发
//       b.不能与delete/insert/update或其他删除分区操作并发
//       c.不能与查询并发
//    删除分区的事务问题:
//       删除分区操作完成的是预删除，提交后才生效，也就是说，即便是当前连接的会话，
//       提交前也无法看到删除的效果。
//       实际上，数据装载也是如此，在提交前，在任何会话，包括当前会话上，都不能看到新数据
//    关于a/b/s切换:
//       开始事务以前切换到s，并交换Read/Save,提交时先调用Table->CommitSaveSession,
//           然后CommitSwitch,s切换到a/b(switchabs).
//      Transaction::Commit==>Table->CommitSaveSession,Table->CommitSwitch
//    删除分区的操作，没有直接在TSxxxxx.ctb，而是在TSXXXXX.ctb.ntrunc。



// Copy data from src to dst,element*e_size bytes
//
void RCAttr::CopyStream(IBFile &dst,IBFile &src,int e_size,int elements)
{
#define CS_BFLEN 1024*64
    assert(e_size<CS_BFLEN);
    char internalbf[CS_BFLEN];
    int max_items=CS_BFLEN/e_size;
    while(elements>0) {
        int ritems=min(elements,max_items);
        src.ReadExact(internalbf, ritems*e_size);
        dst.WriteExact(internalbf, ritems*e_size);
        elements-=ritems;
    }
}

bool RCAttr::prepare_commit_long_session_merge_data() // 判断是否满足提交条件
{
    if(NeedToCreateRSIIndex()) { // fix dma-1462
        std::string knpath=rsi_manager->GetKNFolderPath();
        std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
        std::string newrsifn=rsifn+".ntrunc";
        if(DoesFileExist(newrsifn)) {
            return true;
        } else {
            rclog << lock <<" Error commit_long_session_merge_data : "<< Name() << ": rsi file "<< newrsifn << "  is not exist ,can not commit long session merge data process. " << unlock;
            return false;
        }
    } else {
        return true;
    }
}


bool RCAttr::commit_long_session_merge_data()
{
    try {
        std::string datapart_name="";
        std::vector<int> del_pack_file_list;
        LoadLongSessionComitFile( datapart_name,del_pack_file_list);

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
        if(ldb_index) {// 将所有的db_mrg--->db
            ldb_index->CommitTruncate_ltsession(apartinfo->name());
        }

        //   3. PackFile rm
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
        //   TSxxxxx.ctb.ntrunc->TSxxxxx.ctb
        std::string f0 = AttrFileName(1/*file*/);
        RemoveFile(f0);
        std::string f1 = AttrFileName(1/*file*/);
        RenameFile(f0+".ntrunc",f1);
    } catch(...) {
        rclog << lock << Name() << ": CommitTruncatePartiton failed! Database may be corrupted!" << unlock;
        throw;
    }
    return true;

}

bool RCAttr::CommitTruncatePartiton()
{
    try {
//  check operation conditions
        int sid=-1;
        uint connid=0;
        if(ConnectionInfoOnTLS.IsValid())
            connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;
        if(partitioninfo==NULL || !partitioninfo->GetTruncatePartInfo(sid) || sid!=connid)
            return false;//不是删除分区事务
        attr_partinfo *apartinfo=partitioninfo->gettruncpartptr(sid);
//   1. move RSI xxx.ntrunc->xxx

        bool novalidpack=false;
        if(NeedToCreateRSIIndex()) {
            std::string knpath=rsi_manager->GetKNFolderPath();
            std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
            std::string newrsifn=rsifn+".ntrunc";
            _int64 fsize=0;
            GetFileSize(newrsifn,fsize);
            int headlen=PackType() == PackN?15:14;
            if(headlen==fsize) novalidpack=true;
            assert(DoesFileExist(newrsifn)) ;
            RemoveFile( rsifn);
            RenameFile(newrsifn, rsifn);
        }

//   2. LevelDB  rmdir xxx.trunc ;
        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        if(ldb_index) {
            ldb_index->CommitTruncate(apartinfo->name());
        }
//   3. PackFile rm xxx.trunc
        int fileid=apartinfo->firstfile();
        while(fileid!=-1 && !novalidpack) {
            std::string dtfilename=AttrPackFileNameDirect(attr_number,fileid,path);
            if(!DoesFileExist(dtfilename))
                throw "no data pack file while commit.";
            RemoveFile(dtfilename);
            fileid=apartinfo->nextfile(fileid);
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
//   7. Table.ctb 暂忽略这个过程(7)，table.ctb无须变更
//   8. reload & update table

        //已经包含对leveldb/partitioninfo的重新装载
        // replaced by LoadDirtyData of RCTableImpl
        //Collapse();
        //GlobalDataCache::GetGlobalDataCache().DropObject(SpliceCoordinate(table_number, attr_number, 0 / DEFAULT_SPLICE_SIZE ));
        //Load(AttrFileName(file));
        // load from local cache of transaction!
        //LoadPackInfo();

    } catch(...) {
        rclog << lock << Name() << ": CommitTruncatePartiton failed! Database may be corrupted!" << unlock;
        throw;
    }
    file = FILE_READ_SESSION;
    SetSaveSessionId(INVALID_TRANSACTION_ID);
    return true;
//   9. 删除控制文件--external
}

bool RCAttr::RollbackTruncatePartition()
{
    try {
//  check operation conditions
        int sid=-1;
        uint connid=0;
        if(ConnectionInfoOnTLS.IsValid())
            connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;
        if(partitioninfo==NULL || !partitioninfo->GetTruncatePartInfo(sid) || sid!=connid)
            return false;//不是删除分区事务
//   1. RSI delete xxx.ntrunc
        if(NeedToCreateRSIIndex()) {
            std::string knpath=rsi_manager->GetKNFolderPath();
            std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
            std::string newrsifn=rsifn+".ntrunc";
            if(!DoesFileExist(newrsifn)) return false;
            RemoveFile( newrsifn);
        }
//   2. LevelDB  xxx.trunc ->xxx
        if(ct.IsPackIndex() && ldb_index==NULL) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        if(ldb_index) {
            ldb_index->RollbackTruncate(partitioninfo->gettruncpartptr(sid)->name());
            ldb_index->ClearMergeDB();
        }

//   3. PACKFile : do nothing
//   4. DPN delete xxx.ntrunc
        std::string dpnfn=DPNFileName();
        std::string newdpnfn=dpnfn+".ntrunc";
        if(DoesFileExist(newdpnfn))
            RemoveFile(newdpnfn);
//   5. PARTINFO  delete xxx.ntrunc
        partitioninfo->RollbackTruncate();
//   6. remove TA<ATTRNUM>.ctb.ntrunc
        RemoveFile(AttrFileName(file)+".ntrunc");
//   7. Table.ctb 暂忽略这个过程(7)，table.ctb无须变更
//   8. 删除控制文件--external
    } catch(...) {
        rclog << lock << Name() << ": RollbackTruncatePartition failed! Database may be corrupted!" << unlock;
        throw;
    }
    file = FILE_READ_SESSION;
    SetSaveSessionId(INVALID_TRANSACTION_ID);
    return true;
}

void RCAttr::RemoveTruncateControlFile()
{
    if(partitioninfo!=NULL)
        RemoveFile(partitioninfo->GetTruncateControlFile());
}

// do truncate ,but no commit.
// partition info stored in control file
bool RCAttr::TruncatePartition()
{
//  check operation conditions
    int sid=-1;
    uint connid=0;
    if(no_pack<1)
        return false;
    if(ConnectionInfoOnTLS.IsValid())
        connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;
    if(partitioninfo==NULL || !partitioninfo->GetTruncatePartInfo(sid) || sid!=connid)
        return false;//不是删除分区事务
//  prepare for trunc
    LoadPackInfo();
    attr_partinfo *apartinfo=partitioninfo->gettruncpartptr(sid);
    if(apartinfo==NULL)
        return false;
    SectionMap pmap;
    int lastobjs=partitioninfo->GetPartSection(pmap,apartinfo->name());
    bool trunc_lastp=apartinfo->getlastpack()==no_pack-1;
// 1. Create New RSI Data file
    std::string knpath=rsi_manager->GetKNFolderPath();
    std::string rsifn=RSIndexID(PackType() == PackN?RSI_HIST:RSI_CMAP, table_number, attr_number).GetFileName(knpath.c_str());
    std::string newrsifn=rsifn+".ntrunc";
    int new_totalpack=partitioninfo->getpacks()-apartinfo->getpacks();

    /*
        特别说明:DMA-1291,add by liujs
        1. 在只有一个分区的时候,删除分区流程从dpadmin中通过drop table 将全表进行删除了,如果直接进入该流程会报错
        2. 该流程只适用于存在多个分区时候,删除分区用
    */

    _int64 new_totalobjs=(_int64(new_totalpack-1)<<16)+lastobjs;
    assert(new_totalobjs>0);

    int novalidpack=0;
    //last pack always save ahead in both rsi&dpn
    (*pmap.rbegin()).second--;
    int lastend=(*pmap.rbegin()).second;
    if( NeedToCreateRSIIndex() && DoesFileExist(rsifn)) {
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

        _int64 rsiobjs=*(_int64 *)(header + current_read_loc*headlen+1);
        // last pack at header,so minus 1
        int rsipacks=*(int * )(header + current_read_loc*headlen+9)-1;
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
        memcpy(header+(1-current_read_loc)*headlen,header+current_read_loc*headlen,headlen);
        int rsileftpacks=0;
        SectionMap::iterator iter;
        for(iter=pmap.begin(); iter!=pmap.end(); iter++) {
            if(iter->first+1>=rsipacks) continue;
            int sectend=min(rsipacks+1,iter->second+1);
            if(sectend==lastend+1) sectend++;
            rsileftpacks+=sectend-iter->first;
        }
        *(_int64 *)(header+current_read_loc*headlen+1)=rsiobjs-((_int64)(rsipacks-rsileftpacks)<<16);//new_totalobjs;
        *(int *)(header+current_read_loc*headlen+9)=rsileftpacks;//new_totalpack;
        if(fsize>2*headlen) {
            // maximum pack len : 64*32=2048 bytes(CMAP)
            char lastpacks[2048*2];
            oldfile.ReadExact(lastpacks,rsi_packlen*2);
            int first_packstart=headlen*2+rsi_packlen*2;
            // backup current last pack
            if(trunc_lastp) {
                if(pmap.rbegin()->second+1<=rsipacks) {
                    memcpy(lastpacks+(1-current_read_loc)*rsi_packlen,lastpacks+current_read_loc*rsi_packlen,rsi_packlen);
                    oldfile.Seek((pmap.rbegin()->second)*rsi_packlen,SEEK_CUR);
                    //oldfile.Seek(-rsi_packlen,SEEK_END);
                    oldfile.ReadExact(lastpacks+current_read_loc*rsi_packlen,rsi_packlen);
                }
                // else last pack's rsi is nulls(no rsi)?
                else memset(lastpacks+current_read_loc*rsi_packlen,0,rsi_packlen);
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
            memcpy(lastpacks+(1-current_read_loc)*37,lastpacks+current_read_loc*37,37);
            oldfile.Seek((pmap.rbegin()->second)*37,SEEK_CUR);
            oldfile.ReadExact(lastpacks+current_read_loc*37,37);
        }
        if(lastobjs!=*(unsigned short *)(lastpacks+current_read_loc*37+32)+1) {
            //FIXME: objs in partition info wrong?
            //reset objs:
            lastobjs=*(unsigned short *)(lastpacks+current_read_loc*37+32)+1;
            new_totalobjs=(_int64(new_totalpack-1)<<16)+lastobjs;
        }
        newfile.WriteExact(lastpacks, 37*2);
        // initial range value
        newmax=*(_int64 *)(lastpacks+current_read_loc*37+16);
        newmin=*(_int64 *)(lastpacks+current_read_loc*37+8);
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

    int part_section_list_size=apartinfo->size();

    int *packvars=new int[ /*apartinfo->size()*/part_section_list_size*2];
    std::set<std::string> rebuildparts;
    partitioninfo->Truncate(connid,packvars,rebuildparts);
    if(ldb_index) {
        ldb_index->ClearMergeDB();
        ldb_index->RebuildByTruncate(packvars,part_section_list_size /*apartinfo->size()*/,rebuildparts,sid);
    }
    // total_p_f means maximum file id+1 ,not number of files.
    //int new_pack_files=total_p_f-apartinfo->getfiles();
    delete []packvars;
    int new_pack_files=partitioninfo->GetLastFile()+1;

    partitioninfo->CreateTruncateFile();
    partitioninfo->load(); // retore origin part info
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
        old_savefile_loc=savefile_loc[current_save_loc];
    unsigned int old_savepos_loc=savepos_loc[current_save_loc];
    // estimate natural size:
    natural_size_saved=natural_size_saved*(double)new_totalobjs/no_obj;
    no_obj=new_totalobjs;
    no_nulls=newnullsobj;
    i_min=newmin;
    i_max=newmax;
    no_pack=new_totalpack;
    total_p_f=new_pack_files;
    if(trunc_lastp) {
        if(newlastfid>0)
            GetFileSize(RCAttr::AttrPackFileNameDirect(attr_number, newlastfid, path), size);
        savepos_loc[current_save_loc]=(unsigned int)size;
        savefile_loc[current_save_loc]=newlastfid;
    }
    // skip A/B/S switch
    //if (current_state == SESSION_READ)
    file = FILE_READ_SESSION;
    //if(current_state == SESSION_WRITE)
    //  file = FILE_SAVE_SESSION;
    BHASSERT(FileFormat() == CLMD_FORMAT_RSC10_ID, "Invalid Attribute data file format!");

    Save(AttrFileName(file)+".ntrunc", dic.get(), tsize);
    dom_inj_mngr.Save();
    //since trans has not committed,restore variabls:
    no_obj=old_no_obj;
    no_nulls=old_no_nulls;
    i_min=old_i_min;
    i_max=old_i_max;
    compressed_size=old_compressed_size;
    natural_size_saved=old_natural_size_saved;
    no_pack=old_no_pack;
    total_p_f=old_total_p_f;
    savefile_loc[current_save_loc]=old_savefile_loc;
    savepos_loc[current_save_loc]=old_savepos_loc;
//   7. Table.ctb --> xxx.ntrunc :在上一级调用过程中处理
//   no_obj in table.ctb always 0(dma-526)
//   暂忽略这个过程，table.ctb无须变更
    return true;
}



void * LaunchWork(void *ptr)
{
    ((worker *) ptr)->work();
    return NULL;
}
//  on linux:
//  Get number of cpu online: sysconf(_SC_NPROCESSORS_ONLN);

// 返回CPU数量和 空闲CPU个数
double getfreecpu(int &cpuct)
{
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
        } else if(strncmp(line,"cpu",3)==0) sys_cpuct++;
        else break;
    }
    cpuct=sys_cpuct;
    if(cpuct==0) cpuct==1;
    if(line) free(line);
    fclose(fp);
    return (double)cpuct*idle/((double)user+nice+system+idle+iowait+irq+softirq);
}


int dma_mtreader::work()
{
    //Do thread work here
    Transaction &trans=*(Transaction *)objects[0];
    //int table_number=(_int64)objects[1];
    //int attr_number=(_int64)objects[2];
    unsigned int pack_no=(unsigned _int64)objects[1];
    //PackAllocator *m_allocator=(PackAllocator *)objects[4];
    //AttrPackPtr p = trans.GetAttrPack(PackCoordinate(table_number, attr_number, pack_no,0,0), *m_allocator);
    RCAttr *pAttr=(RCAttr *)objects[2];
    pAttr->LockPackForThread(pack_no,trans);
    //pAttr->UnlockPackFromUse(pack_no);
    LockStatus();
    isdone=true;
    Unlock();
    return 1;
}

#ifndef TIMEOFDAY
//#define TIMEOFDAY CLOCK_PROCESS_CPUTIME_ID
#define TIMEOFDAY CLOCK_REALTIME
#endif
#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
class mytimer
{
    struct timespec st,ct;
public:
    mytimer() {
        memset(&st,0,sizeof(timespec));
        memset(&ct,0,sizeof(timespec));
    }
    void Clear() {
        memset(&ct,0,sizeof(timespec));
    }
    void Start() {
        clock_gettime(TIMEOFDAY,&st);
    }
    void Stop() {
        timespec ed;
        clock_gettime(TIMEOFDAY,&ed);
        ct.tv_sec+=ed.tv_sec-st.tv_sec;
        ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
        st.tv_sec=ed.tv_sec;
        st.tv_nsec=ed.tv_nsec;
    }
    void Restart() {
        Clear();
        Start();
    }
    double GetTime() {
        return (double)ct.tv_sec+ct.tv_nsec/1e9;
    }
};
#endif


class myAutoTimer
{
    mytimer &tm;
public:
    myAutoTimer(mytimer &t):tm(t) {
        tm.Start();
    }
    ~myAutoTimer() {
        tm.Stop();
    }
};


MultiThread_Preload *MultiThread_Preload::instance=NULL;
#ifndef RANGEFULL
#define RANGEFULL 0x0000ffff
#endif
void RCAttr::DeliverPreLoad(_int64 *pahead,DMThreadGroup *pmtr, Transaction& trans,DMThreadData *pThreadData)
{
    _int64 &curahead=pahead[0];
    _int64 &procblocks=pahead[1];
    _int64 *ahead=pahead+2;
    _int64 old_procblocks=procblocks;
    DMThreadGroup &mtr=*pmtr;

    int forward_readat = -1;

    static long preload_dropped_packs=0;
    static long confict_preload_ct=0;
    static long lauched_thread_ct=0;
    mytimer tm_dist,tm_chk,tm_wait,tm_lauchp,tm_lauch,tm_getidle;
    tm_dist.Start();
    tm_wait.Start();
    long local_lauchct=0;

    //same attr concurrent load should be allowed
    try {
        LoadPackInfo(trans);
        tm_wait.Stop();
        while(procblocks<curahead) {
            forward_readat= (int)ahead[(int)procblocks]+packs_omitted;
            //已经预取到最后一个块 停止预取
            if(forward_readat>=no_pack) {
                cont_read_times=0;
                forward_readat=-1;
                break;
            }
            //JIRA DMA-131: mysqld进程core dump
            // 分析: 由于DPN结构中的pack_file_id 负值表示这个块里面都是空置,如果不做处理,直接传给线程,在读取数据时找不到数据文件和数据块
            tm_chk.Start();
            while(forward_readat<NoPack() && procblocks<curahead) {
                IBGuard m_dpns_guard(m_dpns_mutex);
                DPN& dpn( dpns[forward_readat] );
                //已经在缓冲区中的不需要重复预读取
                //已经在缓冲区中的不需要重复预读取
                // sasu-250:查询慢,由于LockPackForUse按精确条件检索,在这里如果发现有完整包数据就忽略,造成多线程失效
                // 因此,改为精确查找
                if(dpn.pack_file>=0 && dpn.is_stored // not empty pack (has stored pack data)
                   // not cached as full-pack(Either query full pack or not) while not query by condition
                   && (!GlobalDataCache::GetGlobalDataCache().HasObject(PackCoordinate(table_number, attr_number, forward_readat,0,0)))
                  )
                    break; // get a pack need to preload
                if(procblocks<curahead)
                    forward_readat=(int)ahead[(int)++procblocks]+packs_omitted;
                else {
                    forward_readat=-1;
                    break;
                }
            }
            tm_chk.Stop();

            if(forward_readat<0 || procblocks>=curahead || forward_readat>=NoPack()) break;
            int cpuct = 0;
            if(getfreecpu(cpuct)/(double)cpuct>0.02) { //CPU 空闲>2%则启动预读取包进程,否则放弃预读取操作
                tm_getidle.Start();
                // get idle thread,if all busy,skip
                DMThreadData *pbc=mtr.WaitIdleAndLock();
                tm_getidle.Stop();
                assert(pbc);
                tm_lauch.Start();
                pbc->StartInt(boost::bind(&RCAttr::LockPackForThread,this,forward_readat,boost::ref(trans)),trans.GetID());
                local_lauchct++;
                tm_lauch.Stop();
            } else {
                preload_dropped_packs++;
            }
            procblocks++;
            //if(rccontrol.isOn()) {
            //  rclog << lock << "prefetch ["<<procblocks-1<<"]="<<forward_readat<<"."<<unlock;
            //}
        }
    } catch(...) {
        /*
        IBGuard m_preload_guard(m_preload_mutex);
        running_ThreadData.erase(trans.GetID());
        */
        rclog<< lock << "RCAttr::DeliverPreLoad got exception.attr:"<<attr_number<<" transid :"<<trans.GetID()<<" pahead :c"<<curahead<<",p"<<procblocks
             <<unlock;
        throw;
    }
    /*
    //unregister thread data
    {
        IBGuard m_preload_guard(m_preload_mutex);
        running_ThreadData.erase(trans.GetID());
    }
    */
    tm_dist.Stop();
    if(rccontrol.isOn() && PRELOAD_OPER_LOG>=1) {
        rclog << lock << "prefetch packs "<<table_number<<"/"<<attr_number<<"["<<ahead[0]<<"-"<<ahead[curahead-1] <<"]/"<<curahead<<","<<
              "lauch threads "<<local_lauchct<<
              ",wait lock "<<confict_preload_ct<<",time stat t"
              <<F2MS(tm_dist.GetTime())<<"ms,c"
              <<F2MS(tm_chk.GetTime())<<"ms,w"
              <<F2MS(tm_wait.GetTime())<<"ms,i"
              <<F2MS(tm_getidle.GetTime())<<"ms,r"
              <<F2MS(tm_lauch.GetTime())<<"ms;"
              <<"inload "<<mtr.GetBusyThreads()<<",inpush "<<MultiThread_Preload::GetInstance()->GetInPush()<<"."<<unlock;
    }
}

bool checkPreloadThread(DMThreadData *pData,ulong sid,RCAttr *pattr)
{
    MultiThread_Preload::push_param *opts=(MultiThread_Preload::push_param *)pData->StructParam();
    return opts->trans->GetID()==sid && opts->pattr==pattr;
}

bool checkPackThread(DMThreadData *pData,ulong sid)
{
    return pData->IntegerParam()==sid;
}

void MultiThread_Preload::StopPrefetching(RCAttr *pattr,ulong sid)
{
    pusher->ClearWait(boost::bind(checkPreloadThread,_1,sid,pattr));
    pmtr->ClearWait(boost::bind(checkPackThread,_1,sid));
    pusher->WaitFor(boost::bind(checkPreloadThread,_1,sid,pattr));
    pmtr->WaitFor(boost::bind(checkPackThread,_1,sid));
}

void RCAttr::WaitPreload(ulong transid)
{
    MultiThread_Preload *pload=MultiThread_Preload::GetInstance();
    pload->StopPrefetching(this, transid);
}

void RCAttr::PreLoadPack(_int64 *pahead,ConnectionInfo& conn)
{
    if(pahead!=NULL) {
        if(pahead[0]-pahead[1]>0) {
            MultiThread_Preload *pload=MultiThread_Preload::GetInstance();
            pload->PushPreload(pahead,this,*conn.GetTransaction());
        }
    }
}

void RCAttr::LockPackForUse(_int64 *pahead,unsigned pack_no, Transaction& trans,boost::function0<void> notify_pack_load)
{
    static mytimer tm_lockpack,tm_dist;
    static long missingct=0,log_ct=0,dist_ct=0;
    myAutoTimer lpt(tm_lockpack);

    bool has_got=false;
    assert((int)pack_no < NoPack());
    LoadPackInfo(trans);
    {
        IBGuard m_dpns_guard(m_dpns_mutex);
        DPN& dpn( dpns[pack_no] );
        if(dpn.no_pack_locks) {
            dpn.no_pack_locks++;
            dpn.pack->Lock();
            has_got=true;
        } else if(!dpn.is_stored || dpn.pack_file<0)
            has_got=true;
    }

    if(pahead!=NULL) {
        if(pahead[0]-pahead[1]>0) {
            MultiThread_Preload *pload=MultiThread_Preload::GetInstance();
            pload->PushPreload(pahead,this,trans);
        }
    }

    log_ct++;
    if(log_ct>=1000 && rccontrol.isOn()) {
        log_ct=0;
        GlobalDataCache &gbc=GlobalDataCache::GetGlobalDataCache();
        rclog << lock << "Packloaded "<<gbc.getPackLoads()<<",waited "<<gbc.getReadWait()<<",inload " <<gbc.getPackLoadInProgress()  << ",inWait "<<gbc.getReadWaitInProgress()<<",hits: "<<gbc.getCacheHits()<<" misses:"<<gbc.getCacheMisses()<<",locktm "<<tm_lockpack.GetTime()<<",disttm "<< tm_dist.GetTime()<<",dist "<<dist_ct<<unlock;
        missingct=0;
        dist_ct=0;
    }
    if(has_got)
        return;
    missingct++;

    // for preload pack ,just pick pack from cache
    AttrPackPtr p = trans.GetAttrPack(PackCoordinate(table_number, attr_number, pack_no + packs_omitted,0,0), *m_allocator);
    {
        IBGuard m_dpns_guard(m_dpns_mutex);
        DPN& dpn( dpns[pack_no] );
        if(dpn.no_pack_locks) {

            dpn.no_pack_locks++;
            // GetAttrPack already does the lock
            return;
        } else if(dpn.pack_file<0 || !dpn.is_stored)
            return;
        dpn.no_pack_locks++;
        if(notify_pack_load)
            notify_pack_load();
        assert(!dpn.pack);
        dpn.pack = p;
    }
}

int RCAttr::LockPackForThread(unsigned pack_no, Transaction& trans) //, boost::function0<void> notify_pack_load)
{
    //DMA-349 give a invalid pack_no ,examp: 12622256 12622320
    // check before lauch thread ,but still pass wrong value to here.
    //assert((int)pack_no < NoPack());
    if((int)pack_no >= NoPack() || (int)pack_no<0) return 0;
    LoadPackInfo(trans);
    /* 不要修改 dpn的结构
    {
        IBGuard m_dpns_guard(m_dpns_mutex);
        DPN& dpn( dpns[pack_no] );
        if(dpn.no_pack_locks) {
            //it's not real read here,just preload
            //dpn.no_pack_locks++;
            //dpn.pack->Lock();
            return;
        } else if(!dpn.is_stored)
            return;
    }*/
    AttrPackPtr p = trans.GetAttrPack(PackCoordinate(table_number, attr_number,
                                      // packs_omitted has added in preload
                                      pack_no /*+ packs_omitted*/,0,0), *m_allocator);
    // always release lock:
    p->Unlock();

    /*不需要调整DPN
    {
        IBGuard m_dpns_guard(m_dpns_mutex);
        DPN& dpn( dpns[pack_no] );
        if(dpn.no_pack_locks) {
            //not lock here because this is not real read ,just preload
            //dpn.no_pack_locks++;
            // GetAttrPack already does the lock
            //dpn.pack->Lock();
            // the data pack has been loaded into buffer
            //release locker by GetAttrPack in case of release buffer failed
            dpn.pack->Unlock();
            return;
        }

        //let read process this preload pack
        //dpn.no_pack_locks++;

        //if(notify_pack_load)
        //  notify_pack_load();
        if(!dpn.pack) {
            //assert(!dpn.pack);
            dpn.pack = p;
            // unlock here as this block may not be readed anyway ,so global need to release this buffer on fulled.

        }
        //0 --> 1 : prepare to be unlock below:
        dpn.no_pack_locks++;
    }
    UnlockPackFromUse(pack_no);
    */
    //else
    //p->Release();
    return 1;
}

void RCAttr::LockPackForUse(unsigned pack_no)
{
    LockPackForUse(NULL,pack_no, ConnectionInfoOnTLS.Get());
}

void RCAttr::LockLastPackForUse(Transaction& trans)
{
    if(NoPack() > 0)
        LockPackForUse(NULL,NoPack() - 1, trans);
}

void RCAttr::UnlockLastPackFromUse()
{
    if(NoPack() > 0)
        UnlockPackFromUse(NoPack() - 1);
}

void RCAttr::UnlockPackFromUse(unsigned pack_no)
{
    MEASURE_FET("RCAttr::UnlockPackFromUse(...)");
    assert((int)pack_no<NoPack());
    LoadPackInfo();
    IBGuard m_dpns_guard(m_dpns_mutex);
    DPN& dpn( dpns[pack_no] );
    if(dpn.no_pack_locks) {
        dpn.no_pack_locks--;
        if (dpn.no_pack_locks==0) {
            if (dpn.pack->IsEmpty() || CachingLevel == 0)
                ConnectionInfoOnTLS->GetTransaction()->ResetAndUnlockOrDropPack(dpn.pack);
            else
                TrackableObject::UnlockAndResetOrDeletePack(dpn.pack);
            if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
                dpn.pack_mode = PACK_MODE_UNLOADED;
        } else
            dpn.pack->Unlock();
    }
}

bool RCAttr::OldFormat()
{
    return (FileFormat() < 10);
}

/******************************************
 * ABSwitcher implementation
 ******************************************/

ABSwitch ABSwitcher::GetState(std::string const&  path)
{
    string name;
    GenerateName(name, path);
    return (DoesFileExist(name)) ? ABS_B_STATE : ABS_A_STATE;
}

int ABSwitcher::FlipState(std::string const& path)
{
    string name;
    bool is_file_exist;

    GenerateName(name, path);

    is_file_exist = DoesFileExist(name);
    if (is_file_exist) {
        // File exists. Delete it now.
        try {
            RemoveFile(name);
        } catch(...) {
            rclog << lock << "ERROR: Failed to delete AB switch file " << name << unlock;
            return -1;
        }
    } else {
        // File does not exist. Create it now.
        try {
            IBFile ibfl;
            ibfl.OpenCreate(name);
        } catch(...) {
            rclog << lock << "ERROR: Failed to create AB switch file " << name << unlock;
            return -1;
        }
    }

    return 0;
}

void ABSwitcher::GenerateName(std::string& name, std::string const& path)
{
    name = path;
    name += "/";
    name += AB_SWITCH_FILE_NAME;
}

/*static*/
const char* ABSwitcher::SwitchName( ABSwitch value )
{
    if (value == ABS_A_STATE) return "ABS_A_STATE";
    return "ABS_B_STATE";
}

/////// RCAttr metadata methods
bool RCAttr::Load(string const& cmd_file,bool loadapindex)
{
    char* tmp_buf = NULL;
    uint bufsize;
    IBFile fattr_read;
    uint bytes_processed;

    tmp_buf = new char [10000];
    if (!tmp_buf) {
        rclog << lock << "Error: Memory allocation failed!" << unlock;
        return false;
    }

    try {
        fattr_read.OpenReadOnly(cmd_file);
        if (fattr_read.Read(tmp_buf, CLMD_HEADER_LEN) != CLMD_HEADER_LEN)
            return BH_FILE_ERROR;

        bytes_processed = LoadHeader(tmp_buf);

        bufsize = fattr_read.Read(tmp_buf, 10000);

        bytes_processed = LoadColumnInfo(tmp_buf);
        LoadSessionInfo(tmp_buf + bytes_processed, bufsize, &fattr_read);

        fattr_read.Close();
        if(ldb_index!=NULL) {
            delete ldb_index;
            ldb_index=NULL;
        }
        if(ct.IsPackIndex()&& current_state == SESSION_READ && loadapindex) {
            ldb_index=new AttrIndex(attr_number,path.c_str());
            ldb_index->LoadHeader();
            ldb_index->LoadForRead();
        }
        if(partitioninfo!=NULL) {
            delete partitioninfo;
            partitioninfo=NULL;
        }
        partitioninfo=new attr_partitions(attr_number,path.c_str());
        partitioninfo->SetSessionID(GetSaveSessionId());
        partitioninfo->LoadNFBlocks(nfblocks);


    } catch(DatabaseRCException& e) {
        delete [] tmp_buf;
        rclog << lock << "Error while opening file " << cmd_file<< " : " << e.what() << unlock;
        throw;
    }
    delete [] tmp_buf;
    return true;
}

void RCAttr::SaveDPN()
{
    MEASURE_FET("RCAttr::SaveDPN()");
    if(NoPack() == 0)
        return;
    const int buf_size = 10000;
    char buf[buf_size];
    IBFile fdpn;
    string fn(DPNFileName());

    try {
        fdpn.OpenCreate(fn);
        ushort buffer_pos = 0;
        int npack = NoPack();
        for(uint p = 0; p < npack - packs_omitted; p++) {
            if(buffer_pos + 37 > buf_size) {
                fdpn.Write(buf, buffer_pos);
                buffer_pos = 0;
            }
            if(p == 0)
                StoreDPN(npack - packs_omitted - 1, buf + buffer_pos); // last pack is saved first
            else
                StoreDPN(p - 1, buf + buffer_pos);
            if(p == 0) {
                // last pack is saved at the beginning of the file
                if(GetCurSaveLocation() == 1)
                    fdpn.Seek(37, SEEK_CUR);
                fdpn.Write(buf, 37);
                // the rest is appended to the end of the file
                if(p < npack - packs_omitted - 1)
                    fdpn.Seek((packs_omitted + 2) * 37, SEEK_SET);
            } else
                buffer_pos += 37;
        }
        if(buffer_pos > 0)
            fdpn.Write(buf, buffer_pos);
        fdpn.Close();
    } catch (DatabaseRCException&) {
        rclog << lock << "Internal error: unable to write data pack nodes to " << fn << ". " << GetErrorMessage(errno)
              << unlock;
        throw;
    }
}


void RCAttr::save_merge_table_header()
{
    if(rsi_hist_update || rsi_cmap_update) {
        SaveRSI();
    }

    BHASSERT(FileFormat()==10, "should be 'file_format==10'");

    SaveDPN();

    if(partitioninfo!=NULL) {
        partitioninfo->save(load_data_from_truncate_partition);
    }

    //--------------------------------------------------
    // 包索引的头部的保存在:merge_packindex_data函数中实现
    /*
    if(ldb_index!=NULL) {
        if(load_data_from_truncate_partition){  // save header.ltsession
            ldb_index->SaveHeader_ltsession();

        }else{// save header
            ldb_index->SaveHeader();
        }
    }
    */

    _int64 css = ComputeCompressedSize();

    if (current_state == SESSION_READ)
        file = FILE_READ_SESSION;
    if(current_state == SESSION_WRITE)
        file = FILE_SAVE_SESSION;
    BHASSERT(FileFormat() == CLMD_FORMAT_RSC10_ID, "Invalid Attribute data file format!");

    std::string fn = AttrFileName(file);
    RCAttr::Save(fn, dic.get(), css);
}

uint RCAttr::LoadHeader(const char* a_buf)
{
    natural_size_saved = 0;
    compressed_size = 0;

    bool is_lookup;
    NullMode nulls_mode;
    int scale = 0;
    int precision = 0;
    AttributeType   type;

    file_format = FILEFORMAT(a_buf);
    if (file_format == CLMD_FORMAT_INVALID_ID)
        return RCATTR_BADFORMAT;

    if ((uchar)a_buf[8] < 128) {
        type = (AttributeType)a_buf[8];
        is_lookup = true;
    } else {
        type = (AttributeType)((uchar)a_buf[8]-128);
        is_lookup = false;
    }

    if (((type == RC_VARCHAR ||
          type == RC_STRING) && is_lookup) ||
        ATI::IsNumericType(type) ||
        ATI::IsDateTimeType(type))
        pack_type = PackN;
    else
        pack_type = PackS;

    no_obj = *((_int64*)(a_buf + 9));
    no_nulls = *((_int64*)(a_buf + 17));

    nulls_mode = (NullMode)((uchar)a_buf[25] % 4);

    SetUnique(((a_buf[25] & 0x10) ? true : false));
    SetUniqueUpdated(((a_buf[25] & 0x20) ? true : false));

    if(a_buf[25] & 0x40)
        inserting_mode = 1;
    else
        inserting_mode = 0;

    int compress_type = 0;
    if(a_buf[25] & 0x80) {
        compress_type = Compress_Soft_Zip;
    } else if(a_buf[25] & 0x08) {
        compress_type = Compress_Snappy;
    } else {
        compress_type = Compress_DEFAULT;
    }

    precision = *((ushort*)(a_buf + 26));
    scale = a_buf[28];

    //  buf[29] = (Type().GetCompressType()== Compress_Lz4? 0x01:0);
    if(a_buf[29] & 0x01) {
        compress_type = Compress_Lz4;
    }

    ct.Initialize(type, nulls_mode, is_lookup, precision, scale,(a_buf[25]&0x04)!=0,(a_buf[25]&0x08)!=0,compress_type);

    no_pack = *((int*)(a_buf + 30));

    packs_offset = *((uint*)(a_buf + 34));
    dict_offset = *((uint*)(a_buf + 38));
    special_offset = *((uint*)(a_buf + 42));

    i_min = 0;
    i_max = 0;

    last_pack_index = no_pack != 0 ? no_pack - 1 : 0;
    no_pack_ptr = 0;

    return 46;  // 42 + 4, where 42 is an offset for "special_offset" and 4 is a size of of "special_offset"
}

uint RCAttr::LoadColumnInfo(const char* a_buf)
{
    const char* buf = a_buf;
//  size_t const name_ct(strlen(buf) + 1);

//  char name_tmp [name_ct];

    SetName(buf);
//  strcpy(name, buf);
    buf += strlen(buf) + 1;

    SetDescription(buf);
//  size_t const desc_ct(strlen(buf)+1);
//  desc = new char [desc_ct];
//  strcpy(desc, buf);
    buf += strlen(buf) + 1;

    total_p_f =* ((int*)buf);
    buf += 4;

    // the remainings after table of files' locations (if not default)
    int p_f = *((int*)buf);
    if (p_f != PF_END) {
        char const eMsg[] = "Error in attribute file: bad format";
        rclog << lock << eMsg << unlock;
        throw DatabaseRCException( eMsg );
    }
    buf += 4;
    return (uint)(a_buf - buf);
}

uint RCAttr::LoadSessionInfo(const char* cur_buf, uint actual_size, IBFile* fcmd_file)
{
    char* tmp_buf = new char [10000];

    // internal representation (which PackX class to use): numerical, text or binary?
    pack_info_collapsed = (dict_offset || no_pack);

    // load special blocks
    if(actual_size < special_offset + 27 + 2 * sizeof(_uint64)) {
        fcmd_file->Seek(special_offset, SEEK_SET);
        fcmd_file->Read(tmp_buf, 27 + 2 * sizeof(_uint64));
        cur_buf = tmp_buf;
    } else
        cur_buf = cur_buf + special_offset;

    if (special_offset>0 && *(uchar*)(cur_buf+5))
        fcmd_file->Seek(special_offset, SEEK_SET);

    while (special_offset > 0) {
        int b_len = *(int*)cur_buf;
        uchar b_type = *(uchar*)(cur_buf + 4);
        uchar b_next = *(uchar*)(cur_buf + 5);

        if(b_type == 0) {                               // type 0: the first special block, rollback and session info
            savefile_loc[0] = *((int*)(cur_buf + 6));       // the first special block: rollback and session info
            savefile_loc[1] = *((int*)(cur_buf + 10));
            savepos_loc[0] = *((uint*)(cur_buf + 14));
            savepos_loc[1] = *((uint*)(cur_buf + 18));
            current_save_loc = cur_buf[22];
            //if (open_session_type == 0 || open_session_type == 3) // only in case of read (normal/backup) sessions, otherwise read_session_id remains 0
            //  last_save_session_id = *((uint*)(cur_buf + 23));
            natural_size_saved = *((_uint64*)(cur_buf + 23 + sizeof(uint)));
            compressed_size = *((_uint64*)(cur_buf + 23 + sizeof(uint) + sizeof(_uint64)));
        } else
            rclog << lock << "Warning: special block (unknown type " << int(b_type) << ") ignored in attribute " << Name() << unlock;

        if (b_next == 0)
            special_offset = 0;
        else {
            fcmd_file->Seek(b_len, SEEK_CUR);
            fcmd_file->Read(&b_len, 4);
            fcmd_file->Seek(-4, SEEK_CUR);
            fcmd_file->Read(tmp_buf, b_len);
            cur_buf = tmp_buf;
        }
    }
    current_read_loc = current_save_loc;
    delete [] tmp_buf;
    return 1;
}
//   dynamic values using:
// no_obj(64)/no_nulls(64)/no_pack(u32)/total_p_f(32)/i_min(64)/i_max(64)/
// compressed_size(calc by comprs_size 64)/savefile_loc[2]/savepos_loc[2]
// natural_size_saved
bool RCAttr::Save(string const& cmd_file, FTree* dic, _int64 comprs_size)
{
    IBFile fattr;
    fattr.OpenCreateEmpty(cmd_file);

    const int buf_size = 10000;
    char buf[buf_size];

    strcpy(buf, CLMD_FORMAT_RSC10);

    // Reserved for security encryption info
    *((uint*)(buf + 4)) = 0;

    // The first bit (0x80) indicates string interpretation: 0 for dictionary, 1 for long text (>=RSc4)
    buf[8] = (char)((char)TypeName() + (Type().IsLookup() ? 0 : 0x80));
    *((_int64*)(buf + 9)) = no_obj;
    *((_int64*)(buf + 17)) = no_nulls;

    // buf[25] stores attribute's flags: null_mode + 4*declared_unique + 8*is_primary  + 16*is_unique + 32*is_unique_updated
    // Now declared_unique, is_primary are obsolete. Can be removed in future file format
    // 0x80 : hardware compress accelerate.
    //nullmode:0-3                        11;
    //isunique: 16                10000;
    //IsUniqueUpdated 32        100000
    //inserting_mode 0x40       1000000
    // compress_zip  0x80      10000000
    // packindex         0x04                100
    // bloom-filter    0x08             1000  -----> snappycompression

    /*
    buf[25] = char(Type().GetNullsMode()) + (IsUnique() ?16:0) + (IsUniqueUpdated() ?32:0) + (inserting_mode == 1? 0x40:0) +
              (Type().IsPackIndex()?0x04:0)+(Type().IsBloomFilter()?0x08:0 +
                      (Type().GetCompressType()!= Compress_DEFAULT) ? 0x80:0);
    */
    // replace by following ,add by liujs
    // bloomfilter --- > snappy
    buf[25] = char(Type().GetNullsMode()) + (IsUnique() ?16:0) + (IsUniqueUpdated() ?32:0) + (inserting_mode == 1? 0x40:0) +
              (Type().IsPackIndex()?0x04:0)+(Type().GetCompressType()== Compress_Snappy?0x08:0)+
              (Type().GetCompressType()== Compress_Soft_Zip? 0x80:0);

    *((ushort*)(buf + 26)) = Type().GetPrecision();
    buf[28] = Type().GetScale();

    // Now it is obsolete field. Can be removed in future file format
    buf[29] = (uchar)0;
    // lz4 use lz4 compression
    buf[29] = (Type().GetCompressType()== Compress_Lz4? 0x01:0);

    *((int*)(buf + 30)) = no_pack;

    // Actual values will be written at the end
    packs_offset = 0;
    dict_offset = 0;
    special_offset = 0;

    // Actual values will be written at the end
    *((uint*)(buf+34)) = packs_offset;
    *((uint*)(buf+38)) = dict_offset;
    *((uint*)(buf+42)) = special_offset;

    // Temporary, because there is no offset values yet
    fattr.Write(buf, 46,true);

    fattr.Write(Name(), (int)strlen(Name()) + 1,true);
    if (Description())
        fattr.Write(Description(),(int)strlen(Description()) + 1,true);
    else
        fattr.Write("\0", 1,true);

    // List of all nontrivial path names
    fattr.Write((char*)(&total_p_f),4,true);

    int p_f=PF_END;
    fattr.Write((char*)(&p_f),4,true);

    packs_offset = (uint)fattr.Tell();
    dict_offset = (uint)fattr.Tell();

    AttributeType a_type = TypeName();
    if(Type().IsLookup()) {
        *((_uint64*)(buf)) = i_min;
        *((_uint64*)(buf + 8)) = i_max;
        fattr.Write(buf, 16);
        uchar *dic_buf = new uchar [dic->ByteSize()];
        if (!dic_buf)
            rclog << lock << "Error: out of memory (" << dic->ByteSize() << " bytes failed). (23)" << unlock;
        uchar *p_dic_buf = dic_buf;
        dic->SaveData(p_dic_buf);
        fattr.Write((char*)dic_buf, dic->ByteSize(),true);
        delete [] dic_buf;
    } else if(ATI::IsNumericType(a_type)  ||
              ATI::IsDateTimeType(a_type) || ATI::IsDateTimeNType(a_type)) {
        *((_uint64*)(buf)) = i_min;
        *((_uint64*)(buf+8)) = i_max;
        fattr.Write(buf,16,true);
    }
    // ... other types of dictionary ...
    else {
        // e.g. string_no_dict==1
        dict_offset = 0;
    }

    special_offset = (uint)fattr.Tell();

    // saving session and rollback info
    uint bs = 27 + 2*sizeof(_uint64);
    *((uint*)(buf)) = bs;
    buf[4] = 0; // = block type
    buf[5] = 0; // = last block

    *((int*)(buf+6)) = savefile_loc[0];
    *((int*)(buf+10)) = savefile_loc[1];

    *((uint*)(buf+14)) = savepos_loc[0];
    *((uint*)(buf+18)) = savepos_loc[1];

    buf[22] = current_save_loc;
    *((uint*)(buf+23)) = save_session_id;

    *((_uint64*)(buf + 23 + sizeof(uint))) = natural_size_saved;
    *((_uint64*)(buf + 23 + sizeof(uint) + sizeof(_uint64))) = compressed_size;
    fattr.Write(buf, bs,true);

    // Update compressed data size
    compressed_size = comprs_size + fattr.Tell();
    fattr.Seek(-(long)sizeof(_uint64), SEEK_CUR);
    fattr.Write(&compressed_size, sizeof(_uint64),true);

    // Update addresses of sections
    *((uint*)(buf)) = packs_offset;
    *((uint*)(buf+4)) = dict_offset;
    *((uint*)(buf+8)) = special_offset;

    //seekp(34,ios::beg);
    fattr.Seek(34, SEEK_SET);
    fattr.Write(buf, 12,true);
    fattr.Close();
    return true;
}

IBFile *
RCAttr::GetPackFile(int pack_no)
{
    IBFile *fh = new IBFile();

    fh->OpenReadOnly(AttrPackFileName(pack_no - packs_omitted));
    fh->Seek(dpns[pack_no - packs_omitted].pack_addr,SEEK_SET);

    return fh;
}

int
RCAttr::Collapse()
{
    IBGuard guard(dpns_load_mutex);

    DeleteRSIs();
    if(!!dic) {
        dic->Release();
        dic.reset();
    }

    //>> begin: try to fix dma-1092, add by liujs, 20140401
    if(ldb_index!=NULL) {
        delete ldb_index;
        ldb_index=NULL;
    }
    if(partitioninfo!=NULL) {
        delete partitioninfo;
        partitioninfo=NULL;
    }
    //<< end: try to fix dma-1092

    GlobalDataCache::GetGlobalDataCache().ReleaseColumnDNPs(_logical_coord.co.rcattr[0],_logical_coord.co.rcattr[1]);
    dpns.clear();
    SetPackInfoCollapsed(true);
    return 0;
}

void
RCAttr::Release()

{
    Collapse();
}

bool
PackAllocator::isLoadable( int pack )
{
    attr->LoadPackInfo();
    return attr->dpns[pack].pack_mode == PACK_MODE_UNLOADED && attr->dpns[pack].is_stored;
}

AttrPackPtr
PackAllocator::lockedAlloc(const PackCoordinate& pack_coord)
{
    AttrPackPtr p;
    if(pt == PackS) {
        if(attr->Type().GetCompressType() == Compress_DEFAULT) {
            p = AttrPackPtr(new AttrPackS(pack_coord, attr->TypeName(), attr->GetInsertingMode(), attr->GetNoCompression(), 0));
        } else if(attr->Type().GetCompressType() == Compress_Soft_Zip) {
            p = AttrPackPtr(new AttrPackS_Zip(pack_coord, attr->TypeName(), attr->GetInsertingMode(), attr->GetNoCompression(), 0));
        } else if(attr->Type().GetCompressType() == Compress_Snappy) {
            p = AttrPackPtr(new AttrPackS_Snappy(pack_coord, attr->TypeName(), attr->GetInsertingMode(), attr->GetNoCompression(), 0));
        } else {
            p = AttrPackPtr(new AttrPackS_lz4(pack_coord, attr->TypeName(), attr->GetInsertingMode(), attr->GetNoCompression(), 0));
        }
    } else {
        if(attr->Type().GetCompressType() == Compress_DEFAULT) {
            p = AttrPackPtr(new AttrPackN(pack_coord, attr->TypeName(), attr->GetInsertingMode(), 0));
        } else if(attr->Type().GetCompressType() == Compress_Soft_Zip) {
            p = AttrPackPtr(new AttrPackN_Zip(pack_coord, attr->TypeName(), attr->GetInsertingMode(), 0));
        } else if(attr->Type().GetCompressType() == Compress_Snappy) {
            p = AttrPackPtr(new AttrPackN_Snappy(pack_coord, attr->TypeName(), attr->GetInsertingMode(), 0));
        } else {
            p = AttrPackPtr(new AttrPackN_lz4(pack_coord, attr->TypeName(), attr->GetInsertingMode(), 0));
        }
    }
    return p;
}

IBFile *
PackAllocator::getFile( int pack )
{
    return attr->GetPackFile(pack);
}

AttrPackPtr AttrPackLoad(const PackCoordinate& coord, PackAllocator &al)
{
    static _int64 unpacks=0;
    unpacks++;
    AttrPackPtr pack = al.lockedAlloc(coord);
    std::auto_ptr<DPN> dpn=al.GetAttrPtr()->GetDPNWithoutDataPack(pc_dp( coord ));
    if(dpn->pack_file<0 || !dpn->is_stored) {
        //never go here!
        return (pack);
    }
    boost::scoped_ptr<IBFile> f(al.getFile( pc_dp( coord ) ));
    pack->LoadData( f.get() );
    f->Close();
    pack->Uncompress(al.GetDomainInjectionManager());
    return ( pack );
}
