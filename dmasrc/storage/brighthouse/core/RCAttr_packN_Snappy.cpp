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

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "bintools.h"
#include "compress/NumCompressor.h"
#include "WinTools.h"
#include "system/MemoryManagement/MMGuard.h"
#include "zlib.h"
#include <snappy-c.h>

AttrPackN_Snappy::AttrPackN_Snappy(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner)
    :   AttrPackN(pc, attr_type, inserting_mode,owner)
{
}

std::auto_ptr<AttrPack> AttrPackN_Snappy::Clone() const
{
    return std::auto_ptr<AttrPack>(new AttrPackN_Snappy(*this) );
}

template<typename etype> void AttrPackN_Snappy::_RemoveNullsAndCompress(etype e,char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv)
{
    MMGuard<etype> tmp_data;
    if(no_nulls > 0) {
        tmp_data = MMGuard<etype>((etype*) (alloc((no_obj - no_nulls) * sizeof(etype), BLOCK_TEMPORARY)), *this);
        for(uint i = 0, d = 0; i < no_obj; i++) {
            if(!IsNull(i))
                tmp_data[d++] = ((etype*) (data_full))[i];
        }
    } else {
        tmp_data = MMGuard<etype>((etype*)data_full, *this, false);
    }

    size_t _tmp_tmp_cb_len = tmp_cb_len;
    int rt = snappy_compress((char*)tmp_data.get(),(size_t)((no_obj - no_nulls) * sizeof(etype)),(char*)tmp_comp_buffer,&_tmp_tmp_cb_len);
    if(rt!=SNAPPY_OK) {
        rclog << lock << "ERROR: AttrPackN_Snappy RemoveNullsAndCompress compress2 error." << unlock;
        BHASSERT(0, "ERROR: AttrPackN_Snappy RemoveNullsAndCompress compress2 error..");
    }
    tmp_cb_len = _tmp_tmp_cb_len;
}


// snappy 解压的时候,解压的缓存大小需要:snappy_uncompressed_length 重新计算,
// 否则直接解压会返回:SNAPPY_BUFFER_TOO_SMALL 
template<typename etype> void AttrPackN_Snappy::_DecompressAndInsertNulls(etype e,uint *& cur_buf)
{
    uint _buf_len = *cur_buf;
    size_t _tmp_dest_len = 0;
    Bytef *_pbuf = (Bytef*)(cur_buf+3);

    size_t uncompressed_length=0;
    size_t data_full_size = (value_type * no_obj) + SNAPPY_EXTERNAL_MEM_SIZE;
    _tmp_dest_len = data_full_size;
    int fret = 0;    
    fret = snappy_uncompressed_length((const char*)_pbuf, _buf_len, &uncompressed_length);
    if (fret == SNAPPY_OK) {
        if ( data_full_size < uncompressed_length){
            _tmp_dest_len = uncompressed_length;
            data_full = rc_realloc(data_full,uncompressed_length, BLOCK_UNCOMPRESSED);
        }            
    }else{
        rclog << lock << "ERROR: AttrPackN_Snappy snappy_uncompressed_length error." << unlock;
        BHASSERT(0, "ERROR: AttrPackN_Snappy snappy_uncompressed_length error..");
    }
        
    fret = snappy_uncompress((const char*)_pbuf,_buf_len,(char*)data_full,&_tmp_dest_len);
    if(SNAPPY_OK != fret) {
        rclog << lock << "ERROR: AttrPackN_Snappy uncompress error." << unlock;
        BHASSERT(0, "ERROR: AttrPackN_Snappy uncompress error..");
    }
    //data_full = rc_realloc(data_full,data_full_size, BLOCK_UNCOMPRESSED);
    
    etype *d = ((etype*)(data_full)) + no_obj - 1;
    etype *s = ((etype*)(data_full)) + no_obj - no_nulls - 1;
    for(int i = no_obj - 1; d > s; i--) {
        if(IsNull(i)) {
            --d;
        } else {
            *(d--) = *(s--);
        }
    }
}

// 数值类型数据压缩，将0的个数压缩后，放入compressed_buf前面，非0的数据压缩后放入compressed_buf缓存的后面
CompressionStatistics AttrPackN_Snappy::Compress(DomainInjectionManager& dim)       // Create new optimal compressed buf. basing on full data.
{
    MEASURE_FET("AttrPackN_Snappy::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
    std::stringstream s1;
    s1 << "aN[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
    FETOperator fet1(s1.str());
#endif

    /////////////////////////////////////////////////////////////////////////
    uint *cur_buf = NULL;
    uint buffer_size = 0;
    MMGuard<char> tmp_comp_buffer;

    uint tmp_cb_len = 0;
    SetModeDataCompressed();

    // 直接压缩data_full是不是更简单呢?
    _uint64 maxv = 0;
    if(data_full) {     // else maxv remains 0
        _uint64 cv = 0;
        for(uint o = 0; o < no_obj; o++) {
            if(!IsNull(o)) {
                cv = (_uint64)GetVal64(o);
                if(cv > maxv)
                    maxv = cv;
            }
        }
    }

    // 1>  remove nulls and compress
    if(maxv != 0) {
        //BHASSERT(last_set + 1 == no_obj - no_nulls, "Expression evaluation failed!");

        if(value_type == UCHAR) {
            tmp_cb_len = (no_obj - no_nulls) * sizeof(uchar) + 20 + SNAPPY_EXTERNAL_MEM_SIZE;

            size_t src_size = tmp_cb_len;
            size_t compressed_msg_len = snappy_max_compressed_length(src_size);
            if(tmp_cb_len<compressed_msg_len) {
                tmp_cb_len = compressed_msg_len;
            }
            
            uchar e;
            if(tmp_cb_len)
                tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
            _RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
        } else if(value_type == USHORT) {
            tmp_cb_len = (no_obj - no_nulls) * sizeof(ushort) + 20 +SNAPPY_EXTERNAL_MEM_SIZE;

            size_t src_size = tmp_cb_len;
            size_t compressed_msg_len = snappy_max_compressed_length(src_size);
            if(tmp_cb_len<compressed_msg_len) {
                tmp_cb_len = compressed_msg_len;
            }


            if(tmp_cb_len)
                tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
            ushort e;
            _RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
        } else if(value_type == UINT) {
            tmp_cb_len = (no_obj - no_nulls) * sizeof(uint) + 20 +SNAPPY_EXTERNAL_MEM_SIZE;

            size_t src_size = tmp_cb_len;
            size_t compressed_msg_len = snappy_max_compressed_length(src_size);
            if(tmp_cb_len<compressed_msg_len) {
                tmp_cb_len = compressed_msg_len;
            }

            if(tmp_cb_len)
                tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
            uint e;
            _RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
        } else {
            tmp_cb_len = (no_obj - no_nulls) * sizeof(_uint64) + 20 +SNAPPY_EXTERNAL_MEM_SIZE;

            size_t src_size = tmp_cb_len;
            size_t compressed_msg_len = snappy_max_compressed_length(src_size);
            if(tmp_cb_len<compressed_msg_len) {
                tmp_cb_len = compressed_msg_len;
            }

            if(tmp_cb_len)
                tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
            _uint64 e;
            _RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
        }
        buffer_size += tmp_cb_len;
    }
    buffer_size += 12;   // 12 = sizeof(tmp_cb_len) + sizeof(maxv)

    ////////////////////////////////////////////////////////////////////////////////////
    // 2> compress nulls
    uint null_buf_size = ((no_obj+7)/8);

    size_t src_size = null_buf_size;
    size_t compressed_msg_len = snappy_max_compressed_length(src_size);
    if(null_buf_size<compressed_msg_len) {
        null_buf_size = compressed_msg_len;
    }

    MMGuard<uchar> comp_null_buf;
    //IBHeapAutoDestructor del((void*&)comp_null_buf, *this);
    if(no_nulls > 0) {
        if(ShouldNotCompress()) {
            comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
            null_buf_size = ((no_obj + 7) / 8);
            ResetModeNullsCompressed();
        } else {
            comp_null_buf = MMGuard<uchar>((uchar*)alloc((null_buf_size + SNAPPY_EXTERNAL_MEM_SIZE) * sizeof(char), BLOCK_TEMPORARY), *this);
            uint cnbl = null_buf_size + 1;
            comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun

            int rt = 0;
            size_t _tmp_null_buf_size = (null_buf_size + SNAPPY_EXTERNAL_MEM_SIZE) * sizeof(char);
            rt = snappy_compress((const char*) nulls, src_size ,(char*)comp_null_buf.get(), &_tmp_null_buf_size);
            if(SNAPPY_OK == rt) {
                null_buf_size = _tmp_null_buf_size;
                SetModeNullsCompressed();
            } else {
                ResetModeNullsCompressed();
                rclog << lock << "ERROR: compress2 return error. (N)." << unlock;
                BHASSERT(0, "ERROR: compress2 return error. (N f).");
            }
        }
        buffer_size += null_buf_size + 2;
    }
    dealloc(compressed_buf);
    compressed_buf = 0;
    compressed_buf= (uchar*)alloc(buffer_size*sizeof(uchar), BLOCK_COMPRESSED);

    if(!compressed_buf) rclog << lock << "Error: out of memory (" << buffer_size << " bytes failed). (29)" << unlock;
    memset(compressed_buf, 0, buffer_size);
    cur_buf = (uint*)compressed_buf;
    if(no_nulls > 0) {
        /*// try to fix dma-1672
          if(null_buf_size > 8192)
             throw DatabaseRCException("Unexpected bytes found (AttrPackN_Snappy::Compress).");
        */    
#ifdef _MSC_VER
        __assume(null_buf_size <= 8192);
#endif
        *(ushort*) compressed_buf = (ushort) null_buf_size;
#pragma warning(suppress: 6385)
        memcpy(compressed_buf + 2, comp_null_buf.get(), null_buf_size);
        cur_buf = (uint*) (compressed_buf + null_buf_size + 2);
    }

    ////////////////////////////////////////////////////////////////////////////////////

    *cur_buf = tmp_cb_len;
    *(_uint64*) (cur_buf + 1) = maxv;
    memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
    comp_buf_size = buffer_size;
    compressed_up_to_date = true;

    CompressionStatistics stats;
    stats.new_no_obj = NoObjs();
    return stats;
}

void AttrPackN_Snappy::Uncompress(DomainInjectionManager& dim)      // Create full_data basing on compressed buf.
{
    MEASURE_FET("AttrPackN_Snappy::Uncompress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
    FETOperator feto1(string("aN[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
    NotifyDataPackDecompression(GetPackCoordinate());
#endif
    is_only_compressed = false;
    if(IsModeNoCompression())
        return;
    assert(compressed_buf);

    uint *cur_buf=(uint*)compressed_buf;

    if(data_full == NULL && bit_rate > 0 && value_type * no_obj){
        data_full = alloc((value_type * no_obj)+SNAPPY_EXTERNAL_MEM_SIZE, BLOCK_UNCOMPRESSED);
    }

    ///////////////////////////////////////////////////////////////
    // decompress nulls
    if(no_nulls > 0) {
        uint null_buf_size = 0;
        if(nulls == NULL){
            nulls = (uint*) alloc((2048 * sizeof(uint) + SNAPPY_EXTERNAL_MEM_SIZE), BLOCK_UNCOMPRESSED);
        }
        
        if(no_obj < 65536){
            memset(nulls, 0, 8192);
        }
        
        null_buf_size = (*(ushort*) cur_buf);
        if(null_buf_size > 8192){
            throw DatabaseRCException("Unexpected bytes found in data pack (AttrPackN_Snappy::Uncompress).");
        }
        
        if(!IsModeNullsCompressed() ){ // no nulls compression
            memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
        }
        else {
			size_t _tmp_no_nulls = 8192+SNAPPY_EXTERNAL_MEM_SIZE;
			size_t _origin_null_buf_size = 8192;
            size_t uncompressed_length=0;
            int fret = 0;    
            fret = snappy_uncompressed_length((const char*)(((uchar*)cur_buf)+2),(size_t)null_buf_size, &uncompressed_length);
            if (fret == SNAPPY_OK) {
                if ( _tmp_no_nulls < uncompressed_length){
                    _tmp_no_nulls = uncompressed_length;
                    nulls = (uint*)rc_realloc(nulls,uncompressed_length, BLOCK_UNCOMPRESSED);
                }            
            }else{
                rclog << lock << "ERROR: AttrPackN_Snappy snappy_uncompressed_length error." << unlock;
                BHASSERT(0, "ERROR: AttrPackN_Snappy snappy_uncompressed_length error..");
            }
            
            fret = snappy_uncompress((const char*)(((uchar*)cur_buf) + 2),(size_t)null_buf_size,(char*)nulls,&_tmp_no_nulls);
            if( SNAPPY_OK != fret) {
                rclog << lock << "ERROR: buffer overrun by uncompress." << unlock;
                BHASSERT(0, "ERROR: snappy_uncompress return != SNAPPY_OK .");
            }
            //nulls = (uint*)rc_realloc(nulls,_origin_null_buf_size, BLOCK_UNCOMPRESSED);           
            
            // For tests:
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
            uint nulls_counted = 0;
            for(uint i = 0; i < 2048; i++)
                nulls_counted += CalculateBinSum(nulls[i]);
            if(no_nulls != nulls_counted)
                throw DatabaseRCException("AttrPackN::Uncompress uncompressed wrong number of nulls.");
#endif
        }
        cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
    }

    ////////////////////////////////////////////////////////////////////////////////////
    // decompress non null object
    if(!IsModeValid()) {
        rclog << lock << "Unexpected byte in data pack (AttrPackN_Snappy)." << unlock;
        throw DatabaseRCException("Unexpected byte in data pack (AttrPackN_Snappy).");
    } else {
        if(IsModeDataCompressed() && bit_rate > 0 && *(_uint64*) (cur_buf + 1) != (_uint64) 0) {
            if(value_type == UCHAR) {
                uchar e;
                _DecompressAndInsertNulls(e,cur_buf);
            } else if(value_type == USHORT) {
                ushort e;
                _DecompressAndInsertNulls(e,cur_buf);
            } else if(value_type == UINT) {
                uint e;
                _DecompressAndInsertNulls(e,cur_buf);
            } else {
                _uint64 e;
                _DecompressAndInsertNulls(e,cur_buf);
            }
        } else if(bit_rate > 0) {
            for(uint o = 0; o < no_obj; o++)
                if(!IsNull(int(o)))
                    SetVal64(o, 0);
        }
    }

    compressed_up_to_date = true;
    dealloc(compressed_buf);
    compressed_buf=0;
    comp_buf_size=0;

#ifdef FUNCTIONS_EXECUTION_TIMES
    SizeOfUncompressedDP += (rc_msize(data_full) + rc_msize(nulls));
#endif
}
