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
#include "compress/BitstreamCompressor.h"
#include "compress/TextCompressor.h"
#include "compress/PartDict.h"
#include "compress/NumCompressor.h"
#include "system/TextUtils.h"
#include "system/IBStream.h"
#include "WinTools.h"
#include "tools.h"
#include "ValueSet.h"
#include "system/MemoryManagement/MMGuard.h"
#include "zlib.h"
#include <snappy-c.h>

AttrPackS_Snappy::AttrPackS_Snappy(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
    :   AttrPackS(pc, attr_type, inserting_mode, no_compression, owner)
{
}

std::auto_ptr<AttrPack> AttrPackS_Snappy::Clone() const
{
    return std::auto_ptr<AttrPack>(new AttrPackS_Snappy(*this));
}

void AttrPackS_Snappy::AllocBuffers()
{
    if(data == NULL) {
        data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
        *data = 0;
        if(data_full_byte_size) {
            *data = (uchar*) alloc(data_full_byte_size+SNAPPY_EXTERNAL_MEM_SIZE, BLOCK_UNCOMPRESSED);
            if(!*data)
                rclog << lock << "Error: out of memory (" << data_full_byte_size+SNAPPY_EXTERNAL_MEM_SIZE << " bytes failed). (40)" << unlock;
        }
        data_id = 1;
    }
    assert(!lens && !index);
    lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
    index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
    if(no_nulls > 0) {
        if(!nulls)
            nulls = (uint*) alloc(2048 * sizeof(uint)+SNAPPY_EXTERNAL_MEM_SIZE, BLOCK_UNCOMPRESSED);
        if(no_obj < 65536)
            memset(nulls, 0, 8192);
    }
}


void AttrPackS_Snappy::Uncompress(DomainInjectionManager& dim)
{
    switch (ver) {
        case 0:
            UncompressOld();
            break;
        case 8:
            Uncompress8(dim);
            break;
        default:
            rclog << lock << "ERROR: wrong version of data pack format" << unlock;
            BHASSERT(0, "ERROR: wrong version of data pack format");
            break;
    }
}

void AttrPackS_Snappy::Uncompress8(DomainInjectionManager& dim)
{
    //>> Begin :add by liujs, compressOld and compress8 are the same
    UncompressOld();
    return ;
    //<< End: add by liujs
}



// snappy 解压的时候,解压的缓存大小需要:snappy_uncompressed_length 重新计算,
// 否则直接解压会返回:SNAPPY_BUFFER_TOO_SMALL
void AttrPackS_Snappy::UncompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
    FETOperator feto("AttrPackS_Snappy::Uncompress()");
    FETOperator feto1(string("aS[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
    NotifyDataPackDecompression(GetPackCoordinate());
#endif
    if(IsModeNoCompression())
        return;

    is_only_compressed = false;
    if(optimal_mode == 0 && ATI::IsBinType(attr_type)) {
        rclog << lock << "Error: Compression format no longer supported." << unlock;
        return;
    }

    AllocBuffers();
    //MMGuard<char*> tmp_index((char**)alloc(no_obj * sizeof(char*), BLOCK_TEMPORARY), *this);
    ////////////////////////////////////////////////////////
    int i; //,j,obj_317,no_of_rep,rle_bits;

    uint *cur_buf = (uint*) compressed_buf;

    ///////////////////////////////////////////////////////
    // decode nulls
    //char (*FREE_PLACE)(reinterpret_cast<char*> (-1));

    uint null_buf_size = 0;
    if(no_nulls > 0) {
        null_buf_size = (*(ushort*) cur_buf);
        if(!IsModeNullsCompressed()) { // flat null encoding
            memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
        } else {
            size_t _tmp_null_buf_size = 8192+SNAPPY_EXTERNAL_MEM_SIZE; //null_buf_size;
            size_t _origin_null_buf_size = 8192;
            size_t uncompressed_length=0;
            int fret = 0;
            fret = snappy_uncompressed_length((const char*)(((uchar*)cur_buf)+2), (size_t)null_buf_size, &uncompressed_length);
            if (fret == SNAPPY_OK) {
                if ( _tmp_null_buf_size < uncompressed_length) {
                    _tmp_null_buf_size = uncompressed_length;
                    nulls = (uint*)rc_realloc(nulls,uncompressed_length, BLOCK_UNCOMPRESSED);
                }
            } else {
                rclog << lock << "ERROR: AttrPackS_Snappy snappy_uncompressed_length error." << unlock;
                BHASSERT(0, "ERROR: AttrPackS_Snappy snappy_uncompressed_length error..");
            }

            fret = snappy_uncompress((const char*)(((uchar*)cur_buf)+2),(size_t)null_buf_size,(char*)nulls,&_tmp_null_buf_size);
            if(SNAPPY_OK != fret) {
                rclog << lock << "ERROR: snappy_uncompress return "<< fret << " . "<< unlock;
                BHASSERT(0, "ERROR: snappy_uncompress return != SNAPPY_OK .");
            }

            // For tests:code optimization,delete in release
            #ifdef DEBUG
            uint nulls_counted = 0;
            for(i = 0; i < 2048; i++) {
                nulls_counted += CalculateBinSum(nulls[i]);
            }

            if(no_nulls != nulls_counted)
                rclog << lock << "Error: AttrPackS_Snappy::Uncompress uncompressed wrong number of nulls." << unlock;
            #endif
        }
        cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
    }

    ///////////////////////////////////////////////////////
    if(optimal_mode == 0) {
        rclog << lock << "Error: Compression format no longer supported." << unlock;
    } else {
        comp_len_buf_size = *cur_buf;
        if((_uint64) * (cur_buf + 1) != 0) { // maxv + lens
            // ------------------------------------------------------------------------
            // 解压lens内容
            size_t src_size = (comp_len_buf_size - 8)*sizeof(char);
            size_t _tmp_len = (1<<16)*sizeof(uint)+SNAPPY_EXTERNAL_MEM_SIZE;

            size_t uncompressed_length=0;
            int fret = 0;
            fret = snappy_uncompressed_length((const char*)(cur_buf + 2),src_size, &uncompressed_length);
            if (fret == SNAPPY_OK) {
                if ( _tmp_len < uncompressed_length) {
                    _tmp_len = uncompressed_length;
                }
            } else {
                rclog << lock << "ERROR: AttrPackN_Snappy snappy_uncompressed_length error." << unlock;
                BHASSERT(0, "ERROR: AttrPackN_Snappy snappy_uncompressed_length error..");
            }

            MMGuard<uint> cn_ptr((uint*) alloc(_tmp_len, BLOCK_TEMPORARY), *this);

            fret = snappy_uncompress((const char*)(cur_buf + 2),src_size,(char*)cn_ptr.get(),&_tmp_len);
            if(SNAPPY_OK != fret) {
                rclog << lock << "ERROR: snappy_uncompress return "<< fret << " . "<< unlock;
                BHASSERT(0, "ERROR: snappy_uncompress return != SNAPPY_OK .");
            }


            // --------------------------------------------------------------------------
            // 解压data内容
            cur_buf = (uint*) ((char*) (cur_buf) + comp_len_buf_size);
            int dlen = *(int*) cur_buf;
            cur_buf += 1;

            assert(data_full_byte_size>0);
            if(data_full_byte_size > 0) {
                size_t _tmp_data_full_byte_size= data_full_byte_size+SNAPPY_EXTERNAL_MEM_SIZE;
                size_t uncompressed_length=0;
                int fret = 0;
                fret = snappy_uncompressed_length((const char*)cur_buf,(size_t)dlen, &uncompressed_length);
                if (fret == SNAPPY_OK) {
                    if ( _tmp_data_full_byte_size < uncompressed_length) {
                        _tmp_data_full_byte_size = uncompressed_length;
                        *data = (uchar*)rc_realloc(*data,uncompressed_length, BLOCK_UNCOMPRESSED);
                    }
                } else {
                    rclog << lock << "ERROR: AttrPackS_Snappy snappy_uncompressed_length error." << unlock;
                    BHASSERT(0, "ERROR: AttrPackS_Snappy snappy_uncompressed_length error..");
                }
            
                fret = snappy_uncompress((const char*)cur_buf,(size_t)dlen,(char*)*data,&_tmp_data_full_byte_size);
                if(SNAPPY_OK != fret) {
                    rclog << lock << "ERROR: snappy_uncompress return "<< fret << " . "<< unlock;
                    BHASSERT(0, "ERROR: snappy_uncompress return != SNAPPY_OK.");
                }
                data_full_byte_size = _tmp_data_full_byte_size;            
            }
        
            //---------------------------------------------------------------------------
            // 设置长度值和data字符串指针索引位置
            BHASSERT(!is_only_compressed, "The pack is compressed!");
            
            uchar *tidx=*data;  // 解压后的数据 
            if(no_nulls != no_obj) {
                if(nulls) {                    
                    uint *_pnull=nulls;
                    uint _nullmask=1;
                    uint *_pcn_ptr = (uint*)cn_ptr.get();
                    if(len_mode == sizeof(ushort)) {
                        ushort *_plens=(ushort*)lens;
                        for(uint o = 0; o < no_obj; o++) {
                            index[o] = 0;
                            if(!(*_pnull&_nullmask)) {//if(!((nulls[o>>5]&((uint)(1)<<(o%32)))!=0)/*!IsNull(int(o))*/) {
                                *_plens = (ushort)*_pcn_ptr++;
                                if(*_plens != 0){
                                    index[o]=tidx;
                                    tidx+=*_plens;
                                }
                            }
                            _plens++;
                            _nullmask<<=1;
                            if(_nullmask==0) {
                                _nullmask=1;_pnull++;
                            }
                        }
                    } else {
                        uint *_plens=(uint*)lens;
                        for(uint o = 0; o < no_obj; o++) {
                            index[o] = 0;
                            if(!(*_pnull&_nullmask)) {//if(!((nulls[o>>5]&((uint)(1)<<(o%32)))!=0)/*!IsNull(int(o))*/) {
                                *_plens = (uint)*_pcn_ptr++;
                                if(*_plens != 0){
                                    index[o]=tidx;
                                    tidx+=*_plens;
                                }
                            }
                            _plens++;
                            _nullmask<<=1;
                            if(_nullmask==0) {
                                _nullmask=1;_pnull++;
                            }
                        }

                    }
                } else { // if(!nulls)
                    uint *_pcn_ptr = (uint*)cn_ptr.get();
                    if(len_mode == sizeof(ushort)) {                       
                        ushort *_plens=(ushort*)lens;
                        for(uint o = 0; o < no_obj; o++) { 
                            *_plens = (ushort)*_pcn_ptr++;
                            if(*_plens !=0){
                                index[o]=tidx;
                                tidx+=*_plens;
                            }else{
                                index[o] = 0;
                            }
                            _plens++;
                        }
                    } else {
                        uint* _plens=(uint*)lens;
                        for(uint o = 0; o < no_obj; o++) {
                            *_plens = (uint)*_pcn_ptr++;
                            if(*_plens !=0){
                                index[o]=tidx;
                                tidx+=*_plens;
                            }else{
                                index[o] = 0;
                            }
                            _plens++;
                        }
                    }
                }
            } else { // if(no_nulls == no_obj)
                memset(lens,0x0,len_mode * no_obj * sizeof(char));
                
                /*
                for(uint o = 0; o < no_obj; o++) {
                    index[o] = 0;
                }
                */
                assert(data_full_byte_size == 0);
            }
        } else {//  if(maxv + lens ==0)        
            memset(lens,0x0,len_mode * no_obj * sizeof(char));
            /*
            for(uint o = 0; o < no_obj; o++) {
                index[o] = 0;
            } 
            */
            assert(data_full_byte_size == 0);
        }
    }   

    dealloc(compressed_buf);
    compressed_buf=0;
    comp_buf_size=0;
#ifdef FUNCTIONS_EXECUTION_TIMES
    SizeOfUncompressedDP += (1 + rc_msize(*data) + rc_msize(nulls) + rc_msize(lens) + rc_msize(index));
#endif
}

CompressionStatistics AttrPackS_Snappy::Compress(DomainInjectionManager& dim)
{
    if (!use_already_set_decomposer) {
        if (dim.HasCurrent()) {
            ver = 8;
            decomposer_id = dim.GetCurrentId();
        } else {
            ver = 0;
            decomposer_id = 0;
        }
    }

    switch (ver) {
        case 0:
            return CompressOld();
        case 8:
            return Compress8(dim.Get(decomposer_id));
        default:
            rclog << lock << "ERROR: wrong version of data pack format" << unlock;
            BHASSERT(0, "ERROR: wrong version of data pack format");
            break;
    }
    return CompressionStatistics();
}

CompressionStatistics AttrPackS_Snappy::Compress8(DomainInjectionDecomposer& decomposer)
{
    return CompressOld();
}

CompressionStatistics AttrPackS_Snappy::CompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
    std::stringstream s1;
    s1 << "aS[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
    FETOperator fet1(s1.str());
#endif
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Compress nulls:

    comp_len_buf_size = comp_null_buf_size = comp_buf_size = 0;
    MMGuard<uchar> comp_null_buf;
    if(no_nulls > 0) {
        if(ShouldNotCompress()) {
            comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
            comp_null_buf_size = ((no_obj + 7) / 8);
            ResetModeNullsCompressed();
        } else {
            comp_null_buf_size = ((no_obj + 7) / 8);

            size_t src_size = comp_null_buf_size;
            size_t compressed_msg_len = snappy_max_compressed_length(src_size);

            comp_null_buf = MMGuard<uchar>((uchar*)alloc(
                                               ((compressed_msg_len>src_size?compressed_msg_len:src_size) + 2) * sizeof(uchar),
                                               BLOCK_TEMPORARY), *this);

            uint cnbl = compressed_msg_len + 1;
            comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun

            //>> Begin: modify by liujs
            size_t _tmp_comp_null_buf_size = compressed_msg_len>src_size?compressed_msg_len:src_size; //comp_null_buf_size;
            int rt = 0;
            rt = snappy_compress((const char*) nulls, src_size ,(char*)comp_null_buf.get(), &_tmp_comp_null_buf_size);
            if(SNAPPY_OK != rt) {
                if(comp_null_buf[cnbl] != 0xBA) {
                    rclog << lock << "ERROR: buffer overrun by snappy_compress ,ret="<< rt << unlock;
                    BHASSERT(0, "ERROR: buffer overrun by snappy_compress .");
                }

                comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
                comp_null_buf_size = ((no_obj + 7) / 8);
                ResetModeNullsCompressed();
            } else {
                comp_null_buf_size=_tmp_comp_null_buf_size;
                SetModeNullsCompressed();
                if(comp_null_buf[cnbl] != 0xBA) {
                    rclog << lock << "ERROR: buffer overrun by  snappy_compress (N)." << unlock;
                    BHASSERT(0, "ERROR: buffer overrun by snappy_compress (N f).");
                }
            }

            //<< End: modify by liujs
        }
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Compress len:
    MMGuard<uint> comp_len_buf;

    //NumCompressor<uint> nc(ShouldNotCompress());
    MMGuard<uint> nc_buffer((uint*)alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);

    int onn = 0;
    uint maxv = 0;
    uint cv = 0;
    for(uint o = 0; o < no_obj; o++) {
        if(!IsNull(o)) {
            cv = GetSize(o);
            *(nc_buffer.get() + onn++) = cv;
            if(cv > maxv)
                maxv = cv;
        }
    }

    if(maxv != 0) {
        size_t src_size = (size_t)(onn * sizeof(uint));
        size_t compressed_msg_len = snappy_max_compressed_length(src_size);
        comp_len_buf_size = onn * sizeof(uint) + 128 + 8;
        if(compressed_msg_len >= onn * sizeof(uint)) {
            comp_len_buf_size = compressed_msg_len + 128 + 8;
        }

        comp_len_buf = MMGuard<uint>((uint*)alloc(comp_len_buf_size / 4 * sizeof(uint), BLOCK_TEMPORARY), *this);

        //>> Begin: modify by liujs
        int rt = 0;
        size_t _tmp_comp_len_buf_size = comp_len_buf_size - 8;
        rt = snappy_compress((const char*) nc_buffer.get(), src_size,(char*)(comp_len_buf.get()+2),&_tmp_comp_len_buf_size);
        if(SNAPPY_OK != rt) {
            rclog << lock << "ERROR: snappy_compress return = "<< rt <<" . "<< unlock;
            BHASSERT(0, "ERROR: snappy_compress return != SNAPPY_OK.");
        }
        //<< end: modify by liujs

        comp_len_buf_size = _tmp_comp_len_buf_size + 8; // 8 = sizeof(comp_len_buf_size) + sizeof(maxv)
    } else {
        comp_len_buf_size = 8;
        comp_len_buf = MMGuard<uint>((uint*) alloc(sizeof(uint) * 2, BLOCK_TEMPORARY), *this);
    }

    *comp_len_buf.get() = comp_len_buf_size;
    *(comp_len_buf.get() + 1) = maxv;


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Compress non nulls:
    //TextCompressor tc;
    int zlo = 0;
    for(uint obj = 0; obj < no_obj; obj++) {
        if(!IsNull(obj) && GetSize(obj) == 0) {
            zlo++;
        }
    }

    int dlen = data_full_byte_size + 128;
    char* comp_buf = (char*)alloc(dlen,BLOCK_TEMPORARY);

    if(data_full_byte_size) {
        int objs = (no_obj - no_nulls) - zlo;

        MMGuard<char*> tmp_index((char**)alloc(objs * sizeof(char*), BLOCK_TEMPORARY), *this);
        MMGuard<ushort> tmp_len((ushort*)alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);

        int nid = 0;

        //>> Begin:natural data len , add by liujs
        uLong _combination_buf_len = 0;
        uLong _combination_pos = 0;
        //<< End ,add by liujs

        for(int id = 0; id < (int) no_obj; id++) {
            if(!IsNull(id) && GetSize(id) != 0) {
                tmp_index[nid] = (char*) index[id];
                tmp_len[nid++] = GetSize(id);

                //>> Begin: natural data len,add by liujs
                _combination_buf_len += GetSize(id);
                //<< End: add by liujs
            }
        }

        MMGuard<char> tmp_combination_index((char*)alloc(_combination_buf_len + 1, BLOCK_TEMPORARY), *this);
        tmp_combination_index[_combination_buf_len] = '\0';

        for(int _i=0; _i<nid; _i++) {
            // merge the combination_index data
            memcpy((tmp_combination_index.get()+_combination_pos),tmp_index[_i],tmp_len[_i]);
            _combination_pos += tmp_len[_i];
        }

        int fret = 0;
        size_t _tmp_dlen = dlen;

        size_t src_size = (size_t)(_combination_buf_len);
        size_t compressed_msg_len = snappy_max_compressed_length(src_size);
        if(_tmp_dlen > _combination_buf_len) {
            _tmp_dlen = compressed_msg_len;
            comp_buf = (char*)rc_realloc(comp_buf,_tmp_dlen,BLOCK_TEMPORARY);
        }

        fret = snappy_compress((const char*)tmp_combination_index.get(),src_size,comp_buf,&_tmp_dlen);
        if( SNAPPY_OK != fret ) {
            dealloc(comp_buf);
            dealloc(compressed_buf);
            rclog << lock << "ERROR: snappy_compress return = "<< fret <<" . "<< unlock;
            BHASSERT(0, "ERROR: snappy_compress return != SNAPPY_OK.");
        }
        dlen = _tmp_dlen;
    } else {
        dlen = 0;
    }

    // caculate all compressed buff size
    comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

    MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
    uchar* p = new_compressed_buf.get();

    // Nulls
    if(no_nulls > 0) {
        *((ushort*) p) = (ushort) comp_null_buf_size;
        p += 2;
        memcpy(p, comp_null_buf.get(), comp_null_buf_size);
        p += comp_null_buf_size;
    }

    // Lens
    if(comp_len_buf_size)
        memcpy(p, comp_len_buf.get(), comp_len_buf_size);

    p += comp_len_buf_size;

    // non null object len
    *((int*) p) = dlen;
    p += sizeof(int);

    // non null object compressed buf
    if(dlen) {
        memcpy(p, comp_buf, dlen);
    }

    dealloc(compressed_buf);
    dealloc(comp_buf);

    // get compressed buf
    compressed_buf = new_compressed_buf.release();

    SetModeDataCompressed();
    compressed_up_to_date = true;
    CompressionStatistics stats;
    stats.previous_no_obj = previous_no_obj;
    stats.new_no_obj = NoObjs();
    return stats;
}


